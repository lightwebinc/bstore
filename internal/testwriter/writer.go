// Package testwriter is a minimal bstore writer for tests only: it funds
// itself from a local test chain's coinbase, builds an object's CHIRP
// closure, mints its piece carriers, a hold output and the entry, submits
// them, asks hosts for advertisements, challenges providers, verifies their
// signed responses as a buyer does (spec section 7.4), buys renewals, and
// settles a bounty by rank with BRC-29 outputs and remittances (spec
// section 9.5). It is the least of the publisher client the provider's
// tests need; the publisher client proper is another component. Nothing
// here is for real use.
package testwriter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	authhttp "github.com/bsv-blockchain/go-sdk/auth/clients/authhttp"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/purse"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Profile is the test writer's wallet profile.
var Profile = bwallet.Profile{FundProtocol: storerec.Protocol, FundKeyID: storerec.KeyFund, FundBasket: "bstore fund",
	Version: "bstore-testwriter", LegacyPoolFile: "bstore-testwriter-pool.json"}

// Writer is one writer identity over a local chain.
type Writer struct {
	W       *bwallet.Embedded
	Signer  *bwallet.Signer
	RPC     *nodeapi.RPC
	Asset   *nodeapi.Asset
	Settler publish.Settler
	Headers chaintracker.ChainTracker
	// HTTP reaches providers (it trusts their test certificate) and hosts.
	HTTP   *http.Client
	Bucket string
	Fees   mint.Fees

	// One AuthFetch for every request, so one BRC-104 session per
	// provider; its wallet is the purse of the request in flight.
	af     *authhttp.AuthFetch
	paying *swapPurse
	mu     sync.Mutex
}

// swapPurse is the AuthFetch's wallet: the purse of the call in flight.
type swapPurse struct{ *purse.Purse }

// New creates and funds a writer under dir.
func New(ctx context.Context, dir, bucket string, rpc *nodeapi.RPC, asset *nodeapi.Asset, headers chaintracker.ChainTracker, hc *http.Client) (*Writer, error) {
	w, err := bwallet.Create(dir, Profile)
	if err != nil {
		return nil, err
	}
	w.Originator = "bstore-testwriter"
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, rpc, asset, 104, 0); err != nil {
		return nil, err
	}
	return &Writer{W: w, Signer: w.Signer(), RPC: rpc, Asset: asset, Settler: &publish.RPCSettler{RPC: rpc},
		Headers: headers, HTTP: hc, Bucket: bucket, Fees: mint.DefaultFees}, nil
}

// Identity is the writer's key.
func (w *Writer) Identity() []byte { return w.Signer.Identity.Compressed() }

func (w *Writer) payer(ctx context.Context) (*producer.Payer, error) {
	h, err := w.Asset.BestHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &producer.Payer{Pool: w.W.Pool, Tip: h.Height, Keys: map[string]*bwallet.Signer{w.Signer.IdentityHex(): w.Signer},
		Settler: w.Settler, Asset: w.Asset, Fees: w.Fees}, nil
}

// mine sends tx and returns it with its proof.
func (w *Writer) mine(ctx context.Context, tx *transaction.Transaction, pay *producer.Payer) (*transaction.Transaction, error) {
	if err := w.Settler.Submit(ctx, tx); err != nil {
		return nil, err
	}
	mp, h, err := nodeapi.WaitMined(ctx, w.Asset, tx.TxID().String(), 20*time.Millisecond)
	if err != nil {
		return nil, err
	}
	tx.MerklePath = mp
	if pay != nil {
		pay.Change(tx, h, mp)
	}
	return tx, nil
}

func params(d pushdrop.Derivation) carrier.Params {
	return carrier.Params{Derivation: d, FundingTag: storerec.TagFunding,
		ValidatePayload: func([]byte) error { return nil }, ErrIdentity: storerec.ErrIdentity}
}

// Tree mints a funding tree of n outputs of one satoshi under d.
func (w *Writer) Tree(ctx context.Context, d pushdrop.Derivation, n int) (*transaction.Transaction, error) {
	pay, err := w.payer(ctx)
	if err != nil {
		return nil, err
	}
	lock, err := carrier.FundingLock(ctx, w.W, w.W.Originator, params(d))
	if err != nil {
		return nil, err
	}
	fee, err := pay.TakeAtLeast(ctx, uint64(n)+w.Fees.Floor*4)
	if err != nil {
		return nil, err
	}
	change, _ := w.Signer.FundScript()
	tx, err := mint.FundingTree(lock, n, 1, fee, change, w.Fees)
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	return w.mine(ctx, tx, pay)
}

// Carrier mints a carrier of record on output vout of tree, under d, and
// returns its Atomic BEEF and commitment (display txid).
func (w *Writer) Carrier(ctx context.Context, d pushdrop.Derivation, record []byte, tree *transaction.Transaction, vout uint32) ([]byte, string, error) {
	tx, err := carrier.Mint(ctx, w.W, w.W.Originator, params(d), record, tree, vout)
	if err != nil {
		return nil, "", err
	}
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return nil, "", err
	}
	return beef, tx.TxID().String(), nil
}

// Object is an object built for a put: its closure and its pieces' BEEF.
type Object struct {
	Content []byte
	Built   *storerec.Built
	Pieces  [][]byte
}

// Build builds content's closure and mints a carrier for each piece.
func (w *Writer) Build(ctx context.Context, content []byte) (*Object, error) {
	b, err := storerec.Build(content)
	if err != nil {
		return nil, err
	}
	pieces := b.Pieces(w.Bucket, w.Identity())
	tree, err := w.Tree(ctx, storerec.PieceDerivation, len(pieces))
	if err != nil {
		return nil, err
	}
	o := &Object{Content: content, Built: b}
	for i, pc := range pieces {
		rec, err := pc.Encode()
		if err != nil {
			return nil, err
		}
		beef, _, err := w.Carrier(ctx, storerec.PieceDerivation, rec, tree, uint32(i))
		if err != nil {
			return nil, err
		}
		o.Pieces = append(o.Pieces, beef)
	}
	return o, nil
}

// Hold is a mined hold output.
type Hold struct {
	Tx   *transaction.Transaction
	Vout uint32
	Sats uint64
}

// HoldOutput mints a hold output of sats.
func (w *Writer) HoldOutput(ctx context.Context, sats uint64) (*Hold, error) {
	key, err := storerec.LockingKey(storerec.HoldDerivation, w.Identity())
	if err != nil {
		return nil, err
	}
	s := script.Script(storerec.HoldScript(key))
	pay, err := w.payer(ctx)
	if err != nil {
		return nil, err
	}
	fee, err := pay.TakeAtLeast(ctx, sats+w.Fees.Floor*4)
	if err != nil {
		return nil, err
	}
	change, _ := w.Signer.FundScript()
	tx, err := mint.Transition(&s, sats, nil, fee, change, w.Fees)
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	if tx, err = w.mine(ctx, tx, pay); err != nil {
		return nil, err
	}
	return &Hold{Tx: tx, Vout: 0, Sats: sats}, nil
}

// AloneBEEF is a mined transaction as a host admits a mined object.
func AloneBEEF(tx *transaction.Transaction) ([]byte, error) {
	c, err := transaction.NewTransactionFromBytes(tx.Bytes())
	if err != nil {
		return nil, err
	}
	c.MerklePath = tx.MerklePath
	return c.BEEF()
}

// Terms are an entry's hold terms before the hold output is known.
type Terms struct {
	Sats                 uint64
	T, K                 uint64
	Retention            uint64
	DeadlineAhead        uint64
	CheckC               uint64
	ContentType          string
	Created              time.Time
	NoHold               bool
	ProofRootOverride    *[32]byte
	ContentHashOverride  *[32]byte
	SizeOverride         *uint64
	ExtraEntryKeySuffix  string
	BucketIDOverride     *[32]byte
	ReuseHold            *Hold
	DeadlineAbsolute     uint64
	SkipHoldConfirmation bool
}

// Put is an entry made.
type Put struct {
	Object *Object
	Entry  *storerec.Entry
	BEEF   []byte
	C      string // display txid
	Hold   *Hold
}

// Entry mints the hold output (unless NoHold) and the put entry for key.
func (w *Writer) Entry(ctx context.Context, o *Object, key string, t Terms) (*Put, error) {
	created := t.Created
	if created.IsZero() {
		created = time.Now()
	}
	e := &storerec.Entry{Bucket: w.Bucket, ID: sha256.Sum256([]byte("bstore test bucket")), Writer: w.Identity(), Key: key,
		Kind: storerec.KindPut, Created: uint64(created.UnixMilli()), Size: o.Built.Size, Root: o.Built.RootHash,
		ContentHash: o.Built.ContentHash, ProofRoot: o.Built.ProofRoot, ProofProfile: 1, ContentType: "application/octet-stream"}
	if t.ContentType != "" {
		e.ContentType = t.ContentType
	}
	if t.ProofRootOverride != nil {
		e.ProofRoot = *t.ProofRootOverride
	}
	put := &Put{Object: o, Entry: e}
	if !t.NoHold {
		h := t.ReuseHold
		if h == nil {
			var err error
			if h, err = w.HoldOutput(ctx, t.Sats); err != nil {
				return nil, err
			}
		}
		tip, err := w.Asset.BestHeader(ctx)
		if err != nil {
			return nil, err
		}
		deadline := uint64(tip.Height) + t.DeadlineAhead
		if t.DeadlineAbsolute != 0 {
			deadline = t.DeadlineAbsolute
		}
		e.Hold = &storerec.Hold{Txid: [32]byte(*h.Tx.TxID()), Vout: uint64(h.Vout), Satoshis: h.Sats, T: t.T, K: t.K,
			Retention: t.Retention, Deadline: deadline, CheckC: t.CheckC}
		put.Hold = h
	}
	rec, err := e.Encode()
	if err != nil {
		return nil, err
	}
	tree, err := w.Tree(ctx, storerec.EntryDerivation, 1)
	if err != nil {
		return nil, err
	}
	beef, c, err := w.Carrier(ctx, storerec.EntryDerivation, rec, tree, 0)
	if err != nil {
		return nil, err
	}
	put.BEEF, put.C = beef, c
	return put, nil
}

// Submit posts a BEEF to base's /submit on topic and returns the answer.
func (w *Writer) Submit(ctx context.Context, base, topic string, beef []byte) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/submit", bytes.NewReader(beef))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", topic)
	res, err := w.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("submit to %s on %s: %d %s", base, topic, res.StatusCode, b)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("submit to %s: %s", base, b)
	}
	return out, nil
}

// Admitted reports whether a submit answer admitted any output for topic.
func Admitted(ans map[string]any, topic string) (bool, string) {
	t, _ := ans[topic].(map[string]any)
	if t == nil {
		return false, "no answer for the topic"
	}
	a, _ := t["outputsToAdmit"].([]any)
	why, _ := t["refused"].(string)
	return len(a) > 0, why
}

// Ad is an advertisement a host answered.
type Ad struct {
	Tx   *transaction.Transaction
	Txid string
	Vout uint32
	Ad   *storerec.Advertisement
}

// Ads asks a host's ls_bstore for root's advertisements.
func (w *Writer) Ads(ctx context.Context, host string, root [32]byte) ([]Ad, error) {
	q := map[string]any{"service": storerec.LookupService, "query": map[string]string{"bucket": w.Bucket, "root": hex.EncodeToString(root[:]), "after": ""}}
	body, _ := json.Marshal(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(host, "/")+"/lookup", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := w.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var ans struct {
		Outputs []struct {
			Beef        []byte `json:"beef"`
			OutputIndex uint32 `json:"outputIndex"`
		} `json:"outputs"`
	}
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lookup: %d %s", res.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &ans); err != nil {
		var alt struct {
			Outputs []struct {
				Beef        []int  `json:"beef"`
				OutputIndex uint32 `json:"outputIndex"`
			} `json:"outputs"`
		}
		if err := json.Unmarshal(raw, &alt); err != nil {
			return nil, fmt.Errorf("lookup answer: %w", err)
		}
		for _, o := range alt.Outputs {
			b := make([]byte, len(o.Beef))
			for i, v := range o.Beef {
				b[i] = byte(v)
			}
			ans.Outputs = append(ans.Outputs, struct {
				Beef        []byte `json:"beef"`
				OutputIndex uint32 `json:"outputIndex"`
			}{b, o.OutputIndex})
		}
	}
	var out []Ad
	for _, o := range ans.Outputs {
		tx, err := transaction.NewTransactionFromBEEF(o.Beef)
		if err != nil || int(o.OutputIndex) >= len(tx.Outputs) {
			continue
		}
		ad, err := storerec.DecodeAdvertisement(*tx.Outputs[o.OutputIndex].LockingScript)
		if err != nil {
			continue
		}
		out = append(out, Ad{Tx: tx, Txid: tx.TxID().String(), Vout: o.OutputIndex, Ad: ad})
	}
	return out, nil
}

// purse is the writer's wallet as an AuthFetch wallet that pays a 402.
func (w *Writer) purse(ctx context.Context, maxPay uint64) (*purse.Purse, error) {
	if _, err := w.payer(ctx); err != nil {
		return nil, err
	}
	return &purse.Purse{Embedded: w.W, NewPayer: func() *producer.Payer {
		p, _ := w.payer(ctx)
		return p
	}, Fees: w.Fees, MaxPay: maxPay, Settler: w.Settler, Asset: w.Asset, Headers: w.Headers, Wait: 20 * time.Second, Poll: 20 * time.Millisecond}, nil
}

// Call is one authenticated request's answer.
type Call struct {
	Status int
	Body   []byte
	Paid   uint64
}

// Post sends body to url over BRC-104, paying a 402 up to maxPay.
func (w *Writer) Post(ctx context.Context, url string, body []byte, maxPay uint64) (*Call, error) {
	pu, err := w.purse(ctx, maxPay)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.af == nil {
		w.paying = &swapPurse{}
		w.af = authhttp.New(w.paying, authhttp.WithoutLogging(), authhttp.WithHttpClient(w.HTTP))
	}
	w.paying.Purse = pu
	res, err := w.af.Fetch(ctx, url, &authhttp.SimplifiedFetchRequestOptions{Method: http.MethodPost,
		Headers: map[string]string{"content-type": "application/json"}, Body: body})
	if err != nil {
		pu.Refund()
		return nil, err
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, storerec.MaxSignedResponse+1))
	c := &Call{Status: res.StatusCode, Body: b}
	if v := res.Header.Get("x-bsv-payment-satoshis-paid"); v != "" || pu.Settle() != nil {
		c.Paid = 1
	}
	if res.StatusCode >= 400 {
		pu.Refund()
	}
	return c, nil
}

// Request builds a challenge request of c samples for provider's
// advertisement of the object, seeded at the tip, with nonce.
func (w *Writer) Request(ctx context.Context, b *storerec.Built, provider []byte, c uint64, nonce [16]byte) (*storerec.Request, []byte, error) {
	h, err := w.Asset.BestHeader(ctx)
	if err != nil {
		return nil, nil, err
	}
	seed, err := chainhash.NewHashFromHex(h.Hash)
	if err != nil {
		return nil, nil, err
	}
	q := &storerec.Request{Root: b.RootHash, ProofRoot: b.ProofRoot, Size: b.Size, Provider: provider, Answerer: provider,
		SeedHeight: uint64(h.Height), SeedHash: [32]byte(*seed), Nonce: nonce, C: c}
	raw, err := q.Encode()
	return q, raw, err
}

// Challenge posts a request to front's challenge route.
func (w *Writer) Challenge(ctx context.Context, front string, raw []byte, entry string, maxPay uint64) (*Call, error) {
	body, _ := json.Marshal(map[string]string{"request": base64.StdEncoding.EncodeToString(raw), "entry": entry})
	return w.Post(ctx, front+"/challenge", body, maxPay)
}

// Fetcher fetches by-reference samples from a tested URL (spec section 7.5).
type Fetcher struct {
	HTTP  *http.Client
	URL   string // the advertisement's url
	Blobs []chirp.Child
	// Ranged counts the Range fetches, Whole the whole-blob fetches.
	Ranged, Whole int
}

func (f *Fetcher) blobURL(blob uint64) string {
	i := strings.LastIndex(f.URL, "/")
	return f.URL[:i+1] + chirp.Identifier(f.Blobs[blob].Hash)
}

// Range fetches length bytes at offset of a blob with an HTTP Range.
func (f *Fetcher) Range(blob, offset, length uint64) ([]byte, error) {
	f.Ranged++
	req, _ := http.NewRequest(http.MethodGet, f.blobURL(blob), nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	res, err := f.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusPartialContent && res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("range: %d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, storerec.BlobSize))
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusOK && uint64(len(b)) >= offset+length {
		b = b[offset : offset+length]
	}
	return b, nil
}

// Blob fetches a whole blob.
func (f *Fetcher) Blob(blob uint64) ([]byte, error) {
	f.Whole++
	res, err := f.HTTP.Get(f.blobURL(blob))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("blob: %d", res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, storerec.BlobSize+1))
}

// Verify is the buyer's check of a signed response (spec section 7.4).
func Verify(q *storerec.Request, raw, signed []byte, o *Object, f *Fetcher) storerec.Verdict {
	closure, _ := storerec.VerifyCHIRP(o.Built.RootHash, func(h [32]byte) ([]byte, error) {
		for _, x := range o.Built.Objects() {
			if sha256.Sum256(x) == h {
				return x, nil
			}
		}
		return nil, errors.New("no such object")
	})
	if f != nil && closure != nil {
		f.Blobs = closure.Blobs
	}
	return storerec.VerifyResponse(q, raw, signed, func(b uint64) [32]byte {
		if closure == nil || b >= uint64(len(closure.Blobs)) {
			return [32]byte{}
		}
		return closure.Blobs[b].Hash
	}, f)
}

// Payee is one rank to pay.
type Payee struct {
	Identity []byte
	Front    string
	Rank     int
	Sats     uint64
}

// Settlement is a minted settlement and each payee's remittance body.
type Settlement struct {
	Tx          *transaction.Transaction
	Remittances map[int][]byte
}

// Settle spends the hold output to the payees' BRC-29 keys, mines it, and
// returns each rank's remittance (spec sections 9.5 and 10.1).
func (w *Writer) Settle(ctx context.Context, put *Put, payees []Payee) (*Settlement, error) {
	c, _ := chainhash.NewHashFromHex(put.C)
	prefix := storerec.DerivationPrefix([32]byte(*c))
	pay, err := w.payer(ctx)
	if err != nil {
		return nil, err
	}
	fee, err := pay.TakeAtLeast(ctx, w.Fees.Floor*4)
	if err != nil {
		return nil, err
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(put.Hold.Tx, put.Hold.Vout, storerec.HoldDerivation.Unlocker(ctx, w.W, w.W.Originator))
	tx.AddInputFromTx(fee.Tx, fee.Vout, fee.Unlocker)
	for _, p := range payees {
		dest, err := w.Signer.PaymentDestination(ctx, hex.EncodeToString(p.Identity), prefix, storerec.DerivationSuffix(p.Rank))
		if err != nil {
			pay.GiveBack()
			return nil, err
		}
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: p.Sats, LockingScript: dest})
	}
	change, _ := w.Signer.FundScript()
	feeIn := fee.Tx.Outputs[fee.Vout].Satoshis
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: feeIn - w.Fees.Floor*2, LockingScript: change})
	if err := tx.Sign(); err != nil {
		pay.GiveBack()
		return nil, err
	}
	if tx, err = w.mine(ctx, tx, pay); err != nil {
		return nil, err
	}
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return nil, err
	}
	s := &Settlement{Tx: tx, Remittances: map[int][]byte{}}
	for i, p := range payees {
		body, _ := json.Marshal(map[string]any{"entry": put.C, "rank": p.Rank, "transaction": base64.StdEncoding.EncodeToString(beef),
			"outputIndex": i, "derivationPrefix": prefix, "derivationSuffix": storerec.DerivationSuffix(p.Rank),
			"senderIdentityKey": w.Signer.IdentityHex()})
		s.Remittances[p.Rank] = body
	}
	return s, nil
}

// PubKey parses a 66-hex key.
func PubKey(s string) *ec.PublicKey {
	k, err := ec.PublicKeyFromString(s)
	if err != nil {
		return nil
	}
	return k
}
