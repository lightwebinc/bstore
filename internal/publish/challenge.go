package publish

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/storerec"
)

// A challenge as its buyer makes it (spec section 7): a request seeded at
// the buyer's tip with a fresh nonce, posted to the answerer's front over
// BRC-104 (a 402 paid within the budget), the signed response checked
// sample-major (section 7.4) with by-reference samples fetched from the
// tested URL and rechecked from the whole blob, and the outcome kept.

// Challenge is what to challenge.
type Challenge struct {
	Bucket string
	// Root, ProofRoot and Size are the entry's.
	Root      [32]byte
	ProofRoot [32]byte
	Size      uint64
	// Entry is the entry's txid (display order), sent with the request:
	// the settlement challenge names the entry whose bounty it is for.
	Entry string
	// Provider is the advertiser's key; Answerer the key that signs (the
	// provider, or a front).
	Provider []byte
	Answerer []byte
	// Front is the answerer's base URL (ending /bstore/v1 when derived).
	Front string
	// URL is the tested URL: the provider's live advertisement's url for
	// the root, latest expiry.
	URL string
	// Blobs are the closure's blob references, by which a fetched blob is
	// checked.
	Blobs []chirp.Child
	C     uint64
	// Nonce, when set, is used instead of a fresh one.
	Nonce *[16]byte
	// Budget is the most paid for the answer; 0 pays nothing.
	Budget uint64
	// Request, when set, is a request made before (its encoding), sent
	// again as it is: a draw is not redrawn because an answer was not
	// given.
	Request []byte
}

// ChallengeResult is a challenge's outcome.
type ChallengeResult struct {
	Request    []byte
	Req        *storerec.Request
	Status     int
	Refusal    string // a 422's label
	Code       string // a refusal's BRC-104 error code, as in ERR_ROUTE_NOT_FOUND
	Signed     []byte
	Verdict    storerec.Verdict
	Outcome    int // storerec.OutcomePass .. OutcomeUnretrievable
	Paid       uint64
	Why        string
	Fetches    int
	WholeBlobs int
	// NotTaken is set when the answerer refused the request after a
	// payment and signed that it did not take it (row C30): the payment's
	// coin is already back in the pool.
	NotTaken *storerec.NotTaken
}

// ErrNoTip is a buyer with no tip to seed a request from.
var ErrNoTip = errors.New("publish: the node did not answer its tip")

// SeedBack is how many blocks below the buyer's tip a request is seeded:
// an answerer whose headers are a block or two behind still holds the
// seed, and it stays within the answerer's six-block window for a while.
const SeedBack = 2

// NewRequest builds a challenge request seeded SeedBack blocks below the
// node's tip.
func (p *Publisher) NewRequest(ctx context.Context, ch *Challenge) (*storerec.Request, []byte, error) {
	h, err := p.cfg.Asset.BestHeader(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrNoTip, err)
	}
	height, hash := h.Height, h.Hash
	if height > SeedBack {
		height -= SeedBack
		if hash, err = p.cfg.Asset.HashAtHeight(ctx, height); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrNoTip, err)
		}
	}
	seed, err := chainhash.NewHashFromHex(hash)
	if err != nil {
		return nil, nil, err
	}
	q := &storerec.Request{Root: ch.Root, ProofRoot: ch.ProofRoot, Size: ch.Size, Provider: ch.Provider, Answerer: ch.Answerer,
		SeedHeight: uint64(height), SeedHash: [32]byte(*seed), C: ch.C}
	if ch.Nonce != nil {
		q.Nonce = *ch.Nonce
	} else if _, err := rand.Read(q.Nonce[:]); err != nil {
		return nil, nil, err
	}
	raw, err := q.Encode()
	if err != nil {
		return nil, nil, err
	}
	return q, raw, nil
}

// Challenge runs one challenge. An error is the buyer's own (no tip, a
// price over the budget, a request it cannot build); everything the
// answerer did or did not do is the result's outcome.
func (p *Publisher) Challenge(ctx context.Context, ch *Challenge, fetch *http.Client) (*ChallengeResult, error) {
	if ch.Size == 0 {
		return nil, errors.New("publish: an empty object has no segments to challenge")
	}
	var q *storerec.Request
	raw := ch.Request
	var err error
	if raw != nil {
		if q, err = storerec.DecodeRequest(raw); err != nil {
			return nil, err
		}
	} else if q, raw, err = p.NewRequest(ctx, ch); err != nil {
		return nil, err
	}
	res := &ChallengeResult{Request: raw, Req: q}
	body, _ := json.Marshal(map[string]string{"request": base64.StdEncoding.EncodeToString(raw), "entry": ch.Entry})
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	call, err := p.Post(cctx, strings.TrimRight(ch.Front, "/")+"/challenge", body, ch.Budget)
	if errors.Is(err, ErrOverBudget) || errors.Is(err, ErrCannotPay) {
		// The buyer's own state: nothing is said about the answerer.
		return nil, err
	}
	if err != nil {
		res.Outcome, res.Why = storerec.OutcomeNoAnswer, "no answer: "+firstLine(err.Error())
		return res, nil
	}
	res.Status, res.Paid = call.Status, call.Paid
	if call.Status != http.StatusOK && call.Payment != nil {
		res.NotTaken = p.notTaken(call, raw, ch.Answerer)
	}
	switch call.Status {
	case http.StatusOK:
	case http.StatusPaymentRequired:
		res.Outcome, res.Why = storerec.OutcomeNoAnswer, "priced: the answer was not paid for"
		return res, nil
	case http.StatusUnprocessableEntity:
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(call.Body, &e)
		res.Refusal = e.Error
		res.Outcome, res.Why = storerec.OutcomeNoAnswer, "refused: "+e.Error
		return res, nil
	default:
		var e struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(call.Body, &e) == nil {
			res.Code = e.Code
		}
		res.Outcome, res.Why = storerec.OutcomeNoAnswer, fmt.Sprintf("no answer: %d %s", call.Status, firstLine(string(call.Body)))
		return res, nil
	}
	res.Signed = call.Body
	f := &segmentFetcher{ctx: cctx, hc: fetch, url: ch.URL, blobs: ch.Blobs}
	res.Verdict = storerec.VerifyResponse(q, raw, call.Body, func(b uint64) [32]byte {
		if b >= uint64(len(ch.Blobs)) {
			return [32]byte{}
		}
		return ch.Blobs[b].Hash
	}, f)
	res.Fetches, res.WholeBlobs = f.ranged, f.whole
	res.Outcome = res.Verdict.Outcome
	if res.Verdict.Err != nil {
		res.Why = res.Verdict.Err.Error()
		if res.Verdict.Sample >= 0 {
			res.Why += fmt.Sprintf(" (sample %d)", res.Verdict.Sample)
		}
	}
	return res, nil
}

// notTaken reads a refusal's payment-not-taken note (row C30) and, when it
// verifies and names this call's payment, its request and the answerer
// asked, releases the payment's coin at once. Anything else leaves the
// payment held, as one whose payee may still broadcast it.
func (p *Publisher) notTaken(call *Call, request, answerer []byte) *storerec.NotTaken {
	var m struct {
		NotTaken string `json:"notTaken"`
	}
	if json.Unmarshal(call.Body, &m) != nil || m.NotTaken == "" {
		return nil
	}
	signed, err := base64.StdEncoding.Strict().DecodeString(m.NotTaken)
	if err != nil {
		return nil
	}
	txid := call.Payment.TxID()
	// The note must come from the party the session authenticated: the
	// payee the payment went to.
	if call.Peer != hex.EncodeToString(answerer) {
		p.cfg.Logf("payment %s: a not-taken note from a session that is not the answerer's; the payment stays held", short(txid.String()))
		return nil
	}
	n, err := storerec.CheckNotTaken(signed, [32]byte(txid[:]), request, answerer)
	if err != nil {
		p.cfg.Logf("payment %s: the refusal's not-taken note is not good (%v); the payment stays held", short(txid.String()), err)
		return nil
	}
	p.ReleasePayment(txid.String())
	return n
}

// OutcomeRecord is the outcome record of a result (spec section 8.1).
func (r *ChallengeResult) OutcomeRecord(bucket string) *storerec.Outcome {
	o := &storerec.Outcome{Bucket: bucket, Request: r.Request, Outcome: uint64(r.Outcome)}
	if r.Outcome == storerec.OutcomePass || r.Outcome == storerec.OutcomeInvalid {
		body, sig, ok := storerec.SplitSigned(r.Signed)
		if !ok {
			return nil
		}
		h := sha256.Sum256(body)
		o.Response, o.Signature = &h, sig
	}
	return o
}

// OutcomeName is an outcome's word.
func OutcomeName(o int) string {
	switch o {
	case storerec.OutcomePass:
		return "pass"
	case storerec.OutcomeInvalid:
		return "invalid"
	case storerec.OutcomeNoAnswer:
		return "no-answer"
	case storerec.OutcomeUnretrievable:
		return "unretrievable"
	}
	return "unknown"
}

// segmentFetcher is the buyer's view of the tested URL (spec section 7.5):
// a Range fetch of one segment within its blob at the blob's object route,
// and the whole blob.
type segmentFetcher struct {
	ctx           context.Context
	hc            *http.Client
	url           string
	blobs         []chirp.Child
	ranged, whole int
}

func (f *segmentFetcher) blobURL(blob uint64) (string, error) {
	if blob >= uint64(len(f.blobs)) {
		return "", errors.New("no such blob")
	}
	i := strings.LastIndex(f.url, "/")
	if i < 0 {
		return "", errors.New("no tested URL")
	}
	return f.url[:i+1] + chirp.Identifier(f.blobs[blob].Hash), nil
}

func (f *segmentFetcher) client() *http.Client {
	if f.hc != nil {
		return f.hc
	}
	return http.DefaultClient
}

func (f *segmentFetcher) Range(blob, offset, length uint64) ([]byte, error) {
	f.ranged++
	u, err := f.blobURL(blob)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	res, err := f.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusPartialContent:
		return io.ReadAll(io.LimitReader(res.Body, int64(length)+1))
	case http.StatusOK:
		// The host ignored Range: the segment is taken from the body.
		b, err := io.ReadAll(io.LimitReader(res.Body, storerec.BlobSize+1))
		if err != nil {
			return nil, err
		}
		if uint64(len(b)) < offset+length {
			return nil, errors.New("short body")
		}
		return b[offset : offset+length], nil
	}
	return nil, fmt.Errorf("range: %d", res.StatusCode)
}

func (f *segmentFetcher) Blob(blob uint64) ([]byte, error) {
	f.whole++
	u, err := f.blobURL(blob)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(f.ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	res, err := f.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("blob: %d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, storerec.BlobSize+1))
	if err != nil {
		return nil, err
	}
	if len(b) > storerec.BlobSize {
		return nil, errors.New("more than a blob")
	}
	return b, nil
}
