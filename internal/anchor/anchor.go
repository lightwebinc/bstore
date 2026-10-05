package anchor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/internal/storerec"
)

// Errors a caller maps to exit codes.
var (
	// ErrBroken is a chain whose fold is not consistent: only the owner
	// could have made it so, and the way on is a new chain (exit 1).
	ErrBroken = errors.New("anchor: the chain's fold is not consistent")
	// ErrIncomplete is a run that did what it could and left something:
	// a host whose answer did not verify, an entry whose funding outpoint
	// the spend source could not vouch for, a token not yet mined. It
	// proves nothing either way (exit 3).
	ErrIncomplete = errors.New("anchor: incomplete")
)

// Chain is a bucket chain this identity owns, as its tooling records it:
// enough to mint the next token on, never the set of what the chain named.
type Chain struct {
	Bucket  string   `json:"bucket"`
	ID      string   `json:"id,omitempty"` // hex, once the first token is signed
	Writers []string `json:"writers"`      // hex, ascending
	// Tree is the chain tree the first token spends (raw hex and proof).
	Tree     string          `json:"tree,omitempty"`
	TreeBump string          `json:"treeBump,omitempty"`
	TreeFee  *bwallet.Output `json:"treeFee,omitempty"`
	// Pending is a token signed and recorded, not yet seen mined and
	// published.
	Pending *PendingToken `json:"pending,omitempty"`
	// Seq is the last token this tooling minted.
	Seq     uint64 `json:"seq"`
	Created int64  `json:"created"`
}

// PendingToken is a token recorded before it is sent.
type PendingToken struct {
	Seq      uint64         `json:"seq"`
	Txid     string         `json:"txid"`
	Raw      string         `json:"raw"`
	Parent   string         `json:"parent"` // raw hex of the transaction it spends
	ParentMP string         `json:"parentBump"`
	Vout     uint32         `json:"vout"` // the parent output it spends
	Fee      bwallet.Output `json:"fee"`
	Changes  int            `json:"changes"`
	Consumed int            `json:"consumed"`
}

// Store is the owner's chains, one file in the identity's directory.
type Store struct {
	path string
}

// OpenStore opens dir's chains file.
func OpenStore(dir string) *Store { return &Store{path: filepath.Join(dir, "chains.json")} }

// Load reads every chain.
func (s *Store) Load() ([]*Chain, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []*Chain
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("chains.json: %w", err)
	}
	return out, nil
}

// Save writes one chain, in place of the record of the same bucket and id.
func (s *Store) Save(c *Chain) error {
	all, err := s.Load()
	if err != nil {
		return err
	}
	found := false
	for i, x := range all {
		if x.Bucket == c.Bucket && (x.ID == c.ID || x.ID == "") {
			all[i], found = c, true
			break
		}
	}
	if !found {
		all = append(all, c)
	}
	raw, err := json.MarshalIndent(all, "", " ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// Find is the chain of a bucket identifier this identity owns; with id
// empty, the only one, else an error naming the choice.
func (s *Store) Find(bucket, id string) (*Chain, error) {
	all, err := s.Load()
	if err != nil {
		return nil, err
	}
	var hits []*Chain
	for _, c := range all {
		if c.Bucket == bucket && (id == "" || c.ID == id) {
			hits = append(hits, c)
		}
	}
	switch len(hits) {
	case 0:
		return nil, fmt.Errorf("anchor: this identity owns no chain of bucket %s", bucket)
	case 1:
		return hits[0], nil
	}
	return nil, fmt.Errorf("anchor: this identity owns %d chains of bucket %s; name one by its id", len(hits), bucket)
}

// Tool is the owner's tooling over an identity's publisher (its wallet,
// node, settlement leg and delivery) and a reader of the hosts.
type Tool struct {
	Pub    *publish.Publisher
	Reader *reader.Reader
	Store  *Store
	Logf   func(format string, args ...any)

	// minted is the last token finishPending saw mined, with its proof.
	minted *transaction.Transaction
}

func (t *Tool) logf(format string, args ...any) {
	if t.Logf != nil {
		t.Logf(format, args...)
	}
}

// NewIdentifier is a bucket identifier for name with a fresh random
// suffix of 10 lowercase letters (spec section 3.1).
func NewIdentifier(name string) (string, error) {
	var b [10]byte
	suffix := make([]byte, 10)
	for i := range suffix {
		for {
			if _, err := rand.Read(b[i : i+1]); err != nil {
				return "", err
			}
			if b[i] < 26*9 { // uniform over a to z
				suffix[i] = 'a' + b[i]%26
				break
			}
		}
	}
	id := name + "_" + string(suffix)
	if err := storerec.CheckBucket(id); err != nil {
		return "", err
	}
	return id, nil
}

// parseWriters checks and orders writer keys: each a valid identity key,
// not the owner, not the anyone key, at most 16, each once.
func parseWriters(owner []byte, ws []string) ([][]byte, []string, error) {
	seen := map[string]bool{}
	var keys [][]byte
	for _, w := range ws {
		w = strings.ToLower(strings.TrimSpace(w))
		if w == "" || seen[w] {
			continue
		}
		b, err := hex.DecodeString(w)
		if err != nil {
			return nil, nil, fmt.Errorf("writer %q is not 66 hex digits", w)
		}
		if _, err := storerec.CheckKey(b); err != nil {
			return nil, nil, fmt.Errorf("writer %s: %v", w, err)
		}
		if bytes.Equal(b, owner) {
			return nil, nil, fmt.Errorf("writer %s is the owner, who writes without being named", w)
		}
		seen[w] = true
		keys = append(keys, b)
	}
	if len(keys) > storerec.MaxWriters {
		return nil, nil, fmt.Errorf("at most %d writers", storerec.MaxWriters)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	hexes := make([]string, len(keys))
	for i, k := range keys {
		hexes[i] = hex.EncodeToString(k)
	}
	return keys, hexes, nil
}

// New creates a bucket chain: a chain tree of one output under
// derive(owner, "chain"), mined, then the first token spending it, each
// recorded before it is sent. A run that stopped part way is finished by
// calling New again with the same identifier.
func (t *Tool) New(ctx context.Context, bucket string, writers []string) (*Chain, error) {
	if err := storerec.CheckBucket(bucket); err != nil {
		return nil, err
	}
	owner := t.Pub.Identity()
	_, ws, err := parseWriters(owner, writers)
	if err != nil {
		return nil, err
	}
	all, err := t.Store.Load()
	if err != nil {
		return nil, err
	}
	var c *Chain
	for _, x := range all {
		if x.Bucket == bucket && x.Seq == 0 && (x.ID == "" || x.Pending != nil) {
			c = x
		}
	}
	if c == nil {
		for _, x := range all {
			if x.Bucket == bucket {
				return nil, fmt.Errorf("anchor: this identity already owns a chain of %s", bucket)
			}
		}
		c = &Chain{Bucket: bucket, Writers: ws, Created: t.Pub.Now().Unix()}
	}
	if c.Pending != nil {
		return c, t.finishPending(ctx, c)
	}
	if c.Tree == "" {
		if err := t.mintChainTree(ctx, c); err != nil {
			return nil, err
		}
	}
	tree, err := funding.Rebuild(c.Tree, c.TreeBump, "")
	if err != nil {
		return nil, err
	}
	if c.TreeBump == "" {
		mp, height, err := t.Pub.NewPayer(ctx).Await(ctx, "chain tree", tree)
		if err != nil {
			return nil, fmt.Errorf("%w: the chain tree %s: %v", ErrIncomplete, tree.TxID(), err)
		}
		tree.MerklePath = mp
		t.Pub.NewPayer(ctx).Change(tree, height, mp)
		c.TreeBump = funding.BumpHex(mp)
		if err := t.Store.Save(c); err != nil {
			return nil, err
		}
	}
	id := storerec.BucketID([32]byte(*tree.TxID()), 0)
	writerKeys, _, _ := parseWriters(owner, c.Writers)
	b := &storerec.Bucket{Bucket: bucket, Owner: owner, ID: id, Seq: 0, Writers: writerKeys, Changes: []storerec.Change{},
		Count: 0, MapRoot: storerec.EmptyHash, Created: uint64(t.Pub.Now().UnixMilli()), Consumed: [][32]byte{}}
	if writerKeys == nil {
		b.Writers = [][]byte{}
	}
	c.ID = hex.EncodeToString(id[:])
	if err := t.mint(ctx, c, b, tree, 0, storerec.ChainDerivation); err != nil {
		return c, err
	}
	return c, nil
}

func (t *Tool) mintChainTree(ctx context.Context, c *Chain) error {
	w := t.Pub.Wallet()
	lock, err := carrier.FundingLock(ctx, w, w.Originator, carrier.Params{Derivation: storerec.ChainDerivation, FundingTag: storerec.TagFunding,
		ValidatePayload: func([]byte) error { return nil }, ErrIdentity: storerec.ErrIdentity})
	if err != nil {
		return err
	}
	fees := t.Pub.Fees()
	pay := t.Pub.NewPayer(ctx)
	fee, err := pay.TakeAtLeast(ctx, 1+fees.Floor*4)
	if err != nil {
		return err
	}
	change, err := w.Signer().FundScript()
	if err != nil {
		pay.GiveBack()
		return err
	}
	tx, err := mint.FundingTree(lock, 1, 1, fee, change, fees)
	if err != nil {
		pay.GiveBack()
		return err
	}
	coin := publish.CoinOf(fee)
	c.Tree, c.TreeFee = hex.EncodeToString(tx.Bytes()), &coin
	if err := t.Store.Save(c); err != nil {
		pay.GiveBack()
		c.Tree, c.TreeFee = "", nil
		return err
	}
	mp, height, err := pay.SettleAndWait(ctx, "chain tree", tx)
	if err != nil {
		return fmt.Errorf("%w: the chain tree %s: %v", ErrIncomplete, tx.TxID(), err)
	}
	pay.Change(tx, height, mp)
	c.TreeBump = funding.BumpHex(mp)
	return t.Store.Save(c)
}

// mint signs a token of record b spending output vout of parent under d,
// records it, sends it, waits for it to mine and publishes it with its
// parent to the facade and every host.
func (t *Tool) mint(ctx context.Context, c *Chain, b *storerec.Bucket, parent *transaction.Transaction, vout uint32, d pushdrop.Derivation) error {
	rec, err := b.Encode()
	if err != nil {
		return err
	}
	w := t.Pub.Wallet()
	lock, err := storerec.BucketDerivation.Lock(ctx, w, w.Originator, [][]byte{storerec.TagToken, rec}, true)
	if err != nil {
		return err
	}
	fees := t.Pub.Fees()
	pay := t.Pub.NewPayer(ctx)
	fee, err := pay.TakeAtLeast(ctx, 1+fees.Floor*4)
	if err != nil {
		return err
	}
	change, err := w.Signer().FundScript()
	if err != nil {
		pay.GiveBack()
		return err
	}
	tx, err := mint.Transition(lock, 1, &mint.Input{Tx: parent, Vout: vout, Unlocker: d.Unlocker(ctx, w, w.Originator)}, fee, change, fees)
	if err != nil {
		pay.GiveBack()
		return err
	}
	c.Pending = &PendingToken{Seq: b.Seq, Txid: tx.TxID().String(), Raw: hex.EncodeToString(tx.Bytes()), Parent: hex.EncodeToString(parent.Bytes()),
		ParentMP: funding.BumpHex(parent.MerklePath), Vout: vout, Fee: publish.CoinOf(fee), Changes: len(b.Changes), Consumed: len(b.Consumed)}
	if err := t.Store.Save(c); err != nil {
		pay.GiveBack()
		c.Pending = nil
		return err
	}
	t.logf("token %d of %s: %s signed and recorded (%d change(s), %d consumed)", b.Seq, c.Bucket, short(c.Pending.Txid), len(b.Changes), len(b.Consumed))
	return t.finishPending(ctx, c)
}

// finishPending sees a recorded token through: the same bytes sent again
// until it mines, then published with its parent. A token whose parent
// output another transaction spent lost a race (spec section 3.6): its
// record is dropped and its fee coin returned.
func (t *Tool) finishPending(ctx context.Context, c *Chain) error {
	pt := c.Pending
	raw, err := hex.DecodeString(pt.Raw)
	if err != nil {
		return err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return err
	}
	parent, err := funding.Rebuild(pt.Parent, pt.ParentMP, "")
	if err != nil {
		return err
	}
	asset := t.Pub.Asset()
	mp, height, perr := asset.Proof(ctx, pt.Txid)
	if perr != nil {
		cf, err := t.Pub.SpentElsewhere(ctx, tx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrIncomplete, err)
		}
		parentID := parent.TxID().String()
		spendsParent := cf != nil && tx.Inputs[cf.Input].SourceTXID.String() == parentID && tx.Inputs[cf.Input].SourceTxOutIndex == pt.Vout
		switch {
		case cf != nil && cf.Same:
			// A miner re-encoded the token (spec section 13.3): the same
			// record, the successor builds on the mined txid.
			t.logf("token %d of %s: %s was mined re-encoded as %s", pt.Seq, c.Bucket, short(pt.Txid), short(cf.By.TxID().String()))
			tx = cf.By
			pt.Txid, pt.Raw = tx.TxID().String(), hex.EncodeToString(tx.Bytes())
			if err := t.Store.Save(c); err != nil {
				return err
			}
		case spendsParent:
			t.logf("token %d of %s: %s lost: the output it spends is spent by %s; the chain is rebuilt on the winner", pt.Seq, c.Bucket, short(pt.Txid), short(cf.By.TxID().String()))
			t.Pub.ReturnCoin(ctx, pt.Fee)
			c.Pending = nil
			if err := t.Store.Save(c); err != nil {
				return err
			}
			return fmt.Errorf("%w: another token spent the chain's latest token first; run anchor again", ErrIncomplete)
		case cf != nil:
			// Its fee coin went elsewhere: it can never mine, and the
			// chain's latest token is still unspent. The next run builds
			// the token again.
			t.logf("token %d of %s: %s can never mine (its fee input was spent by %s); it is built again", pt.Seq, c.Bucket, short(pt.Txid), short(cf.By.TxID().String()))
			c.Pending = nil
			if err := t.Store.Save(c); err != nil {
				return err
			}
			return fmt.Errorf("%w: a token could never mine; run anchor again", ErrIncomplete)
		default:
			if err := t.Pub.Settler().Submit(ctx, tx); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
				t.logf("token %s: sent: %v", short(pt.Txid), err)
			}
		}
		pay := t.Pub.NewPayer(ctx)
		if mp, height, err = pay.Await(ctx, "bucket token", tx); err != nil {
			return fmt.Errorf("%w: token %s: %v", ErrIncomplete, short(pt.Txid), err)
		}
	}
	tx.MerklePath = mp
	t.Pub.NewPayer(ctx).Change(tx, height, mp)
	t.minted = tx
	beef, err := storerec.TokenBEEF(tx, parent)
	if err != nil {
		return err
	}
	targets := t.Pub.RecordTargets(c.Bucket)
	ok := t.Pub.Resend(ctx, pt.Txid, "bucket token", beef, targets)
	c.Seq = pt.Seq
	c.Pending = nil
	if err := t.Store.Save(c); err != nil {
		return err
	}
	t.logf("token %d of %s: %s mined at height %d and taken by %d of %d target(s)", pt.Seq, c.Bucket, short(pt.Txid), height, len(ok), len(targets))
	if len(ok) < len(targets) {
		return fmt.Errorf("%w: token %d is mined and %d target(s) have not taken it yet; it is sent again (bstore sync)", ErrIncomplete, pt.Seq, len(targets)-len(ok))
	}
	return nil
}

// walkBudget bounds the walk of every writer's unanchored entries at every
// host in one run: a host that answers slowly delays the run, not stalls
// it.
const walkBudget = 10 * time.Minute

// Options are what an anchoring run may change besides the map.
type Options struct {
	AddWriters    []string
	RemoveWriters []string
	// DryRun plans and reports, and mints nothing.
	DryRun bool
}

// Report is what an anchoring run did.
type Report struct {
	FromSeq   uint64
	Tokens    []string // txids minted
	Applied   int      // changes written
	Consumed  int
	Entries   int // entries considered (each once)
	Named     int // entries skipped because a token already named them
	Swept     []string
	Unvouched []string // entries whose funding outpoint the spend source could not vouch for
	Strangers int      // entries an answer returned for another writer, bucket or id
	Hosts     []string // hosts named, and why
	Deferred  []string
	Plan      *Plan
}

// Candidate is an entry found unanchored and verified.
type candidate struct {
	e       *reader.Entry
	funding transaction.Outpoint
}

// Anchor runs the owner's tooling once over a chain: it rebuilds the
// chain from the hosts, walks every writer's unanchored entries, checks
// each, folds the batch and mints the tokens that carry it.
func (t *Tool) Anchor(ctx context.Context, c *Chain, o Options) (*Report, error) {
	if c.Pending != nil {
		if err := t.finishPending(ctx, c); err != nil && !errors.Is(err, ErrIncomplete) {
			return nil, err
		} else if c.Pending != nil {
			return nil, err
		}
	}
	owner := t.Pub.Identity()
	idb, err := hex.DecodeString(c.ID)
	if err != nil || len(idb) != 32 {
		return nil, errors.New("anchor: the chain has no bucket id yet; run bucket new again to finish it")
	}
	ch := reader.Chain{Bucket: c.Bucket, Owner: owner, ID: [32]byte(idb)}
	toks, err := t.Reader.Tokens(ctx, ch)
	if err != nil {
		return nil, fmt.Errorf("%w: the chain: %v", ErrIncomplete, err)
	}
	recs := make([]*storerec.Bucket, len(toks))
	for i, tk := range toks {
		recs[i] = tk.Bucket
	}
	fold, err := storerec.FoldChain(recs)
	if err != nil {
		return nil, err
	}
	for i, st := range fold.Statuses {
		if st != storerec.StatusConsistent {
			return nil, fmt.Errorf("%w: token %d is %s (%s); the way on is a new chain", ErrBroken, i, st, fold.Reasons[i])
		}
	}
	latest := toks[len(toks)-1]
	rep := &Report{FromSeq: latest.Bucket.Seq}
	// The writers of the token to mint.
	cur := make([]string, len(latest.Bucket.Writers))
	for i, w := range latest.Bucket.Writers {
		cur[i] = hex.EncodeToString(w)
	}
	next := slices.Clone(cur)
	next = append(next, o.AddWriters...)
	for _, r := range o.RemoveWriters {
		r = strings.ToLower(r)
		next = slices.DeleteFunc(next, func(w string) bool { return w == r })
	}
	writerKeys, writerHex, err := parseWriters(owner, next)
	if err != nil {
		return nil, err
	}
	// A writer leaves only when the map names none of its entries (spec
	// section 3.5).
	for _, r := range o.RemoveWriters {
		if !slices.Contains(cur, strings.ToLower(r)) {
			continue
		}
		n, err := t.entriesOf(ctx, ch, strings.ToLower(r))
		if err != nil {
			return nil, fmt.Errorf("%w: whether the map names an entry of writer %s: %v", ErrIncomplete, short(r), err)
		}
		if n > 0 {
			return nil, fmt.Errorf("anchor: the map names %d entr(ies) of writer %s: replace or remove those keys first", n, short(r))
		}
	}
	// Every writer's unanchored entries, at every host.
	found := map[[32]byte]*candidate{}
	ask := append([]string{hex.EncodeToString(owner)}, writerHex...)
	wctx, cancel := context.WithTimeout(ctx, walkBudget)
	for _, w := range ask {
		t.walkUnanchored(wctx, ch, w, found, rep)
	}
	cancel()
	var batch []storerec.Applicable
	keysOf := make([][32]byte, 0, len(found))
	for k := range found {
		keysOf = append(keysOf, k)
	}
	sort.Slice(keysOf, func(i, j int) bool { return bytes.Compare(keysOf[i][:], keysOf[j][:]) < 0 })
	for _, k := range keysOf {
		cd := found[k]
		rep.Entries++
		if fold.Named[cd.e.C] {
			rep.Named++
			continue
		}
		free, by, err := t.Pub.Unspent(ctx, cd.funding.Txid.String(), cd.funding.Index)
		switch {
		case err != nil:
			rep.Unvouched = append(rep.Unvouched, cd.e.Txid())
			continue
		case !free:
			rep.Swept = append(rep.Swept, cd.e.Txid()+" (funding spent by "+short(by)+")")
			continue
		}
		e := cd.e.Entry
		batch = append(batch, storerec.Applicable{C: cd.e.C, Key: e.Key, Kind: e.Kind, Created: e.Created})
	}
	base := SizingBase(c.Bucket, owner, ch.ID, latest.Txid, writerKeys)
	if base.Writers == nil {
		base.Writers = [][]byte{}
	}
	plan, err := PlanBatch(fold.Map, fold.Named, batch, RecordSizer(base))
	if err != nil {
		return nil, err
	}
	rep.Plan, rep.Deferred = plan, plan.Deferred
	writersChange := !slices.Equal(cur, writerHex)
	if len(plan.Steps) == 0 && writersChange {
		plan.Steps = []Step{{Map: fold.Map.Clone()}}
	}
	for _, s := range plan.Steps {
		rep.Applied += len(s.Changes)
		rep.Consumed += len(s.Consumed)
	}
	if o.DryRun || len(plan.Steps) == 0 {
		return rep, t.incomplete(rep)
	}
	prev := latest.Tx()
	prevVout := latest.Vout
	seq := latest.Bucket.Seq
	prevTxid := latest.Txid
	for _, s := range plan.Steps {
		seq++
		b := &storerec.Bucket{Bucket: c.Bucket, Owner: owner, ID: ch.ID, Seq: seq, Prev: prevTxid, Writers: base.Writers,
			Changes: s.Changes, Count: uint64(len(s.Map)), MapRoot: s.Map.Root(), Created: uint64(t.Pub.Now().UnixMilli()), Consumed: s.Consumed}
		if b.Changes == nil {
			b.Changes = []storerec.Change{}
		}
		if b.Consumed == nil {
			b.Consumed = [][32]byte{}
		}
		t.minted = nil
		err := t.mint(ctx, c, b, prev, prevVout, storerec.BucketDerivation)
		if err != nil && !(errors.Is(err, ErrIncomplete) && c.Pending == nil && t.minted != nil) {
			return rep, err
		}
		if err != nil {
			// Mined, and not yet taken by every target: the next token
			// builds on it, and the outbox sends it again.
			rep.Hosts = append(rep.Hosts, err.Error())
		}
		tk := t.minted
		rep.Tokens = append(rep.Tokens, tk.TxID().String())
		prev, prevVout, prevTxid = tk, tokenVout(tk), [32]byte(*tk.TxID())
	}
	return rep, t.incomplete(rep)
}

func tokenVout(tx *transaction.Transaction) uint32 {
	tk, err := storerec.ReadToken(tx)
	if err != nil {
		return 0
	}
	return tk.Vout
}

func (t *Tool) incomplete(rep *Report) error {
	if len(rep.Hosts) > 0 || len(rep.Unvouched) > 0 {
		return ErrIncomplete
	}
	return nil
}

// walkUnanchored asks every host for writer w's entries no token names,
// every page from the start, and keeps each that verifies and is w's for
// this chain. A host whose answer does not verify, is out of order or
// returns another's entry is named.
func (t *Tool) walkUnanchored(ctx context.Context, ch reader.Chain, w string, found map[[32]byte]*candidate, rep *Report) {
	wb, _ := hex.DecodeString(w)
	for _, h := range t.Reader.Hosts {
		after := ""
		for page := 0; page < 1<<14; page++ {
			q := map[string]string{"bucket": ch.Bucket, "owner": hex.EncodeToString(ch.Owner), "id": hex.EncodeToString(ch.ID[:]), "writer": w, "after": after}
			a, err := t.Reader.Ask(ctx, h, q)
			if err != nil {
				rep.Hosts = append(rep.Hosts, fmt.Sprintf("host %s: unanchored for %s: %v", h, short(w), err))
				break
			}
			bad := ""
			last := after
			if len(a.Outputs) > 64 {
				// The class answers at most 64: more is not an answer.
				rep.Hosts = append(rep.Hosts, fmt.Sprintf("host %s: unanchored for %s: %d outputs to one page", h, short(w), len(a.Outputs)))
				break
			}
			for _, o := range a.Outputs {
				tx, err := publish.BEEFSubject(o.Beef, 1<<20)
				if err != nil {
					bad = "an output that is not a transaction"
					break
				}
				txid := tx.TxID().String()
				if txid <= last {
					bad = "outputs not ascending after the cursor"
					break
				}
				last = txid
				e, err := t.Reader.VerifyEntry(ctx, ch.Bucket, o.Beef)
				if err != nil {
					bad = "an entry that does not verify: " + err.Error()
					continue
				}
				if !bytes.Equal(e.Entry.Writer, wb) || e.Entry.Bucket != ch.Bucket || e.Entry.ID != ch.ID {
					rep.Strangers++
					bad = "an entry of another writer, bucket or chain"
					continue
				}
				if found[e.C] == nil {
					in := tx.Inputs[0]
					found[e.C] = &candidate{e: e, funding: transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}}
				}
			}
			if bad != "" {
				// What verified on the page is kept; the host is not asked
				// for more this run.
				rep.Hosts = append(rep.Hosts, fmt.Sprintf("host %s: unanchored for %s: %s", h, short(w), bad))
				break
			}
			if len(a.Outputs) < 64 {
				break
			}
			after = last
		}
	}
}

// entriesOf counts the entries of writer w the latest map names, by
// listing the whole map, every page proven complete.
func (t *Tool) entriesOf(ctx context.Context, ch reader.Chain, w string) (int, error) {
	wb, _ := hex.DecodeString(w)
	n := 0
	after := ""
	for {
		pg, err := t.Reader.List(ctx, ch, "", after)
		if err != nil {
			return 0, err
		}
		for _, e := range pg.Entries {
			if bytes.Equal(e.Entry.Writer, wb) {
				n++
			}
		}
		if pg.Done || pg.Next == "" {
			return n, nil
		}
		after = pg.Next
	}
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// SizingBase is the record a batch's tokens are measured against: every
// token at the widest seq, count and time it could carry, so a later token
// of a split, whose seq is wider, still fits.
func SizingBase(bucket string, owner []byte, id, prev [32]byte, writers [][]byte) storerec.Bucket {
	return storerec.Bucket{Bucket: bucket, Owner: owner, ID: id, Seq: storerec.MaxSafe, Prev: prev,
		Writers: writers, Count: storerec.MaxKeysInMap, Created: 253402300799999}
}
