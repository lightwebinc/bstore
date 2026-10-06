// Package reader is a bstore reader (spec section 15): it asks the hosts it
// is given the free lookup questions of spec section 11.1, verifies every
// token, entry and advertisement against its own headers, proves what a key
// names, that a key is absent and that a listing page is complete against
// the chain's latest token, and reads an object's closure from its
// advertisers, every byte held to its hash before it is released. A host
// that answers something that does not verify is named in the log and the
// next host is asked; a provider that serves wrong bytes is named and the
// next advertiser is asked.
package reader

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/lookup"

	"github.com/lightwebinc/bstore/storerec"
)

// Errors a caller maps to its answers.
var (
	// ErrUnavailable is no host answering anything that verifies: an empty
	// answer proves nothing (spec section 16.3).
	ErrUnavailable = errors.New("reader: no host answered anything that verifies")
	// ErrUnheld is a key the map names whose entry no host produces.
	ErrUnheld = errors.New("reader: the map names the key's entry and no host produces it")
	// ErrRefused is an answer that verifies and fails a check that names
	// the owner or the writer (spec section 15.4, exit 1).
	ErrRefused = errors.New("reader: refused")
	// ErrNoSource is a blob no advertiser served as its hash names it.
	ErrNoSource = errors.New("reader: no advertiser served the object")
)

// Chain is a bucket chain a reader reads: the identifier, the owner and the
// bucket id (spec section 3.3).
type Chain struct {
	Bucket string
	Owner  []byte
	ID     [32]byte
}

func (c Chain) key() string {
	return c.Bucket + ":" + hex.EncodeToString(c.Owner) + ":" + hex.EncodeToString(c.ID[:])
}

func (c Chain) query(extra ...string) map[string]string {
	q := map[string]string{"bucket": c.Bucket, "owner": hex.EncodeToString(c.Owner), "id": hex.EncodeToString(c.ID[:])}
	for i := 0; i+1 < len(extra); i += 2 {
		q[extra[i]] = extra[i+1]
	}
	return q
}

// HeldStore keeps, per chain, the highest token a reader verified, across
// restarts (spec section 15.1).
type HeldStore interface {
	Held(chain string) *storerec.Held
	SetHeld(chain string, h storerec.Held) error
}

// Reader reads buckets.
type Reader struct {
	// Hosts are the overlay hosts asked, in order.
	Hosts []string
	// HTTP asks hosts; Fetch reaches advertisers' URLs.
	HTTP  *http.Client
	Fetch *http.Client
	// Headers are the reader's own block headers.
	Headers chaintracker.ChainTracker
	Held    HeldStore
	Logf    func(format string, args ...any)
	Now     func() time.Time
	// MaxAnswer bounds one host answer's body.
	MaxAnswer int64
	// Spender, when set, is a spend source: the transaction that spent an
	// output, or "" while it is unspent (bcommon nodeapi). The latest
	// token's output is checked unspent with it (spec section 15.1); an
	// answer it cannot give is no evidence either way.
	Spender func(ctx context.Context, txid string, vout uint32) (string, error)
	// OnToken, when set, is handed every token a walk verifies past the one
	// held, oldest first: what it names is the map's from then on.
	OnToken func(b *storerec.Bucket)
	// WalkDir, when set, keeps a walk's progress across a restart
	// (walkstore.go).
	WalkDir string

	walkMu  sync.Map // chain key -> *sync.Mutex
	walks   sync.Map // chain key and target txid -> *walkJob
	tokens  tokenCache
	unspent sync.Map // txid -> time checked unspent
	ads     adCache
	bad     badSources
	wf      walkFiles
}

func (r *Reader) logf(format string, args ...any) {
	if r.Logf != nil {
		r.Logf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

func (r *Reader) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// hostError is an answer from a host that does not verify: the read is
// incomplete, not refused, and the host is named (spec section 14).
type hostError struct {
	host string
	err  error
}

func (e *hostError) Error() string { return fmt.Sprintf("host %s: %v", e.host, e.err) }
func (e *hostError) Unwrap() error { return e.err }

// ask posts one question to one host's /lookup.
func (r *Reader) ask(ctx context.Context, host string, q map[string]string) (*lookup.Answer, error) {
	body, err := json.Marshal(lookup.Question{Service: storerec.LookupService, Query: q})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(host, "/")+"/lookup", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	hc := r.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	res, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	max := r.MaxAnswer
	if max <= 0 {
		max = 32 << 20
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > max {
		return nil, fmt.Errorf("an answer over %d bytes", max)
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lookup: %d %s", res.StatusCode, firstLine(raw))
	}
	var a lookup.Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, fmt.Errorf("lookup answer: %w", err)
	}
	if a.Type != lookup.TypeOutputList {
		return nil, lookup.ErrNotOutputList
	}
	return &a, nil
}

func firstLine(b []byte) string {
	s := string(b)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

// subject is the transaction an output-list item names: the BEEF's last.
func subject(beef []byte, vout uint32) (*transaction.Transaction, error) {
	tx, err := transaction.NewTransactionFromBEEF(beef)
	if err != nil {
		return nil, err
	}
	if int(vout) >= len(tx.Outputs) || tx.Outputs[vout] == nil || tx.Outputs[vout].LockingScript == nil {
		return nil, errors.New("no such output")
	}
	return tx, nil
}

// Token is a bucket token a reader verified against its own headers.
type Token struct {
	Txid   [32]byte // hash byte order
	Vout   uint32
	Bucket *storerec.Bucket
	tx     *transaction.Transaction
}

// TxidHex is the token's txid in display order.
func (t *Token) TxidHex() string { return displayHex(t.Txid) }

// verifyToken holds an answered output to be a well-formed token of the
// chain whose proof verifies in the reader's headers.
func (r *Reader) verifyToken(ctx context.Context, c Chain, o lookup.Output) (*Token, error) {
	tx, err := subject(o.Beef, o.OutputIndex)
	if err != nil {
		return nil, fmt.Errorf("token: %w", err)
	}
	txid := [32]byte(*tx.TxID())
	if t := r.tokens.get(txid); t != nil {
		if t.Vout != o.OutputIndex {
			return nil, errors.New("token: another output of a token")
		}
		return t, nil
	}
	tk, err := storerec.ReadToken(tx)
	if err != nil {
		return nil, fmt.Errorf("token: %w", err)
	}
	if tk.Vout != o.OutputIndex {
		return nil, errors.New("token: the output answered is not the token's")
	}
	b := tk.Bucket
	if b.Bucket != c.Bucket || !bytes.Equal(b.Owner, c.Owner) || b.ID != c.ID {
		return nil, errors.New("token: another chain's")
	}
	if !chaintoken.Mined(ctx, tx, r.Headers) {
		return nil, errors.New("token: its proof does not verify in the reader's headers")
	}
	t := &Token{Txid: txid, Vout: tk.Vout, Bucket: b, tx: tx}
	r.tokens.put(t)
	return t, nil
}

// spends is the outpoint a first token spends from its chain tree: the
// input whose outpoint is the bucket id's (spec section 3.3). The token's
// parent is not in what a host answers, so the reader finds the input by
// the id itself.
func spends(t *Token) *storerec.Outpoint {
	for _, in := range t.tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		op := storerec.Outpoint{Txid: [32]byte(*in.SourceTXID), Vout: in.SourceTxOutIndex}
		if storerec.BucketID(op.Txid, op.Vout) == t.Bucket.ID {
			return &op
		}
	}
	return nil
}

func (r *Reader) chainLock(c Chain) *sync.Mutex {
	m, _ := r.walkMu.LoadOrStore(c.key(), &sync.Mutex{})
	return m.(*sync.Mutex)
}

// latest is step 1 on a token a host answered as the chain's latest: a
// token below the seq the reader holds is a rollback; one above it is
// walked back by prev, through bucket-seq at the same host, to the token
// held or to seq 0 (spec section 15.1), and becomes the token held.
func (r *Reader) latest(ctx context.Context, host string, c Chain, t *Token) error {
	var held *storerec.Held
	if r.Held != nil {
		held = r.Held.Held(c.key())
	}
	if held != nil && t.Bucket.Seq == held.Seq && t.Txid == held.Txid {
		return r.checkUnspent(ctx, t)
	}
	if held != nil && t.Bucket.Seq < held.Seq {
		// An older token: this host is behind, or answers a rollback.
		return fmt.Errorf("%w: token at seq %d, the reader holds seq %d", storerec.ErrRollback, t.Bucket.Seq, held.Seq)
	}
	// The walk runs on its own, under its own deadline, so a long chain is
	// walked to the end however many requests give up waiting for it; each
	// request waits for it as long as its own context lasts.
	key := c.key() + ":" + hex.EncodeToString(t.Txid[:])
	jv, started := r.walks.LoadOrStore(key, &walkJob{done: make(chan struct{})})
	job := jv.(*walkJob)
	if !started {
		go func() {
			wctx, cancel := context.WithTimeout(context.Background(), walkTimeout)
			defer cancel()
			job.err = r.walk(wctx, host, c, t)
			close(job.done)
			time.AfterFunc(time.Minute, func() { r.walks.Delete(key) })
		}()
	}
	select {
	case <-job.done:
		return job.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// walkTimeout bounds one walk; walkJob is one in progress.
const walkTimeout = 30 * time.Minute

type walkJob struct {
	done chan struct{}
	err  error
}

// walk walks t back by prev to the token held or to seq 0, under the
// chain's lock, and holds t when the walk checks out (spec section 15.1).
// Every token walked past the one held is handed to OnToken, oldest first.
func (r *Reader) walk(ctx context.Context, host string, c Chain, t *Token) error {
	mu := r.chainLock(c)
	mu.Lock()
	defer mu.Unlock()
	var held *storerec.Held
	if r.Held != nil {
		held = r.Held.Held(c.key())
	}
	if held != nil && t.Bucket.Seq == held.Seq && t.Txid == held.Txid {
		return r.checkUnspent(ctx, t)
	}
	if held != nil && t.Bucket.Seq < held.Seq {
		return fmt.Errorf("%w: token at seq %d, the reader holds seq %d", storerec.ErrRollback, t.Bucket.Seq, held.Seq)
	}
	walk := []storerec.Link{{Txid: t.Txid, Bucket: t.Bucket}}
	cur := t
	for cur.Bucket.Seq > 0 && (held == nil || cur.Bucket.Seq > held.Seq) {
		prev, err := r.tokenAt(ctx, host, c, cur.Bucket.Seq-1, cur.Bucket.Prev, true)
		if err != nil {
			return err
		}
		walk = append(walk, storerec.Link{Txid: prev.Txid, Bucket: prev.Bucket})
		cur = prev
	}
	if cur.Bucket.Seq == 0 {
		walk[len(walk)-1].Spends = spends(cur)
	}
	if err := storerec.CheckWalk(held, walk); err != nil {
		return err
	}
	if err := r.checkUnspent(ctx, t); err != nil {
		return err
	}
	if r.OnToken != nil {
		for i := len(walk) - 1; i >= 0; i-- {
			if held != nil && walk[i].Bucket.Seq <= held.Seq {
				continue
			}
			r.OnToken(walk[i].Bucket)
		}
	}
	if r.Held != nil {
		if err := r.Held.SetHeld(c.key(), storerec.Held{Seq: t.Bucket.Seq, Txid: t.Txid}); err != nil {
			return err
		}
	}
	r.walkDone(c)
	return nil
}

// unspentTTL is how long a token checked unspent is taken as unspent.
const unspentTTL = 15 * time.Second

// checkUnspent is step 1's spend check of the latest token's output: a
// token the spend source shows spent is not the chain's latest (a later
// token spent it, or the bucket is closed), so the host that answered it
// is behind.
func (r *Reader) checkUnspent(ctx context.Context, t *Token) error {
	if r.Spender == nil {
		return nil
	}
	if at, ok := r.unspent.Load(t.Txid); ok && r.now().Sub(at.(time.Time)) < unspentTTL {
		return nil
	}
	by, err := r.Spender(ctx, t.TxidHex(), t.Vout)
	if err != nil {
		r.logf("spend source: token %s: %v (no evidence; the bucket may be stale)", short(t.TxidHex()), err)
		return nil
	}
	if by != "" {
		return fmt.Errorf("%w: the token at seq %d is spent by %s: a later token exists, or the bucket is closed", storerec.ErrBucket, t.Bucket.Seq, short(by))
	}
	r.unspent.Store(t.Txid, r.now())
	return nil
}

// tokenAt finds the chain's token at seq whose txid is want, at host then
// at every other host.
// keep records each token found in the chain's walk file (walk only; a
// walk's completion removes the file).
func (r *Reader) tokenAt(ctx context.Context, host string, c Chain, seq uint64, want [32]byte, keep bool) (*Token, error) {
	if t := r.tokens.get(want); t != nil && t.Bucket.Seq == seq {
		return t, nil
	}
	if t := r.walkSaved(ctx, c, seq, want); t != nil {
		return t, nil
	}
	hosts := append([]string{host}, r.others(host)...)
	for _, h := range hosts {
		a, err := r.ask(ctx, h, c.query("seq", strconv.FormatUint(seq, 10)))
		if err != nil {
			continue
		}
		for _, o := range a.Outputs {
			t, err := r.verifyToken(ctx, c, o)
			if err == nil && t.Txid == want && t.Bucket.Seq == seq {
				if keep {
					r.walkKeep(c, want, o)
				}
				return t, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: no host answers the token at seq %d", storerec.ErrBucket, seq)
}

func (r *Reader) others(host string) []string {
	var out []string
	for _, h := range r.Hosts {
		if h != host {
			out = append(out, h)
		}
	}
	return out
}

// Entry is an entry a reader verified: its carrier's rules (spec section
// 12.1) and its funding tree's proof in the reader's headers.
type Entry struct {
	Entry  *storerec.Entry
	Record []byte
	C      [32]byte // hash byte order
	// BEEF is the carrier as the host answered it.
	BEEF []byte
}

// Txid is the carrier's txid in display order.
func (e *Entry) Txid() string { return displayHex(e.C) }

// VerifyEntry holds a carrier's BEEF to the carrier rules of an entry of
// bucket, and its funding tree's proof to the reader's headers.
func (r *Reader) VerifyEntry(ctx context.Context, bucket string, beef []byte) (*Entry, error) {
	a, err := storerec.Admit(ctx, beef, nil, storerec.Host{Bucket: bucket, Headers: r.Headers})
	if err != nil {
		return nil, fmt.Errorf("entry: %w", err)
	}
	if a.Kind != storerec.TxEntry || a.Carrier == nil || a.Carrier.Entry == nil {
		return nil, errors.New("entry: not an entry carrier")
	}
	tx, err := transaction.NewTransactionFromBEEF(beef)
	if err != nil {
		return nil, err
	}
	if len(tx.Inputs) != 1 || tx.Inputs[0].SourceTransaction == nil || !chaintoken.Mined(ctx, tx.Inputs[0].SourceTransaction, r.Headers) {
		return nil, errors.New("entry: its funding tree's proof does not verify in the reader's headers")
	}
	return &Entry{Entry: a.Carrier.Entry, Record: a.Carrier.Record, C: a.Carrier.Commitment, BEEF: beef}, nil
}

// EntryByC asks every host for the entry carrier whose commitment is c
// (`entry`, spec section 11.1) and returns the first that verifies.
func (r *Reader) EntryByC(ctx context.Context, bucket string, c [32]byte, skip string) (*Entry, error) {
	for _, h := range r.Hosts {
		if h == skip && len(r.Hosts) > 1 {
			continue
		}
		a, err := r.ask(ctx, h, map[string]string{"bucket": bucket, "entry": displayHex(c)})
		if err != nil {
			continue
		}
		for _, o := range a.Outputs {
			e, err := r.VerifyEntry(ctx, bucket, o.Beef)
			if err == nil && e.C == c {
				return e, nil
			}
			if err != nil {
				r.logf("host %s answered entry %s with something that does not verify: %v", h, short(displayHex(c)), err)
			}
		}
	}
	return nil, ErrUnheld
}

// Found is what an object answer proves about a key.
type Found struct {
	Token  *Token
	Result storerec.ObjectResult
	// Entry is the put the map names, for ObjectFound.
	Entry *Entry
}

// Object is a reader's steps 1 to 5 for key (spec section 15.2): the
// latest token, the map's proof, the entry carrier, its writer and kind. A
// key the map names whose entry the answering host cannot produce is asked
// of every other host by `entry`; ErrUnheld when none does. A host whose
// answer does not verify is named and the next asked; ErrUnavailable when
// none answers anything that verifies.
func (r *Reader) Object(ctx context.Context, c Chain, key string) (*Found, error) {
	var refused error
	for _, h := range r.Hosts {
		f, err := r.objectAt(ctx, h, c, key)
		if err == nil {
			return f, nil
		}
		if errors.Is(err, ErrRefused) {
			refused = err
			continue
		}
		if errors.Is(err, ErrUnheld) {
			return nil, err
		}
		r.logf("%v", &hostError{h, err})
	}
	if refused != nil {
		return nil, refused
	}
	return nil, ErrUnavailable
}

func (r *Reader) objectAt(ctx context.Context, host string, c Chain, key string) (*Found, error) {
	a, err := r.ask(ctx, host, c.query("key", key))
	if err != nil {
		return nil, err
	}
	if len(a.Outputs) == 0 {
		return nil, errors.New("an empty answer")
	}
	if len(a.Outputs) > 2 {
		return nil, fmt.Errorf("%d outputs to an object question", len(a.Outputs))
	}
	tok, err := r.verifyToken(ctx, c, a.Outputs[0])
	if err != nil {
		return nil, err
	}
	leaves, err := storerec.DecodeTokenContext(a.Outputs[0].Context)
	if err != nil {
		return nil, err
	}
	var ent *Entry
	var cptr *[32]byte
	var ectx *storerec.EntryContext
	if len(a.Outputs) == 2 {
		if ent, err = r.VerifyEntry(ctx, c.Bucket, a.Outputs[1].Beef); err != nil {
			return nil, err
		}
		x, err := storerec.DecodeEntryContext(a.Outputs[1].Context)
		if err != nil {
			return nil, err
		}
		cptr, ectx = &ent.C, &x
	}
	if err := r.latest(ctx, host, c, tok); err != nil {
		return nil, err
	}
	var e *storerec.Entry
	if ent != nil {
		e = ent.Entry
	}
	res, err := storerec.CheckHead(tok.Bucket, key, cptr, e, ectx, leaves)
	if err != nil {
		if errors.Is(err, storerec.ErrMap) {
			return nil, err
		}
		// The map proves the commitment; what the entry says is the owner's
		// and the writer's.
		return nil, fmt.Errorf("%w: %v", ErrRefused, err)
	}
	f := &Found{Token: tok, Result: res, Entry: ent}
	if res == storerec.ObjectUnheld {
		_, unheld, _ := storerec.CheckObject(tok.Bucket.Count, tok.Bucket.MapRoot, key, nil, nil, leaves)
		got, err := r.EntryByC(ctx, c.Bucket, unheld, host)
		if err != nil {
			return nil, ErrUnheld
		}
		if err := checkNamed(tok, key, got); err != nil {
			return nil, err
		}
		f.Result, f.Entry = storerec.ObjectFound, got
	}
	return f, nil
}

// checkNamed is steps 3 to 5 for an entry fetched by its commitment for a
// key the map names.
func checkNamed(tok *Token, key string, e *Entry) error {
	if err := storerec.CheckEntry(e.Entry, tok.Bucket.Bucket, tok.Bucket.ID, key); err != nil {
		return fmt.Errorf("%w: %v", ErrRefused, err)
	}
	if err := storerec.CheckWriter(e.Entry, tok.Bucket); err != nil {
		return fmt.Errorf("%w: %v", ErrRefused, err)
	}
	if err := storerec.CheckKind(e.Entry); err != nil {
		return fmt.Errorf("%w: %v", ErrRefused, err)
	}
	return nil
}

// Page is one listing page, proven complete (spec section 11.3).
type Page struct {
	Token   *Token
	Entries []*Entry
	// Next is the cursor the next page asks with; Done when no key of the
	// range is left.
	Next string
	Done bool
}

// List is one `list` page for prefix after a, proven complete against the
// latest token, every entry verified. A key the page names as unheld is
// listed from its entry fetched by `entry` at another host; ErrUnheld when
// no host produces it (spec section 16.2).
func (r *Reader) List(ctx context.Context, c Chain, prefix, after string) (*Page, error) {
	var refused error
	for _, h := range r.Hosts {
		p, err := r.listAt(ctx, h, c, prefix, after)
		if err == nil {
			return p, nil
		}
		if errors.Is(err, ErrUnheld) {
			return nil, err
		}
		if errors.Is(err, ErrRefused) {
			refused = err
			continue
		}
		r.logf("%v", &hostError{h, err})
	}
	if refused != nil {
		return nil, refused
	}
	return nil, ErrUnavailable
}

// PageList is the frozen `list` page maximum.
const PageList = 256

func (r *Reader) listAt(ctx context.Context, host string, c Chain, prefix, after string) (*Page, error) {
	a, err := r.ask(ctx, host, c.query("prefix", prefix, "after", after))
	if err != nil {
		return nil, err
	}
	if len(a.Outputs) == 0 {
		return nil, errors.New("an empty answer")
	}
	if len(a.Outputs) > PageList+1 {
		return nil, fmt.Errorf("%d outputs to a list question", len(a.Outputs))
	}
	tok, err := r.verifyToken(ctx, c, a.Outputs[0])
	if err != nil {
		return nil, err
	}
	leaves, err := storerec.DecodeTokenContext(a.Outputs[0].Context)
	if err != nil {
		return nil, err
	}
	var pes []storerec.PageEntry
	var ents []*Entry
	for _, o := range a.Outputs[1:] {
		e, err := r.VerifyEntry(ctx, c.Bucket, o.Beef)
		if err != nil {
			return nil, err
		}
		x, err := storerec.DecodeEntryContext(o.Context)
		if err != nil {
			return nil, err
		}
		pes = append(pes, storerec.PageEntry{Key: e.Entry.Key, C: e.C, Ctx: x})
		ents = append(ents, e)
	}
	if err := storerec.CheckPage(tok.Bucket.Count, tok.Bucket.MapRoot, prefix, after, PageList, pes, leaves); err != nil {
		return nil, err
	}
	if err := r.latest(ctx, host, c, tok); err != nil {
		return nil, err
	}
	for _, e := range ents {
		if err := checkNamed(tok, e.Entry.Key, e); err != nil {
			return nil, err
		}
	}
	p := &Page{Token: tok, Entries: ents}
	var unheld *storerec.MapLeaf
	for i := range leaves {
		if leaves[i].Role == storerec.RoleUnheld {
			unheld = &leaves[i]
		}
	}
	switch {
	case unheld != nil:
		e, err := r.EntryByC(ctx, c.Bucket, unheld.C, host)
		if err != nil {
			return nil, ErrUnheld
		}
		if err := checkNamed(tok, unheld.Key, e); err != nil {
			return nil, err
		}
		p.Entries = append(p.Entries, e)
		p.Next = unheld.Key
	case len(ents) == PageList:
		p.Next = ents[len(ents)-1].Entry.Key
	default:
		p.Done = true
	}
	return p, nil
}

func displayHex(h [32]byte) string {
	var r [32]byte
	for i := range h {
		r[i] = h[31-i]
	}
	return hex.EncodeToString(r[:])
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// tokenCache keeps verified tokens by txid, bounded.
type tokenCache struct {
	mu sync.Mutex
	m  map[[32]byte]*Token
}

func (c *tokenCache) get(txid [32]byte) *Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[txid]
}

func (c *tokenCache) put(t *Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[[32]byte]*Token{}
	}
	if len(c.m) >= 4096 {
		// Drop a quarter, arbitrarily: a cache, never what a walk needs.
		n := 0
		for k := range c.m {
			delete(c.m, k)
			if n++; n >= 1024 {
				break
			}
		}
	}
	c.m[t.Txid] = t
}
