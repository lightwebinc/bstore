package provider

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// What a provider keeps for each root it holds, under roots/<root hex>/:
//
//	leaves    the proof tree's leaf hashes, 32 bytes a segment, in order
//	blobroots each blob position's subtree root, 32 bytes a blob
//	blobs     each blob position's object hash, 32 bytes a blob
//	objects   the closure's distinct objects, the root first, in its order
//	sorted    the same, ascending: membership by binary search
//
// A sample's path is the path inside its blob's subtree (from that blob's
// 1,024 leaves) followed by the path of the blob's subtree root among the
// blob roots, which is RFC 6962's path because every blob but the last
// holds exactly 1,024 segments (spec section 6). A challenge reads one 4 KiB
// segment, 32 KiB of leaves and the blob roots.

const decisionHeld = "held"

func (p *Provider) rootDir(rootHex string) string { return filepath.Join(p.cfg.Data, "roots", rootHex) }

// A root is in the pipeline from enqueue until its decisions are made:
// waiting in p.queue, then taken (verified, then decided). One taken that
// is asked for again is marked, and queued once more after its decisions,
// so the same root is never verified or decided twice at once.
const (
	rootWaiting uint8 = 1 + iota
	rootTaken
	rootAgain
)

// enqueue asks the worker to look at a root again.
func (p *Provider) enqueue(rootHex string) {
	p.qmu.Lock()
	switch p.queued[rootHex] {
	case rootWaiting, rootAgain:
		p.qmu.Unlock()
		return
	case rootTaken:
		p.queued[rootHex] = rootAgain
		p.qmu.Unlock()
		return
	}
	p.queued[rootHex] = rootWaiting
	p.qmu.Unlock()
	select {
	case p.queue <- rootHex:
	default:
		// The queue is full: the next background pass looks again.
		p.qmu.Lock()
		delete(p.queued, rootHex)
		p.qmu.Unlock()
	}
}

// verified is a root's verification half: the undecided entries it looked
// at, oldest first, the entries it declined, in the order it declined them,
// and whether the decisions go on (ok) or the root waits for its next look.
type verified struct {
	undecided []*EntryRec
	declined  []verdict
	ok        bool
	err       error
}

type verdict struct{ c, why string }

// pipelineHooks, set by tests before Start, see a root taken from the
// queue, about to be verified (where a test makes a disk slow), and
// decided.
type pipelineHooks struct {
	taken, verify, decided func(root string)
}

// worker looks at the queued roots: limits.verify_workers workers each
// take the next root in queue order and see it through, verified and then
// decided, so the file reads, hashes and syncs of several closures overlap,
// and so do the chain reads, signatures and journal syncs of their holds
// and advertisements. A root is in one worker at a time, and its entries
// are decided oldest first; roots are independent of each other, so no
// root waits for a root queued before it (2026-11-05: one decision worker
// held small entries to about 60 a second, a few network round trips and a
// sync each, in a row).
func (p *Provider) worker(ctx context.Context) {
	n := p.cfg.Limits.VerifyWorkers
	jobs := make(chan string)
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(n)
	for range n {
		go func() { defer wg.Done(); p.decider(ctx, jobs) }()
	}
	for {
		var r string
		select {
		case <-ctx.Done():
			return
		case r = <-p.queue:
		}
		p.qmu.Lock()
		p.queued[r] = rootTaken
		p.qmu.Unlock()
		if p.hooks.taken != nil {
			p.hooks.taken(r)
		}
		select {
		case <-ctx.Done():
			return
		case jobs <- r:
		}
	}
}

// decider verifies and decides the roots it is handed, one at a time.
func (p *Provider) decider(ctx context.Context, jobs <-chan string) {
	for {
		var r string
		select {
		case <-ctx.Done():
			return
		case r = <-jobs:
		}
		if p.hooks.verify != nil {
			p.hooks.verify(r)
		}
		v := p.verify(ctx, r)
		if ctx.Err() != nil {
			// Not decided: a start, or the next background pass, looks
			// at it again.
			return
		}
		if err := p.evaluate(ctx, r, v); err != nil && ctx.Err() == nil {
			p.logf("root %s: %v", short(r), err)
		}
		if p.hooks.decided != nil {
			p.hooks.decided(r)
		}
		p.qmu.Lock()
		again := p.queued[r] == rootAgain
		delete(p.queued, r)
		p.qmu.Unlock()
		if again {
			p.enqueue(r)
		}
	}
}

// verify is a root's verification half: its undecided entries, and its
// closure verified when it is not held verified yet. It decides nothing:
// the entries it declines are returned for evaluate to decline, in order.
// Workers run it for different roots at once.
func (p *Provider) verify(ctx context.Context, rootHex string) verified {
	p.mu.Lock()
	var undecided []*EntryRec
	for _, e := range p.index.undecided[rootHex] {
		undecided = append(undecided, e)
	}
	h := p.st.Holdings[rootHex]
	p.mu.Unlock()
	if len(undecided) == 0 {
		return verified{}
	}
	// Oldest first: the first entry seen is the first raced for.
	sort.Slice(undecided, func(i, j int) bool {
		if undecided[i].At != undecided[j].At {
			return undecided[i].At < undecided[j].At
		}
		return undecided[i].C < undecided[j].C
	})
	v := verified{undecided: undecided, ok: true}
	if h == nil || !h.Verified {
		v.ok, v.err = p.verifyRoot(ctx, rootHex, undecided, func(c, why string) {
			v.declined = append(v.declined, verdict{c, why})
		})
	}
	return v
}

// evaluate decides what to do for a root once verified: hold it, advertise
// it, or wait for more of its closure. Workers run it for different roots
// at once; a root's own entries are decided one at a time, oldest first.
func (p *Provider) evaluate(ctx context.Context, rootHex string, v verified) error {
	for _, d := range v.declined {
		p.decide(d.c, d.why)
	}
	if v.err != nil || !v.ok {
		return v.err
	}
	p.mu.Lock()
	// An entry decided since it was looked at (declined above, or ended
	// by a delete while the roots before this one were decided) keeps
	// its decision.
	undecided := slices.DeleteFunc(slices.Clone(v.undecided), func(e *EntryRec) bool { return e.Decision != "" })
	h := p.st.Holdings[rootHex]
	damaged := h == nil || h.Damaged
	p.mu.Unlock()
	if damaged {
		for _, e := range undecided {
			p.decide(e.C, "the closure is damaged here")
		}
		return nil
	}
	for _, e := range undecided {
		if err := p.decideEntry(ctx, rootHex, e); err != nil {
			return err
		}
	}
	p.dropUnheld(rootHex)
	return nil
}

// decideEntry decides one entry of a verified root, its hold output claimed
// for the while: a second entry naming the output, in another worker,
// waits for this decision. It returns an error only when the run is over.
func (p *Provider) decideEntry(ctx context.Context, rootHex string, e *EntryRec) error {
	if e.Hold != nil {
		op := e.Hold.Outpoint()
		p.mu.Lock()
		if p.claims == nil {
			p.claims = map[string]string{}
		}
		if c := p.claims[op]; c != "" && c != e.C {
			p.mu.Unlock()
			return nil // looked at again on the next pass
		}
		p.claims[op] = e.C
		p.mu.Unlock()
		defer func() {
			p.mu.Lock()
			if p.claims[op] == e.C {
				delete(p.claims, op)
			}
			p.mu.Unlock()
		}()
	}
	// A closure held already is the one its root names: an entry that
	// says otherwise of its size, content or proof root is the writer's
	// fault, and not taken.
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	mismatch := h != nil && (e.Size != h.Size || e.ContentHash != h.ContentHash || e.ProofRoot != h.ProofRoot)
	p.mu.Unlock()
	if mismatch {
		p.decide(e.C, "the entry's size, content hash or proof root is not the closure's")
		return nil
	}
	until, why := p.policy(ctx, e)
	if why == decisionRetry {
		// Looked at again on the next pass, for as long as an
		// unclaimed piece is kept, and then declined.
		if p.now().UnixNano()-e.At > int64(time.Duration(p.cfg.UnclaimedSeconds)*time.Second) {
			p.decide(e.C, "the bounty could not be checked in time")
		}
		return nil
	}
	if why != "" {
		p.decide(e.C, why)
		p.logf("entry %s for root %s: declined: %s", short(e.C), short(rootHex), why)
		return nil
	}
	if err := p.holdFor(ctx, rootHex, e, until); err != nil {
		if ctx.Err() != nil {
			return err
		}
		p.logf("entry %s for root %s: %v; it is looked at again", short(e.C), short(rootHex), err)
		return nil
	}
	p.decide(e.C, decisionHeld)
	return nil
}

// expireUndecided declines the entries whose closure has not arrived
// within the time an unclaimed piece is kept: the pieces that came are gone
// by then too.
func (p *Provider) expireUndecided(entries []*EntryRec, rootHere bool, decide func(c, why string)) {
	wait := time.Duration(p.cfg.UnclaimedSeconds) * time.Second
	if !rootHere {
		// Not even the root node came: a closure that is coming starts
		// with its pieces, so such an entry waits a short while only.
		wait = min(wait, rootWait)
	}
	cutoff := p.now().Add(-wait).UnixNano()
	for _, e := range entries {
		if e.At < cutoff {
			decide(e.C, "the closure did not arrive in time")
		}
	}
}

// rootWait is how long an entry waits when no piece of its root came.
const rootWait = 5 * time.Minute

func (p *Provider) decide(c, decision string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.st.Entries[c]; e != nil {
		p.setDecision(e, decision)
		if decision != decisionHeld {
			p.releasePiecesLocked(e.Root)
		}
		// A start looks at every entry undecided or held again.
		p.dirty = true
	}
}

// dropUnheld forgets a verified holding no entry was taken for, and its
// pieces go back to being unclaimed (evicted on their own clock).
func (p *Provider) dropUnheld(rootHex string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.st.Holdings[rootHex]
	if h == nil || len(h.Entries) > 0 || h.Ad != nil || h.Lease > 0 {
		return
	}
	if len(p.index.undecided[rootHex]) > 0 {
		return
	}
	p.dropHoldingLocked(rootHex)
	p.dirty = true
}

// fetch is the store as a CHIRP fetch: an object's whole bytes.
func (p *Provider) fetch(h [32]byte) ([]byte, error) {
	n := p.store.Size(h)
	if n < 0 {
		return nil, errors.New("not held")
	}
	return p.store.Get(h, 0, uint64(n))
}

// verifyRoot checks a closure the store holds against the first entry that
// names it with the right size and content hash: spec section 9.3 steps 1
// and 2, as rows C1 to C4 of frozen.md section 14 have them. A closure not
// yet complete is waited for (false, nil). A closure that fails for any
// other reason is the writer's: every entry it fails is declined, through
// decline, which evaluate applies. It writes the proof tree's
// files and the holding. Workers run it for different roots at once: what
// it writes is the root's own directory and, under p.mu, its holding.
func (p *Provider) verifyRoot(ctx context.Context, rootHex string, entries []*EntryRec, decline func(c, why string)) (bool, error) {
	root := mustHash(rootHex)
	// Every object the closure names must be here before a blob is read:
	// a closure still arriving costs its node reads, never its blobs.
	if !p.store.Has(root) || !p.complete(root) {
		p.expireUndecided(entries, p.store.Has(root), decline)
		return false, nil
	}
	select {
	case p.limits.verifyMu <- struct{}{}:
		defer func() { <-p.limits.verifyMu }()
	case <-ctx.Done():
		return false, ctx.Err()
	}
	rootBytes, err := p.fetch(root)
	if err != nil {
		return false, nil
	}
	// The root's own rules against each entry's size and content hash:
	// an entry that disagrees is the writer's fault, declined alone.
	var fit []*EntryRec
	for _, x := range entries {
		if x.Size > p.cfg.Policy.MaxObject {
			decline(x.C, "the object is larger than this provider takes")
			continue
		}
		if _, err := storerec.CheckRoot(rootBytes, root, x.Size, mustHash(x.ContentHash)); err != nil {
			decline(x.C, why(err))
			continue
		}
		fit = append(fit, x)
	}
	if len(fit) == 0 {
		return false, nil
	}
	entries = fit
	c, err := p.checkClosure(root)
	if err != nil {
		if errors.Is(err, storerec.ErrClosure) {
			// Not all here yet; looked at again when the next piece lands.
			return false, nil
		}
		for _, x := range entries {
			decline(x.C, why(err))
		}
		p.inc.forget(root, nil)
		return false, nil
	}
	if !c.Canonical {
		for _, x := range entries {
			decline(x.C, "root: not profile 1's canonical construction")
		}
		p.inc.forget(root, c.Blobs)
		return false, nil
	}
	pr, bytesHeld, err := p.writeProofFiles(rootHex, c)
	if err != nil {
		if errors.Is(err, errIncomplete) {
			return false, nil
		}
		return false, err
	}
	// The proof root is computed once and every entry decided against it,
	// so entries naming the root with another proof root cost one pass.
	var e *EntryRec
	for _, x := range entries {
		if mustHash(x.ProofRoot) != pr {
			decline(x.C, "proof-root: the recomputed proof root differs from the entry's")
		} else if e == nil {
			e = x
		}
	}
	if e == nil {
		_ = os.RemoveAll(p.rootDir(rootHex))
		return false, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Another root dropped while this one was read may have taken an
	// object the two share (its last holder, as far as it could tell):
	// checked under p.mu, where a drop deletes, the closure is whole.
	for _, o := range c.Objects {
		if !p.store.Has(o) {
			_ = os.RemoveAll(p.rootDir(rootHex))
			return false, nil
		}
	}
	h := p.st.Holdings[rootHex]
	if h == nil {
		h = &Holding{Root: rootHex, At: p.now().Unix()}
		p.st.Holdings[rootHex] = h
	}
	h.Size, h.ContentHash, h.ProofRoot, h.RootLen = e.Size, e.ContentHash, hex.EncodeToString(pr[:]), uint64(len(rootBytes))
	if !h.Verified {
		p.noteHeld(c.Objects, 1)
	}
	h.Objects, h.Blobs, h.Bytes, h.Verified, h.Damaged = len(c.Objects), len(c.Blobs), bytesHeld, true, false
	// The closure's pieces are held now; a piece another closure still
	// waiting for its entry sent keeps its record.
	for _, o := range c.Objects {
		k := hex.EncodeToString(o[:])
		if u := p.st.Unclaimed[k]; u != nil && (u.Root == rootHex || u.Root == "") {
			delete(p.st.Unclaimed, k)
		}
	}
	// Checked again on a start, if a crash loses this.
	p.dirty = true
	p.inc.forget(root, c.Blobs)
	p.logf("root %s: closure verified (%d objects, %d bytes, proof root %s)", short(rootHex), len(c.Objects), e.Size, short(h.ProofRoot))
	if p.export != nil {
		if err := p.export.Add(root, c.Objects); err != nil {
			p.logf("root %s: export: %v", short(rootHex), err)
		}
	}
	return true, nil
}

var errShort = errors.New("a file is shorter than its record")

var errIncomplete = errors.New("an object of the closure is not held")

// writeProofFiles reads every blob of a verified closure once more, hashes
// its segments and writes the root's files; it returns the proof root and
// the bytes the closure's distinct objects take.
func (p *Provider) writeProofFiles(rootHex string, c *chirp.Closure) ([32]byte, uint64, error) {
	dir := p.rootDir(rootHex)
	tmp := dir + ".tmp"
	_ = os.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return [32]byte{}, 0, err
	}
	defer os.RemoveAll(tmp)
	files := map[string]*os.File{}
	for _, n := range []string{"leaves", "blobroots", "blobs", "objects", "sorted"} {
		f, err := os.Create(filepath.Join(tmp, n))
		if err != nil {
			return [32]byte{}, 0, err
		}
		defer f.Close()
		files[n] = f
	}
	var roots [][32]byte
	for _, b := range c.Blobs {
		leaves, r, ok := p.preLeaves(b.Hash)
		if !ok {
			blob, err := p.fetch(b.Hash)
			if err != nil {
				return [32]byte{}, 0, errIncomplete
			}
			var h [32]byte
			if h, leaves, r = storerec.HashBlob(blob); h != b.Hash {
				return [32]byte{}, 0, errIncomplete
			}
		}
		if _, err := files["leaves"].Write(leaves); err != nil {
			return [32]byte{}, 0, err
		}
		roots = append(roots, r)
		if _, err := files["blobroots"].Write(r[:]); err != nil {
			return [32]byte{}, 0, err
		}
		if _, err := files["blobs"].Write(b.Hash[:]); err != nil {
			return [32]byte{}, 0, err
		}
	}
	var total uint64
	for _, o := range c.Objects {
		total += uint64(max(0, p.store.Size(o)))
		if _, err := files["objects"].Write(o[:]); err != nil {
			return [32]byte{}, 0, err
		}
	}
	sorted := slices.Clone(c.Objects)
	slices.SortFunc(sorted, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
	for _, o := range sorted {
		if _, err := files["sorted"].Write(o[:]); err != nil {
			return [32]byte{}, 0, err
		}
	}
	if err := syncAll(files); err != nil {
		return [32]byte{}, 0, err
	}
	pr := storerec.EmptyHash
	if len(roots) > 0 {
		var err error
		if pr, err = commit.RootOfSubtrees(roots); err != nil {
			return [32]byte{}, 0, err
		}
	}
	_ = os.RemoveAll(dir)
	if err := os.Rename(tmp, dir); err != nil {
		return [32]byte{}, 0, err
	}
	return pr, total, nil
}

// syncAll makes the files durable together: on a journaling file system
// fsyncs in flight at once share one journal commit, so the root's five
// files cost about one device flush instead of five in a row (2026-11-04:
// the provider's root worker spent most of its time in these syncs). It
// returns the first error.
func syncAll(files map[string]*os.File) error {
	errs := make(chan error, len(files))
	for _, f := range files {
		go func() { errs <- f.Sync() }()
	}
	var first error
	for range files {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// inClosure reports whether obj is an object of a held root's closure, by
// binary search of its sorted list.
func (p *Provider) inClosure(rootHex string, obj [32]byte) bool {
	f, err := os.Open(filepath.Join(p.rootDir(rootHex), "sorted"))
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return false
	}
	n := st.Size() / 32
	var buf [32]byte
	lo, hi := int64(0), n
	for lo < hi {
		mid := (lo + hi) / 2
		if _, err := f.ReadAt(buf[:], mid*32); err != nil {
			return false
		}
		switch bytes.Compare(buf[:], obj[:]) {
		case 0:
			return true
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false
}

// rootObjects reads a root's closure objects, in order.
func (p *Provider) rootObjects(rootHex string) ([][32]byte, error) {
	raw, err := os.ReadFile(filepath.Join(p.rootDir(rootHex), "objects"))
	if err != nil {
		return nil, err
	}
	out := make([][32]byte, 0, len(raw)/32)
	for i := 0; i+32 <= len(raw); i += 32 {
		out = append(out, [32]byte(raw[i:i+32]))
	}
	return out, nil
}

// readAt32 reads n 32-byte records from file name of a root from record i.
func (p *Provider) readAt32(rootHex, name string, i, n uint64) ([][32]byte, error) {
	f, err := os.Open(filepath.Join(p.rootDir(rootHex), name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, 32*n)
	if _, err := f.ReadAt(buf, int64(32*i)); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	} else if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %w", name, errShort)
	}
	out := make([][32]byte, n)
	for k := range out {
		out[k] = [32]byte(buf[32*k : 32*k+32])
	}
	return out, nil
}

// damaged marks a root whose bytes went bad: it is no longer served,
// answered for or renewed, and its advertisement lapses.
func (p *Provider) damaged(rootHex, why string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.st.Holdings[rootHex]
	if h == nil || h.Damaged {
		return
	}
	h.Damaged = true
	_ = p.save()
	p.metrics.inc("bstore_provider_damaged_total")
	p.logf("root %s: DAMAGED: %s; it is no longer served, answered for or renewed", short(rootHex), why)
}

func mustHash(h string) [32]byte {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 32 {
		return [32]byte{}
	}
	return [32]byte(b)
}

// displayHash is a txid in display order as its hash byte order.
func displayHash(txid string) ([32]byte, bool) {
	h, err := chainhash.NewHashFromHex(txid)
	if err != nil || len(txid) != 64 {
		return [32]byte{}, false
	}
	return [32]byte(*h), true
}

func blobPos(i uint64) (blob, off uint64) {
	return i / storerec.SegmentsPerBlob, (i % storerec.SegmentsPerBlob) * storerec.SegmentSize
}

// why is a refusal as its label and, where there is one, its rule.
func why(err error) string {
	if r := storerec.Rule(err); r != "" {
		return storerec.Reason(err) + " (" + r + ")"
	}
	return storerec.Reason(err)
}
