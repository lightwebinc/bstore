package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/internal/storerec"
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

// enqueue asks the worker to look at a root again.
func (p *Provider) enqueue(rootHex string) {
	p.qmu.Lock()
	if p.queued[rootHex] {
		p.qmu.Unlock()
		return
	}
	p.queued[rootHex] = true
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

func (p *Provider) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-p.queue:
			p.qmu.Lock()
			delete(p.queued, r)
			p.qmu.Unlock()
			if err := p.evaluate(ctx, r); err != nil && ctx.Err() == nil {
				p.logf("root %s: %v", short(r), err)
			}
		}
	}
}

// evaluate decides what to do for a root: hold it, advertise it, or wait
// for more of its closure. It runs on the one worker, so one root at a
// time is decided and every advertisement is minted in arrival order.
func (p *Provider) evaluate(ctx context.Context, rootHex string) error {
	p.mu.Lock()
	var undecided []*EntryRec
	for _, e := range p.st.Entries {
		if e.Root == rootHex && e.Decision == "" {
			undecided = append(undecided, e)
		}
	}
	h := p.st.Holdings[rootHex]
	p.mu.Unlock()
	if len(undecided) == 0 {
		return nil
	}
	// Oldest first: the first entry seen is the first raced for.
	sort.Slice(undecided, func(i, j int) bool {
		if undecided[i].At != undecided[j].At {
			return undecided[i].At < undecided[j].At
		}
		return undecided[i].C < undecided[j].C
	})
	if h == nil || !h.Verified {
		ok, err := p.verifyRoot(ctx, rootHex, undecided)
		if err != nil || !ok {
			return err
		}
	}
	p.mu.Lock()
	h = p.st.Holdings[rootHex]
	damaged := h == nil || h.Damaged
	p.mu.Unlock()
	if damaged {
		for _, e := range undecided {
			p.decide(e.C, "the closure is damaged here")
		}
		return nil
	}
	for _, e := range undecided {
		until, why := p.policy(ctx, e)
		if why != "" {
			p.decide(e.C, why)
			p.logf("entry %s for root %s: declined: %s", short(e.C), short(rootHex), why)
			continue
		}
		if err := p.holdFor(ctx, rootHex, e, until); err != nil {
			if ctx.Err() != nil {
				return err
			}
			p.logf("entry %s for root %s: %v; it is looked at again", short(e.C), short(rootHex), err)
			continue
		}
		p.decide(e.C, decisionHeld)
	}
	p.dropUnheld(rootHex)
	return nil
}

func (p *Provider) decide(c, decision string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.st.Entries[c]; e != nil {
		e.Decision = decision
		_ = p.save()
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
	delete(p.st.Holdings, rootHex)
	_ = os.RemoveAll(p.rootDir(rootHex))
	_ = p.save()
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
// other reason is the writer's: every entry it fails is declined. It
// writes the proof tree's files and the holding.
func (p *Provider) verifyRoot(ctx context.Context, rootHex string, entries []*EntryRec) (bool, error) {
	root := mustHash(rootHex)
	if !p.store.Has(root) {
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
	var e *EntryRec
	for _, x := range entries {
		if x.Size > p.cfg.Policy.MaxObject {
			p.decide(x.C, "the object is larger than this provider takes")
			continue
		}
		if _, err := storerec.CheckRoot(rootBytes, root, x.Size, mustHash(x.ContentHash)); err != nil {
			p.decide(x.C, why(err))
			continue
		}
		e = x
		break
	}
	if e == nil {
		return false, nil
	}
	c, err := storerec.VerifyCHIRP(root, p.fetch)
	if err != nil {
		if errors.Is(err, storerec.ErrClosure) {
			// Not all here yet; looked at again when the next piece lands.
			return false, nil
		}
		for _, x := range entries {
			p.decide(x.C, why(err))
		}
		return false, nil
	}
	if !c.Canonical {
		for _, x := range entries {
			p.decide(x.C, "root: not profile 1's canonical construction")
		}
		return false, nil
	}
	pr, bytesHeld, err := p.writeProofFiles(rootHex, c)
	if err != nil {
		if errors.Is(err, errIncomplete) {
			return false, nil
		}
		return false, err
	}
	if pr != mustHash(e.ProofRoot) {
		_ = os.RemoveAll(p.rootDir(rootHex))
		for _, x := range entries {
			if x.ProofRoot == e.ProofRoot {
				p.decide(x.C, "proof-root: the recomputed proof root differs from the entry's")
			}
		}
		return false, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.st.Holdings[rootHex]
	if h == nil {
		h = &Holding{Root: rootHex, At: p.now().Unix()}
		p.st.Holdings[rootHex] = h
	}
	h.Size, h.ContentHash, h.ProofRoot, h.RootLen = e.Size, e.ContentHash, hex.EncodeToString(pr[:]), uint64(len(rootBytes))
	h.Objects, h.Blobs, h.Bytes, h.Verified, h.Damaged = len(c.Objects), len(c.Blobs), bytesHeld, true, false
	for _, o := range c.Objects {
		delete(p.st.Unclaimed, hex.EncodeToString(o[:]))
	}
	if err := p.save(); err != nil {
		return false, err
	}
	p.logf("root %s: closure verified (%d objects, %d bytes, proof root %s)", short(rootHex), len(c.Objects), e.Size, short(h.ProofRoot))
	if p.export != nil {
		if err := p.export.Add(root, c.Objects); err != nil {
			p.logf("root %s: export: %v", short(rootHex), err)
		}
	}
	return true, nil
}

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
		blob, err := p.fetch(b.Hash)
		if err != nil || sha256.Sum256(blob) != b.Hash {
			return [32]byte{}, 0, errIncomplete
		}
		leaves := make([]byte, 0, 32*storerec.SegmentsPerBlob)
		var lh [][32]byte
		for off := 0; off < len(blob); off += storerec.SegmentSize {
			l := commit.HashLeaf(blob[off:min(off+storerec.SegmentSize, len(blob))])
			lh = append(lh, l)
			leaves = append(leaves, l[:]...)
		}
		if _, err := files["leaves"].Write(leaves); err != nil {
			return [32]byte{}, 0, err
		}
		r := commit.RootOfLeafHashes(lh)
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
	for _, f := range files {
		if err := f.Sync(); err != nil {
			return [32]byte{}, 0, err
		}
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
		return nil, fmt.Errorf("%s: short", name)
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
