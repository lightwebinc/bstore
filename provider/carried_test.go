package provider

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// carriedTerms are a bounty one provider can settle.
var carriedTerms = testwriter.Terms{Sats: 5000, T: 1, K: 2, Retention: 86400, DeadlineAhead: 200, CheckC: 30}

// withLeaves is the gateway's account of a piece with its segment leaves,
// as a storage node's gateway hands a piece of a closure it staged.
func withLeaves(t *testing.T, beef []byte) publish.LocalPiece {
	lp := account(t, beef)
	lp.Leaves, _ = storerec.LeavesOf(lp.Piece)
	lp.Staged = true
	return lp
}

// A storage node's provider, handed every piece of a closure its gateway
// staged on that gateway's account, with the leaves, checks the closure on
// the account: no walk, no piece kept for one, no leaves hashed, and the
// root held with the entry's proof root and advertised. With the piece
// check on, or for a closure the gateway did not stage (a copy: its content
// hash is the source entry's), the same flow is walked.
func TestStorageNodeChecksCarriedClosure(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	for i, mode := range []string{"carried", "verify", "unstaged"} {
		verify := mode == "verify"
		p := r.start(r.config(t.TempDir()), true)
		o, err := r.writer.Build(ctx, content(2*storerec.BlobSize+12345, byte(20+i)))
		if err != nil {
			t.Fatal(err)
		}
		put, err := r.writer.Entry(ctx, o, "node/carried-"+mode+".bin", carriedTerms)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.TakeLocal(ctx, publish.LocalPiece{Topic: storerec.RecordsTopic(testBucket), BEEF: put.BEEF}, false); err != nil {
			t.Fatalf("%s, the entry: %v", mode, err)
		}
		for j, pc := range o.Pieces {
			lp := withLeaves(t, pc)
			lp.Staged = mode != "unstaged"
			if err := p.TakeLocal(ctx, lp, verify); err != nil {
				t.Fatalf("%s, piece %d: %v", mode, j, err)
			}
		}
		root := o.Built.RootHash
		r.eventually("advertised", 20*time.Second, func() bool { return p.liveAd(root) != nil })
		p.mu.Lock()
		h := p.st.Holdings[hex.EncodeToString(root[:])]
		p.mu.Unlock()
		if h == nil || !h.Verified || h.ProofRoot != hex.EncodeToString(o.Built.ProofRoot[:]) {
			t.Fatalf("%s: holding %+v, want the entry's proof root", mode, h)
		}
		carried, walked := p.counter("bstore_provider_closure_checks_total", "mode=carried"), p.counter("bstore_provider_closure_checks_total", "mode=walked")
		if mode != "carried" {
			if walked == 0 || carried != 0 {
				t.Fatalf("checked: carried %v, walked %v; want walked", carried, walked)
			}
			continue
		}
		if carried == 0 || walked != 0 {
			t.Fatalf("checked: carried %v, walked %v; want carried", carried, walked)
		}
		if n := p.counter("bstore_provider_local_leaves_total", "leaves=carried"); n != float64(len(o.Pieces)) {
			t.Fatalf("%v pieces taken with the gateway's leaves, want %d", n, len(o.Pieces))
		}
		p.inc.mu.Lock()
		walks, recent := len(p.inc.walks), len(p.inc.recent)
		p.inc.mu.Unlock()
		if walks != 0 || recent != 0 {
			t.Fatalf("%d walks and %d pieces kept for one, carried", walks, recent)
		}
	}
}

// A carried piece the store already held is not taken on the account: its
// bytes on disk were not written from the gateway's, so the carried check
// reads and hashes them, and a damaged copy holds the root back.
func TestCarriedCheckReadsAPieceItDidNotStore(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	p := r.start(r.config(t.TempDir()), true)
	o, err := r.writer.Build(ctx, content(storerec.BlobSize+999, 31))
	if err != nil {
		t.Fatal(err)
	}
	put, err := r.writer.Entry(ctx, o, "node/damaged.bin", carriedTerms)
	if err != nil {
		t.Fatal(err)
	}
	planted := false
	for _, pc := range o.Pieces {
		lp := account(t, pc)
		if len(lp.Piece) == storerec.BlobSize {
			damaged := bytes.Clone(lp.Piece)
			damaged[len(damaged)/2] ^= 1
			if err := p.store.PutHashed(lp.Hash, damaged); err != nil {
				t.Fatal(err)
			}
			planted = true
		}
	}
	if !planted {
		t.Fatal("no full blob to damage")
	}
	if err := p.TakeLocal(ctx, publish.LocalPiece{Topic: storerec.RecordsTopic(testBucket), BEEF: put.BEEF}, false); err != nil {
		t.Fatalf("the entry: %v", err)
	}
	for j, pc := range o.Pieces {
		if err := p.TakeLocal(ctx, withLeaves(t, pc), false); err != nil {
			t.Fatalf("piece %d: %v", j, err)
		}
	}
	r.eventually("checked", 20*time.Second, func() bool {
		return p.counter("bstore_provider_closure_checks_total", "mode=carried") > 0
	})
	time.Sleep(300 * time.Millisecond)
	if p.liveAd(o.Built.RootHash) != nil {
		t.Fatal("advertised over a damaged copy the provider did not store")
	}
	// The carried check itself refuses the copy (the proof files would
	// refuse it after, as their own read of a blob without leaves does).
	if _, err := p.checkClosure(o.Built.RootHash); !errors.Is(err, storerec.ErrClosure) || storerec.Rule(err) != "hash" {
		t.Fatalf("the carried check over a damaged copy: %v", err)
	}
}

// limits.closure_cache_mb bounds the pieces kept for their walks.
func TestClosureCacheBound(t *testing.T) {
	in := newIncremental(2 * chirp.ChunkSize)
	in.mu.Lock()
	for i := range 3 {
		in.keepRecent([32]byte{byte(i + 1)}, [32]byte{9}, make([]byte, chirp.ChunkSize))
	}
	n, spilled := in.recentLen, len(in.spilled)
	in.mu.Unlock()
	if n != 2*chirp.ChunkSize || spilled != 1 {
		t.Fatalf("kept %d bytes, %d spilled; want 2 pieces kept and 1 spilled", n, spilled)
	}
	if newIncremental(0).recentMax != defaultRecentBytes {
		t.Fatal("the default bound")
	}
	var c Config
	c.defaults()
	if c.Limits.ClosureCacheMB != defaultRecentBytes>>20 {
		t.Fatalf("closure_cache_mb defaults to %d", c.Limits.ClosureCacheMB)
	}
}
