package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/storerec"
)

// benchBlobs is the blobs of the object the ingest benchmark submits: full
// 4 MiB pieces, as the plane carries a large object.
const benchBlobs = 8

// BenchmarkSubmitPiece is the provider's ingest of one full 4 MiB piece
// carrier through /submit, as a writer's unicast is taken: the body read,
// the carrier checked, the piece hashed and stored, its leaves hashed and
// the closure walk moved on. An op is one piece. The object's root goes
// first, so its walk is there when the blobs arrive, then the blobs:
// inorder as a single sender delivers them, shuffled as concurrent takes
// reach the walk. After each object the store, the unclaimed records and
// the walk are reset, outside the timer, so every piece is a new one.
func BenchmarkSubmitPiece(b *testing.B) {
	r := newRig(b)
	ctx := context.Background()
	o, err := r.writer.Build(ctx, content(benchBlobs*storerec.BlobSize, 7))
	if err != nil {
		b.Fatal(err)
	}
	cfg := r.config(b.TempDir())
	cfg.Limits.SubmitsPerAddrPerSec, cfg.Limits.SubmitBurst = 1e9, 1e9
	cfg.UnclaimedBytes = 1 << 34
	cfg.QuotaBytes = 1 << 36
	p := r.start(cfg, false)
	topic := storerec.BlobTopic(testBucket)
	root := o.Built.RootHash
	var objs [][32]byte
	var blobs []chirp.Child
	for _, ob := range o.Built.Objects() {
		h := sha256.Sum256(ob)
		objs = append(objs, h)
		blobs = append(blobs, chirp.Child{Hash: h})
	}
	var total int64
	for _, pc := range o.Pieces {
		total += int64(len(pc))
	}
	if sha256.Sum256(o.Built.Objects()[0]) != root {
		b.Fatal("the first object is not the root")
	}
	reset := func() {
		for _, h := range objs {
			_ = p.store.Delete(h)
		}
		p.mu.Lock()
		for _, h := range objs {
			delete(p.st.Unclaimed, hex.EncodeToString(h[:]))
		}
		p.mu.Unlock()
		// The leaves too: every piece of the next round is new, hashed in full.
		p.inc.forget(root, blobs)
	}
	submit := func(beef []byte) {
		req := httptest.NewRequest(http.MethodPost, "/submit", bytes.NewReader(beef))
		req.RemoteAddr = "192.0.2.1:4000"
		req.Header.Set("x-topics", topic)
		w := httptest.NewRecorder()
		p.handleSubmit(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("submit: %d %s", w.Code, w.Body.String())
		}
		if ok, why := taken(b, w.Body.String(), topic); !ok {
			b.Fatalf("not taken: %s", why)
		}
	}
	for _, mode := range []string{"inorder", "shuffled"} {
		b.Run(mode, func(b *testing.B) {
			rng := rand.New(rand.NewPCG(1, 2))
			order := make([]int, len(o.Pieces))
			b.SetBytes(total / int64(len(o.Pieces)))
			b.ReportAllocs()
			i := len(order)
			// cpu-ns/op is the process's CPU time while the timer runs, every
			// thread's (the collector's included), per piece.
			var cpu time.Duration
			b.ResetTimer()
			at := processCPU()
			for range b.N {
				if i == len(order) {
					b.StopTimer()
					cpu += processCPU() - at
					reset()
					for k := range order {
						order[k] = k
					}
					if mode == "shuffled" {
						// The root (piece 0) stays first: it starts the walk.
						rng.Shuffle(len(order)-1, func(x, y int) { order[x+1], order[y+1] = order[y+1], order[x+1] })
					}
					i = 0
					at = processCPU()
					b.StartTimer()
				}
				submit(o.Pieces[order[i]])
				i++
			}
			b.StopTimer()
			cpu += processCPU() - at
			b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
		})
	}
}
