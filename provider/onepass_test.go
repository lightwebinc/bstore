package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/storerec"
)

// A piece taken through /submit has the hashes the separate computations
// give: the carrier's txid (its commitment), the piece's SHA-256 (the
// object the store files it under), its segment leaves and subtree root
// (the proofs), and the closure walk's verdict is chirp.Verify's over the
// store. One object of a full 4 MiB blob, a multi-segment blob and a short
// one, and one of a single short blob.
func TestTakenPieceHashesUnchanged(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(t.TempDir())
	cfg.Limits.SubmitsPerAddrPerSec, cfg.Limits.SubmitBurst = 1e9, 1e9
	p := r.start(cfg, false)
	topic := storerec.BlobTopic(testBucket)
	for _, n := range []int{storerec.BlobSize + 10*storerec.SegmentSize + 7 + storerec.BlobSize, 1000} {
		o, err := r.writer.Build(ctx, content(n, byte(n)))
		if err != nil {
			t.Fatal(err)
		}
		for _, beef := range o.Pieces {
			pc, err := storerec.CheckPieceCarrier(ctx, beef, testBucket, nil)
			if err != nil {
				t.Fatal(err)
			}
			w, err := chaintoken.ReadWire(beef, storerec.MaxPieceBEEF)
			if err != nil {
				t.Fatal(err)
			}
			if txid := chainhash.DoubleHashH(w.SubjectTx().Bytes()); pc.Commitment != [32]byte(txid) {
				t.Fatal("the commitment is not the carrier's txid")
			}
			req := httptest.NewRequest(http.MethodPost, "/submit", bytes.NewReader(beef))
			req.RemoteAddr = "192.0.2.1:4000"
			req.Header.Set("x-topics", topic)
			rec := httptest.NewRecorder()
			p.handleSubmit(rec, req)
			if ok, why := taken(t, rec.Body.String(), topic); rec.Code != http.StatusOK || !ok {
				t.Fatalf("submit: %d %s %s", rec.Code, rec.Body.String(), why)
			}
			piece := pc.Piece.Piece
			h := sha256.Sum256(piece)
			p.mu.Lock()
			u := p.st.Unclaimed[hex.EncodeToString(h[:])]
			p.mu.Unlock()
			if u == nil || u.Size != uint64(len(piece)) {
				t.Fatalf("piece %x not filed under its SHA-256", h[:4])
			}
			if got, err := p.fetch(h); err != nil || !bytes.Equal(got, piece) {
				t.Fatalf("the store does not hold the piece under its hash: %v", err)
			}
			leaves, sub, ok := p.preLeaves(h)
			if !ok {
				t.Fatalf("no leaves kept for %x", h[:4])
			}
			var want []byte
			var lh [][32]byte
			for off := 0; off < len(piece); off += storerec.SegmentSize {
				l := commit.HashLeaf(piece[off:min(off+storerec.SegmentSize, len(piece))])
				lh = append(lh, l)
				want = append(want, l[:]...)
			}
			if !bytes.Equal(leaves, want) || sub != commit.RootOfLeafHashes(lh) {
				t.Fatalf("leaves or subtree root differ for %x", h[:4])
			}
		}
		w := p.walk(o.Built.RootHash, false)
		if w == nil {
			t.Fatal("no walk")
		}
		c, err := w.inc.Result()
		if err != nil || c == nil {
			t.Fatalf("the walk did not finish: %v", err)
		}
		full, err := storerec.VerifyCHIRP(o.Built.RootHash, p.fetch)
		if err != nil {
			t.Fatal(err)
		}
		if len(c.Objects) != len(full.Objects) || len(c.Blobs) != len(full.Blobs) {
			t.Fatal("the walk's closure is not Verify's")
		}
		for i := range c.Objects {
			if c.Objects[i] != full.Objects[i] {
				t.Fatal("the walk's closure is not Verify's")
			}
		}
		if len(p.inc.recent) != 0 {
			t.Fatalf("%d pieces still kept after the walk", len(p.inc.recent))
		}
	}
}
