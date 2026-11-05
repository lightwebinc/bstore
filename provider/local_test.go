package provider

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/storerec"
)

// account is the gateway's account of a piece carrier: what the carrier
// check derives from it.
func account(t *testing.T, beef []byte) publish.LocalPiece {
	t.Helper()
	pc, err := storerec.CheckPieceCarrier(context.Background(), bytes.Clone(beef), testBucket, nil)
	if err != nil {
		t.Fatal(err)
	}
	return publish.LocalPiece{Topic: storerec.BlobTopic(testBucket), BEEF: beef, Known: true, Bucket: pc.Piece.Bucket,
		Root: pc.Piece.Root, Writer: pc.Piece.Writer, Piece: pc.Piece.Piece, Hash: pc.Piece.Hash(), Funding: pc.Funding,
		Commitment: pc.Commitment}
}

// TakeLocal keeps a piece the same way trusted or verified: the same object
// under the same name, its funding output claimed. Verifying, it refuses a
// carrier that breaks a rule; trusting, it takes the gateway's account and
// still holds the bucket and the funding output's one carrier.
func TestTakeLocal(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	o, err := r.writer.Build(ctx, bytes.Repeat([]byte("storage node piece "), 400))
	if err != nil {
		t.Fatal(err)
	}
	var beef []byte
	for _, pc := range o.Pieces {
		if bytes.Contains(pc, []byte("storage node piece")) {
			beef = pc
		}
	}
	if beef == nil {
		t.Fatal("no piece carries the content")
	}
	lp := account(t, beef)

	for _, verify := range []bool{true, false} {
		p := r.start(r.config(t.TempDir()), false)
		if err := p.TakeLocal(ctx, lp, verify); err != nil {
			t.Fatalf("verify %v: %v", verify, err)
		}
		if !p.store.Has(lp.Hash) {
			t.Fatalf("verify %v: the piece is not in the store", verify)
		}
		if p.fundingFree(lp.Funding, [32]byte{1}) {
			t.Fatalf("verify %v: the funding output is not claimed", verify)
		}
		// The same piece again is no harm; another carrier on its funding
		// output is refused, trusted or not.
		if err := p.TakeLocal(ctx, lp, verify); err != nil {
			t.Fatalf("verify %v, again: %v", verify, err)
		}
		other := lp
		other.Commitment[0] ^= 1
		other.Known = true
		if !verify {
			if err := p.TakeLocal(ctx, other, false); err == nil || !strings.Contains(err.Error(), "funding") {
				t.Fatalf("a second carrier on one funding output: %v", err)
			}
		}
	}

	// A broken carrier: refused when verified; the trust is in the account,
	// so a trusted take with a true account still keeps the true piece.
	bad := bytes.Clone(beef)
	bad[bytes.Index(bad, []byte("storage node piece"))] ^= 0x01
	broken := lp
	broken.BEEF = bad
	p := r.start(r.config(t.TempDir()), false)
	if err := p.TakeLocal(ctx, broken, true); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a broken carrier, verified: %v", err)
	}
	if p.store.Has(lp.Hash) {
		t.Fatal("a refused piece was stored")
	}
	// No account: checked whatever verify says.
	unknown := publish.LocalPiece{Topic: lp.Topic, BEEF: bad}
	if err := p.TakeLocal(ctx, unknown, false); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a broken carrier with no account: %v", err)
	}
	// Another bucket's topic, or an account of another bucket: refused.
	wrong := lp
	wrong.Topic = storerec.BlobTopic("other_qzvmkwerty")
	if err := p.TakeLocal(ctx, wrong, false); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("another bucket's topic: %v", err)
	}
	wrong = lp
	wrong.Bucket = "other_qzvmkwerty"
	if err := p.TakeLocal(ctx, wrong, false); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("an account of another bucket: %v", err)
	}
}
