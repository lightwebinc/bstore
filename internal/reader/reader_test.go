package reader

import (
	"context"
	"errors"
	"fmt"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/storerec"
)

// The latest token's spend check acts on the spend source's "unspent"
// only when it is "" with no error: an unknown status is no evidence, and
// is neither cached as unspent nor taken as spent.
func TestCheckUnspentNeedsEvidence(t *testing.T) {
	tok := &Token{Txid: [32]byte{1}, Bucket: &storerec.Bucket{Seq: 3}}
	calls := 0
	answer := func() (string, error) { return "", nodeapi.ErrSpendUnknown }
	r := &Reader{Logf: t.Logf, Spender: func(context.Context, string, uint32) (string, error) { calls++; return answer() }}
	for i := 0; i < 2; i++ {
		if err := r.checkUnspent(context.Background(), tok); err != nil {
			t.Fatalf("an unknown status refused the token: %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("an unknown status was cached as unspent: %d calls", calls)
	}
	answer = func() (string, error) { return "ab", nil }
	if err := r.checkUnspent(context.Background(), tok); !errors.Is(err, storerec.ErrBucket) {
		t.Fatalf("a spent token: %v", err)
	}
	answer = func() (string, error) { return "", nil }
	_ = r.checkUnspent(context.Background(), tok)
	_ = r.checkUnspent(context.Background(), tok)
	if calls != 4 {
		t.Fatalf("an explicit unspent was not cached: %d calls", calls)
	}
}

// A token spent by a transaction the chain view knows and shows unmined is
// still the latest: the read goes on. Anything short of that evidence
// refuses, as any spend did before: a spender that mined, one the view does
// not vouch for, a view that cannot answer, and no Unmined at all.
func TestCheckUnspentUnminedSpender(t *testing.T) {
	now := time.Unix(1000, 0)
	tok := &Token{Txid: [32]byte{2}, Bucket: &storerec.Bucket{Seq: 7}}
	unmined, uerr := true, error(nil)
	asked := 0
	r := &Reader{Logf: t.Logf, Now: func() time.Time { return now },
		Spender: func(context.Context, string, uint32) (string, error) { return "ab", nil },
		Unmined: func(_ context.Context, txid string) (bool, error) {
			asked++
			if txid != "ab" {
				t.Fatalf("asked about %q", txid)
			}
			return unmined, uerr
		}}
	ctx := context.Background()
	if err := r.checkUnspent(ctx, tok); err != nil {
		t.Fatalf("a token spent by an unmined transaction was refused: %v", err)
	}
	// Taken as the latest for unspentTTL, then asked again.
	if err := r.checkUnspent(ctx, tok); err != nil || asked != 1 {
		t.Fatalf("asked %d time(s) inside the time: %v", asked, err)
	}
	now = now.Add(unspentTTL)
	if err := r.checkUnspent(ctx, tok); err != nil || asked != 2 {
		t.Fatalf("after the time: asked %d time(s): %v", asked, err)
	}
	// The spender mined (or the view no longer vouches for it): refused,
	// and the refusal is not remembered as unspent.
	now = now.Add(unspentTTL)
	unmined = false
	for i := 0; i < 2; i++ {
		if err := r.checkUnspent(ctx, tok); !errors.Is(err, storerec.ErrBucket) {
			t.Fatalf("a token whose spender is not shown unmined: %v", err)
		}
	}
	// A view that cannot answer is not evidence: refused.
	unmined, uerr = true, errors.New("node down")
	if err := r.checkUnspent(ctx, tok); !errors.Is(err, storerec.ErrBucket) {
		t.Fatalf("a view that could not answer let the token through: %v", err)
	}
	r.Unmined = nil
	if err := r.checkUnspent(ctx, tok); !errors.Is(err, storerec.ErrBucket) {
		t.Fatalf("without Unmined a spend must refuse: %v", err)
	}
}

// UnminedBy answers true only for a transaction the view knows and has no
// proof of.
func TestUnminedBy(t *testing.T) {
	down := errors.New("down")
	for _, c := range []struct {
		name     string
		known    bool
		kerr     error
		perr     error
		want     bool
		wantsErr bool
	}{
		{"known, not mined", true, nil, nodeapi.ErrNotMined, true, false},
		{"known, not mined (wrapped)", true, nil, fmt.Errorf("x: %w", nodeapi.ErrNotMined), true, false},
		{"known, mined", true, nil, nil, false, false},
		{"known, a proof these headers refuse", true, nil, nodeapi.ErrProofRefused, false, false},
		{"not known", false, nil, nodeapi.ErrNotMined, false, false},
		{"whether it is known: down", false, down, nil, false, true},
		{"known, the proof question down", true, nil, down, false, true},
	} {
		f := UnminedBy(func(context.Context, string) (bool, error) { return c.known, c.kerr },
			func(context.Context, string) (*transaction.MerklePath, uint32, error) { return nil, 0, c.perr })
		got, err := f(context.Background(), "ab")
		if got != c.want || (err != nil) != c.wantsErr {
			t.Errorf("%s: got %t, %v", c.name, got, err)
		}
	}
}
