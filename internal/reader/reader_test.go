package reader

import (
	"context"
	"errors"
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

// A token spent by a transaction that has not mined is still the latest:
// the read goes on. Once the spender has mined, the host that answered the
// token is behind. A source that cannot say whether it mined is no
// evidence. Without Mined, any spend refuses, as before.
func TestCheckUnspentUnminedSpender(t *testing.T) {
	now := time.Unix(1000, 0)
	tok := &Token{Txid: [32]byte{2}, Bucket: &storerec.Bucket{Seq: 7}}
	mined, merr := false, error(nil)
	asked := 0
	r := &Reader{Logf: t.Logf, Now: func() time.Time { return now },
		Spender: func(context.Context, string, uint32) (string, error) { return "ab", nil },
		Mined: func(_ context.Context, txid string) (bool, error) {
			asked++
			if txid != "ab" {
				t.Fatalf("asked whether %q mined", txid)
			}
			return mined, merr
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
	mined = true
	if err := r.checkUnspent(ctx, tok); !errors.Is(err, storerec.ErrBucket) {
		t.Fatalf("a token spent by a mined transaction: %v", err)
	}
	// Not cached as unspent: the next read is refused too.
	if err := r.checkUnspent(ctx, tok); !errors.Is(err, storerec.ErrBucket) {
		t.Fatalf("the refusal did not hold: %v", err)
	}
	mined, merr = false, errors.New("node down")
	before := asked
	for i := 0; i < 2; i++ {
		if err := r.checkUnspent(ctx, tok); err != nil {
			t.Fatalf("no evidence refused the token: %v", err)
		}
	}
	if asked != before+2 {
		t.Fatal("no evidence was cached")
	}
	r.Mined = nil
	if err := r.checkUnspent(ctx, tok); !errors.Is(err, storerec.ErrBucket) {
		t.Fatalf("without Mined a spend must refuse: %v", err)
	}
}
