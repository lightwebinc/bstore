package reader

import (
	"context"
	"errors"
	"testing"

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
