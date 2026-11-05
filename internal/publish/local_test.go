package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/testchain"

	"github.com/lightwebinc/bstore/storerec"
)

// A gateway with a local provider hands it every piece beside the facade,
// with an account of each that is exactly what the carrier check derives
// from the BEEF: the record's bucket, root, writer and piece, the piece's
// hash, the funding output and the txid. A refusal is not sent again; an
// error is owed.
func TestLocalProviderGetsTheGatewaysAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := httptest.NewServer(testchain.New(700))
	defer cs.Close()
	dir := t.TempDir()
	if _, err := Init(filepath.Join(dir, "id")); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	facade, host := newTarget(t), newTarget(t)
	var mu sync.Mutex
	var got []LocalPiece
	answer := error(nil)
	d := &Delivery{Facade: facade.srv.URL, Hosts: []string{host.srv.URL}}
	d.Local = func(ctx context.Context, lp LocalPiece) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, lp)
		return answer
	}
	p := open(t, ctx, filepath.Join(dir, "id"), cs, store, d)
	w := p.Wallet()
	if _, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, &nodeapi.RPC{URL: cs.URL + "/rpc", ID: "t"}, &nodeapi.Asset{Base: cs.URL}, 104, 0); err != nil {
		t.Fatal(err)
	}
	put := func(key string, content []byte) {
		t.Helper()
		st, err := store.Stage(ctx, bytes.NewReader(content), uint64(len(content)))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Release()
		f := Fields{Bucket: testBucket, ID: sha256.Sum256([]byte("id")), Key: key, ContentType: "application/octet-stream"}
		obj := Object{Root: st.Root, Size: st.Size, ContentHash: st.ContentHash, ProofRoot: st.ProofRoot, Closure: StoreClosure{Store: store, Manifest: st.Manifest}}
		r, err := p.Put(ctx, f, obj, nil)
		if err == nil {
			err = r.Wait(ctx)
		}
		if err != nil || !r.Sent {
			t.Fatalf("put %s: %v %+v", key, err, r)
		}
	}

	// 9 MiB: four pieces, each to the facade and to the local provider.
	put("a", bytes.Repeat([]byte("storage node, "), 9<<20/14+1)[:9<<20])
	blob := storerec.BlobTopic(testBucket)
	if facade.count(blob) != 4 || len(got) != 4 {
		t.Fatalf("pieces: facade %d, local %d", facade.count(blob), len(got))
	}
	for i, lp := range got {
		if lp.Topic != blob || !lp.Known {
			t.Fatalf("piece %d: topic %q known %v", i, lp.Topic, lp.Known)
		}
		pc, err := storerec.CheckPieceCarrier(ctx, bytes.Clone(lp.BEEF), testBucket, nil)
		if err != nil {
			t.Fatalf("piece %d: the BEEF handed over is not a carrier: %v", i, err)
		}
		switch {
		case lp.Bucket != pc.Piece.Bucket || lp.Root != pc.Piece.Root || !bytes.Equal(lp.Writer, pc.Piece.Writer):
			t.Fatalf("piece %d: the record differs from the carrier's", i)
		case !bytes.Equal(lp.Piece, pc.Piece.Piece) || lp.Hash != pc.Piece.Hash():
			t.Fatalf("piece %d: the piece or its hash differs", i)
		case lp.Funding != pc.Funding || lp.Commitment != pc.Commitment:
			t.Fatalf("piece %d: funding %v/%v, commitment %x/%x", i, lp.Funding, pc.Funding, lp.Commitment, pc.Commitment)
		case cap(lp.Piece) != len(lp.Piece):
			t.Fatalf("piece %d: the piece's capacity runs past it", i)
		}
	}
	if p.Outbox().Pending() != 0 {
		t.Fatal("pieces the local provider took are still owed")
	}

	// Refused: delivered nowhere more, owed nowhere.
	mu.Lock()
	got, answer = nil, Refused("room: full")
	mu.Unlock()
	put("b", bytes.Repeat([]byte("refused, "), 3<<20/9+1)[:3<<20])
	if p.Outbox().Pending() != 0 {
		t.Fatal("a refused piece is owed")
	}

	// An error: the pieces stay owed to the local provider only.
	mu.Lock()
	got, answer = nil, errors.New("store: the piece could not be stored")
	mu.Unlock()
	put("c", bytes.Repeat([]byte("errored, "), 3<<20/9+1)[:3<<20])
	if p.Outbox().Pending() == 0 {
		t.Fatal("pieces the local provider failed to store are not owed")
	}
	for _, it := range p.outbox.load() {
		for _, tg := range it.Targets {
			if !tg.Local {
				t.Fatalf("an item owed to %s, not only the local provider", tg.Base)
			}
		}
	}
}
