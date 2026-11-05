package provider

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bstore/internal/testwriter"
	"github.com/lightwebinc/bstore/storerec"
)

// Roots held at once are advertised in one transaction, an output each,
// and every root ends with the live advertisement that names it.
func TestAdvertisementsAreMintedInBatches(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	cfg := r.config(filepath.Join(t.TempDir(), "p"))
	cfg.Limits.VerifyWorkers = 4
	cfg.Limits.AdBatch = 8
	cfg.Limits.AdBatchMs = 200
	gate := make(chan struct{})
	var log pipelineLog
	p := r.startHooked(cfg, true, Options{FreshSpends: true}, func(p *Provider) {
		p.hooks = log.hooks(func(string) { <-gate })
	})
	var puts []*testwriter.Put
	for i := range 12 {
		puts = append(puts, r.put(ctx, []*Provider{p}, content(1024+i, byte(60+i)), "batch/"+string(rune('a'+i)), bounty))
	}
	close(gate)
	for _, put := range puts {
		root := put.Object.Built.RootHash
		r.eventually("advertised", 60*time.Second, func() bool { return p.liveAd(root) != nil })
	}
	txs := map[string]map[uint32]string{}
	for _, put := range puts {
		root := put.Object.Built.RootHash
		ad := p.liveAd(root)
		raw, err := hex.DecodeString(ad.RawHex)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := transaction.NewTransactionFromBytes(raw)
		if err != nil {
			t.Fatal(err)
		}
		if int(ad.Vout) >= len(tx.Outputs) {
			t.Fatalf("root %x: output %d of %d", root[:4], ad.Vout, len(tx.Outputs))
		}
		dec, err := storerec.DecodeAdvertisement(*tx.Outputs[ad.Vout].LockingScript)
		if err != nil {
			t.Fatalf("root %x: output %d: %v", root[:4], ad.Vout, err)
		}
		if dec.Hash != root {
			t.Fatalf("root %x: output %d advertises %x", root[:4], ad.Vout, dec.Hash[:4])
		}
		if txs[ad.Txid] == nil {
			txs[ad.Txid] = map[uint32]string{}
		}
		if other, dup := txs[ad.Txid][ad.Vout]; dup {
			t.Fatalf("output %d of %s names %s and %x", ad.Vout, short(ad.Txid), other, root[:4])
		}
		txs[ad.Txid][ad.Vout] = hex.EncodeToString(root[:])
	}
	if len(txs) >= len(puts) {
		t.Fatalf("%d roots in %d transactions: none batched", len(puts), len(txs))
	}
	for txid, outs := range txs {
		if len(outs) > cfg.Limits.AdBatch {
			t.Fatalf("%s carries %d roots, the batch is %d", short(txid), len(outs), cfg.Limits.AdBatch)
		}
	}
	t.Logf("%d roots in %d transactions", len(puts), len(txs))
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.st.PendingAds); n != 0 {
		t.Fatalf("%d rows still pending", n)
	}
	for _, put := range puts {
		if e := p.st.Entries[put.C]; e == nil || e.Decision != decisionHeld {
			t.Fatalf("entry %s: %+v", short(put.C), e)
		}
	}
}

// A finished row of a transaction minted for several roots leaves the
// others pending, in the state and after a replay.
func TestAdJournalDropsOneRowOfABatch(t *testing.T) {
	st := &State{Holdings: map[string]*Holding{}, Entries: map[string]*EntryRec{}}
	st.apply(&logRow{Ad: &PendingAd{Root: "aa", Txid: "t1", AdVout: 0, Expiry: 500}})
	st.apply(&logRow{Ad: &PendingAd{Root: "bb", Txid: "t1", AdVout: 1, Expiry: 500}})
	st.apply(&logRow{Ad: &PendingAd{Root: "bb", Txid: "t1", AdVout: 1, Expiry: 500}})
	if len(st.PendingAds) != 2 {
		t.Fatalf("%d rows, want one a root", len(st.PendingAds))
	}
	one := uint32(1)
	st.apply(&logRow{AdDone: "t1", AdDoneVout: &one})
	if len(st.PendingAds) != 1 || st.PendingAds[0].Root != "aa" {
		t.Fatalf("rows after one finished: %+v", st.PendingAds)
	}
	st.apply(&logRow{AdDone: "t1"})
	if len(st.PendingAds) != 0 {
		t.Fatalf("rows after the transaction is done: %+v", st.PendingAds)
	}
}
