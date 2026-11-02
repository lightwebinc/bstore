package publish

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/funding"

	"github.com/lightwebinc/bstore/storerec"
)

// benchTx is a transaction of n outputs locked as a hold output or a
// funding output is (49 bytes an output: 1,024 outputs are 50 KB).
func benchTx(tb testing.TB, n int, seed byte) (string, string) {
	tb.Helper()
	key, err := ec.NewPrivateKey()
	if err != nil {
		tb.Fatal(err)
	}
	lock := script.Script(storerec.HoldScript(key.PubKey()))
	prev, err := chainhash.NewHashFromHex(strings.Repeat(fmt.Sprintf("%02x", seed), 32))
	if err != nil {
		tb.Fatal(err)
	}
	unlock, _ := hex.DecodeString(strings.Repeat("ab", 107))
	us := script.Script(unlock)
	tx := transaction.NewTransaction()
	tx.Inputs = append(tx.Inputs, &transaction.TransactionInput{SourceTXID: prev, SourceTxOutIndex: 0, UnlockingScript: &us, SequenceNumber: 0xffffffff})
	for i := 0; i < n; i++ {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &lock})
	}
	return tx.TxID().String(), hex.EncodeToString(tx.Bytes())
}

// benchState is a state of holdTxs hold transactions of holdOuts outputs
// each, all mined, the first used of them taken by an entry; four piece
// trees of 1,023 outputs (the current one, one spent, two ahead), three
// entry trees and two outcome trees of 16.
func benchState(tb testing.TB, holdTxs, holdOuts, used int) *state {
	tb.Helper()
	st := &state{Identity: strings.Repeat("02", 33), Buckets: []string{"a-hmqzvtkwrx", "b-xkqwtrvzmh"}}
	bump := strings.Repeat("ab", 426)
	now := time.Now().Unix()
	for i := 0; i < holdTxs; i++ {
		txid, raw := benchTx(tb, holdOuts, byte(i+1))
		for v := 0; v < holdOuts; v++ {
			h := HoldRecord{Txid: txid, Vout: uint32(v), Sats: 1000, RawHex: raw, BumpHex: bump, Height: 800000, Sent: now}
			if used > 0 {
				h.Used, h.Entry = true, strings.Repeat("e1", 32)
				used--
			}
			st.Holds = append(st.Holds, h)
		}
	}
	tree := func(n int, seed byte, next uint32) funding.Tree {
		txid, raw := benchTx(tb, n, seed)
		return funding.Tree{IdentityKeyHex: st.Identity, Txid: txid, RawHex: raw, BumpHex: bump, Height: 800000, Sats: 1, Count: uint32(n), Next: next, Funder: "pool"}
	}
	cur := tree(1023, 0x50, 500)
	st.Piece = treeSet{Tree: &cur, Trees: []funding.Tree{tree(1023, 0x51, 1023), cur}, Ahead: []funding.Tree{tree(1023, 0x52, 0), tree(1023, 0x53, 0)}}
	ecur := tree(16, 0x60, 7)
	st.Entry = treeSet{Tree: &ecur, Trees: []funding.Tree{tree(16, 0x61, 16), ecur}, Ahead: []funding.Tree{tree(16, 0x62, 0)}}
	ocur := tree(16, 0x70, 3)
	st.Outcome = treeSet{Tree: &ocur, Trees: []funding.Tree{ocur}, Ahead: []funding.Tree{tree(16, 0x71, 0)}}
	return st
}

// putSized is what a put changes in the state: one hold output taken and
// named by an entry, one piece output and one entry output spent. The
// caller holds p.mu.
func putSized(st *state, i int) {
	h := &st.Holds[i%len(st.Holds)]
	h.Used, h.Entry = true, strings.Repeat("e2", 32)
	st.Piece.Tree.Next++
	st.Entry.Tree.Next++
}

// One save of a put-sized change with 4,096 holds in four 1,024-output
// transactions, 1,024 of them taken and named by an entry (the change is
// to one of those, so the state's size holds across the run).
func BenchmarkSave(b *testing.B) {
	p := &Publisher{cfg: Config{Dir: b.TempDir(), Logf: b.Logf}, st: benchState(b, 4, 1024, 1024)}
	if err := p.save(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.mu.Lock()
		putSized(p.st, i%1024)
		err := p.saveLocked()
		p.mu.Unlock()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	p.flushSaves()
}
