// Package hostledger holds the host module's payment ledger to bcommon's
// payee package: the line the TypeScript ledger writes (host/src/paid.ts,
// checked byte for byte against the same golden file by its own test) is a
// payee ledger line of the current version, so the payee's settle tooling
// reads, claims and settles a bstore host's payments as it does any host's.
package hostledger_test

import (
	"bytes"
	"encoding/base64"
	"os"
	"slices"
	"testing"

	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/payee"
)

const golden = "../../testdata/host/payments-v2.jsonl"

func TestHostLedgerLineIsThePayeeLedgersOwn(t *testing.T) {
	raw, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	var warn bytes.Buffer
	ps, err := payee.ReadLedger(bytes.NewReader(raw), payee.LedgerFile, &warn)
	if err != nil || warn.Len() != 0 || len(ps) != 1 {
		t.Fatalf("read %d payments, err %v, warnings %q", len(ps), err, warn.String())
	}
	p := ps[0]
	if p.Version() != payee.LedgerVersion {
		t.Fatalf("version %d, want %d", p.Version(), payee.LedgerVersion)
	}
	line, err := p.Line()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(line, raw) {
		t.Fatalf("the payee writes\n%s\nthe host wrote\n%s", line, raw)
	}
	// The coins it names are the coins its transaction spends.
	fromLine, err := p.Spends()
	if err != nil {
		t.Fatal(err)
	}
	q := p
	q.Inputs = nil
	fromTx, err := q.Spends()
	if err != nil || !slices.Equal(fromLine, fromTx) || len(fromTx) == 0 {
		t.Fatalf("spends %v from the line, %v from the transaction (%v)", fromLine, fromTx, err)
	}
	// Its transaction is the payment it names: the txid, and output 0 holding the satoshis.
	b, err := base64.StdEncoding.DecodeString(p.Beef)
	if err != nil {
		t.Fatal(err)
	}
	_, tx, _, err := guard.ParseBEEF(b, guard.DefaultBound)
	if err != nil || tx == nil {
		t.Fatalf("the BEEF: %v", err)
	}
	if tx.TxID().String() != p.Txid || p.OutputIndex != 0 || tx.Outputs[0].Satoshis != p.Satoshis {
		t.Fatalf("txid %s output 0 %d; the line says %s and %d", tx.TxID(), tx.Outputs[0].Satoshis, p.Txid, p.Satoshis)
	}
	// The host's claim rule is the payee's: a txid once, a coin once.
	c := payee.ClaimsOf(ps)
	if got := c.Refusal(p.Txid, p.Inputs); got != payee.Replayed {
		t.Fatalf("the same txid: %v", got)
	}
	if got := c.Refusal("00"+p.Txid[2:], p.Inputs); got != payee.Conflict {
		t.Fatalf("another transaction spending its coin: %v", got)
	}
}
