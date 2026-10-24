package storerec

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chaintoken"
)

// Prepare and Pending.Admit decide every golden transaction vector as Admit
// did before it was split (admitBefore, kept verbatim below), with the
// vector's previous coins and with previous coins out of range, missing,
// repeated and negative, and under host configurations that refuse: the
// same admission, or the same refusal.
func TestPrepareDecidesAsAdmitDid(t *testing.T) {
	var v struct {
		Bucket  string `json:"bucket"`
		Headers []struct {
			Height     uint32 `json:"height"`
			MerkleRoot string `json:"merkleRoot"`
		} `json:"headers"`
		Cases []struct {
			Name string `json:"name"`
			BEEF string `json:"beef"`
			Fill *struct {
				At   int  `json:"at"`
				Byte byte `json:"byte"`
				Len  int  `json:"len"`
			} `json:"beefFill"`
			PreviousCoins []int `json:"previousCoins"`
		} `json:"cases"`
	}
	b, err := os.ReadFile(filepath.Join("..", "testdata", "vectors", "transaction-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	roots := fixedRoots{}
	for _, x := range v.Headers {
		r, err := chainhash.NewHashFromHex(x.MerkleRoot)
		if err != nil {
			t.Fatal(err)
		}
		roots[x.Height] = *r
	}
	hosts := []Host{
		{Bucket: v.Bucket, Headers: roots},
		{Bucket: v.Bucket, Headers: roots, MaxBEEF: 1 << 20, MaxSweepInputs: 4096},
		{Bucket: "not a bucket", Headers: roots},
		{Bucket: v.Bucket, Headers: roots, MaxBEEF: DefaultMaxBEEF - 1},
		{Bucket: "other_qzvmkwerty", Headers: roots},
		{Bucket: v.Bucket, Headers: fixedRoots{}},
	}
	ctx := context.Background()
	n, admitted, hostConfig, refused := 0, 0, 0, 0
	for _, c := range v.Cases {
		beef, err := hex.DecodeString(c.BEEF)
		if err != nil {
			t.Fatal(err)
		}
		if c.Fill != nil {
			out := append([]byte(nil), beef[:c.Fill.At]...)
			for range c.Fill.Len {
				out = append(out, c.Fill.Byte)
			}
			beef = append(out, beef[c.Fill.At:]...)
		}
		helds := [][]int{c.PreviousCoins, nil, {999}, {-1}, {0, 0}, append(append([]int(nil), c.PreviousCoins...), 999)}
		for hi, h := range hosts {
			p := Prepare(ctx, beef, h)
			for _, held := range helds {
				want, werr := admitBefore(ctx, beef, held, h)
				got, gerr := p.Admit(held)
				via, verr := Admit(ctx, beef, held, h)
				where := fmt.Sprintf("%s host %d held %v", c.Name, hi, held)
				if errText(werr) != errText(gerr) || errText(werr) != errText(verr) {
					t.Fatalf("%s: before %v, prepared %v, Admit %v", where, werr, gerr, verr)
				}
				if !reflect.DeepEqual(want, got) || !reflect.DeepEqual(want, via) {
					t.Fatalf("%s: before %+v, prepared %+v, Admit %+v", where, want, got, via)
				}
				n++
				switch {
				case werr == nil:
					admitted++
				case errors.Is(werr, ErrHostConfig):
					hostConfig++
				default:
					refused++
				}
			}
		}
	}
	if n < 1000 || admitted == 0 || hostConfig == 0 || refused == 0 {
		t.Fatalf("%d decisions compared: %d admitted, %d host-config, %d other refusals", n, admitted, hostConfig, refused)
	}
}

// A prepared verdict finishes as often as asked, each admission its own.
func TestPendingAdmitsIndependently(t *testing.T) {
	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &chainhash.Hash{1}, SourceTxOutIndex: 0})
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &chainhash.Hash{2}, SourceTxOutIndex: 0})
	p := &Pending{read: true, tx: tx, a: &Admission{Kind: TxSweep, Outputs: []int{0}}}
	a, err := p.Admit([]int{1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Admit([]int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Retain, []int{1}) || !reflect.DeepEqual(b.Retain, []int{0, 1}) || p.a.Retain != nil {
		t.Fatalf("retained %v and %v, prepared %v", a.Retain, b.Retain, p.a.Retain)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type fixedRoots map[uint32]chainhash.Hash

func (r fixedRoots) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	x, ok := r[height]
	return ok && root != nil && x == *root, nil
}

func (r fixedRoots) CurrentHeight(context.Context) (uint32, error) {
	var top uint32
	for h := range r {
		top = max(top, h)
	}
	return top, nil
}

// admitBefore is Admit as it was before Prepare split it off, verbatim.
func admitBefore(ctx context.Context, beef []byte, held []int, h Host) (*Admission, error) {
	if CheckBucket(h.Bucket) != nil {
		return nil, fmt.Errorf("%w: bucket identifier breaks the grammar", ErrHostConfig)
	}
	bound, maxSweep, err := h.bounds()
	if err != nil {
		return nil, err
	}
	w, err := chaintoken.ReadWire(beef, bound)
	if err != nil {
		return nil, err
	}
	// A value above 2^53 - 1 satoshis is refused with the BEEF: no amount of
	// money is that large, and a reader that holds values as doubles cannot
	// read it, so both codecs refuse it at the same step.
	for _, e := range w.Entries {
		if e.Tx == nil {
			continue
		}
		for _, o := range e.Tx.Outputs {
			if o != nil && o.Satoshis > MaxSafe {
				return nil, fmt.Errorf("%w: an output of %d satoshis", ErrBEEF, o.Satoshis)
			}
		}
	}
	tx := w.SubjectTx()
	retain, err := retained(tx, held)
	if err != nil {
		return nil, err
	}
	a := &Admission{Txid: [32]byte(*tx.TxID()), Retain: retain}

	claim, claims, at := ClaimNone, 0, 0
	for i, o := range tx.Outputs {
		if o == nil || o.LockingScript == nil {
			continue
		}
		if k := Classify(*o.LockingScript); k.Claims() {
			claim, at = k, i
			claims++
		}
	}
	if claims > 1 {
		return nil, fmt.Errorf("%w: %d outputs claim a token or a record", ErrShape, claims)
	}
	switch claim {
	case ClaimPiece:
		return nil, ErrPieceTx
	case ClaimEntry, ClaimOutcome, ClaimFront:
		c, err := checkCarrier(w, tx, claim, len(beef), h)
		if err != nil {
			return nil, err
		}
		a.Kind, a.Carrier, a.Outputs = c.Kind, c, []int{0}
		return a, nil
	case ClaimToken:
		if err := a.checkToken(ctx, w, tx, at, len(beef), h); err != nil {
			return nil, err
		}
		a.Kind, a.Outputs = TxToken, []int{at}
		return a, nil
	}

	// Mined objects: advertisements and hold-shaped outputs.
	ads := map[int]*Advertisement{}
	var holds, admitted []int
	for i, o := range tx.Outputs {
		if o == nil || o.LockingScript == nil {
			continue
		}
		s := *o.LockingScript
		if Classify(s) == ClaimHold {
			holds = append(holds, i)
			admitted = append(admitted, i)
			continue
		}
		if ad, err := DecodeAdvertisement(s); err == nil {
			ads[i] = ad
			admitted = append(admitted, i)
		}
	}
	if len(admitted) > 0 {
		if err := w.Alone(tx); err != nil {
			return nil, err
		}
		if len(beef) > MaxMinedBEEF {
			return nil, fmt.Errorf("%w: a mined object's BEEF of %d bytes", ErrBEEF, len(beef))
		}
		if !chaintoken.Mined(ctx, tx, h.Headers) {
			return nil, ErrUnmined
		}
		if len(admitted) > MaxAdsPerTx {
			return nil, fmt.Errorf("%w: %d", ErrTooMany, len(admitted))
		}
		a.Kind, a.Outputs, a.Ads, a.Holds = TxMined, admitted, ads, holds
		return a, nil
	}

	// A sweep: at most two outputs, output 0 funding-shaped under any key.
	if len(tx.Outputs) >= 1 && len(tx.Outputs) <= 2 && tx.Outputs[0] != nil && tx.Outputs[0].LockingScript != nil &&
		Classify(*tx.Outputs[0].LockingScript) == ClaimFunding {
		if err := w.Alone(tx); err != nil {
			return nil, err
		}
		if len(beef) > MaxSweepBEEF {
			return nil, fmt.Errorf("%w: a sweep's BEEF of %d bytes", ErrBEEF, len(beef))
		}
		if !chaintoken.Mined(ctx, tx, h.Headers) {
			return nil, ErrUnmined
		}
		if len(tx.Inputs) > maxSweep {
			return nil, fmt.Errorf("%w: a sweep of %d inputs, host bound %d", ErrTooLarge, len(tx.Inputs), maxSweep)
		}
		a.Kind, a.Outputs = TxSweep, []int{0}
		for _, in := range tx.Inputs {
			a.Spent = append(a.Spent, transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex})
		}
		return a, nil
	}
	return nil, ErrNotBstore
}
