package publish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// The kill switch (spec sections 12.4 and 13.5): a sweep of every funding
// tree this identity holds, each output used or not, so that once it mines
// every carrier those outputs funded (every piece, entry and outcome this
// identity published) is a double spend that honest hosts retract and stop
// answering. Each sweep is recorded before it is sent and published, once
// mined, to the records topic of every bucket this identity published in.
// An entry a token already anchored stays anchored: a key leaves a map by
// a delete entry and the owner's next token, never by a sweep.

// SweepRecord is one tree's sweep.
type SweepRecord struct {
	Kind   string         `json:"kind"`
	Tree   string         `json:"tree"`
	Vouts  []uint32       `json:"vouts"`
	Txid   string         `json:"txid"`
	Raw    string         `json:"raw"`
	Fee    bwallet.Output `json:"fee"`
	Height uint32         `json:"height,omitempty"`
	Bump   string         `json:"bump,omitempty"`
	// Published is set once every target took it.
	Published bool   `json:"published,omitempty"`
	Failed    string `json:"failed,omitempty"`
}

// KillTree is a tree the kill switch would sweep.
type KillTree struct {
	Kind  string
	Txid  string
	Vouts []uint32
	// Unmined trees are named and left: a tree not yet mined funds no
	// carrier a host admitted.
	Unmined bool
}

func derivationOf(kind string) pushdrop.Derivation {
	switch kind {
	case kindPiece:
		return storerec.PieceDerivation
	case kindOutcome:
		return storerec.OutcomeDerivation
	}
	return storerec.EntryDerivation
}

// KillPlan is every tree output the kill switch would sweep: every output
// of every tree this identity holds that no sweep it recorded spends.
func (p *Publisher) KillPlan() []KillTree {
	p.mu.Lock()
	defer p.mu.Unlock()
	swept := map[string]bool{}
	for _, sw := range p.st.Sweeps {
		if sw.Failed != "" {
			continue
		}
		for _, v := range sw.Vouts {
			swept[fmt.Sprintf("%s.%d", sw.Tree, v)] = true
		}
	}
	seen := map[string]bool{}
	var out []KillTree
	for _, kind := range []string{kindPiece, kindEntry, kindOutcome} {
		s := p.set(kind)
		var all []funding.Tree
		if s.Tree != nil {
			all = append(all, *s.Tree)
		}
		all = append(append(all, s.Trees...), s.Ahead...)
		for _, t := range all {
			if seen[t.Txid] || t.IdentityKeyHex != p.signer.IdentityHex() {
				continue
			}
			seen[t.Txid] = true
			kt := KillTree{Kind: kind, Txid: t.Txid, Unmined: t.BumpHex == ""}
			for v := uint32(0); v < t.Count; v++ {
				if !swept[fmt.Sprintf("%s.%d", t.Txid, v)] {
					kt.Vouts = append(kt.Vouts, v)
				}
			}
			if len(kt.Vouts) > 0 {
				out = append(out, kt)
			}
		}
	}
	return out
}

// Kill sweeps every tree of the plan, one mined transaction a tree, and
// publishes each sweep. The trees are retired first, so no carrier is made
// on them again. An output the chain already shows spent is left out: the
// transaction that spends it is another instance's sweep or a carrier that
// was mined, which a carrier never is. It returns the sweeps made and
// published.
func (p *Publisher) Kill(ctx context.Context) ([]SweepRecord, error) {
	plan := p.KillPlan()
	p.mu.Lock()
	for _, kind := range []string{kindPiece, kindEntry, kindOutcome} {
		s := p.set(kind)
		if s.Tree != nil {
			s.Trees = producer.Index(s.Trees, s.Tree)
			s.Tree = nil
		}
		s.Trees = append(s.Trees, s.Ahead...)
		s.Ahead = nil
	}
	err := p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var done []SweepRecord
	var errs []error
	for _, kt := range plan {
		if kt.Unmined {
			p.cfg.Logf("tree %s is not mined: it funds no carrier a host admitted, and is not swept", short(kt.Txid))
			continue
		}
		rec, err := p.sweepTree(ctx, kt)
		if err != nil {
			errs = append(errs, fmt.Errorf("tree %s: %w", short(kt.Txid), err))
			continue
		}
		if rec != nil {
			done = append(done, *rec)
		}
	}
	// Sweeps recorded and not yet seen through, from this run or another.
	if err := p.finishSweeps(ctx); err != nil {
		errs = append(errs, err)
	}
	// What the sweeps retract is never sent again.
	p.outbox.Drop("piece", "entry", "outcome")
	return done, errors.Join(errs...)
}

func (p *Publisher) treeTx(txid string) (*transaction.Transaction, error) {
	return p.loadKept(txid)
}

func (p *Publisher) sweepTree(ctx context.Context, kt KillTree) (*SweepRecord, error) {
	tree, err := p.treeTx(kt.Txid)
	if err != nil {
		return nil, err
	}
	var vouts []uint32
	for _, v := range kt.Vouts {
		free, _, err := p.unspent(ctx, kt.Txid, v)
		if err != nil {
			return nil, err
		}
		if free {
			vouts = append(vouts, v)
		}
	}
	if len(vouts) == 0 {
		return nil, nil
	}
	pay := p.newPayer(ctx)
	fee, err := pay.TakeAtLeast(ctx, p.cfg.Fees.Floor*4+uint64(len(vouts))*p.cfg.Fees.SatPerByte*160)
	if err != nil {
		return nil, feeError(err)
	}
	change, err := p.signer.FundScript()
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	tx, err := carrier.Sweep(ctx, p.wallet, p.wallet.Originator, carrierParams(derivationOf(kt.Kind)), tree, vouts,
		fee.Tx, fee.Vout, fee.Unlocker, change, p.cfg.Fees.SatPerByte, p.cfg.Fees.Floor)
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	rec := SweepRecord{Kind: kt.Kind, Tree: kt.Txid, Vouts: vouts, Txid: tx.TxID().String(), Raw: hex.EncodeToString(tx.Bytes()), Fee: coinOf(fee)}
	p.mu.Lock()
	p.st.Sweeps = append(p.st.Sweeps, rec)
	err = p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	return &rec, nil
}

// finishSweeps sends every recorded sweep not yet mined (the same bytes),
// waits for it, and publishes it to every bucket's records topic.
func (p *Publisher) finishSweeps(ctx context.Context) error {
	p.mu.Lock()
	todo := slices.Clone(p.st.Sweeps)
	buckets := slices.Clone(p.st.Buckets)
	p.mu.Unlock()
	var errs []error
	for _, sw := range todo {
		if sw.Failed != "" || sw.Published {
			continue
		}
		raw, err := hex.DecodeString(sw.Raw)
		if err != nil {
			continue
		}
		tx, err := transaction.NewTransactionFromBytes(raw)
		if err != nil {
			continue
		}
		if sw.Bump == "" {
			mp, height, err := p.cfg.Asset.Proof(ctx, sw.Txid)
			if err != nil {
				cf, cerr := p.SpentElsewhere(ctx, tx)
				switch {
				case cerr != nil:
					errs = append(errs, fmt.Errorf("sweep %s: %w", short(sw.Txid), cerr))
					continue
				case cf != nil && cf.Same:
					tx = cf.By
					sw.Txid, sw.Raw = tx.TxID().String(), hex.EncodeToString(tx.Bytes())
					p.updateSweepTxid(sw)
				case cf != nil:
					// It can never mine: the outputs left are swept again
					// by the next kill.
					sw.Failed = "an input was spent by " + cf.By.TxID().String()
					p.updateSweepTxid(sw)
					errs = append(errs, fmt.Errorf("%w: sweep %s: %s; kill again sweeps what is left", ErrNotYet, short(sw.Txid), sw.Failed))
					continue
				default:
					if serr := p.cfg.Settler.Submit(ctx, tx); serr != nil {
						p.cfg.Logf("sweep %s: sent: %v", short(sw.Txid), serr)
					}
				}
				mp, height, err = p.newPayer(ctx).Await(ctx, "sweep", tx)
				if err != nil {
					errs = append(errs, fmt.Errorf("sweep %s: %w", short(sw.Txid), err))
					continue
				}
			}
			tx.MerklePath = mp
			p.newPayer(ctx).Change(tx, height, mp)
			sw.Bump, sw.Height = funding.BumpHex(mp), height
			p.updateSweep(sw)
		} else {
			mp, err := transaction.NewMerklePathFromHex(sw.Bump)
			if err != nil {
				continue
			}
			tx.MerklePath = mp
		}
		beef, err := aloneBEEF(tx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		all := true
		for _, b := range buckets {
			targets := p.cfg.Delivery.recordTargets(b, false)
			ok := p.Resend(ctx, sw.Txid+"-"+short(b), "sweep", beef, targets)
			if len(ok) < len(targets) {
				all = false
			}
		}
		if all {
			sw.Published = true
			p.updateSweep(sw)
		}
	}
	return errors.Join(errs...)
}

// updateSweepTxid replaces the sweep recorded for the same tree and
// outputs: its txid may have changed (re-encoded).
func (p *Publisher) updateSweepTxid(sw SweepRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Sweeps {
		x := &p.st.Sweeps[i]
		if x.Tree == sw.Tree && x.Failed == "" && slices.Equal(x.Vouts, sw.Vouts) {
			*x = sw
		}
	}
	_ = p.saveLocked()
}

func (p *Publisher) updateSweep(sw SweepRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Sweeps {
		if p.st.Sweeps[i].Txid == sw.Txid {
			p.st.Sweeps[i] = sw
		}
	}
	_ = p.saveLocked()
}

// Sweeps are the kill switch's sweeps.
func (p *Publisher) Sweeps() []SweepRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.st.Sweeps)
}

// Killed reports whether a kill retired this identity's trees.
func (p *Publisher) Killed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.st.Sweeps) > 0
}
