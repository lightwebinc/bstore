package publish

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/funding"
)

// Remitting on the network's acceptance (payment-validation, row 15): a
// settlement whose largest payout is at or below the acceptance threshold
// is remitted as soon as arcade says the network took it and the node
// shows its inputs spent by it alone, without waiting for its block. The
// race is then RaceRemitted: remitted, not yet mined. Its proof is
// collected by a later settle, which marks it settled. Each payee decides
// its own output on its side (it may hold one above its own threshold
// until it mines). A larger settlement, a writer with no status source, or
// one the network has not clearly taken waits for its block as before.

// remitUnmined remits a sent settlement before it mines when it may; done
// is set when it did (or had), and the caller does not wait for the block.
func (p *Publisher) remitUnmined(ctx context.Context, r *Race, tx *transaction.Transaction) (bool, error) {
	s := r.Settlement
	if r.State != RaceRemitted {
		if p.cfg.Status == nil || p.cfg.Asset == nil {
			return false, nil
		}
		if r.RemittedBefore() {
			// A settlement of this race was remitted unmined and never
			// mined: its payees were told of a payment they never got, and
			// hold this writer for confirmation. The one built in its
			// place is remitted only once it mines.
			p.cfg.Logf("settlement %s: an earlier settlement of this race was remitted and never mined; remitted once mined", short(s.Txid))
			return false, nil
		}
		var largest uint64
		for _, py := range s.Payees {
			largest = max(largest, py.Satoshis)
		}
		v := p.cfg.Accept.Decide(ctx, acceptance.NewExposure(), acceptance.Request{Payer: p.IdentityHex(), Txid: s.Txid, Sats: largest})
		if v.Decision != acceptance.Fast {
			p.cfg.Logf("settlement %s: largest payout %d sat: %s; remitted once mined", short(s.Txid), largest, v.Reason)
			return false, nil
		}
		if why := p.networkTook(ctx, tx); why != "" {
			p.cfg.Logf("settlement %s: %s; remitted once mined", short(s.Txid), why)
			return false, nil
		}
	}
	if err := p.withSources(r, tx); err != nil {
		p.cfg.Logf("settlement %s: its parents cannot be attached (%v); remitted once mined", short(s.Txid), err)
		return false, nil
	}
	if r.State != RaceRemitted {
		r.State = RaceRemitted
		s.RemittedAt = p.cfg.Now().Unix()
		if err := p.saveRace(r); err != nil {
			return true, err
		}
		p.cfg.Logf("race %s: settlement %s taken by the network; remitted before it mines", short(r.Entry), short(s.Txid))
	}
	return true, p.remit(ctx, r, tx)
}

// networkTook is "" once arcade answers tx accepted, with no conflict, and
// the node shows each input unspent or spent by tx; else why not.
func (p *Publisher) networkTook(ctx context.Context, tx *transaction.Transaction) string {
	txid := tx.TxID().String()
	wait, poll := p.cfg.Accept.Wait, p.cfg.Accept.Poll
	if wait <= 0 {
		wait = acceptance.DefaultWait
	}
	if poll <= 0 {
		poll = acceptance.DefaultPoll
	}
	deadline := time.Now().Add(wait)
	for {
		st, err := p.cfg.Status.Status(ctx, txid)
		if err == nil && st != nil {
			switch {
			case st.TxStatus == "REJECTED":
				return "arcade refused it: " + st.Why()
			case st.TxStatus == "DOUBLE_SPEND_ATTEMPTED" || len(st.CompetingTxs) > 0:
				return "a double spend is attempted: " + st.Why()
			case st.Accepted():
				for i, in := range tx.Inputs {
					if in.SourceTXID == nil {
						return fmt.Sprintf("input %d names no source", i)
					}
					by, err := p.cfg.Asset.Spender(ctx, in.SourceTXID.String(), in.SourceTxOutIndex)
					if err != nil {
						return "the node's spend view cannot say: " + err.Error()
					}
					if by != "" && !strings.EqualFold(by, txid) {
						return fmt.Sprintf("input %d is spent by %s", i, by)
					}
				}
				return ""
			}
		}
		if time.Now().After(deadline) {
			return "arcade had not answered it accepted"
		}
		select {
		case <-ctx.Done():
			return ctx.Err().Error()
		case <-time.After(poll):
		}
	}
}

// withSources attaches the settlement's proven parents (the hold
// transaction and the fee coin's), so its Atomic BEEF verifies before it
// mines.
func (p *Publisher) withSources(r *Race, tx *transaction.Transaction) error {
	if tx.MerklePath != nil || len(tx.Inputs) < 2 {
		return nil
	}
	hold, err := p.holdOf(r)
	if err != nil {
		return err
	}
	holdTx, err := funding.Rebuild(hold.RawHex, hold.BumpHex, "")
	if err != nil {
		return err
	}
	fee := r.Settlement.Fee
	if fee.Raw == "" || fee.Bump == "" {
		return errors.New("the fee coin's parent is not proven")
	}
	feeTx, err := funding.Rebuild(fee.Raw, fee.Bump, "")
	if err != nil {
		return err
	}
	for i, src := range []*transaction.Transaction{holdTx, feeTx} {
		in := tx.Inputs[i]
		if in.SourceTXID == nil || !in.SourceTXID.Equal(*src.TxID()) {
			return fmt.Errorf("input %d is not the recorded parent's", i)
		}
		in.SourceTransaction = src
	}
	return nil
}

// RemittedBefore reports whether a settlement of this race was remitted
// before mining and then abandoned (it could never mine).
func (r *Race) RemittedBefore() bool {
	for _, a := range r.Abandoned {
		if a.RemittedAt != 0 {
			return true
		}
	}
	return false
}

// RemittedStale is how long a settlement may stay remitted and unmined
// before it is reported: a block is expected within minutes, so an hour
// means the node does not serve its proof, or it will never mine.
const RemittedStale = time.Hour

// StaleRemitted counts this identity's races remitted before mining whose
// proof the node has not served for at least age, and the oldest's age.
// A race recorded before RemittedAt existed counts from its start.
func (p *Publisher) StaleRemitted(age time.Duration) (n int, oldest time.Duration) {
	races, err := p.Races()
	if err != nil {
		return 0, 0
	}
	now := p.cfg.Now()
	for _, r := range races {
		if a, ok := r.RemittedAge(now); ok && a >= age {
			n++
			oldest = max(oldest, a)
		}
	}
	return n, oldest
}

// RemittedAge is how long the race has been remitted and unmined.
func (r *Race) RemittedAge(now time.Time) (time.Duration, bool) {
	if r.State != RaceRemitted || r.Settlement == nil {
		return 0, false
	}
	at := r.Settlement.RemittedAt
	if at == 0 {
		at = r.Started
	}
	return now.Sub(time.Unix(at, 0)), true
}
