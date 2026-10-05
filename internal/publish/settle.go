package publish

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/producer"
	bpub "github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Settlement by rank (spec section 9.5): one mined transaction that spends
// the hold output and pays each rank one P2PKH output to the key BRC-29
// derives for its advertiser (prefix base64 of the entry's commitment,
// suffix base64 of the two-byte rank), with square-root weights summing to
// exactly the hold output's value; the fee from another input of the
// writer's. Before it settles, the writer checks each payee's advertisement
// (or its renewal successor) unspent by positive evidence; a cancelled one
// is dropped and t counted again (storerec.Settle). The settlement is
// signed and recorded before it reaches the settlement leg, so a run that
// stops part way sends the same bytes again, and never builds a second
// settlement for the hold output. Once it mines, each payee is sent its
// remittance, unicast, at its settle route.

// SettleRecord is a settlement made.
type SettleRecord struct {
	Txid   string         `json:"txid"`
	Raw    string         `json:"raw"`
	Bump   string         `json:"bump,omitempty"`
	Height uint32         `json:"height,omitempty"`
	Fee    bwallet.Output `json:"fee"`
	Payees []PayeeRecord  `json:"payees"`
	// Dropped are advertisers that passed and whose advertisement was
	// then cancelled.
	Dropped []string `json:"dropped,omitempty"`
	// Abandoned is set when the network never took this settlement and
	// the hold output went elsewhere.
	Abandoned string `json:"abandoned,omitempty"`
}

// PayeeRecord is one rank of a settlement.
type PayeeRecord struct {
	Rank       int    `json:"rank"`
	Advertiser string `json:"advertiser"`
	Ad         string `json:"ad"`
	Satoshis   uint64 `json:"satoshis"`
	Vout       uint32 `json:"vout"`
	Front      string `json:"front"`
	Remitted   bool   `json:"remitted,omitempty"`
	Refused    string `json:"refused,omitempty"`
	Last       string `json:"last,omitempty"`
}

// ReclaimRecord is a hold output spent back to the writer after its
// deadline.
type ReclaimRecord struct {
	Txid   string         `json:"txid"`
	Raw    string         `json:"raw"`
	Fee    bwallet.Output `json:"fee"`
	Height uint32         `json:"height,omitempty"`
}

// Errors a caller maps to exit codes.
var (
	// ErrNotYet is a settlement or reclaim that cannot be decided now:
	// the race is open, a spend source gave no evidence, or a transaction
	// is on its way. It proves nothing either way.
	ErrNotYet = errors.New("publish: not decidable yet")
	// ErrPastDeadline is a settlement asked for at or after the deadline.
	ErrPastDeadline = errors.New("publish: the deadline has come; the bounty is reclaimed, not settled")
	// ErrBeforeDeadline is a reclaim asked for before the deadline.
	ErrBeforeDeadline = errors.New("publish: the deadline has not come; a bounty is reclaimed only after it")
)

func (p *Publisher) newPayer(ctx context.Context) *producer.Payer {
	pay := &producer.Payer{Pool: p.wallet.Pool, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
		Kept: p.kept, Settler: p.cfg.Settler, Asset: p.cfg.Asset, Fees: p.cfg.Fees, Note: p.note}
	if h, err := p.cfg.Asset.BestHeader(ctx); err == nil {
		pay.Tip = h.Height
	}
	return pay
}

// unspent is a spend source's positive evidence: "" with no error is
// unspent; a txid is spent by it; anything else decides nothing.
func (p *Publisher) unspent(ctx context.Context, txid string, vout uint32) (bool, string, error) {
	by, err := p.cfg.Asset.Spender(ctx, txid, vout)
	if err != nil {
		return false, "", fmt.Errorf("%w: the spend source cannot say whether %s.%d is spent: %v", ErrNotYet, short(txid), vout, err)
	}
	return by == "", by, nil
}

// adChain follows an advertisement through its renewal successors, as far
// as positive evidence goes, into the shape storerec.Settle reads.
func (p *Publisher) adChain(ctx context.Context, txid string, vout uint32, adv []byte, root [32]byte, expiry uint64, depth int) (*storerec.Ad, error) {
	a := &storerec.Ad{Advertiser: adv, Root: root, Expiry: expiry}
	a.Txid, _ = parseDisplay(txid)
	free, by, err := p.unspent(ctx, txid, vout)
	if err != nil {
		return nil, err
	}
	if free {
		return a, nil
	}
	a.Spent = true
	if depth >= 16 {
		return a, nil
	}
	raw, err := p.cfg.Asset.TxRaw(ctx, by)
	if err != nil {
		return nil, fmt.Errorf("%w: the transaction that spends advertisement %s: %v", ErrNotYet, short(txid), err)
	}
	tx, err := guard.ParseTransaction(raw, len(raw))
	if err != nil || tx.TxID().String() != by {
		return nil, fmt.Errorf("%w: the node answered other bytes for %s", ErrNotYet, short(by))
	}
	for i, o := range tx.Outputs {
		if o.LockingScript == nil {
			continue
		}
		ad, err := storerec.DecodeAdvertisement(*o.LockingScript)
		if err != nil || string(ad.Host) != string(adv) || ad.Hash != root || ad.Expiry <= expiry {
			continue
		}
		next, err := p.adChain(ctx, by, uint32(i), adv, root, ad.Expiry, depth+1)
		if err != nil {
			return nil, err
		}
		a.SpentBy = append(a.SpentBy, next)
	}
	return a, nil
}

// Settle settles a race whose race has closed: or resumes a settlement a
// run recorded and did not see through. It returns the record of what it
// did (nil when nothing is paid).
func (p *Publisher) Settle(ctx context.Context, r *Race, force bool) (*SettleRecord, error) {
	if r.Settlement != nil && r.Settlement.Abandoned == "" {
		return r.Settlement, p.resumeSettlement(ctx, r)
	}
	switch r.State {
	case RaceSettled, RaceReclaimd, RaceReclaim, RaceSpent:
		return r.Settlement, nil
	case RaceLapsed:
		return nil, ErrPastDeadline
	}
	e := r.entry
	tip, err := p.cfg.Asset.BestHeader(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoTip, err)
	}
	if uint64(tip.Height) >= e.Hold.Deadline {
		r.State, r.Note = RaceLapsed, "the deadline came before settlement"
		_ = p.saveRace(r)
		return nil, ErrPastDeadline
	}
	passed := r.Passed()
	if !force && uint64(len(passed)) < e.Hold.K && r.State == RaceOpen {
		return nil, fmt.Errorf("%w: %d of K = %d passed; the race is open", ErrNotYet, len(passed), e.Hold.K)
	}
	hold, err := p.holdOf(r)
	if err != nil {
		return nil, err
	}
	free, by, err := p.unspent(ctx, hold.Txid, hold.Vout)
	if err != nil {
		return nil, err
	}
	if !free {
		r.State, r.Note = RaceSpent, "the hold output is spent by "+by+", which this race did not make"
		_ = p.saveRace(r)
		return nil, fmt.Errorf("publish: %s", r.Note)
	}
	// The payees' advertisements, checked unspent with their renewals.
	// In rank order, as far as K live ones: a candidate below them is
	// never asked about.
	var cands []storerec.Candidate
	live := 0
	for _, c := range passed {
		if uint64(live) == e.Hold.K {
			break
		}
		adv, _ := hex.DecodeString(c.Advertiser)
		ad, err := p.adChain(ctx, c.First, c.FirstVout, adv, e.Root, c.FirstExpiry, 0)
		if err != nil {
			return nil, err
		}
		if ad.Live() {
			live++
		}
		cands = append(cands, storerec.Candidate{Ad: ad, FirstSeen: uint64(c.FirstSeen), Passed: true})
	}
	s := storerec.Settle(r.C(), hold.Sats, int(e.Hold.T), int(e.Hold.K), cands)
	for _, d := range s.Dropped {
		k := hex.EncodeToString(d)
		for _, c := range r.Cands {
			if c.Advertiser == k && c.Status == CandPassed {
				c.Status = CandCancelled
			}
		}
	}
	if !s.Settled {
		if r.State == RaceOpen && !force {
			_ = p.saveRace(r)
			return nil, fmt.Errorf("%w: fewer than t = %d live passed advertisers", ErrNotYet, e.Hold.T)
		}
		r.State = RaceClosed
		r.Note = fmt.Sprintf("closed with fewer than t = %d live passed advertisers: nothing is paid", e.Hold.T)
		return nil, p.saveRace(r)
	}
	rec, err := p.buildSettlement(ctx, r, hold, s)
	if err != nil {
		return nil, err
	}
	r.Settlement, r.State = rec, RaceSettling
	r.Note = ""
	if err := p.saveRace(r); err != nil {
		// Not recorded: nothing was sent, and the fee coin goes back.
		_ = p.wallet.Pool.Return(rec.Fee)
		r.Settlement, r.State = nil, RaceOpen
		return nil, err
	}
	return rec, p.resumeSettlement(ctx, r)
}

// buildSettlement signs the settlement transaction: the hold output and a
// fee coin in, one output per paid rank in rank order, change last.
func (p *Publisher) buildSettlement(ctx context.Context, r *Race, hold *HoldRecord, s storerec.Settlement) (*SettleRecord, error) {
	holdTx, err := funding.Rebuild(hold.RawHex, hold.BumpHex, "")
	if err != nil {
		return nil, err
	}
	if int(hold.Vout) >= len(holdTx.Outputs) || holdTx.Outputs[hold.Vout].Satoshis != hold.Sats {
		return nil, errors.New("publish: the hold output recorded is not the transaction's")
	}
	var sum uint64
	for _, py := range s.Payees {
		sum += py.Satoshis
	}
	if sum != hold.Sats {
		return nil, fmt.Errorf("publish: the payouts sum to %d, not the hold output's %d", sum, hold.Sats)
	}
	pay := p.newPayer(ctx)
	fee, err := pay.TakeAtLeast(ctx, p.cfg.Fees.Floor*4)
	if err != nil {
		return nil, feeError(err)
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(holdTx, hold.Vout, storerec.HoldDerivation.Unlocker(ctx, p.wallet, p.wallet.Originator))
	tx.AddInputFromTx(fee.Tx, fee.Vout, fee.Unlocker)
	rec := &SettleRecord{}
	byKey := map[string]*Cand{}
	for _, c := range r.Cands {
		byKey[c.Advertiser] = c
	}
	for _, py := range s.Payees {
		k := hex.EncodeToString(py.Advertiser)
		dest, err := p.signer.PaymentDestination(ctx, k, py.Prefix, py.Suffix)
		if err != nil {
			pay.GiveBack()
			return nil, err
		}
		front := ""
		if c := byKey[k]; c != nil {
			front = c.Front
		}
		rec.Payees = append(rec.Payees, PayeeRecord{Rank: py.Rank, Advertiser: k, Ad: displayOf(py.Ad), Satoshis: py.Satoshis,
			Vout: uint32(len(tx.Outputs)), Front: front})
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: py.Satoshis, LockingScript: dest})
	}
	for _, d := range s.Dropped {
		rec.Dropped = append(rec.Dropped, hex.EncodeToString(d))
	}
	change, err := p.signer.FundScript()
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	feeIn := fee.Tx.Outputs[fee.Vout].Satoshis
	size := uint64(10 + 2*150 + (len(tx.Outputs)+1)*34)
	cost := max(p.cfg.Fees.Floor, p.cfg.Fees.SatPerByte*size)
	if feeIn < cost+p.cfg.Fees.Floor {
		pay.GiveBack()
		return nil, fmt.Errorf("fee input: %d satoshis does not cover the fee %d", feeIn, cost)
	}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: feeIn - cost, LockingScript: change})
	if err := tx.Sign(); err != nil {
		pay.GiveBack()
		return nil, err
	}
	// The payouts are exactly the hold output, whatever was signed.
	var out uint64
	for _, py := range rec.Payees {
		out += tx.Outputs[py.Vout].Satoshis
	}
	if out != hold.Sats {
		pay.GiveBack()
		return nil, errors.New("publish: the settlement's payouts are not the hold output's value")
	}
	rec.Txid, rec.Raw, rec.Fee = tx.TxID().String(), hex.EncodeToString(tx.Bytes()), coinOf(fee)
	return rec, nil
}

// resumeSettlement sees a recorded settlement through: sent (again, the
// same bytes) until it mines, then each payee's remittance.
func (p *Publisher) resumeSettlement(ctx context.Context, r *Race) error {
	s := r.Settlement
	raw, err := hex.DecodeString(s.Raw)
	if err != nil {
		return err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return err
	}
	if s.Bump == "" {
		hold, err := p.holdOf(r)
		if err != nil {
			return err
		}
		mp, height, perr := p.cfg.Asset.Proof(ctx, s.Txid)
		if perr != nil {
			free, by, err := p.unspent(ctx, hold.Txid, hold.Vout)
			if err != nil {
				return err
			}
			if !free && by != s.Txid {
				// Another transaction spent the hold output: this
				// settlement never mines and nobody was told of it.
				s.Abandoned = "the hold output was spent by " + by
				r.State, r.Note = RaceSpent, s.Abandoned
				if f, _, err := p.unspent(ctx, s.Fee.TxID, s.Fee.Vout); err == nil && f {
					_ = p.wallet.Pool.Return(s.Fee)
				}
				return p.saveRace(r)
			}
			if free {
				if err := p.cfg.Settler.Submit(ctx, tx); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
					p.cfg.Logf("settlement %s: sent again: %v", short(s.Txid), err)
				}
			}
			pay := p.newPayer(ctx)
			mp, height, err = pay.Await(ctx, "settlement", tx)
			if err != nil {
				return fmt.Errorf("%w: settlement %s: %v", ErrNotYet, short(s.Txid), err)
			}
		}
		tx.MerklePath = mp
		s.Bump, s.Height = funding.BumpHex(mp), height
		p.newPayer(ctx).Change(tx, height, mp)
		r.State = RaceSettled
		if err := p.saveRace(r); err != nil {
			return err
		}
		p.cfg.Logf("race %s: settlement %s mined at height %d", short(r.Entry), short(s.Txid), height)
	}
	if tx.MerklePath == nil {
		mp, err := transaction.NewMerklePathFromHex(s.Bump)
		if err != nil {
			return err
		}
		tx.MerklePath = mp
	}
	return p.remit(ctx, r, tx)
}

// remit sends each payee not yet told its BRC-29 remittance, unicast, at
// its settle route (spec sections 9.5 and 10.1).
func (p *Publisher) remit(ctx context.Context, r *Race, tx *transaction.Transaction) error {
	s := r.Settlement
	beef, err := atomicWithParents(ctx, p, tx)
	if err != nil {
		return err
	}
	prefix := storerec.DerivationPrefix(r.C())
	left := 0
	for i := range s.Payees {
		py := &s.Payees[i]
		if py.Remitted || py.Refused != "" {
			continue
		}
		if py.Front == "" {
			if f, ok := p.frontOf(r, py.Advertiser); ok {
				py.Front = f
			}
		}
		if py.Front == "" {
			py.Last = "no front to send the remittance to; the payee finds its output from the hold output's spend"
			left++
			continue
		}
		body, _ := json.Marshal(map[string]any{"entry": r.Entry, "rank": py.Rank, "transaction": base64.StdEncoding.EncodeToString(beef),
			"outputIndex": py.Vout, "derivationPrefix": prefix, "derivationSuffix": storerec.DerivationSuffix(py.Rank),
			"senderIdentityKey": p.IdentityHex()})
		call, err := p.Post(ctx, strings.TrimRight(py.Front, "/")+"/settle", body, 0)
		switch {
		case err != nil:
			py.Last = firstLine(err.Error())
			left++
		case call.Status == http.StatusOK:
			py.Remitted, py.Last = true, ""
		case call.Status == http.StatusBadRequest:
			py.Refused = firstLine(string(call.Body))
		default:
			py.Last = fmt.Sprintf("%d %s", call.Status, firstLine(string(call.Body)))
			left++
		}
	}
	if err := p.saveRace(r); err != nil {
		return err
	}
	if left > 0 {
		return fmt.Errorf("%w: %d remittance(s) not yet taken; settle again to resend them", ErrNotYet, left)
	}
	return nil
}

func (p *Publisher) frontOf(r *Race, adv string) (string, bool) {
	for _, c := range r.Cands {
		if c.Advertiser == adv {
			if c.Front != "" {
				return c.Front, true
			}
			return storerec.DerivedFront(c.URL)
		}
	}
	return "", false
}

// coinOf is a fee input as the pool keeps it, so that a coin a recorded
// transaction never spent can go back.
func coinOf(in mint.Input) bwallet.Output {
	o := in.Tx.Outputs[in.Vout]
	c := bwallet.Output{TxID: in.Tx.TxID().String(), Vout: in.Vout, Satoshis: o.Satoshis, LockingScript: o.LockingScript.String(),
		Raw: in.Tx.Hex(), Coinbase: in.Tx.IsCoinbase()}
	if in.Tx.MerklePath != nil {
		c.Bump, c.Height = funding.BumpHex(in.Tx.MerklePath), in.Tx.MerklePath.BlockHeight
	}
	return c
}

// atomicWithParents is a mined transaction as Atomic BEEF.
func atomicWithParents(_ context.Context, _ *Publisher, tx *transaction.Transaction) ([]byte, error) {
	return tx.AtomicBEEF(false)
}

func displayOf(h [32]byte) string {
	var r [32]byte
	for i := range h {
		r[i] = h[31-i]
	}
	return hex.EncodeToString(r[:])
}

// Reclaim spends a race's hold output back to the writer after its
// deadline (spec section 9.6), unless a settlement of it is recorded and
// not proven abandoned: a hold output settles or is reclaimed, never both.
func (p *Publisher) Reclaim(ctx context.Context, r *Race) (*ReclaimRecord, error) {
	if r.Reclaim != nil {
		return r.Reclaim, p.resumeReclaim(ctx, r)
	}
	if r.Settlement != nil && r.Settlement.Abandoned == "" {
		return nil, fmt.Errorf("publish: race %s has a settlement (%s); it is not reclaimed", short(r.Entry), short(r.Settlement.Txid))
	}
	switch r.State {
	case RaceSettled, RaceSettling, RaceReclaimd:
		return nil, fmt.Errorf("publish: race %s is %s", short(r.Entry), r.State)
	}
	tip, err := p.cfg.Asset.BestHeader(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoTip, err)
	}
	if uint64(tip.Height) < r.entry.Hold.Deadline {
		return nil, fmt.Errorf("%w: tip %d, deadline %d", ErrBeforeDeadline, tip.Height, r.entry.Hold.Deadline)
	}
	hold, err := p.holdOf(r)
	if err != nil {
		return nil, err
	}
	rec, err := p.reclaimHold(ctx, hold)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		r.State, r.Note = RaceSpent, "the hold output is already spent"
		return nil, p.saveRace(r)
	}
	r.Reclaim, r.State = rec, RaceReclaim
	if err := p.saveRace(r); err != nil {
		_ = p.wallet.Pool.Return(rec.Fee)
		return nil, err
	}
	return rec, p.resumeReclaim(ctx, r)
}

// reclaimHold signs a spend of a hold output back to the fund key, with a
// fee coin; nil when the output is already spent. The caller records it
// before it is sent.
func (p *Publisher) reclaimHold(ctx context.Context, hold *HoldRecord) (*ReclaimRecord, error) {
	free, _, err := p.unspent(ctx, hold.Txid, hold.Vout)
	if err != nil {
		return nil, err
	}
	if !free {
		return nil, nil
	}
	holdTx, err := funding.Rebuild(hold.RawHex, hold.BumpHex, "")
	if err != nil {
		return nil, err
	}
	pay := p.newPayer(ctx)
	fee, err := pay.TakeAtLeast(ctx, p.cfg.Fees.Floor*4)
	if err != nil {
		return nil, feeError(err)
	}
	change, err := p.signer.FundScript()
	if err != nil {
		pay.GiveBack()
		return nil, err
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(holdTx, hold.Vout, storerec.HoldDerivation.Unlocker(ctx, p.wallet, p.wallet.Originator))
	tx.AddInputFromTx(fee.Tx, fee.Vout, fee.Unlocker)
	in := hold.Sats + fee.Tx.Outputs[fee.Vout].Satoshis
	cost := max(p.cfg.Fees.Floor, p.cfg.Fees.SatPerByte*uint64(10+2*150+34))
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: in - cost, LockingScript: change})
	if err := tx.Sign(); err != nil {
		pay.GiveBack()
		return nil, err
	}
	return &ReclaimRecord{Txid: tx.TxID().String(), Raw: hex.EncodeToString(tx.Bytes()), Fee: coinOf(fee)}, nil
}

func (p *Publisher) resumeReclaim(ctx context.Context, r *Race) error {
	rc := r.Reclaim
	if rc.Height > 0 {
		return nil
	}
	hold, err := p.holdOf(r)
	if err != nil {
		return err
	}
	mp, height, err := p.sendSpend(ctx, "reclaim", rc.Raw, rc.Txid, hold, rc.Fee)
	if err != nil {
		if errors.Is(err, errSpentElsewhere) {
			r.State, r.Note = RaceSpent, err.Error()
			return p.saveRace(r)
		}
		return err
	}
	rc.Height = height
	_ = mp
	r.State = RaceReclaimd
	return p.saveRace(r)
}

var errSpentElsewhere = errors.New("publish: the hold output was spent by another transaction")

// sendSpend sends a recorded spend of a hold output (the same bytes again
// on a later run) and waits for it to mine; its change goes to the pool.
func (p *Publisher) sendSpend(ctx context.Context, what, rawHex, txid string, hold *HoldRecord, fee bwallet.Output) (*transaction.MerklePath, uint32, error) {
	raw, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil, 0, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, 0, err
	}
	if mp, height, err := p.cfg.Asset.Proof(ctx, txid); err == nil {
		tx.MerklePath = mp
		p.newPayer(ctx).Change(tx, height, mp)
		return mp, height, nil
	}
	free, by, err := p.unspent(ctx, hold.Txid, hold.Vout)
	if err != nil {
		return nil, 0, err
	}
	if !free && by != txid {
		if f, _, err := p.unspent(ctx, fee.TxID, fee.Vout); err == nil && f {
			_ = p.wallet.Pool.Return(fee)
		}
		return nil, 0, fmt.Errorf("%w: %s", errSpentElsewhere, by)
	}
	if free {
		if err := p.cfg.Settler.Submit(ctx, tx); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
			p.cfg.Logf("%s %s: sent: %v", what, short(txid), err)
		}
	}
	mp, height, err := p.newPayer(ctx).Await(ctx, what, tx)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %s %s: %v", ErrNotYet, what, short(txid), err)
	}
	tx.MerklePath = mp
	p.newPayer(ctx).Change(tx, height, mp)
	return mp, height, nil
}

// UnusedHold is a hold output minted ahead that no entry took.
type UnusedHold struct {
	Hold HoldRecord
	Txid string
}

// ReclaimUnused spends every mined hold output no entry took back to the
// fund key: each is marked taken, and saved, before it is spent, so no put
// can name it. It returns the reclaims made.
func (p *Publisher) ReclaimUnused(ctx context.Context) ([]string, error) {
	p.mu.Lock()
	var todo []HoldRecord
	for i := range p.st.Holds {
		h := &p.st.Holds[i]
		if !h.Used && h.BumpHex != "" {
			h.Used = true
			h.Entry = "reclaimed"
			todo = append(todo, *h)
		}
	}
	err := p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	var done []string
	for i := range todo {
		h := todo[i]
		rec, err := p.reclaimHold(ctx, &h)
		if err != nil {
			return done, err
		}
		if rec == nil {
			continue
		}
		if err := p.recordUnusedReclaim(h, rec); err != nil {
			_ = p.wallet.Pool.Return(rec.Fee)
			return done, err
		}
		if _, _, err := p.sendSpend(ctx, "reclaim", rec.Raw, rec.Txid, &h, rec.Fee); err != nil {
			return done, err
		}
		done = append(done, rec.Txid)
	}
	return done, nil
}

func (p *Publisher) recordUnusedReclaim(h HoldRecord, rec *ReclaimRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Holds {
		x := &p.st.Holds[i]
		if x.Txid == h.Txid && x.Vout == h.Vout {
			x.Entry = "reclaimed by " + rec.Txid
		}
	}
	return p.saveLocked()
}

// HoldScriptOf is the hold output's locking script for this identity.
func (p *Publisher) HoldScriptOf() (*script.Script, error) {
	key, err := storerec.LockingKey(storerec.HoldDerivation, p.Identity())
	if err != nil {
		return nil, err
	}
	s := script.Script(storerec.HoldScript(key))
	return &s, nil
}

// NewPayer is a Payer over the identity's pool and settlement leg.
func (p *Publisher) NewPayer(ctx context.Context) *producer.Payer { return p.newPayer(ctx) }

// Unspent is a spend source's positive evidence about an output: true
// only for "" with no error; by names the spender; an error decides
// nothing (ErrNotYet).
func (p *Publisher) Unspent(ctx context.Context, txid string, vout uint32) (bool, string, error) {
	return p.unspent(ctx, txid, vout)
}

// CoinOf is a fee input as the pool keeps it.
func CoinOf(in mint.Input) bwallet.Output { return coinOf(in) }

// ReturnCoin puts a fee coin a recorded transaction never spent back in
// the pool, when the spend source shows it unspent.
func (p *Publisher) ReturnCoin(ctx context.Context, c bwallet.Output) {
	if c.TxID == "" {
		return
	}
	if free, _, err := p.unspent(ctx, c.TxID, c.Vout); err == nil && free {
		_ = p.wallet.Pool.Return(c)
	}
}

// Logf writes to the identity's log.
func (p *Publisher) Logf(format string, args ...any) { p.cfg.Logf(format, args...) }

// Settler is the identity's settlement leg.
func (p *Publisher) Settler() bpub.Settler { return p.cfg.Settler }

// Fees is the identity's fee policy.
func (p *Publisher) Fees() mint.Fees { return p.cfg.Fees }

// Now is the identity's clock.
func (p *Publisher) Now() time.Time { return p.cfg.Now() }
