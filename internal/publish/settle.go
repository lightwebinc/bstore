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

	"github.com/lightwebinc/bstore/storerec"
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
	// Spent is the transaction the node shows spending this payee's
	// output: only the payee's key can, so the payee has its share
	// whoever told it (a bridged incumbent learns of it from its front,
	// which forwards the settlement, spec C28).
	Spent string `json:"spent,omitempty"`
}

// Done is a payee nothing more is owed: remitted, refused, or its output
// already spent by the payee.
func (py *PayeeRecord) Done() bool { return py.Remitted || py.Refused != "" || py.Spent != "" }

// Forwarder is the front that answered for an advertiser in the race when
// that front is not the advertiser itself (a bridge in front of an
// incumbent UHRP host). Such a front forwards the settlement to the
// advertiser (C28); the advertiser's own front has no settle route.
func (r *Race) Forwarder(adv string) string {
	for _, c := range r.Cands {
		if c.Advertiser == adv && c.Status == CandPassed && c.Answerer != "" && c.Answerer != adv {
			return c.Front
		}
	}
	return ""
}

// CheckPayouts asks the node, for each payee of a mined settlement not yet
// done, whether its output is spent, and records a spend. It saves the
// race when anything changed. A spend source that cannot answer decides
// nothing.
func (p *Publisher) CheckPayouts(ctx context.Context, r *Race) error {
	s := r.Settlement
	if s == nil || s.Bump == "" {
		return nil
	}
	changed := false
	for i := range s.Payees {
		py := &s.Payees[i]
		if py.Done() {
			continue
		}
		unspent, by, err := p.unspent(ctx, s.Txid, py.Vout)
		if err != nil || unspent {
			continue
		}
		py.Spent, py.Last = by, ""
		changed = true
		p.cfg.Logf("race %s: rank %d's output %s.%d is spent by %s: the payee has its share", short(r.Entry), py.Rank, short(s.Txid), py.Vout, short(by))
	}
	if !changed {
		return nil
	}
	return p.saveRace(r)
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
		Kept: p.kept, Settler: p.cfg.Settler, Asset: p.cfg.Asset, Fees: p.cfg.Fees, Note: p.note, Poll: p.cfg.Poll}
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

// maxAdHops bounds the renewals followed from one advertisement: a chain
// longer than that is taken as not live, and its candidate dropped.
const maxAdHops = 16

// adChain follows an advertisement through its renewal successors, as far
// as positive evidence goes, into the shape storerec.Settle reads: one
// successor a spend (the first output of the spending transaction that is
// an advertisement of the same key for the same root with a later expiry,
// which is the one storerec.Ad.Live follows), each outpoint asked once,
// at most maxAdHops of them.
func (p *Publisher) adChain(ctx context.Context, txid string, vout uint32, adv []byte, root [32]byte, expiry uint64, _ int) (*storerec.Ad, error) {
	first := &storerec.Ad{Advertiser: adv, Root: root, Expiry: expiry, Vout: vout}
	first.Txid, _ = parseDisplay(txid)
	cur := first
	seen := map[string]bool{}
	for hop := 0; ; hop++ {
		op := fmt.Sprintf("%s.%d", txid, vout)
		if seen[op] || hop >= maxAdHops {
			cur.Spent = true
			return first, nil
		}
		seen[op] = true
		free, by, err := p.unspent(ctx, txid, vout)
		if err != nil {
			return nil, err
		}
		if free {
			return first, nil
		}
		cur.Spent = true
		raw, err := p.cfg.Asset.TxRaw(ctx, by)
		if err != nil {
			return nil, fmt.Errorf("%w: the transaction that spends advertisement %s: %v", ErrNotYet, short(txid), err)
		}
		tx, err := guard.ParseTransaction(raw, len(raw))
		if err != nil || tx.TxID().String() != by {
			return nil, fmt.Errorf("%w: the node answered other bytes for %s", ErrNotYet, short(by))
		}
		var next *storerec.Ad
		for i, o := range tx.Outputs {
			if o.LockingScript == nil {
				continue
			}
			ad, err := storerec.DecodeAdvertisement(*o.LockingScript)
			if err != nil || string(ad.Host) != string(adv) || ad.Hash != root || ad.Expiry <= cur.Expiry {
				continue
			}
			next = &storerec.Ad{Advertiser: adv, Root: root, Expiry: ad.Expiry, Vout: uint32(i)}
			next.Txid, _ = parseDisplay(by)
			txid, vout = by, uint32(i)
			break
		}
		if next == nil {
			return first, nil
		}
		cur.SpentBy = []*storerec.Ad{next}
		cur = next
	}
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
	if !force && !r.Closing && uint64(len(passed)) < e.Hold.K && r.State == RaceOpen {
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
		if r.State == RaceOpen && !force && !r.Closing {
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
		// The remittance goes to the advertiser's own route, derived from
		// its advertisement, never to a front that answered for it.
		front := ""
		if c := byKey[k]; c != nil {
			front, _ = storerec.DerivedFront(c.URL)
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
		mp, height, perr := p.cfg.Asset.Proof(ctx, s.Txid)
		if perr != nil {
			cf, err := p.SpentElsewhere(ctx, tx)
			if err != nil {
				return err
			}
			switch {
			case cf != nil && cf.Same:
				// A miner re-encoded it: the same settlement, under
				// another txid.
				p.cfg.Logf("settlement %s was mined re-encoded as %s", short(s.Txid), short(cf.By.TxID().String()))
				tx = cf.By
				s.Txid, s.Raw = tx.TxID().String(), hex.EncodeToString(tx.Bytes())
				if err := p.saveRace(r); err != nil {
					return err
				}
			case cf != nil && cf.Input == 0:
				// Another transaction spent the hold output: this
				// settlement never mines and nobody was told of it.
				s.Abandoned = "the hold output was spent by " + cf.By.TxID().String()
				r.State, r.Note = RaceSpent, s.Abandoned
				p.ReturnCoin(ctx, s.Fee)
				return p.saveRace(r)
			case cf != nil:
				// Its fee coin went elsewhere: it can never mine. It is set
				// aside and the next settle builds it again on another
				// coin; both spend the one hold output, so at most one of
				// them ever pays.
				s.Abandoned = "its fee input was spent by " + cf.By.TxID().String()
				r.Abandoned = append(r.Abandoned, *s)
				r.Settlement, r.State = nil, RaceOpen
				r.Closing = true
				if err := p.saveRace(r); err != nil {
					return err
				}
				return fmt.Errorf("%w: settlement %s can never mine (%s); settle again builds it anew", ErrNotYet, short(s.Txid), s.Abandoned)
			default:
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
	if err := p.CheckPayouts(ctx, r); err != nil {
		return err
	}
	left := 0
	for i := range s.Payees {
		py := &s.Payees[i]
		if py.Done() {
			continue
		}
		// A payee whose front forwards for it still gets its remittance
		// offered at its own route, which an incumbent does not serve; it
		// is not counted as owed, since its front forwards the settlement.
		fwd := r.Forwarder(py.Advertiser)
		if py.Front == "" {
			if f, ok := p.frontOf(r, py.Advertiser); ok {
				py.Front = f
			}
		}
		if py.Front == "" {
			py.Last = "no front to send the remittance to; the payee finds its output from the hold output's spend"
			if fwd == "" {
				left++
			} else {
				py.Last = ForwardedNote(fwd)
			}
			continue
		}
		body, _ := json.Marshal(map[string]any{"entry": r.Entry, "rank": py.Rank, "transaction": base64.StdEncoding.EncodeToString(beef),
			"outputIndex": py.Vout, "derivationPrefix": prefix, "derivationSuffix": storerec.DerivationSuffix(py.Rank),
			"senderIdentityKey": p.IdentityHex()})
		call, err := p.Post(ctx, strings.TrimRight(py.Front, "/")+"/settle", body, 0)
		switch {
		case err != nil:
			py.Last = firstLine(err.Error())
		case call.Status == http.StatusOK:
			py.Remitted, py.Last = true, ""
		case call.Status == http.StatusBadRequest:
			py.Refused = firstLine(string(call.Body))
		default:
			py.Last = fmt.Sprintf("%d %s", call.Status, firstLine(string(call.Body)))
		}
		if !py.Done() {
			if fwd != "" {
				py.Last = ForwardedNote(fwd)
			} else {
				left++
			}
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

// ForwardedNote says why a forwarded payee's remittance is not owed.
func ForwardedNote(front string) string {
	return "forwarded by the front at " + front + ", which answered for the advertiser; done once the payee spends its output"
}

func (p *Publisher) frontOf(r *Race, adv string) (string, bool) {
	for _, c := range r.Cands {
		if c.Advertiser == adv {
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
	if err := refuseMixed(tx); err != nil {
		pay.GiveBack()
		return nil, err
	}
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
	_, height, adopted, err := p.sendSpend(ctx, "reclaim", rc.Raw, rc.Txid)
	switch {
	case errors.Is(err, errSpentElsewhere):
		r.State, r.Note = RaceSpent, err.Error()
		p.ReturnCoin(ctx, rc.Fee)
		return p.saveRace(r)
	case errors.Is(err, errStranded):
		// Its fee coin went elsewhere: built again on the next reclaim.
		r.Reclaim, r.State = nil, RaceLapsed
		if serr := p.saveRace(r); serr != nil {
			return serr
		}
		return fmt.Errorf("%w: %v; reclaim again builds it anew", ErrNotYet, err)
	case err != nil:
		return err
	}
	if adopted != nil {
		rc.Txid, rc.Raw = adopted.TxID().String(), hex.EncodeToString(adopted.Bytes())
	}
	rc.Height = height
	r.State = RaceReclaimd
	return p.saveRace(r)
}

var (
	errSpentElsewhere = errors.New("publish: the hold output was spent by another transaction")
	errStranded       = errors.New("publish: a fee input was spent by another transaction")
)

// sendSpend sends a recorded spend of a hold output (its first input; the
// same bytes again on a later run) and waits for it to mine; its change
// goes to the pool. The hold output spent by another transaction is
// errSpentElsewhere, unless that transaction is this one re-encoded, which
// is adopted and returned; the fee input spent elsewhere is errStranded.
func (p *Publisher) sendSpend(ctx context.Context, what, rawHex, txid string) (*transaction.MerklePath, uint32, *transaction.Transaction, error) {
	raw, err := hex.DecodeString(rawHex)
	if err != nil {
		return nil, 0, nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, 0, nil, err
	}
	if mp, height, err := p.cfg.Asset.Proof(ctx, txid); err == nil {
		tx.MerklePath = mp
		p.newPayer(ctx).Change(tx, height, mp)
		return mp, height, nil, nil
	}
	var adopted *transaction.Transaction
	cf, err := p.SpentElsewhere(ctx, tx)
	switch {
	case err != nil:
		return nil, 0, nil, err
	case cf != nil && cf.Same:
		adopted, tx = cf.By, cf.By
	case cf != nil && cf.Input == 0:
		return nil, 0, nil, fmt.Errorf("%w: %s", errSpentElsewhere, cf.By.TxID())
	case cf != nil:
		return nil, 0, nil, fmt.Errorf("%w: %s", errStranded, cf.By.TxID())
	default:
		if err := p.cfg.Settler.Submit(ctx, tx); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
			p.cfg.Logf("%s %s: sent: %v", what, short(txid), err)
		}
	}
	mp, height, err := p.newPayer(ctx).Await(ctx, what, tx)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("%w: %s %s: %v", ErrNotYet, what, short(txid), err)
	}
	tx.MerklePath = mp
	p.newPayer(ctx).Change(tx, height, mp)
	return mp, height, adopted, nil
}

// ReclaimUnused spends back to the fund key every mined hold output no
// entry names: one minted ahead and never taken, and one a put took and
// then failed before its entry was made. Each is marked taken, and its
// reclaim recorded, raw transaction and all, before it is sent; one a run
// left on its way is sent again; an error on one does not stop the rest.
// It returns the reclaims seen mined.
func (p *Publisher) ReclaimUnused(ctx context.Context) ([]string, error) {
	p.mu.Lock()
	var todo []HoldRecord
	for _, h := range p.st.Holds {
		free := !h.Used || h.Entry == ""
		if h.BumpHex != "" && (free || (h.Reclaim != nil && h.Reclaim.Height == 0)) {
			todo = append(todo, h)
		}
	}
	p.mu.Unlock()
	var done []string
	var errs []error
	for i := range todo {
		h := todo[i]
		if h.Reclaim == nil {
			if err := p.updateHold(h, func(x *HoldRecord) { x.Used, x.Entry = true, "reclaiming" }); err != nil {
				errs = append(errs, err)
				continue
			}
			rec, err := p.reclaimHold(ctx, &h)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if rec == nil {
				_ = p.updateHold(h, func(x *HoldRecord) { x.Entry = "spent elsewhere" })
				continue
			}
			if err := p.updateHold(h, func(x *HoldRecord) { x.Reclaim = rec }); err != nil {
				_ = p.wallet.Pool.Return(rec.Fee)
				errs = append(errs, err)
				continue
			}
			h.Reclaim = rec
		}
		rc := h.Reclaim
		_, height, adopted, err := p.sendSpend(ctx, "reclaim", rc.Raw, rc.Txid)
		switch {
		case errors.Is(err, errSpentElsewhere):
			p.ReturnCoin(ctx, rc.Fee)
			_ = p.updateHold(h, func(x *HoldRecord) { x.Entry, x.Reclaim = "spent elsewhere", nil })
			continue
		case errors.Is(err, errStranded):
			_ = p.updateHold(h, func(x *HoldRecord) { x.Reclaim = nil })
			errs = append(errs, fmt.Errorf("%w: hold %s.%d: %v", ErrNotYet, short(h.Txid), h.Vout, err))
			continue
		case err != nil:
			errs = append(errs, err)
			continue
		}
		if adopted != nil {
			rc.Txid, rc.Raw = adopted.TxID().String(), hex.EncodeToString(adopted.Bytes())
		}
		rc.Height = height
		_ = p.updateHold(h, func(x *HoldRecord) { x.Reclaim, x.Entry = rc, "reclaimed by "+rc.Txid })
		done = append(done, rc.Txid)
	}
	return done, errors.Join(errs...)
}

func (p *Publisher) updateHold(h HoldRecord, f func(*HoldRecord)) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.st.Holds {
		x := &p.st.Holds[i]
		if x.Txid == h.Txid && x.Vout == h.Vout {
			f(x)
		}
	}
	return p.saveLocked()
}

// releaseHold gives a hold output a put took back for the next put, when
// no entry names it.
func (p *Publisher) releaseHold(h *HoldRecord) {
	_ = p.updateHold(*h, func(x *HoldRecord) {
		if x.Entry == "" {
			x.Used = false
		}
	})
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
