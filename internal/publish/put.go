package publish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Bounty is the hold terms a put asks for (spec section 4.3), its hold
// output minted by the publisher before the entry.
type Bounty struct {
	Satoshis       uint64
	T, K           uint64
	Retention      uint64 // seconds
	DeadlineBlocks uint64 // ahead of the tip when the entry is made
	CheckC         uint64
}

// Fields are what an entry says beyond the closure.
type Fields struct {
	Bucket      string
	ID          [32]byte
	Key         string
	ContentType string
	ETag        *string
	Meta        []storerec.MetaPair
	Created     time.Time
}

// Object is the closure a put names.
type Object struct {
	Root        [32]byte
	Size        uint64
	ContentHash [32]byte
	ProofRoot   [32]byte
	// Closure is where the pieces come from; nil publishes no pieces (a
	// copy within one bucket names a root its providers already hold).
	Closure Closure
}

// Receipt is an entry made.
type Receipt struct {
	Entry  *storerec.Entry
	Record []byte
	// C is the entry's commitment, hash byte order; Txid the same in
	// display order.
	C    [32]byte
	Txid string
	// Sent is whether the entry counts as sent (spec section 14): the
	// facade took it, or a quorum of the hosts. What a target did not take
	// is sent again from the outbox either way.
	Sent bool
	// Pieces is how many piece carriers were minted.
	Pieces int
}

// ErrNotSent is a put whose entry no facade or host quorum took while the
// request waited; the outbox keeps sending it.
var ErrNotSent = errors.New("publish: the entry was minted but no facade or host quorum took it yet")

// Put publishes obj under f: a hold output first when b asks for one, then
// the closure's pieces on the bucket's blob topic (unless obj.Closure is
// nil), then a put entry. It returns once the entry is sent.
func (p *Publisher) Put(ctx context.Context, f Fields, obj Object, b *Bounty) (*Receipt, error) {
	e := &storerec.Entry{Bucket: f.Bucket, ID: f.ID, Writer: p.Identity(), Key: f.Key, Kind: storerec.KindPut,
		Created: uint64(f.Created.UnixMilli()), Size: obj.Size, Root: obj.Root, ContentHash: obj.ContentHash,
		ProofRoot: obj.ProofRoot, ProofProfile: 1, ContentType: f.ContentType, ETag: f.ETag, Meta: f.Meta}
	if f.Created.IsZero() {
		e.Created = uint64(p.cfg.Now().UnixMilli())
	}
	// The entry's own rules before anything is spent.
	if _, err := e.Encode(); err != nil {
		return nil, err
	}
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	var hold *HoldRecord
	if b != nil && obj.Size > 0 {
		h, err := p.holdOutput(ctx, f.Bucket, b.Satoshis)
		if err != nil {
			return nil, fmt.Errorf("hold output: %w", err)
		}
		hold = h
		tip, err := p.cfg.Asset.BestHeader(ctx)
		if err != nil {
			return nil, err
		}
		txid, _ := parseDisplay(h.Txid)
		e.Hold = &storerec.Hold{Txid: txid, Vout: uint64(h.Vout), Satoshis: h.Sats, T: b.T, K: b.K,
			Retention: b.Retention, Deadline: uint64(tip.Height) + b.DeadlineBlocks, CheckC: b.CheckC}
	}
	rec, err := e.Encode()
	if err != nil {
		return nil, err
	}
	n := 0
	if obj.Closure != nil {
		if n, err = p.publishPieces(ctx, f.Bucket, obj.Root, obj.Closure); err != nil {
			return nil, fmt.Errorf("pieces: %w", err)
		}
	}
	r, err := p.publishEntry(ctx, e, rec, hold)
	if r != nil {
		r.Pieces = n
	}
	return r, err
}

// Delete publishes a delete entry for key (spec section 16.2).
func (p *Publisher) Delete(ctx context.Context, bucket string, id [32]byte, key string, created time.Time) (*Receipt, error) {
	if created.IsZero() {
		created = p.cfg.Now()
	}
	e := &storerec.Entry{Bucket: bucket, ID: id, Writer: p.Identity(), Key: key, Kind: storerec.KindDelete,
		Created: uint64(created.UnixMilli())}
	rec, err := e.Encode()
	if err != nil {
		return nil, err
	}
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	return p.publishEntry(ctx, e, rec, nil)
}

// publishPieces mints one piece carrier per distinct object of the closure
// and delivers each as it is made: memory holds one object and one
// carrier. The tree outputs are recorded used before the first is signed.
func (p *Publisher) publishPieces(ctx context.Context, bucket string, root [32]byte, c Closure) (int, error) {
	objs := c.Objects()
	targets := p.cfg.Delivery.pieceTargets(bucket)
	if len(targets) == 0 {
		return 0, errors.New("no facade and no provider to deliver pieces to")
	}
	params := carrierParams(storerec.PieceDerivation)
	done := 0
	for done < len(objs) {
		batch := min(len(objs)-done, p.cfg.PieceTree, mint.MaxFundingOutputs)
		tree, vout, err := p.spend(ctx, kindPiece, uint32(batch))
		if err != nil {
			return done, err
		}
		for i := 0; i < batch; i++ {
			h := objs[done+i]
			data, err := c.Object(ctx, h)
			if err != nil {
				return done + i, fmt.Errorf("object %x: %w", h[:6], err)
			}
			pc := &storerec.Piece{Bucket: bucket, Writer: p.Identity(), Root: root, Piece: data}
			rec, err := pc.Encode()
			if err != nil {
				return done + i, err
			}
			tx, err := carrier.Mint(ctx, p.wallet, p.wallet.Originator, params, rec, tree, vout+uint32(i))
			if err != nil {
				return done + i, err
			}
			beef, err := tx.AtomicBEEF(false)
			if err != nil {
				return done + i, err
			}
			it, err := p.outbox.add(tx.TxID().String(), "piece", beef, targets)
			if err != nil {
				return done + i, err
			}
			if ok := p.outbox.send(ctx, it, beef); len(ok) == 0 {
				// Every target failed: the next put would only add to the
				// backlog, so this one stops here. Its pieces stay in the
				// outbox and are sent again.
				return done + i + 1, fmt.Errorf("piece %s reached no target", short(it.ID))
			}
		}
		done += batch
	}
	return done, nil
}

// publishEntry mints the entry carrier on an entry tree output recorded
// used first, writes it to the outbox, and sends it.
func (p *Publisher) publishEntry(ctx context.Context, e *storerec.Entry, rec []byte, hold *HoldRecord) (*Receipt, error) {
	targets := p.cfg.Delivery.recordTargets(e.Bucket, true)
	if len(targets) == 0 {
		return nil, errors.New("no facade, host or provider to deliver the entry to")
	}
	tree, vout, err := p.spend(ctx, kindEntry, 1)
	if err != nil {
		return nil, err
	}
	tx, err := carrier.Mint(ctx, p.wallet, p.wallet.Originator, carrierParams(storerec.EntryDerivation), rec, tree, vout)
	if err != nil {
		return nil, err
	}
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return nil, err
	}
	r := &Receipt{Entry: e, Record: rec, C: carrier.Commitment(tx), Txid: tx.TxID().String()}
	if hold != nil {
		p.mu.Lock()
		for i := range p.st.Holds {
			if p.st.Holds[i].Txid == hold.Txid && p.st.Holds[i].Vout == hold.Vout {
				p.st.Holds[i].Entry = r.Txid
			}
		}
		err := p.saveLocked()
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	it, err := p.outbox.add(r.Txid, "entry", beef, targets)
	if err != nil {
		return nil, err
	}
	r.Sent = p.cfg.Delivery.sent(p.outbox.send(ctx, it, beef))
	if !r.Sent {
		return r, ErrNotSent
	}
	return r, nil
}

// holdOutput mints and mines a hold output of sats (spec section 9.2) and
// delivers it, alone with its proof, to the records topic. It is recorded
// in the state before it reaches the settlement leg.
func (p *Publisher) holdOutput(ctx context.Context, bucket string, sats uint64) (*HoldRecord, error) {
	key, err := storerec.LockingKey(storerec.HoldDerivation, p.Identity())
	if err != nil {
		return nil, err
	}
	s := script.Script(storerec.HoldScript(key))
	if h, err := p.cfg.Asset.BestHeader(ctx); err == nil {
		p.payer.Tip = h.Height
	}
	fee, err := p.payer.TakeAtLeast(ctx, sats+p.cfg.Fees.Floor*4)
	if err != nil {
		return nil, feeError(err)
	}
	change, err := p.signer.FundScript()
	if err != nil {
		p.payer.GiveBack()
		return nil, err
	}
	tx, err := mint.Transition(&s, sats, nil, fee, change, p.cfg.Fees)
	if err != nil {
		p.payer.GiveBack()
		return nil, err
	}
	rec := HoldRecord{Txid: tx.TxID().String(), Vout: 0, Sats: sats, RawHex: hex.EncodeToString(tx.Bytes())}
	p.mu.Lock()
	p.st.Holds = append(p.st.Holds, rec)
	err = p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		p.payer.GiveBack()
		return nil, err
	}
	mp, height, err := p.payer.SettleAndWait(ctx, "hold output", tx)
	if err != nil {
		p.payer.GiveBack()
		return nil, err
	}
	tx.MerklePath = mp
	p.payer.Change(tx, height, mp)
	rec.BumpHex, rec.Height = funding.BumpHex(mp), height
	p.mu.Lock()
	for i := range p.st.Holds {
		if p.st.Holds[i].Txid == rec.Txid {
			p.st.Holds[i] = rec
		}
	}
	err = p.saveLocked()
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	beef, err := aloneBEEF(tx)
	if err != nil {
		return nil, err
	}
	if targets := p.cfg.Delivery.recordTargets(bucket, false); len(targets) > 0 {
		it, err := p.outbox.add(rec.Txid, "hold output", beef, targets)
		if err != nil {
			return nil, err
		}
		p.outbox.send(ctx, it, beef)
	}
	return &rec, nil
}

// aloneBEEF is a mined transaction as a host admits a mined object: alone,
// with its own proof.
func aloneBEEF(tx *transaction.Transaction) ([]byte, error) {
	c, err := transaction.NewTransactionFromBytes(tx.Bytes())
	if err != nil {
		return nil, err
	}
	c.MerklePath = tx.MerklePath
	return c.BEEF()
}

func parseDisplay(s string) ([32]byte, error) {
	h, err := parseHash(s)
	if err != nil {
		return h, err
	}
	var r [32]byte
	for i := range h {
		r[i] = h[31-i]
	}
	return r, nil
}
