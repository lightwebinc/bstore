package publish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/mint"

	"github.com/lightwebinc/bstore/storerec"
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
	// Uploads are the providers' answers when the closure went by upload
	// sessions instead of piece carriers.
	Uploads []UploadResult
	// Hold is the hold output the entry names, when it asked for a bounty.
	Hold *HoldRecord
}

// ErrKilled is a write after the kill switch swept this identity's trees.
var ErrKilled = errors.New("publish: the kill switch swept this identity's trees; it publishes nothing more")

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
	if p.Killed() {
		return nil, ErrKilled
	}
	// Backpressure: a target that takes nothing (a provider down) leaves
	// its share owed in the outbox; past the bound new puts are refused
	// until it takes it, rather than pinning ever more of the store.
	if obj.Closure != nil {
		if err := p.outbox.room(int64(obj.Size)); err != nil {
			return nil, err
		}
	}
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	var hold *HoldRecord
	minted := false
	defer func() {
		// A put that stops before its entry is made gives its hold output
		// back: no entry names it.
		if hold != nil && !minted {
			p.releaseHold(hold)
		}
	}()
	if b != nil && obj.Size > 0 {
		h, err := p.takeHold(ctx, f.Bucket, b.Satoshis)
		if err != nil {
			return nil, err
		}
		hold = h
		tip, err := p.bestHeader(ctx)
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
	if err := p.noteBucket(f.Bucket); err != nil {
		return nil, err
	}
	n := 0
	var ups []UploadResult
	if obj.Closure != nil && obj.Size > 0 && p.cfg.Delivery.uploading() {
		retention := uint64(7 * 24 * 3600)
		if b != nil {
			retention = b.Retention
		}
		ups = p.uploadClosure(ctx, obj.Root, obj.Closure, retention)
		taken := 0
		var why error
		for _, u := range ups {
			if u.Err == nil {
				taken++
			} else {
				why = u.Err
				p.cfg.Logf("closure %s: not taken by %s (%v); it is sent again later", short(hex.EncodeToString(obj.Root[:])), u.Provider, u.Err)
			}
		}
		if taken == 0 {
			return nil, fmt.Errorf("pieces: no provider took the closure by upload session: %w", why)
		}
	} else if obj.Closure != nil {
		if n, err = p.publishPieces(ctx, f.Bucket, obj.Root, obj.Closure); err != nil {
			return nil, fmt.Errorf("pieces: %w", err)
		}
	}
	r, err := p.publishEntry(ctx, e, rec, hold)
	minted = r != nil
	if r != nil {
		r.Pieces = n
		r.Uploads = ups
		r.Hold = hold
	}
	return r, err
}

// noteBucket records a bucket this identity publishes carriers in.
func (p *Publisher) noteBucket(bucket string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.Contains(p.st.Buckets, bucket) {
		return nil
	}
	p.noteBucketLocked(bucket)
	return p.saveLocked()
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
	if p.Killed() {
		return nil, ErrKilled
	}
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	if err := p.noteBucket(bucket); err != nil {
		return nil, err
	}
	return p.publishEntry(ctx, e, rec, nil)
}

// publishPieces mints one piece carrier per distinct object of the
// closure and delivers each as it is made, PieceWorkers at a time: memory
// holds one object and one carrier a worker. The outputs of each tree
// spent are recorded used before the first carrier on them is signed, and
// each carrier is written to the outbox, synced, before it is sent. A put
// spends what the current tree has left before it switches to the next,
// minted ahead. A piece that reaches no target stops the put: the next one
// would only add to the backlog; what was minted stays in the outbox and
// is sent again.
func (p *Publisher) publishPieces(ctx context.Context, bucket string, root [32]byte, c Closure) (int, error) {
	objs := c.Objects()
	targets := p.cfg.Delivery.pieceTargets(bucket)
	if len(targets) == 0 {
		return 0, errors.New("no facade and no provider to deliver pieces to")
	}
	// A closure of the local store is kept in the outbox by reference.
	byRef := false
	if sc, ok := c.(StoreClosure); ok && sc.Store == p.cfg.Store && sc.Store != nil {
		byRef = true
	}
	b, err := p.outbox.newBatch()
	if err != nil {
		return 0, err
	}
	defer b.close()
	var minted atomic.Int64
	done := 0
	for done < len(objs) {
		batch := min(len(objs)-done, mint.MaxFundingOutputs)
		if room := int(p.pieceRoom()); room > 0 {
			batch = min(batch, room)
		} else {
			batch = min(batch, p.cfg.PieceTree)
		}
		tree, vout, err := p.spend(ctx, kindPiece, uint32(batch))
		if err != nil {
			return int(minted.Load()), err
		}
		if err := p.sendPieces(ctx, bucket, root, c, objs[done:done+batch], tree, vout, targets, b, byRef, &minted); err != nil {
			return int(minted.Load()), err
		}
		done += batch
	}
	return int(minted.Load()), nil
}

// sendPieces mints and sends the carriers of objs on consecutive outputs of
// tree from vout, PieceWorkers at a time, and returns the first failure
// after every worker has stopped.
func (p *Publisher) sendPieces(ctx context.Context, bucket string, root [32]byte, c Closure, objs [][32]byte,
	tree *transaction.Transaction, vout uint32, targets []Target, b *outBatch, byRef bool, minted *atomic.Int64) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var mu sync.Mutex
	var first error
	firstAt := len(objs)
	var wg sync.WaitGroup
	for w := 0; w < min(p.cfg.PieceWorkers, len(objs)); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				// A carrier being sent is sent to the end: only new
				// carriers stop after a failure.
				err := p.sendPiece(ctx, bucket, root, c, objs[i], tree, vout+uint32(i), targets, b, byRef, minted)
				if err != nil {
					mu.Lock()
					if i < firstAt {
						first, firstAt = err, i
					}
					mu.Unlock()
					cancel()
				}
			}
		}()
	}
	for i := range objs {
		if wctx.Err() != nil {
			break
		}
		select {
		case jobs <- i:
		case <-wctx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	if first == nil && ctx.Err() != nil {
		first = ctx.Err()
	}
	return first
}

// sendPiece mints one piece carrier, records it in the outbox and sends it.
// Its transaction id is computed once and kept on the transaction, so the
// BEEF and the outbox key hash the 4 MiB carrier no second time.
func (p *Publisher) sendPiece(ctx context.Context, bucket string, root [32]byte, c Closure, h [32]byte,
	tree *transaction.Transaction, out uint32, targets []Target, b *outBatch, byRef bool, minted *atomic.Int64) error {
	data, err := c.Object(ctx, h)
	if err != nil {
		return fmt.Errorf("object %x: %w", h[:6], err)
	}
	pc := &storerec.Piece{Bucket: bucket, Writer: p.Identity(), Root: root, Piece: data}
	rec, err := pc.Encode()
	if err != nil {
		return err
	}
	tx, err := carrier.Mint(ctx, p.wallet, p.wallet.Originator, carrierParams(storerec.PieceDerivation), rec, tree, out)
	if err != nil {
		return err
	}
	minted.Add(1)
	txid := tx.TxID()
	tx.SetTxHash(txid)
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return err
	}
	var it *outItem
	if byRef {
		it, err = p.outbox.addRef(b, txid.String(), "piece", beef, h, data, targets)
	} else {
		it, err = p.outbox.add(txid.String(), "piece", beef, targets)
	}
	if err != nil {
		return err
	}
	data, rec = nil, nil
	if ok, refused := p.outbox.sendAll(ctx, it, beef); len(ok)+len(refused) == 0 {
		return fmt.Errorf("piece %s reached no target", short(it.ID))
	}
	return nil
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
		if err := p.registerRace(e, rec, r.Txid, hold); err != nil {
			return nil, err
		}
	}
	if err := p.logEntry(e, rec, r.Txid, beef); err != nil {
		return nil, err
	}
	it, err := p.outbox.add(r.Txid, "entry", beef, targets)
	if err != nil {
		return nil, err
	}
	// The entry counts as sent once the facade or a host quorum takes it.
	// A put whose entry is minted is not given up lightly: a client that
	// retries would mint a second entry, so the targets still owed it are
	// asked again for a while before the put answers.
	ok := p.outbox.send(ctx, it, beef)
	for wait := time.Second; !p.cfg.Delivery.sent(ok) && len(it.Targets) > 0 && wait <= 8*time.Second; wait *= 2 {
		select {
		case <-ctx.Done():
			return r, ErrNotSent
		case <-time.After(wait):
		}
		ok = append(ok, p.outbox.send(ctx, it, beef)...)
	}
	r.Sent = p.cfg.Delivery.sent(ok)
	if !r.Sent {
		return r, ErrNotSent
	}
	return r, nil
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
