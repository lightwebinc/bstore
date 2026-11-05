package publish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/util"

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
	// is sent again from the outbox either way. A put's entry is sent in
	// the background: Sent is valid once Delivered is closed.
	Sent bool
	// Delivered is closed once the first send of the entry and, for a put,
	// of every piece carrier has been answered; the outbox keeps sending
	// what was not taken.
	Delivered <-chan struct{}
	delivered chan struct{}
	// Pieces is how many piece carriers the put committed, each minted
	// in the background on the output reserved for it.
	Pieces int
	// Uploads are the providers' answers when the closure went by upload
	// sessions instead of piece carriers.
	Uploads []UploadResult
	// Hold is the hold output the entry names, when it asked for a bounty.
	Hold *HoldRecord
}

// ErrKilled is a write after the kill switch swept this identity's trees.
var ErrKilled = errors.New("publish: the kill switch swept this identity's trees; it publishes nothing more")

// ErrNotSent is an entry no facade or host quorum took on its first send;
// the outbox keeps sending it.
var ErrNotSent = errors.New("publish: the entry was minted but no facade or host quorum took it yet")

// Wait returns once the receipt's first send is answered: nil when the
// entry counts as sent, ErrNotSent when no facade or host quorum took it
// yet, or ctx's error.
func (r *Receipt) Wait(ctx context.Context) error {
	select {
	case <-r.Delivered:
	case <-ctx.Done():
		return ctx.Err()
	}
	if !r.Sent {
		return ErrNotSent
	}
	return nil
}

// Put publishes obj under f: a hold output first when b asks for one, then
// the closure's pieces on the bucket's blob topic (unless obj.Closure is
// nil), then a put entry. It returns once the put is committed: the hold
// taken, a funding output reserved for every piece carrier and recorded
// used, a record to mint each from written to the outbox, the entry
// minted on an output recorded used and written to the outbox, all
// synced, and the entry logged with its race. The pieces are minted and
// sent in the background, PieceWorkers at a time, the entry sent after
// them, and what a target did not take is sent again from the outbox,
// after a restart too, each piece minted once on its reserved output;
// Wait reports the first send. A closure that goes by upload session is
// uploaded before Put returns, as the session answers for it.
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
	var pb *pieceBatch
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
		if pb, err = p.mintPieces(ctx, f.Bucket, obj.Root, obj.Closure); err != nil {
			return nil, fmt.Errorf("pieces: %w", err)
		}
		n = len(pb.items)
	}
	r, it, beef, err := p.mintEntry(ctx, e, rec, hold)
	if err != nil {
		if pb != nil {
			// No entry names the carriers: they leave the outbox, and a
			// retry of the put mints its own.
			pb.b.abandon()
			pb.b.close()
		}
		return nil, err
	}
	minted = true
	r.Pieces = n
	r.Uploads = ups
	r.Hold = hold
	p.senders.Add(1)
	go p.deliver(pb, r, it, beef)
	return r, nil
}

// deliver is a put's background send: every piece carrier, PieceWorkers
// at a time, then the entry. A piece no target took stays in the outbox
// and is sent again later; the entry goes after the first pass over the
// pieces either way (a provider takes them in any order).
func (p *Publisher) deliver(pb *pieceBatch, r *Receipt, it *outItem, beef []byte) {
	defer p.senders.Done()
	defer close(r.delivered)
	ctx := p.run
	if pb != nil {
		p.sendMinted(ctx, pb)
		pb.b.close()
	}
	r.Sent = p.cfg.Delivery.sent(p.outbox.send(ctx, it, beef))
	if !r.Sent {
		p.cfg.Logf("entry %s: %v", short(r.Txid), ErrNotSent)
	}
}

// waitSenders waits up to wait for the puts' background sends to finish;
// what is left stays in the outbox for the next start.
func (p *Publisher) waitSenders(wait time.Duration) {
	done := make(chan struct{})
	go func() {
		p.senders.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(wait):
	}
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
	if err := p.noteBucket(bucket); err != nil {
		return nil, err
	}
	return p.publishEntry(ctx, e, rec, nil)
}

// pieceBatch is a put's piece carriers in the outbox: to mint later and
// send, or minted already, to send.
type pieceBatch struct {
	b     *outBatch
	items []*outItem
	// c is the closure the pieces to mint later are read from (the local
	// store, as the put staged it); nil when every carrier is minted.
	c Closure
}

// mintPieces commits one piece carrier per distinct object of the
// closure. The outputs of each tree spent are recorded used, synced,
// first. A closure of the local store (every put of the gateway and the
// command) is then written to the outbox as records to mint later, one
// an object naming its output, all synced at once, and mintPieces
// returns: the carriers are minted and sent in the background (deliver),
// and after a restart by the outbox's retry, each exactly once on the
// output its record reserved. Any other closure's carriers are minted
// here, PieceWorkers at a time, and kept whole. The batch stays the put's
// (live) until its first sends are done. A put spends what the current
// tree has left before it switches to the next, minted ahead. A failure
// stops the put, and what it wrote is dropped from the outbox, since no
// entry names it and the client's retry commits its own.
func (p *Publisher) mintPieces(ctx context.Context, bucket string, root [32]byte, c Closure) (*pieceBatch, error) {
	objs := c.Objects()
	targets := p.cfg.Delivery.pieceTargets(bucket)
	if len(targets) == 0 {
		return nil, errors.New("no facade and no provider to deliver pieces to")
	}
	// A closure of the local store is kept in the outbox by reference.
	byRef := false
	if sc, ok := c.(StoreClosure); ok && sc.Store == p.cfg.Store && sc.Store != nil {
		byRef = true
	}
	b, err := p.outbox.newBatch()
	if err != nil {
		return nil, err
	}
	pb := &pieceBatch{b: b, items: make([]*outItem, len(objs))}
	if byRef {
		pb.c = c
	}
	done := 0
	for done < len(objs) {
		batch := min(len(objs)-done, mint.MaxFundingOutputs)
		if room := int(p.pieceRoom()); room > 0 {
			batch = min(batch, room)
		} else {
			batch = min(batch, p.cfg.PieceTree)
		}
		tree, vout, err := p.spend(ctx, kindPiece, uint32(batch))
		if err == nil && byRef {
			p.cacheTree(tree)
			var items []*outItem
			if items, err = p.outbox.addMint(b, bucket, root, objs[done:done+batch], tree.TxID().String(), vout, targets); err == nil {
				copy(pb.items[done:], items)
			}
		} else if err == nil {
			err = p.eachPiece(ctx, len(objs[done:done+batch]), func(ctx context.Context, i int) error {
				it, err := p.mintOne(ctx, bucket, root, c, objs[done+i], tree, vout+uint32(i), targets, b)
				pb.items[done+i] = it
				return err
			})
		}
		if err != nil {
			b.abandon()
			b.close()
			return nil, err
		}
		done += batch
	}
	return pb, nil
}

// cacheTree keeps a mined funding tree for the pieces minted later on it,
// so that each does not rebuild it from the state; keptTree is the tree,
// from the cache or the state. A mined tree does not change. The cache is
// small: the trees of the puts in flight.
func (p *Publisher) cacheTree(tree *transaction.Transaction) {
	if tree == nil || tree.MerklePath == nil {
		return
	}
	p.treeMu.Lock()
	defer p.treeMu.Unlock()
	if len(p.trees) >= 8 {
		clear(p.trees)
	}
	p.trees[tree.TxID().String()] = tree
}

func (p *Publisher) keptTree(txid string) (*transaction.Transaction, error) {
	p.treeMu.Lock()
	tree, ok := p.trees[txid]
	p.treeMu.Unlock()
	if ok {
		return tree, nil
	}
	tree, err := p.loadKept(txid)
	if err != nil {
		return nil, err
	}
	p.cacheTree(tree)
	return tree, nil
}

// mintLater mints the carrier of a piece kept to mint later, on the
// output its record reserved, writes the minted form to the outbox,
// synced, and returns the BEEF to send (to release with putBEEF after).
// c reads the object when it is the closure the put staged (no second
// hash); nil reads the store, which holds the object to its hash. What
// it needs may be gone (the tree swept, the object trimmed, the identity
// killed): the error is the caller's to keep the record owed.
func (p *Publisher) mintLater(ctx context.Context, it *outItem, c Closure) ([]byte, error) {
	if p.Killed() {
		return nil, ErrKilled
	}
	h, err := parseHash(it.Ref)
	if err != nil {
		return nil, err
	}
	root, err := parseHash(it.Root)
	if err != nil {
		return nil, err
	}
	tree, err := p.keptTree(it.Tree)
	if err != nil {
		return nil, err
	}
	d, t0 := p.cfg.Delivery, time.Now()
	data, err := p.readPiece(ctx, c, h)
	if err != nil {
		return nil, fmt.Errorf("object %x: %w", h[:6], err)
	}
	d.observe(pieceStage("read"), t0)
	t0 = time.Now()
	tx, beef, at, err := p.mintPiece(ctx, it.Bucket, root, data, tree, it.Vout)
	putObject(data)
	if err != nil {
		return nil, err
	}
	d.observe(pieceStage("mint"), t0)
	t0 = time.Now()
	// The local hand-off's account of the carrier, taken before minted
	// clears the item's record fields (they are kept only while unminted).
	it.local = &localMeta{bucket: it.Bucket, root: root, hash: h, funding: outpoint(tree.TxID(), it.Vout),
		commitment: *tx.TxID(), at: at, n: len(data)}
	if err := p.outbox.minted(it, tx.TxID().String(), beef, at, len(data)); err != nil {
		putBEEF(beef)
		return nil, err
	}
	d.observe(pieceStage("outbox"), t0)
	return beef, nil
}

// readPiece is one object of a closure, in a buffer from the pool when
// the closure is the local store (putObject gives it back).
func (p *Publisher) readPiece(ctx context.Context, c Closure, h [32]byte) ([]byte, error) {
	switch sc := c.(type) {
	case nil:
		return p.cfg.Store.readInto(h, false, getBuf(&objectBufs))
	case StoreClosure:
		return sc.Store.readInto(h, sc.Staged, getBuf(&objectBufs))
	}
	return c.Object(ctx, h)
}

// The buffers a piece passes through are pooled: a 4 MiB object and its
// carrier's BEEF, fresh each piece, cost page faults and garbage before
// any byte is hashed.
var objectBufs, beefBufs sync.Pool

func getBuf(pool *sync.Pool) []byte {
	if b, ok := pool.Get().(*[]byte); ok {
		return (*b)[:0]
	}
	return nil
}

func putBuf(pool *sync.Pool, b []byte) {
	if cap(b) >= storerec.BlobSize {
		pool.Put(&b)
	}
}

func putObject(b []byte) { putBuf(&objectBufs, b) }

// putBEEF gives a BEEF mintPiece built back to the pool, once its send is
// answered. One that is not given back is garbage, no worse.
func putBEEF(b []byte) { putBuf(&beefBufs, b) }

// eachPiece runs f over 0 to n-1, PieceWorkers at a time, and returns the
// first failure after every worker has stopped. A step under way is run to
// its end: only new steps stop after a failure.
func (p *Publisher) eachPiece(ctx context.Context, n int, f func(ctx context.Context, i int) error) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var mu sync.Mutex
	var first error
	firstAt := n
	var wg sync.WaitGroup
	for w := 0; w < min(p.cfg.PieceWorkers, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := f(ctx, i); err != nil {
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
	for i := range n {
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

// mintOne mints one piece carrier of a closure that is not the local
// store and records it in the outbox, whole. Its transaction id is
// computed once, over the carrier's bytes where they lie in the BEEF, and
// kept on the transaction, so the outbox key hashes the 4 MiB carrier no
// second time.
func (p *Publisher) mintOne(ctx context.Context, bucket string, root [32]byte, c Closure, h [32]byte,
	tree *transaction.Transaction, out uint32, targets []Target, b *outBatch) (*outItem, error) {
	d, t0 := p.cfg.Delivery, time.Now()
	data, err := c.Object(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("object %x: %w", h[:6], err)
	}
	d.observe(pieceStage("read"), t0)
	t0 = time.Now()
	tx, beef, _, err := p.mintPiece(ctx, bucket, root, data, tree, out)
	if err != nil {
		return nil, err
	}
	d.observe(pieceStage("mint"), t0)
	t0 = time.Now()
	it, err := p.outbox.add(tx.TxID().String(), "piece", beef, targets)
	putBEEF(beef)
	if err != nil {
		return nil, err
	}
	b.track(it.ID)
	d.observe(pieceStage("outbox"), t0)
	return it, nil
}

// sendMinted mints and sends a put's piece carriers, PieceWorkers at a
// time: each minted on the output its record reserved (or read back from
// the outbox when it was minted already), then sent. One no target took
// is logged by the outbox and sent again later; one that could not be
// minted stays owed and is minted again later.
func (p *Publisher) sendMinted(ctx context.Context, pb *pieceBatch) {
	_ = p.eachPiece(ctx, len(pb.items), func(ctx context.Context, i int) error {
		it := pb.items[i]
		var beef []byte
		var err error
		if it.What == whatMint {
			if beef, err = p.mintLater(ctx, it, pb.c); err != nil {
				p.outbox.later(it, err)
				return nil
			}
		} else if beef, err = p.outbox.beefOf(it); err != nil {
			p.cfg.Logf("piece %s: %v; it cannot be sent", it.name(), err)
			p.outbox.remove(it)
			return nil
		}
		t0 := time.Now()
		local := it.owesLocal()
		p.outbox.sendAll(ctx, it, beef)
		p.cfg.Delivery.observe(pieceStage("send"), t0)
		if !local {
			putBEEF(beef)
		}
		return nil
	})
}

// mintPiece mints the carrier of one object of a closure on output out of
// tree: the piece record, the carrier spending the output, and its atomic
// BEEF, with the offset of the object's bytes in it (-1 when they are not
// placed, and the outbox then keeps the carrier whole).
func (p *Publisher) mintPiece(ctx context.Context, bucket string, root [32]byte, data []byte,
	tree *transaction.Transaction, out uint32) (*transaction.Transaction, []byte, int, error) {
	pc := &storerec.Piece{Bucket: bucket, Writer: p.Identity(), Root: root, Piece: data}
	rec, err := pc.Encode()
	if err != nil {
		return nil, nil, 0, err
	}
	tx, err := carrier.Mint(ctx, p.wallet, p.wallet.Originator, carrierParams(storerec.PieceDerivation), rec, tree, out)
	if err != nil {
		return nil, nil, 0, err
	}
	beef, at, err := pieceBEEF(tx, tree)
	if err != nil {
		return nil, nil, 0, err
	}
	return tx, beef, objectAt(beef, at, tx, rec, len(data)), nil
}

// objectAt is the offset in beef of the object a piece carrier's record
// ends with, from the carrier's layout rather than a search of the 4 MiB:
// the carrier's bytes at carrierAt hold one input and one output, whose
// script is <key> OP_CHECKSIG <record> <signature> OP_2DROP, and the object
// is the record's last field (storerec.PieceAt), so it ends 4 + 1 +
// len(signature) + 1 bytes before the BEEF does. It is -1 when the bytes
// there are not laid out so.
func objectAt(beef []byte, carrierAt int, tx *transaction.Transaction, rec []byte, n int) int {
	if carrierAt < 0 || len(tx.Inputs) != 1 || len(tx.Outputs) != 1 || tx.Inputs[0].UnlockingScript == nil || tx.Outputs[0].LockingScript == nil {
		return -1
	}
	u, s := len(*tx.Inputs[0].UnlockingScript), len(*tx.Outputs[0].LockingScript)
	// The version, one input (outpoint, script, sequence), one output
	// (value, script) and the locktime.
	at := carrierAt + 4 + 1 + 32 + 4 + varintLen(u) + u + 4 + 1 + 8 + varintLen(s)
	head := pushHead(len(rec))
	recAt := at + 35 + len(head)
	end := recAt + len(rec)
	if at+s+4 != len(beef) || end+2 > at+s || !bytes.Equal(beef[at+35:recAt], head) {
		return -1
	}
	sig := int(beef[end])
	if sig < 1 || sig > 75 || end+1+sig+1 != at+s || beef[at+s-1] != script.Op2DROP {
		return -1
	}
	i := storerec.PieceAt(beef[recAt:end], n)
	if i < 0 {
		return -1
	}
	return recAt + i
}

func varintLen(n int) int { return util.VarInt(uint64(n)).Length() }

// pushHead is the opcode and length of the minimal push of n bytes, as the
// SDK's PushDrop writes a field of more than one byte.
func pushHead(n int) []byte {
	switch {
	case n <= 75:
		return []byte{byte(n)}
	case n <= 0xff:
		return []byte{script.OpPUSHDATA1, byte(n)}
	case n <= 0xffff:
		return []byte{script.OpPUSHDATA2, byte(n), byte(n >> 8)}
	}
	return []byte{script.OpPUSHDATA4, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)}
}

// pieceBEEF is tx.AtomicBEEF(false) for a carrier spending one output of a
// mined funding tree, built once into a buffer of the exact size (from
// the pool, when one is there; putBEEF gives it back): the
// atomic prefix, the carrier's txid, BEEF v2, one BUMP (the tree's proof),
// then the tree (raw, BUMP 0) and the carrier (raw), as the SDK lays them
// out. The txid is hashed over the carrier's bytes where they lie, written
// into the header and kept on tx. It returns the BEEF and the offset of the
// carrier's bytes within it: the carrier is the BEEF's last transaction. A
// tree without a proof (unmined, so the BEEF carries its ancestry) goes
// through the SDK, which lays the carrier last as well.
func pieceBEEF(tx, tree *transaction.Transaction) ([]byte, int, error) {
	if tree == nil || tree.MerklePath == nil || len(tx.Inputs) != 1 || tx.Inputs[0].SourceTransaction != tree {
		tx.SetTxHash(tx.TxID())
		beef, err := tx.AtomicBEEF(false)
		if err != nil {
			return nil, 0, err
		}
		at := len(beef) - tx.Size()
		if at < 1 || beef[at-1] != byte(transaction.RawTx) {
			return nil, 0, errors.New("publish: the carrier is not the last transaction of its BEEF")
		}
		return beef, at, nil
	}
	bump := tree.MerklePath.Bytes()
	n := 4 + 32 + 4 + 1 + len(bump) + 1 + 2 + tree.Size() + 1 + tx.Size()
	beef := getBuf(&beefBufs)
	if cap(beef) < n {
		beef = make([]byte, 0, n)
	}
	beef = binary.LittleEndian.AppendUint32(beef, transaction.ATOMIC_BEEF)
	beef = append(beef, make([]byte, 32)...) // the txid, once the carrier's bytes are in place
	beef = binary.LittleEndian.AppendUint32(beef, transaction.BEEF_V2)
	beef = append(beef, 1) // one BUMP
	beef = append(beef, bump...)
	beef = append(beef, 2) // two transactions: the tree, proven by BUMP 0, then the carrier
	beef = append(beef, byte(transaction.RawTxAndBumpIndex), 0)
	beef = tree.AppendBytes(beef)
	beef = append(beef, byte(transaction.RawTx))
	at := len(beef)
	beef = tx.AppendBytes(beef)
	if len(beef) != n {
		return nil, 0, fmt.Errorf("publish: a piece BEEF of %d bytes was sized at %d", len(beef), n)
	}
	first := sha256.Sum256(beef[at:])
	txid := chainhash.Hash(sha256.Sum256(first[:]))
	copy(beef[4:36], txid[:])
	tx.SetTxHash(&txid)
	return beef, at, nil
}

// pieceStage names the histogram of one step of a piece carrier.
func pieceStage(stage string) string { return `bstore_publish_piece_seconds{stage="` + stage + `"}` }

// mintEntry mints the entry carrier on an entry tree output recorded used
// first, marks the hold taken by it, registers its race, logs it and
// writes it to the outbox, synced. It returns the receipt, the outbox item
// and the carrier's BEEF to send.
func (p *Publisher) mintEntry(ctx context.Context, e *storerec.Entry, rec []byte, hold *HoldRecord) (*Receipt, *outItem, []byte, error) {
	targets := p.cfg.Delivery.recordTargets(e.Bucket, true)
	if len(targets) == 0 {
		return nil, nil, nil, errors.New("no facade, host or provider to deliver the entry to")
	}
	tree, vout, err := p.spend(ctx, kindEntry, 1)
	if err != nil {
		return nil, nil, nil, err
	}
	tx, err := carrier.Mint(ctx, p.wallet, p.wallet.Originator, carrierParams(storerec.EntryDerivation), rec, tree, vout)
	if err != nil {
		return nil, nil, nil, err
	}
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return nil, nil, nil, err
	}
	done := make(chan struct{})
	r := &Receipt{Entry: e, Record: rec, C: carrier.Commitment(tx), Txid: tx.TxID().String(), Delivered: done, delivered: done}
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
			return nil, nil, nil, err
		}
		if err := p.registerRace(e, rec, r.Txid, hold); err != nil {
			return nil, nil, nil, err
		}
	}
	if err := p.logEntry(e, rec, r.Txid, beef); err != nil {
		return nil, nil, nil, err
	}
	it, err := p.outbox.add(r.Txid, "entry", beef, targets)
	if err != nil {
		return nil, nil, nil, err
	}
	return r, it, beef, nil
}

// publishEntry is mintEntry and one send, waited for: the entry counts as
// sent once the facade or a host quorum takes it, and the outbox keeps
// sending it otherwise (ErrNotSent).
func (p *Publisher) publishEntry(ctx context.Context, e *storerec.Entry, rec []byte, hold *HoldRecord) (*Receipt, error) {
	r, it, beef, err := p.mintEntry(ctx, e, rec, hold)
	if err != nil {
		return nil, err
	}
	r.Sent = p.cfg.Delivery.sent(p.outbox.send(ctx, it, beef))
	close(r.delivered)
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
