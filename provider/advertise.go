package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chainview"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/purse"

	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/storerec"
)

// An advertisement is a BRC-26 output exactly as the SDK makes it, in a
// transaction of its own that spends one funding output for its fee (and,
// for a renewal, the advertisement it renews, so the renewal is its
// successor: the same key and root, a later expiry, in the transaction
// that spends it, spec section 9.3). Before a transaction reaches the
// settlement leg it is persisted with the funding output marked used and
// the lease extended (BRC-167: the closure is leased before the
// advertisement is broadcast); a restart finds it in PendingAds and
// finishes it. Once it mines it is published, alone with its own proof, to
// the records topic of every bucket the root is held for, at every host,
// and on the plane when a facade is set: what a writer's client sees first
// ranks first, so nothing waits that need not.

// advertMinLife is the shortest an advertisement is made to last.
const advertMinLife = 3600

// advertise is the native backend's Advertise: lease the closure through
// until and mint an advertisement saying so, or a renewal of the live one.
// It returns once the transaction is on the settlement leg, with the proof
// when it mined within a few seconds. The one worker (noAdWait) goes on as
// soon as the transaction is journaled, and at most maxAdSends are sent at
// once behind it.
func (p *Provider) advertise(ctx context.Context, root [32]byte, until uint64) (*backend.Advert, error) {
	pa, tx, known, err := p.mintAd(ctx, root, until)
	if tx == nil {
		return known, err
	}
	if ctx.Value(noAdWait{}) != nil {
		select {
		case p.adSends <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.adSends }()
			if err := p.sendAd(ctx, pa, tx); err != nil {
				p.logf("advertisement %s: %v", short(pa.Txid), err)
			}
		}()
		return p.pendingAdvert(pa)
	}
	if err := p.sendAd(ctx, pa, tx); err != nil {
		return nil, err
	}
	// A block may come at once; the rest is the background's.
	wctx, cancel := context.WithTimeout(ctx, adWait)
	defer cancel()
	if mp, height, err := nodeapi.WaitMinedOn(wctx, p.chain, pa.Txid, 250*time.Millisecond); err == nil {
		if rec, err := p.finishAd(pa, tx, mp, height); err == nil {
			return adAdvert(rec)
		}
	}
	return p.pendingAdvert(pa)
}

// sendAd puts a journaled advertisement on the settlement leg. One that
// fails stays pending, and the background sends it again.
func (p *Provider) sendAd(ctx context.Context, pa PendingAd, tx *transaction.Transaction) error {
	if err := p.send(ctx, tx); err != nil {
		return err
	}
	p.mu.Lock()
	for i := range p.st.PendingAds {
		if p.st.PendingAds[i].Txid == pa.Txid {
			p.st.PendingAds[i].Sent = true
		}
	}
	// A start sends a pending advertisement again whichever this says.
	p.dirty = true
	p.mu.Unlock()
	p.payer.Change(tx, 0, nil)
	return nil
}

// mintAd signs the root's advertisement and journals it. With no new
// transaction to send it returns the advertisement that already answers, or
// why there is none.
func (p *Provider) mintAd(ctx context.Context, root [32]byte, until uint64) (PendingAd, *transaction.Transaction, *backend.Advert, error) {
	var none PendingAd
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	if h == nil || !h.Verified || h.Damaged {
		p.mu.Unlock()
		return none, nil, nil, errors.New("the closure is not held verified")
	}
	for _, pa := range p.st.PendingAds {
		if pa.Root == rootHex {
			p.mu.Unlock()
			if pa.Expiry >= until {
				known, err := p.pendingAdvert(pa)
				return none, nil, known, err
			}
			return none, nil, nil, errAdPending
		}
	}
	var prev *AdRec
	if h.Ad != nil {
		c := *h.Ad
		prev = &c
		if prev.Expiry >= until {
			p.mu.Unlock()
			known, err := adAdvert(prev)
			return none, nil, known, err
		}
	}
	// The lease reaches the disk with the journal row, before anything is
	// broadcast.
	h.Lease = max(h.Lease, until)
	p.dirty = true
	rootLen := h.RootLen
	p.mu.Unlock()

	lock, err := storerec.AdDerivation.Lock(ctx, p.wallet, p.wallet.Originator, adFields(p.signer.Identity.Compressed(), root, p.be.ServeURL(root), until, rootLen), true)
	if err != nil {
		return none, nil, nil, fmt.Errorf("advertisement: %w", err)
	}
	if _, err := storerec.DecodeAdvertisement(*lock); err != nil {
		return none, nil, nil, fmt.Errorf("the advertisement made is not one the SDK validates: %w", err)
	}
	var prevIn *mint.Input
	if prev != nil {
		ptx, err := funding.Rebuild(prev.RawHex, prev.Bump, "")
		if err != nil {
			return none, nil, nil, fmt.Errorf("the advertisement to renew: %w", err)
		}
		prevIn = &mint.Input{Tx: ptx, Vout: prev.Vout, Unlocker: storerec.AdDerivation.Unlocker(ctx, p.wallet, p.wallet.Originator)}
	}
	tree, vout, err := p.spendTree(ctx)
	if err != nil {
		return none, nil, nil, err
	}
	change, err := p.signer.FundScript()
	if err != nil {
		return none, nil, nil, err
	}
	// An advertisement keeps no change: what the funding output holds past
	// the fee goes to the fee. A coin that small costs more to spend than it
	// is worth, and each would be another row of the pool, one per root.
	fees := p.fees()
	fees.Dust = p.cfg.TreeSats + 1
	tx, err := mint.Transition(lock, 1, prevIn, mint.Input{Tx: tree, Vout: vout, Unlocker: p.signer.FundUnlocker()}, change, fees)
	if err != nil {
		return none, nil, nil, fmt.Errorf("advertisement transaction: %w", err)
	}
	beef, err := funding.BEEF(tx)
	if err != nil {
		return none, nil, nil, err
	}
	pa := PendingAd{Root: rootHex, Txid: tx.TxID().String(), BeefHex: hex.EncodeToString(beef), Expiry: until,
		Tree: tree.TxID().String(), Vout: vout, Prev: prev, At: p.now().Unix()}
	if err := p.journalAd(pa); err != nil {
		return none, nil, nil, err
	}
	what := "advertisement"
	if prev != nil {
		what = "renewal"
	}
	p.logf("root %s: %s %s minted, expiry %d", short(rootHex), what, short(pa.Txid), until)
	return pa, tx, nil, nil
}

// adWait is how long advertise waits for a block before it answers with
// the transaction unmined.
var adWait = 5 * time.Second

// noAdWait marks a context whose caller goes on without the proof: the one
// worker, for which a wait for a block or a verdict is a wait for every root
// behind this one. The background finishes the advertisement once it mines.
type noAdWait struct{}

// maxAdSends bounds the advertisements on their way to the settlement leg
// behind the worker.
const maxAdSends = 16

var errAdPending = errors.New("an advertisement for this root is pending; it is looked at again once it mines")

// adFields are an advertisement's five fields before its signature.
func adFields(host []byte, root [32]byte, url string, expiry, fileSize uint64) [][]byte {
	return [][]byte{host, append([]byte(nil), root[:]...), []byte(url), storerec.CompactSize(expiry), storerec.CompactSize(fileSize)}
}

// send hands a transaction to the settlement leg, its unmined ancestors
// first; one the network holds already is not an error.
func (p *Provider) send(ctx context.Context, tx *transaction.Transaction) error {
	for _, anc := range purse.Unmined(tx) {
		_ = p.settler.Submit(ctx, anc)
	}
	if err := p.settler.Submit(ctx, tx); err != nil && !alreadyKnown(err) {
		if why, ok := chainview.RefusedAnswer(err); ok {
			return fmt.Errorf("the network refused %s: %s", short(tx.TxID().String()), why)
		}
		return fmt.Errorf("broadcasting %s: %w (it is persisted and sent again)", short(tx.TxID().String()), err)
	}
	return nil
}

// alreadyKnown reports a leg's answer for a transaction the network holds
// already; an input already spent is not that.
func alreadyKnown(err error) bool {
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "spent") || strings.Contains(s, "conflict") || strings.Contains(s, "double") {
		return false
	}
	return strings.Contains(s, "txn-already-known") || strings.Contains(s, "already known") ||
		strings.Contains(s, "already in block") || strings.Contains(s, "already in the mempool") || strings.Contains(s, "already mined")
}

// finishAd records a mined advertisement as the root's live one, drops its
// pending record, and queues it for every host.
func (p *Provider) finishAd(pa PendingAd, tx *transaction.Transaction, mp *transaction.MerklePath, height uint32) (*AdRec, error) {
	txid := tx.TxID()
	mmp, err := minimalPath(mp, txid)
	if err != nil {
		return nil, err
	}
	first := pa.Txid
	if pa.Prev != nil && pa.Prev.First != "" {
		first = pa.Prev.First
	} else if pa.Prev != nil {
		first = pa.Prev.Txid
	}
	rec := &AdRec{Txid: pa.Txid, Vout: 0, Expiry: pa.Expiry, Height: height, RawHex: hex.EncodeToString(tx.Bytes()),
		Bump: hex.EncodeToString(mmp.Bytes()), First: first}
	beef, err := aloneBEEF(rec)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.st.Holdings[pa.Root]
	if h != nil {
		h.Ad = rec
		h.Lease = max(h.Lease, rec.Expiry)
		p.queueAd(h, rec.Txid, beef, h.Buckets)
	}
	p.st.dropPendingAd(pa.Txid)
	// Pending on disk until the next pass: a start finishes it again.
	p.dirty = true
	p.metrics.inc("bstore_provider_advertisements_total")
	p.logf("root %s: advertisement %s mined at %d, expiry %d", short(pa.Root), short(rec.Txid), height, rec.Expiry)
	go p.publishOutbox(context.Background())
	return rec, nil
}

// queueAd puts an advertisement on the outbox for every host and the
// facade, on the records topic of each bucket, and tm_uhrp when set; the
// caller holds p.mu.
func (p *Provider) queueAd(h *Holding, txid string, beef []byte, buckets []string) {
	var topics []string
	for _, b := range buckets {
		topics = append(topics, storerec.RecordsTopic(b))
	}
	if p.cfg.UHRPTopic {
		topics = append(topics, "tm_uhrp")
	}
	var targets []string
	for _, f := range p.hosts {
		targets = append(targets, f.Base)
	}
	if p.facade != nil {
		targets = append(targets, p.facade.Base)
	}
	b := hex.EncodeToString(beef)
	for _, host := range targets {
		for _, t := range topics {
			if slices.ContainsFunc(p.st.Outbox, func(o Outgoing) bool { return o.Host == host && o.Topic == t && o.Txid == txid }) {
				continue
			}
			p.st.Outbox = append(p.st.Outbox, Outgoing{Host: host, Topic: t, Txid: txid, Beef: b})
		}
	}
}

// publishOutbox submits every queued advertisement that is due, each to
// its host on its own; a failure waits longer each time.
func (p *Provider) publishOutbox(ctx context.Context) {
	if !p.outboxRun.CompareAndSwap(false, true) {
		p.outboxAgain.Store(true)
		return
	}
	defer p.outboxRun.Store(false)
	for {
		p.outboxAgain.Store(false)
		p.publishDue(ctx)
		if !p.outboxAgain.Load() {
			return
		}
	}
}

// maxOutboxSends bounds the outbox's submissions at once.
const maxOutboxSends = 8

// publishDue makes one pass over the rows due. Rows are sent side by side,
// and all answers are applied together; a row sent twice by a crash is one
// the host has seen.
func (p *Provider) publishDue(ctx context.Context) {
	p.mu.Lock()
	now := p.now().Unix()
	var due []Outgoing
	for _, o := range p.st.Outbox {
		if o.Next <= now {
			due = append(due, o)
		}
	}
	p.mu.Unlock()
	if len(due) == 0 {
		return
	}
	type row struct {
		host, topic, txid string
	}
	answers := make(map[row]error, len(due))
	var amu sync.Mutex
	var wg sync.WaitGroup
	slots := make(chan struct{}, maxOutboxSends)
	for _, o := range due {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			beef, _ := hex.DecodeString(o.Beef)
			err := errNoHost
			if f := p.facadeFor(o.Host); f != nil {
				_, err = f.Submit(ctx, o.Topic, beef)
			}
			amu.Lock()
			answers[row{o.Host, o.Topic, o.Txid}] = err
			amu.Unlock()
		}()
	}
	wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.Outbox = slices.DeleteFunc(p.st.Outbox, func(o Outgoing) bool {
		err, asked := answers[row{o.Host, o.Topic, o.Txid}]
		if !asked {
			return false
		}
		if err == nil {
			return true
		}
		if o.Tries >= 20 || errors.Is(err, errNoHost) {
			p.logf("advertisement %s to %s on %s: giving up: %v", short(o.Txid), o.Host, o.Topic, err)
			return true
		}
		return false
	})
	for i := range p.st.Outbox {
		o := &p.st.Outbox[i]
		if err, asked := answers[row{o.Host, o.Topic, o.Txid}]; asked && err != nil {
			o.Tries++
			o.Next = now + int64(min(3600, 5<<min(o.Tries, 10)))
			p.logf("advertisement %s to %s on %s: %v; tried again later", short(o.Txid), o.Host, o.Topic, err)
		}
	}
	// A row a crash brings back is sent again, and the host has seen it.
	p.dirty = true
}

var errNoHost = errors.New("no such host configured")

func (p *Provider) facadeFor(host string) *publish.Facade {
	for _, f := range p.hosts {
		if f.Base == host {
			return f
		}
	}
	if p.facade != nil && p.facade.Base == host {
		return p.facade
	}
	return nil
}

// resumeAds finishes every advertisement a run persisted: one mined is
// recorded and published; one the node does not know is sent again, the
// same bytes; one whose funding output another transaction spent is gone,
// and the root is advertised afresh. A node that cannot answer decides
// nothing.
//
// One sent is looked at once a block: only a block changes its answer, and
// each look is several reads of the node.
func (p *Provider) resumeAds(ctx context.Context) {
	tip := p.lastTip.Load()
	p.mu.Lock()
	pending := slices.Clone(p.st.PendingAds)
	sweep := tip != p.adSweep || tip == 0
	p.adSweep = tip
	p.mu.Unlock()
	for _, pa := range pending {
		if pa.Sent && !sweep {
			continue
		}
		raw, err := hex.DecodeString(pa.BeefHex)
		if err != nil {
			continue
		}
		tx, err := transaction.NewTransactionFromBEEF(raw)
		if err != nil {
			p.logf("pending advertisement %s: %v", short(pa.Txid), err)
			continue
		}
		if mp, height, err := p.chain.Proof(ctx, pa.Txid); err == nil {
			if _, err := p.finishAd(pa, tx, mp, height); err != nil {
				p.logf("pending advertisement %s: %v", short(pa.Txid), err)
			}
			p.payer.Change(tx, height, mp)
			continue
		}
		if known, err := p.view.Known.Known(ctx, pa.Txid); err == nil && known {
			continue // known, not mined yet
		}
		by, err := p.chain.Spender(ctx, pa.Tree, pa.Vout)
		if err != nil {
			continue
		}
		if by != "" && by != pa.Txid {
			p.logf("pending advertisement %s: its funding output was spent by %s; the root is advertised afresh", short(pa.Txid), short(by))
			p.mu.Lock()
			p.st.dropPendingAd(pa.Txid)
			_ = p.save()
			p.mu.Unlock()
			p.enqueue(pa.Root)
			continue
		}
		if pa.Prev != nil {
			if by, err := p.chain.Spender(ctx, pa.Prev.Txid, pa.Prev.Vout); err == nil && by != "" && by != pa.Txid {
				p.logf("pending renewal %s: the advertisement it renews was spent by %s; the root is advertised afresh", short(pa.Txid), short(by))
				p.mu.Lock()
				p.st.dropPendingAd(pa.Txid)
				if h := p.st.Holdings[pa.Root]; h != nil && h.Ad != nil && h.Ad.Txid == pa.Prev.Txid {
					h.Ad = nil
				}
				_ = p.save()
				p.mu.Unlock()
				p.enqueue(pa.Root)
				continue
			}
		}
		if err := p.send(ctx, tx); err != nil {
			p.logf("pending advertisement %s: %v", short(pa.Txid), err)
			continue
		}
		p.payer.Change(tx, 0, nil)
	}
}

// pendingAdvert is a pending advertisement as the backend answers it.
func (p *Provider) pendingAdvert(pa PendingAd) (*backend.Advert, error) {
	raw, err := hex.DecodeString(pa.BeefHex)
	if err != nil {
		return nil, err
	}
	return &backend.Advert{Txid: pa.Txid, Vout: 0, Expiry: pa.Expiry, BEEF: raw}, nil
}

func adAdvert(a *AdRec) (*backend.Advert, error) {
	beef, err := aloneBEEF(a)
	if err != nil {
		return nil, err
	}
	return &backend.Advert{Txid: a.Txid, Vout: a.Vout, Expiry: a.Expiry, BEEF: beef}, nil
}

// aloneBEEF is a mined advertisement as a host admits a mined object: the
// one transaction and its own minimal path.
func aloneBEEF(a *AdRec) ([]byte, error) {
	raw, err := hex.DecodeString(a.RawHex)
	if err != nil {
		return nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, err
	}
	bump, err := hex.DecodeString(a.Bump)
	if err != nil {
		return nil, err
	}
	mp, err := transaction.NewMerklePathFromBinary(bump)
	if err != nil {
		return nil, err
	}
	tx.MerklePath = mp
	return tx.BEEF()
}

// minimalPath cuts a proof to the path of txid alone.
func minimalPath(mp *transaction.MerklePath, txid *chainhash.Hash) (*transaction.MerklePath, error) {
	if mp == nil || len(mp.Path) == 0 {
		return nil, errors.New("no proof")
	}
	var at *transaction.PathElement
	for _, e := range mp.Path[0] {
		if e.Hash != nil && *e.Hash == *txid {
			at = e
		}
	}
	if at == nil {
		return nil, fmt.Errorf("the proof does not hold %s", txid)
	}
	index := make([]map[uint64]*transaction.PathElement, len(mp.Path))
	for l, leaves := range mp.Path {
		index[l] = map[uint64]*transaction.PathElement{}
		for _, e := range leaves {
			index[l][e.Offset] = e
		}
	}
	isTxid := true
	path := make([][]*transaction.PathElement, len(mp.Path))
	path[0] = []*transaction.PathElement{{Offset: at.Offset, Hash: txid, Txid: &isTxid}}
	for l := range mp.Path {
		off := at.Offset>>uint(l) ^ 1
		e := index[l][off]
		if e == nil {
			// The proof does not carry this sibling itself (it is computed
			// from lower levels): kept whole.
			return mp, nil
		}
		leaf := &transaction.PathElement{Offset: off}
		if e.Duplicate != nil && *e.Duplicate {
			dup := true
			leaf.Duplicate = &dup
		} else if e.Hash != nil {
			h := *e.Hash
			leaf.Hash = &h
		}
		path[l] = append(path[l], leaf)
	}
	slices.SortFunc(path[0], func(a, b *transaction.PathElement) int { return int(a.Offset) - int(b.Offset) })
	out := transaction.NewMerklePath(mp.BlockHeight, path)
	want, err := mp.ComputeRoot(txid)
	if err != nil {
		return nil, err
	}
	got, err := out.ComputeRoot(txid)
	if err != nil || *got != *want {
		return mp, nil
	}
	return out, nil
}
