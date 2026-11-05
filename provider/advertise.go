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
// when it mined within a few seconds. A worker (noAdWait) goes on as soon
// as the transaction is journaled, and at most limits.ad_sends are sent at
// once behind the workers. Each stage is timed
// (bstore_provider_advertise_seconds{stage}): mint (the signature), journal
// (the row's sync), slot (the wait for a send slot), ancestors (unmined
// inputs put on the leg first), submit (the leg's answer for the
// advertisement).
func (p *Provider) advertise(ctx context.Context, root [32]byte, until uint64) (*backend.Advert, error) {
	pa, tx, known, batched, err := p.mintAd(ctx, root, until)
	if tx == nil {
		return known, err
	}
	if batched && ctx.Value(noAdWait{}) != nil {
		// The minter sent the transaction for every root it carries.
		return p.pendingAdvert(pa)
	}
	if ctx.Value(noAdWait{}) != nil {
		at := time.Now()
		select {
		case p.adSends <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		p.metrics.timed("bstore_provider_advertise_seconds", at, "stage=slot")
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
	if !batched {
		if err := p.sendAd(ctx, pa, tx); err != nil {
			return nil, err
		}
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
// why there is none. Workers mint for different roots at once: only the
// funding output is taken under mintMu; the signing and the journal's sync
// run side by side, and a root being minted for is pending to any other
// caller until its row is journaled.
//
// A fresh advertisement (no renewal) with limits.ad_batch above 1 is signed
// here and minted by the minter (adbatch.go) in a transaction with every
// other root asked for inside limits.ad_batch_ms, an output each; the
// minter journals and sends that transaction once, and the answer says so
// (batched).
func (p *Provider) mintAd(ctx context.Context, root [32]byte, until uint64) (pa PendingAd, tx *transaction.Transaction, known *backend.Advert, batched bool, err error) {
	var none PendingAd
	minted := time.Now()
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	if h == nil || !h.Verified || h.Damaged {
		p.mu.Unlock()
		return none, nil, nil, false, errors.New("the closure is not held verified")
	}
	if p.minting[rootHex] {
		p.mu.Unlock()
		return none, nil, nil, false, errAdPending
	}
	for _, pa := range p.st.PendingAds {
		if pa.Root == rootHex {
			p.mu.Unlock()
			if pa.Expiry >= until {
				known, err := p.pendingAdvert(pa)
				return none, nil, known, false, err
			}
			return none, nil, nil, false, errAdPending
		}
	}
	var prev *AdRec
	if h.Ad != nil {
		c := *h.Ad
		prev = &c
		if prev.Expiry >= until {
			p.mu.Unlock()
			known, err := adAdvert(prev)
			return none, nil, known, false, err
		}
	}
	// The lease reaches the disk with the journal row, before anything is
	// broadcast.
	h.Lease = max(h.Lease, until)
	p.dirty = true
	rootLen := h.RootLen
	if p.minting == nil {
		p.minting = map[string]bool{}
	}
	p.minting[rootHex] = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.minting, rootHex)
		p.mu.Unlock()
	}()

	lock, err := storerec.AdDerivation.Lock(ctx, p.wallet, p.wallet.Originator, adFields(p.signer.Identity.Compressed(), root, p.be.ServeURL(root), until, rootLen), true)
	if err != nil {
		return none, nil, nil, false, fmt.Errorf("advertisement: %w", err)
	}
	if _, err := storerec.DecodeAdvertisement(*lock); err != nil {
		return none, nil, nil, false, fmt.Errorf("the advertisement made is not one the SDK validates: %w", err)
	}
	if prev == nil && p.adAsks != nil {
		reply := make(chan adReply, 1)
		select {
		case p.adAsks <- adAsk{root: rootHex, until: until, lock: lock, reply: reply}:
		case <-ctx.Done():
			return none, nil, nil, false, ctx.Err()
		}
		select {
		case r := <-reply:
			if r.err != nil {
				return none, nil, nil, false, r.err
			}
			p.logf("root %s: advertisement %s:%d minted, expiry %d", short(rootHex), short(r.pa.Txid), r.pa.AdVout, until)
			return r.pa, r.tx, nil, true, nil
		case <-ctx.Done():
			return none, nil, nil, false, ctx.Err()
		}
	}
	var prevIn *mint.Input
	if prev != nil {
		ptx, err := funding.Rebuild(prev.RawHex, prev.Bump, "")
		if err != nil {
			return none, nil, nil, false, fmt.Errorf("the advertisement to renew: %w", err)
		}
		prevIn = &mint.Input{Tx: ptx, Vout: prev.Vout, Unlocker: storerec.AdDerivation.Unlocker(ctx, p.wallet, p.wallet.Originator)}
	}
	// The output is marked taken before the next mint asks for one: the
	// library hands out the state's next (treeState.Current). A mint that
	// fails past this point leaves its output unused.
	p.mintMu.Lock()
	locked := time.Now()
	tree, vout, err := p.spendTree(ctx)
	if err == nil {
		p.mu.Lock()
		p.st.markSpent(tree.TxID().String(), vout)
		p.record(&logRow{Spent: &spentRow{Tree: tree.TxID().String(), Vout: vout}})
		p.mu.Unlock()
	}
	p.mintMu.Unlock()
	p.metrics.inc("bstore_provider_mint_lock_total")
	p.metrics.add("bstore_provider_mint_lock_seconds_total", time.Since(locked).Seconds())
	if err != nil {
		return none, nil, nil, false, err
	}
	change, err := p.signer.FundScript()
	if err != nil {
		return none, nil, nil, false, err
	}
	// An advertisement keeps no change: what the funding output holds past
	// the fee goes to the fee. A coin that small costs more to spend than it
	// is worth, and each would be another row of the pool, one per root.
	fees := p.fees()
	fees.Dust = p.cfg.TreeSats + 1
	tx, err = mint.Transition(lock, 1, prevIn, mint.Input{Tx: tree, Vout: vout, Unlocker: p.signer.FundUnlocker()}, change, fees)
	if err != nil {
		return none, nil, nil, false, fmt.Errorf("advertisement transaction: %w", err)
	}
	pa = PendingAd{Root: rootHex, Txid: tx.TxID().String(), RawHex: hex.EncodeToString(tx.Bytes()), Expiry: until,
		Tree: tree.TxID().String(), Vout: vout, Prev: prev, At: p.now().Unix()}
	p.metrics.timed("bstore_provider_advertise_seconds", minted, "stage=mint")
	at := time.Now()
	if err := p.journalAd(pa); err != nil {
		return none, nil, nil, false, err
	}
	p.metrics.timed("bstore_provider_advertise_seconds", at, "stage=journal")
	what := "advertisement"
	if prev != nil {
		what = "renewal"
	}
	p.logf("root %s: %s %s minted, expiry %d", short(rootHex), what, short(pa.Txid), until)
	return pa, tx, nil, false, nil
}

// adWait is how long advertise waits for a block before it answers with
// the transaction unmined.
var adWait = 5 * time.Second

// noAdWait marks a context whose caller goes on without the proof: the one
// worker, for which a wait for a block or a verdict is a wait for every root
// behind this one. The background finishes the advertisement once it mines.
type noAdWait struct{}

var errAdPending = errors.New("an advertisement for this root is pending; it is looked at again once it mines")

// adFields are an advertisement's five fields before its signature.
func adFields(host []byte, root [32]byte, url string, expiry, fileSize uint64) [][]byte {
	return [][]byte{host, append([]byte(nil), root[:]...), []byte(url), storerec.CompactSize(expiry), storerec.CompactSize(fileSize)}
}

// ancestorEvery is how often one unmined ancestor is put on the leg again
// ahead of a transaction that spends it.
const ancestorEvery = 2 * time.Minute

// send hands a transaction to the settlement leg, its unmined ancestors
// first; one the network holds already is not an error. An ancestor is
// sent once in ancestorEvery, not once per spender: a funding tree of 16
// outputs reached the leg 16 times over (2026-11-05: Arcade counted as many
// duplicates as new transactions), each a round trip in the worker's way.
func (p *Provider) send(ctx context.Context, tx *transaction.Transaction) error {
	at := time.Now()
	for _, anc := range purse.Unmined(tx) {
		if p.ancestorDue(anc.TxID().String()) {
			_ = p.settler.Submit(ctx, anc)
		}
	}
	p.metrics.timed("bstore_provider_advertise_seconds", at, "stage=ancestors")
	at = time.Now()
	defer p.metrics.timed("bstore_provider_advertise_seconds", at, "stage=submit")
	settler := p.settler
	if p.adSettler != nil {
		settler = p.adSettler
	}
	if err := settler.Submit(ctx, tx); err != nil && !alreadyKnown(err) {
		if why, ok := chainview.RefusedAnswer(err); ok {
			return fmt.Errorf("the network refused %s: %s", short(tx.TxID().String()), why)
		}
		return fmt.Errorf("broadcasting %s: %w (it is persisted and sent again)", short(tx.TxID().String()), err)
	}
	return nil
}

// ancestorDue reports whether txid is to be put on the leg now, and
// notes that it was; at most maxAncestors are remembered.
func (p *Provider) ancestorDue(txid string) bool {
	p.ancMu.Lock()
	defer p.ancMu.Unlock()
	now := p.now()
	if at, ok := p.ancSent[txid]; ok && now.Sub(at) < ancestorEvery {
		return false
	}
	if p.ancSent == nil || len(p.ancSent) >= maxAncestors {
		p.ancSent = map[string]time.Time{}
	}
	p.ancSent[txid] = now
	return true
}

const maxAncestors = 4096

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

// finishAd records a mined advertisement as its root's live one, with
// every other root the transaction was minted for, and answers pa's.
func (p *Provider) finishAd(pa PendingAd, tx *transaction.Transaction, mp *transaction.MerklePath, height uint32) (*AdRec, error) {
	recs, err := p.finishAdTx(tx, mp, height)
	if err != nil {
		return nil, err
	}
	rec := recs[pa.Root]
	if rec == nil {
		return nil, fmt.Errorf("advertisement %s: no pending row for root %s", short(pa.Txid), short(pa.Root))
	}
	return rec, nil
}

// finishAdTx records a mined advertisement transaction as the live
// advertisement of every root it carries (one output each), drops their
// pending rows, and queues each for every host. The records are by root.
func (p *Provider) finishAdTx(tx *transaction.Transaction, mp *transaction.MerklePath, height uint32) (map[string]*AdRec, error) {
	txid := tx.TxID()
	mmp, err := minimalPath(mp, txid)
	if err != nil {
		return nil, err
	}
	raw, bump := hex.EncodeToString(tx.Bytes()), hex.EncodeToString(mmp.Bytes())
	p.mu.Lock()
	rows := p.st.pendingAdsOf(txid.String())
	p.mu.Unlock()
	if len(rows) == 0 {
		return nil, fmt.Errorf("advertisement %s: no pending row", short(txid.String()))
	}
	recs := map[string]*AdRec{}
	for _, pa := range rows {
		first := pa.Txid
		if pa.Prev != nil && pa.Prev.First != "" {
			first = pa.Prev.First
		} else if pa.Prev != nil {
			first = pa.Prev.Txid
		}
		rec := &AdRec{Txid: pa.Txid, Vout: pa.AdVout, Expiry: pa.Expiry, Height: height, RawHex: raw, Bump: bump, First: first}
		beef, err := aloneBEEF(rec)
		if err != nil {
			return nil, err
		}
		vout := pa.AdVout
		p.mu.Lock()
		h := p.st.Holdings[pa.Root]
		row := &logRow{AdDone: pa.Txid, AdDoneVout: &vout}
		if h != nil {
			h.Ad = rec
			h.Lease = max(h.Lease, rec.Expiry)
			row.Out = p.queueAd(h, rec.Txid, beef, h.Buckets)
			c := *h
			row.Hold = &c
		}
		p.st.dropPendingAdOut(pa.Txid, pa.AdVout)
		// Synced with the next row that waits, or the pass: a start finishes
		// it again if a crash loses this.
		p.record(row)
		p.mu.Unlock()
		p.metrics.inc("bstore_provider_advertisements_total")
		p.logf("root %s: advertisement %s:%d mined at %d, expiry %d", short(pa.Root), short(rec.Txid), pa.AdVout, height, rec.Expiry)
		recs[pa.Root] = rec
	}
	p.spawn(p.publishOutbox)
	return recs, nil
}

// queueAd puts an advertisement on the outbox for every host and the
// facade, on the records topic of each bucket, and tm_uhrp when set, and
// returns the rows queued; the caller holds p.mu.
func (p *Provider) queueAd(h *Holding, txid string, beef []byte, buckets []string) []Outgoing {
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
	var queued []Outgoing
	for _, host := range targets {
		for _, t := range topics {
			if slices.ContainsFunc(p.st.Outbox, func(o Outgoing) bool { return o.Host == host && o.Topic == t && o.Txid == txid }) {
				continue
			}
			o := Outgoing{Host: host, Topic: t, Txid: txid, Beef: b}
			p.st.Outbox = append(p.st.Outbox, o)
			queued = append(queued, o)
		}
	}
	return queued
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
				at := time.Now()
				_, err = f.Submit(ctx, o.Topic, beef)
				p.metrics.timed("bstore_provider_publish_seconds", at, "host="+o.Host)
			}
			amu.Lock()
			answers[row{o.Host, o.Topic, o.Txid}] = err
			amu.Unlock()
		}()
	}
	wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	var sent []Outgoing
	p.st.Outbox = slices.DeleteFunc(p.st.Outbox, func(o Outgoing) bool {
		err, asked := answers[row{o.Host, o.Topic, o.Txid}]
		if !asked {
			return false
		}
		if err == nil {
			sent = append(sent, Outgoing{Host: o.Host, Topic: o.Topic, Txid: o.Txid})
			return true
		}
		if o.Tries >= 20 || errors.Is(err, errNoHost) {
			p.logf("advertisement %s to %s on %s: giving up: %v", short(o.Txid), o.Host, o.Topic, err)
			sent = append(sent, Outgoing{Host: o.Host, Topic: o.Topic, Txid: o.Txid})
			return true
		}
		return false
	})
	if len(sent) > 0 {
		p.record(&logRow{OutDone: sent})
	}
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
	// A transaction minted for several roots has a row each: it is asked
	// about once, and what is learned decides every row.
	seen := map[string]bool{}
	for _, pa := range pending {
		if pa.Sent && !sweep {
			continue
		}
		if seen[pa.Txid] {
			continue
		}
		seen[pa.Txid] = true
		tx, err := p.pendingTx(pa)
		if err != nil {
			p.logf("pending advertisement %s: %v", short(pa.Txid), err)
			continue
		}
		if mp, height, err := p.chain.Proof(ctx, pa.Txid); err == nil {
			if _, err := p.finishAdTx(tx, mp, height); err != nil {
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
			p.logf("pending advertisement %s: its funding output was spent by %s; its roots are advertised afresh", short(pa.Txid), short(by))
			p.mu.Lock()
			rows := p.st.pendingAdsOf(pa.Txid)
			p.st.dropPendingAd(pa.Txid)
			seq := p.record(&logRow{AdDone: pa.Txid})
			p.mu.Unlock()
			_ = p.wait(seq)
			for _, r := range rows {
				p.enqueue(r.Root)
			}
			continue
		}
		if pa.Prev != nil {
			if by, err := p.chain.Spender(ctx, pa.Prev.Txid, pa.Prev.Vout); err == nil && by != "" && by != pa.Txid {
				p.logf("pending renewal %s: the advertisement it renews was spent by %s; the root is advertised afresh", short(pa.Txid), short(by))
				p.mu.Lock()
				p.st.dropPendingAd(pa.Txid)
				row := &logRow{AdDone: pa.Txid}
				if h := p.st.Holdings[pa.Root]; h != nil && h.Ad != nil && h.Ad.Txid == pa.Prev.Txid {
					h.Ad = nil
					row.Hold = &Holding{}
					*row.Hold = *h
				}
				seq := p.record(row)
				p.mu.Unlock()
				_ = p.wait(seq)
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
	tx, err := p.pendingTx(pa)
	if err != nil {
		return nil, err
	}
	raw, err := funding.BEEF(tx)
	if err != nil {
		return nil, err
	}
	return &backend.Advert{Txid: pa.Txid, Vout: pa.AdVout, Expiry: pa.Expiry, BEEF: raw}, nil
}

// pendingTx is a pending advertisement's transaction with its inputs'
// transactions behind it: the funding tree the state keeps and the
// advertisement a renewal spends, as mintAd built it. A row from before
// RawHex carries its BEEF whole.
func (p *Provider) pendingTx(pa PendingAd) (*transaction.Transaction, error) {
	if pa.RawHex == "" {
		raw, err := hex.DecodeString(pa.BeefHex)
		if err != nil {
			return nil, err
		}
		return transaction.NewTransactionFromBEEF(raw)
	}
	raw, err := hex.DecodeString(pa.RawHex)
	if err != nil {
		return nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, err
	}
	for _, in := range tx.Inputs {
		switch id := in.SourceTXID.String(); {
		case pa.Prev == nil || id != pa.Prev.Txid:
			// A funding output: the row names the first, a transaction
			// minted for several roots may spend more, all of kept trees.
			if in.SourceTransaction, err = p.loadKept(id); err != nil {
				return nil, err
			}
		case pa.Prev != nil && id == pa.Prev.Txid:
			if in.SourceTransaction, err = funding.Rebuild(pa.Prev.RawHex, pa.Prev.Bump, ""); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("input %s is neither its funding tree nor the advertisement it renews", short(id))
		}
	}
	return tx, nil
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
