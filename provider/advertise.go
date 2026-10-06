package provider

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
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
// when it mined within a few seconds.
func (p *Provider) advertise(ctx context.Context, root [32]byte, until uint64) (*backend.Advert, error) {
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	if h == nil || !h.Verified || h.Damaged {
		p.mu.Unlock()
		return nil, errors.New("the closure is not held verified")
	}
	for _, pa := range p.st.PendingAds {
		if pa.Root == rootHex {
			p.mu.Unlock()
			if pa.Expiry >= until {
				return p.pendingAdvert(pa)
			}
			return nil, errAdPending
		}
	}
	var prev *AdRec
	if h.Ad != nil {
		c := *h.Ad
		prev = &c
		if prev.Expiry >= until {
			p.mu.Unlock()
			return adAdvert(prev)
		}
	}
	// The lease is durable before anything is broadcast.
	h.Lease = max(h.Lease, until)
	rootLen := h.RootLen
	if err := p.save(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Unlock()

	lock, err := storerec.AdDerivation.Lock(ctx, p.wallet, p.wallet.Originator, adFields(p.signer.Identity.Compressed(), root, p.be.ServeURL(root), until, rootLen), true)
	if err != nil {
		return nil, fmt.Errorf("advertisement: %w", err)
	}
	if _, err := storerec.DecodeAdvertisement(*lock); err != nil {
		return nil, fmt.Errorf("the advertisement made is not one the SDK validates: %w", err)
	}
	var prevIn *mint.Input
	if prev != nil {
		ptx, err := funding.Rebuild(prev.RawHex, prev.Bump, "")
		if err != nil {
			return nil, fmt.Errorf("the advertisement to renew: %w", err)
		}
		prevIn = &mint.Input{Tx: ptx, Vout: prev.Vout, Unlocker: storerec.AdDerivation.Unlocker(ctx, p.wallet, p.wallet.Originator)}
	}
	tree, vout, err := p.spendTree(ctx)
	if err != nil {
		return nil, err
	}
	change, err := p.signer.FundScript()
	if err != nil {
		return nil, err
	}
	tx, err := mint.Transition(lock, 1, prevIn, mint.Input{Tx: tree, Vout: vout, Unlocker: p.signer.FundUnlocker()}, change, p.fees())
	if err != nil {
		return nil, fmt.Errorf("advertisement transaction: %w", err)
	}
	beef, err := funding.BEEF(tx)
	if err != nil {
		return nil, err
	}
	pa := PendingAd{Root: rootHex, Txid: tx.TxID().String(), BeefHex: hex.EncodeToString(beef), Expiry: until,
		Tree: tree.TxID().String(), Vout: vout, Prev: prev, At: p.now().Unix()}
	p.mu.Lock()
	p.markSpent(pa.Tree, vout)
	p.st.PendingAds = append(p.st.PendingAds, pa)
	err = p.save()
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	what := "advertisement"
	if prev != nil {
		what = "renewal"
	}
	p.logf("root %s: %s %s minted, expiry %d", short(rootHex), what, short(pa.Txid), until)
	if err := p.send(ctx, tx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	for i := range p.st.PendingAds {
		if p.st.PendingAds[i].Txid == pa.Txid {
			p.st.PendingAds[i].Sent = true
		}
	}
	_ = p.save()
	p.mu.Unlock()
	p.payer.Change(tx, 0, nil)
	// A block may come at once; the rest is the background's.
	wctx, cancel := context.WithTimeout(ctx, adWait)
	defer cancel()
	if mp, height, err := nodeapi.WaitMined(wctx, p.asset, pa.Txid, 250*time.Millisecond); err == nil {
		if rec, err := p.finishAd(pa, tx, mp, height); err == nil {
			return adAdvert(rec)
		}
	}
	return p.pendingAdvert(pa)
}

// adWait is how long advertise waits for a block before it answers with
// the transaction unmined.
var adWait = 5 * time.Second

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
	if err := p.save(); err != nil {
		return nil, err
	}
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
	p.mu.Lock()
	now := p.now().Unix()
	var due []Outgoing
	for _, o := range p.st.Outbox {
		if o.Next <= now {
			due = append(due, o)
		}
	}
	p.mu.Unlock()
	for _, o := range due {
		beef, _ := hex.DecodeString(o.Beef)
		f := p.facadeFor(o.Host)
		var err error
		if f == nil {
			err = errors.New("no such host configured")
		} else {
			_, err = f.Submit(ctx, o.Topic, beef)
		}
		p.mu.Lock()
		i := slices.IndexFunc(p.st.Outbox, func(x Outgoing) bool { return x.Host == o.Host && x.Topic == o.Topic && x.Txid == o.Txid })
		if i >= 0 {
			if err == nil || p.st.Outbox[i].Tries >= 20 || f == nil {
				if err != nil {
					p.logf("advertisement %s to %s on %s: giving up: %v", short(o.Txid), o.Host, o.Topic, err)
				}
				p.st.Outbox = slices.Delete(p.st.Outbox, i, i+1)
			} else {
				p.st.Outbox[i].Tries++
				p.st.Outbox[i].Next = now + int64(min(3600, 5<<min(p.st.Outbox[i].Tries, 10)))
				p.logf("advertisement %s to %s on %s: %v; tried again later", short(o.Txid), o.Host, o.Topic, err)
			}
			_ = p.save()
		}
		p.mu.Unlock()
	}
}

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
func (p *Provider) resumeAds(ctx context.Context) {
	p.mu.Lock()
	pending := slices.Clone(p.st.PendingAds)
	p.mu.Unlock()
	for _, pa := range pending {
		raw, err := hex.DecodeString(pa.BeefHex)
		if err != nil {
			continue
		}
		tx, err := transaction.NewTransactionFromBEEF(raw)
		if err != nil {
			p.logf("pending advertisement %s: %v", short(pa.Txid), err)
			continue
		}
		if mp, height, err := p.asset.Proof(ctx, pa.Txid); err == nil {
			if _, err := p.finishAd(pa, tx, mp, height); err != nil {
				p.logf("pending advertisement %s: %v", short(pa.Txid), err)
			}
			p.payer.Change(tx, height, mp)
			continue
		}
		if _, err := p.asset.TxMeta(ctx, pa.Txid); err == nil {
			continue // known, not mined yet
		}
		by, err := p.asset.Spender(ctx, pa.Tree, pa.Vout)
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
			if by, err := p.asset.Spender(ctx, pa.Prev.Txid, pa.Prev.Vout); err == nil && by != "" && by != pa.Txid {
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
