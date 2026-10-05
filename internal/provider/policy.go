package provider

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// policy decides whether to take an entry and through what expiry; why is
// "" when it is taken.
func (p *Provider) policy(ctx context.Context, e *EntryRec) (until uint64, why string) {
	now := uint64(p.now().Unix())
	pol := p.cfg.Policy
	if e.Size == 0 {
		return 0, "an empty object has nothing to hold"
	}
	if e.Size > pol.MaxObject {
		return 0, "the object is larger than this provider takes"
	}
	unbountied := func(reason string) (uint64, string) {
		if !pol.HoldUnbountied {
			return 0, reason
		}
		return now + pol.UnbountiedSeconds, ""
	}
	if e.Hold == nil {
		return unbountied("no bounty, and this provider holds only for one")
	}
	if e.Hold.Retention > pol.MaxRetentionSeconds {
		return unbountied("the retention asked is longer than this provider holds for")
	}
	if err := p.checkBounty(ctx, e, true); err != nil {
		return unbountied("bounty: " + err.Error())
	}
	until = e.Created/1000 + e.Hold.Retention
	if until < now+advertMinLife {
		until = now + advertMinLife
	}
	return until, ""
}

// checkBounty is spec section 9.3's check of an entry's bounty, with the
// provider's own headers and spend evidence: the hold output exists, is
// mined, is locked to derive(writer, "hold") and holds exactly satoshis,
// is unspent by positive evidence (an empty or failed answer is not
// evidence), is worth at least the provider's minimum, and, for racing,
// its deadline is far enough ahead.
func (p *Provider) checkBounty(ctx context.Context, e *EntryRec, racing bool) error {
	hr := e.Hold
	if hr == nil {
		return errors.New("no hold terms")
	}
	if hr.Sats < p.cfg.Terms.MinBounty {
		return fmt.Errorf("%d satoshis is below this provider's minimum of %d", hr.Sats, p.cfg.Terms.MinBounty)
	}
	tip, err := p.tip(ctx)
	if err != nil {
		return err
	}
	if uint64(tip) >= hr.Deadline {
		return errors.New("the deadline has passed")
	}
	if racing && uint64(tip)+p.cfg.Policy.MinDeadlineBlocks > hr.Deadline {
		return errors.New("the deadline is too near to race for")
	}
	height, err := p.holdOutput(ctx, e)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if x := p.st.Entries[e.C]; x != nil && x.Hold != nil {
		x.Hold.Height = height
	}
	p.mu.Unlock()
	return nil
}

// holdOutput checks the hold output on the node and returns its height.
func (p *Provider) holdOutput(ctx context.Context, e *EntryRec) (uint32, error) {
	hr := e.Hold
	raw, err := p.asset.TxRaw(ctx, hr.Txid)
	if err != nil {
		return 0, fmt.Errorf("the hold transaction is not known to the node: %v", err)
	}
	tx, err := guard.ParseTransaction(raw, len(raw))
	if err != nil {
		return 0, fmt.Errorf("the hold transaction does not parse: %v", err)
	}
	if tx.TxID().String() != hr.Txid {
		return 0, errors.New("the node answered another transaction")
	}
	if uint64(hr.Vout) >= uint64(len(tx.Outputs)) {
		return 0, errors.New("the hold transaction has no such output")
	}
	out := tx.Outputs[hr.Vout]
	writer, _ := hex.DecodeString(e.Writer)
	key, err := storerec.LockingKey(storerec.HoldDerivation, writer)
	if err != nil {
		return 0, err
	}
	if out.LockingScript == nil || !bytes.Equal(*out.LockingScript, storerec.HoldScript(key)) {
		return 0, errors.New("the output is not the writer's hold output")
	}
	if out.Satoshis != hr.Sats {
		return 0, fmt.Errorf("the hold output holds %d satoshis, not %d", out.Satoshis, hr.Sats)
	}
	mp, height, err := p.asset.Proof(ctx, hr.Txid)
	if err != nil {
		return 0, errors.New("the hold output is not mined")
	}
	id := chainhash.Hash(*tx.TxID())
	if ok, err := mp.Verify(ctx, &id, p.hc); err != nil || !ok {
		return 0, errors.New("the hold output's proof does not verify against this provider's headers")
	}
	by, err := p.asset.Spender(ctx, hr.Txid, hr.Vout)
	if err != nil {
		return 0, fmt.Errorf("the node gave no evidence the hold output is unspent: %v", err)
	}
	if by != "" {
		return 0, fmt.Errorf("the hold output is spent by %s", short(by))
	}
	return height, nil
}

// holdFor takes entry e for a verified root: the lease runs through until,
// and the root's advertisement is made, renewed to last long enough, or
// published to the entry's bucket as it is.
func (p *Provider) holdFor(ctx context.Context, rootHex string, e *EntryRec, until uint64) error {
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	if h == nil || !h.Verified || h.Damaged {
		p.mu.Unlock()
		return errors.New("the closure is not held verified")
	}
	if !slices.Contains(h.Entries, e.C) {
		h.Entries = append(h.Entries, e.C)
	}
	newBucket := !slices.Contains(h.Buckets, e.Bucket)
	if newBucket {
		h.Buckets = append(h.Buckets, e.Bucket)
	}
	h.Lease = max(h.Lease, until)
	if err := p.save(); err != nil {
		p.mu.Unlock()
		return err
	}
	ad := h.Ad
	p.mu.Unlock()
	if ad != nil && ad.Expiry >= until {
		if newBucket {
			if beef, err := aloneBEEF(ad); err == nil {
				p.mu.Lock()
				p.queueAd(h, ad.Txid, beef, []string{e.Bucket})
				_ = p.save()
				p.mu.Unlock()
				go p.publishOutbox(context.Background())
			}
		}
		return nil
	}
	_, err := p.be.Advertise(ctx, mustHash(rootHex), until)
	if errors.Is(err, errAdPending) {
		return err
	}
	return err
}

// room says whether n more bytes of unclaimed pieces from writer fit, or
// why not. Unclaimed pieces have a share of the store of their own, and
// one writer at most a quarter of it, so that pieces a stranger sends can
// neither push out pieces still waiting for their entry nor take the whole
// share; they go only when their time runs out. Holdings that may be
// evicted go first when the store is short.
func (p *Provider) room(n uint64, writer string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evictLocked(false)
	var unclaimed, mine uint64
	for _, u := range p.st.Unclaimed {
		unclaimed += u.Size
		if u.Writer == writer {
			mine += u.Size
		}
	}
	switch {
	case unclaimed+n > p.cfg.UnclaimedBytes:
		p.metrics.inc("bstore_provider_refused_total", "reason=unclaimed-full")
		return "room: the space for pieces no entry has claimed is full"
	case mine+n > max(p.cfg.UnclaimedBytes/4, storerec.BlobSize+1<<16):
		p.metrics.inc("bstore_provider_refused_total", "reason=writer-share")
		return "room: this writer's share of the space for unclaimed pieces is full"
	case p.usageLocked()+n > p.cfg.QuotaBytes:
		p.evictLocked(true)
		if p.usageLocked()+n > p.cfg.QuotaBytes {
			p.metrics.inc("bstore_provider_refused_total", "reason=quota")
			return "room: the store is full"
		}
	}
	return ""
}

// usageLocked is the store's bytes: held closures and unclaimed pieces.
func (p *Provider) usageLocked() uint64 {
	var n uint64
	for _, h := range p.st.Holdings {
		n += h.Bytes
	}
	for _, u := range p.st.Unclaimed {
		n += u.Size
	}
	return n
}

// evictLocked drops unclaimed pieces past their time and every holding
// that is neither advertised nor leased any more: an expired holding, or
// one never advertised. Nothing advertised and unexpired is ever evicted.
// With all, holdings past their lease go too even when space is not short.
func (p *Provider) evictLocked(all bool) {
	now := p.now().Unix()
	changed := false
	for h, u := range p.st.Unclaimed {
		if uint64(now-u.At) > p.cfg.UnclaimedSeconds && !p.heldObjectLocked(h) {
			_ = p.store.Delete(mustHash(h))
			delete(p.st.Unclaimed, h)
			changed = true
		}
	}
	for rootHex, h := range p.st.Holdings {
		if !p.evictable(h, uint64(now)) {
			continue
		}
		if !all && h.Lease != 0 {
			continue
		}
		p.dropHoldingLocked(rootHex)
		changed = true
	}
	if changed {
		_ = p.save()
	}
}

// evictable: no pending advertisement, no advertisement unexpired, and the
// lease run out.
func (p *Provider) evictable(h *Holding, now uint64) bool {
	if h.Lease >= now || (h.Ad != nil && h.Ad.Expiry >= now) {
		return false
	}
	for _, pa := range p.st.PendingAds {
		if pa.Root == h.Root {
			return false
		}
	}
	// A closure verified for an entry still being decided is not evicted
	// under it.
	for _, e := range p.st.Entries {
		if e.Root == h.Root && e.Decision == "" {
			return false
		}
	}
	return true
}

// dropHoldingLocked removes a holding and every object of it no other
// holding names.
func (p *Provider) dropHoldingLocked(rootHex string) {
	objs, _ := p.rootObjects(rootHex)
	delete(p.st.Holdings, rootHex)
	for _, o := range objs {
		if !p.heldObjectLocked(hex.EncodeToString(o[:])) {
			_ = p.store.Delete(o)
		}
	}
	if p.export != nil {
		_ = p.export.Remove(mustHash(rootHex))
	}
	_ = os.RemoveAll(p.rootDir(rootHex))
	p.logf("root %s: evicted, its lease and advertisement run out", short(rootHex))
	p.metrics.inc("bstore_provider_evicted_total")
}

// heldObjectLocked reports whether a held root's closure names an object.
func (p *Provider) heldObjectLocked(objHex string) bool {
	obj := mustHash(objHex)
	for r, h := range p.st.Holdings {
		if h.Verified && p.inClosure(r, obj) {
			return true
		}
	}
	return false
}
