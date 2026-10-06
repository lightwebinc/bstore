package provider

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/backend"
	"github.com/lightwebinc/bstore/storerec"
)

// A front's side of spec section 17 (Options.Front): the closure handed to
// the backend, the advertiser's advertisement checked and adopted, the
// front's own bytes dropped, and settlements forwarded.

// maxAdBEEF bounds an advertisement's BEEF as a backend returns it: a mined
// object's bound (spec section 19.1).
const maxAdBEEF = 65536

// advertiseVia is the backend's Advertise as the front calls it: a native
// backend's mints and records its own; a front's is handed the closure
// first and its answer checked and adopted.
func (p *Provider) advertiseVia(ctx context.Context, root [32]byte, until uint64) (*backend.Advert, error) {
	if p.front == nil {
		return p.be.Advertise(ctx, root, until)
	}
	return p.frontAdvertise(ctx, root, until)
}

// frontAdvertise hands a verified closure to the backend (once), has the
// backend make it held through until, checks the advertisement it returns
// against everything the front knows of the root, adopts it as the root's
// live advertisement, queues it for the records topic of every bucket the
// root is held for, and drops the front's own copy of the bytes. Nothing is
// adopted that the front did not check: the advertiser's key, the root,
// the URL the backend serves, the expiry, the file size and a proof that
// verifies in the front's headers.
func (p *Provider) frontAdvertise(ctx context.Context, root [32]byte, until uint64) (*backend.Advert, error) {
	p.mintMu.Lock()
	defer p.mintMu.Unlock()
	rootHex := hex.EncodeToString(root[:])
	p.mu.Lock()
	h := p.st.Holdings[rootHex]
	if h == nil || !h.Verified || h.Damaged {
		p.mu.Unlock()
		return nil, errors.New("the closure is not held verified")
	}
	if h.Handed && h.Ad != nil && h.Ad.Expiry >= until {
		prev := *h.Ad
		p.mu.Unlock()
		return adAdvert(&prev)
	}
	handed, rootLen := h.Handed, h.RootLen
	var prev *AdRec
	if h.Ad != nil {
		c := *h.Ad
		prev = &c
	}
	h.Lease = max(h.Lease, until)
	if err := p.save(); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	p.mu.Unlock()
	if !handed {
		objs, err := p.rootObjects(rootHex)
		if err != nil {
			return nil, fmt.Errorf("the closure's object list: %w", err)
		}
		for _, o := range objs {
			data, err := p.fetch(o)
			if err != nil {
				p.damaged(rootHex, "an object of its closure went missing before it was handed to the backend")
				return nil, fmt.Errorf("object %s: %w", short(hex.EncodeToString(o[:])), err)
			}
			if err := p.be.Put(o, data); err != nil {
				return nil, fmt.Errorf("handing object %s to the backend: %w", short(hex.EncodeToString(o[:])), err)
			}
		}
	}
	ad, err := p.be.Advertise(ctx, root, until)
	if err != nil {
		return nil, err
	}
	rec, err := p.checkFrontAd(ctx, root, rootLen, until, ad, prev)
	if err != nil {
		return nil, fmt.Errorf("the backend's advertisement is not adopted: %w", err)
	}
	beef, err := aloneBEEF(rec)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	h = p.st.Holdings[rootHex]
	if h == nil || h.Damaged {
		p.mu.Unlock()
		return nil, errors.New("the holding was dropped or damaged while it was handed")
	}
	first := !h.Handed
	h.Ad, h.Handed = rec, true
	h.Lease = max(h.Lease, rec.Expiry)
	if first {
		h.Bytes = 0
	}
	p.queueAd(h, rec.Txid, beef, h.Buckets)
	err = p.save()
	if err == nil && first {
		p.dropStagedLocked(rootHex)
	}
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	p.metrics.inc("bstore_provider_advertisements_total")
	what := "advertisement"
	if prev != nil {
		what = "renewal"
	}
	p.logf("root %s: the advertiser's %s %s adopted, expiry %d; published to the records topic", short(rootHex), what, short(rec.Txid), rec.Expiry)
	go p.publishOutbox(context.Background())
	return &backend.Advert{Txid: rec.Txid, Vout: rec.Vout, Expiry: rec.Expiry, BEEF: beef}, nil
}

// checkFrontAd checks an advertisement a front's backend returned and
// makes the record the front keeps of it.
func (p *Provider) checkFrontAd(ctx context.Context, root [32]byte, rootLen, until uint64, ad *backend.Advert, prev *AdRec) (*AdRec, error) {
	if ad == nil || len(ad.BEEF) == 0 {
		return nil, errors.New("no advertisement")
	}
	b, _, _, err := guard.ParseBEEF(ad.BEEF, maxAdBEEF)
	if err != nil {
		return nil, err
	}
	tx := b.FindAtomicTransaction(ad.Txid)
	if tx == nil || tx.TxID().String() != ad.Txid {
		return nil, errors.New("the BEEF does not hold the advertisement's transaction")
	}
	if uint64(ad.Vout) >= uint64(len(tx.Outputs)) || tx.Outputs[ad.Vout].LockingScript == nil {
		return nil, errors.New("no such output")
	}
	a, err := storerec.DecodeAdvertisement(*tx.Outputs[ad.Vout].LockingScript)
	if err != nil {
		return nil, err
	}
	switch {
	case !bytes.Equal(a.Host, p.front.Advertiser):
		return nil, errors.New("it is not by the advertiser this front answers for")
	case a.Hash != root:
		return nil, errors.New("it names another root")
	case a.URL != p.be.ServeURL(root):
		return nil, fmt.Errorf("its url %q is not the backend's %q", a.URL, p.be.ServeURL(root))
	case a.Expiry != ad.Expiry:
		return nil, errors.New("its expiry is not the one the backend reported")
	case a.Expiry < until:
		return nil, fmt.Errorf("it expires at %d, before %d", a.Expiry, until)
	case a.FileSize != rootLen:
		return nil, errors.New("its fileSize is not the root node's length")
	}
	mp := tx.MerklePath
	if mp == nil {
		return nil, errors.New("it is not mined: no proof")
	}
	vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ok, err := mp.Verify(vctx, tx.TxID(), p.hc); err != nil {
		return nil, fmt.Errorf("the header source did not answer: %w", err)
	} else if !ok {
		return nil, errors.New("its proof does not verify in this front's headers")
	}
	mmp, err := minimalPath(mp, tx.TxID())
	if err != nil {
		return nil, err
	}
	rec := &AdRec{Txid: ad.Txid, Vout: ad.Vout, Expiry: a.Expiry, Height: mp.BlockHeight, RawHex: hex.EncodeToString(tx.Bytes()),
		Bump: hex.EncodeToString(mmp.Bytes()), First: ad.Txid}
	// A renewal that spends the advertisement it renews is its successor
	// and keeps the original's first sight (spec section 9.3).
	if prev != nil && prev.Txid != ad.Txid && spendsOutpoint(tx, prev.Txid, prev.Vout) {
		rec.First = prev.Txid
		if prev.First != "" {
			rec.First = prev.First
		}
	}
	return rec, nil
}

func spendsOutpoint(tx *transaction.Transaction, txid string, vout uint32) bool {
	for _, in := range tx.Inputs {
		if in.SourceTXID != nil && in.SourceTXID.String() == txid && in.SourceTxOutIndex == vout {
			return true
		}
	}
	return false
}

// dropStagedLocked deletes the front's own copy of a handed closure's
// objects: each that no unclaimed piece record keeps and no other closure
// still to be handed names. The caller holds p.mu.
func (p *Provider) dropStagedLocked(rootHex string) {
	objs, err := p.rootObjects(rootHex)
	if err != nil {
		return
	}
	n := 0
	for _, o := range objs {
		k := hex.EncodeToString(o[:])
		if p.st.Unclaimed[k] != nil || p.stagedElsewhereLocked(o, rootHex) {
			continue
		}
		if p.store.Has(o) {
			_ = p.store.Delete(o)
			n++
		}
	}
	if n > 0 {
		p.logf("root %s: handed to the backend; %d object(s) of the front's copy dropped, the segment hashes kept", short(rootHex), n)
	}
}

// stagedElsewhereLocked reports whether a verified closure not yet handed,
// other than except, names obj.
func (p *Provider) stagedElsewhereLocked(obj [32]byte, except string) bool {
	for r, h := range p.st.Holdings {
		if r != except && h.Verified && !h.Handed && p.inClosure(r, obj) {
			return true
		}
	}
	return false
}

// handedObjectLocked reports whether obj belongs to a closure the front
// has handed: a piece of it that arrives again is not kept.
func (p *Provider) handedObjectLocked(objHex string) bool {
	obj := mustHash(objHex)
	for r, h := range p.st.Holdings {
		if h.Handed && p.inClosure(r, obj) && !p.stagedElsewhereLocked(obj, "") {
			return true
		}
	}
	return false
}

// recheckEvery is how often a front looks again at a root it marked
// damaged.
var recheckEvery = 10 * time.Minute

// recheckBlobs bounds the blobs read whole, held to their hash, when a
// damaged root is looked at again.
const recheckBlobs = 8

// recheckDamaged is a front's: a root it marked damaged because the
// advertiser's URL did not serve a sample may have met a passing fault (a
// restart, a proxy answering 404 during a deploy). Every recheckEvery it
// asks the backend whether every object of the closure is served again,
// and reads up to recheckBlobs blobs whole, held to their object hashes;
// when all are, the root is answered for and renewed again. The front never
// had a copy to restore from: what it restores is its willingness to
// vouch, and only for what it has just read.
func (p *Provider) recheckDamaged(ctx context.Context) {
	if p.front == nil {
		return
	}
	now := p.now()
	p.mu.Lock()
	if p.rechecked == nil {
		p.rechecked = map[string]time.Time{}
	}
	var todo []Holding
	for r, h := range p.st.Holdings {
		if h.Damaged && h.Handed && h.Ad != nil && h.Ad.Expiry > uint64(now.Unix()) && now.Sub(p.rechecked[r]) >= recheckEvery {
			p.rechecked[r] = now
			todo = append(todo, *h)
		}
	}
	p.mu.Unlock()
	for _, h := range todo {
		if ctx.Err() != nil {
			return
		}
		if why := p.servedAgain(h); why != "" {
			p.logf("root %s: still damaged: %s", short(h.Root), why)
			continue
		}
		p.mu.Lock()
		if x := p.st.Holdings[h.Root]; x != nil && x.Damaged {
			x.Damaged = false
			_ = p.save()
			p.logf("root %s: the advertiser serves the closure again; it is answered for and renewed again", short(h.Root))
		}
		p.mu.Unlock()
	}
}

// servedAgain is why a damaged root is not served yet, or "".
func (p *Provider) servedAgain(h Holding) string {
	objs, err := p.rootObjects(h.Root)
	if err != nil {
		return "its object list: " + err.Error()
	}
	for _, o := range objs {
		if !p.be.Has(o) {
			return "object " + short(hex.EncodeToString(o[:])) + " is not served"
		}
	}
	nb := storerec.Blobs(h.Size)
	step := max(uint64(1), nb/recheckBlobs)
	for b := uint64(0); b < nb; b += step {
		hs, err := p.readAt32(h.Root, "blobs", b, 1)
		if err != nil {
			return "its blob list: " + err.Error()
		}
		if _, err := p.recheck(hs[0], b, 0, 1, h.Size, false); err != nil {
			return "blob " + itoa(b) + ": " + err.Error()
		}
	}
	return ""
}
