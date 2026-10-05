package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/auth104"
	"github.com/lightwebinc/bstore/internal/storerec"
)

// challenge is <front>/challenge (spec sections 7 and 10.1). The order:
//
//  1. the buyer's budget (an identity key's, or an address's when the
//     request is not authenticated), before any work;
//  2. the request's own refusals, 422: request (it does not decode, or
//     names another provider or answerer), seed (no block seedHash at
//     seedHeight in this provider's headers, or seedHeight outside
//     tip - 6 to tip), unknown-root (not a root held verified, or proofRoot
//     or size not its own);
//  3. the free settlement challenge of spec section 9.4, exactly as frozen;
//     every other challenge is priced (BRC-105), its payment checked;
//  4. the answer built, every sampled segment read and checked against the
//     proof tree before anything is signed: a segment that is missing or
//     read back wrong marks the root damaged and the answer is 422
//     unknown-root, and no payment is taken;
//  5. the payment taken (the ledger synced), or the free answer counted;
//  6. the signed response [body, signature] under derive(answerer,
//     "challenge").
func (p *Provider) challenge(ctx context.Context, r *http.Request, body []byte, who *auth104.Caller) auth104.Reply {
	var req struct {
		Request string `json:"request"`
		Entry   string `json:"entry"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return refuse422("request")
	}
	buyer := "addr:" + auth104.AddressKey(r.RemoteAddr)
	if who != nil {
		buyer = who.Hex
	}
	if ok, retry := p.limits.buyers.Take(buyer); !ok {
		p.metrics.inc("bstore_provider_challenges_total", "result=limited")
		return auth104.Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many challenges from this buyer; try again later", "Retry-After", itoa(uint64(retry)))
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(req.Request)
	if err != nil || len(raw) > storerec.MaxRequest {
		return refuse422("request")
	}
	me := p.signer.Identity.Compressed()
	q, err := storerec.DecodeRequest(raw)
	if err != nil || !bytes.Equal(q.Provider, me) || !bytes.Equal(q.Answerer, me) {
		return refuse422("request")
	}
	tip, err := p.tip(ctx)
	if err != nil {
		return auth104.Error(http.StatusServiceUnavailable, "ERR_NODE", "the node did not answer")
	}
	rootHex := hex.EncodeToString(q.Root[:])
	if _, err := storerec.AnswerCheck(raw, uint64(tip), p.headerAt(ctx), p.own); err != nil {
		switch {
		case errors.Is(err, storerec.ErrSeed):
			return refuse422("seed")
		case errors.Is(err, storerec.ErrUnknownRoot):
			return refuse422("unknown-root")
		}
		return refuse422("request")
	}
	outpoint, free := p.freeChallenge(ctx, q, req.Entry, who, tip)
	if free {
		defer func() {
			if outpoint != "" {
				p.unreserveFree(outpoint)
			}
		}()
	}
	t := p.cfg.Terms
	price := t.ChallengeBase + t.ChallengePerSample*q.C
	var pay *paymentPending
	if !free && price > 0 {
		if who == nil {
			return auth104.Error(http.StatusUnauthorized, "ERR_AUTH_REQUIRED", "a challenge is priced; ask over BRC-104 authentication")
		}
		h := r.Header.Get("x-bsv-payment")
		if h == "" {
			_, rep := p.requirePayment(ctx, r, who, price, "challenge")
			return *rep
		}
		pl, rep := p.checkPayment(ctx, h, who, price)
		if rep != nil {
			p.metrics.inc("bstore_provider_payments_total", "result=refused")
			return *rep
		}
		pay = &paymentPending{pl: *pl}
	}
	select {
	case p.limits.work <- struct{}{}:
		defer func() { <-p.limits.work }()
	case <-ctx.Done():
		return auth104.Error(http.StatusServiceUnavailable, "ERR_BUSY", "try again later")
	}
	rb, err := p.answer(ctx, rootHex, q, raw)
	if err != nil {
		p.damaged(rootHex, err.Error())
		return refuse422("unknown-root")
	}
	signed, err := p.signResponse(ctx, raw, rb)
	if err != nil {
		return auth104.Error(http.StatusInternalServerError, "ERR_INTERNAL", "the response could not be signed")
	}
	if pay != nil {
		if _, rep := p.claimPayment(pay.pl, "challenge"); rep != nil {
			p.metrics.inc("bstore_provider_payments_total", "result=refused")
			return *rep
		}
		p.metrics.inc("bstore_provider_payments_total", "result=accepted")
	}
	if free {
		p.mu.Lock()
		_ = p.save() // the reservation stands: it counts
		p.mu.Unlock()
		outpoint = ""
		p.metrics.inc("bstore_provider_challenges_total", "result=free")
	} else {
		p.metrics.inc("bstore_provider_challenges_total", "result=answered")
	}
	h := http.Header{"Content-Type": {"application/cbor"}}
	return auth104.Reply{Status: http.StatusOK, Headers: h, Body: signed}
}

type paymentPending struct{ pl paymentLine }

func refuse422(label string) auth104.Reply {
	return auth104.JSON(http.StatusUnprocessableEntity, map[string]string{"error": label})
}

// own is the answerer's own computation for a root it holds: its proof
// root and size, for a root held verified and not damaged.
func (p *Provider) own(root [32]byte) ([32]byte, uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.st.Holdings[hex.EncodeToString(root[:])]
	if h == nil || !h.Verified || h.Damaged {
		return [32]byte{}, 0, false
	}
	return mustHash(h.ProofRoot), h.Size, true
}

// headerAt is the block hash at a height, from the node, in hash byte
// order; a few recent heights are kept.
func (p *Provider) headerAt(ctx context.Context) func(uint64) ([32]byte, bool) {
	return func(height uint64) ([32]byte, bool) {
		if height > 0xffffffff {
			return [32]byte{}, false
		}
		p.hmu.Lock()
		if p.hcache == nil {
			p.hcache = map[uint64]headerEntry{}
		}
		if e, ok := p.hcache[height]; ok && p.now().Sub(e.at) < 30*time.Second {
			p.hmu.Unlock()
			return e.hash, true
		}
		p.hmu.Unlock()
		s, err := p.asset.HashAtHeight(ctx, uint32(height))
		if err != nil {
			return [32]byte{}, false
		}
		h, ok := displayHash(s)
		if !ok {
			return [32]byte{}, false
		}
		p.hmu.Lock()
		if len(p.hcache) > 64 {
			p.hcache = map[uint64]headerEntry{}
		}
		p.hcache[height] = headerEntry{hash: h, at: p.now()}
		p.hmu.Unlock()
		return h, true
	}
}

type headerEntry struct {
	hash [32]byte
	at   time.Time
}

// freeChallenge decides spec section 9.4's settlement challenge: owed only
// to the entry's writer (its BRC-103 identity), for an entry with hold
// terms whose root this provider advertises, before the entry's deadline,
// when the provider's own bounty checks pass now (the hold output exists,
// is mined, is the writer's, holds satoshis, is unspent by positive
// evidence, and is worth at least the minimum), with c at most
// min(checkC, freeC), at most two per hold outpoint (this provider is one
// advertiser) whatever entries name it, and only when the hold output is at
// least minAge blocks deep (tip - h + 1). A free answer is reserved here
// and counted once the answer is signed; outpoint is the reservation.
func (p *Provider) freeChallenge(ctx context.Context, q *storerec.Request, entry string, who *auth104.Caller, tip uint32) (string, bool) {
	if who == nil || entry == "" {
		return "", false
	}
	p.mu.Lock()
	e := p.st.Entries[strings.ToLower(entry)]
	var ec EntryRec
	if e != nil {
		ec = *e
		if e.Hold != nil {
			hc := *e.Hold
			ec.Hold = &hc
		}
	}
	h := p.st.Holdings[hex.EncodeToString(q.Root[:])]
	advertised := h != nil && h.Ad != nil && h.Ad.Expiry >= uint64(p.now().Unix()) && !h.Damaged
	p.mu.Unlock()
	if e == nil || ec.Hold == nil || !advertised || ec.Writer != who.Hex || ec.Root != hex.EncodeToString(q.Root[:]) {
		return "", false
	}
	t := p.cfg.Terms
	if uint64(tip) >= ec.Hold.Deadline || q.C > min(ec.Hold.CheckC, t.FreeC) || ec.Hold.Sats < t.MinBounty {
		return "", false
	}
	op := ec.Hold.Outpoint()
	p.mu.Lock()
	n := p.st.Free[op]
	p.mu.Unlock()
	if n >= 2 {
		return "", false
	}
	if ok, _ := p.limits.writers.Take(who.Hex); !ok {
		return "", false
	}
	height, err := p.holdOutput(ctx, &ec)
	if err != nil {
		return "", false
	}
	if depth := int64(tip) - int64(height) + 1; depth < int64(t.MinAge) {
		return "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.st.Free[op] >= 2 {
		return "", false
	}
	p.st.Free[op]++
	return op, true
}

func (p *Provider) unreserveFree(op string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.st.Free[op] > 0 {
		p.st.Free[op]--
	}
	if p.st.Free[op] == 0 {
		delete(p.st.Free, op)
	}
}

// answer builds the response body for a request about a root held
// verified: every sample's segment read through the backend and checked
// against the kept leaf hash, and its path checked to reproduce the proof
// root, before anything is signed. Samples are inline, or by reference
// when the provider answers so (the buyer fetches each from the advertised
// URL). It returns the body's encoding.
func (p *Provider) answer(_ context.Context, rootHex string, q *storerec.Request, raw []byte) ([]byte, error) {
	n := storerec.Segments(q.Size)
	nb := storerec.Blobs(q.Size)
	roots, err := p.readAt32(rootHex, "blobroots", 0, nb)
	if err != nil {
		return nil, errors.New("the proof tree's blob roots are unreadable")
	}
	upper := buildLevels(roots)
	inline := p.cfg.answerInline()
	body := &storerec.Body{Request: sha256.Sum256(raw), Answerer: p.signer.Identity.Compressed()}
	type blobData struct {
		leaves [][32]byte
		hash   [32]byte
	}
	cache := map[uint64]*blobData{}
	for k := uint64(0); k < q.C; k++ {
		i := q.Index(uint32(k))
		b, off := blobPos(i)
		bd := cache[b]
		if bd == nil {
			first := b * storerec.SegmentsPerBlob
			count := min(uint64(storerec.SegmentsPerBlob), n-first)
			leaves, err := p.readAt32(rootHex, "leaves", first, count)
			if err != nil {
				return nil, errors.New("the proof tree's leaves are unreadable")
			}
			hs, err := p.readAt32(rootHex, "blobs", b, 1)
			if err != nil {
				return nil, errors.New("the closure's blob list is unreadable")
			}
			bd = &blobData{leaves: leaves, hash: hs[0]}
			cache[b] = bd
		}
		within := i % storerec.SegmentsPerBlob
		seglen := storerec.SegmentLen(i, q.Size)
		seg, err := p.be.Get(bd.hash, off, seglen)
		if err != nil {
			return nil, errors.New("a sampled segment is missing: " + err.Error())
		}
		leaf := commit.HashLeaf(seg)
		if leaf != bd.leaves[within] {
			return nil, errors.New("a sampled segment read back wrong")
		}
		inner, err := commit.ProveLeafHashes(bd.leaves, int(within))
		if err != nil {
			return nil, err
		}
		sib := append(inner.Siblings(), upper.path(b)...)
		if !commit.VerifyAt(leaf, i, n, sib, q.ProofRoot) {
			return nil, errors.New("a sampled segment's path does not reproduce the proof root")
		}
		s := storerec.Sample{Siblings: sib}
		if inline {
			s.Segment = seg
		}
		body.Samples = append(body.Samples, s)
	}
	return body.Encode()
}

// levels are the tree over the blob subtree roots, level by level, an odd
// node carried up unchanged: RFC 6962's tree over them (spec section 6),
// built once for a challenge so every sample's upper path is lookups.
type levels [][][32]byte

func buildLevels(roots [][32]byte) levels {
	ls := levels{roots}
	for cur := roots; len(cur) > 1; {
		next := make([][32]byte, 0, (len(cur)+1)/2)
		for j := 0; j+1 < len(cur); j += 2 {
			next = append(next, commit.NodeHash(cur[j], cur[j+1]))
		}
		if len(cur)%2 == 1 {
			next = append(next, cur[len(cur)-1])
		}
		ls = append(ls, next)
		cur = next
	}
	return ls
}

// path is blob b's subtree root's siblings up to the proof root.
func (ls levels) path(b uint64) [][32]byte {
	var out [][32]byte
	i := b
	for _, l := range ls[:len(ls)-1] {
		if s := i ^ 1; s < uint64(len(l)) {
			out = append(out, l[s])
		}
		i >>= 1
	}
	return out
}

// signResponse signs a body under derive(answerer, "challenge") and returns
// the signed response [body, signature]; the signature is checked as a
// buyer checks it before it leaves.
func (p *Provider) signResponse(ctx context.Context, raw, body []byte) ([]byte, error) {
	reqHash, respHash := sha256.Sum256(raw), sha256.Sum256(body)
	d := storerec.SignedDigest(reqHash, respHash)
	res, err := p.wallet.CreateSignature(ctx, wallet.CreateSignatureArgs{
		EncryptionArgs:     wallet.EncryptionArgs{ProtocolID: storerec.Protocol, KeyID: storerec.KeyChallenge, Counterparty: pushdrop.Anyone()},
		HashToDirectlySign: d[:],
	}, p.wallet.Originator)
	if err != nil {
		return nil, err
	}
	sig := res.Signature.Serialize()
	if !storerec.VerifyChallengeSig(p.signer.Identity.Compressed(), reqHash, respHash, sig) {
		return nil, errors.New("the response signature does not verify under the derived challenge key")
	}
	return cbor.Encode([]cbor.Value{body, sig})
}
