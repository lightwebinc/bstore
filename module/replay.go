package module

import (
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/replay"

	"github.com/lightwebinc/bstore/storerec"
)

// Ordered replay (bcommon module.Replayer): how this host repairs a gap on
// a bucket's topic from a sibling host, as spec section 12.6 says a host
// joins a bucket ("by replay: tokens, entries, advertisements, hold
// outputs, outcomes and statements are self-verifying, so anyone who holds
// one may submit it. A replayer assembles each token's BEEF from the token
// at seq and its parent at seq - 1"), never by an unordered synchronization
// of unspent outputs. The questions are the free classes of section 11.1,
// asked of the peer's ls_bstore; everything answered is admitted here
// through the host's ordinary path, so every rule of section 12.1 holds.
//
// A walk is phases, each finished before the next reads the index:
//
//  1. tokens: for every chain of the bucket the host holds a token of or
//     serves, the peer's latest (bucket), then each seq below it the host
//     holds no token at (bucket-seq), each token in the BEEF of the token
//     and its parent, parents first; a chain's first token takes its
//     parent, the chain tree, from the chain view (BSTORE_ASSET_URL or
//     BSTORE_CHAIN), since no host keeps one;
//  2. named: for each served chain, every entry its current map holds, or a
//     token first seen inside the keep floor less a day names, that the
//     host does not hold (entry): an entry only older tokens name has left
//     every host by its floor;
//  3. since: what the peer took after the position this host last reached
//     with it, in the peer's order (since; since_replay.go): the entries no
//     token names, advertisements, hold outputs, statements, outcomes and
//     sweeps. A peer that refuses the class is asked phases 4 to 6 in its
//     place, which ask about everything the host holds:
//  4. unanchored: for each served chain's owner and latest writers, the
//     peer's unanchored entries, page by page (unanchored);
//  5. mined: the advertisements of each root the held entries name (ads),
//     and each hold output a held entry names that the host does not hold
//     (hold);
//  6. attached: the front statements and outcomes of each advertiser the
//     host holds an advertisement of (fronts, outcomes-of), and the sweeps
//     of each held carrier's funding outpoint (sweep).
//
// Replay never lengthens what the host's retention floors keep: a carrier
// is replayed only while it is dated within its floor's window less a
// margin (an entry no token names, ReplayPendingMargin under the pending
// floor; an outcome or a statement, ReplayOtherMargin under the 24-hour
// one), a mined advertisement or hold output only within ReplayMinedBlocks
// of the tip, and nothing on a funding outpoint this host retracted or
// gave to another carrier. Otherwise two hosts whose first sights differ by
// a second would hand an object they dropped back to each other at every
// floor.

// The margins and the window above.
const (
	ReplayPendingMargin = 24 * time.Hour
	ReplayOtherMargin   = time.Hour
	ReplayMinedBlocks   = 4032
	// ReplaySkew is how far ahead of the host's clock a record may be dated
	// and still be replayed.
	ReplaySkew = 5 * time.Minute
)

// KindsReplayed are the kinds a walk answers objects as.
var KindsReplayed = []string{replay.KindToken, "entry", "ad", "hold", "front", "outcome", "sweep"}

var _ bmodule.Replayer = (*Service)(nil)

// Replay implements bcommon module.Replayer for tm_bstore_ topics. A blob
// topic holds nothing (no host admits a piece), so its walk is over at
// once.
func (s *Service) Replay(ctx context.Context, r *bmodule.ReplayRequest) (*bmodule.ReplayAnswer, error) {
	if strings.HasPrefix(r.Topic, storerec.BlobPrefix) {
		return &bmodule.ReplayAnswer{Done: true}, nil
	}
	b, ok := s.bucketOf(r.Topic)
	if !ok {
		return nil, bmodule.ErrNoReplay
	}
	if !s.Restored() {
		return nil, errors.New("ls_bstore: the index is not restored yet")
	}
	// The asker says when the peer refused a question, which a walk answers
	// as nothing: the since phase needs to tell the two apart.
	refused := false
	rr := *r
	rr.Ask = func(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
		ans, err := r.Ask(ctx, q)
		if errors.Is(err, bmodule.ErrRefused) {
			refused = true
		}
		return ans, err
	}
	w := replay.New(&rr)
	return w.Run(ctx, s.replayTokens(b), s.replayNamed(b), s.replaySince(ctx, b, r.Peer, &refused),
		unlessSince(s.replayUnanchored(b)), unlessSince(s.replayMined(ctx, b)), unlessSince(s.replayAttached(b)))
}

// withValue is q with one more member.
func withValue(q map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(q)+1)
	for x, y := range q {
		out[x] = y
	}
	out[k] = v
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func hashOfHex(s string) (chainhash.Hash, bool) {
	h, err := chainhash.NewHashFromHex(s)
	if err != nil {
		return chainhash.Hash{}, false
	}
	return *h, true
}

// ---- phase 1: tokens

func (s *Service) replayTokens(b string) replay.Phase {
	return replay.Phase{Name: "tokens",
		Items: func(context.Context, *replay.Walk) ([]string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			keys := map[string]bool{}
			for _, v := range s.chains {
				if v.b == b {
					keys[v.owner+"|"+v.id] = true
				}
			}
			for _, sv := range s.servedOf(b) {
				keys[sv.owner+"|"+sv.id] = true
			}
			return sortedKeys(keys), nil
		},
		Do: func(ctx context.Context, w *replay.Walk, item string, page *string) (bool, error) {
			owner, id, _ := strings.Cut(item, "|")
			key := b + "|" + owner + "|" + id
			q := map[string]string{"bucket": b, "owner": owner, "id": id}
			c := &replay.Chain{
				Name:    "bucket " + b + " chain " + short(owner) + "/" + short(id),
				LatestQ: func() (string, map[string]string) { return storerec.LookupService, q },
				AtSeq: func(seq uint64) (string, map[string]string) {
					return storerec.LookupService, withValue(q, "seq", strconv.FormatUint(seq, 10))
				},
				Held:  func(seq uint64) []chainhash.Hash { return s.heldTokens(key, seq) },
				Local: s.localToken,
				Read:  func(tx *transaction.Transaction) (*replay.Token, error) { return readChainToken(tx, b, owner, id) },
				Trees: s.trees,
				Check: func(ctx context.Context, beef []byte) error {
					_, err := storerec.Admit(ctx, beef, nil, storerec.Host{Bucket: b, Headers: s.headers, MaxBEEF: s.maxBEEF, MaxSweepInputs: s.maxSweep})
					return err
				},
			}
			return w.Tokens(ctx, c, page)
		}}
}

// heldTokens are the txids of the tokens the host holds of a chain at seq.
func (s *Service) heldTokens(key string, seq uint64) []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.chains[key]
	if v == nil {
		return nil
	}
	var out []chainhash.Hash
	for _, t := range v.bySeq[seq] {
		if h, ok := hashOfHex(t.txid); ok {
			out = append(out, h)
		}
	}
	return out
}

// localToken is a token the host holds, as the engine stores it: alone,
// with its proof.
func (s *Service) localToken(ctx context.Context, txid chainhash.Hash) (*transaction.Transaction, error) {
	s.mu.Lock()
	storage := s.storage
	var vout uint32
	found := false
	if k, ok := s.tokenChain[txid.String()]; ok {
		if t := s.chains[k].byTxid[txid.String()]; t != nil {
			vout, found = t.vout, true
		}
	}
	s.mu.Unlock()
	if !found || storage == nil {
		return nil, nil
	}
	o, err := storage.FindOutput(ctx, &transaction.Outpoint{Txid: txid, Index: vout}, "")
	if err != nil || o == nil || o.Beef == nil {
		return nil, err
	}
	return parse(o.Beef)
}

// readChainToken reads a token of one chain from its transaction: its seq,
// the token it spends, and at seq 0 the chain-tree outpoint whose bucket id
// is the chain's (frozen.md row C15).
func readChainToken(tx *transaction.Transaction, b, owner, id string) (*replay.Token, error) {
	tok, err := storerec.ReadToken(tx)
	if err != nil {
		return nil, err
	}
	bk := tok.Bucket
	if bk.Bucket != b || hexOf(bk.Owner) != owner || hexOf(bk.ID[:]) != id {
		return nil, errors.New("ls_bstore: a token of another chain")
	}
	t := &replay.Token{Seq: bk.Seq}
	if bk.Seq > 0 {
		t.Prev = chainhash.Hash(bk.Prev)
		return t, nil
	}
	for _, in := range tx.Inputs {
		if in.SourceTXID != nil && storerec.BucketID([32]byte(*in.SourceTXID), in.SourceTxOutIndex) == bk.ID {
			t.Tree = transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}
			return t, nil
		}
	}
	return nil, errors.New("ls_bstore: a first token with no input of its bucket id")
}

// ---- phase 2: entries the held tokens name

func (s *Service) replayNamed(b string) replay.Phase {
	return replay.Phase{Name: "named",
		Items: func(context.Context, *replay.Walk) ([]string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			keys := map[string]bool{}
			for _, v := range s.servedChains(b) {
				keys[v.owner+"|"+v.id] = true
			}
			return sortedKeys(keys), nil
		},
		Do: func(ctx context.Context, w *replay.Walk, item string, page *string) (bool, error) {
			for _, c := range s.unheldNamed(ctx, b+"|"+item, *page) {
				h, err := chainhash.NewHash(mustHex(c))
				if err != nil {
					continue
				}
				txid := h.String()
				keep := func(o *replay.Output) bool { return o.Txid == *h && s.replayable(b, o.Tx, 0) }
				if _, err := w.Each(ctx, "entry", storerec.LookupService, map[string]string{"bucket": b, "entry": txid}, keep); err != nil {
					return false, err
				}
				*page = c
			}
			return true, nil
		}}
}

func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

// unheldNamed are the commitments a served chain still keeps named whose
// entry the host does not hold, ascending, after after: those its current
// map holds, and those a token first seen inside the keep floor less a day
// names (in its changes or consumed). An entry only older tokens name has
// been replaced or removed for longer than the floor keeps it, on every host,
// so asking for it every walk would ask for nothing, ever more often.
func (s *Service) unheldNamed(ctx context.Context, key, after string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.chains[key]
	if v == nil {
		return nil
	}
	wanted := map[string]bool{}
	cutoff := s.nowMs() - s.keep + day
	for _, t := range v.byTxid {
		if t.at < cutoff || t.rec == nil {
			continue
		}
		for _, ch := range t.rec.Changes {
			wanted[hexOf(ch.C[:])] = true
		}
		for _, c := range t.rec.Consumed {
			wanted[hexOf(c[:])] = true
		}
	}
	if _, ms := s.view(ctx, v); ms != nil {
		for _, c := range ms.cs {
			wanted[hexOf(c[:])] = true
		}
	}
	var out []string
	for c := range wanted {
		if c <= after || v.named[c] == 0 {
			continue
		}
		if e, ok := s.carriers.get(c); ok && e.kind == "entry" {
			continue
		}
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// ---- phase 4: entries no token names

func (s *Service) replayUnanchored(b string) replay.Phase {
	return replay.Phase{Name: "unanchored",
		Items: func(context.Context, *replay.Walk) ([]string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			keys := map[string]bool{}
			for _, sv := range s.servedOf(b) {
				for w := range s.parties(sv) {
					keys[sv.owner+"|"+sv.id+"|"+w] = true
				}
			}
			return sortedKeys(keys), nil
		},
		Do: func(ctx context.Context, w *replay.Walk, item string, page *string) (bool, error) {
			parts := strings.Split(item, "|")
			window := s.pendingWindow()
			keep := func(o *replay.Output) bool { return s.replayable(b, o.Tx, window) }
			for {
				q := map[string]string{"bucket": b, "owner": parts[0], "id": parts[1], "writer": parts[2], "after": *page}
				outs, err := w.Each(ctx, "entry", storerec.LookupService, q, keep)
				if err != nil {
					return false, err
				}
				if len(outs) == 0 {
					return true, nil
				}
				*page = outs[len(outs)-1].Txid.String()
				if len(outs) < PageLimit {
					return true, nil
				}
			}
		}}
}

// replayable reports whether a carrier the peer answered is one to replay
// here: dated within window of the host's clock (no older than window, no
// later than ReplaySkew ahead; window 0 asks nothing of the date), and on a
// funding outpoint this host has not decided otherwise, by a retraction
// mark or another first carrier.
func (s *Service) replayable(b string, tx *transaction.Transaction, window time.Duration) bool {
	if tx == nil || len(tx.Inputs) == 0 || len(tx.Outputs) == 0 || tx.Outputs[0].LockingScript == nil {
		return false
	}
	script := []byte(*tx.Outputs[0].LockingScript)
	kind := storerec.Classify(script)
	switch kind {
	case storerec.ClaimEntry, storerec.ClaimOutcome, storerec.ClaimFront:
	default:
		return false
	}
	op := outpoint(srcTxid(tx.Inputs[0]), tx.Inputs[0].SourceTxOutIndex)
	c := cOf(tx.TxID().String())
	s.mu.Lock()
	now := s.nowMs()
	_, marked := s.marks.get(op)
	row, ruled := s.rows.get(op)
	s.mu.Unlock()
	if marked || (ruled && row.c != c) {
		return false
	}
	if window <= 0 {
		return true
	}
	raw, _ := pushdrop.FirstPush(script)
	e, err := carrierOf(b, string(kind), raw, tx.TxID().String(), c, op, now)
	if err != nil {
		return false
	}
	created := int64(e.created) //nolint:gosec // a record's time is far below 2^63
	return created > now-window.Milliseconds() && created <= now+ReplaySkew.Milliseconds()
}

// ---- phases 5 and 6: what hangs off what the host holds

// pendingWindow and otherWindow are how far back a carrier is replayed: an
// entry no token names, and an outcome or a statement.
func (s *Service) pendingWindow() time.Duration {
	return time.Duration(s.pending)*time.Millisecond - ReplayPendingMargin
}

func (s *Service) otherWindow() time.Duration {
	return time.Duration(s.other)*time.Millisecond - ReplayOtherMargin
}

// freshMined answers whether a mined output a peer answered is one to
// replay: proven, within ReplayMinedBlocks of the tip, read once.
func (s *Service) freshMined(ctx context.Context) func(*replay.Output) bool {
	var tip uint32
	tipKnown := false
	return func(o *replay.Output) bool {
		if o.Tx.MerklePath == nil {
			return false
		}
		if !tipKnown && s.headers != nil {
			if h, err := s.headers.CurrentHeight(ctx); err == nil {
				tip, tipKnown = h, true
			}
		}
		return !tipKnown || o.Tx.MerklePath.BlockHeight+ReplayMinedBlocks >= tip
	}
}

func (s *Service) replayMined(ctx context.Context, b string) replay.Phase {
	fresh := s.freshMined(ctx)
	return replay.Phase{Name: "mined",
		Items: func(context.Context, *replay.Walk) ([]string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			keys := map[string]bool{}
			for _, e := range s.carriers.values() {
				if e.b != b || e.kind != "entry" {
					continue
				}
				if e.root != "" {
					keys["ads|"+e.root] = true
				}
				if e.hold != "" && !s.holds.has(b+"|"+e.hold) {
					keys["hold|"+e.hold] = true
				}
			}
			return sortedKeys(keys), nil
		},
		Do: func(ctx context.Context, w *replay.Walk, item string, page *string) (bool, error) {
			kind, key, _ := strings.Cut(item, "|")
			if kind == "hold" {
				_, err := w.Each(ctx, "hold", storerec.LookupService, map[string]string{"bucket": b, "hold": key}, fresh)
				return err == nil, err
			}
			for {
				outs, err := w.Each(ctx, "ad", storerec.LookupService, map[string]string{"bucket": b, "root": key, "after": *page}, fresh)
				if err != nil {
					return false, err
				}
				if len(outs) == 0 {
					return true, nil
				}
				*page = outs[len(outs)-1].Txid.String()
				if len(outs) < PageLimit {
					return true, nil
				}
			}
		}}
}

func (s *Service) replayAttached(b string) replay.Phase {
	window := s.otherWindow()
	keep := func(o *replay.Output) bool { return s.replayable(b, o.Tx, window) }
	return replay.Phase{Name: "attached",
		Items: func(context.Context, *replay.Walk) ([]string, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			keys := map[string]bool{}
			for _, ad := range s.ads.values() {
				if ad.b == b && !ad.anyone {
					keys["fronts|"+ad.advertiser] = true
					keys["outcomes-of|"+ad.advertiser] = true
				}
			}
			for _, e := range s.carriers.values() {
				if e.b == b {
					keys["sweep|"+e.op] = true
				}
			}
			return sortedKeys(keys), nil
		},
		Do: func(ctx context.Context, w *replay.Walk, item string, page *string) (bool, error) {
			kind, key, _ := strings.Cut(item, "|")
			switch kind {
			case "fronts":
				for {
					outs, err := w.Each(ctx, "front", storerec.LookupService, map[string]string{"bucket": b, "advertiser": key, "after": *page}, keep)
					if err != nil {
						return false, err
					}
					if len(outs) == 0 {
						return true, nil
					}
					*page = outs[len(outs)-1].Txid.String()
					if len(outs) < PerSmall {
						return true, nil
					}
				}
			case "outcomes-of":
				_, err := w.Each(ctx, "outcome", storerec.LookupService, map[string]string{"bucket": b, "provider": key}, keep)
				return err == nil, err
			case "sweep":
				_, err := w.Each(ctx, "sweep", storerec.LookupService, map[string]string{"bucket": b, "spent": key}, nil)
				return err == nil, err
			}
			return true, nil
		}}
}
