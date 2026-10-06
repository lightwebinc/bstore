package storerec

import (
	"encoding/hex"

	"github.com/lightwebinc/bcommon/cbor"
)

// The views below are each record's fields as plain JSON values: hex for
// bytes, numbers for integers (every one is at most 2^53 - 1), the
// preserved keys as the hex of their CBOR map ("" for none). The vectors
// carry them and the TypeScript twin writes the same view of what it
// decodes.

func hx(b []byte) string     { return hex.EncodeToString(b) }
func hx32(b [32]byte) string { return hex.EncodeToString(b[:]) }

func extraHex(m cbor.Map) string {
	if len(m) == 0 {
		return ""
	}
	b, err := cbor.Encode(m)
	if err != nil {
		return "!"
	}
	return hx(b)
}

// View is the bucket record's view.
func (r *Bucket) View() map[string]any {
	writers := make([]string, len(r.Writers))
	for i, w := range r.Writers {
		writers[i] = hx(w)
	}
	changes := make([][]string, len(r.Changes))
	for i, c := range r.Changes {
		if c.Removal {
			changes[i] = []string{c.Key, hx32(zero32), hx32(c.C)}
		} else {
			changes[i] = []string{c.Key, hx32(c.C)}
		}
	}
	consumed := make([]string, len(r.Consumed))
	for i, c := range r.Consumed {
		consumed[i] = hx32(c)
	}
	return map[string]any{"bucket": r.Bucket, "owner": hx(r.Owner), "id": hx32(r.ID), "seq": r.Seq, "prev": hx32(r.Prev),
		"writers": writers, "changes": changes, "count": r.Count, "mapRoot": hx32(r.MapRoot), "created": r.Created,
		"consumed": consumed, "extra": extraHex(r.Extra)}
}

// View is the hold terms' view.
func (h *Hold) View() map[string]any {
	return map[string]any{"txid": hx32(h.Txid), "vout": h.Vout, "satoshis": h.Satoshis, "t": h.T, "K": h.K,
		"retention": h.Retention, "deadline": h.Deadline, "checkC": h.CheckC}
}

// View is the entry's view.
func (e *Entry) View() map[string]any {
	v := map[string]any{"bucket": e.Bucket, "id": hx32(e.ID), "writer": hx(e.Writer), "key": e.Key, "kind": e.Kind,
		"created": e.Created, "extra": extraHex(e.Extra), "unknown": extraHex(e.Unknown)}
	if e.Kind == KindPut {
		v["size"], v["root"], v["contentHash"], v["proofRoot"] = e.Size, hx32(e.Root), hx32(e.ContentHash), hx32(e.ProofRoot)
		v["proofProfile"], v["contentType"] = e.ProofProfile, e.ContentType
		if e.ETag != nil {
			v["etag"] = *e.ETag
		}
		if e.Meta != nil {
			m := make([][]string, len(e.Meta))
			for i, p := range e.Meta {
				m[i] = []string{p.Key, p.Value}
			}
			v["meta"] = m
		}
		if e.Hold != nil {
			v["hold"] = e.Hold.View()
		}
	}
	return v
}

// View is the piece's view; the piece itself is given by its hash and
// length.
func (p *Piece) View() map[string]any {
	h := p.Hash()
	return map[string]any{"bucket": p.Bucket, "writer": hx(p.Writer), "root": hx32(p.Root), "pieceHash": hx32(h),
		"pieceLength": len(p.Piece), "extra": extraHex(p.Extra)}
}

// View is the front statement's view.
func (s *Front) View() map[string]any {
	return map[string]any{"bucket": s.Bucket, "front": hx(s.Front), "advertiser": hx(s.Advertiser), "url": s.URL,
		"created": s.Created, "extra": extraHex(s.Extra)}
}

// View is the request's view.
func (q *Request) View() map[string]any {
	v := map[string]any{"root": hx32(q.Root), "proofRoot": hx32(q.ProofRoot), "size": q.Size, "provider": hx(q.Provider),
		"answerer": hx(q.Answerer), "seedHeight": q.SeedHeight, "seedHash": hx32(q.SeedHash), "nonce": hx(q.Nonce[:]), "c": q.C}
	if q.Range != nil {
		v["range"] = []uint64{q.Range[0], q.Range[1]}
	}
	return v
}

// View is the outcome's view.
func (o *Outcome) View() map[string]any {
	v := map[string]any{"bucket": o.Bucket, "buyer": hx(o.Buyer), "created": o.Created, "request": hx(o.Request),
		"outcome": o.Outcome, "extra": extraHex(o.Extra)}
	if o.Response != nil {
		v["response"] = hx32(*o.Response)
	}
	if o.Signature != nil {
		v["signature"] = hx(o.Signature)
	}
	return v
}

// View is the response body's view.
func (r *Body) View() map[string]any {
	samples := make([]map[string]any, len(r.Samples))
	for i, s := range r.Samples {
		sib := make([]string, len(s.Siblings))
		for j, h := range s.Siblings {
			sib[j] = hx32(h)
		}
		var seg any
		if s.Segment != nil {
			seg = hx(s.Segment)
		}
		samples[i] = map[string]any{"segment": seg, "siblings": sib}
	}
	return map[string]any{"request": hx32(r.Request), "samples": samples, "answerer": hx(r.Answerer)}
}
