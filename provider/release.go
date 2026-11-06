package provider

import "github.com/lightwebinc/bstore/storerec"

// What the store gives back, and when. A piece waits for the entry that
// names its closure; a closure waits for the writer who keeps paying for
// it. Both have an end the writer decides, and the store follows it then,
// not an hour or a term later:
//
//   - A delete entry from one of the bucket's writers ends the key's put.
//     The holding it named loses that entry, and a holding no entry names
//     any more is dropped at once: its objects leave the store unless
//     another held closure names them, its export and renewals owed go
//     with it. The advertisement on chain runs out on its own; nothing
//     answers for it meanwhile (a dropped holding is not advertised).
//   - An entry declined for any reason (no closure in time, a bad proof)
//     releases the pieces that waited for it, unless another undecided
//     entry wants the same root or a held closure names them.

const decisionDeleted = "deleted by its writer"

// noteDelete applies a delete entry: every put of the same bucket and key
// made no later than the delete is ended, and what it alone held is
// dropped.
func (p *Provider) noteDelete(e *storerec.Entry) {
	p.mu.Lock()
	k := e.Bucket + "/" + e.Key
	row := &logRow{}
	if d := p.st.Deletes[k]; d == nil || d.Created < e.Created {
		p.st.Deletes[k] = &DeleteRec{Created: e.Created, At: p.now().Unix()}
		p.pruneDeletesLocked()
		row.Deletes = map[string]*DeleteRec{k: p.st.Deletes[k]}
	}
	roots := map[string]bool{}
	for _, x := range p.st.Entries {
		if x.Bucket != e.Bucket || x.Key == "" || x.Key != e.Key || x.Created > e.Created {
			continue
		}
		if x.Decision != "" && x.Decision != decisionHeld {
			continue
		}
		p.setDecision(x, decisionDeleted)
		row.Entries = append(row.Entries, *x)
		roots[x.Root] = true
		p.metrics.inc("bstore_provider_deletes_total")
	}
	for rootHex := range roots {
		p.releaseRootLocked(rootHex)
	}
	// The writer reads the answer as the delete taken: on disk first.
	seq := p.record(row)
	p.mu.Unlock()
	_ = p.wait(seq)
}

// The deletes remembered: the newest per key, and past this many the
// oldest go. A put that old and still traveling is not expected.
const maxDeletes = 100000

func (p *Provider) pruneDeletesLocked() {
	for len(p.st.Deletes) > maxDeletes {
		oldest, at := "", int64(0)
		for k, d := range p.st.Deletes {
			if oldest == "" || d.At < at {
				oldest, at = k, d.At
			}
		}
		delete(p.st.Deletes, oldest)
	}
}

// deletedOnArrival reports whether a put entry meets a delete that ended
// it before it arrived. Must be called with p.mu held.
func (p *Provider) deletedOnArrival(e *storerec.Entry) bool {
	d := p.st.Deletes[e.Bucket+"/"+e.Key]
	return d != nil && d.Created >= e.Created
}

// releaseRootLocked drops a root nothing holds any more: no held or
// undecided entry names it. Must be called with p.mu held.
func (p *Provider) releaseRootLocked(rootHex string) {
	for _, x := range p.st.Entries {
		if x.Root == rootHex && (x.Decision == "" || x.Decision == decisionHeld) {
			return
		}
	}
	if h := p.st.Holdings[rootHex]; h != nil {
		h.Entries, h.Ad, h.Lease = nil, nil, 0
		for _, pa := range p.st.PendingAds {
			if pa.Root == rootHex {
				vout := pa.AdVout
				p.record(&logRow{AdDone: pa.Txid, AdDoneVout: &vout})
			}
		}
		p.st.PendingAds = deleteIf(p.st.PendingAds, func(pa PendingAd) bool { return pa.Root == rootHex })
		p.st.Owed = deleteIf(p.st.Owed, func(o Owed) bool { return o.Root == rootHex })
		p.recordOwedLocked()
		p.dropHoldingLocked(rootHex)
	}
	p.releasePiecesLocked(rootHex)
}

// releasePiecesLocked drops the unclaimed pieces that waited for a root no
// undecided entry wants. Must be called with p.mu held.
func (p *Provider) releasePiecesLocked(rootHex string) {
	if len(p.index.undecided[rootHex]) > 0 {
		return
	}
	var gone []string
	for h, u := range p.st.Unclaimed {
		if u.Root != rootHex || p.heldObjectLocked(h) {
			continue
		}
		_ = p.store.Delete(mustHash(h))
		delete(p.st.Unclaimed, h)
		gone = append(gone, h)
	}
	if len(gone) > 0 {
		p.record(&logRow{Claimed: gone})
		p.logf("root %s: %d piece(s) that waited for it released", short(rootHex), len(gone))
		p.metrics.inc("bstore_provider_released_total")
	}
}

func deleteIf[T any](xs []T, drop func(T) bool) []T {
	out := xs[:0]
	for _, x := range xs {
		if !drop(x) {
			out = append(out, x)
		}
	}
	return out
}
