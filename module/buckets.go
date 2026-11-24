package module

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

// Buckets come and go on a running host without the index being built
// again: a host that adds a bucket hands over that bucket's rows alone, and
// one that drops a bucket forgets that bucket's part of the index. Every
// other bucket keeps its index as it is, so the time a change takes follows
// the size of the bucket that changed, not of the host. The result is the
// index a restart with the new buckets builds: each piece of the index is
// keyed by its bucket, and the journal's state, which is not, is loaded for
// every bucket, configured or not.

// AddBuckets configures buckets, with the served chains named for them, and
// indexes them from rows (their records topics' unspent rows, as a restore
// is handed them) and what the journal names for them. The other buckets'
// index is untouched. storage, when not nil, replaces the restore seam.
//
// Lookups may run meanwhile: they wait on the index's lock. The host must
// hold its admissions from before it reads rows until AddBuckets returns,
// or an admission between the two is missed. A failed AddBuckets forgets
// what it added, so the buckets are as they were.
func (s *Service) AddBuckets(ctx context.Context, buckets, served []string, rows []bmodule.Output, storage bmodule.RestoreStorage) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	only := map[string]bool{}
	for _, b := range buckets {
		if s.buckets[b] {
			return 0, fmt.Errorf("ls_bstore: bucket %s is configured already", b)
		}
		only[b] = true
	}
	for _, sv := range served {
		if b, _, _ := strings.Cut(sv, "|"); !only[b] {
			return 0, fmt.Errorf("ls_bstore: served chain %s is not of a bucket being added", sv)
		}
	}
	// Replaced whole: published sets are never written (publish).
	s.buckets, s.served = maps.Clone(s.buckets), maps.Clone(s.served)
	for b := range only {
		s.buckets[b] = true
	}
	for _, sv := range served {
		if s.served[sv] {
			continue
		}
		s.served[sv] = true
		s.servedList = append(s.servedList, sv)
		b, _, _ := strings.Cut(sv, "|")
		s.servedBuckets[b] = true
		s.servedIdx[sv] = newServedIdx(sv)
	}
	s.publish()
	if storage != nil {
		s.storage = storage
	}
	n, err := s.restore(ctx, rows, s.storage, only)
	if err != nil {
		s.forgetLocked(only)
		return 0, err
	}
	return n, nil
}

// ForgetBuckets stops serving buckets: they are no longer configured, their
// served chains are dropped and their part of the index is forgotten, as a
// restart without them would leave it. The journal keeps their lines.
func (s *Service) ForgetBuckets(buckets []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	drop := map[string]bool{}
	for _, b := range buckets {
		if !s.buckets[b] {
			return fmt.Errorf("ls_bstore: bucket %s is not configured", b)
		}
		drop[b] = true
	}
	s.forgetLocked(drop)
	return nil
}

// forgetLocked is ForgetBuckets with mu held and the buckets checked.
func (s *Service) forgetLocked(drop map[string]bool) {
	// Replaced whole: published sets are never written (publish).
	s.buckets, s.served = maps.Clone(s.buckets), maps.Clone(s.served)
	for b := range drop {
		delete(s.buckets, b)
	}
	kept := s.servedList[:0]
	for _, sv := range s.servedList {
		if b, _, _ := strings.Cut(sv, "|"); drop[b] {
			delete(s.served, sv)
			delete(s.servedIdx, sv)
			continue
		}
		kept = append(kept, sv)
	}
	s.servedList = kept
	for b := range drop {
		delete(s.servedBuckets, b)
	}
	s.publish()
	s.forgetIndex(drop)
	// As a restart does: the walks' tries are not kept (the positions
	// reached are, and a walk still running is fenced off by reach and
	// tried), and positions listed in a bucket no index holds go.
	for k := range s.sinceTries {
		if b, _, _ := strings.Cut(k, "|"); drop[b] {
			delete(s.sinceTries, k)
		}
	}
	s.sinceSettle()
}

// forgetIndex removes the buckets' part of what the host holds from storage.
func (s *Service) forgetIndex(drop map[string]bool) {
	of := func(k string) bool { b, _, _ := strings.Cut(k, "|"); return drop[b] }
	for k, v := range s.chains {
		if drop[v.b] {
			delete(s.chains, k)
		}
	}
	for txid, k := range s.tokenChain {
		if of(k) {
			delete(s.tokenChain, txid)
		}
	}
	for _, c := range s.carriers.keys() {
		if e, _ := s.carriers.get(c); drop[e.b] {
			removeFrom(s.byOp, e.op, e.c)
			s.carriers.del(c)
		}
	}
	for _, k := range s.ads.keys() {
		if e, _ := s.ads.get(k); drop[e.b] {
			s.ads.del(k)
		}
	}
	for _, k := range s.holds.keys() {
		if e, _ := s.holds.get(k); drop[e.b] {
			s.holds.del(k)
		}
	}
	for _, k := range s.sweeps.keys() {
		if e, _ := s.sweeps.get(k); drop[e.b] {
			s.sweeps.del(k)
		}
	}
	for _, m := range []map[string]*sortedStrings{s.outcomesBy, s.outcomeHistory, s.frontsByAdvertiser, s.frontsByPair, s.adsLive} {
		for k := range m {
			if of(k) {
				delete(m, k)
			}
		}
	}
	for k := range s.sweepsByOp {
		if of(k) {
			delete(s.sweepsByOp, k)
		}
	}
	for k := range s.heldByTx {
		if of(k) {
			delete(s.heldByTx, k)
		}
	}
	for b := range drop {
		delete(s.since, b)
	}
	s.rereads = slices.DeleteFunc(s.rereads, func(r reread) bool { return drop[r.b] })
	for k, d := range s.drops {
		if drop[d.b] {
			delete(s.drops, k)
		}
	}
}

// BucketCount is how many buckets the service serves.
func (s *Service) BucketCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buckets)
}

// BucketChange is a change of the host's buckets that the module takes in
// place (Change): the module as it is with the new buckets, and Apply,
// which makes the change.
type BucketChange struct {
	m       *Mounted
	next    *Config
	module  bmodule.Module
	added   []string
	removed []string
	served  []string
}

// Module is the module with the new buckets: the managers of the buckets
// kept are the ones running, the added buckets' are new.
func (c *BucketChange) Module() bmodule.Module { return c.module }

// Added and Removed are the buckets the change adds and drops.
func (c *BucketChange) Added() []string   { return c.added }
func (c *BucketChange) Removed() []string { return c.removed }

// Topics are the records topics of the added buckets: the topics whose
// unspent rows Apply needs.
func (c *BucketChange) Topics() []string {
	out := make([]string, 0, len(c.added))
	for _, b := range c.added {
		out = append(out, storerec.RecordsPrefix+b)
	}
	return out
}

// Apply makes the change: the removed buckets are forgotten and the added
// ones indexed from rows (Topics' unspent rows). Lookups, the terms route,
// a replay and the module's background work may run meanwhile: each waits
// on the index's lock, and a walk for a dropped bucket is fenced off. The
// host must hold its admissions from before it reads rows until Apply
// returns. A failed Apply leaves the dropped buckets forgotten and the
// added ones not added: the host then builds the module afresh.
func (c *BucketChange) Apply(ctx context.Context, rows []bmodule.Output, storage bmodule.RestoreStorage) (int, error) {
	m := c.m
	m.changeMu.Lock()
	defer m.changeMu.Unlock()
	if len(c.removed) > 0 {
		if err := m.LS.ForgetBuckets(c.removed); err != nil {
			return 0, err
		}
	}
	n := 0
	if len(c.added) > 0 {
		var err error
		if n, err = m.LS.AddBuckets(ctx, c.added, c.served, rows, storage); err != nil {
			return 0, err
		}
	}
	m.config, m.Module = c.next, c.module
	m.host.Log("bstore buckets changed in place", map[string]any{"added": orNone(c.added), "removed": orNone(c.removed), "indexed": n,
		"buckets": len(c.next.Buckets), "build": Build})
	return n, nil
}

// Change is how the module takes new settings without being built again:
// a change of BSTORE_BUCKETS, of BSTORE_SERVED for the buckets added or
// dropped, and of the topics carried (OVERLAY_TOPICS). It returns nil when
// anything else changed, or a served chain of a bucket kept: the host then
// builds the module afresh. An error is settings a new module would refuse.
func (m *Mounted) Change(getenv func(string) string) (*BucketChange, error) {
	if getenv == nil {
		return nil, nil
	}
	m.changeMu.Lock()
	defer m.changeMu.Unlock()
	var topics []string
	for _, t := range strings.Split(getenv("OVERLAY_TOPICS"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			topics = append(topics, t)
		}
	}
	if err := CheckNoSync(getenv("OVERLAY_SYNC_PEERS")); err != nil {
		return nil, err
	}
	next, err := ParseConfig(getenv)
	if err != nil {
		return nil, err
	}
	cur := m.config
	a, b := *cur, *next
	a.Buckets, b.Buckets, a.Served, b.Served = nil, nil, nil, nil
	if !reflect.DeepEqual(a, b) {
		return nil, nil
	}
	was := map[string]bool{}
	for _, x := range cur.Buckets {
		was[x] = true
	}
	now := map[string]bool{}
	for _, x := range next.Buckets {
		now[x] = true
	}
	ch := &BucketChange{m: m, next: next}
	for _, x := range next.Buckets {
		if !was[x] {
			ch.added = append(ch.added, x)
		}
	}
	for _, x := range cur.Buckets {
		if !now[x] {
			ch.removed = append(ch.removed, x)
		}
	}
	changed := func(x string) bool { return !was[x] || !now[x] }
	wasServed := map[string]bool{}
	for _, sv := range cur.Served {
		wasServed[sv] = true
	}
	nowServed := map[string]bool{}
	for _, sv := range next.Served {
		nowServed[sv] = true
		bk, _, _ := strings.Cut(sv, "|")
		if !wasServed[sv] && !changed(bk) {
			return nil, nil
		}
		if !was[bk] {
			ch.served = append(ch.served, sv)
		}
	}
	for _, sv := range cur.Served {
		if bk, _, _ := strings.Cut(sv, "|"); !nowServed[sv] && !changed(bk) {
			return nil, nil
		}
	}
	if err := checkTopics(next.Buckets, topics); err != nil {
		return nil, err
	}
	tms, _, _, err := m.managers(next, topics)
	if err != nil {
		return nil, err
	}
	ch.module = bmodule.Module{Topics: tms, Lookups: m.Module.Lookups}
	return ch, nil
}
