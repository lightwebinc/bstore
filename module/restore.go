package module

import (
	"context"
	"sort"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

type restoreRow struct {
	o     bmodule.Output
	sweep bool
}

// isSweep is whether a row is output 0 of a funding transaction, read from
// the row's script when it carries one.
func isSweep(o bmodule.Output, read func(bmodule.Output) (*transaction.Transaction, error)) (bool, error) {
	if o.Outpoint.Index != 0 {
		return false, nil
	}
	if o.OutputScript != nil {
		return storerec.Classify(*o.OutputScript) == storerec.ClaimFunding, nil
	}
	tx, err := read(o)
	if err != nil || tx == nil || len(tx.Outputs) == 0 || tx.Outputs[0] == nil || tx.Outputs[0].LockingScript == nil {
		return false, err
	}
	return storerec.Classify(*tx.Outputs[0].LockingScript) == storerec.ClaimFunding, nil
}

// scoreMs is a stored row's score as the host's first sight in Unix
// milliseconds: the TypeScript engine scores in milliseconds, the Go
// engine in microseconds, and a score past the year 5138 in milliseconds
// is taken for microseconds.
func scoreMs(score float64) int64 {
	if score >= 1e14 {
		return int64(score / 1000)
	}
	return int64(score)
}

// Restore implements module.Restorer (spec section 12.5): it rebuilds the
// index. The journal's state, then every output of this module's topics the
// host hands over (a row handed without its BEEF is read again with it),
// then every token and mined output the journal names that the unspent
// rows did not carry, and then each held token's predecessors through the
// input that spends them. A token is read as the engine stores it. Chains
// are rooted and folded from seq 0 whatever order storage returns rows in.
// A storage failure propagates: a restore that skipped a sweep would answer
// retracted carriers.
func (s *Service) Restore(ctx context.Context, outputs []bmodule.Output, storage bmodule.RestoreStorage) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clear()
	lines, err := s.journal.Load()
	if err != nil {
		return 0, err
	}
	for i := range lines {
		s.apply(&lines[i])
	}
	s.storage = storage
	s.ready = false
	seen := map[string]bool{}
	key := func(topic, txid string, vout uint32) string { return topic + "|" + outpoint(txid, vout) }
	var queue []bmodule.Output
	for _, o := range outputs {
		k := key(o.Topic, o.Outpoint.Txid.String(), o.Outpoint.Index)
		if _, ok := s.bucketOf(o.Topic); !ok || seen[k] {
			continue
		}
		seen[k] = true
		queue = append(queue, o)
	}
	type named struct {
		topic, txid string
		vout        uint32
	}
	var names []named
	for _, txid := range s.tokenSeen.keys() {
		if t, _ := s.tokenSeen.get(txid); s.buckets[t.b] {
			names = append(names, named{storerec.RecordsPrefix + t.b, txid, t.vout})
		}
	}
	// Every mined output the journal names, spent or not: the engine marks an
	// output spent before it stores the transaction that spends it, so a
	// crash between leaves one spent that no w line names, and the unspent
	// rows never carry it.
	for _, k := range s.minedSeen.keys() {
		if o, _ := s.minedSeen.get(k); s.buckets[o.b] {
			names = append(names, named{storerec.RecordsPrefix + o.b, o.txid, o.vout})
		}
	}
	for _, n := range names {
		k := key(n.topic, n.txid, n.vout)
		if seen[k] {
			continue
		}
		seen[k] = true
		h, err := chainhash.NewHashFromHex(n.txid)
		if err != nil {
			continue
		}
		o, err := storage.FindOutput(ctx, &transaction.Outpoint{Txid: *h, Index: n.vout}, n.topic)
		if err != nil {
			return 0, err
		}
		if o != nil {
			queue = append(queue, *o)
		}
	}
	read := func(o bmodule.Output) (*transaction.Transaction, error) {
		row := o
		if row.Beef == nil {
			full, err := storage.FindOutput(ctx, &row.Outpoint, row.Topic)
			if err != nil {
				return nil, err
			}
			if full != nil {
				row = *full
			}
		}
		if row.Beef == nil {
			return nil, nil
		}
		tx, err := parse(row.Beef)
		if err != nil {
			return nil, nil
		}
		return tx, nil
	}
	// A row is sorted on what it says of itself and parsed when its turn
	// comes: a carrier's transaction parses with its whole parent, a hold
	// transaction of 1,024 outputs, and hundreds of thousands of them held
	// for the sort are gigabytes, on a host that restarts the engine.
	rows := make([]restoreRow, 0, len(queue))
	for _, o := range queue {
		sweep, err := isSweep(o, read)
		if err != nil {
			return 0, err
		}
		rows = append(rows, restoreRow{o: o, sweep: sweep})
	}
	// Everything else first, in storage's order; sweeps after them.
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.sweep != b.sweep {
			return !a.sweep
		}
		if a.o.Score != b.o.Score {
			return a.o.Score < b.o.Score
		}
		return a.o.Outpoint.Txid.String() < b.o.Outpoint.Txid.String()
	})
	indexed := 0
	s.restoring = true
	take := func(o bmodule.Output, tx *transaction.Transaction) {
		b, _ := s.bucketOf(o.Topic)
		txid := o.Outpoint.Txid.String()
		k := heldKey(b, txid, o.Outpoint.Index)
		if o.Spent && s.minedSeen.has(k) && !s.spentHeld[k] {
			s.write(Line{K: "w", B: b, Txid: txid, Vout: int64(o.Outpoint.Index)})
		}
		at := s.nowMs()
		if o.Score > 0 {
			at = scoreMs(o.Score)
		}
		if s.index(b, tx, o.Outpoint.Index, at) {
			indexed++
		}
	}
	for _, r := range rows {
		tx, err := read(r.o)
		if err != nil {
			s.restoring = false
			return 0, err
		}
		if tx == nil {
			s.host.Log("ls_bstore could not restore an output without its transaction", map[string]any{"topic": r.o.Topic, "outpoint": r.o.Outpoint.String()})
			continue
		}
		take(r.o, tx)
	}
	// Each held token's predecessors, through the input that spends them.
	type step struct {
		topic string
		tx    *transaction.Transaction
		prev  string
	}
	var walk []step
	next := func(o bmodule.Output, tx *transaction.Transaction) {
		txid := o.Outpoint.Txid.String()
		if k, ok := s.tokenChain[txid]; ok {
			if t := s.chains[k].byTxid[txid]; t != nil && t.vout == o.Outpoint.Index && t.prev != "" {
				walk = append(walk, step{o.Topic, tx, t.prev})
			}
		}
	}
	for _, r := range rows {
		txid := r.o.Outpoint.Txid.String()
		k, ok := s.tokenChain[txid]
		if !ok {
			continue
		}
		if t := s.chains[k].byTxid[txid]; t == nil || t.vout != r.o.Outpoint.Index || t.prev == "" {
			continue
		}
		tx, err := read(r.o)
		if err != nil {
			s.restoring = false
			return 0, err
		}
		if tx != nil {
			next(r.o, tx)
		}
	}
	for len(walk) > 0 {
		w := walk[len(walk)-1]
		walk = walk[:len(walk)-1]
		if _, held := s.tokenChain[w.prev]; held {
			continue
		}
		var in *transaction.TransactionInput
		for _, i := range w.tx.Inputs {
			if srcTxid(i) == w.prev {
				in = i
				break
			}
		}
		if in == nil {
			continue
		}
		h, err := chainhash.NewHashFromHex(w.prev)
		if err != nil {
			continue
		}
		o, err := storage.FindOutput(ctx, &transaction.Outpoint{Txid: *h, Index: in.SourceTxOutIndex}, w.topic)
		if err != nil {
			s.restoring = false
			return 0, err
		}
		if o == nil {
			continue
		}
		tx, err := read(*o)
		if err != nil {
			s.restoring = false
			return 0, err
		}
		if tx == nil {
			continue
		}
		take(*o, tx)
		next(*o, tx)
	}
	s.restoring = false
	keys := make([]string, 0, len(s.chains))
	for k := range s.chains {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := s.chains[k]
		for _, t := range append([]*tokenEntry(nil), v.bySeq[0]...) {
			s.rootFrom(v, t, true)
		}
		for _, t := range v.byTxid {
			if !t.rooted {
				s.counted(v, t)
			}
		}
		for _, seq := range append([]uint64(nil), v.seqs.a...) {
			s.bound(v, seq)
		}
	}
	// Later carriers on an outpoint stay only where a served chain names them.
	for _, c := range s.recheck {
		e, ok := s.carriers.get(c)
		if !ok {
			continue
		}
		if r, ok := s.rows.get(e.op); (ok && r.c == c) || s.namedByServed(e.b, c) {
			continue
		}
		s.drop(e, "second-carrier")
	}
	s.recheck = nil
	s.drain(ctx)
	s.flush(ctx)
	s.ready = true
	return indexed, nil
}
