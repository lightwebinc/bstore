package publish

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lightwebinc/bcommon/funding"
)

// The state on disk is two parts. The hot part, publisher.json, is what a
// put changes: the funding trees without their bytes, a row per hold
// output, the buckets, sweeps, uploads and payments; it is written as a
// whole on every save (save.go). The cold part is each transaction the
// state keeps (a hold transaction, a funding tree), its bytes and its
// proof, in its own file txs/<txid>.json, written when the transaction is
// minted and again when it mines, and removed once no record names it. A
// hold transaction of 1,024 outputs is 100 KB of hex and a put used to
// encode every one of them, with every tree, to mark one output used.
//
// A state an earlier build saved in one file, the transaction on each hold
// record or once in holdTxs, loads the same; the first save after that
// writes the two parts (and publisher.json no longer carries the bytes).

// txRecord is a transaction as a cold file keeps it: the raw bytes, the
// proof once mined, the BEEF a funding tree carries while unmined, and the
// height. It is also the shape of an earlier build's holdTxs.
type txRecord struct {
	Raw    string `json:"raw"`
	Bump   string `json:"bump,omitempty"`
	Beef   string `json:"beef,omitempty"`
	Height uint32 `json:"height,omitempty"`
}

// coldKey is what a save compares a transaction against the cold file it
// last wrote or read by: the raw bytes never change for a txid, so their
// length says whether they are there; the proof and the BEEF change once,
// when the transaction mines.
type coldKey struct {
	raw, beef int
	bump      string
	height    uint32
}

func (r txRecord) key() coldKey {
	return coldKey{raw: len(r.Raw), beef: len(r.Beef), bump: r.Bump, height: r.Height}
}

// noteTx records a transaction in refs, keeping the copy with a proof when
// one copy has it and another has not.
func noteTx(refs map[string]txRecord, txid string, r txRecord) {
	if have, ok := refs[txid]; !ok || (have.Bump == "" && r.Bump != "") {
		refs[txid] = r
	}
}

// holdGroup is one hold transaction's outputs as the hot file keeps them:
// the transaction named once, and a row an output: [vout, used, entry,
// reclaim], the trailing fields that are zero left out.
type holdGroup struct {
	Txid string    `json:"txid"`
	Sats uint64    `json:"sats"`
	Sent int64     `json:"sent,omitempty"`
	Outs []holdRow `json:"outs"`
}

type holdRow struct {
	Vout    uint32
	Used    bool
	Entry   string
	Reclaim *ReclaimRecord
}

func (r *holdRow) UnmarshalJSON(b []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(b, &parts); err != nil {
		return err
	}
	if len(parts) == 0 {
		return errors.New("a hold row names no output")
	}
	for i, dst := range []any{&r.Vout, &r.Used, &r.Entry, &r.Reclaim} {
		if i >= len(parts) {
			break
		}
		if err := json.Unmarshal(parts[i], dst); err != nil {
			return err
		}
	}
	return nil
}

// appendHoldGroups appends the hold records to b as the hot file keeps them
// and notes each hold transaction in refs. Records of one transaction, in a
// run, are one group; a hold transaction of 1,024 outputs minted ahead is
// about 7 bytes an output here, 100 bytes a record as JSON objects.
func appendHoldGroups(b []byte, holds []HoldRecord, refs map[string]txRecord) ([]byte, error) {
	if len(holds) == 0 {
		return b, nil
	}
	b = append(b, '[')
	for i := range holds {
		h := &holds[i]
		if i == 0 || h.Txid != holds[i-1].Txid || h.Sats != holds[i-1].Sats || h.Sent != holds[i-1].Sent {
			// The records of a transaction carry one copy of it (markMined
			// sets them all), so a run is noted once.
			noteTx(refs, h.Txid, txRecord{Raw: h.RawHex, Bump: h.BumpHex, Height: h.Height})
			if i > 0 {
				b = append(b, ']', '}', ',')
			}
			b = append(b, `{"txid":`...)
			b = appendJSONString(b, h.Txid)
			b = append(b, `,"sats":`...)
			b = strconv.AppendUint(b, h.Sats, 10)
			if h.Sent != 0 {
				b = append(b, `,"sent":`...)
				b = strconv.AppendInt(b, h.Sent, 10)
			}
			b = append(b, `,"outs":[`...)
		} else {
			b = append(b, ',')
		}
		b = append(b, '[')
		b = strconv.AppendUint(b, uint64(h.Vout), 10)
		n := 0
		switch {
		case h.Reclaim != nil:
			n = 3
		case h.Entry != "":
			n = 2
		case h.Used:
			n = 1
		}
		if n >= 1 {
			b = append(b, ',')
			b = strconv.AppendBool(b, h.Used)
		}
		if n >= 2 {
			b = append(b, ',')
			b = appendJSONString(b, h.Entry)
		}
		if n >= 3 {
			rc, err := json.Marshal(h.Reclaim)
			if err != nil {
				return nil, err
			}
			b = append(b, ',')
			b = append(b, rc...)
		}
		b = append(b, ']')
	}
	return append(b, ']', '}', ']'), nil
}

// appendJSONString appends s as a JSON string: copied when it is plain
// ASCII with nothing to escape (a txid, a commitment), encoded otherwise.
func appendJSONString(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == '"' || c == '\\' || c >= 0x7f {
			q, _ := json.Marshal(s)
			return append(b, q...)
		}
	}
	b = append(b, '"')
	b = append(b, s...)
	return append(b, '"')
}

// eachTree calls f on every copy of a funding tree the state keeps.
func (s *state) eachTree(f func(t *funding.Tree)) {
	for _, ts := range []*treeSet{&s.Piece, &s.Entry, &s.Outcome} {
		if ts.Tree != nil {
			f(ts.Tree)
		}
		for i := range ts.Trees {
			f(&ts.Trees[i])
		}
		for i := range ts.Ahead {
			f(&ts.Ahead[i])
		}
	}
	for i := range s.Pending {
		f(&s.Pending[i].Tree)
	}
}

func stripTree(t *funding.Tree) { t.RawHex, t.BumpHex, t.BeefHex = "", "", "" }

func (ts treeSet) hot() treeSet {
	c := treeSet{Trees: append([]funding.Tree(nil), ts.Trees...), Ahead: append([]funding.Tree(nil), ts.Ahead...)}
	if ts.Tree != nil {
		t := *ts.Tree
		stripTree(&t)
		c.Tree = &t
	}
	for i := range c.Trees {
		stripTree(&c.Trees[i])
	}
	for i := range c.Ahead {
		stripTree(&c.Ahead[i])
	}
	return c
}

// hot is the state as publisher.json keeps it, without the hold rows, which
// the saver writes after it (saver.encode): the trees without their bytes.
// The transactions go to the cold files.
func (s *state) hot() *state {
	c := *s
	c.Holds, c.HoldTxs, c.HoldOutputs = nil, nil, nil
	c.Piece, c.Entry, c.Outcome = s.Piece.hot(), s.Entry.hot(), s.Outcome.hot()
	c.Pending = append([]pendingTree(nil), s.Pending...)
	for i := range c.Pending {
		stripTree(&c.Pending[i].Tree)
	}
	return &c
}

// refs notes every transaction the state keeps in refs, as the cold files
// keep it; rows is the hold rows for hot.
func (s *state) refs(rows []byte, refs map[string]txRecord) ([]byte, error) {
	rows, err := appendHoldGroups(rows, s.Holds, refs)
	if err != nil {
		return nil, err
	}
	s.eachTree(func(t *funding.Tree) {
		noteTx(refs, t.Txid, txRecord{Raw: t.RawHex, Bump: t.BumpHex, Beef: t.BeefHex, Height: t.Height})
	})
	return rows, nil
}

// expand gives the state its shape in memory from what was read: the hold
// records of each group, and every record and tree without its transaction
// given it by cold. Records an earlier build saved with the transaction on
// them, or once in HoldTxs, keep what they carry.
func (s *state) expand(cold func(txid string) (txRecord, error)) error {
	for i := range s.Holds {
		h := &s.Holds[i]
		if t, ok := s.HoldTxs[h.Txid]; ok && h.RawHex == "" {
			h.RawHex, h.BumpHex, h.Height = t.Raw, t.Bump, t.Height
		}
	}
	s.HoldTxs = nil
	if len(s.HoldOutputs) > 0 {
		var groups []holdGroup
		if err := json.Unmarshal(s.HoldOutputs, &groups); err != nil {
			return fmt.Errorf("holdOutputs: %w", err)
		}
		for _, g := range groups {
			for _, r := range g.Outs {
				s.Holds = append(s.Holds, HoldRecord{Txid: g.Txid, Vout: r.Vout, Sats: g.Sats, Sent: g.Sent,
					Used: r.Used, Entry: r.Entry, Reclaim: r.Reclaim})
			}
		}
		s.HoldOutputs = nil
	}
	for i := range s.Holds {
		h := &s.Holds[i]
		if h.RawHex != "" || h.BumpHex != "" {
			continue
		}
		t, err := cold(h.Txid)
		if err != nil {
			return err
		}
		h.RawHex, h.BumpHex = t.Raw, t.Bump
		if h.Height == 0 {
			h.Height = t.Height
		}
	}
	var err error
	s.eachTree(func(t *funding.Tree) {
		if err != nil || t.RawHex != "" || t.BumpHex != "" {
			return
		}
		var r txRecord
		if r, err = cold(t.Txid); err != nil {
			return
		}
		t.RawHex, t.BumpHex, t.BeefHex = r.Raw, r.Bump, r.Beef
		if t.Height == 0 {
			t.Height = r.Height
		}
	})
	return err
}

// coldPath is a transaction's cold file; an id that cannot name a file is
// refused.
func coldPath(dir, txid string) (string, error) {
	if txid == "" || txid == "." || txid == ".." || strings.ContainsAny(txid, `/\`) {
		return "", fmt.Errorf("publish: transaction id %q cannot name a file", txid)
	}
	return filepath.Join(dir, "txs", txid+".json"), nil
}

// loadState reads an identity's state, in either layout.
func loadState(dir string) (*state, error) {
	st, _, err := readState(dir)
	return st, err
}

// readState reads an identity's state: publisher.json, and the cold file
// of every transaction the records do not carry. It returns the key of
// each cold file read, for the saver to compare the state against, and
// removes the cold files the state does not name (left by a stop between
// a write and the removal after it).
func readState(dir string) (*state, map[string]coldKey, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "publisher.json"))
	if errors.Is(err, os.ErrNotExist) {
		return &state{}, map[string]coldKey{}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var st state
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, nil, fmt.Errorf("publisher.json: %w", err)
	}
	known := map[string]coldKey{}
	read := map[string]txRecord{}
	cold := func(txid string) (txRecord, error) {
		if r, ok := read[txid]; ok {
			return r, nil
		}
		path, err := coldPath(dir, txid)
		if err != nil {
			return txRecord{}, err
		}
		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			return txRecord{}, fmt.Errorf("publisher state: transaction %s is named and txs/%s.json is not there", short(txid), txid)
		}
		if err != nil {
			return txRecord{}, err
		}
		var r txRecord
		if err := json.Unmarshal(b, &r); err != nil {
			return txRecord{}, fmt.Errorf("txs/%s.json: %w", txid, err)
		}
		read[txid], known[txid] = r, r.key()
		return r, nil
	}
	if err := st.expand(cold); err != nil {
		return nil, nil, err
	}
	named := map[string]bool{}
	for _, h := range st.Holds {
		named[h.Txid] = true
	}
	st.eachTree(func(t *funding.Tree) { named[t.Txid] = true })
	if ents, err := os.ReadDir(filepath.Join(dir, "txs")); err == nil {
		for _, e := range ents {
			if txid, ok := strings.CutSuffix(e.Name(), ".json"); ok && !named[txid] {
				_ = os.Remove(filepath.Join(dir, "txs", e.Name()))
			}
		}
	}
	return &st, known, nil
}
