package provider

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// adRow is what the journal keeps of one advertisement: the pending record,
// and the holding and undecided entries it answers for. The lease, the
// funding output and the root's entries reach the disk with it, before the
// transaction reaches the settlement leg (BRC-167), so a crash that loses the
// state's latest changes still leaves an advertisement on chain answered for.
//
// A row is a file of its own, data/ads/<txid>.json, synced before the send
// and removed once a save has the same facts in state.json: per
// advertisement that is one small file against a state written whole. An
// entry a writer is answered for is a row of its own too (no
// advertisement, the one entry: data/ads/entry-<c>.json), so the answer
// waits for one small sync, not for the whole state (2026-11-05: a save
// per entry cost every entry what the state had grown to).
type adRow struct {
	Ad      PendingAd  `json:"ad"`
	Holding Holding    `json:"holding"`
	Entries []EntryRec `json:"entries,omitempty"`
}

// journalAd writes pa's row, then records pa as pending.
func (p *Provider) journalAd(pa PendingAd) error {
	p.mu.Lock()
	h := p.st.Holdings[pa.Root]
	row := adRow{Ad: pa}
	if h != nil {
		row.Holding = *h
		row.Holding.Entries = nil
		for _, c := range h.Entries {
			if e := p.st.Entries[c]; e != nil && e.Decision == "" {
				row.Entries = append(row.Entries, *e)
			}
		}
	}
	raw, err := json.Marshal(row)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	file := filepath.Join(p.adDir, pa.Txid+".json")
	if err := os.MkdirAll(p.adDir, 0o700); err != nil {
		return err
	}
	if err := writeFileAtomic(file, raw); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.markSpent(pa.Tree, pa.Vout)
	p.st.PendingAds = append(p.st.PendingAds, pa)
	p.adFiles = append(p.adFiles, file)
	p.dirty = true
	return nil
}

// journalEntry writes entry c's row, for a writer answered that the entry
// is taken.
func (p *Provider) journalEntry(c string) error {
	p.mu.Lock()
	e := p.st.Entries[c]
	if e == nil {
		p.mu.Unlock()
		return nil
	}
	raw, err := json.Marshal(adRow{Entries: []EntryRec{*e}})
	p.mu.Unlock()
	if err != nil {
		return err
	}
	file := filepath.Join(p.adDir, "entry-"+c+".json")
	if err := os.MkdirAll(p.adDir, 0o700); err != nil {
		return err
	}
	if err := writeFileAtomic(file, raw); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.adFiles = append(p.adFiles, file)
	p.dirty = true
	return nil
}

// replayAds brings back what the state lacks of the rows a stopped run left,
// and returns their files, for the next save to cover. A row whose
// advertisement the state already holds, pending or finished, adds nothing.
func (st *State) replayAds(dir string, logf func(string, ...any)) ([]string, error) {
	names, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []string
	for _, n := range names {
		file := filepath.Join(dir, n.Name())
		if !strings.HasSuffix(n.Name(), ".json") {
			continue // a write a crash cut off before its rename
		}
		raw, err := os.ReadFile(file) //nolint:gosec // the provider's own data directory
		if err != nil {
			return nil, err
		}
		var row adRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		files = append(files, file)
		st.replayAd(&row)
		logf("bstore-provider: advertisement %s: journal row read", short(row.Ad.Txid))
	}
	return files, nil
}

func (st *State) replayAd(row *adRow) {
	pa := row.Ad
	if pa.Txid == "" {
		// An entry's row: the entry, if the state lost it.
		for i := range row.Entries {
			e := row.Entries[i]
			if st.Entries[e.C] == nil {
				st.Entries[e.C] = &e
			}
		}
		return
	}
	for i := range st.PendingAds {
		if st.PendingAds[i].Txid == pa.Txid {
			return
		}
	}
	h := st.Holdings[pa.Root]
	if h != nil && h.Ad != nil && h.Ad.Expiry >= pa.Expiry {
		return
	}
	if h == nil {
		c := row.Holding
		h = &c
		st.Holdings[pa.Root] = h
	}
	h.Lease = max(h.Lease, pa.Expiry)
	for i := range row.Entries {
		e := row.Entries[i]
		if st.Entries[e.C] == nil {
			st.Entries[e.C] = &e
		}
	}
	st.markSpent(pa.Tree, pa.Vout)
	st.PendingAds = append(st.PendingAds, pa)
}

// dropJournal removes rows a save covered. One left behind is read at the
// next start and adds nothing.
func dropJournal(files []string) {
	for _, f := range files {
		_ = os.Remove(f)
	}
}
