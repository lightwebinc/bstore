package provider

// entryIndex is what the hot paths ask of the entries, kept as they change:
// a piece must not walk the table, which holds every object this provider
// took. It is built from the state at start and not saved.
type entryIndex struct {
	// undecided are the entries with no decision, by root and commitment;
	// writers counts them by writer and total in all.
	undecided map[string]map[string]*EntryRec
	writers   map[string]int
	total     int
	// holds is the entry held on each hold output (spec section 9.2).
	holds map[string]string
}

func newEntryIndex(entries map[string]*EntryRec) *entryIndex {
	x := &entryIndex{undecided: map[string]map[string]*EntryRec{}, writers: map[string]int{}, holds: map[string]string{}}
	for _, e := range entries {
		x.add(e)
	}
	return x
}

// add takes in an entry as it stands.
func (x *entryIndex) add(e *EntryRec) {
	switch e.Decision {
	case "":
		set := x.undecided[e.Root]
		if set == nil {
			set = map[string]*EntryRec{}
			x.undecided[e.Root] = set
		}
		if set[e.C] == nil {
			set[e.C] = e
			x.writers[e.Writer]++
			x.total++
		}
	case decisionHeld:
		if e.Hold != nil {
			x.holds[e.Hold.Outpoint()] = e.C
		}
	}
}

// drop forgets an entry as it stands.
func (x *entryIndex) drop(e *EntryRec) {
	switch e.Decision {
	case "":
		set := x.undecided[e.Root]
		if set[e.C] == nil {
			return
		}
		delete(set, e.C)
		if len(set) == 0 {
			delete(x.undecided, e.Root)
		}
		if x.writers[e.Writer]--; x.writers[e.Writer] <= 0 {
			delete(x.writers, e.Writer)
		}
		x.total--
	case decisionHeld:
		if e.Hold != nil && x.holds[e.Hold.Outpoint()] == e.C {
			delete(x.holds, e.Hold.Outpoint())
		}
	}
}

// setDecision records an entry's decision; the caller holds p.mu.
func (p *Provider) setDecision(e *EntryRec, decision string) {
	p.index.drop(e)
	e.Decision = decision
	p.index.add(e)
}
