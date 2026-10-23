package module

import "sort"

// sortedStrings is strings in ascending byte order, each once: the lists
// the index pages from. Each answer starts with a binary search and walks
// at most a page, plus what aged out of its window since the last
// retention pass, which it takes out as it meets it.
type sortedStrings struct{ a []string }

func (s *sortedStrings) size() int { return len(s.a) }

// lower is the index of the first element at or above k.
func (s *sortedStrings) lower(k string) int { return sort.SearchStrings(s.a, k) }

// upper is the index of the first element above k.
func (s *sortedStrings) upper(k string) int {
	return sort.Search(len(s.a), func(i int) bool { return s.a[i] > k })
}

func (s *sortedStrings) insert(k string) {
	i := s.lower(k)
	if i < len(s.a) && s.a[i] == k {
		return
	}
	s.a = append(s.a, "")
	copy(s.a[i+1:], s.a[i:])
	s.a[i] = k
}

func (s *sortedStrings) remove(k string) {
	i := s.lower(k)
	if i < len(s.a) && s.a[i] == k {
		s.a = append(s.a[:i], s.a[i+1:]...)
	}
}

// sortedNumbers is numbers in ascending order, each once.
type sortedNumbers struct{ a []uint64 }

func (s *sortedNumbers) size() int { return len(s.a) }

// upper is the index of the first element above n.
func (s *sortedNumbers) upper(n uint64) int {
	return sort.Search(len(s.a), func(i int) bool { return s.a[i] > n })
}

func (s *sortedNumbers) insert(n uint64) {
	i := s.upper(n)
	if i > 0 && s.a[i-1] == n {
		return
	}
	s.a = append(s.a, 0)
	copy(s.a[i+1:], s.a[i:])
	s.a[i] = n
}

func (s *sortedNumbers) remove(n uint64) {
	i := s.upper(n) - 1
	if i >= 0 && s.a[i] == n {
		s.a = append(s.a[:i], s.a[i+1:]...)
	}
}

// seqIndex is seqs with a count of tokens at each: a sorted list the
// classes search by binary search.
type seqIndex struct {
	sorted sortedNumbers
	n      map[uint64]int
}

func newSeqIndex() *seqIndex { return &seqIndex{n: map[uint64]int{}} }

func (x *seqIndex) add(seq uint64) {
	x.n[seq]++
	if x.n[seq] == 1 {
		x.sorted.insert(seq)
	}
}

func (x *seqIndex) remove(seq uint64) {
	k := x.n[seq] - 1
	if k > 0 {
		x.n[seq] = k
		return
	}
	delete(x.n, seq)
	x.sorted.remove(seq)
}

func (x *seqIndex) clear() {
	x.n = map[uint64]int{}
	x.sorted.a = nil
}

// sortedIn is the sorted list under k, made when it is new.
func sortedIn(m map[string]*sortedStrings, k string) *sortedStrings {
	s := m[k]
	if s == nil {
		s = &sortedStrings{}
		m[k] = s
	}
	return s
}

// sortedOut takes x from the sorted list under k, and the list when it is
// empty.
func sortedOut(m map[string]*sortedStrings, k, x string) {
	s := m[k]
	if s == nil {
		return
	}
	s.remove(x)
	if s.size() == 0 {
		delete(m, k)
	}
}

// addTo adds v to the set under k, making the set when it is new.
func addTo[V comparable](m map[string]map[V]bool, k string, v V) {
	s := m[k]
	if s == nil {
		s = map[V]bool{}
		m[k] = s
	}
	s[v] = true
}

// removeFrom takes v from the set under k, and the set when it is empty.
func removeFrom[V comparable](m map[string]map[V]bool, k string, v V) {
	s := m[k]
	if s == nil {
		return
	}
	delete(s, v)
	if len(s) == 0 {
		delete(m, k)
	}
}
