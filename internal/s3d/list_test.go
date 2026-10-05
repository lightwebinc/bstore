package s3d

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/internal/storerec"
)

// pages serves a sorted map as proven pages of three.
func pages(keys map[string]uint64, prefix string) func(after string) (*reader.Page, error) {
	var all []string
	for k := range keys {
		if strings.HasPrefix(k, prefix) {
			all = append(all, k)
		}
	}
	sort.Strings(all)
	return func(after string) (*reader.Page, error) {
		p := &reader.Page{}
		for _, k := range all {
			if k <= after {
				continue
			}
			c := [32]byte{byte(len(k)), k[len(k)-1]}
			p.Entries = append(p.Entries, &reader.Entry{Entry: &storerec.Entry{Key: k, Created: keys[k], Kind: storerec.KindPut, Size: 1}, C: c})
			if len(p.Entries) == 3 {
				p.Next = k
				return p, nil
			}
		}
		p.Done = true
		return p, nil
	}
}

func pend(key string, created uint64, kind uint64) *pendingEntry {
	e := &storerec.Entry{Key: key, Created: created, Kind: kind, Size: 2}
	return &pendingEntry{Key: key, Created: created, Kind: kind, C: hex.EncodeToString([]byte{0xff, byte(created)}), entry: e}
}

func keysOf(l *listing) []string {
	var out []string
	for _, it := range l.items {
		out = append(out, it.key)
	}
	return out
}

func TestListMergesPendingEntries(t *testing.T) {
	anchored := map[string]uint64{"a": 10, "b": 10, "d": 30, "e": 10, "f": 10, "g": 10, "h": 10}
	l := &lister{list: pages(anchored, ""), pend: []*pendingEntry{
		pend("b", 20, storerec.KindDelete), // newer delete hides b
		pend("c", 20, storerec.KindPut),    // a key only pending
		pend("d", 20, storerec.KindPut),    // older than the anchored d: the anchored one stands
		pend("e", 20, storerec.KindPut),    // newer put replaces e
		pend("z", 20, storerec.KindDelete), // a delete of a key nobody holds
	}}
	got, e := l.walk("", 1000)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Join(keysOf(got), ",") != "a,c,d,e,f,g,h" || got.truncated {
		t.Fatalf("%v %v", keysOf(got), got.truncated)
	}
	for _, it := range got.items {
		switch it.key {
		case "d":
			if it.entry.Size != 1 {
				t.Fatal("the older pending put of d won")
			}
		case "c", "e":
			if it.entry.Size != 2 {
				t.Fatalf("the pending put of %s lost", it.key)
			}
		}
	}
}

func TestListPaginatesAndFoldsPrefixes(t *testing.T) {
	keys := map[string]uint64{"p/x/1": 1, "p/x/2": 1, "p/x/3": 1, "p/x/4": 1, "p/y": 1, "p/z/1": 1, "p/z/2": 1, "p/zz": 1, "q": 1}
	walk := func(cursor, delim string, max int) *listing {
		l := &lister{list: pages(keys, "p/"), prefix: "p/", cursor: cursor, next: cursor}
		got, e := l.walk(delim, max)
		if e != nil {
			t.Fatal(e)
		}
		return got
	}
	got := walk("", "/", 1000)
	if strings.Join(keysOf(got), ",") != "p/x/,p/y,p/z/,p/zz" {
		t.Fatal(keysOf(got))
	}
	// Two at a time, resuming from each cursor.
	var all []string
	cursor := ""
	for i := 0; i < 10; i++ {
		got = walk(cursor, "/", 2)
		all = append(all, keysOf(got)...)
		if !got.truncated {
			break
		}
		cursor = got.last
	}
	if strings.Join(all, ",") != "p/x/,p/y,p/z/,p/zz" {
		t.Fatal(all)
	}
	// Without a delimiter, three pages of three over eight keys.
	all, cursor = nil, ""
	for i := 0; i < 10; i++ {
		got = walk(cursor, "", 3)
		all = append(all, keysOf(got)...)
		if !got.truncated {
			break
		}
		cursor = got.last
	}
	if len(all) != 8 || all[0] != "p/x/1" || all[7] != "p/zz" {
		t.Fatal(all)
	}
}

func TestReplayedWriteAnsweredOnce(t *testing.T) {
	now := time.Now()
	s := &Server{replay: newReplayCache(func() time.Time { return now }, 10)}
	a := &Auth{Seed: "sig", Time: now, Key: &Key{ID: "k"}}
	calls := 0
	do := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("PUT", "/b/k", nil)
		if e := s.replayed(w, r, a, func(w http.ResponseWriter) *s3Error {
			calls++
			w.Header().Set("ETag", `"x"`)
			w.WriteHeader(200)
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		return w
	}
	first, second := do(), do()
	if calls != 1 || second.Code != 200 || second.Header().Get("ETag") != first.Header().Get("ETag") {
		t.Fatalf("a replay ran the write again: %d calls", calls)
	}
	// Past the skew window the signature is not remembered, and the
	// request itself is refused as too old by the signer.
	now = now.Add(maxSkew + time.Second)
	do()
	if calls != 2 {
		t.Fatal("the cache outlived its window")
	}
	// A failed write is not remembered: the client's retry runs.
	s.replay = newReplayCache(func() time.Time { return now }, 10)
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		_ = s.replayed(w, httptest.NewRequest("PUT", "/b/k", nil), a, func(http.ResponseWriter) *s3Error { calls++; return errSlowDown })
	}
	if calls != 4 {
		t.Fatalf("a failed write was cached: %d", calls)
	}
}
