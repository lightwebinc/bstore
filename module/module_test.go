package module

import (
	"context"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/lightwebinc/bstore/storerec"
)

// The module's configuration and its mount (module.test.ts): every BSTORE_
// variable read exactly or refused, the floors held, prices needing their
// route and payee, OVERLAY_TOPICS and OVERLAY_SYNC_PEERS checked, and the
// factory mounting each bucket and ls_bstore from the environment.

const (
	b1      = "photos_qzvmkwerty"
	b2      = "backups_abcdefghij"
	ownerK  = "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
	anyoneK = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
)

var idK = strings.Repeat("ab", 32)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestParseBuckets(t *testing.T) {
	got, err := ParseBuckets(" " + b1 + " , " + b2)
	if err != nil || !slices.Equal(got, []string{b1, b2}) {
		t.Fatal(got, err)
	}
	for _, bad := range []string{"", "photos", "Photos_qzvmkwerty", b1 + "," + b1, "photos__x_qzvmkwerty", "photos_qzvmkwert"} {
		if _, err := ParseBuckets(bad); err == nil {
			t.Errorf("%q", bad)
		}
	}
}

func TestParseServed(t *testing.T) {
	got, err := ParseServed(b1+":"+ownerK+":"+idK, []string{b1})
	if err != nil || !slices.Equal(got, []string{b1 + "|" + ownerK + "|" + idK}) {
		t.Fatal(got, err)
	}
	if got, err := ParseServed("", []string{b1}); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	for _, bad := range []string{b2 + ":" + ownerK + ":" + idK, b1 + ":" + ownerK, b1 + ":" + strings.ToUpper(ownerK) + ":" + idK, b1 + ":" + anyoneK + ":" + idK,
		b1 + ":" + ownerK + ":" + idK[2:], b1 + ":" + ownerK + ":" + idK + "," + b1 + ":" + ownerK + ":" + idK} {
		if _, err := ParseServed(bad, []string{b1}); err == nil {
			t.Errorf("%q", bad)
		}
	}
}

func TestParseConfig(t *testing.T) {
	base := map[string]string{"BSTORE_BUCKETS": b1, "BSTORE_STATE_DIR": "/var/lib/bstore", "BSTORE_HEADERS_URL": "https://headers.example"}
	c, err := ParseConfig(env(base))
	if err != nil {
		t.Fatal(err)
	}
	if c.KeepDays != 31 || c.PendingDays != 7 || c.OtherHours != 24 || c.MaxBEEF != 262144 || c.MaxSweepInputs != 1024 || c.Sessions.Max != 10000 || c.Sessions.TTL.Seconds() != 600 {
		t.Fatalf("%+v", c)
	}
	if c.Budget.PerSec != 4 || c.Budget.Burst != 8 || c.Budget.PerAddressPerSec != 1 || c.Budget.AddressBurst != 4 || c.Response.PerSec != 5 || c.Response.Burst != 20 {
		t.Fatalf("%+v %+v", c.Budget, c.Response)
	}
	if c.Listen != "" || len(c.Prices) != 0 || c.PayeeKey != nil {
		t.Fatal("a route by default")
	}
	tracker := map[string]string{}
	for k, v := range base {
		tracker[k] = v
	}
	delete(tracker, "BSTORE_HEADERS_URL")
	tracker["OVERLAY_CHAIN_TRACKER_URL"] = "http://127.0.0.1:8090"
	if c, err := ParseConfig(env(tracker)); err != nil || c.HeadersURL != "http://127.0.0.1:8090" {
		t.Fatal(c, err)
	}
	set := func(kv ...string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	if c, err := ParseConfig(env(set("BSTORE_KEEP_DAYS", "60", "BSTORE_PENDING_DAYS", "14", "BSTORE_OTHER_HOURS", "48"))); err != nil || c.KeepDays != 60 || c.PendingDays != 14 || c.OtherHours != 48 {
		t.Fatal(c, err)
	}
	for _, kv := range [][2]string{
		{"BSTORE_STATE_DIR", "relative/dir"}, {"BSTORE_HEADERS_URL", "ftp://x"}, {"BSTORE_KEEP_DAYS", "30"}, {"BSTORE_PENDING_DAYS", "6"},
		{"BSTORE_OTHER_HOURS", "23"}, {"BSTORE_MAX_BEEF", "262143"}, {"BSTORE_MAX_SWEEP_INPUTS", "1023"}, {"BSTORE_LISTEN", "nope"},
		{"BSTORE_PAYEE_KEY", strings.Repeat("AB", 32)}, {"BSTORE_PRICES", "list=5"}, {"BSTORE_PRICES", "history=5"},
		{"BSTORE_HANDSHAKES_PER_SEC", "0"}, {"BSTORE_SESSIONS", "0"}, {"BSTORE_LISTEN", "127.0.0.1:0"}, {"BSTORE_LISTEN", ":8294"},
		{"BSTORE_ACCEPT_THRESHOLD_SATS", "x"}, {"BSTORE_ARCADE_URL", "ftp://arcade"},
	} {
		if _, err := ParseConfig(env(set(kv[0], kv[1]))); err == nil {
			t.Errorf("%s=%s", kv[0], kv[1])
		}
	}
	if _, err := ParseConfig(env(map[string]string{"BSTORE_BUCKETS": b1, "BSTORE_STATE_DIR": "/s", "OVERLAY_CHAIN_TRACKER_URL": "whatsonchain"})); err == nil || !strings.Contains(err.Error(), "BSTORE_HEADERS_URL is required") {
		t.Fatal(err)
	}
	if _, err := ParseConfig(env(set("BSTORE_PRICES", "history=5", "BSTORE_LISTEN", "127.0.0.1:9000"))); err == nil || !strings.Contains(err.Error(), "BSTORE_PAYEE_KEY are required") {
		t.Fatal(err)
	}
	if _, err := ParseConfig(env(set("BSTORE_PRICES", "history=0"))); err == nil || !strings.Contains(err.Error(), "needs BSTORE_LISTEN") {
		t.Fatal(err)
	}
	priced, err := ParseConfig(env(set("BSTORE_PRICES", "history=5,chain=0", "BSTORE_LISTEN", "[::1]:9000", "BSTORE_PAYEE_KEY", strings.Repeat("11", 32))))
	if err != nil || priced.Listen != "[::1]:9000" || priced.Prices["history"] != 5 || priced.PayeeKey == nil {
		t.Fatal(priced, err)
	}
	if c, err := ParseConfig(env(set("BSTORE_LISTEN", "0.0.0.0:8294"))); err != nil || c.Listen != "0.0.0.0:8294" {
		t.Fatal(c, err)
	}
}

func TestOverlayTopics(t *testing.T) {
	c := baseConfig()
	c.Buckets = []string{b1}
	h := headersOf{}
	if _, err := Mount(newFakeHost(), c, []string{"tm_bstore_" + b1, "tm_bstore_" + b2}, &MemoryJournal{}, h, nil); err == nil || !strings.Contains(err.Error(), "tm_bstore_"+b2) {
		t.Fatal(err)
	}
	if _, err := Mount(newFakeHost(), c, []string{"tm_bstore_" + b1, "tm_bstoreblob_" + b2}, &MemoryJournal{}, h, nil); err == nil || !strings.Contains(err.Error(), "tm_bstoreblob_"+b2) {
		t.Fatal(err)
	}
	m, err := Mount(newFakeHost(), c, []string{"tm_bstore_" + b1}, &MemoryJournal{}, h, nil)
	if err != nil || len(m.Module.Topics) != 1 || m.Module.Topics["tm_bstore_"+b1] == nil {
		t.Fatal(err)
	}
	host := newFakeHost()
	m, err = Mount(host, c, []string{"tm_anytx", "tm_bstore_" + b1, "tm_bstoreblob_" + b1}, &MemoryJournal{}, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for n := range m.Module.Topics {
		names = append(names, n)
	}
	sort.Strings(names)
	if !slices.Equal(names, []string{"tm_bstore_" + b1, "tm_bstoreblob_" + b1}) {
		t.Fatal(names)
	}
	if !host.logged("bstore module mounted", func(x map[string]any) bool { return strings.Contains(x["blobs"].(string), "refuses everything") }) {
		t.Fatal("the blob topic is not logged")
	}
	if err := CheckNoSync("tm_anytx=https://peer.example;tm_bstore_" + b1 + "=https://peer.example"); err == nil || !strings.Contains(err.Error(), "never synchronised") {
		t.Fatal(err)
	}
	if err := CheckNoSync("tm_bstoreblob_" + b1 + "=https://peer.example"); err == nil {
		t.Fatal("blob topic synchronised")
	}
	if CheckNoSync("tm_anytx=https://peer.example") != nil || CheckNoSync("") != nil {
		t.Fatal("refused")
	}
}

func TestFactory(t *testing.T) {
	dir := t.TempDir()
	e := map[string]string{"BSTORE_BUCKETS": b1 + "," + b2, "BSTORE_STATE_DIR": dir, "BSTORE_HEADERS_URL": "http://127.0.0.1:1",
		"OVERLAY_TOPICS": "tm_anytx,tm_bstore_" + b1 + ",tm_bstore_" + b2}
	host := newFakeHost()
	m, err := New(host, env(e))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for n := range m.Module.Topics {
		names = append(names, n)
	}
	sort.Strings(names)
	if !slices.Equal(names, []string{"tm_bstore_" + b2, "tm_bstore_" + b1}) || len(m.Module.Lookups) != 1 || m.Module.Lookups[storerec.LookupService] == nil {
		t.Fatal(names, m.Module.Lookups)
	}
	for _, reason := range Reasons {
		if _, ok := host.counters[mkey("bstore_refused_total", map[string]string{"reason": reason})]; !ok {
			t.Errorf("%s is not preset", reason)
		}
	}
	for _, status := range TokenStatuses {
		if _, ok := host.counters[mkey("bstore_token_status_total", map[string]string{"status": status})]; !ok {
			t.Errorf("%s is not preset", status)
		}
	}
	for _, g := range []string{"bstore_buckets", "bstore_tokens", "bstore_entries", "bstore_outcomes", "bstore_fronts", "bstore_advertisements", "bstore_hold_outputs",
		"bstore_sweeps", "bstore_sweep_outpoints", "bstore_retraction_marks", "bstore_pending_drops", "bstore_maps_incomplete"} {
		if host.gauges[g] == nil {
			t.Errorf("no gauge %s", g)
		}
	}
	if host.gauges["bstore_buckets"]() != 2 {
		t.Fatal("buckets")
	}
	if !host.logged("bstore module mounted", func(x map[string]any) bool {
		return x["lookup"] == "ls_bstore" && x["build"] == Build && x["terms"] == "none"
	}) {
		t.Fatal("mount not logged")
	}
	// The module restores and answers: the index starts empty.
	if _, err := m.LS.Restore(context.Background(), nil, newMemStorage()); err != nil || !m.LS.Restored() {
		t.Fatal(err)
	}
	e["OVERLAY_SYNC_PEERS"] = "tm_bstore_" + b1 + "=https://peer.example"
	if _, err := New(newFakeHost(), env(e)); err == nil || !strings.Contains(err.Error(), "OVERLAY_SYNC_PEERS names tm_bstore_"+b1) {
		t.Fatal(err)
	}
	delete(e, "OVERLAY_SYNC_PEERS")
	delete(e, "BSTORE_BUCKETS")
	if _, err := New(newFakeHost(), env(e)); err == nil || !strings.Contains(err.Error(), "BSTORE_BUCKETS") {
		t.Fatal(err)
	}
}

// TestLoneSurrogate: a question's values reach the codec as UTF-8 holds
// them; a lone surrogate in a key reads as U+FFFD, as the TypeScript
// module reads it.
func TestLoneSurrogate(t *testing.T) {
	raw := `{"bucket":"photos_qzvmkwerty","owner":"` + ownerK + `","id":"` + strings.Repeat("0", 64) + `","key":"a\ud800b"}`
	q, err := storerec.ParseQuery([]byte(raw), nil, nil)
	if err != nil || q.Class != "object" || q.Values["key"] != "a�b" {
		t.Fatal(q, err)
	}
}
