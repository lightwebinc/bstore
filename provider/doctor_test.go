package provider

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lightwebinc/bcommon/testchain"
)

// Doctor reads and reports; an endpoint that does not answer is a line,
// not an error.
func TestDoctorReports(t *testing.T) {
	cs := httptest.NewServer(testchain.New(700))
	defer cs.Close()
	data := filepath.Join(t.TempDir(), "p")
	id, err := Init(data)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &Config{Data: data, PublicURL: "https://127.0.0.1:1/store", Node: cs.URL, Settle: "rpc:" + cs.URL + "/rpc", Headers: cs.URL, Hosts: []string{cs.URL}, Backend: "native"}
	var out bytes.Buffer
	if err := Doctor(context.Background(), cfg, "", &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"identity     " + id, "pool         0 output(s)", "chain        asset:" + cs.URL + ": node tip 700",
		"headers      " + cs.URL + ": height 700", "settle       rpc:" + cs.URL + "/rpc", "fee          100/1000 satoshis/bytes, floor 250, static", "host         " + cs.URL + ": answers", "front        https://127.0.0.1:1/store/bstore/v1: does not answer",
		"store        0 object(s)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	if err := Doctor(context.Background(), &Config{Data: filepath.Join(t.TempDir(), "none")}, "", &out); err == nil {
		t.Fatal("a data directory with no wallet was reported")
	}
}
