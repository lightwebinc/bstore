package s3d

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/lightwebinc/bstore/internal/reader"
)

// A read stopped by the gateway's header source says so, as a 503 the
// reader retries, never as a proof the hosts failed.
func TestAHeaderOutageReadsAsTheHeaders(t *testing.T) {
	err := fmt.Errorf("%w: %w", reader.ErrUnavailable, fmt.Errorf("entry: funding tree: %w: dial", reader.ErrHeaders))
	e := readError(err)
	if e.Status != http.StatusServiceUnavailable || e.Code != "SlowDown" || !strings.Contains(e.Message, "header source") {
		t.Fatalf("got %+v, want a 503 SlowDown naming the header source", e)
	}
	if e := readError(reader.ErrUnavailable); strings.Contains(e.Message, "header source") {
		t.Fatalf("a plain unavailable read names the header source: %+v", e)
	}
}
