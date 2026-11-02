package s3d

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHistogramRendersCumulativeBuckets(t *testing.T) {
	m := newMetrics()
	m.observe(`x_seconds{stage="send"}`, 0.003) // in the 0.005 bucket
	m.observe(`x_seconds{stage="send"}`, 0.2)   // in the 0.25 bucket
	m.observe(`x_seconds{stage="send"}`, 60)    // past every bound
	m.observe(`y_seconds`, 0.001)
	rec := httptest.NewRecorder()
	m.serve(rec, nil)
	out := rec.Body.String()
	for _, want := range []string{
		`x_seconds_bucket{stage="send",le="0.0025"} 0`,
		`x_seconds_bucket{stage="send",le="0.005"} 1`,
		`x_seconds_bucket{stage="send",le="0.1"} 1`,
		`x_seconds_bucket{stage="send",le="0.25"} 2`,
		`x_seconds_bucket{stage="send",le="30"} 2`,
		`x_seconds_bucket{stage="send",le="+Inf"} 3`,
		`x_seconds_sum{stage="send"} 60.203`,
		`x_seconds_count{stage="send"} 3`,
		`y_seconds_bucket{le="0.001"} 1`,
		`y_seconds_sum{} 0.001`,
		`y_seconds_count{} 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
