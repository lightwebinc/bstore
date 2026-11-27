package s3d

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A gateway with nowhere to deliver serves reads only: every write is
// AccessDenied before anything is read, staged or signed.
func TestReadOnlyRefusesWrites(t *testing.T) {
	c := minimalConfig()
	c.Providers, c.ReadOnlyGateway = nil, true
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
	s := &Server{cfg: c}
	b := &bucket{name: "photos-qzvmkwerty"}
	for _, tc := range []struct{ method, target string }{
		{http.MethodPut, "/photos-qzvmkwerty/k"},
		{http.MethodPut, "/photos-qzvmkwerty/k?partNumber=1&uploadId=u"},
		{http.MethodPost, "/photos-qzvmkwerty/k?uploads"},
		{http.MethodPost, "/photos-qzvmkwerty/k?uploadId=u"},
		{http.MethodDelete, "/photos-qzvmkwerty/k"},
	} {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		_, e := s.objectOp(context.Background(), httptest.NewRecorder(), r, &Auth{}, b, "k", "req")
		if e != errReadOnly {
			t.Fatalf("%s %s: %v", tc.method, tc.target, e)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/photos-qzvmkwerty?delete", nil)
	if _, e := s.bucketOp(context.Background(), httptest.NewRecorder(), r, &Auth{}, b, "req"); e != errReadOnly {
		t.Fatalf("DeleteObjects: %v", e)
	}
	if errReadOnly.Status != http.StatusForbidden || errReadOnly.Code != "AccessDenied" {
		t.Fatal("not AccessDenied")
	}
}
