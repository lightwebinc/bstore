package publish

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// A limiter in front of a provider's auth layer answers a plain 429 or
// 503 that the SDK reports as a failed authentication; the tap keeps the
// status so the call is answered as busy, with its Retry-After.
func TestPaymentTapKeepsTheStatusOfABusyAnswer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	tap := &paymentTap{base: http.DefaultTransport}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/challenge", nil)
	res, err := tap.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if tap.status != http.StatusTooManyRequests || tap.retryAfter != "3" {
		t.Fatalf("tap kept %d %q", tap.status, tap.retryAfter)
	}
	if !busy(tap.status) || busy(http.StatusUnauthorized) || busy(http.StatusOK) {
		t.Fatal("busy is 429 and 503 only")
	}
}
