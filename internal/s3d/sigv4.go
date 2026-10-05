package s3d

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AWS Signature Version 4, gateway mode (spec section 16): each access key
// id maps to one identity the operator configured, and the secret is
// provisioned per access key. Only AWS4-HMAC-SHA256 is accepted, in the
// Authorization header or as a presigned URL's query.

const (
	algorithm     = "AWS4-HMAC-SHA256"
	timeFormat    = "20060102T150405Z"
	dateFormat    = "20060102"
	unsigned      = "UNSIGNED-PAYLOAD"
	streamSigned  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamSignedT = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	streamUnsignT = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	emptySHA256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// maxSkew is how far a signed request's time may be from the
	// gateway's, as S3 has it.
	maxSkew = 15 * time.Minute
	// maxPresign is the longest a presigned URL lives (seven days).
	maxPresign = 7 * 24 * time.Hour
)

// Key is one access key.
type Key struct {
	ID       string
	Secret   string
	Identity string
	// Buckets are the S3 bucket names the key may use; empty is every
	// configured bucket.
	Buckets map[string]bool
}

// Auth is a request whose signature verified.
type Auth struct {
	Key *Key
	// Seed is the request's signature: a streaming body's chunks chain
	// from it, and replay protection is keyed by it.
	Seed    string
	Scope   string
	Time    time.Time
	sigKey  []byte
	Payload string // the x-amz-content-sha256 value
	Presign bool
}

// authError is an S3 error from authentication.
type authError struct {
	code, msg string
	status    int
}

func (e *authError) Error() string { return e.code + ": " + e.msg }

func denied(code, format string, args ...any) *authError {
	status := http.StatusForbidden
	if code == "AuthorizationHeaderMalformed" || code == "AuthorizationQueryParametersError" || code == "InvalidArgument" ||
		code == "MissingSecurityHeader" || code == "InvalidRequest" {
		status = http.StatusBadRequest
	}
	return &authError{code, fmt.Sprintf(format, args...), status}
}

// signer verifies requests against the configured keys.
type signer struct {
	keys   map[string]*Key
	region string
	now    func() time.Time

	mu    sync.Mutex
	cache map[string][]byte // id/date -> signing key
}

func (s *signer) signingKey(k *Key, date string) []byte {
	ck := k.ID + "/" + date
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.cache[ck]; ok {
		return b
	}
	b := hmacSHA256([]byte("AWS4"+k.Secret), date)
	b = hmacSHA256(b, s.region)
	b = hmacSHA256(b, "s3")
	b = hmacSHA256(b, "aws4_request")
	if s.cache == nil || len(s.cache) > 4096 {
		s.cache = map[string][]byte{}
	}
	s.cache[ck] = b
	return b
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// verify authenticates r. A request with neither an Authorization header
// nor a presigned query is refused: there is no anonymous access.
func (s *signer) verify(r *http.Request) (*Auth, error) {
	q := r.URL.Query()
	if h := r.Header.Get("Authorization"); h != "" {
		if q.Get("X-Amz-Algorithm") != "" || q.Get("X-Amz-Signature") != "" {
			return nil, denied("InvalidArgument", "only one auth mechanism allowed")
		}
		return s.verifyHeader(r, h)
	}
	if q.Get("X-Amz-Algorithm") != "" {
		return s.verifyQuery(r, q)
	}
	if q.Get("AWSAccessKeyId") != "" || strings.HasPrefix(r.Header.Get("Authorization"), "AWS ") {
		return nil, denied("AccessDenied", "Signature Version 2 is not supported; sign with AWS4-HMAC-SHA256")
	}
	return nil, denied("AccessDenied", "anonymous access is not supported")
}

func (s *signer) verifyHeader(r *http.Request, h string) (*Auth, error) {
	if strings.HasPrefix(h, "AWS ") {
		return nil, denied("AccessDenied", "Signature Version 2 is not supported; sign with AWS4-HMAC-SHA256")
	}
	if !strings.HasPrefix(h, algorithm+" ") {
		return nil, denied("AuthorizationHeaderMalformed", "the authorization header is not %s", algorithm)
	}
	var cred, signed, sig string
	for _, part := range strings.Split(h[len(algorithm)+1:], ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, denied("AuthorizationHeaderMalformed", "the authorization header is malformed")
		}
		switch k {
		case "Credential":
			cred = v
		case "SignedHeaders":
			signed = v
		case "Signature":
			sig = v
		default:
			return nil, denied("AuthorizationHeaderMalformed", "the authorization header is malformed")
		}
	}
	if cred == "" || signed == "" || sig == "" {
		return nil, denied("AuthorizationHeaderMalformed", "the authorization header is missing a part")
	}
	if r.Header.Get("X-Amz-Security-Token") != "" {
		return nil, denied("InvalidArgument", "session tokens are not supported")
	}
	amz := r.Header.Get("X-Amz-Date")
	if amz == "" {
		return nil, denied("AccessDenied", "x-amz-date is required")
	}
	t, err := time.Parse(timeFormat, amz)
	if err != nil {
		return nil, denied("AccessDenied", "x-amz-date is not %s", timeFormat)
	}
	if d := s.now().Sub(t); d > maxSkew || d < -maxSkew {
		return nil, denied("RequestTimeTooSkewed", "the difference between the request time and the server's time is too large")
	}
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		return nil, denied("InvalidRequest", "missing required header for this request: x-amz-content-sha256")
	}
	if !validPayload(payload) {
		return nil, denied("InvalidArgument", "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, a streaming value or a SHA-256 in hex")
	}
	k, scope, err := s.credential(cred, t)
	if err != nil {
		return nil, err
	}
	names, err := s.signedNames(r, signed, false)
	if err != nil {
		return nil, err
	}
	creq := canonicalRequest(r, names, canonicalQuery(r.URL.RawQuery, ""), payload)
	sk := s.signingKey(k, t.Format(dateFormat))
	want := hex.EncodeToString(hmacSHA256(sk, stringToSign(amz, scope, creq)))
	if subtle.ConstantTimeCompare([]byte(want), []byte(sig)) != 1 {
		return nil, denied("SignatureDoesNotMatch", "the request signature we calculated does not match the signature you provided")
	}
	return &Auth{Key: k, Seed: sig, Scope: scope, Time: t, sigKey: sk, Payload: payload}, nil
}

func (s *signer) verifyQuery(r *http.Request, q url.Values) (*Auth, error) {
	if q.Get("X-Amz-Algorithm") != algorithm {
		return nil, denied("AuthorizationQueryParametersError", "X-Amz-Algorithm must be %s", algorithm)
	}
	cred, signed, sig, amz, exp := q.Get("X-Amz-Credential"), q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Signature"), q.Get("X-Amz-Date"), q.Get("X-Amz-Expires")
	if cred == "" || signed == "" || sig == "" || amz == "" || exp == "" {
		return nil, denied("AuthorizationQueryParametersError", "a presigned URL needs X-Amz-Credential, X-Amz-SignedHeaders, X-Amz-Signature, X-Amz-Date and X-Amz-Expires")
	}
	if q.Get("X-Amz-Security-Token") != "" {
		return nil, denied("InvalidArgument", "session tokens are not supported")
	}
	t, err := time.Parse(timeFormat, amz)
	if err != nil {
		return nil, denied("AuthorizationQueryParametersError", "X-Amz-Date is not %s", timeFormat)
	}
	secs, err := strconv.ParseUint(exp, 10, 32)
	if err != nil || secs < 1 || time.Duration(secs)*time.Second > maxPresign {
		return nil, denied("AuthorizationQueryParametersError", "X-Amz-Expires must be 1 to 604800 seconds")
	}
	now := s.now()
	if now.Before(t.Add(-maxSkew)) {
		return nil, denied("AccessDenied", "request is not valid yet")
	}
	if now.After(t.Add(time.Duration(secs) * time.Second)) {
		return nil, denied("AccessDenied", "request has expired")
	}
	k, scope, err := s.credential(cred, t)
	if err != nil {
		return nil, err
	}
	names, err := s.signedNames(r, signed, true)
	if err != nil {
		return nil, err
	}
	payload := unsigned
	if v := r.Header.Get("X-Amz-Content-Sha256"); v != "" {
		payload = v
	}
	if !validPayload(payload) || strings.HasPrefix(payload, "STREAMING-") {
		return nil, denied("InvalidArgument", "a presigned request's payload is UNSIGNED-PAYLOAD or a SHA-256 in hex")
	}
	creq := canonicalRequest(r, names, canonicalQuery(r.URL.RawQuery, "X-Amz-Signature"), payload)
	sk := s.signingKey(k, t.Format(dateFormat))
	want := hex.EncodeToString(hmacSHA256(sk, stringToSign(amz, scope, creq)))
	if subtle.ConstantTimeCompare([]byte(want), []byte(sig)) != 1 {
		return nil, denied("SignatureDoesNotMatch", "the request signature we calculated does not match the signature you provided")
	}
	return &Auth{Key: k, Seed: sig, Scope: scope, Time: t, sigKey: sk, Payload: payload, Presign: true}, nil
}

func validPayload(p string) bool {
	switch p {
	case unsigned, streamSigned, streamSignedT, streamUnsignT:
		return true
	}
	if len(p) != 64 {
		return false
	}
	_, err := hex.DecodeString(p)
	return err == nil && strings.ToLower(p) == p
}

// credential reads <id>/<date>/<region>/s3/aws4_request and holds it to
// the request's date and the gateway's region.
func (s *signer) credential(cred string, t time.Time) (*Key, string, error) {
	parts := strings.Split(cred, "/")
	if len(parts) != 5 || parts[3] != "s3" || parts[4] != "aws4_request" {
		return nil, "", denied("AuthorizationHeaderMalformed", "the credential is not <key>/<date>/<region>/s3/aws4_request")
	}
	k, ok := s.keys[parts[0]]
	if !ok {
		return nil, "", denied("InvalidAccessKeyId", "the access key id you provided does not exist in our records")
	}
	if parts[1] != t.Format(dateFormat) {
		return nil, "", denied("AuthorizationHeaderMalformed", "the credential's date is not the request's")
	}
	if parts[2] != s.region {
		return nil, "", denied("AuthorizationHeaderMalformed", "the region %q is wrong; expecting %q", parts[2], s.region)
	}
	return k, strings.Join(parts[1:], "/"), nil
}

// signedNames checks the signed header list: lowercase, ascending, no
// repeats, host among them, and every x-amz-* header the request carries
// (and content-md5) signed, so nothing that changes what is written rides
// unsigned. A presigned URL need sign only host.
func (s *signer) signedNames(r *http.Request, list string, presign bool) ([]string, error) {
	names := strings.Split(list, ";")
	seen := map[string]bool{}
	for i, n := range names {
		if n == "" || n != strings.ToLower(n) || seen[n] || (i > 0 && names[i-1] >= n) {
			return nil, denied("AuthorizationHeaderMalformed", "the signed headers are not lowercase, ascending and distinct")
		}
		seen[n] = true
	}
	if !seen["host"] {
		return nil, denied("AuthorizationHeaderMalformed", "the host header must be signed")
	}
	for name := range r.Header {
		l := strings.ToLower(name)
		if (strings.HasPrefix(l, "x-amz-") || l == "content-md5") && !seen[l] {
			if presign && l == "x-amz-content-sha256" {
				continue
			}
			return nil, denied("AccessDenied", "header %s must be signed", l)
		}
	}
	return names, nil
}

// canonicalRequest is SigV4's canonical request for S3: the path
// URI-encoded once per segment from its decoded form, slashes kept.
func canonicalRequest(r *http.Request, names []string, query, payload string) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(encodePath(r.URL.Path))
	b.WriteByte('\n')
	b.WriteString(query)
	b.WriteByte('\n')
	for _, n := range names {
		b.WriteString(n)
		b.WriteByte(':')
		b.WriteString(headerValue(r, n))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	b.WriteString(strings.Join(names, ";"))
	b.WriteByte('\n')
	b.WriteString(payload)
	return b.String()
}

func headerValue(r *http.Request, name string) string {
	var vals []string
	switch name {
	case "host":
		vals = []string{r.Host}
	case "transfer-encoding":
		vals = r.TransferEncoding
	case "content-length":
		if v := r.Header.Values("Content-Length"); len(v) > 0 {
			vals = v
		} else if r.ContentLength >= 0 {
			vals = []string{strconv.FormatInt(r.ContentLength, 10)}
		}
	default:
		vals = r.Header.Values(name)
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(out, ",")
}

func stringToSign(amz, scope, creq string) string {
	return algorithm + "\n" + amz + "\n" + scope + "\n" + sha256Hex([]byte(creq))
}

// canonicalQuery is the query's pairs, each decoded then URI-encoded,
// sorted by name then value, without the pair named skip.
func canonicalQuery(raw, skip string) string {
	if raw == "" {
		return ""
	}
	type kv struct{ k, v string }
	var pairs []kv
	for _, p := range strings.Split(raw, "&") {
		if p == "" {
			continue
		}
		k, v, _ := strings.Cut(p, "=")
		dk, err1 := url.QueryUnescape(k)
		dv, err2 := url.QueryUnescape(v)
		if err1 != nil {
			dk = k
		}
		if err2 != nil {
			dv = v
		}
		if dk == skip {
			continue
		}
		pairs = append(pairs, kv{uriEncode(dk, true), uriEncode(dv, true)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

func encodePath(p string) string {
	if p == "" {
		return "/"
	}
	return uriEncode(p, false)
}

// uriEncode is SigV4's URI encoding: unreserved characters kept, every
// other byte %XX in upper case; a slash is kept unless slash is set.
func uriEncode(s string, slash bool) string {
	const hexd = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !slash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexd[c>>4])
			b.WriteByte(hexd[c&15])
		}
	}
	return b.String()
}

// chunkSignature is one aws-chunked chunk's signature, chained from prev.
func (a *Auth) chunkSignature(prev string, chunkHash []byte) string {
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + a.Time.Format(timeFormat) + "\n" + a.Scope + "\n" + prev + "\n" + emptySHA256 + "\n" + hex.EncodeToString(chunkHash)
	return hex.EncodeToString(hmacSHA256(a.sigKey, sts))
}

// trailerSignature is the signature of a signed body's trailing headers.
func (a *Auth) trailerSignature(prev string, trailer []byte) string {
	sts := "AWS4-HMAC-SHA256-TRAILER\n" + a.Time.Format(timeFormat) + "\n" + a.Scope + "\n" + prev + "\n" + sha256Hex(trailer)
	return hex.EncodeToString(hmacSHA256(a.sigKey, sts))
}
