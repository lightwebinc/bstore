package s3d

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The examples of AWS's Signature Version 4 documentation for S3, which
// every SDK reproduces.
const (
	exampleKey    = "AKIAIOSFODNN7EXAMPLE"
	exampleSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

func exampleSigner() *signer {
	k := &Key{ID: exampleKey, Secret: exampleSecret, Identity: "main"}
	return &signer{keys: map[string]*Key{exampleKey: k}, region: "us-east-1",
		now: func() time.Time { return time.Date(2013, 5, 24, 0, 5, 0, 0, time.UTC) }}
}

func auth(signed, sig string) string {
	return "AWS4-HMAC-SHA256 Credential=" + exampleKey + "/20130524/us-east-1/s3/aws4_request,SignedHeaders=" + signed + ",Signature=" + sig
}

func TestSigV4HeaderExamples(t *testing.T) {
	s := exampleSigner()
	cases := []struct {
		name, method, target, signed, sig, payload string
		hdr                                        map[string]string
	}{
		{"get object", "GET", "/test.txt", "host;range;x-amz-content-sha256;x-amz-date",
			"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", emptySHA256, map[string]string{"Range": "bytes=0-9"}},
		{"put object", "PUT", "/test$file.text", "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
			"98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
			"44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
			map[string]string{"Date": "Fri, 24 May 2013 00:00:00 GMT", "X-Amz-Storage-Class": "REDUCED_REDUNDANCY"}},
		{"get lifecycle", "GET", "/?lifecycle", "host;x-amz-content-sha256;x-amz-date",
			"fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543", emptySHA256, nil},
		{"list objects", "GET", "/?max-keys=2&prefix=J", "host;x-amz-content-sha256;x-amz-date",
			"34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7", emptySHA256, nil},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, "http://examplebucket.s3.amazonaws.com"+c.target, nil)
		r.Host = "examplebucket.s3.amazonaws.com"
		r.Header.Set("X-Amz-Date", "20130524T000000Z")
		r.Header.Set("X-Amz-Content-Sha256", c.payload)
		for k, v := range c.hdr {
			r.Header.Set(k, v)
		}
		r.Header.Set("Authorization", auth(c.signed, c.sig))
		a, err := s.verify(r)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if a.Key.ID != exampleKey || a.Seed != c.sig {
			t.Fatalf("%s: %+v", c.name, a)
		}
		// One flipped digit is refused.
		bad := []byte(c.sig)
		bad[0] ^= 1
		r.Header.Set("Authorization", auth(c.signed, string(bad)))
		if _, err := s.verify(r); !isCode(err, "SignatureDoesNotMatch") {
			t.Fatalf("%s: a wrong signature: %v", c.name, err)
		}
	}
}

func isCode(err error, code string) bool {
	var ae *authError
	return errors.As(err, &ae) && ae.code == code
}

func TestSigV4PresignedExample(t *testing.T) {
	s := exampleSigner()
	target := "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	r := httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com"+target, nil)
	r.Host = "examplebucket.s3.amazonaws.com"
	a, err := s.verify(r)
	if err != nil || !a.Presign {
		t.Fatalf("presigned: %v", err)
	}
	// Expired a day and a second later.
	s.now = func() time.Time { return time.Date(2013, 5, 25, 0, 0, 1, 0, time.UTC) }
	if _, err := s.verify(r); !isCode(err, "AccessDenied") {
		t.Fatalf("expired: %v", err)
	}
}

func TestSigV4Refusals(t *testing.T) {
	s := exampleSigner()
	base := func() *http.Request {
		r := httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/test.txt", nil)
		r.Host = "examplebucket.s3.amazonaws.com"
		r.Header.Set("X-Amz-Date", "20130524T000000Z")
		r.Header.Set("X-Amz-Content-Sha256", emptySHA256)
		r.Header.Set("Range", "bytes=0-9")
		r.Header.Set("Authorization", auth("host;range;x-amz-content-sha256;x-amz-date", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
		return r
	}
	if _, err := s.verify(base()); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		mut  func(r *http.Request)
		code string
	}{
		{"anonymous", func(r *http.Request) { r.Header.Del("Authorization") }, "AccessDenied"},
		{"signature version 2", func(r *http.Request) { r.Header.Set("Authorization", "AWS AKIA:abc") }, "AccessDenied"},
		{"unknown key", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), exampleKey, "AKIAOTHER", 1))
		}, "InvalidAccessKeyId"},
		{"skewed", func(r *http.Request) { r.Header.Set("X-Amz-Date", "20130523T000000Z") }, "RequestTimeTooSkewed"},
		{"other region", func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "us-east-1", "eu-west-1", 1))
		}, "AuthorizationHeaderMalformed"},
		{"unsigned amz header", func(r *http.Request) { r.Header.Set("X-Amz-Meta-Evil", "1") }, "AccessDenied"},
		{"unsigned content-md5", func(r *http.Request) { r.Header.Set("Content-MD5", "AAAA") }, "AccessDenied"},
		{"host not signed", func(r *http.Request) {
			r.Header.Set("Authorization", auth("range;x-amz-content-sha256;x-amz-date", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
		}, "AuthorizationHeaderMalformed"},
		{"signed headers out of order", func(r *http.Request) {
			r.Header.Set("Authorization", auth("range;host;x-amz-content-sha256;x-amz-date", "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
		}, "AuthorizationHeaderMalformed"},
		{"changed range", func(r *http.Request) { r.Header.Set("Range", "bytes=0-10") }, "SignatureDoesNotMatch"},
		{"changed path", func(r *http.Request) { r.URL.Path = "/test.txt2" }, "SignatureDoesNotMatch"},
		{"bad payload hash value", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", "nope") }, "InvalidArgument"},
		{"no payload hash", func(r *http.Request) { r.Header.Del("X-Amz-Content-Sha256") }, "InvalidRequest"},
	}
	for _, c := range cases {
		r := base()
		c.mut(r)
		if _, err := s.verify(r); !isCode(err, c.code) {
			t.Errorf("%s: want %s, got %v", c.name, c.code, err)
		}
	}
}

func TestCanonicalPathAndQuery(t *testing.T) {
	// A key's bytes are encoded once from the decoded path; the slash is
	// kept, a space is %20, unicode is its UTF-8 bytes.
	if got := encodePath("/b/a b/ü/%2F"); got != "/b/a%20b/%C3%BC/%252F" {
		t.Fatal(got)
	}
	if got := canonicalQuery("prefix=a%20b&max-keys=2&list-type=2&x=", ""); got != "list-type=2&max-keys=2&prefix=a%20b&x=" {
		t.Fatal(got)
	}
	if got := canonicalQuery("a=1&X-Amz-Signature=zz", "X-Amz-Signature"); got != "a=1" {
		t.Fatal(got)
	}
}

// The chunked upload example of AWS's documentation: 66,560 bytes of 'a'
// in a chunk of 65,536 and one of 1,024, then the final chunk.
func TestChunkedExample(t *testing.T) {
	s := exampleSigner()
	data := bytes.Repeat([]byte{'a'}, 66560)
	sigs := []string{"ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
		"0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
		"b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"}
	var b bytes.Buffer
	fmt.Fprintf(&b, "10000;chunk-signature=%s\r\n", sigs[0])
	b.Write(data[:65536])
	fmt.Fprintf(&b, "\r\n400;chunk-signature=%s\r\n", sigs[1])
	b.Write(data[65536:])
	fmt.Fprintf(&b, "\r\n0;chunk-signature=%s\r\n\r\n", sigs[2])
	if b.Len() != 66824 {
		t.Fatalf("body %d bytes", b.Len())
	}
	r := httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/chunkObject.txt", bytes.NewReader(b.Bytes()))
	r.Host = "s3.amazonaws.com"
	r.Header.Set("X-Amz-Date", "20130524T000000Z")
	r.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	r.Header.Set("X-Amz-Content-Sha256", streamSigned)
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	r.Header.Set("Content-Length", "66824")
	r.Header.Set("Authorization", auth("content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class",
		"4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"))
	a, err := s.verify(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(newChunkReader(bytes.NewReader(b.Bytes()), a, true, false))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("decode: %v, %d bytes", err, len(got))
	}
	// A byte changed in the second chunk fails that chunk's signature.
	bad := bytes.Clone(b.Bytes())
	bad[65536+100+90] ^= 1
	if _, err := io.ReadAll(newChunkReader(bytes.NewReader(bad), a, true, false)); !errors.Is(err, errChunkSig) {
		t.Fatalf("tampered: %v", err)
	}
	// Chunks reordered or cut short are refused.
	if _, err := io.ReadAll(newChunkReader(bytes.NewReader(b.Bytes()[:b.Len()-10]), a, true, false)); err == nil {
		t.Fatal("a truncated body passed")
	}
}

func TestChunkedTrailers(t *testing.T) {
	a := &Auth{Seed: strings.Repeat("0", 64), Scope: "20130524/us-east-1/s3/aws4_request", Time: time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC),
		sigKey: exampleSigner().signingKey(&Key{ID: exampleKey, Secret: exampleSecret}, "20130524")}
	data := []byte("hello, bstore")
	// Unsigned chunks with a trailing checksum.
	sum := sha256.Sum256(data)
	_ = hex.EncodeToString(sum[:])
	body := fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(data), data, "AAAAAA==")
	c := newChunkReader(strings.NewReader(body), a, false, true)
	got, err := io.ReadAll(c)
	if err != nil || string(got) != string(data) || c.Trailers["x-amz-checksum-crc32"] != "AAAAAA==" {
		t.Fatalf("unsigned trailer: %v %q %v", err, got, c.Trailers)
	}
	// Signed chunks with a signed trailer, built here as a client would.
	prev := a.Seed
	h := sha256.Sum256(data)
	s1 := a.chunkSignature(prev, h[:])
	e := sha256.Sum256(nil)
	s2 := a.chunkSignature(s1, e[:])
	trailer := "x-amz-checksum-crc32:AAAAAA==\n"
	ts := a.trailerSignature(s2, []byte(trailer))
	body = fmt.Sprintf("%x;chunk-signature=%s\r\n%s\r\n0;chunk-signature=%s\r\nx-amz-checksum-crc32:AAAAAA==\r\nx-amz-trailer-signature:%s\r\n\r\n", len(data), s1, data, s2, ts)
	c = newChunkReader(strings.NewReader(body), a, true, true)
	if got, err := io.ReadAll(c); err != nil || string(got) != string(data) {
		t.Fatalf("signed trailer: %v", err)
	}
	// The trailer changed after signing.
	c = newChunkReader(strings.NewReader(strings.Replace(body, "AAAAAA==\r\nx-amz-trailer", "AAAAAB==\r\nx-amz-trailer", 1)), a, true, true)
	if _, err := io.ReadAll(c); !errors.Is(err, errTrailerSg) {
		t.Fatalf("tampered trailer: %v", err)
	}
	// An oversized line is refused, not buffered.
	c = newChunkReader(strings.NewReader(strings.Repeat("1", 10000)+"\r\n"), a, false, true)
	if _, err := io.ReadAll(c); err == nil {
		t.Fatal("a long chunk line passed")
	}
}

func TestChecksums(t *testing.T) {
	// The CRC-64/NVME check value.
	h := checksumAlgos["crc64nvme"]()
	h.Write([]byte("123456789"))
	if got := hex.EncodeToString(h.Sum(nil)); got != "ae8b14860a799888" {
		t.Fatalf("crc64nvme check value %s", got)
	}
	h = checksumAlgos["crc32c"]()
	h.Write([]byte("123456789"))
	if got := hex.EncodeToString(h.Sum(nil)); got != "e3069283" {
		t.Fatalf("crc32c check value %s", got)
	}
}
