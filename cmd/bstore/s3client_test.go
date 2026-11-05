package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeS3 is enough of S3 for the client: objects and multipart uploads in
// memory, over TLS so the SDK sends its checksums as trailers, every body
// decoded from aws-chunked and its CRC32C (or CRC-64/NVME) checked as the
// gateway checks it. It records what it saw of each request's signing.
type fakeS3 struct {
	t  *testing.T
	mu sync.Mutex
	// objects by "bucket/key"; parts by "uploadId/n".
	objects map[string][]byte
	parts   map[string][]byte
	uploads int
	// what the client sent
	payloadHashes []string
	trailers      int
	partPuts      int
	algorithms    map[string]int // bodies checked, by checksum algorithm
}

var crc64NVME = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func newFakeS3(t *testing.T) (*fakeS3, *httptest.Server, string) {
	f := &fakeS3{t: t, objects: map[string][]byte{}, parts: map[string][]byte{}, algorithms: map[string]int{}}
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return f, srv, ca
}

// body reads the request body, aws-chunked decoded when so encoded, and
// checks a CRC-64/NVME in the header or the trailer.
func (f *fakeS3) body(r *http.Request) ([]byte, error) {
	f.mu.Lock()
	f.payloadHashes = append(f.payloadHashes, r.Header.Get("x-amz-content-sha256"))
	f.mu.Unlock()
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		return nil, fmt.Errorf("unsigned request")
	}
	// The checksum: "crc32c" or "crc64nvme", from the header or the trailer.
	alg, want := "", ""
	for _, a := range []string{"crc32c", "crc64nvme"} {
		if v := r.Header.Get("x-amz-checksum-" + a); v != "" {
			alg, want = a, v
		}
	}
	var data []byte
	if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		br := bufio.NewReader(r.Body)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return nil, err
			}
			size, _, _ := strings.Cut(strings.TrimSpace(line), ";")
			n, err := strconv.ParseInt(size, 16, 64)
			if err != nil {
				return nil, fmt.Errorf("chunk size %q", line)
			}
			if n == 0 {
				break
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(br, buf); err != nil {
				return nil, err
			}
			data = append(data, buf...)
			if crlf, err := br.ReadString('\n'); err != nil || crlf != "\r\n" {
				return nil, fmt.Errorf("chunk end %q", crlf)
			}
		}
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return nil, err
			}
			line = strings.TrimSpace(line)
			if line == "" {
				break
			}
			k, v, _ := strings.Cut(line, ":")
			if a, ok := strings.CutPrefix(strings.ToLower(k), "x-amz-checksum-"); ok && (a == "crc32c" || a == "crc64nvme") {
				alg, want = a, strings.TrimSpace(v)
				f.mu.Lock()
				f.trailers++
				f.mu.Unlock()
			}
		}
	} else {
		var err error
		if data, err = io.ReadAll(r.Body); err != nil {
			return nil, err
		}
	}
	if want == "" {
		return nil, fmt.Errorf("no crc32c or crc64nvme checksum on %s %s", r.Method, r.URL)
	}
	got := crc32cb64(data)
	if alg == "crc64nvme" {
		got = crc64b64(data)
	}
	if got != want {
		return nil, fmt.Errorf("BadDigest: %s %s, want %s", alg, got, want)
	}
	f.mu.Lock()
	f.algorithms[alg]++
	f.mu.Unlock()
	return data, nil
}

func crc32cb64(b []byte) string {
	sum := crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))
	return base64.StdEncoding.EncodeToString([]byte{byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum)})
}

func crc64b64(b []byte) string {
	sum := crc64.Checksum(b, crc64NVME)
	var be [8]byte
	for i := range be {
		be[i] = byte(sum >> (56 - 8*i))
	}
	return base64.StdEncoding.EncodeToString(be[:])
}

func etag(b []byte) string {
	s := md5.Sum(b)
	return `"` + hex.EncodeToString(s[:]) + `"`
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	name := bucket + "/" + key
	fail := func(code int, s3code string) {
		w.WriteHeader(code)
		fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message></Error>`, s3code, s3code)
	}
	switch {
	case r.Method == "PUT" && q.Has("partNumber"):
		data, err := f.body(r)
		if err != nil {
			f.t.Error(err)
			fail(400, "BadDigest")
			return
		}
		f.mu.Lock()
		f.parts[q.Get("uploadId")+"/"+q.Get("partNumber")] = data
		f.partPuts++
		f.mu.Unlock()
		w.Header().Set("ETag", etag(data))
		w.Header().Set("x-amz-checksum-crc32c", crc32cb64(data))
		w.WriteHeader(200)
	case r.Method == "PUT" && key != "":
		data, err := f.body(r)
		if err != nil {
			f.t.Error(err)
			fail(400, "BadDigest")
			return
		}
		f.mu.Lock()
		f.objects[name] = data
		f.mu.Unlock()
		w.Header().Set("ETag", etag(data))
		w.Header().Set("x-amz-checksum-crc32c", crc32cb64(data))
		w.Header().Set("x-amz-checksum-type", "FULL_OBJECT")
		w.WriteHeader(200)
	case r.Method == "POST" && q.Has("uploads"):
		f.mu.Lock()
		f.uploads++
		id := fmt.Sprintf("up-%d", f.uploads)
		f.mu.Unlock()
		fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, bucket, key, id)
	case r.Method == "POST" && q.Has("uploadId"):
		var req struct {
			Parts []struct {
				PartNumber int    `xml:"PartNumber"`
				ETag       string `xml:"ETag"`
			} `xml:"Part"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			fail(400, "MalformedXML")
			return
		}
		sort.Slice(req.Parts, func(i, j int) bool { return req.Parts[i].PartNumber < req.Parts[j].PartNumber })
		var whole []byte
		var md5s []byte
		f.mu.Lock()
		for _, p := range req.Parts {
			data, ok := f.parts[q.Get("uploadId")+"/"+strconv.Itoa(p.PartNumber)]
			if !ok || etag(data) != p.ETag {
				f.mu.Unlock()
				fail(400, "InvalidPart")
				return
			}
			whole = append(whole, data...)
			s := md5.Sum(data)
			md5s = append(md5s, s[:]...)
		}
		f.objects[name] = whole
		f.mu.Unlock()
		s := md5.Sum(md5s)
		tag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(s[:]), len(req.Parts))
		w.Header().Set("x-amz-checksum-crc32c", crc32cb64(whole))
		w.Header().Set("x-amz-checksum-type", "FULL_OBJECT")
		fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><ETag>%s</ETag><ChecksumCRC32C>%s</ChecksumCRC32C><ChecksumType>FULL_OBJECT</ChecksumType></CompleteMultipartUploadResult>`,
			bucket, key, html(tag), crc32cb64(whole))
	case (r.Method == "GET" || r.Method == "HEAD") && key != "":
		f.mu.Lock()
		data, ok := f.objects[name]
		f.mu.Unlock()
		if !ok {
			fail(404, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", etag(data))
		w.Header().Set("Accept-Ranges", "bytes")
		if rg := r.Header.Get("Range"); rg != "" {
			var a, b int64
			if _, err := fmt.Sscanf(rg, "bytes=%d-%d", &a, &b); err != nil {
				fail(416, "InvalidRange")
				return
			}
			if b >= int64(len(data)) {
				b = int64(len(data)) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(data)))
			w.Header().Set("Content-Length", strconv.FormatInt(b-a+1, 10))
			w.WriteHeader(206)
			if r.Method == "GET" {
				_, _ = w.Write(data[a : b+1])
			}
			return
		}
		w.Header().Set("x-amz-checksum-crc32c", crc32cb64(data))
		w.Header().Set("x-amz-checksum-type", "FULL_OBJECT")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(200)
		if r.Method == "GET" {
			_, _ = w.Write(data)
		}
	case r.Method == "GET" && q.Get("list-type") == "2":
		prefix := q.Get("prefix")
		var keys []string
		f.mu.Lock()
		for k := range f.objects {
			if b, key, _ := strings.Cut(k, "/"); b == bucket && strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		var sb strings.Builder
		sb.WriteString(`<ListBucketResult><IsTruncated>false</IsTruncated>`)
		for _, k := range keys {
			d := f.objects[bucket+"/"+k]
			fmt.Fprintf(&sb, `<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-11-04T00:00:00.000Z</LastModified><ETag>%s</ETag></Contents>`, html(k), len(d), html(etag(d)))
		}
		f.mu.Unlock()
		sb.WriteString(`</ListBucketResult>`)
		_, _ = io.WriteString(w, sb.String())
	case r.Method == "DELETE" && key != "":
		f.mu.Lock()
		delete(f.objects, name)
		f.mu.Unlock()
		w.WriteHeader(204)
	default:
		fail(501, "NotImplemented")
	}
}

func html(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func TestS3Client(t *testing.T) {
	f, srv, ca := newFakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	dir := t.TempDir()
	content := make([]byte, 12<<20+123)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "src.bin")
	if err := os.WriteFile(src, content, 0o600); err != nil {
		t.Fatal(err)
	}
	s3 := []string{"-endpoint", srv.URL, "-ca", ca, "-part", "5MiB", "-conc", "3"}
	cli := func(args ...string) (string, string, int) {
		var out, errb bytes.Buffer
		full := append(append([]string{args[0]}, s3...), args[1:]...) // a case's own flag comes last and wins
		code := run(context.Background(), full, strings.NewReader(""), &out, &errb, os.Getenv)
		return out.String(), errb.String(), code
	}

	// A multipart upload: 3 parts, each a trailer-checksummed, unsigned body.
	out, errs, code := cli("cp", src, "s3://photos/albums/big.bin")
	if code != 0 {
		t.Fatalf("cp up: exit %d: %s%s", code, out, errs)
	}
	if !strings.Contains(out, "parts    5242880 bytes, 3 at a time") || !strings.Contains(out, "etag     \"") || !strings.Contains(out, "crc32c   "+crc32cb64(content)) {
		t.Fatalf("cp up printed:\n%s", out)
	}
	if !bytes.Equal(f.objects["photos/albums/big.bin"], content) {
		t.Fatal("the object is not the file")
	}
	if f.partPuts != 3 || f.trailers != 3 || f.algorithms["crc32c"] != 3 {
		t.Fatalf("%d parts put, %d trailers, %v; want 3, 3 and crc32c", f.partPuts, f.trailers, f.algorithms)
	}
	for _, h := range f.payloadHashes {
		if h != "UNSIGNED-PAYLOAD" && h != "STREAMING-UNSIGNED-PAYLOAD-TRAILER" {
			t.Fatalf("a body was hashed for signing: x-amz-content-sha256 %q", h)
		}
	}

	// Down again, in ranges, the same bytes.
	dst := filepath.Join(dir, "dst.bin")
	out, errs, code = cli("cp", "s3://photos/albums/big.bin", dst)
	if code != 0 {
		t.Fatalf("cp down: exit %d: %s%s", code, out, errs)
	}
	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("downloaded %d bytes, equal %v (%v)", len(got), bytes.Equal(got, content), err)
	}
	if !strings.Contains(out, "bytes    "+strconv.Itoa(len(content))) || !strings.Contains(out, "crc32c   "+crc32cb64(content)) {
		t.Fatalf("cp down printed:\n%s", out)
	}

	// To standard output: the object's bytes alone, the result on standard error.
	var out2, errb bytes.Buffer
	code = run(context.Background(), append([]string{"cp", "s3://photos/albums/big.bin", "-"}, s3...), strings.NewReader(""), &out2, &errb, os.Getenv)
	if code != 0 || !bytes.Equal(out2.Bytes(), content) {
		t.Fatalf("cp to -: exit %d, %d bytes on stdout, equal %v; stderr: %s", code, out2.Len(), bytes.Equal(out2.Bytes(), content), errb.String())
	}
	if !strings.Contains(errb.String(), "bytes    "+strconv.Itoa(len(content))) || !strings.Contains(errb.String(), "crc32c   "+crc32cb64(content)) {
		t.Fatalf("cp to - printed on stderr:\n%s", errb.String())
	}
	out2.Reset()
	errb.Reset()
	code = run(context.Background(), append([]string{"cp", "s3://photos/albums/big.bin", "-", "-json"}, s3...), strings.NewReader(""), &out2, &errb, os.Getenv)
	if code != 0 || !bytes.Equal(out2.Bytes(), content) || !strings.HasPrefix(errb.String(), `{"op":"get"`) {
		t.Fatalf("cp to - -json: exit %d, equal %v; stderr: %s", code, bytes.Equal(out2.Bytes(), content), errb.String())
	}
	// From standard input: a stream, the result on standard error.
	out2.Reset()
	errb.Reset()
	code = run(context.Background(), append([]string{"cp", "-", "s3://photos/albums/stdin.bin"}, s3...), bytes.NewReader(content[:6<<20]), &out2, &errb, os.Getenv)
	if code != 0 || out2.Len() != 0 || !bytes.Equal(f.objects["photos/albums/stdin.bin"], content[:6<<20]) || !strings.Contains(errb.String(), "put s3://photos/albums/stdin.bin") {
		t.Fatalf("cp from -: exit %d, %d bytes on stdout; stderr: %s", code, out2.Len(), errb.String())
	}

	// One put under the part size, as JSON.
	small := filepath.Join(dir, "small.txt")
	if err := os.WriteFile(small, []byte("hello, gateway\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, errs, code = cli("cp", small, "s3://photos/albums/small.txt", "-json")
	if code != 0 {
		t.Fatalf("cp small: exit %d: %s%s", code, out, errs)
	}
	var res s3Result
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("json: %v: %s", err, out)
	}
	if res.Op != "put" || res.Bytes != 15 || res.Multipart || res.ETag != etag([]byte("hello, gateway\n")) || len(res.Checksums) != 1 {
		t.Fatalf("json result: %+v", res)
	}

	// -checksum crc64 still rides and verifies.
	if out, errs, code = cli("cp", small, "s3://photos/albums/small64.txt", "-checksum", "crc64"); code != 0 || f.algorithms["crc64nvme"] != 1 {
		t.Fatalf("cp -checksum crc64: exit %d, %v: %s%s", code, f.algorithms, out, errs)
	}

	// ls, rm, and the key gone.
	out, errs, code = cli("ls", "s3://photos/albums/")
	if code != 0 {
		t.Fatalf("ls: exit %d: %s%s", code, out, errs)
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		lines = append(lines, strings.Join(strings.Fields(l), " "))
	}
	if want := []string{strconv.Itoa(len(content)) + " 2026-11-04T00:00:00Z albums/big.bin", "15 2026-11-04T00:00:00Z albums/small.txt", "15 2026-11-04T00:00:00Z albums/small64.txt", "6291456 2026-11-04T00:00:00Z albums/stdin.bin"}; strings.Join(lines, "|") != strings.Join(want, "|") {
		t.Fatalf("ls printed:\n%s", out)
	}
	if out, errs, code = cli("rm", "s3://photos/albums/big.bin"); code != 0 {
		t.Fatalf("rm: exit %d: %s%s", code, out, errs)
	}
	if _, errs, code = cli("cp", "s3://photos/albums/big.bin", dst); code != exitRefused || !strings.Contains(errs, "NoSuchKey") {
		t.Fatalf("get of a deleted key: exit %d: %s", code, errs)
	}

	// Usage errors before anything moves.
	for _, args := range [][]string{
		{"cp", src, "s3://photos"},
		{"cp", src, dst},
		{"cp", "s3://a/b", "s3://c/d"},
		{"rm", "s3://photos"},
		{"cp", src, "s3://photos/k", "-checksum", "md5"},
		{"cp", src, "s3://photos/k", "-part", "1MiB"},
	} {
		if _, _, code := cli(args...); code != exitUsage {
			t.Errorf("%v: exit %d, want %d", args, code, exitUsage)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"64MiB": 64 << 20, "5m": 5 << 20, "4G": 4 << 30, "1048576": 1 << 20, "2k": 2048, "1TiB": 1 << 40} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("%q: %d %v, want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "x", "1x", "-1", "9999999999999999999G"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("%q: no error", in)
		}
	}
}
