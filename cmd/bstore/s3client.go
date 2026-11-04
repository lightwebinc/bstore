// The S3 client: `bstore cp`, `bstore ls` and `bstore rm` with an s3://
// argument, and `bstore bench`, speaking S3 to the gateway (or to any S3)
// through the AWS SDK for Go v2 (docs/usage.md, docs/s3.md). The body is
// never hashed before it moves: payload signing is off (UNSIGNED-PAYLOAD),
// the flexible checksum rides in the trailer over TLS, and a file above one
// part goes by multipart upload, its parts read in place and sent -conc at
// a time. The timings printed are the client's own, for publishing.
package main

import (
	"context"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// s3Target is an s3://bucket/key argument.
type s3Target struct {
	bucket, key string
}

// parseS3 reads s3://bucket[/key]; ok is false for anything else.
func parseS3(s string) (t s3Target, ok bool) {
	rest, ok := strings.CutPrefix(s, "s3://")
	if !ok {
		return t, false
	}
	t.bucket, t.key, _ = strings.Cut(rest, "/")
	return t, t.bucket != ""
}

// isS3 reports whether any argument names an S3 target, which routes a
// command to the S3 client rather than the overlay.
func isS3(args []string) bool {
	for _, a := range args {
		if strings.HasPrefix(a, "s3://") {
			return true
		}
	}
	return false
}

// s3Flags are the S3 client's flags, shared by its commands.
type s3Flags struct {
	endpoint, region, ca, checksum string
	insecure, json                 bool
	part                           sizeFlag
	conc                           int
}

const s3FlagsHelp = `
S3 flags:
  -endpoint URL     the gateway (default $AWS_ENDPOINT_URL_S3, $AWS_ENDPOINT_URL)
  -region R         the signing region (default us-east-1)
  -ca FILE          a PEM certificate to trust besides the system's
  -insecure         trust any certificate
  -part SIZE        the part size (default 64MiB; K, M, G, Ki, Mi, Gi suffixes)
  -conc N           parts in flight at once (default 16)
  -checksum ALG     crc64, sha256 or none (default crc64)
  -json             the result as one JSON object on standard output

Credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, AWS_PROFILE, or
~/.aws/credentials, as the AWS CLI reads them.`

func (f *s3Flags) bind(fs *flag.FlagSet) {
	f.part = 64 << 20
	fs.StringVar(&f.endpoint, "endpoint", "", "the gateway URL")
	fs.StringVar(&f.region, "region", "us-east-1", "the signing region")
	fs.StringVar(&f.ca, "ca", "", "a PEM certificate to trust besides the system's")
	fs.BoolVar(&f.insecure, "insecure", false, "trust any certificate")
	fs.Var(&f.part, "part", "the part size")
	fs.IntVar(&f.conc, "conc", 16, "parts in flight at once")
	fs.StringVar(&f.checksum, "checksum", "crc64", "crc64, sha256 or none")
	fs.BoolVar(&f.json, "json", false, "the result as JSON")
}

// sizeFlag is a byte count with an optional unit suffix.
type sizeFlag int64

func (s *sizeFlag) String() string { return strconv.FormatInt(int64(*s), 10) }

func (s *sizeFlag) Set(v string) error {
	n, err := parseSize(v)
	if err != nil {
		return err
	}
	*s = sizeFlag(n)
	return nil
}

// parseSize reads "64MiB", "4G", "1048576" and the like.
func parseSize(v string) (int64, error) {
	v = strings.TrimSpace(v)
	i := len(v)
	for i > 0 && (v[i-1] < '0' || v[i-1] > '9') {
		i--
	}
	num, unit := v[:i], strings.ToLower(strings.TrimSuffix(strings.TrimSuffix(strings.ToLower(v[i:]), "b"), "i"))
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("size %q", v)
	}
	mult := int64(1)
	switch unit {
	case "":
	case "k":
		mult = 1 << 10
	case "m":
		mult = 1 << 20
	case "g":
		mult = 1 << 30
	case "t":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("size %q: unit %q", v, v[i:])
	}
	if n > (1<<62)/mult {
		return 0, fmt.Errorf("size %q is too large", v)
	}
	return n * mult, nil
}

// algorithm is the SDK's checksum algorithm for -checksum, "" for none.
func (f *s3Flags) algorithm() (types.ChecksumAlgorithm, error) {
	switch strings.ToLower(f.checksum) {
	case "crc64", "crc64nvme":
		return types.ChecksumAlgorithmCrc64nvme, nil
	case "sha256":
		return types.ChecksumAlgorithmSha256, nil
	case "none", "":
		return "", nil
	}
	return "", usage("-checksum %q: crc64, sha256 or none", f.checksum)
}

// client opens the S3 client: path style at -endpoint, SigV4 in -region,
// the body never signed, checksums as asked.
func (f *s3Flags) client(ctx context.Context) (*s3.Client, error) {
	if f.conc < 1 {
		return nil, usage("-conc must be at least 1")
	}
	if int64(f.part) < manager.MinUploadPartSize {
		return nil, usage("-part must be at least %d bytes", manager.MinUploadPartSize)
	}
	if _, err := f.algorithm(); err != nil {
		return nil, err
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if f.ca != "" {
		pem, err := os.ReadFile(f.ca)
		if err != nil {
			return nil, usage("-ca: %v", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, usage("-ca %s: no certificate in it", f.ca)
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, InsecureSkipVerify: f.insecure, MinVersion: tls.VersionTLS12} //nolint:gosec // -insecure is the operator's choice
	tr.MaxIdleConnsPerHost = f.conc + 2
	tr.MaxConnsPerHost = 0
	tr.ForceAttemptHTTP2 = false
	httpClient := &http.Client{Transport: tr}
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(f.region),
		config.WithHTTPClient(httpClient),
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, usage("AWS configuration: %v", err)
	}
	if _, err := cfg.Credentials.Retrieve(ctx); err != nil {
		return nil, usage("credentials: %v (set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or AWS_PROFILE)", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if f.endpoint != "" {
			o.BaseEndpoint = aws.String(f.endpoint)
		}
		o.UsePathStyle = true
		o.DisableLogOutputChecksumValidationSkipped = true
		if strings.EqualFold(f.checksum, "none") {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		}
		o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
	}), nil
}

// s3Result is what a transfer prints.
type s3Result struct {
	Op        string   `json:"op"`
	Bucket    string   `json:"bucket"`
	Key       string   `json:"key"`
	Bytes     int64    `json:"bytes"`
	Elapsed   float64  `json:"elapsed_s"`
	MBps      float64  `json:"mb_per_s"`
	Part      int64    `json:"part_bytes,omitempty"`
	Conc      int      `json:"conc,omitempty"`
	Multipart bool     `json:"multipart,omitempty"`
	ETag      string   `json:"etag,omitempty"`
	Checksums []string `json:"checksums,omitempty"` // "crc64nvme:<base64>"
}

func newResult(op string, t s3Target, n int64, d time.Duration, f *s3Flags) *s3Result {
	r := &s3Result{Op: op, Bucket: t.bucket, Key: t.key, Bytes: n, Elapsed: d.Seconds(), Part: int64(f.part), Conc: f.conc}
	if d > 0 {
		r.MBps = float64(n) / 1e6 / d.Seconds()
	}
	return r
}

func (r *s3Result) sum(name string, v *string) {
	if v != nil && *v != "" {
		r.Checksums = append(r.Checksums, name+":"+*v)
	}
}

func (r *s3Result) print(g *global, asJSON bool) error {
	if asJSON {
		b, err := json.Marshal(r)
		if err != nil {
			return err
		}
		fmt.Fprintln(g.stdout, string(b))
		return nil
	}
	g.out("%s s3://%s/%s", r.Op, r.Bucket, r.Key)
	g.out("bytes    %d", r.Bytes)
	g.out("elapsed  %.3fs", r.Elapsed)
	g.out("rate     %.1f MB/s", r.MBps)
	if r.Multipart {
		g.out("parts    %d bytes, %d at a time", r.Part, r.Conc)
	}
	if r.ETag != "" {
		g.out("etag     %s", r.ETag)
	}
	for _, c := range r.Checksums {
		name, v, _ := strings.Cut(c, ":")
		g.out("%-8s %s", name, v)
	}
	return nil
}

// s3Error maps an SDK error: a key or a bucket the service says is absent
// is refused (exit 1); anything else is a transport or usage error.
func s3Error(err error) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		msg := ae.ErrorCode()
		if m := ae.ErrorMessage(); m != "" {
			msg += ": " + m
		}
		switch ae.ErrorCode() {
		case "NoSuchKey", "NoSuchBucket", "NotFound":
			return refused("%s", msg)
		}
		return usage("%s", msg)
	}
	return err
}

const s3CpHelp = `usage: bstore cp SRC DST [S3 flags]

Copy a local file to the gateway or an object back, one side a path (or -
for standard input or output) and the other s3://bucket/key. An upload
above one part is a multipart upload, its parts sent -conc at a time; a
download is read in -conc ranges at once. The body is never hashed before
it is sent: the request is signed with UNSIGNED-PAYLOAD and the checksum
(-checksum) rides as a trailer over TLS, checked by the gateway. Prints the
bytes, the elapsed time, the rate, the ETag and the checksums the gateway
returned.` + s3FlagsHelp

// cmdS3Cp is `bstore cp` with an s3:// side.
func cmdS3Cp(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("cp", s3CpHelp)
	var f s3Flags
	f.bind(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usage("cp SRC DST")
	}
	src, srcS3 := parseS3(pos[0])
	dst, dstS3 := parseS3(pos[1])
	switch {
	case srcS3 == dstS3:
		return usage("cp: one side is a local path and the other s3://bucket/key")
	case dstS3 && dst.key == "":
		return usage("cp: the destination needs a key: s3://%s/KEY", dst.bucket)
	case srcS3 && src.key == "":
		return usage("cp: the source needs a key: s3://%s/KEY", src.bucket)
	}
	c, err := f.client(ctx)
	if err != nil {
		return err
	}
	var r *s3Result
	if dstS3 {
		r, err = s3Upload(ctx, c, &f, pos[0], dst)
	} else {
		r, err = s3Download(ctx, c, &f, src, pos[1])
	}
	if err != nil {
		return s3Error(err)
	}
	return r.print(g, f.json)
}

// s3Upload sends a file (or standard input) as one object.
func s3Upload(ctx context.Context, c *s3.Client, f *s3Flags, path string, t s3Target) (*s3Result, error) {
	var body io.Reader
	var size int64 = -1
	if path == "-" {
		body = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, usage("%v", err)
		}
		defer file.Close()
		st, err := file.Stat()
		if err != nil {
			return nil, usage("%v", err)
		}
		size = st.Size()
		body = file
	}
	alg, _ := f.algorithm()
	in := &s3.PutObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key), Body: body, ChecksumAlgorithm: alg}
	if size >= 0 {
		in.ContentLength = aws.Int64(size)
	}
	up := manager.NewUploader(c, func(u *manager.Uploader) {
		u.PartSize = int64(f.part)
		u.Concurrency = f.conc
		u.LeavePartsOnError = false
	})
	start := time.Now()
	out, err := up.Upload(ctx, in)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		size = 0 // standard input's length is not known to the client
	}
	r := newResult("put", t, size, time.Since(start), f)
	r.Multipart = out.UploadID != ""
	r.ETag = aws.ToString(out.ETag)
	r.sum("crc64nvme", out.ChecksumCRC64NVME)
	r.sum("sha256", out.ChecksumSHA256)
	return r, nil
}

// s3Download reads an object to a file, or whole to standard output.
func s3Download(ctx context.Context, c *s3.Client, f *s3Flags, t s3Target, path string) (*s3Result, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)}
	start := time.Now()
	if path == "-" {
		out, err := c.GetObject(ctx, in)
		if err != nil {
			return nil, err
		}
		defer out.Body.Close()
		n, err := io.Copy(os.Stdout, out.Body)
		if err != nil {
			return nil, err
		}
		r := newResult("get", t, n, time.Since(start), f)
		r.ETag = aws.ToString(out.ETag)
		r.sum("crc64nvme", out.ChecksumCRC64NVME)
		r.sum("sha256", out.ChecksumSHA256)
		return r, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, usage("%v", err)
	}
	dl := manager.NewDownloader(c, func(d *manager.Downloader) {
		d.PartSize = int64(f.part)
		d.Concurrency = f.conc
	})
	n, err := dl.Download(ctx, file, in)
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	r := newResult("get", t, n, time.Since(start), f)
	r.Multipart = n > int64(f.part)
	// The ETag and the whole object's checksum are on the head; the
	// ranged answers carry none.
	if h, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: in.Bucket, Key: in.Key}); err == nil {
		r.ETag = aws.ToString(h.ETag)
		r.sum("crc64nvme", h.ChecksumCRC64NVME)
		r.sum("sha256", h.ChecksumSHA256)
	}
	return r, nil
}

const s3LsHelp = `usage: bstore ls s3://bucket[/prefix] [S3 flags]

List the keys under a prefix, every page: size, last modified, key. With
-json, one array of objects.` + s3FlagsHelp

// cmdS3Ls is `bstore ls` with an s3:// argument.
func cmdS3Ls(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("ls", s3LsHelp)
	var f s3Flags
	f.bind(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("ls s3://bucket[/prefix]")
	}
	t, ok := parseS3(pos[0])
	if !ok {
		return usage("ls s3://bucket[/prefix]")
	}
	c, err := f.client(ctx)
	if err != nil {
		return err
	}
	type entry struct {
		Key          string `json:"key"`
		Size         int64  `json:"size"`
		LastModified string `json:"last_modified"`
		ETag         string `json:"etag,omitempty"`
	}
	var all []entry
	in := &s3.ListObjectsV2Input{Bucket: aws.String(t.bucket)}
	if t.key != "" {
		in.Prefix = aws.String(t.key)
	}
	pg := s3.NewListObjectsV2Paginator(c, in)
	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return s3Error(err)
		}
		for _, o := range page.Contents {
			e := entry{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: aws.ToString(o.ETag)}
			if o.LastModified != nil {
				e.LastModified = o.LastModified.UTC().Format(time.RFC3339)
			}
			if f.json {
				all = append(all, e)
			} else {
				g.out("%d\t%s\t%s", e.Size, e.LastModified, e.Key)
			}
		}
	}
	if f.json {
		if all == nil {
			all = []entry{}
		}
		b, err := json.Marshal(all)
		if err != nil {
			return err
		}
		fmt.Fprintln(g.stdout, string(b))
	}
	return nil
}

const s3RmHelp = `usage: bstore rm s3://bucket/key [S3 flags]

Delete one object through the gateway.` + s3FlagsHelp

// cmdS3Rm is `bstore rm` with an s3:// argument.
func cmdS3Rm(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("rm", s3RmHelp)
	var f s3Flags
	f.bind(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("rm s3://bucket/key")
	}
	t, ok := parseS3(pos[0])
	if !ok || t.key == "" {
		return usage("rm s3://bucket/key")
	}
	c, err := f.client(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)}); err != nil {
		return s3Error(err)
	}
	if f.json {
		return newResult("rm", t, 0, time.Since(start), &f).print(g, true)
	}
	g.out("rm s3://%s/%s", t.bucket, t.key)
	return nil
}

const benchHelp = `usage: bstore bench put|get s3://bucket/prefix -n N -size SIZE [S3 flags]

A thin benchmark through the gateway. put: N objects of SIZE fresh random
bytes, one after another, each the same multipart upload cp makes, deleted
when all are done. get: the N objects put reads (bench put -keep leaves
them), each downloaded in -conc ranges and discarded. Prints each object's
rate and the aggregate; with -json, one object per line and a last one
for the aggregate.` + s3FlagsHelp

// cmdBench is `bstore bench`.
func cmdBench(ctx context.Context, g *global, args []string) error {
	if len(args) < 1 || isHelp(args[0]) {
		return usage("%s", benchHelp)
	}
	op := args[0]
	if op != "put" && op != "get" {
		return usage("bench put|get\n%s", benchHelp)
	}
	fs := g.flagSet("bench "+op, benchHelp)
	var f s3Flags
	f.bind(fs)
	n := fs.Int("n", 4, "objects")
	var size sizeFlag = 256 << 20
	fs.Var(&size, "size", "each object's size")
	keep := fs.Bool("keep", false, "put: leave the objects for bench get")
	pos, err := parse(fs, args[1:])
	if err != nil {
		return err
	}
	if len(pos) != 1 || *n < 1 {
		return usage("bench %s s3://bucket/prefix -n N -size SIZE", op)
	}
	t, ok := parseS3(pos[0])
	if !ok {
		return usage("bench %s s3://bucket/prefix", op)
	}
	c, err := f.client(ctx)
	if err != nil {
		return err
	}
	key := func(i int) s3Target {
		return s3Target{bucket: t.bucket, key: strings.TrimSuffix(t.key, "/") + fmt.Sprintf("/bench-%03d", i)}
	}
	var total int64
	start := time.Now()
	for i := range *n {
		k := key(i)
		var r *s3Result
		if op == "put" {
			r, err = benchPut(ctx, c, &f, k, int64(size))
		} else {
			r, err = benchGet(ctx, c, &f, k)
		}
		if err != nil {
			return s3Error(err)
		}
		total += r.Bytes
		if err := r.print(g, f.json); err != nil {
			return err
		}
	}
	agg := newResult("bench "+op, t, total, time.Since(start), &f)
	agg.Multipart = int64(size) > int64(f.part)
	if err := agg.print(g, f.json); err != nil {
		return err
	}
	if op == "put" && !*keep {
		for i := range *n {
			k := key(i)
			if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(k.bucket), Key: aws.String(k.key)}); err != nil {
				return s3Error(err)
			}
		}
	}
	return nil
}

// randomReader is size bytes of fresh random data: a stream, so the
// uploader buffers each part as it would standard input.
type randomReader struct {
	rng  *rand.ChaCha8
	left int64
}

func (r *randomReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.left {
		p = p[:r.left]
	}
	n, _ := r.rng.Read(p)
	r.left -= int64(n)
	return n, nil
}

func benchPut(ctx context.Context, c *s3.Client, f *s3Flags, t s3Target, size int64) (*s3Result, error) {
	var seed [32]byte
	if _, err := crand.Read(seed[:]); err != nil {
		return nil, err
	}
	body := &randomReader{rng: rand.NewChaCha8(seed), left: size}
	alg, _ := f.algorithm()
	up := manager.NewUploader(c, func(u *manager.Uploader) {
		u.PartSize = int64(f.part)
		u.Concurrency = f.conc
	})
	start := time.Now()
	out, err := up.Upload(ctx, &s3.PutObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key), Body: body,
		ContentLength: aws.Int64(size), ChecksumAlgorithm: alg})
	if err != nil {
		return nil, err
	}
	r := newResult("put", t, size, time.Since(start), f)
	r.Multipart = out.UploadID != ""
	r.ETag = aws.ToString(out.ETag)
	r.sum("crc64nvme", out.ChecksumCRC64NVME)
	r.sum("sha256", out.ChecksumSHA256)
	return r, nil
}

// discardAt is a WriterAt that keeps nothing.
type discardAt struct{}

func (discardAt) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

func benchGet(ctx context.Context, c *s3.Client, f *s3Flags, t s3Target) (*s3Result, error) {
	dl := manager.NewDownloader(c, func(d *manager.Downloader) {
		d.PartSize = int64(f.part)
		d.Concurrency = f.conc
	})
	start := time.Now()
	n, err := dl.Download(ctx, discardAt{}, &s3.GetObjectInput{Bucket: aws.String(t.bucket), Key: aws.String(t.key)})
	if err != nil {
		return nil, err
	}
	r := newResult("get", t, n, time.Since(start), f)
	r.Multipart = n > int64(f.part)
	return r, nil
}
