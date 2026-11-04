package s3d

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
)

// TestMultipartThroughput times a multipart upload as the AWS CLI sends one
// by default (8 MiB parts, 10 at once, each with a CRC-64/NVME trailer) and
// its completion, on the local disk under BSTORE_MP_BENCH_DIR:
//
//	BSTORE_MP_BENCH_DIR=/path/on/disk go test -run TestMultipartThroughput -v ./internal/s3d
//
// BSTORE_MP_BENCH_MODE "legacy" keeps each part in one file and stages the
// object at completion by reading it, as the gateway did before parts were
// kept as blobs; anything else is the gateway's path. BSTORE_MP_BENCH_SIZE
// and BSTORE_MP_BENCH_PART (bytes) and BSTORE_MP_BENCH_CONC change the
// shape.
func TestMultipartThroughput(t *testing.T) {
	dir := os.Getenv("BSTORE_MP_BENCH_DIR")
	if dir == "" {
		t.Skip("set BSTORE_MP_BENCH_DIR to a directory on the disk to measure")
	}
	env := func(k string, d int) int {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
			return v
		}
		return d
	}
	size, partSize, conc := env("BSTORE_MP_BENCH_SIZE", 1<<30), env("BSTORE_MP_BENCH_PART", 8<<20), env("BSTORE_MP_BENCH_CONC", 10)
	legacy := os.Getenv("BSTORE_MP_BENCH_MODE") == "legacy"
	work, err := os.MkdirTemp(dir, "mpbench-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(work)
	content := make([]byte, size)
	rng := rand.NewChaCha8([32]byte{1})
	_, _ = rng.Read(content)

	r := &mpRig{t: t}
	r.ups, err = openUploads(filepath.Join(work, "uploads"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	r.s = &Server{opts: Options{Logf: t.Logf}, uploads: r.ups}
	if r.s.store, err = publish.OpenStore(filepath.Join(work, "store")); err != nil {
		t.Fatal(err)
	}
	up := r.create()
	var parts [][]byte
	for off := 0; off < size; off += partSize {
		parts = append(parts, content[off:min(off+partSize, size)])
	}
	trailer := http.Header{}
	start := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, conc)
	for i := range parts {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			bh := newBodyHashes(trailer, "x-amz-checksum-crc64nvme")
			src := bh.reader(bytes.NewReader(parts[i]))
			if legacy {
				legacyPart(t, up, i+1, src, bh)
			} else {
				r.putFrom(up, i+1, src, bh)
			}
		})
	}
	wg.Wait()
	landed := time.Since(start)
	var names []int
	for i := range parts {
		names = append(names, i+1)
	}
	mid := time.Now()
	if legacy {
		legacyComplete(t, r, up, names, uint64(size))
	} else {
		up.mu.Lock()
		hashed := len(up.run.states)
		up.mu.Unlock()
		staged, bad, linked := r.complete(up, names, nil)
		if bad != nil {
			t.Fatal(bad)
		}
		staged.Release()
		t.Logf("from blobs: %v; content hash ran over %d of %d parts before completion", linked, hashed, len(parts))
	}
	done := time.Since(mid)
	total := time.Since(start)
	mbs := func(d time.Duration) float64 { return float64(size) / d.Seconds() / 1e6 }
	mode := "staged"
	if legacy {
		mode = "legacy"
	}
	t.Logf("%s: %d MiB in %d parts of %d bytes, %d at once: parts %.2f s (%.0f MB/s), completion %.2f s, total %.2f s (%.0f MB/s)",
		mode, size>>20, len(parts), partSize, conc, landed.Seconds(), mbs(landed), done.Seconds(), total.Seconds(), mbs(total))
}

// putFrom is put of a part read from src through its body hashes.
func (r *mpRig) putFrom(up *upload, n int, src io.Reader, bh *bodyHashes) {
	r.t.Helper()
	name := fmt.Sprintf("part-%05d.bench%d", n, n)
	pw, err := newPartWriter(filepath.Join(up.dir, name))
	if err != nil {
		r.t.Fatal(err)
	}
	if _, err := io.Copy(pw, src); err != nil {
		r.t.Fatal(err)
	}
	hashes, segs, err := pw.finish()
	if err != nil {
		r.t.Fatal(err)
	}
	part := partRecord{Size: bh.n, MD5: bh.md5Hex(), Dir: name}
	for i := range hashes {
		part.Blobs = append(part.Blobs, hex.EncodeToString(hashes[i][:]))
		part.Segs = append(part.Segs, hex.EncodeToString(segs[i][:]))
	}
	if e := up.keep(n, part, pw.syncs(), 1<<40); e != nil {
		r.t.Fatal(e)
	}
}

// legacyPart is uploadPart as it was: the part in one file, synced, then
// its record.
func legacyPart(t *testing.T, up *upload, n int, src io.Reader, bh *bodyHashes) {
	f, err := os.CreateTemp(up.dir, "tmp-*")
	if err != nil {
		t.Error(err)
		return
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, err = io.Copy(f, src)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Error(err)
		return
	}
	md5h := bh.md5Hex()
	rec, _ := json.Marshal(partRecord{Size: bh.n, MD5: md5h})
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.size(n)+bh.n > 1<<40 {
		t.Error("too large")
	}
	if err := os.Rename(tmp, up.partPath(n)); err != nil {
		t.Error(err)
	}
	if err := writeSynced(up.partPath(n)+".json", rec); err != nil {
		t.Error(err)
	}
	up.parts[n] = partRecord{Size: bh.n, MD5: md5h}
}

// legacyComplete is the completion as it was: every part read back
// through an MD5 and staged as a put's body.
func legacyComplete(t *testing.T, r *mpRig, up *upload, names []int, total uint64) {
	var files []string
	for _, n := range names {
		files = append(files, up.partPath(n))
	}
	sums := newBodyHashes(http.Header{}, "")
	defer sums.sync()
	staged, err := r.s.store.Stage(context.Background(), sums.reader(&partsReader{files: files}), total)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Size != total {
		t.Fatal("size")
	}
	staged.Release()
}
