package s3d

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc64"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/publish"
)

const mib = 1 << 20

type mpRig struct {
	t   *testing.T
	s   *Server
	ups *uploads
}

func newMPRig(t *testing.T) *mpRig {
	t.Helper()
	data := t.TempDir()
	st, err := publish.OpenStore(filepath.Join(data, "store"))
	if err != nil {
		t.Fatal(err)
	}
	ups, err := openUploads(filepath.Join(data, "uploads"), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return &mpRig{t: t, s: &Server{store: st, opts: Options{Logf: t.Logf}, uploads: ups}, ups: ups}
}

func (r *mpRig) create() *upload {
	var idb [16]byte
	_, _ = rand.Read(idb[:])
	up := &upload{ID: hex.EncodeToString(idb[:]), Bucket: "b", Key: "k", Created: time.Now()}
	up.dir = filepath.Join(r.ups.dir, up.ID)
	if err := os.MkdirAll(up.dir, 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := writeSynced(filepath.Join(up.dir, "upload.json"), []byte(fmt.Sprintf(`{"id":%q,"bucket":"b","key":"k"}`, up.ID))); err != nil {
		r.t.Fatal(err)
	}
	r.ups.mu.Lock()
	r.ups.m[up.ID] = up
	r.ups.mu.Unlock()
	return up
}

// put sends part n as uploadPart keeps it, written in uneven pieces, with
// an MD5 ETag.
func (r *mpRig) put(up *upload, n int, content []byte) {
	r.t.Helper()
	r.putETag(up, n, content, true)
}

// putETag is put with the ETag of either mode: the MD5 or the SHA-256.
func (r *mpRig) putETag(up *upload, n int, content []byte, etagMD5 bool) {
	r.t.Helper()
	part, pw, closed, err := up.openPart(n, int64(len(content)))
	if err != nil {
		r.t.Fatal(err)
	}
	defer closed()
	for rest, step := content, 1; len(rest) > 0; step = (step*7 + 13) % (300 << 10) {
		k := min(len(rest), step+1)
		if _, err := pw.Write(rest[:k]); err != nil {
			r.t.Fatal(err)
		}
		rest = rest[k:]
	}
	hashes, segs, err := pw.finish()
	if err != nil {
		r.t.Fatal(err)
	}
	if etagMD5 {
		sum := md5.Sum(content)
		part.fill(int64(len(content)), hex.EncodeToString(sum[:]), "", hashes, segs)
	} else {
		sum := sha256.Sum256(content)
		part.fill(int64(len(content)), "", hex.EncodeToString(sum[:]), hashes, segs)
	}
	if e := up.keep(n, part, pw.syncs(), 1<<40); e != nil {
		r.t.Fatal(e)
	}
}

// complete assembles the parts named as completeUpload does.
func (r *mpRig) complete(up *upload, names []int, whole http.Header) (*publish.Staged, *s3Error, bool) {
	r.t.Helper()
	up.mu.Lock()
	up.completing = true
	up.loadParts()
	var recs []partRecord
	var total uint64
	for _, n := range names {
		rec, ok := up.parts[n]
		if !ok {
			r.t.Fatalf("part %d not held", n)
		}
		recs = append(recs, rec)
		total += uint64(rec.Size)
	}
	up.mu.Unlock()
	defer func() {
		up.mu.Lock()
		up.completing = false
		up.mu.Unlock()
	}()
	linked := true
	for i := range recs {
		linked = linked && recs[i].staged(i == len(recs)-1)
	}
	staged, bad, err := r.s.assemble(context.Background(), up, names, recs, total, whole)
	if err != nil {
		r.t.Fatal(err)
	}
	return staged, bad, linked
}

// reference is what a put of content stages, in a store of its own.
func reference(t *testing.T, content []byte) *publish.Staged {
	t.Helper()
	st, err := publish.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := st.Stage(context.Background(), bytes.NewReader(content), uint64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Release)
	return s
}

func (r *mpRig) same(got *publish.Staged, content []byte) {
	r.t.Helper()
	want := reference(r.t, content)
	if got.Root != want.Root || got.ContentHash != want.ContentHash || got.ProofRoot != want.ProofRoot || got.Size != want.Size {
		r.t.Fatalf("staged differs: root %x/%x content %x/%x proof %x/%x size %d/%d", got.Root[:4], want.Root[:4],
			got.ContentHash[:4], want.ContentHash[:4], got.ProofRoot[:4], want.ProofRoot[:4], got.Size, want.Size)
	}
	if !reflect.DeepEqual(got.Manifest, want.Manifest) {
		r.t.Fatalf("manifest differs:\n%+v\n%+v", got.Manifest, want.Manifest)
	}
	// Every object of the closure is in the store, holding its hash.
	m, err := r.s.store.Manifest(got.Root)
	if err != nil || !reflect.DeepEqual(m, want.Manifest) {
		r.t.Fatalf("stored manifest: %v", err)
	}
	for _, x := range append(append([]string{m.Root}, m.Branches...), m.Blobs...) {
		h, _ := hex.DecodeString(x)
		if _, err := r.s.store.Get([32]byte(h)); err != nil {
			r.t.Fatalf("object %s: %v", x[:8], err)
		}
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestMultipartStagedMatchesPut(t *testing.T) {
	const c = chirp.ChunkSize
	type shape struct {
		name   string
		sizes  []int
		order  []int // arrival order, as indexes into sizes; nil is in order
		linked bool
	}
	shapes := []shape{
		{"aligned, last short", []int{2 * c, 2 * c, 2 * c, 3*mib + 5}, nil, true},
		{"aligned, last whole", []int{2 * c, 4 * c}, nil, true},
		{"aligned, last under a blob", []int{c, 100}, nil, true},
		{"aligned, last one byte", []int{3 * c, 1}, nil, true},
		{"single part", []int{3*c + 7}, nil, true},
		{"single part, one blob", []int{c}, nil, true},
		{"single empty part", []int{0}, nil, true},
		{"unaligned 5, 5, 3 MiB", []int{5 * mib, 5 * mib, 3 * mib}, nil, false},
		{"unaligned 6,000,000", []int{6000000, 6000000, 6000000, 1234567}, nil, false},
		{"aligned but one unaligned in the middle", []int{2 * c, c + 1, 2 * c, 9}, nil, false},
		{"out of order", []int{2 * c, 2 * c, 2 * c, 2 * c, mib}, []int{3, 1, 4, 0, 2}, true},
		{"reverse order", []int{c, c, c, c, 77}, []int{4, 3, 2, 1, 0}, true},
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			r := newMPRig(t)
			up := r.create()
			parts := make([][]byte, len(sh.sizes))
			for i, n := range sh.sizes {
				parts[i] = randBytes(n)
			}
			order := sh.order
			if order == nil {
				for i := range parts {
					order = append(order, i)
				}
			}
			for _, i := range order {
				r.put(up, i+1, parts[i])
			}
			var names []int
			for i := range parts {
				names = append(names, i+1)
			}
			got, bad, linked := r.complete(up, names, nil)
			if bad != nil {
				t.Fatal(bad)
			}
			defer got.Release()
			if linked != sh.linked {
				t.Fatalf("linked %v, want %v", linked, sh.linked)
			}
			r.same(got, cat(parts...))
		})
	}
}

func TestMultipartStagedConcurrentParts(t *testing.T) {
	r := newMPRig(t)
	up := r.create()
	parts := make([][]byte, 24)
	for i := range parts {
		parts[i] = randBytes(2 * chirp.ChunkSize)
	}
	parts[len(parts)-1] = randBytes(12345)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10)
	for i := range parts {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			r.put(up, i+1, parts[i])
		})
	}
	wg.Wait()
	var names []int
	for i := range parts {
		names = append(names, i+1)
	}
	got, bad, linked := r.complete(up, names, nil)
	if bad != nil || !linked {
		t.Fatal(bad, linked)
	}
	defer got.Release()
	r.same(got, cat(parts...))
	// The content hash ran over every part as they landed.
	up.mu.Lock()
	hashed := len(up.run.states)
	up.mu.Unlock()
	if hashed != len(parts) {
		t.Fatalf("the content hash covered %d of %d parts", hashed, len(parts))
	}
}

func TestMultipartStagedReplacedPart(t *testing.T) {
	const c = chirp.ChunkSize
	r := newMPRig(t)
	up := r.create()
	p := [][]byte{randBytes(2 * c), randBytes(2 * c), randBytes(2 * c), randBytes(999)}
	for i := range p {
		r.put(up, i+1, p[i])
	}
	// Replace part 2 with other bytes, then part 4 with a longer tail,
	// then part 1 with a different whole number of blobs.
	p[1] = randBytes(2 * c)
	r.put(up, 2, p[1])
	p[3] = randBytes(c + 3)
	r.put(up, 4, p[3])
	p[0] = randBytes(3 * c)
	r.put(up, 1, p[0])
	got, bad, linked := r.complete(up, []int{1, 2, 3, 4}, nil)
	if bad != nil || !linked {
		t.Fatal(bad, linked)
	}
	defer got.Release()
	r.same(got, cat(p...))
	// Each part keeps exactly one directory.
	ents, _ := os.ReadDir(up.dir)
	dirs := 0
	for _, e := range ents {
		if e.IsDir() {
			dirs++
		}
	}
	if dirs != 4 {
		t.Fatalf("%d part directories, want 4", dirs)
	}
	// A replacement with an unaligned part falls back, and still matches.
	p[1] = randBytes(c + 1)
	r.put(up, 2, p[1])
	got2, bad, linked := r.complete(up, []int{1, 2, 3, 4}, nil)
	if bad != nil || linked {
		t.Fatal(bad, linked)
	}
	defer got2.Release()
	r.same(got2, cat(p...))
}

func TestMultipartStagedSubset(t *testing.T) {
	const c = chirp.ChunkSize
	r := newMPRig(t)
	up := r.create()
	p := [][]byte{randBytes(c), randBytes(c), randBytes(2 * c), randBytes(c), randBytes(5)}
	for i := range p {
		r.put(up, i+1, p[i])
	}
	for _, names := range [][]int{{1, 2, 4}, {2, 3}, {1, 3, 5}, {4}, {1, 2, 3, 4}, {5}} {
		var content [][]byte
		for _, n := range names {
			content = append(content, p[n-1])
		}
		got, bad, linked := r.complete(up, names, nil)
		if bad != nil || !linked {
			t.Fatal(names, bad, linked)
		}
		r.same(got, cat(content...))
		got.Release()
	}
}

func TestMultipartStagedRepeatedBlobs(t *testing.T) {
	const c = chirp.ChunkSize
	r := newMPRig(t)
	up := r.create()
	same := randBytes(c)
	p := [][]byte{cat(same, same), cat(same, same), make([]byte, 2*c), cat(same, []byte("tail"))}
	for i := range p {
		r.put(up, i+1, p[i])
	}
	got, bad, linked := r.complete(up, []int{1, 2, 3, 4}, nil)
	if bad != nil || !linked {
		t.Fatal(bad, linked)
	}
	defer got.Release()
	r.same(got, cat(p...))
}

func TestMultipartStagedAfterRestart(t *testing.T) {
	const c = chirp.ChunkSize
	r := newMPRig(t)
	up := r.create()
	p := [][]byte{randBytes(2 * c), randBytes(2 * c), randBytes(c)}
	for i := range p {
		r.put(up, i+1, p[i])
	}
	// A stop cut a part short: its directory, and a temporary file.
	if err := os.Mkdir(filepath.Join(up.dir, "part-00002.0000aaaa"), 0o700); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(up.dir, "tmp-1"), []byte("x"), 0o600)
	// A part kept in one file, as before blobs, beside the others.
	p = append(p, randBytes(c+11))
	if err := os.WriteFile(filepath.Join(up.dir, "part-00004"), p[3], 0o600); err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum(p[3])
	_ = writeSynced(filepath.Join(up.dir, "part-00004.json"), []byte(fmt.Sprintf(`{"size":%d,"md5":%q}`, len(p[3]), hex.EncodeToString(sum[:]))))
	ups, err := openUploads(r.ups.dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	r.ups = ups
	r.s.uploads = ups
	up2 := ups.get(up.ID)
	if up2 == nil {
		t.Fatal("upload not reopened")
	}
	if _, err := os.Stat(filepath.Join(up.dir, "part-00002.0000aaaa")); !os.IsNotExist(err) {
		t.Fatal("an unnamed part directory survived the restart")
	}
	if _, err := os.Stat(filepath.Join(up.dir, "tmp-1")); !os.IsNotExist(err) {
		t.Fatal("a temporary file survived the restart")
	}
	// Parts 1 to 3 are blobs, hashed before the restart.
	got, bad, linked := r.complete(up2, []int{1, 2, 3}, nil)
	if bad != nil || !linked {
		t.Fatal(bad, linked)
	}
	r.same(got, cat(p[:3]...))
	got.Release()
	// The part kept in one file is read.
	got, bad, linked = r.complete(up2, []int{1, 2, 3, 4}, nil)
	if bad != nil || linked {
		t.Fatal(bad, linked)
	}
	r.same(got, cat(p...))
	got.Release()
	// Replacing it with blobs makes the object linkable again, and
	// removes the file it was kept in.
	p[3] = randBytes(c + 11)
	r.put(up2, 4, p[3])
	if _, err := os.Stat(filepath.Join(up.dir, "part-00004")); !os.IsNotExist(err) {
		t.Fatal("the replaced part's file survived")
	}
	got, bad, linked = r.complete(up2, []int{1, 2, 3, 4}, nil)
	if bad != nil || !linked {
		t.Fatal(bad, linked)
	}
	r.same(got, cat(p...))
	got.Release()
}

func TestMultipartWholeChecksum(t *testing.T) {
	const c = chirp.ChunkSize
	for _, aligned := range []bool{true, false} {
		r := newMPRig(t)
		up := r.create()
		p := [][]byte{randBytes(2 * c), randBytes(c / 3)}
		if !aligned {
			p[0] = randBytes(2*c + 1)
		}
		for i := range p {
			r.put(up, i+1, p[i])
		}
		h := crc64.New(crc64NVME)
		h.Write(cat(p...))
		whole := http.Header{}
		whole.Set("X-Amz-Checksum-Crc64nvme", base64.StdEncoding.EncodeToString(h.Sum(nil)))
		got, bad, linked := r.complete(up, []int{1, 2}, whole)
		if bad != nil || linked != aligned {
			t.Fatal(bad, linked)
		}
		r.same(got, cat(p...))
		got.Release()
		whole.Set("X-Amz-Checksum-Crc64nvme", base64.StdEncoding.EncodeToString(make([]byte, 8)))
		got, bad, _ = r.complete(up, []int{1, 2}, whole)
		if bad == nil || bad.Code != "BadDigest" {
			t.Fatalf("a wrong whole-object checksum: %v", bad)
		}
		got.Release()
	}
}

// TestMultipartUnlinkablePartsInOneFile checks that once an upload cannot
// be linked (two parts that are not whole blobs), its parts are kept in one
// file each and not hashed, and that a restart keeps them.
func TestMultipartUnlinkablePartsInOneFile(t *testing.T) {
	r := newMPRig(t)
	up := r.create()
	p := [][]byte{randBytes(6000000), randBytes(6000000), randBytes(6000000), randBytes(777)}
	for i := range p {
		r.put(up, i+1, p[i])
	}
	up.mu.Lock()
	first, rest := up.parts[1], []partRecord{up.parts[2], up.parts[3], up.parts[4]}
	hashed := len(up.run.states)
	up.mu.Unlock()
	if first.Dir == "" || len(first.Blobs) != 2 {
		t.Fatalf("part 1, alone when it arrived, is not kept as blobs: %+v", first.Dir)
	}
	for i, rec := range rest {
		if rec.File == "" || rec.Dir != "" || len(rec.Blobs) != 0 {
			t.Fatalf("part %d is not kept in one file", i+2)
		}
	}
	// Part 1 may have been hashed while it was the only part.
	if hashed > 1 {
		t.Fatalf("the content hash ran over %d parts of an upload no object can be linked through", hashed)
	}
	ups, err := openUploads(r.ups.dir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	up2 := ups.get(up.ID)
	got, bad, linked := r.complete(up2, []int{1, 2, 3, 4}, nil)
	if bad != nil || linked {
		t.Fatal(bad, linked)
	}
	r.same(got, cat(p...))
	got.Release()
}

func TestMultipartAbortRemovesParts(t *testing.T) {
	r := newMPRig(t)
	up := r.create()
	for i := 1; i <= 6; i++ {
		r.put(up, i, randBytes(2*chirp.ChunkSize))
	}
	up.mu.Lock()
	up.gone = true
	up.mu.Unlock()
	r.ups.remove(up)
	if _, err := os.Stat(up.dir); !os.IsNotExist(err) {
		t.Fatal("the upload's directory survived its abort")
	}
	// The hash goroutine winds down on its own.
	deadline := time.Now().Add(10 * time.Second)
	for {
		up.mu.Lock()
		busy := up.run.busy
		up.mu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the content hash goroutine did not stop")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMultipartPartRaceAndRefusal(t *testing.T) {
	const c = chirp.ChunkSize
	r := newMPRig(t)
	up := r.create()
	a, b := randBytes(2*c), randBytes(2*c)
	var wg sync.WaitGroup
	wg.Go(func() { r.put(up, 1, a) })
	wg.Go(func() { r.put(up, 1, b) })
	wg.Wait()
	r.put(up, 2, randBytes(10))
	up.mu.Lock()
	rec := up.parts[1]
	up.mu.Unlock()
	want := a
	if sum := md5.Sum(b); hex.EncodeToString(sum[:]) == rec.MD5 {
		want = b
	}
	got, bad, linked := r.complete(up, []int{1}, nil)
	if bad != nil || !linked {
		t.Fatal(bad, linked)
	}
	r.same(got, want)
	got.Release()
	dirs := 0
	ents, _ := os.ReadDir(up.dir)
	for _, e := range ents {
		if e.IsDir() {
			dirs++
		}
	}
	if dirs != 2 {
		t.Fatalf("%d part directories, want 2", dirs)
	}

	// A part refused (over the bound, or the upload closing) leaves
	// nothing behind.
	for _, closing := range []bool{false, true} {
		pw, err := newPartWriter(filepath.Join(up.dir, "part-00003.refused"))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = pw.Write(randBytes(c))
		if _, _, err := pw.finish(); err != nil {
			t.Fatal(err)
		}
		max := int64(1 << 40)
		if closing {
			up.mu.Lock()
			up.completing = true
			up.mu.Unlock()
		} else {
			max = 1
		}
		part := partRecord{Size: c, MD5: "00", Dir: "part-00003.refused"}
		if e := up.keep(3, part, pw.syncs(), max); e == nil {
			t.Fatal("kept")
		}
		up.mu.Lock()
		up.completing = false
		_, held := up.parts[3]
		up.mu.Unlock()
		if _, err := os.Stat(filepath.Join(up.dir, "part-00003.refused")); held || !os.IsNotExist(err) {
			t.Fatal("a refused part was left behind")
		}
	}
}

// TestMultipartStagedRandomShapes uploads random aligned and unaligned
// shapes in random order with random replacements.
func TestMultipartStagedRandomShapes(t *testing.T) {
	const c = chirp.ChunkSize
	rnd := func(n int) int {
		v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
		return int(v.Int64())
	}
	rounds := 12
	if testing.Short() {
		rounds = 4
	}
	for round := range rounds {
		r := newMPRig(t)
		up := r.create()
		n := 1 + rnd(6)
		p := make([][]byte, n)
		for i := range p {
			if rnd(4) == 0 {
				p[i] = randBytes(1 + rnd(3*c))
			} else {
				p[i] = randBytes(c * (1 + rnd(2)))
			}
		}
		if rnd(2) == 0 {
			p[n-1] = randBytes(1 + rnd(c))
		}
		perm := make([]int, n)
		for i := range perm {
			perm[i] = i
		}
		for i := n - 1; i > 0; i-- {
			j := rnd(i + 1)
			perm[i], perm[j] = perm[j], perm[i]
		}
		for _, i := range perm {
			r.put(up, i+1, p[i])
			if rnd(3) == 0 {
				k := rnd(n)
				up.mu.Lock()
				size := up.parts[k+1].Size
				up.mu.Unlock()
				if size > 0 {
					p[k] = randBytes(int(size))
					r.put(up, k+1, p[k])
				}
			}
		}
		var names []int
		for i := range p {
			names = append(names, i+1)
		}
		got, bad, _ := r.complete(up, names, nil)
		if bad != nil {
			t.Fatal(round, bad)
		}
		r.same(got, cat(p...))
		got.Release()
	}
}
