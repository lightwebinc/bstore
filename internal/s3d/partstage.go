package s3d

import (
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/publish"
)

// A part is kept as it arrives the way the closure will hold it: cut into
// blobs of chirp.ChunkSize from the part's start, each blob a file of its
// own in the part's directory, hashed (publish.HashBlob) as it lands. When
// every part but the last is a whole number of blobs, the object's blobs
// are exactly the parts' blobs in order, and completion links them into
// the store and builds the closure from the recorded hashes without
// reading the content again (publish.StageLinked). Otherwise completion
// reads the blob files in order and stages the content as a put does.

// partWorkers is how many of one part's blobs are hashed and written at
// once; a part holds at most partWorkers + 1 blob buffers.
const partWorkers = 2

var blobBufs = sync.Pool{New: func() any {
	b := make([]byte, 0, chirp.ChunkSize)
	return &b
}}

// partWriter cuts a part into blob files under dir as it is written.
type partWriter struct {
	dir  string
	cur  []byte
	work chan blobJob
	free chan []byte
	made int
	wg   sync.WaitGroup

	mu     sync.Mutex
	hashes [][32]byte
	segs   [][32]byte
	err    error
}

type blobJob struct {
	i int
	b []byte
}

func newPartWriter(dir string) (*partWriter, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, err
	}
	w := &partWriter{dir: dir, work: make(chan blobJob, partWorkers), free: make(chan []byte, partWorkers+1)}
	w.wg.Add(partWorkers)
	for range partWorkers {
		go w.worker()
	}
	return w, nil
}

func blobFile(dir string, i int) string { return filepath.Join(dir, fmt.Sprintf("%06d", i)) }

func (w *partWriter) failed() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *partWriter) worker() {
	defer w.wg.Done()
	for j := range w.work {
		if w.failed() == nil {
			h, seg, err := publish.HashBlob(j.b)
			if err == nil {
				err = writeBlob(blobFile(w.dir, j.i), j.b)
			}
			w.mu.Lock()
			if err != nil && w.err == nil {
				w.err = err
			}
			w.hashes[j.i], w.segs[j.i] = h, seg
			w.mu.Unlock()
		}
		w.free <- j.b[:0]
	}
}

// writeBlob writes a blob file; it is synced with the rest of its part in
// finish.
func writeBlob(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (w *partWriter) buffer() []byte {
	select {
	case b := <-w.free:
		return b
	default:
	}
	if w.made < cap(w.free) {
		w.made++
		return (*blobBufs.Get().(*[]byte))[:0]
	}
	return <-w.free
}

func (w *partWriter) dispatch() {
	w.mu.Lock()
	i := len(w.hashes)
	w.hashes = append(w.hashes, [32]byte{})
	w.segs = append(w.segs, [32]byte{})
	w.mu.Unlock()
	w.work <- blobJob{i: i, b: w.cur}
	w.cur = nil
}

func (w *partWriter) Write(p []byte) (int, error) {
	if err := w.failed(); err != nil {
		return 0, err
	}
	n := len(p)
	for len(p) > 0 {
		if w.cur == nil {
			w.cur = w.buffer()
		}
		k := min(chirp.ChunkSize-len(w.cur), len(p))
		w.cur = append(w.cur, p[:k]...)
		p = p[k:]
		if len(w.cur) == chirp.ChunkSize {
			w.dispatch()
		}
	}
	return n, nil
}

// stop waits for the workers and returns every buffer to the pool.
func (w *partWriter) stop() {
	close(w.work)
	w.wg.Wait()
	if w.cur != nil {
		w.free <- w.cur
		w.cur = nil
	}
	for len(w.free) > 0 {
		b := <-w.free
		blobBufs.Put(&b)
	}
}

// finish writes the last blob and waits for every blob: each is hashed and
// written, not yet synced (syncs).
func (w *partWriter) finish() (hashes, segs [][32]byte, err error) {
	if len(w.cur) > 0 && w.failed() == nil {
		w.dispatch()
	}
	w.stop()
	if err := w.failed(); err != nil {
		return nil, nil, err
	}
	return w.hashes, w.segs, nil
}

// syncs make the part durable once finish returned: a sync of each blob
// file and of the directory, to run at once with each other (parallel).
func (w *partWriter) syncs() []func() error {
	out := []func() error{func() error { return syncDir(w.dir) }}
	for i := range w.hashes {
		out = append(out, func() error {
			f, err := os.OpenFile(blobFile(w.dir, i), os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			return f.Sync()
		})
	}
	return out
}

// parallel runs fns, at most limit at once, and returns their errors.
func parallel(limit int, fns []func() error) error {
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	errs := make([]error, len(fns))
	for i, f := range fns {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			errs[i] = f()
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// abort stops the writer and removes what it wrote.
func (w *partWriter) abort() {
	w.mu.Lock()
	if w.err == nil {
		w.err = errors.New("s3d: part aborted")
	}
	w.mu.Unlock()
	w.stop()
	_ = os.RemoveAll(w.dir)
}

// files are the files a part's bytes are in, in order: its blob files, or
// the one file a part was kept in before blobs were.
func (up *upload) files(n int, p partRecord) []string {
	if p.Dir == "" {
		return []string{up.partPath(n)}
	}
	dir := filepath.Join(up.dir, p.Dir)
	out := make([]string, len(p.Blobs))
	for i := range out {
		out[i] = blobFile(dir, i)
	}
	return out
}

// staged reports whether a part is kept as blobs that an object's blobs
// can be made of: as a part before the last, it must be a whole number of
// blobs.
func (p partRecord) staged(last bool) bool {
	if p.Dir == "" || len(p.Blobs) != len(p.Segs) || int64(len(p.Blobs)) != (p.Size+chirp.ChunkSize-1)/chirp.ChunkSize {
		return false
	}
	return last || p.Size%chirp.ChunkSize == 0
}

// linked is a part's blobs as publish.StageLinked takes them.
func (up *upload) linked(p partRecord) ([]publish.LinkedBlob, error) {
	out := make([]publish.LinkedBlob, len(p.Blobs))
	dir := filepath.Join(up.dir, p.Dir)
	for i := range out {
		h, err1 := hex.DecodeString(p.Blobs[i])
		s, err2 := hex.DecodeString(p.Segs[i])
		if err1 != nil || err2 != nil || len(h) != 32 || len(s) != 32 {
			return nil, errors.New("s3d: a part record's blob hashes are malformed")
		}
		n := uint64(chirp.ChunkSize)
		if i == len(out)-1 {
			n = uint64(p.Size) - uint64(i)*chirp.ChunkSize
		}
		out[i] = publish.LinkedBlob{Path: blobFile(dir, i), Hash: [32]byte(h), SegRoot: [32]byte(s), Length: n, Fresh: p.fresh}
	}
	return out, nil
}

// The content hash, SHA-256 over the whole object, runs while the parts
// land: whenever part k+1 is held and the hash covers parts 1 to k, one
// goroutine of the upload's reads the part's files and takes the hash on
// over it, keeping the hash's state after every part. A part replaced
// drops the states from it on. Completion waits for the goroutine, takes
// the state after the longest run of the parts it names that starts at
// part 1 and is hashed, and hashes the rest itself.

// runHash is an upload's content hash so far; its fields are guarded by
// the upload's mu.
type runHash struct {
	states [][]byte // states[k] is the state after parts 1 to k+1
	busy   bool
	idle   *sync.Cond
}

// dropFrom forgets the states from part n on; the caller holds up.mu.
func (up *upload) dropFrom(n int) {
	if len(up.run.states) >= n {
		up.run.states = up.run.states[:n-1]
	}
}

// kick starts the hash goroutine unless it runs; the caller holds up.mu.
func (up *upload) kick() {
	if up.run.busy || up.gone {
		return
	}
	up.run.busy = true
	go up.advance()
}

func (up *upload) advance() {
	up.mu.Lock()
	defer up.mu.Unlock()
	defer func() {
		up.run.busy = false
		up.idleCond().Broadcast()
	}()
	for !up.gone {
		k := len(up.run.states) + 1
		p, ok := up.parts[k]
		if !ok {
			return
		}
		var base []byte
		if k > 1 {
			base = up.run.states[k-2]
		}
		files := up.files(k, p)
		up.mu.Unlock()
		st, err := hashOn(base, files)
		up.mu.Lock()
		current := len(up.run.states)+1 == k && up.parts[k].gen == p.gen
		if current && err == nil {
			up.run.states = append(up.run.states, st)
		}
		if err != nil && current {
			// The part's files cannot be read: the completion reads them
			// itself, and fails as it should.
			return
		}
	}
}

func (up *upload) idleCond() *sync.Cond {
	if up.run.idle == nil {
		up.run.idle = sync.NewCond(&up.mu)
	}
	return up.run.idle
}

// contentHash is SHA-256 over the parts named, in order, from the states
// kept where they apply. The caller has marked the upload completing, so
// no part changes under it.
func (up *upload) contentHash(names []int, recs []partRecord) ([32]byte, error) {
	up.mu.Lock()
	for up.run.busy {
		up.idleCond().Wait()
	}
	k := 0
	for k < len(names) && k < len(up.run.states) && names[k] == k+1 {
		k++
	}
	var base []byte
	if k > 0 {
		base = up.run.states[k-1]
	}
	up.mu.Unlock()
	h, err := restore(base)
	if err != nil {
		return [32]byte{}, err
	}
	for i := k; i < len(names); i++ {
		if err := hashFiles(h, up.files(names[i], recs[i])); err != nil {
			return [32]byte{}, err
		}
	}
	return [32]byte(h.Sum(nil)), nil
}

func restore(state []byte) (hash.Hash, error) {
	h := sha256.New()
	if state != nil {
		if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func hashOn(base []byte, files []string) ([]byte, error) {
	h, err := restore(base)
	if err != nil {
		return nil, err
	}
	if err := hashFiles(h, files); err != nil {
		return nil, err
	}
	return h.(encoding.BinaryMarshaler).MarshalBinary()
}

// hashFiles writes the files, in order, to h.
func hashFiles(h io.Writer, files []string) error {
	_, err := io.CopyBuffer(h, &partsReader{files: files}, make([]byte, 1<<20))
	return err
}
