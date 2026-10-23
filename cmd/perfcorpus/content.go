package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"

	"github.com/lightwebinc/bstore/storerec"
)

// content is deterministic pseudo-random bytes for (label, size, i): the
// same object every run, so a closure computed once can be cached.
func content(label string, size int, i int) []byte {
	seed := sha256.Sum256([]byte(fmt.Sprintf("bstore-perfcorpus/%s/%d/%d", label, size, i)))
	r := rand.NewChaCha8(seed)
	b := make([]byte, size)
	for off := 0; off < size; off += 8 {
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], r.Uint64())
		copy(b[off:], w[:])
	}
	return b
}

func refOf(b *storerec.Built) closureRef {
	return closureRef{Root: hex.EncodeToString(b.RootHash[:]), ContentHash: hex.EncodeToString(b.ContentHash[:]),
		ProofRoot: hex.EncodeToString(b.ProofRoot[:]), Size: b.Size, RootLen: len(b.Root)}
}

func (c closureRef) root() [32]byte        { return h32(c.Root) }
func (c closureRef) contentHash() [32]byte { return h32(c.ContentHash) }
func (c closureRef) proofRoot() [32]byte   { return h32(c.ProofRoot) }

func h32(s string) [32]byte {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		panic(fmt.Sprintf("not 32 bytes of hex: %q", s))
	}
	return [32]byte(b)
}

// closures returns n closures of objects of size bytes, cached under
// <root>/closures/<label>-<size>.json: a 256 MiB closure is built once.
func closures(root, label string, size, n int) ([]closureRef, error) {
	dir := filepath.Join(root, "closures")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.json", label, size))
	var cached []closureRef
	if err := readJSON(path, &cached); err == nil && len(cached) >= n {
		return cached[:n], nil
	}
	out := make([]closureRef, n)
	errs := make([]error, n)
	// Bound the memory: a few large objects at once.
	par := 8
	if size >= 64<<20 {
		par = 2
	}
	sem := make(chan struct{}, par)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			b, err := storerec.Build(content(label, size, i))
			if err != nil {
				errs[i] = err
				return
			}
			out[i] = refOf(b)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, writeJSON(path, out)
}
