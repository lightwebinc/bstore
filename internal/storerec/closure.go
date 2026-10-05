package storerec

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/lightwebinc/bcommon/chirp"
	"github.com/lightwebinc/bcommon/commit"
)

// ClosureLimits are the limits bstore verifies a closure with: content of
// at most 2^43 bytes (an entry's size bound), and bcommon's default bound
// on references.
var ClosureLimits = chirp.Limits{MaxLength: MaxSize}

// Built is an object's CHIRP Profile 1 closure, as bstore builds it (no
// extension on the root), and its proof root.
type Built struct {
	chirp.Built
	ContentHash [32]byte
	ProofRoot   [32]byte
	Size        uint64
}

// Build builds an object's closure and proof root from content in memory.
func Build(content []byte) (*Built, error) {
	if uint64(len(content)) > MaxSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrSize, len(content))
	}
	b, err := chirp.Build(content, nil)
	if err != nil {
		return nil, err
	}
	return &Built{Built: b, ContentHash: sha256.Sum256(content), ProofRoot: ProofRoot(content), Size: uint64(len(content))}, nil
}

// Objects are the closure's distinct objects in publishing order: the root,
// the branches, then the blobs, each once.
func (b *Built) Objects() [][]byte {
	seen := map[[32]byte]bool{}
	var out [][]byte
	add := func(o []byte) {
		h := sha256.Sum256(o)
		if !seen[h] {
			seen[h] = true
			out = append(out, o)
		}
	}
	add(b.Root)
	for _, br := range b.Branches {
		add(br)
	}
	for _, bl := range b.Blobs {
		add(bl)
	}
	return out
}

// Pieces are the closure's pieces for a bucket and writer: one per distinct
// object (spec section 5).
func (b *Built) Pieces(bucket string, writer []byte) []*Piece {
	var out []*Piece
	for _, o := range b.Objects() {
		out = append(out, &Piece{Bucket: bucket, Writer: writer, Root: b.RootHash, Piece: o})
	}
	return out
}

// Streaming builds the closure and the proof root of content written in
// pieces, holding at most one blob and one segment: each blob goes to emit
// as soon as it is complete.
type Streaming struct {
	chunker *chirp.Chunker
	proof   *commit.SegmentWriter
	content [32]byte
	n       uint64
}

// NewStreaming returns a streaming builder that hands each blob to emit.
func NewStreaming(emit func(blob []byte) error) *Streaming {
	w, _ := commit.NewSegmentWriter(SegmentSize)
	return &Streaming{chunker: chirp.NewChunker(emit), proof: w}
}

// Write takes content in order.
func (s *Streaming) Write(p []byte) (int, error) {
	if s.n+uint64(len(p)) > MaxSize {
		return 0, fmt.Errorf("%w: over 2^43 bytes", ErrSize)
	}
	n, err := s.chunker.Write(p)
	s.proof.Write(p[:n])
	s.n += uint64(n)
	return n, err
}

// Finish hands over the last blob and returns the closure (Blobs nil) and
// its proof root.
func (s *Streaming) Finish() (*Built, error) {
	b, err := s.chunker.Finish(nil)
	if err != nil {
		return nil, err
	}
	root, err := chirp.DecodeRoot(b.Root)
	if err != nil {
		return nil, err
	}
	return &Built{Built: b, ContentHash: root.ContentHash, ProofRoot: s.proof.Root(), Size: s.n}, nil
}

// CheckRoot holds a root node's bytes to bstore's root rules (spec sections
// 5 and 15.2 step 6): it hashes to root, decodes as a CHIRP version 1.0 root
// (a later minor version is refused), declares chunking profile 1, carries
// no extension (bcommon's decoder keeps advisory extensions, so bstore
// checks the root holds none itself), and its logicalLength and contentHash
// are the entry's. Rule names the first that fails.
func CheckRoot(rootBytes []byte, root [32]byte, size uint64, contentHash [32]byte) (*chirp.Root, error) {
	if sha256.Sum256(rootBytes) != root {
		return nil, ruled(ErrRoot, "hash")
	}
	r, err := chirp.DecodeRoot(rootBytes)
	if err != nil {
		code, _ := chirp.Reason(err)
		return nil, ruled(ErrRoot, code)
	}
	switch {
	case r.Profile != chirp.Profile1:
		return nil, ruled(ErrRoot, "profile")
	case len(r.Extensions) != 0:
		return nil, ruled(ErrRoot, "extension")
	case r.Length != size:
		return nil, ruled(ErrRoot, "length")
	case r.ContentHash != contentHash:
		return nil, ruled(ErrRoot, "content-hash")
	}
	return &r, nil
}

// VerifyCHIRP is bcommon chirp.Verify with ClosureLimits behind a fetch
// that takes only bytes hashing to the hash asked for, as a reader that
// asks several advertisers does, with its refusals as labels: an object no
// fetch served, or served only as other bytes, is closure (rule missing or
// hash); a node's own checks, a length, depth, cycle, references, too-large
// or canonical construction is root (the closure the root names is not
// valid, whoever serves it); content-hash is content.
func VerifyCHIRP(rootHash [32]byte, fetch chirp.Fetch) (*chirp.Closure, error) {
	wrong := false
	checked := func(h [32]byte) ([]byte, error) {
		o, err := fetch(h)
		if err != nil {
			return nil, err
		}
		if sha256.Sum256(o) != h {
			wrong = true
			return nil, errors.New("served other bytes")
		}
		return o, nil
	}
	c, err := chirp.Verify(rootHash, checked, ClosureLimits)
	if err == nil {
		return c, nil
	}
	code, _ := chirp.Reason(err)
	switch {
	case errors.Is(err, chirp.ErrMissing) && wrong:
		return nil, ruled(ErrClosure, "hash")
	case errors.Is(err, chirp.ErrMissing), errors.Is(err, chirp.ErrHash):
		return nil, ruled(ErrClosure, code)
	case errors.Is(err, chirp.ErrContentHash):
		return nil, ruled(ErrContent, code)
	default:
		return nil, ruled(ErrRoot, code)
	}
}

// VerifyClosure is a provider's check before it holds, and a reader's
// steps 6 to 9 on a whole read (spec sections 9.3 and 15.2): the root
// fetched (closure when none is served as its hash), CheckRoot (root), the
// closure by VerifyCHIRP, then, when proof is true, the proof root
// recomputed over the content against the entry's (proof-root). It
// returns the verified closure.
func VerifyClosure(e *Entry, fetch chirp.Fetch, proof bool) (*chirp.Closure, error) {
	rb, err := fetch(e.Root)
	if err != nil {
		return nil, ruled(ErrClosure, "missing")
	}
	if sha256.Sum256(rb) != e.Root {
		return nil, ruled(ErrClosure, "hash")
	}
	if _, err := CheckRoot(rb, e.Root, e.Size, e.ContentHash); err != nil {
		return nil, err
	}
	c, err := VerifyCHIRP(e.Root, fetch)
	if err != nil {
		return nil, err
	}
	if proof {
		w, _ := commit.NewSegmentWriter(SegmentSize)
		for _, bl := range c.Blobs {
			o, err := fetch(bl.Hash)
			if err != nil || sha256.Sum256(o) != bl.Hash {
				return nil, ruled(ErrClosure, "missing")
			}
			w.Write(o)
		}
		if w.Root() != e.ProofRoot {
			return nil, ErrProofRoot
		}
	}
	return c, nil
}
