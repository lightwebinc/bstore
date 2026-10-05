package storerec

import (
	"crypto/sha256"

	"github.com/lightwebinc/bcommon/commit"
)

// ProofRoot is the proof root of content (spec section 6): the RFC 6962
// Merkle Tree Hash over its 4 KiB segments in order, the last unpadded;
// SHA-256 of the empty string for no bytes.
func ProofRoot(content []byte) [32]byte {
	r, _ := commit.SegmentRoot(content, SegmentSize)
	return r
}

// BlobSubtree is the subtree root over one blob's segments: what a native
// provider keeps per blob and rebuilds on demand.
func BlobSubtree(blob []byte) [32]byte { return ProofRoot(blob) }

// ProofRootOfBlobs is the proof root as built per blob: each blob's subtree
// root, then RFC 6962's split over the subtree roots taken as nodes. It
// equals ProofRoot because every blob but the last holds exactly 1024
// segments (bcommon commit.RootOfSubtrees).
func ProofRootOfBlobs(blobs [][]byte) [32]byte {
	if len(blobs) == 0 {
		return EmptyHash
	}
	roots := make([][32]byte, len(blobs))
	for i, bl := range blobs {
		roots[i] = BlobSubtree(bl)
	}
	r, _ := commit.RootOfSubtrees(roots)
	return r
}

// ProofTree holds every segment's leaf hash of an object: what a bridge
// front keeps (32 bytes per segment) and answers paths from.
type ProofTree struct {
	Size   uint64
	Leaves [][32]byte
}

// NewProofTree hashes content's segments.
func NewProofTree(content []byte) *ProofTree {
	t := &ProofTree{Size: uint64(len(content))}
	for off := 0; off < len(content); off += SegmentSize {
		t.Leaves = append(t.Leaves, commit.HashLeaf(content[off:min(off+SegmentSize, len(content))]))
	}
	return t
}

// Root is the tree's proof root.
func (t *ProofTree) Root() [32]byte { return commit.RootOfLeafHashes(t.Leaves) }

// Path is segment i's audit path, leaf to root, siblings only.
func (t *ProofTree) Path(i uint64) [][32]byte {
	p, err := commit.ProveLeafHashes(t.Leaves, int(i))
	if err != nil {
		return nil
	}
	return p.Siblings()
}

// Answer builds the response body for a request from the content: inline
// segments when inline, else every segment by reference (null).
func Answer(req *Request, requestRaw, content []byte, t *ProofTree, inline bool) *Body {
	body := &Body{Request: sha256.Sum256(requestRaw), Answerer: req.Answerer}
	for k := uint64(0); k < req.C; k++ {
		i := req.Index(uint32(k))
		s := Sample{Siblings: t.Path(i)}
		if inline {
			off := i * SegmentSize
			s.Segment = append([]byte{}, content[off:off+SegmentLen(i, req.Size)]...)
		}
		body.Samples = append(body.Samples, s)
	}
	return body
}
