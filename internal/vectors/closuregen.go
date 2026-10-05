package vectors

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/lightwebinc/bcommon/chirp"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// closureCase is one closure a provider or reader verifies: its nodes, its
// blobs (by the content rule of proof-v1.json, or given), what a fetch
// serves in place of an object, and the entry's fields it is checked
// against.
type closureCase struct {
	name        string
	nodes       [][]byte
	blobs       [][]byte
	ruleSize    uint64 // the content rule's content, when blobs is nil
	serve       map[[32]byte][]byte
	missing     map[[32]byte]bool
	root        [32]byte
	size        uint64
	contentHash [32]byte
	proofRoot   [32]byte
	proof       bool
	chirpOnly   bool // VerifyCHIRP on the root alone, no entry
}

func (c *closureCase) fetch() chirp.Fetch {
	objs := map[[32]byte][]byte{}
	for _, n := range c.nodes {
		objs[sha256.Sum256(n)] = n
	}
	blobs := c.blobs
	if blobs == nil {
		content := contentOf(c.ruleSize)
		for off := uint64(0); off < c.ruleSize; off += storerec.BlobSize {
			blobs = append(blobs, content[off:min(off+storerec.BlobSize, c.ruleSize)])
		}
	}
	for _, b := range blobs {
		objs[sha256.Sum256(b)] = b
	}
	return func(h [32]byte) ([]byte, error) {
		if c.missing[h] {
			return nil, errors.New("not served")
		}
		if o, ok := c.serve[h]; ok {
			return o, nil
		}
		if o, ok := objs[h]; ok {
			return o, nil
		}
		return nil, errors.New("not served")
	}
}

func fromBuilt(name string, b *storerec.Built, ruleSize uint64, blobs [][]byte) *closureCase {
	nodes := append([][]byte{b.Root}, b.Branches...)
	return &closureCase{name: name, nodes: nodes, blobs: blobs, ruleSize: ruleSize, root: b.RootHash, size: b.Size,
		contentHash: b.ContentHash, proofRoot: b.ProofRoot, proof: true}
}

func blobChild(b []byte) chirp.Child {
	return chirp.Child{Kind: chirp.ChildBlob, Length: uint64(len(b)), Hash: sha256.Sum256(b)}
}

func rootOver(children []chirp.Child, contentHash [32]byte, profile uint16, ext []chirp.Extension) []byte {
	var l uint64
	for _, c := range children {
		l += c.Length
	}
	return must(chirp.EncodeRoot(chirp.Root{Profile: profile, Length: l, ContentHash: contentHash, Children: children, Extensions: ext}))
}

func genClosure(files map[string][]byte) error {
	small := contentBuilt
	two := must(storerec.Build(contentOf(storerec.BlobSize + 1)))
	empty := must(storerec.Build(nil))
	var cases []*closureCase
	add := func(c *closureCase) { cases = append(cases, c) }

	add(fromBuilt("the sample object", small, 0, [][]byte{content}))
	add(fromBuilt("the empty object", empty, 0, [][]byte{}))
	add(fromBuilt("one blob and one byte: two blobs", two, storerec.BlobSize+1, nil))
	c := fromBuilt("the entry's proof root is not the content's", small, 0, [][]byte{content})
	c.proofRoot[0] ^= 1
	add(c)
	c = fromBuilt("the root not served", small, 0, [][]byte{content})
	c.missing = map[[32]byte]bool{small.RootHash: true}
	add(c)
	c = fromBuilt("the root served as other bytes", small, 0, [][]byte{content})
	c.serve = map[[32]byte][]byte{small.RootHash: append(append([]byte{}, small.Root...), 0)}
	add(c)
	c = fromBuilt("a blob not served", two, storerec.BlobSize+1, nil)
	c.missing = map[[32]byte]bool{sha256.Sum256(contentOf(1)): false, sha256.Sum256(contentOf(storerec.BlobSize + 1)[storerec.BlobSize:]): true}
	add(c)
	c = fromBuilt("a blob served as other bytes of its length", small, 0, [][]byte{content})
	other := append([]byte{}, content...)
	other[0] ^= 1
	c.serve = map[[32]byte][]byte{sha256.Sum256(content): other}
	add(c)
	c = fromBuilt("a blob served as other bytes of another length", small, 0, [][]byte{content})
	c.serve = map[[32]byte][]byte{sha256.Sum256(content): content[:5]}
	add(c)

	// The root's own rules: no extension, version 1.0, profile 1, the
	// entry's length and content hash.
	ch := sha256.Sum256(content)
	for _, x := range []struct {
		name string
		ext  []chirp.Extension
	}{
		{"a root with mediaType", []chirp.Extension{{Type: chirp.MediaType, Value: []byte("text/plain")}}},
		{"a root with an unknown advisory extension", []chirp.Extension{{Type: 3, Value: []byte{1}}}},
	} {
		r := rootOver([]chirp.Child{blobChild(content)}, ch, 1, x.ext)
		add(&closureCase{name: x.name, nodes: [][]byte{r}, blobs: [][]byte{content}, root: sha256.Sum256(r), size: uint64(len(content)),
			contentHash: ch, proofRoot: small.ProofRoot, proof: true})
	}
	{
		r := append([]byte{}, small.Root...)
		r[6] = 1 // minor version 1
		add(&closureCase{name: "a root of version 1.1 (a later minor version is refused)", nodes: [][]byte{r}, blobs: [][]byte{content},
			root: sha256.Sum256(r), size: small.Size, contentHash: ch, proofRoot: small.ProofRoot, proof: true})
		r2 := rootOver([]chirp.Child{blobChild(content)}, ch, 2, nil)
		add(&closureCase{name: "a root of chunking profile 2", nodes: [][]byte{r2}, blobs: [][]byte{content},
			root: sha256.Sum256(r2), size: small.Size, contentHash: ch, proofRoot: small.ProofRoot, proof: true})
		r3 := append([]byte{}, small.Root...)
		r3 = append(r3[:len(r3)-1], 1, 2, 1, 0) // one extension of type 2 (critical)
		add(&closureCase{name: "a root with an unknown critical extension", nodes: [][]byte{r3}, blobs: [][]byte{content},
			root: sha256.Sum256(r3), size: small.Size, contentHash: ch, proofRoot: small.ProofRoot, proof: true})
	}
	c = fromBuilt("the entry's size is not the root's length", small, 0, [][]byte{content})
	c.size++
	add(c)
	c = fromBuilt("the entry's content hash is not the root's", small, 0, [][]byte{content})
	c.contentHash[0] ^= 1
	add(c)
	{
		wrong := sha256.Sum256([]byte("other content"))
		r := rootOver([]chirp.Child{blobChild(content)}, wrong, 1, nil)
		add(&closureCase{name: "the root's content hash (and the entry's) not the content's", nodes: [][]byte{r}, blobs: [][]byte{content},
			root: sha256.Sum256(r), size: small.Size, contentHash: wrong, proofRoot: small.ProofRoot, proof: true})
	}
	// Construction and the walk.
	{
		a, bb := content[:7], content[7:]
		r := rootOver([]chirp.Child{blobChild(a), blobChild(bb)}, ch, 1, nil)
		add(&closureCase{name: "two short blobs: not profile 1's construction", nodes: [][]byte{r}, blobs: [][]byte{a, bb},
			root: sha256.Sum256(r), size: small.Size, contentHash: ch, proofRoot: small.ProofRoot, proof: true})
	}
	chain := func(depth int, branchLen uint64) ([][]byte, []byte) {
		child := blobChild(content)
		var nodes [][]byte
		for i := 0; i < depth; i++ {
			br := must(chirp.EncodeBranch(chirp.Branch{Length: child.Length, Children: []chirp.Child{child}}))
			nodes = append(nodes, br)
			child = chirp.Child{Kind: chirp.ChildBranch, Length: branchLen, Hash: sha256.Sum256(br)}
			branchLen = child.Length
		}
		r := rootOver([]chirp.Child{child}, ch, 1, nil)
		return append(nodes, r), r
	}
	for _, d := range []struct {
		name  string
		depth int
	}{
		{"fourteen branches deep: sixteen objects root to blob, the depth holds, the construction does not", 14},
		{"fifteen branches deep: seventeen objects root to blob, past the depth", 15},
	} {
		nodes, r := chain(d.depth, uint64(len(content)))
		add(&closureCase{name: d.name, nodes: nodes, blobs: [][]byte{content}, root: sha256.Sum256(r), size: small.Size,
			contentHash: ch, proofRoot: small.ProofRoot, proof: true})
	}
	{
		// A root whose blob reference claims one byte more than the blob.
		long := chirp.Child{Kind: chirp.ChildBlob, Length: uint64(len(content)) + 1, Hash: sha256.Sum256(content)}
		r := rootOver([]chirp.Child{long}, ch, 1, nil)
		add(&closureCase{name: "a blob reference whose length is not the blob's", nodes: [][]byte{r}, blobs: [][]byte{content},
			root: sha256.Sum256(r), size: small.Size + 1, contentHash: ch, proofRoot: small.ProofRoot, proof: true})
	}
	// The size bound, on the root alone.
	for _, n := range []uint64{storerec.MaxSize, storerec.MaxSize + 1} {
		ref := chirp.Child{Kind: chirp.ChildBlob, Length: n, Hash: label("a blob never served")}
		r := rootOver([]chirp.Child{ref}, ch, 1, nil)
		add(&closureCase{name: fmt.Sprintf("a root of %d bytes, verified alone with the bound 2^43", n), nodes: [][]byte{r}, blobs: [][]byte{},
			root: sha256.Sum256(r), chirpOnly: true})
	}

	var out []map[string]any
	for _, c := range cases {
		var err error
		if c.chirpOnly {
			_, err = storerec.VerifyCHIRP(c.root, c.fetch())
		} else {
			e := &storerec.Entry{Kind: storerec.KindPut, Root: c.root, Size: c.size, ContentHash: c.contentHash, ProofRoot: c.proofRoot}
			_, err = storerec.VerifyClosure(e, c.fetch(), c.proof)
		}
		var nodes []string
		for _, n := range c.nodes {
			nodes = append(nodes, hx(n))
		}
		var blobs []string
		if c.ruleSize == 0 {
			for _, b := range c.blobs {
				blobs = append(blobs, hx(b))
			}
		}
		serve := map[string]string{}
		for h, o := range c.serve {
			serve[h32(h)] = hx(o)
		}
		var missing []string
		for h, m := range c.missing {
			if m {
				missing = append(missing, h32(h))
			}
		}
		out = append(out, map[string]any{"name": c.name, "nodes": nodes, "blobs": blobs, "ruleSize": c.ruleSize, "serve": serve,
			"missing": missing, "root": h32(c.root), "size": c.size, "contentHash": h32(c.contentHash), "proofRoot": h32(c.proofRoot),
			"proof": c.proof, "chirpOnly": c.chirpOnly, "label": reason(err), "rule": storerec.Rule(err)})
	}
	return put(files, "closure-v1.json", map[string]any{
		"note":  "bstore's checks of a CHIRP Profile 1 closure (BRC-167's own vectors are bcommon's chirp-v1.json at the tag go.mod pins): the root's rules (no extension, version 1.0, profile 1, the entry's length and content hash), bcommon's Verify with a 2^43-byte bound and its refusals as labels, a fetch that takes only bytes hashing to the object asked for, and the proof root; blobs given, or by proof-v1.json's content rule (ruleSize)",
		"cases": out,
	})
}
