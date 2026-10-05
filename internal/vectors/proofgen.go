package vectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// ContentRule is how every proof vector's content is made, so that both
// codecs regenerate it: segment s (4,096 bytes) is 1,024 words of xorshift32
// (x ^= x << 13; x ^= x >> 17; x ^= x << 5), little-endian, from the state
// x = (s * 0x9e3779b9 + 0x7f4a7c15) mod 2^32 (1 if that is 0), stepping
// before each word; the content is the segments in order, cut to its size.
const ContentRule = "segment s is 1,024 little-endian words of xorshift32 (x ^= x << 13; x ^= x >> 17; x ^= x << 5, 32-bit) stepped before each word from x = (s * 0x9e3779b9 + 0x7f4a7c15) mod 2^32 (1 if 0); the content is the segments in order, cut to its size"

func segmentBytes(s uint64, out []byte) {
	x := uint32(s)*0x9e3779b9 + 0x7f4a7c15
	if x == 0 {
		x = 1
	}
	for w := 0; w < 1024; w++ {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		binary.LittleEndian.PutUint32(out[4*w:], x)
	}
}

// contentOf is the first size bytes of the rule's content.
func contentOf(size uint64) []byte {
	out := make([]byte, ((size+4095)/4096)*4096)
	for s := uint64(0); s*4096 < size; s++ {
		segmentBytes(s, out[s*4096:])
	}
	return out[:size]
}

// proofRootOfRule is the proof root of the rule's content of size bytes,
// built per blob without holding the content.
func proofRootOfRule(size uint64) ([32]byte, [32]byte) {
	n := storerec.Segments(size)
	w, _ := commit.NewSegmentWriter(storerec.SegmentSize)
	var roots [][32]byte
	seg := make([]byte, 4096)
	var blob *commit.SegmentWriter
	for s := uint64(0); s < n; s++ {
		if s%storerec.SegmentsPerBlob == 0 {
			if blob != nil {
				roots = append(roots, blob.Root())
			}
			blob, _ = commit.NewSegmentWriter(storerec.SegmentSize)
		}
		segmentBytes(s, seg)
		l := storerec.SegmentLen(s, size)
		w.Write(seg[:l])
		blob.Write(seg[:l])
	}
	if blob != nil {
		roots = append(roots, blob.Root())
	}
	if len(roots) == 0 {
		return w.Root(), storerec.EmptyHash
	}
	perBlob, _ := commit.RootOfSubtrees(roots)
	return w.Root(), perBlob
}

// ruleFetcher serves the rule's content of one size, with faults.
type ruleFetcher struct {
	content    []byte
	rangeFault string // "", "error", "flip", "short"
	faultBlob  int64  // -1: every blob
	blobFault  string // "", "error", "flip"
}

func (f ruleFetcher) blob(b uint64) []byte {
	lo := b * storerec.BlobSize
	return f.content[lo:min(lo+storerec.BlobSize, uint64(len(f.content)))]
}

func (f ruleFetcher) faulty(b uint64) bool { return f.faultBlob < 0 || uint64(f.faultBlob) == b }

func (f ruleFetcher) Range(b, off, length uint64) ([]byte, error) {
	seg := append([]byte{}, f.blob(b)[off:off+length]...)
	if f.faulty(b) {
		switch f.rangeFault {
		case "error":
			return nil, errors.New("range refused")
		case "flip":
			seg[0] ^= 1
		case "short":
			seg = seg[:len(seg)-1]
		}
	}
	return seg, nil
}

func (f ruleFetcher) Blob(b uint64) ([]byte, error) {
	bl := append([]byte{}, f.blob(b)...)
	if f.faulty(b) {
		switch f.blobFault {
		case "error":
			return nil, errors.New("blob refused")
		case "flip":
			bl[len(bl)/2] ^= 1
		}
	}
	return bl, nil
}

func genProof(files map[string][]byte) error {
	// Proof roots by size; each also built per blob, which must agree.
	var roots []map[string]any
	for _, sz := range []struct {
		name string
		size uint64
	}{
		{"0 bytes", 0}, {"1 byte", 1}, {"4,095 bytes", 4095}, {"4,096 bytes", 4096}, {"4,097 bytes", 4097},
		{"one blob", storerec.BlobSize}, {"one blob and one byte", storerec.BlobSize + 1},
		{"25 blobs", 25 * storerec.BlobSize}, {"257 blobs", 257 * storerec.BlobSize},
	} {
		whole, perBlob := proofRootOfRule(sz.size)
		if whole != perBlob {
			return fmt.Errorf("proof root %s: per blob %x, whole %x", sz.name, perBlob, whole)
		}
		if sz.size <= storerec.BlobSize+1 && storerec.ProofRoot(contentOf(sz.size)) != whole {
			return fmt.Errorf("proof root %s: SegmentRoot disagrees", sz.name)
		}
		roots = append(roots, map[string]any{"name": sz.name, "size": sz.size, "segments": storerec.Segments(sz.size), "proofRoot": h32(whole)})
	}
	// The two-byte object and the 4 KiB segment worked by hand.
	two := []byte{0x00, 0x01}
	leaf := commit.HashLeaf(two)
	hand := map[string]any{"content": hx(two), "leaf": h32(leaf), "proofRoot": h32(storerec.ProofRoot(two)),
		"note": "one segment: the proof root is SHA-256(0x00 || segment)"}

	// Path lengths for (index, segments), and audit paths of a ten-segment
	// object.
	var lens []map[string]any
	for _, c := range [][2]uint64{{0, 1}, {0, 2}, {1, 2}, {0, 3}, {2, 3}, {4, 5}, {0, 25600}, {25599, 25600}, {12345, 25600},
		{0, 262144}, {268435455, 268435456}, {0, 1 << 31}, {1<<31 - 1, 1 << 31}, {1 << 30, 1<<30 + 1}} {
		l, _ := commit.PathLen(c[0], c[1])
		lens = append(lens, map[string]any{"index": c[0], "segments": c[1], "length": l})
	}
	ten := contentOf(10*4096 - 100)
	tt := storerec.NewProofTree(ten)
	var paths []map[string]any
	for i := uint64(0); i < 10; i++ {
		var sib []string
		for _, h := range tt.Path(i) {
			sib = append(sib, h32(h))
		}
		paths = append(paths, map[string]any{"index": i, "siblings": sib})
	}

	// Index derivations.
	var idx []map[string]any
	for _, q := range indexRequests() {
		raw := must(q.Encode())
		var ds []string
		var is []uint64
		for k := uint32(0); k < uint32(q.C); k++ {
			d := q.IndexDigest(k)
			ds = append(ds, h32(d))
			is = append(is, q.Index(k))
		}
		m, off := q.Domain()
		idx = append(idx, map[string]any{"request": hx(raw), "domain": m, "offset": off, "digests": ds, "indices": is})
	}

	// Responses through the buyer's check, sample-major.
	responses, err := responseVectors()
	if err != nil {
		return err
	}
	answers, err := answerVectors()
	if err != nil {
		return err
	}

	a, s := storerec.DetectionTables()
	var detect []map[string]any
	for _, c := range storerec.DetectionCs {
		for _, f := range storerec.DetectionLosses {
			detect = append(detect, map[string]any{"c": c, "f": f, "p": storerec.Detect(c, f)})
		}
	}
	var needed []map[string]any
	for _, f := range storerec.DetectionLosses {
		for _, conf := range storerec.ConfidenceLevels {
			needed = append(needed, map[string]any{"f": f, "confidence": conf, "c": storerec.SamplesFor(f, conf)})
		}
	}
	return put(files, "proof-v1.json", map[string]any{
		"note":             "proof roots by size (the content rule below; each built whole and per blob); path lengths and audit paths; index derivations, the digest read little-endian; responses through the buyer's check in sample-major order, with a fetcher's faults; an answerer's refusals; the detection tables",
		"contentRule":      ContentRule,
		"roots":            roots,
		"hand":             hand,
		"pathLengths":      lens,
		"tenSegments":      map[string]any{"size": len(ten), "proofRoot": h32(tt.Root()), "paths": paths},
		"indices":          idx,
		"responses":        responses,
		"answers":          answers,
		"detection":        detect,
		"samplesNeeded":    needed,
		"detectionTable":   a,
		"samplesNeededTbl": s,
	})
}

func indexRequests() []*storerec.Request {
	base := sampleRequest()
	var out []*storerec.Request
	add := func(size uint64, c uint64, rng *[2]uint64, provider []byte) {
		q := *base
		q.Size, q.C, q.Range, q.Provider = size, c, rng, provider
		q.Nonce = label16("nonce %d %d", size, c)
		out = append(out, &q)
	}
	add(1, 4, nil, Bob.Pub())
	add(3*4096, 8, nil, Bob.Pub())
	add(3*4096, 8, nil, Carol.Pub())
	add(100*1048576, 30, nil, Bob.Pub())
	add(storerec.MaxSize, 16, nil, Bob.Pub())
	add(storerec.MaxSize, 16, &[2]uint64{2047, 2047}, Bob.Pub())
	add(3*storerec.BlobSize+5, 12, &[2]uint64{3, 3}, Bob.Pub())
	add(3*storerec.BlobSize+5, 12, &[2]uint64{1, 2}, Bob.Pub())
	q := *base
	q.SeedHash = [32]byte{}
	q.C = 2
	out = append(out, &q)
	return out
}

// The response scenarios run against the rule's content of one and a half
// blobs and 100 bytes (two blobs, the last segment 100 bytes), whose blob
// hashes are its CHIRP blob references.
const respSize = storerec.BlobSize + storerec.BlobSize/2 + 100

func responseVectors() ([]map[string]any, error) {
	content := contentOf(respSize)
	tree := storerec.NewProofTree(content)
	blobHash := func(b uint64) [32]byte {
		lo := b * storerec.BlobSize
		return sha256.Sum256(content[lo:min(lo+storerec.BlobSize, uint64(len(content)))])
	}
	mkReq := func(answerer []byte, c uint64, nonce string) (*storerec.Request, []byte) {
		q := &storerec.Request{Root: label("resp root"), ProofRoot: tree.Root(), Size: respSize, Provider: Bob.Pub(), Answerer: answerer,
			SeedHeight: 900000, SeedHash: label("block 900000"), Nonce: label16("%s", nonce), C: c}
		return q, must(q.Encode())
	}
	sign := func(p Party, reqRaw []byte, body *storerec.Body) []byte {
		return must(storerec.SignResponse(challengePriv(p), reqRaw, must(body.Encode())))
	}
	type scen struct {
		name     string
		answerer Party
		c        uint64
		nonce    string
		inline   bool
		edit     func(q *storerec.Request, reqRaw []byte, b *storerec.Body) []byte // returns the signed response; nil: sign as is
		fetch    ruleFetcher
		wantLast bool
	}
	honest := ruleFetcher{faultBlob: -1}
	byFault := func(rf string, fb int64, bf string) ruleFetcher {
		return ruleFetcher{rangeFault: rf, faultBlob: fb, blobFault: bf}
	}
	flipSib := func(k int) func(q *storerec.Request, reqRaw []byte, b *storerec.Body) []byte {
		return func(q *storerec.Request, reqRaw []byte, b *storerec.Body) []byte {
			b.Samples[k].Siblings[0][0] ^= 1
			return nil
		}
	}
	scens := []scen{
		{name: "pass, inline, from the provider", answerer: Bob, c: 6, nonce: "a", inline: true, fetch: honest},
		{name: "pass, by reference, from a front", answerer: Carol, c: 6, nonce: "b", fetch: honest},
		{name: "pass, by reference, from the provider", answerer: Bob, c: 4, nonce: "c", fetch: honest},
		{name: "signature: not [body, signature]", answerer: Bob, c: 3, nonce: "d", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				return enc([]cbor.Value{must(b.Encode())})
			}},
		{name: "signature: signed by another key", answerer: Bob, c: 3, nonce: "e", inline: true,
			edit: func(_ *storerec.Request, reqRaw []byte, b *storerec.Body) []byte { return sign(Carol, reqRaw, b) }},
		{name: "signature: high S", answerer: Bob, c: 3, nonce: "f", inline: true,
			edit: func(_ *storerec.Request, reqRaw []byte, b *storerec.Body) []byte {
				body, sig, _ := storerec.SplitSigned(sign(Bob, reqRaw, b))
				return enc([]cbor.Value{body, highS(sig)})
			}},
		{name: "signature: over another request", answerer: Bob, c: 3, nonce: "g", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				return must(storerec.SignResponse(challengePriv(Bob), []byte("another request"), must(b.Encode())))
			}},
		{name: "signature: over 8 MiB", answerer: Bob, c: 3, nonce: "g2", inline: true,
			edit: func(_ *storerec.Request, _ []byte, _ *storerec.Body) []byte {
				return make([]byte, storerec.MaxSignedResponse+1)
			}},
		{name: "request: the body is not CBOR", answerer: Bob, c: 3, nonce: "h", inline: true,
			edit: func(_ *storerec.Request, reqRaw []byte, _ *storerec.Body) []byte {
				return must(storerec.SignResponse(challengePriv(Bob), reqRaw, []byte{0xff}))
			}},
		{name: "request: the body names another request", answerer: Bob, c: 3, nonce: "i", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte { b.Request[0] ^= 1; return nil }},
		{name: "request: the body names another answerer", answerer: Bob, c: 3, nonce: "j", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte { b.Answerer = Carol.Pub(); return nil }},
		{name: "request: a front answers a sample inline", answerer: Carol, c: 3, nonce: "k",
			edit: func(q *storerec.Request, _ []byte, b *storerec.Body) []byte {
				i := q.Index(2)
				off := i * 4096
				b.Samples[2].Segment = append([]byte{}, content[off:off+storerec.SegmentLen(i, q.Size)]...)
				return nil
			}},
		{name: "request: the body has a key above 3", answerer: Bob, c: 3, nonce: "l", inline: true,
			edit: func(_ *storerec.Request, reqRaw []byte, b *storerec.Body) []byte {
				m := must(cbor.DecodeValue(must(b.Encode()))).(cbor.Map)
				return must(storerec.SignResponse(challengePriv(Bob), reqRaw, enc(with(m, uint64(4), uint64(0)))))
			}},
		{name: "request: a sample of three elements", answerer: Bob, c: 3, nonce: "l2", inline: true,
			edit: func(_ *storerec.Request, reqRaw []byte, b *storerec.Body) []byte {
				m := must(cbor.DecodeValue(must(b.Encode()))).(cbor.Map)
				s, _ := m.Get(storerec.RKSamples)
				ss := s.([]cbor.Value)
				ss[1] = append(ss[1].([]cbor.Value), nil)
				return must(storerec.SignResponse(challengePriv(Bob), reqRaw, enc(with(m, storerec.RKSamples, ss))))
			}},
		{name: "count: one sample too many", answerer: Bob, c: 3, nonce: "m", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				b.Samples = append(b.Samples, b.Samples[0])
				return nil
			}},
		{name: "count: one sample too few", answerer: Bob, c: 3, nonce: "n", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte { b.Samples = b.Samples[:2]; return nil }},
		{name: "count: a path one sibling short at sample 2, sample 0's segment also wrong (count first)", answerer: Bob, c: 3, nonce: "o", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				b.Samples[2].Siblings = b.Samples[2].Siblings[1:]
				b.Samples[0].Segment = b.Samples[0].Segment[1:]
				return nil
			}},
		{name: "count: a by-reference path one sibling long, the URL down (count first)", answerer: Carol, c: 3, nonce: "o2",
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				b.Samples[1].Siblings = append(b.Samples[1].Siblings, [32]byte{})
				return nil
			}, fetch: byFault("error", -1, "error")},
		{name: "segment: an inline segment one byte short at sample 1", answerer: Bob, c: 3, nonce: "p", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				b.Samples[1].Segment = b.Samples[1].Segment[:len(b.Samples[1].Segment)-1]
				return nil
			}},
		{name: "path: an inline segment altered at sample 1", answerer: Bob, c: 3, nonce: "q", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte { b.Samples[1].Segment[7] ^= 1; return nil }},
		{name: "path: sample 0's sibling altered and sample 1's segment short (sample-major: sample 0 first)", answerer: Bob, c: 3, nonce: "r", inline: true,
			edit: func(_ *storerec.Request, _ []byte, b *storerec.Body) []byte {
				b.Samples[0].Siblings[0][5] ^= 1
				b.Samples[1].Segment = b.Samples[1].Segment[1:]
				return nil
			}},
		{name: "by reference: Range refused, the blob verifies: the sample passes", answerer: Carol, c: 4, nonce: "s", fetch: byFault("error", -1, "")},
		{name: "by reference: Range serves wrong bytes, the blob verifies: the sample passes", answerer: Carol, c: 4, nonce: "t", fetch: byFault("flip", -1, "")},
		{name: "by reference: Range serves a short segment, the blob verifies: the sample passes", answerer: Bob, c: 4, nonce: "t2", fetch: byFault("short", -1, "")},
		{name: "by reference: Range refused and the blob refused: unretrievable", answerer: Carol, c: 4, nonce: "u", fetch: byFault("error", -1, "error")},
		{name: "by reference: Range wrong and the blob altered: unretrievable", answerer: Carol, c: 4, nonce: "v", fetch: byFault("flip", -1, "flip")},
		{name: "by reference: the front's path wrong, the blob verifies: invalid", answerer: Carol, c: 4, nonce: "w", edit: flipSib(1), fetch: honest},
		{name: "by reference: the front's path wrong and the blob refused: unretrievable", answerer: Carol, c: 4, nonce: "x", edit: flipSib(1), fetch: byFault("", -1, "error")},
		{name: "by reference: only blob 1 refused, the samples in blob 0 before it pass first", answerer: Carol, c: 12, nonce: "y", fetch: byFault("error", 1, "error")},
		{name: "pass, inline, a sample on the short last segment", answerer: Bob, c: 30, nonce: "last", inline: true, fetch: honest, wantLast: true},
		{name: "pass, by reference, a sample on the short last segment", answerer: Carol, c: 30, nonce: "lastref", fetch: honest, wantLast: true},
		{name: "segment: the short last segment padded to 4,096 bytes", answerer: Bob, c: 30, nonce: "lastpad", inline: true, wantLast: true,
			edit: func(q *storerec.Request, _ []byte, b *storerec.Body) []byte {
				for k := range b.Samples {
					if q.Index(uint32(k)) == storerec.Segments(q.Size)-1 {
						b.Samples[k].Segment = append(b.Samples[k].Segment, make([]byte, 4096-len(b.Samples[k].Segment))...)
					}
				}
				return nil
			}},
	}
	var out []map[string]any
	n := storerec.Segments(respSize)
	for _, s := range scens {
		q, reqRaw := mkReq(s.answerer.Pub(), s.c, s.nonce)
		if s.wantLast {
			for i := 0; ; i++ {
				q, reqRaw = mkReq(s.answerer.Pub(), s.c, fmt.Sprintf("%s%d", s.nonce, i))
				hit := false
				for k := uint32(0); k < uint32(q.C); k++ {
					hit = hit || q.Index(k) == n-1
				}
				if hit {
					break
				}
			}
		}
		body := storerec.Answer(q, reqRaw, content, tree, s.inline)
		var signed []byte
		if s.edit != nil {
			signed = s.edit(q, reqRaw, body)
		}
		if signed == nil {
			signed = sign(s.answerer, reqRaw, body)
		}
		f := s.fetch
		f.content = content
		if f.faultBlob == 0 && f.rangeFault == "" && f.blobFault == "" {
			f.faultBlob = -1
		}
		v := storerec.VerifyResponse(q, reqRaw, signed, blobHash, f)
		var idxs []uint64
		for k := uint32(0); k < uint32(q.C); k++ {
			idxs = append(idxs, q.Index(k))
		}
		rc := v.Rechecked
		if rc == nil {
			rc = []int{}
		}
		sig := hx(signed)
		var fl *fill
		if len(signed) > 1<<20 {
			// The oversized response is all zeros.
			sig, fl = "", &fill{At: 0, Byte: 0, Len: len(signed)}
		}
		out = append(out, map[string]any{"name": s.name, "request": hx(reqRaw), "signed": sig, "signedFill": fl, "indices": idxs,
			"fetch":   map[string]any{"rangeFault": f.rangeFault, "faultBlob": f.faultBlob, "blobFault": f.blobFault},
			"outcome": v.Outcome, "label": reason(v.Err), "rule": storerec.Rule(v.Err), "sample": v.Sample, "recheck": v.Recheck, "rechecked": rc})
	}
	return out, nil
}

// answerVectors are an answerer's refusals before it answers: request,
// seed (outside the window or not its block), unknown-root.
func answerVectors() ([]map[string]any, error) {
	q := sampleRequest()
	raw := must(q.Encode())
	headers := map[uint64][32]byte{}
	for h := uint64(899990); h <= 900010; h++ {
		headers[h] = label("block %d", h)
	}
	ownOK := func(root [32]byte) ([32]byte, uint64, bool) {
		if root == q.Root {
			return q.ProofRoot, q.Size, true
		}
		return [32]byte{}, 0, false
	}
	var out []map[string]any
	add := func(name string, raw []byte, tip uint64, own func([32]byte) ([32]byte, uint64, bool)) {
		_, err := storerec.AnswerCheck(raw, tip, func(h uint64) ([32]byte, bool) { x, ok := headers[h]; return x, ok }, own)
		hs := map[string]string{}
		for h, x := range headers {
			hs[fmt.Sprint(h)] = h32(x)
		}
		out = append(out, map[string]any{"name": name, "request": hx(raw), "tip": tip, "label": reason(err), "headers": hs})
	}
	add("the seed is the tip", raw, 900000, ownOK)
	add("the seed 6 below the tip", raw, 900006, ownOK)
	add("the seed 7 below the tip", raw, 900007, ownOK)
	add("the seed above the tip", raw, 899999, ownOK)
	other := *q
	other.SeedHash = label("not the block")
	add("the seed hash is not the block at the height", must(other.Encode()), 900000, ownOK)
	pr := *q
	pr.ProofRoot[0] ^= 1
	add("a proof root not the root's", must(pr.Encode()), 900000, ownOK)
	sz := *q
	sz.Size++
	add("a size not the root's", must(sz.Encode()), 900000, ownOK)
	add("a root not held", raw, 900000, func([32]byte) ([32]byte, uint64, bool) { return [32]byte{}, 0, false })
	add("not a request", []byte{0xa0}, 900000, ownOK)
	add("order: request before seed", bytes.Repeat([]byte{0}, 3), 1, ownOK)
	add("order: seed before unknown-root", must(other.Encode()), 900000, func([32]byte) ([32]byte, uint64, bool) { return [32]byte{}, 0, false })
	return out, nil
}
