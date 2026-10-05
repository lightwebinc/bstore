package storerec_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Every frozen bound of docs/frozen.md section 12, at the bound and one
// over. Each is also pinned case by case in refusal-v1.json (and the BEEF
// ceilings in transaction-v1.json); these build each pair directly so that a
// bound changed in the codec fails here by name.

func key(i int) []byte {
	h := sha256.Sum256([]byte{byte(i), byte(i >> 8), 0x42})
	k, _ := ec.PrivateKeyFromBytes(h[:])
	return k.PubKey().Compressed()
}

func keys(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = key(i)
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && bytes.Compare(out[j-1], out[j]) > 0; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func pair(t *testing.T, what string, at, over error, want string) {
	t.Helper()
	if at != nil {
		t.Errorf("%s at its bound: %v", what, at)
	}
	if got := storerec.Reason(over); got != want {
		t.Errorf("%s one over: %v (%s), want %s", what, over, got, want)
	}
}

const bucket = "photos_qzvmkwerty"

func bucketRec(f func(b *storerec.Bucket)) error {
	b := &storerec.Bucket{Bucket: bucket, Owner: key(1000), Seq: 1, Prev: [32]byte{1}, Writers: [][]byte{}, Changes: []storerec.Change{},
		Count: 0, MapRoot: storerec.EmptyHash, Created: 1, Consumed: [][32]byte{}}
	f(b)
	raw, err := b.Encode()
	if err != nil {
		return err
	}
	_, err = storerec.DecodeBucket(raw)
	return err
}

func TestBucketRecordBounds(t *testing.T) {
	ws := keys(17)
	pair(t, "writers", bucketRec(func(b *storerec.Bucket) { b.Writers = ws[:16] }), bucketRec(func(b *storerec.Bucket) { b.Writers = ws }), "writers")
	changes := func(n int) []storerec.Change {
		out := make([]storerec.Change, n)
		for i := range out {
			out[i] = storerec.Change{Key: fmt.Sprintf("k%05d", i), C: sha256.Sum256([]byte{byte(i), byte(i >> 8)})}
		}
		return out
	}
	pair(t, "changes", bucketRec(func(b *storerec.Bucket) { b.Changes = changes(1024) }), bucketRec(func(b *storerec.Bucket) { b.Changes = changes(1025) }), "changes")
	consumed := func(n int) [][32]byte {
		out := make([][32]byte, n)
		for i := range out {
			out[i][0], out[i][30], out[i][31] = 0x80, byte(i>>8), byte(i)
		}
		return out
	}
	pair(t, "consumed", bucketRec(func(b *storerec.Bucket) { b.Consumed = consumed(1024) }), bucketRec(func(b *storerec.Bucket) { b.Consumed = consumed(1025) }), "consumed")
	pair(t, "count", bucketRec(func(b *storerec.Bucket) { b.Count = storerec.MaxKeysInMap }), bucketRec(func(b *storerec.Bucket) { b.Count = storerec.MaxKeysInMap + 1 }), "count")
	pair(t, "seq", bucketRec(func(b *storerec.Bucket) { b.Seq = storerec.MaxSafe }), bucketRec(func(b *storerec.Bucket) { b.Seq = storerec.MaxSafe + 1 }), "seq")
	pair(t, "created", bucketRec(func(b *storerec.Bucket) { b.Created = storerec.MaxTime }), bucketRec(func(b *storerec.Bucket) { b.Created = storerec.MaxTime + 1 }), "created")
	// The encoded bound governs: 1,024 changes of long keys do not fit.
	long := changes(1024)
	for i := range long {
		long[i].Key = fmt.Sprintf("%s%05d", strings.Repeat("k", 60), i)
	}
	if got := storerec.Reason(bucketRec(func(b *storerec.Bucket) { b.Changes = long })); got != "too-large" {
		t.Errorf("1,024 changes of 65-byte keys: %s, want too-large", got)
	}
	pad := func(n int) error {
		return bucketRec(func(b *storerec.Bucket) { b.Extra = cbor.Map{{Key: uint64(12), Val: make([]byte, n)}} })
	}
	// Find the padding at the bound, then one byte over.
	var at int
	for n := 65000; n < 65536; n++ {
		b := &storerec.Bucket{Bucket: bucket, Owner: key(1000), Seq: 1, Prev: [32]byte{1}, Writers: [][]byte{}, Changes: []storerec.Change{},
			MapRoot: storerec.EmptyHash, Created: 1, Consumed: [][32]byte{}, Extra: cbor.Map{{Key: uint64(12), Val: make([]byte, n)}}}
		if raw, err := b.Encode(); err == nil && len(raw) == storerec.MaxBucketRecord {
			at = n
			break
		}
	}
	if at == 0 {
		t.Fatal("no padding reaches 65,536 bytes")
	}
	pair(t, "bucket record bytes", pad(at), pad(at+1), "too-large")
}

func putEntry(f func(e *storerec.Entry)) error {
	content := []byte("hello")
	b, _ := storerec.Build(content)
	e := &storerec.Entry{Bucket: bucket, ID: [32]byte{1}, Writer: key(1), Key: "k", Kind: storerec.KindPut, Created: 1, Size: b.Size,
		Root: b.RootHash, ContentHash: b.ContentHash, ProofRoot: b.ProofRoot, ProofProfile: 1, ContentType: "a/b"}
	f(e)
	raw, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = storerec.DecodeEntry(raw)
	return err
}

func TestEntryBounds(t *testing.T) {
	pair(t, "key", putEntry(func(e *storerec.Entry) { e.Key = strings.Repeat("k", 1024) }), putEntry(func(e *storerec.Entry) { e.Key = strings.Repeat("k", 1025) }), "objkey")
	pair(t, "size", putEntry(func(e *storerec.Entry) { e.Size = storerec.MaxSize }), putEntry(func(e *storerec.Entry) { e.Size = storerec.MaxSize + 1 }), "size")
	pair(t, "contentType", putEntry(func(e *storerec.Entry) { e.ContentType = "a/" + strings.Repeat("b", 125) }),
		putEntry(func(e *storerec.Entry) { e.ContentType = "a/" + strings.Repeat("b", 126) }), "content-type")
	pair(t, "contentType short", putEntry(func(e *storerec.Entry) { e.ContentType = "a/b" }), putEntry(func(e *storerec.Entry) { e.ContentType = "ab" }), "content-type")
	et := func(n int) *string { s := strings.Repeat("e", n); return &s }
	pair(t, "etag", putEntry(func(e *storerec.Entry) { e.ETag = et(64) }), putEntry(func(e *storerec.Entry) { e.ETag = et(65) }), "etag")
	meta := func(n int) []storerec.MetaPair {
		out := make([]storerec.MetaPair, n)
		for i := range out {
			out[i] = storerec.MetaPair{Key: fmt.Sprintf("k%02d", i), Value: "v"}
		}
		return out
	}
	pair(t, "meta pairs", putEntry(func(e *storerec.Entry) { e.Meta = meta(32) }), putEntry(func(e *storerec.Entry) { e.Meta = meta(33) }), "meta")
	pair(t, "meta key", putEntry(func(e *storerec.Entry) { e.Meta = []storerec.MetaPair{{Key: strings.Repeat("a", 128)}} }),
		putEntry(func(e *storerec.Entry) { e.Meta = []storerec.MetaPair{{Key: strings.Repeat("a", 129)}} }), "meta")
	pair(t, "meta value", putEntry(func(e *storerec.Entry) { e.Meta = []storerec.MetaPair{{Key: "a", Value: strings.Repeat("v", 1024)}} }),
		putEntry(func(e *storerec.Entry) { e.Meta = []storerec.MetaPair{{Key: "a", Value: strings.Repeat("v", 1025)}} }), "meta")
	metaBytes := func(n int) []storerec.MetaPair {
		for l := 0; l < 1024; l++ {
			m := []storerec.MetaPair{{Key: "a", Value: strings.Repeat("v", 1000)}, {Key: "b", Value: strings.Repeat("v", 1000)}, {Key: "c", Value: strings.Repeat("w", l)}}
			cm := cbor.Map{}
			for _, p := range m {
				cm = append(cm, cbor.Pair{Key: p.Key, Val: p.Value})
			}
			if b, _ := cbor.Encode(cm); len(b) == n {
				return m
			}
		}
		panic("meta size")
	}
	pair(t, "meta bytes", putEntry(func(e *storerec.Entry) { e.Meta = metaBytes(2048) }), putEntry(func(e *storerec.Entry) { e.Meta = metaBytes(2049) }), "meta")
	hold := func(f func(h *storerec.Hold)) func(e *storerec.Entry) {
		return func(e *storerec.Entry) {
			h := &storerec.Hold{Vout: 0, Satoshis: 1, T: 1, K: 1, Retention: 1, Deadline: 1, CheckC: 1}
			f(h)
			e.Hold = h
		}
	}
	pair(t, "t and K", putEntry(hold(func(h *storerec.Hold) { h.T, h.K = 16, 16 })), putEntry(hold(func(h *storerec.Hold) { h.T, h.K = 17, 17 })), "hold")
	pair(t, "checkC", putEntry(hold(func(h *storerec.Hold) { h.CheckC = 1024 })), putEntry(hold(func(h *storerec.Hold) { h.CheckC = 1025 })), "hold")
	pair(t, "satoshis", putEntry(hold(func(h *storerec.Hold) { h.Satoshis = storerec.MaxSatoshis })), putEntry(hold(func(h *storerec.Hold) { h.Satoshis = storerec.MaxSatoshis + 1 })), "hold")
	pair(t, "vout", putEntry(hold(func(h *storerec.Hold) { h.Vout = storerec.MaxU32 })), putEntry(hold(func(h *storerec.Hold) { h.Vout = storerec.MaxU32 + 1 })), "hold")
	big := func(n int) error {
		return putEntry(func(e *storerec.Entry) { e.Extra = cbor.Map{{Key: uint64(16), Val: make([]byte, n)}} })
	}
	var at int
	for n := 7900; n < 8192; n++ {
		if putEntry(func(e *storerec.Entry) { e.Extra = cbor.Map{{Key: uint64(16), Val: make([]byte, n)}} }) == nil {
			at = n
		}
	}
	pair(t, "entry bytes", big(at), big(at+1), "too-large")
}

func TestPieceOutcomeFrontRequestBounds(t *testing.T) {
	piece := func(n int) error {
		p := &storerec.Piece{Bucket: bucket, Writer: key(1), Piece: make([]byte, n)}
		raw, err := p.Encode()
		if err != nil {
			return err
		}
		_, err = storerec.DecodePiece(raw)
		return err
	}
	pair(t, "piece", piece(storerec.MaxPiece), piece(storerec.MaxPiece+1), "piece")
	front := func(url string, extra int) error {
		f := &storerec.Front{Bucket: bucket, Front: key(1), Advertiser: key(2), URL: url, Created: 1}
		if extra > 0 {
			f.Extra = cbor.Map{{Key: uint64(6), Val: make([]byte, extra)}}
		}
		raw, err := f.Encode()
		if err != nil {
			return err
		}
		_, err = storerec.DecodeFront(raw)
		return err
	}
	pair(t, "front url", front("https://"+strings.Repeat("a", 504), 0), front("https://"+strings.Repeat("a", 505), 0), "url")
	pair(t, "front url short", front("https://a.ex", 0), front("https://a.e", 0), "url")
	var at int
	for n := 1500; n < 2048; n++ {
		if front("https://a.example", n) == nil {
			at = n
		}
	}
	pair(t, "front bytes", front("https://a.example", at), front("https://a.example", at+1), "too-large")
	req := func(c uint64, size uint64) error {
		q := &storerec.Request{Root: [32]byte{1}, ProofRoot: [32]byte{2}, Size: size, Provider: key(1), Answerer: key(2), C: c}
		_, err := q.Encode()
		return err
	}
	pair(t, "c", req(1024, 1), req(1025, 1), "request")
	pair(t, "request size", req(1, storerec.MaxSize), req(1, storerec.MaxSize+1), "request")
	if storerec.Rule(req(1025, 1)) != "c" {
		t.Errorf("c 1025: rule %q", storerec.Rule(req(1025, 1)))
	}
	if _, _, ok := storerec.SplitSigned(make([]byte, storerec.MaxSignedResponse+1)); ok {
		t.Error("a signed response over 8,388,608 bytes splits")
	}
	at8 := func(n int) []byte {
		for l := n - 16; l < n; l++ {
			b, _ := cbor.Encode([]cbor.Value{make([]byte, l), make([]byte, 8)})
			if len(b) == n {
				return b
			}
		}
		panic("8 MiB")
	}
	if _, _, ok := storerec.SplitSigned(at8(storerec.MaxSignedResponse)); !ok {
		t.Error("a signed response of exactly 8,388,608 bytes does not split")
	}
}
