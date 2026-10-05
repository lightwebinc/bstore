package vectors

import (
	"crypto/sha256"
	"math/big"
	"strconv"
	"strings"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func holdValue(h storerec.Hold) []cbor.Value {
	return []cbor.Value{h.Txid[:], h.Vout, h.Satoshis, h.T, h.K, h.Retention, h.Deadline, h.CheckC}
}

func entryRefusals() []refusalCase {
	p := toMap(samplePut())
	d := toMap(sampleDelete())
	u := toMap(sampleUnknownKind())
	e0 := toMap(sampleEmpty())
	l := &caseList{kind: "entry"}
	l.cases = recordSteps("entry", p, storerec.MaxEntry, storerec.MagicEntry, storerec.EKLast)
	l.add("exactly 8,192 bytes", padTo(p, 40, storerec.MaxEntry), "ok")
	l.bucketCases(p, storerec.EKBucket)
	l.b32Cases(p, storerec.EKID, "id")
	l.keyCases(p, storerec.EKWriter, "writer")
	l.m("key missing", without(p, storerec.EKKey), "missing")
	l.m("key as bytes", with(p, storerec.EKKey, []byte("k")), "type")
	objKeyCases(func(name string, k cbor.Value, label string) { l.m(name, with(p, storerec.EKKey, k), label) })
	// kind
	l.m("kind missing", without(p, storerec.EKKind), "missing")
	l.m("kind negative", with(p, storerec.EKKind, int64(-1)), "type")
	l.m("kind 0", with(d, storerec.EKKind, uint64(0)), "kind")
	l.m("kind 256", with(d, storerec.EKKind, uint64(256)), "kind")
	l.m("kind 255, keys 0 to 6 only", with(d, storerec.EKKind, uint64(255)), "ok")
	l.m("kind 3 with a bad key 7, unread", with(u, storerec.EKKind, uint64(3)), "ok")
	l.m("kind 7 with every key 7 to 15 of any type", with(with(u, storerec.EKSize, cbor.Map{}), storerec.EKHold, int64(-5)), "ok")
	l.m("kind 7 with a bad key 4 (keys 0 to 6 are checked)", with(u, storerec.EKKey, ""), "objkey")
	l.createdCases(p, storerec.EKCreated)
	// size
	l.m("size negative", with(p, storerec.EKSize, int64(-1)), "type")
	l.m("size 2^43 + 1", with(p, storerec.EKSize, uint64(storerec.MaxSize+1)), "size")
	l.m("size 2^43", with(p, storerec.EKSize, uint64(storerec.MaxSize)), "ok")
	for _, k := range []struct {
		key  uint64
		name string
	}{{storerec.EKRoot, "root"}, {storerec.EKContentHash, "contentHash"}, {storerec.EKProofRoot, "proofRoot"}} {
		l.m(k.name+" of 31 bytes", with(p, k.key, make([]byte, 31)), "type")
		l.m(k.name+" as text", with(p, k.key, strings.Repeat("a", 32)), "type")
	}
	l.m("proofProfile 2", with(p, storerec.EKProofProfile, uint64(2)), "profile")
	l.m("proofProfile 0", with(p, storerec.EKProofProfile, uint64(0)), "profile")
	l.m("proofProfile as text", with(p, storerec.EKProofProfile, "1"), "type")
	// contentType
	for _, ct := range []struct {
		v, label string
	}{
		{"a/b", "ok"}, {"ab", "content-type"}, {"/ab", "ok"}, {"ab/", "ok"}, {"a//b", "content-type"}, {"Image/jpeg", "content-type"},
		{"image/jpeg; charset=x", "content-type"}, {"image/jpeg;q=1", "content-type"}, {"image /jpeg", "content-type"},
		{"text/é", "content-type"}, {"a/" + strings.Repeat("b", 125), "ok"}, {"a/" + strings.Repeat("b", 126), "content-type"},
		{"application/vnd.example+json", "ok"}, {"a/b\x7f", "content-type"},
	} {
		l.m("contentType "+ct.v, with(p, storerec.EKContentType, ct.v), ct.label)
	}
	l.m("contentType as bytes", with(p, storerec.EKContentType, []byte("a/b")), "type")
	// etag
	for _, et := range []struct {
		v, label string
	}{
		{"", "etag"}, {strings.Repeat("e", 64), "ok"}, {strings.Repeat("e", 65), "etag"}, {"a\"b", "etag"},
		{"with space", "ok"}, {"tab\t", "etag"}, {"d41d8cd98f00b204e9800998ecf8427e-2", "ok"}, {"é", "etag"},
	} {
		l.m("etag "+et.v, with(p, storerec.EKETag, et.v), et.label)
	}
	l.m("etag as bytes", with(p, storerec.EKETag, []byte("x")), "type")
	// meta
	mp := func(n int, k func(i int) string, v func(i int) string) cbor.Map {
		m := cbor.Map{}
		for i := 0; i < n; i++ {
			m = with(m, k(i), v(i))
		}
		return m
	}
	kf := func(i int) string { return "k" + string(rune('a'+i/26)) + string(rune('a'+i%26)) }
	vf := func(int) string { return "v" }
	l.m("meta an array", with(p, storerec.EKMeta, []cbor.Value{}), "type")
	l.m("meta empty", with(p, storerec.EKMeta, cbor.Map{}), "meta")
	l.m("meta of 32 pairs", with(p, storerec.EKMeta, mp(32, kf, vf)), "ok")
	l.m("meta of 33 pairs", with(p, storerec.EKMeta, mp(33, kf, vf)), "meta")
	l.m("meta key uppercase", with(p, storerec.EKMeta, cbor.Map{{Key: "Album", Val: "x"}}), "meta")
	l.m("meta key empty", with(p, storerec.EKMeta, cbor.Map{{Key: "", Val: "x"}}), "meta")
	l.m("meta key underscore", with(p, storerec.EKMeta, cbor.Map{{Key: "a_b", Val: "x"}}), "meta")
	l.m("meta key of 128 bytes", with(p, storerec.EKMeta, cbor.Map{{Key: strings.Repeat("a", 128), Val: "x"}}), "ok")
	l.m("meta key of 129 bytes", with(p, storerec.EKMeta, cbor.Map{{Key: strings.Repeat("a", 129), Val: "x"}}), "meta")
	l.m("meta key an integer", with(p, storerec.EKMeta, cbor.Map{{Key: uint64(1), Val: "x"}}), "meta")
	l.m("meta value of 1024 bytes", with(p, storerec.EKMeta, cbor.Map{{Key: "a", Val: strings.Repeat("v", 1024)}}), "ok")
	l.m("meta value of 1025 bytes", with(p, storerec.EKMeta, cbor.Map{{Key: "a", Val: strings.Repeat("v", 1025)}}), "meta")
	l.m("meta value with a newline", with(p, storerec.EKMeta, cbor.Map{{Key: "a", Val: "x\ny"}}), "meta")
	l.m("meta value as bytes", with(p, storerec.EKMeta, cbor.Map{{Key: "a", Val: []byte("x")}}), "meta")
	big := func(n int) cbor.Map {
		// three pairs whose encoding is exactly n bytes
		for l := 0; l <= n; l++ {
			m := cbor.Map{{Key: "a", Val: strings.Repeat("v", 1000)}, {Key: "b", Val: strings.Repeat("v", 1000)}, {Key: "c", Val: strings.Repeat("w", l)}}
			if len(enc(m)) == n {
				return m
			}
		}
		panic("meta size")
	}
	l.m("meta of 2,048 bytes", with(p, storerec.EKMeta, big(2048)), "ok")
	l.m("meta of 2,049 bytes", with(p, storerec.EKMeta, big(2049)), "meta")
	// hold
	h := *samplePut().Hold
	hv := func(f func(h *storerec.Hold)) cbor.Map {
		x := h
		f(&x)
		return with(p, storerec.EKHold, holdValue(x))
	}
	l.m("hold a map", with(p, storerec.EKHold, cbor.Map{}), "type")
	l.m("hold of 7 elements", with(p, storerec.EKHold, holdValue(h)[:7]), "hold")
	l.m("hold of 9 elements", with(p, storerec.EKHold, append(holdValue(h), uint64(1))), "hold")
	l.m("hold txid of 31 bytes", with(p, storerec.EKHold, append([]cbor.Value{make([]byte, 31)}, holdValue(h)[1:]...)), "hold")
	l.m("hold vout 2^32 - 1", hv(func(h *storerec.Hold) { h.Vout = storerec.MaxU32 }), "ok")
	l.m("hold vout 2^32", hv(func(h *storerec.Hold) { h.Vout = storerec.MaxU32 + 1 }), "hold")
	l.m("hold satoshis 0", hv(func(h *storerec.Hold) { h.Satoshis = 0 }), "hold")
	l.m("hold satoshis 21e14", hv(func(h *storerec.Hold) { h.Satoshis = storerec.MaxSatoshis }), "ok")
	l.m("hold satoshis 21e14 + 1", hv(func(h *storerec.Hold) { h.Satoshis = storerec.MaxSatoshis + 1 }), "hold")
	l.m("hold t 0", hv(func(h *storerec.Hold) { h.T = 0 }), "hold")
	l.m("hold t 16, K 16", hv(func(h *storerec.Hold) { h.T, h.K = 16, 16 }), "ok")
	l.m("hold t 17", hv(func(h *storerec.Hold) { h.T, h.K = 17, 17 }), "hold")
	l.m("hold K 17", hv(func(h *storerec.Hold) { h.K = 17 }), "hold")
	l.m("hold K below t", hv(func(h *storerec.Hold) { h.T, h.K = 3, 2 }), "hold")
	l.m("hold K equal to t", hv(func(h *storerec.Hold) { h.T, h.K = 3, 3 }), "ok")
	l.m("hold retention 0", hv(func(h *storerec.Hold) { h.Retention = 0 }), "hold")
	l.m("hold retention 2^32 - 1", hv(func(h *storerec.Hold) { h.Retention = storerec.MaxU32 }), "ok")
	l.m("hold retention 2^32", hv(func(h *storerec.Hold) { h.Retention = storerec.MaxU32 + 1 }), "hold")
	l.m("hold deadline 0", hv(func(h *storerec.Hold) { h.Deadline = 0 }), "hold")
	l.m("hold deadline 2^32 - 1", hv(func(h *storerec.Hold) { h.Deadline = storerec.MaxU32 }), "ok")
	l.m("hold deadline 2^32", hv(func(h *storerec.Hold) { h.Deadline = storerec.MaxU32 + 1 }), "hold")
	l.m("hold checkC 0", hv(func(h *storerec.Hold) { h.CheckC = 0 }), "hold")
	l.m("hold checkC 1024", hv(func(h *storerec.Hold) { h.CheckC = 1024 }), "ok")
	l.m("hold checkC 1025", hv(func(h *storerec.Hold) { h.CheckC = 1025 }), "hold")
	l.m("hold an element negative", with(p, storerec.EKHold, append(holdValue(h)[:2], append([]cbor.Value{int64(-1)}, holdValue(h)[3:]...)...)), "hold")
	// kind presence
	for _, k := range []uint64{storerec.EKSize, storerec.EKRoot, storerec.EKContentHash, storerec.EKProofRoot, storerec.EKProofProfile, storerec.EKContentType} {
		l.m("a put without key "+itoa(k), without(p, k), "kind")
	}
	l.m("a put without its optional keys", without(without(without(p, storerec.EKETag), storerec.EKMeta), storerec.EKHold), "ok")
	for _, k := range []uint64{storerec.EKSize, storerec.EKETag, storerec.EKMeta, storerec.EKHold} {
		v, _ := p.Get(k)
		l.m("a delete with key "+itoa(k), with(d, k, v), "kind")
	}
	l.m("a delete with a bad key 7 (unread: kind)", with(d, storerec.EKSize, "x"), "kind")
	l.m("order: a put's bad size before a missing root", with(without(p, storerec.EKRoot), storerec.EKSize, "x"), "type")
	l.m("order: a missing root before a bad etag (kind is after every key)", with(without(p, storerec.EKRoot), storerec.EKETag, ""), "etag")
	// empty
	l.m("the empty object", e0, "ok")
	l.m("size 0 with another root", with(e0, storerec.EKRoot, contentBuilt.RootHash[:]), "empty")
	l.m("size 0 with another contentHash", with(e0, storerec.EKContentHash, must32("x")), "empty")
	l.m("size 0 with another proofRoot", with(e0, storerec.EKProofRoot, must32("x")), "empty")
	l.m("size 0 with hold terms", with(e0, storerec.EKHold, holdValue(h)), "empty")
	l.m("size 1 with the empty root", with(e0, storerec.EKSize, uint64(1)), "empty")
	l.m("size 13 with the empty root", with(p, storerec.EKRoot, storerec.EmptyRoot[:]), "empty")
	l.m("order: kind before empty", without(with(e0, storerec.EKRoot, must32("x")), storerec.EKContentType), "kind")
	return l.cases
}

func itoa(k uint64) string { return strconv.FormatUint(k, 10) }

// highS rewrites a strict low-S DER signature to its high-S twin.
func highS(der []byte) []byte {
	sig := must(ec.ParseDERSignature(der))
	derInt := func(v *big.Int) []byte {
		b := v.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	r, s := derInt(sig.R), derInt(new(big.Int).Sub(ec.S256().N, sig.S))
	body := append(append([]byte{0x02, byte(len(r))}, r...), append([]byte{0x02, byte(len(s))}, s...)...)
	return append([]byte{0x30, byte(len(body))}, body...)
}

// signDigest signs an outcome's signed digest with another key.
func signDigest(k *ec.PrivateKey, o cbor.Map) []byte {
	req, _ := o.Get(storerec.OKRequest)
	resp, _ := o.Get(storerec.OKResponse)
	d := storerec.SignedDigest(sha256.Sum256(req.([]byte)), [32]byte(resp.([]byte)))
	return must(k.Sign(d[:])).Serialize()
}

func pieceRefusals() []refusalCase {
	p := toMap(samplePiece())
	l := &caseList{kind: "piece"}
	l.cases = recordSteps("piece", p, storerec.MaxPieceRecord, storerec.MagicPiece, storerec.PKLast)
	l.add("exactly 4,194,560 bytes", padTo(p, 40, storerec.MaxPieceRecord), "ok")
	l.bucketCases(p, storerec.PKBucket)
	l.keyCases(p, storerec.PKWriter, "writer")
	l.b32Cases(p, storerec.PKRoot, "root")
	l.m("piece missing", without(p, storerec.PKPiece), "missing")
	l.m("piece as text", with(p, storerec.PKPiece, "x"), "type")
	l.m("piece empty", with(p, storerec.PKPiece, []byte{}), "piece")
	l.m("piece of one byte", with(p, storerec.PKPiece, []byte{0}), "ok")
	l.m("piece of 4,194,304 bytes", with(p, storerec.PKPiece, make([]byte, storerec.MaxPiece)), "ok")
	l.m("piece of 4,194,305 bytes", with(p, storerec.PKPiece, make([]byte, storerec.MaxPiece+1)), "piece")
	return l.cases
}

func frontRefusals() []refusalCase {
	f := toMap(sampleFront())
	l := &caseList{kind: "front"}
	l.cases = recordSteps("front", f, storerec.MaxFront, storerec.MagicFront, storerec.FKLast)
	l.add("exactly 2,048 bytes", padTo(f, 40, storerec.MaxFront), "ok")
	l.bucketCases(f, storerec.FKBucket)
	l.keyCases(f, storerec.FKFront, "front")
	l.keyCases(f, storerec.FKAdvertiser, "advertiser")
	l.m("the front is the advertiser", with(f, storerec.FKFront, Bob.Pub()), "ok")
	l.m("url missing", without(f, storerec.FKURL), "missing")
	l.m("url as bytes", with(f, storerec.FKURL, []byte("https://a.example")), "type")
	for _, u := range []struct {
		v, label string
	}{
		{"https://a.example", "ok"}, {"https://a.ex", "ok"}, {"https://a.e", "url"}, {"http://a.example", "url"},
		{"HTTPS://a.example", "url"}, {"https://a.example/", "url"}, {"https://a.example/x/", "url"},
		{"https://u@a.example", "url"}, {"https://u:p@a.example", "url"}, {"https://a.example/x@y", "ok"},
		{"https://a.example/?q", "url"}, {"https://a.example?q", "url"}, {"https://a.example#f", "url"},
		{"https://a.example/a b", "url"}, {"https:///abcdefg", "url"}, {"https://a.example/é", "url"},
		{"https://a.example:8443/bstore/v1", "ok"}, {"https://" + strings.Repeat("a", 504), "ok"},
		{"https://" + strings.Repeat("a", 505), "url"}, {"https://a.example/x\x7f", "url"},
	} {
		l.m("url "+u.v, with(f, storerec.FKURL, u.v), u.label)
	}
	l.createdCases(f, storerec.FKCreated)
	return l.cases
}

func outcomeRefusals() []refusalCase {
	pass := toMap(sampleOutcome(storerec.OutcomePass))
	na := toMap(sampleOutcome(storerec.OutcomeNoAnswer))
	l := &caseList{kind: "outcome"}
	l.cases = recordSteps("outcome", pass, storerec.MaxOutcome, storerec.MagicOutcome, storerec.OKLast)
	l.add("exactly 4,096 bytes", padTo(pass, 40, storerec.MaxOutcome), "ok")
	l.bucketCases(pass, storerec.OKBucket)
	l.keyCases(pass, storerec.OKBuyer, "buyer")
	l.createdCases(pass, storerec.OKCreated)
	l.m("request missing", without(pass, storerec.OKRequest), "missing")
	l.m("request as text", with(pass, storerec.OKRequest, "x"), "type")
	l.m("request not a request", with(pass, storerec.OKRequest, []byte{0xa0}), "request")
	q := toMap(sampleRequest())
	l.m("request with an unknown key", with(na, storerec.OKRequest, enc(with(q, uint64(11), uint64(0)))), "request")
	l.m("request with c 0", with(na, storerec.OKRequest, enc(with(q, storerec.QKC, uint64(0)))), "request")
	l.m("outcome missing", without(pass, storerec.OKOutcome), "missing")
	l.m("outcome as text", with(pass, storerec.OKOutcome, "1"), "type")
	l.m("outcome 0", with(na, storerec.OKOutcome, uint64(0)), "outcome")
	l.m("outcome 5", with(na, storerec.OKOutcome, uint64(5)), "outcome")
	l.m("response of 31 bytes", with(pass, storerec.OKResponse, make([]byte, 31)), "type")
	l.m("signature as text", with(pass, storerec.OKSignature, "x"), "type")
	l.m("pass without a response", without(pass, storerec.OKResponse), "kind")
	l.m("pass without a signature", without(pass, storerec.OKSignature), "kind")
	sig, _ := pass.Get(storerec.OKSignature)
	resp, _ := pass.Get(storerec.OKResponse)
	l.m("no-answer with a response", with(na, storerec.OKResponse, resp), "kind")
	l.m("unretrievable with a signature", with(with(na, storerec.OKOutcome, uint64(4)), storerec.OKSignature, sig), "kind")
	l.m("invalid with the pass's signature (it signs the response, not the outcome)", with(pass, storerec.OKOutcome, uint64(2)), "ok")
	l.m("pass over another response", with(pass, storerec.OKResponse, must32("other")), "response-signature")
	l.m("pass with a high-S signature", with(pass, storerec.OKSignature, highS(sig.([]byte))), "response-signature")
	l.m("pass with a signature of another key", with(pass, storerec.OKSignature, signDigest(challengePriv(Carol), pass)), "response-signature")
	l.m("pass with an empty signature", with(pass, storerec.OKSignature, []byte{}), "response-signature")
	l.m("order: kind before response-signature", without(with(pass, storerec.OKResponse, must32("x")), storerec.OKSignature), "kind")
	return l.cases
}

func requestRefusals() []refusalCase {
	q := toMap(sampleRequest())
	l := &caseList{kind: "request"}
	steps := recordSteps("request", q, storerec.MaxRequest, storerec.MagicRequest, storerec.QKLast)
	for i := range steps {
		steps[i].Label = "request"
	}
	l.cases = steps
	l.m("a key above 10", with(q, uint64(11), uint64(0)), "request")
	l.m("a key above 10, otherwise valid with range", with(toMap(sampleRequestRange()), uint64(64), "x"), "request")
	l.b32Cases(q, storerec.QKRoot, "root")
	l.b32Cases(q, storerec.QKProofRoot, "proofRoot")
	l.m("size 0", with(q, storerec.QKSize, uint64(0)), "request")
	l.m("size 2^43", with(q, storerec.QKSize, uint64(storerec.MaxSize)), "ok")
	l.m("size 2^43 + 1", with(q, storerec.QKSize, uint64(storerec.MaxSize+1)), "request")
	l.keyCases(q, storerec.QKProvider, "provider")
	l.keyCases(q, storerec.QKAnswerer, "answerer")
	l.m("seedHeight 2^32 - 1", with(q, storerec.QKSeedHeight, uint64(storerec.MaxU32)), "ok")
	l.m("seedHeight 2^32", with(q, storerec.QKSeedHeight, uint64(storerec.MaxU32+1)), "request")
	l.b32Cases(q, storerec.QKSeedHash, "seedHash")
	l.m("nonce of 15 bytes", with(q, storerec.QKNonce, make([]byte, 15)), "request")
	l.m("nonce of 17 bytes", with(q, storerec.QKNonce, make([]byte, 17)), "request")
	l.m("nonce missing", without(q, storerec.QKNonce), "request")
	l.m("c 0", with(q, storerec.QKC, uint64(0)), "request")
	l.m("c 1024", with(q, storerec.QKC, uint64(1024)), "ok")
	l.m("c 1025", with(q, storerec.QKC, uint64(1025)), "request")
	rq := toMap(sampleRequestRange())
	l.m("range [1, 3] of 4 blobs", rq, "ok")
	l.m("range [0, 0]", with(rq, storerec.QKRange, []cbor.Value{uint64(0), uint64(0)}), "ok")
	l.m("range [3, 4] past the last blob", with(rq, storerec.QKRange, []cbor.Value{uint64(3), uint64(4)}), "request")
	l.m("range [2, 1] reversed", with(rq, storerec.QKRange, []cbor.Value{uint64(2), uint64(1)}), "request")
	l.m("range of one element", with(rq, storerec.QKRange, []cbor.Value{uint64(1)}), "request")
	l.m("range of three elements", with(rq, storerec.QKRange, []cbor.Value{uint64(1), uint64(2), uint64(3)}), "request")
	l.m("range negative", with(rq, storerec.QKRange, []cbor.Value{int64(-1), uint64(2)}), "request")
	l.m("range a map", with(rq, storerec.QKRange, cbor.Map{}), "request")
	l.m("range on a one-blob object", with(q, storerec.QKRange, []cbor.Value{uint64(0), uint64(0)}), "ok")
	l.add("the largest request, well under 512 bytes", must(sampleRequestRange().Encode()), "ok")
	for i := range l.cases {
		if l.cases[i].Label != "ok" {
			l.cases[i].Label = "request"
		}
	}
	return l.cases
}
