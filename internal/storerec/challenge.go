package storerec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/chaintoken"
	"github.com/lightwebinc/bcommon/commit"
	"github.com/lightwebinc/bcommon/record"
)

// Request is a challenge request (magic stq 0x01).
type Request struct {
	Root       [32]byte
	ProofRoot  [32]byte
	Size       uint64
	Provider   []byte
	Answerer   []byte
	SeedHeight uint64
	SeedHash   [32]byte
	Nonce      [16]byte
	C          uint64
	// Range is [first, last] blob indices, or nil.
	Range *[2]uint64
}

// Encode writes the request canonically; it refuses what DecodeRequest
// refuses.
func (q *Request) Encode() ([]byte, error) {
	m := cbor.Map{
		{Key: QKMagic, Val: MagicRequest},
		{Key: QKRoot, Val: b(q.Root)},
		{Key: QKProofRoot, Val: b(q.ProofRoot)},
		{Key: QKSize, Val: q.Size},
		{Key: QKProvider, Val: q.Provider},
		{Key: QKAnswerer, Val: q.Answerer},
		{Key: QKSeedHeight, Val: q.SeedHeight},
		{Key: QKSeedHash, Val: b(q.SeedHash)},
		{Key: QKNonce, Val: append([]byte(nil), q.Nonce[:]...)},
		{Key: QKC, Val: q.C},
	}
	if q.Range != nil {
		m = append(m, cbor.Pair{Key: QKRange, Val: []cbor.Value{q.Range[0], q.Range[1]}})
	}
	out, err := record.Encode(m, nil, MaxRequest)
	if err != nil {
		return nil, err
	}
	if _, err := DecodeRequest(out); err != nil {
		return nil, err
	}
	return out, nil
}

// Hash is SHA-256 of a request's encoding.
func RequestHash(raw []byte) [32]byte { return sha256.Sum256(raw) }

// asRequest wraps a refusal of a request as request, keeping its rule.
func asRequest(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ruled(ErrRequest, privateRule(err)), err)
}

// DecodeRequest reads a request. Every refusal is request; its rule is, in
// order: too-large (over 512 bytes), cbor, key-type, magic, unknown-key (a
// key above 10), then per key missing, type and its rule: identity for
// provider and answerer, size (1 to 2^43), seed-height (0 to 2^32 - 1), c
// (1 to 1024), range ([first, last], first <= last < the blob count).
func DecodeRequest(raw []byte) (*Request, error) {
	q, err := decodeRequest(raw)
	return q, asRequest(err)
}

var (
	errSeedHeight = errors.New("storerec: seedHeight out of range")
	errC          = errors.New("storerec: c out of range")
	errRange      = errors.New("storerec: range out of range")
	errUnknownKey = errors.New("storerec: a key this record does not define")
)

// privateRule names a refusal inside a request or a body: a contract label
// where one applies, else the codec's own rule.
func privateRule(err error) string {
	for _, p := range []struct {
		err  error
		rule string
	}{{errSeedHeight, "seed-height"}, {errC, "c"}, {errRange, "range"}, {errUnknownKey, "unknown-key"}, {errSamples, "samples"}} {
		if errors.Is(err, p.err) {
			return p.rule
		}
	}
	return Reason(err)
}

func decodeRequest(raw []byte) (*Request, error) {
	f, err := readRecord(raw, MaxRequest, QKLast, MagicRequest)
	if err != nil {
		return nil, err
	}
	if len(f.extra()) > 0 {
		return nil, errUnknownKey
	}
	q := &Request{}
	if q.Root, err = f.b32(QKRoot); err != nil {
		return nil, err
	}
	if q.ProofRoot, err = f.b32(QKProofRoot); err != nil {
		return nil, err
	}
	if q.Size, err = f.uint(QKSize, 1, MaxSize, ErrSize); err != nil {
		return nil, err
	}
	if q.Provider, err = f.key(QKProvider); err != nil {
		return nil, err
	}
	if q.Answerer, err = f.key(QKAnswerer); err != nil {
		return nil, err
	}
	if q.SeedHeight, err = f.uint(QKSeedHeight, 0, MaxU32, errSeedHeight); err != nil {
		return nil, err
	}
	if q.SeedHash, err = f.b32(QKSeedHash); err != nil {
		return nil, err
	}
	nonce, err := f.bytesN(QKNonce, NonceLen)
	if err != nil {
		return nil, err
	}
	q.Nonce = [16]byte(nonce)
	if q.C, err = f.uint(QKC, 1, MaxC, errC); err != nil {
		return nil, err
	}
	if f.has(QKRange) {
		a, err := f.array(QKRange)
		if err != nil {
			return nil, err
		}
		if len(a) != 2 {
			return nil, errRange
		}
		first, ok1 := a[0].(uint64)
		last, ok2 := a[1].(uint64)
		if !ok1 || !ok2 || first > last || last >= Blobs(q.Size) {
			return nil, errRange
		}
		q.Range = &[2]uint64{first, last}
	}
	return q, nil
}

// Segments is the number of 4 KiB segments of an object of size bytes.
func Segments(size uint64) uint64 { return (size + SegmentSize - 1) / SegmentSize }

// Blobs is the number of 4 MiB blobs of an object of size bytes.
func Blobs(size uint64) uint64 { return (size + BlobSize - 1) / BlobSize }

// SegmentLen is the length of segment i of an object of size bytes.
func SegmentLen(i, size uint64) uint64 {
	if n := Segments(size); i == n-1 {
		return size - SegmentSize*(n-1)
	}
	return SegmentSize
}

// Domain is the number of segments the indices are drawn from, and the
// first of them: every segment without range, the segments of blobs first
// to last with it.
func (q *Request) Domain() (m, offset uint64) {
	n := Segments(q.Size)
	if q.Range == nil {
		return n, 0
	}
	lo := SegmentsPerBlob * q.Range[0]
	hi := min(SegmentsPerBlob*(q.Range[1]+1), n)
	return hi - lo, lo
}

// IndexDigest is SHA-256(seedHash || proofRoot || provider || nonce ||
// uint32le(k)).
func (q *Request) IndexDigest(k uint32) [32]byte {
	h := sha256.New()
	h.Write(q.SeedHash[:])
	h.Write(q.ProofRoot[:])
	h.Write(q.Provider)
	h.Write(q.Nonce[:])
	var kb [4]byte
	binary.LittleEndian.PutUint32(kb[:], k)
	h.Write(kb[:])
	return [32]byte(h.Sum(nil))
}

// Index is the segment sample k asks for (spec section 7.2): the digest read
// as an unsigned little-endian integer, modulo the domain, offset by the
// domain's first segment.
func (q *Request) Index(k uint32) uint64 {
	d := q.IndexDigest(k)
	be := make([]byte, 32)
	for i := range d {
		be[i] = d[31-i]
	}
	m, off := q.Domain()
	r := new(big.Int).Mod(new(big.Int).SetBytes(be), new(big.Int).SetUint64(m))
	return off + r.Uint64()
}

// Sample is one sample of a response: the segment, or nil when the answerer
// leaves it to be fetched, and the siblings of its audit path.
type Sample struct {
	Segment  []byte // nil: by reference
	Siblings [][32]byte
}

// Body is a response body (magic str 0x01).
type Body struct {
	Request  [32]byte
	Samples  []Sample
	Answerer []byte
}

// Encode writes the body canonically.
func (r *Body) Encode() ([]byte, error) {
	samples := make([]cbor.Value, len(r.Samples))
	for i, s := range r.Samples {
		sib := make([]cbor.Value, len(s.Siblings))
		for j, h := range s.Siblings {
			sib[j] = b(h)
		}
		var seg cbor.Value
		if s.Segment != nil {
			seg = append([]byte(nil), s.Segment...)
		}
		samples[i] = []cbor.Value{seg, sib}
	}
	return record.Encode(cbor.Map{
		{Key: RKMagic, Val: MagicResponse},
		{Key: RKRequest, Val: b(r.Request)},
		{Key: RKSamples, Val: samples},
		{Key: RKAnswerer, Val: r.Answerer},
	}, nil, MaxSignedResponse)
}

var errSamples = errors.New("storerec: a sample is not [segment or null, siblings]")

// DecodeBody reads a response body for spec section 7.3's shape alone: the
// record steps, unknown-key, then per key missing and type, every sample
// [byte string or null, array of 32-byte strings] (samples). How many
// samples, and whether they fit the request, is the buyer's check.
func DecodeBody(raw []byte) (*Body, error) {
	f, err := readRecord(raw, MaxSignedResponse, RKLast, MagicResponse)
	if err != nil {
		return nil, err
	}
	if len(f.extra()) > 0 {
		return nil, errUnknownKey
	}
	r := &Body{}
	if r.Request, err = f.b32(RKRequest); err != nil {
		return nil, err
	}
	a, err := f.array(RKSamples)
	if err != nil {
		return nil, err
	}
	r.Samples = make([]Sample, len(a))
	for i, e := range a {
		s, ok := e.([]cbor.Value)
		if !ok || len(s) != 2 {
			return nil, fmt.Errorf("%w: sample %d", errSamples, i)
		}
		switch seg := s[0].(type) {
		case nil:
		case []byte:
			r.Samples[i].Segment = seg
		default:
			return nil, fmt.Errorf("%w: sample %d", errSamples, i)
		}
		sib, ok := s[1].([]cbor.Value)
		if !ok {
			return nil, fmt.Errorf("%w: sample %d", errSamples, i)
		}
		r.Samples[i].Siblings = make([][32]byte, len(sib))
		for j, h := range sib {
			x, ok := h.([]byte)
			if !ok || len(x) != 32 {
				return nil, fmt.Errorf("%w: sample %d sibling %d", errSamples, i, j)
			}
			r.Samples[i].Siblings[j] = [32]byte(x)
		}
	}
	if r.Answerer, err = f.bytes(RKAnswerer); err != nil {
		return nil, err
	}
	return r, nil
}

// SignedDigest is what a response signature covers: SHA-256 of
// SHA-256(request) || response hash (64 bytes), as BRC-100's
// createSignature computes a signature over data.
func SignedDigest(requestHash, responseHash [32]byte) [32]byte {
	return sha256.Sum256(append(append([]byte(nil), requestHash[:]...), responseHash[:]...))
}

// ChallengeKey is derive(answerer, "challenge"), read from the identity key.
func ChallengeKey(answerer []byte) (*ec.PublicKey, error) {
	k, err := CheckKey(answerer)
	if err != nil {
		return nil, err
	}
	return ChallengeDerivation.ExpectedLockingKey(k)
}

// VerifyChallengeSig checks a strict low-S DER signature over
// SignedDigest under derive(answerer, "challenge").
func VerifyChallengeSig(answerer []byte, requestHash, responseHash [32]byte, sig []byte) bool {
	if chaintoken.CheckDER(sig) != nil {
		return false
	}
	key, err := ChallengeKey(answerer)
	if err != nil {
		return false
	}
	s, err := ec.ParseDERSignature(sig)
	if err != nil {
		return false
	}
	d := SignedDigest(requestHash, responseHash)
	return s.Verify(d[:], key)
}

// SignResponse signs a body with the answerer's derived challenge key and
// returns the signed response [body, signature].
func SignResponse(priv *ec.PrivateKey, requestRaw, body []byte) ([]byte, error) {
	d := SignedDigest(sha256.Sum256(requestRaw), sha256.Sum256(body))
	sig, err := priv.Sign(d[:])
	if err != nil {
		return nil, err
	}
	return cbor.Encode([]cbor.Value{body, sig.Serialize()})
}

// SplitSigned reads a signed response [body, signature] as byte strings.
func SplitSigned(signed []byte) (body, sig []byte, ok bool) {
	if len(signed) > MaxSignedResponse {
		return nil, nil, false
	}
	v, err := cbor.DecodeValue(signed)
	if err != nil {
		return nil, nil, false
	}
	a, ok := v.([]cbor.Value)
	if !ok || len(a) != 2 {
		return nil, nil, false
	}
	body, ok1 := a[0].([]byte)
	sig, ok2 := a[1].([]byte)
	return body, sig, ok1 && ok2
}

// The buyer's labels (spec section 7.4) and the answerer's refusals.
var (
	ErrSignature   = errors.New("storerec: the signed response is not [body, signature] or does not verify")
	ErrRespCount   = errors.New("storerec: samples are not c, or a path's length is not the audit path's")
	ErrFetch       = errors.New("storerec: a by-reference segment could not be fetched")
	ErrSegment     = errors.New("storerec: a segment is not its segment's length")
	ErrPath        = errors.New("storerec: a leaf and its siblings do not reproduce proofRoot")
	ErrSeed        = errors.New("storerec: the seed is not a block the answerer holds within the window")
	ErrUnknownRoot = errors.New("storerec: root is not held, or proofRoot or size is not its own")
)

// Fetcher is the buyer's view of the tested URL: a Range fetch of one
// segment within its blob, and a whole blob, by blob index.
type Fetcher interface {
	Range(blob, offset, length uint64) ([]byte, error)
	Blob(blob uint64) ([]byte, error)
}

// Verdict is a buyer's verdict on a signed response.
type Verdict struct {
	// Outcome is OutcomePass, OutcomeInvalid, OutcomeNoAnswer or
	// OutcomeUnretrievable.
	Outcome int
	// Err is the step that failed (nil for a pass); Sample is the sample it
	// failed at, -1 when no sample is concerned; Recheck is true when the
	// verdict came from the whole-blob recheck.
	Err     error
	Sample  int
	Recheck bool
	// Rechecked lists the by-reference samples the whole blob passed after
	// a fetched segment failed (the URL's Range handling was wrong).
	Rechecked []int
}

// VerifyResponse is the buyer's check of spec section 7.4: steps 1 to 3
// once (signature, request, count with every path's length), then per
// sample in k order fetch, segment and path, stopping at the first that
// fails. A by-reference sample that fails step 4, 5 or 6 is rechecked from
// the whole blob: the blob is held to blobHash(blob) (its CHIRP object
// hash); if it verifies, the segment is taken from it and steps 5 and 6 run
// again (a pass, or invalid); if it does not verify or cannot be fetched,
// the sample is unretrievable. req must be a request DecodeRequest admits
// and requestRaw its encoding.
func VerifyResponse(req *Request, requestRaw, signed []byte, blobHash func(blob uint64) [32]byte, fetch Fetcher) Verdict {
	fail := func(o int, err error, k int) Verdict { return Verdict{Outcome: o, Err: err, Sample: k} }
	// 1. signature.
	body, sig, ok := SplitSigned(signed)
	if !ok {
		return fail(OutcomeNoAnswer, ruled(ErrSignature, "shape"), -1)
	}
	if !VerifyChallengeSig(req.Answerer, sha256.Sum256(requestRaw), sha256.Sum256(body), sig) {
		return fail(OutcomeNoAnswer, ruled(ErrSignature, "verify"), -1)
	}
	// 2. request.
	r, err := DecodeBody(body)
	if err != nil {
		return fail(OutcomeInvalid, ruled(ErrRequest, "body-"+privateRule(err)), -1)
	}
	if r.Request != sha256.Sum256(requestRaw) {
		return fail(OutcomeInvalid, ruled(ErrRequest, "request-hash"), -1)
	}
	if !bytes.Equal(r.Answerer, req.Answerer) {
		return fail(OutcomeInvalid, ruled(ErrRequest, "answerer"), -1)
	}
	front := !bytes.Equal(req.Answerer, req.Provider)
	if front {
		for _, s := range r.Samples {
			if s.Segment != nil {
				return fail(OutcomeInvalid, ruled(ErrRequest, "inline-from-front"), -1)
			}
		}
	}
	// 3. count.
	if uint64(len(r.Samples)) != req.C {
		return fail(OutcomeInvalid, ruled(ErrRespCount, "samples"), -1)
	}
	n := Segments(req.Size)
	idx := make([]uint64, req.C)
	for k := range r.Samples {
		idx[k] = req.Index(uint32(k))
		want, _ := commit.PathLen(idx[k], n)
		if len(r.Samples[k].Siblings) != want {
			return fail(OutcomeInvalid, ruled(ErrRespCount, "path-length"), k)
		}
	}
	// 4 to 6, sample-major.
	v := Verdict{Outcome: OutcomePass, Sample: -1}
	for k, s := range r.Samples {
		i := idx[k]
		want := SegmentLen(i, req.Size)
		blob, off := i/SegmentsPerBlob, (i%SegmentsPerBlob)*SegmentSize
		if s.Segment != nil {
			if err := checkSample(s.Segment, want, i, n, s.Siblings, req.ProofRoot); err != nil {
				return fail(OutcomeInvalid, err, k)
			}
			continue
		}
		seg, ferr := fetch.Range(blob, off, want)
		var first error
		if ferr != nil {
			first = ErrFetch
		} else {
			first = checkSample(seg, want, i, n, s.Siblings, req.ProofRoot)
		}
		if first == nil {
			continue
		}
		whole, berr := fetch.Blob(blob)
		if berr != nil || sha256.Sum256(whole) != blobHash(blob) {
			return Verdict{Outcome: OutcomeUnretrievable, Err: first, Sample: k, Recheck: true}
		}
		var from []byte
		if off+want <= uint64(len(whole)) {
			from = whole[off : off+want]
		}
		if err := checkSample(from, want, i, n, s.Siblings, req.ProofRoot); err != nil {
			return Verdict{Outcome: OutcomeInvalid, Err: err, Sample: k, Recheck: true}
		}
		v.Rechecked = append(v.Rechecked, k)
	}
	return v
}

// checkSample is steps 5 and 6 for one segment.
func checkSample(seg []byte, want, i, n uint64, siblings [][32]byte, proofRoot [32]byte) error {
	if uint64(len(seg)) != want {
		return ErrSegment
	}
	if !commit.VerifyAt(commit.HashLeaf(seg), i, n, siblings, proofRoot) {
		return ErrPath
	}
	return nil
}

// AnswerCheck is an answerer's refusals before it answers (spec sections
// 7.2 and 10.1): request when the bytes are not a request, seed when its
// headers hold no block seedHash at seedHeight or seedHeight is outside
// tip - 6 to tip, unknown-root when it does not hold root or proofRoot or
// size is not its own computation for root. own reports the answerer's own
// proof root and size for a root it holds.
func AnswerCheck(requestRaw []byte, tip uint64, headerAt func(height uint64) ([32]byte, bool),
	own func(root [32]byte) (proofRoot [32]byte, size uint64, ok bool)) (*Request, error) {
	q, err := DecodeRequest(requestRaw)
	if err != nil {
		return nil, err
	}
	if h, ok := headerAt(q.SeedHeight); !ok || h != q.SeedHash || q.SeedHeight > tip || q.SeedHeight+SeedWindow < tip {
		return nil, ErrSeed
	}
	pr, size, ok := own(q.Root)
	if !ok || pr != q.ProofRoot || size != q.Size {
		return nil, ErrUnknownRoot
	}
	return q, nil
}
