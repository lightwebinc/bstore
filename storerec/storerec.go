// Package storerec is the byte-level contract of bstore: the bucket record
// a mined token carries, the object entry, the piece, the outcome record and
// the front statement that ride unmined carriers, the challenge request and
// response, the hold terms, the key map and its contexts, the proof tree,
// settlement, BRC-26 advertisements, the names and lookup questions, the
// topic manager's transaction rules and a reader's checks that need no
// network. docs/spec.md is the normative text; this package generates the
// golden vectors under testdata/vectors.
//
// Every record is canonical CBOR through bcommon's cbor package, so one value
// has one encoding a reader accepts. Every decoder bounds its input before
// the CBOR decoder runs, and the CBOR decoder bounds every declared length
// against the bytes present before it allocates.
package storerec

import (
	"crypto/sha256"
	"errors"

	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"
	"github.com/lightwebinc/bcommon/record"
)

// Registry values (docs/frozen.md section 1).
const (
	// ProtocolName is the BRC-43 protocol name of every bstore derivation,
	// at security level 1.
	ProtocolName = "bstore"

	KeyBucket    = "bucket"
	KeyChain     = "chain"
	KeyEntry     = "entry"
	KeyPiece     = "piece"
	KeyOutcome   = "outcome"
	KeyFront     = "front"
	KeyHold      = "hold"
	KeyChallenge = "challenge"
	KeyFund      = "fund"

	RecordsPrefix = "tm_bstore_"
	BlobPrefix    = "tm_bstoreblob_"
	LookupService = "ls_bstore"
	HostTerms     = "/ls_bstore/terms"
	FrontSuffix   = "/bstore/v1"

	// DomainBucketID is hashed into a bucket id (spec section 3.3).
	DomainBucketID = "bstore bucket id v1"

	// AdProtocolName and AdKeyID are BRC-26's advertisement derivation as
	// the SDK has it, at security level 2; borrowed, not registered.
	AdProtocolName = "uhrp advertisement"
	AdKeyID        = "1"
	// SettleProtocolName is BRC-29's, at security level 2; borrowed.
	SettleProtocolName = "3241645161d8"
)

// KeyIDs are the registry's key ids in the order of spec section 13.1.
var KeyIDs = []string{KeyBucket, KeyChain, KeyEntry, KeyPiece, KeyOutcome, KeyFront, KeyHold, KeyChallenge, KeyFund}

// Baskets are the wallet baskets bstore's tooling uses; never on a host.
var Baskets = []string{"bstore fund", "bstore entry funding", "bstore piece funding", "bstore outcome funding",
	"bstore front funding", "bstore chain funding", "bstore chain", "bstore hold", "bstore kill tombstone"}

// Tags and magics under the prefix "st".
var (
	TagPrefix  = []byte("st")
	TagToken   = []byte{'s', 't', 0x01}
	TagFunding = []byte{'s', 't', 0x02}
	TagHold    = []byte{'s', 't', 0x03}

	MagicBucket   = []byte{'s', 't', 'b', 0x01}
	MagicEntry    = []byte{'s', 't', 'e', 0x01}
	MagicPiece    = []byte{'s', 't', 'p', 0x01}
	MagicOutcome  = []byte{'s', 't', 'o', 0x01}
	MagicFront    = []byte{'s', 't', 'f', 0x01}
	MagicRequest  = []byte{'s', 't', 'q', 0x01}
	MagicResponse = []byte{'s', 't', 'r', 0x01}
)

// Derivations under [1, "bstore"], each with counterparty anyone, so a
// reader derives the key from the identity key alone. The funding key uses
// counterparty self and no rule reads it.
var (
	Protocol            = wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryApp, Protocol: ProtocolName}
	BucketDerivation    = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyBucket}
	ChainDerivation     = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyChain}
	EntryDerivation     = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyEntry}
	PieceDerivation     = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyPiece}
	OutcomeDerivation   = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyOutcome}
	FrontDerivation     = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyFront}
	HoldDerivation      = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyHold}
	ChallengeDerivation = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyChallenge}
	FundDerivation      = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyFund}

	AdProtocol     = wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryAppAndCounterparty, Protocol: AdProtocolName}
	AdDerivation   = pushdrop.Derivation{Protocol: AdProtocol, KeyID: AdKeyID}
	SettleProtocol = wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryAppAndCounterparty, Protocol: SettleProtocolName}
)

// Bounds. A reader accepts every record within them; a later version may
// raise a bound but never lower it (docs/frozen.md section 12).
const (
	// MaxSafe is 2^53 - 1, the bound of every unsigned value the contract
	// leaves unbounded, so that a JSON or JavaScript reader holds it exactly.
	MaxSafe = 1<<53 - 1
	// MinTime and MaxTime bound a time in a record, Unix milliseconds.
	MinTime = 1
	MaxTime = 253402300799999

	MaxBucketRecord = 65536
	MaxWriters      = 16
	MaxChanges      = 1024
	MaxConsumed     = 1024
	MaxKeysInMap    = 1048576

	MaxEntry       = 8192
	MaxObjectKey   = 1024
	MaxSize        = 1 << 43
	MinContentType = 3
	MaxContentType = 127
	MaxETag        = 64
	MaxMetaPairs   = 32
	MaxMetaBytes   = 2048
	MaxMetaKey     = 128
	MaxMetaValue   = 1024
	MaxKind        = 255

	MaxPiece       = 4194304
	MaxPieceRecord = 4194560

	MaxOutcome = 4096
	MaxFront   = 2048
	MinURL     = 12
	MaxURL     = 512

	MaxRequest        = 512
	MaxC              = 1024
	NonceLen          = 16
	MaxSignedResponse = 8388608

	// Hold terms (spec section 4.3).
	MaxSatoshis  = 2100000000000000
	MaxT         = 16
	MaxU32       = 1<<32 - 1
	MaxCheckC    = 1024
	ProofProfile = 1

	// Contexts (spec section 11.3).
	MaxEntryContext = 1024
	MaxTokenContext = 4096
	MaxMapPath      = 20
	PageList        = 256
	PageExport      = 512

	// The proof tree (spec section 6).
	SegmentSize      = 4096
	BlobSize         = 4194304
	SegmentsPerBlob  = BlobSize / SegmentSize
	SeedWindow       = 6
	MaxProofPath     = 31
	DefaultC         = 30
	MaxAdsPerTx      = 16
	MaxAdURL         = 2048
	MinAge           = 0
	MaxMinAge        = 1008
	MinFreeC         = 30
	AdvertisementMin = 1

	// Identifiers (spec section 3.1).
	SuffixLen  = 10
	MaxName    = 25
	MinBucket  = 1 + 1 + SuffixLen
	MaxBucket  = MaxName + 1 + SuffixLen
	BucketID32 = 32

	// The BEEF ceilings of spec section 12.1, whatever a host's own bound.
	MaxCarrierBEEF = 81920
	MaxTokenBEEF   = 196608
	MaxSweepBEEF   = 196608
	MaxMinedBEEF   = 65536
	MaxPieceBEEF   = 4259840
	// DefaultMaxBEEF is a host's own BEEF bound at its floor.
	DefaultMaxBEEF = 262144
	// DefaultMaxSweepInputs is a host's sweep input bound at its floor.
	DefaultMaxSweepInputs = 1024
)

// Record keys of the bucket record (spec section 3.2).
const (
	BKMagic uint64 = iota
	BKBucket
	BKOwner
	BKID
	BKSeq
	BKPrev
	BKWriters
	BKChanges
	BKCount
	BKMapRoot
	BKCreated
	BKConsumed
	BKLast = BKConsumed
)

// Record keys of the object entry (spec section 4.1).
const (
	EKMagic uint64 = iota
	EKBucket
	EKID
	EKWriter
	EKKey
	EKKind
	EKCreated
	EKSize
	EKRoot
	EKContentHash
	EKProofRoot
	EKProofProfile
	EKContentType
	EKETag
	EKMeta
	EKHold
	EKLast = EKHold
)

// Record keys of the piece (spec section 5).
const (
	PKMagic uint64 = iota
	PKBucket
	PKWriter
	PKRoot
	PKPiece
	PKLast = PKPiece
)

// Record keys of the outcome record (spec section 8.1).
const (
	OKMagic uint64 = iota
	OKBucket
	OKBuyer
	OKCreated
	OKRequest
	OKOutcome
	OKResponse
	OKSignature
	OKLast = OKSignature
)

// Record keys of the front statement (spec section 8.2).
const (
	FKMagic uint64 = iota
	FKBucket
	FKFront
	FKAdvertiser
	FKURL
	FKCreated
	FKLast = FKCreated
)

// Record keys of the challenge request (spec section 7.1).
const (
	QKMagic uint64 = iota
	QKRoot
	QKProofRoot
	QKSize
	QKProvider
	QKAnswerer
	QKSeedHeight
	QKSeedHash
	QKNonce
	QKC
	QKRange
	QKLast = QKRange
)

// Record keys of the response body (spec section 7.3).
const (
	RKMagic uint64 = iota
	RKRequest
	RKSamples
	RKAnswerer
	RKLast = RKAnswerer
)

// Entry kinds.
const (
	KindPut    = 1
	KindDelete = 2
)

// Outcomes (spec section 8.1).
const (
	OutcomePass          = 1
	OutcomeInvalid       = 2
	OutcomeNoAnswer      = 3
	OutcomeUnretrievable = 4
)

// EmptyHash is SHA-256 of no bytes: the content hash and proof root of the
// empty object, and the map root of the empty map.
var EmptyHash = sha256.Sum256(nil)

// EmptyRoot is the CHIRP Profile 1 root of empty content (BRC-167's vector).
var EmptyRoot = [32]byte{0x04, 0x03, 0x64, 0x0d, 0x63, 0x5f, 0xd2, 0x7b, 0x6c, 0x71, 0x9d, 0x2b, 0x81, 0xdb, 0x85, 0x34,
	0x83, 0xff, 0x8d, 0x0f, 0xb4, 0x6f, 0x7b, 0x90, 0xce, 0x9f, 0x9d, 0x7e, 0x9a, 0x27, 0x29, 0xee}

// The record refusals, each labelled as its spec section labels it.
var (
	ErrTooLarge = record.ErrTooLarge
	ErrCBOR     = record.ErrCBOR
	ErrKeyType  = record.ErrKeyType
	ErrMagic    = record.ErrMagic
	ErrMissing  = record.ErrMissing
	ErrType     = record.ErrType

	ErrBucket      = errors.New("storerec: bucket identifier breaks the grammar, or is not the topic's")
	ErrIdentity    = errors.New("storerec: key is not a canonical compressed key, or is the anyone key")
	ErrSeq         = errors.New("storerec: seq out of range")
	ErrWriters     = errors.New("storerec: writers over the bound, out of order, or naming the owner")
	ErrChanges     = errors.New("storerec: changes break their rule")
	ErrCount       = errors.New("storerec: count out of range")
	ErrCreated     = errors.New("storerec: created out of range")
	ErrConsumed    = errors.New("storerec: consumed breaks its rule")
	ErrFirst       = errors.New("storerec: seq and prev, changes, consumed, count or mapRoot disagree")
	ErrObjKey      = errors.New("storerec: object key breaks its rule")
	ErrKind        = errors.New("storerec: kind out of range, or a key present or absent against the kind")
	ErrSize        = errors.New("storerec: size out of range")
	ErrProfile     = errors.New("storerec: proofProfile is not 1")
	ErrContentType = errors.New("storerec: contentType breaks its rule")
	ErrETag        = errors.New("storerec: etag breaks its rule")
	ErrMeta        = errors.New("storerec: meta breaks its rule")
	ErrHold        = errors.New("storerec: hold terms break their bounds")
	ErrEmpty       = errors.New("storerec: the empty object's rule is broken")
	ErrPiece       = errors.New("storerec: piece out of range")
	ErrRequest     = errors.New("storerec: not a challenge request under spec section 7.1")
	ErrOutcome     = errors.New("storerec: outcome out of range")
	ErrRespSig     = errors.New("storerec: the answerer's signature does not verify")
	ErrURL         = errors.New("storerec: url breaks its rule")
	ErrQuery       = errors.New("storerec: question is not one of the defined classes")
)

// Reason is the fixed label a host counts a refusal by, and a reader or a
// buyer names a failed check by. A refusal of no defined label is "other".
func Reason(err error) string {
	if err == nil {
		return "ok"
	}
	for _, e := range reasons {
		if errors.Is(err, e.err) {
			return e.s
		}
	}
	return "other"
}

var reasons = []struct {
	err error
	s   string
}{
	{ErrTooLarge, "too-large"}, {ErrCBOR, "cbor"}, {ErrKeyType, "key-type"}, {ErrMagic, "magic"},
	{ErrMissing, "missing"}, {ErrType, "type"},
	{ErrBucket, "bucket"}, {ErrIdentity, "identity"}, {ErrSeq, "seq"}, {ErrWriters, "writers"},
	{ErrChanges, "changes"}, {ErrCount, "count"}, {ErrCreated, "created"}, {ErrConsumed, "consumed"},
	{ErrFirst, "first"}, {ErrObjKey, "objkey"}, {ErrKind, "kind"}, {ErrSize, "size"}, {ErrProfile, "profile"},
	{ErrContentType, "content-type"}, {ErrETag, "etag"}, {ErrMeta, "meta"}, {ErrHold, "hold"},
	{ErrEmpty, "empty"}, {ErrPiece, "piece"}, {ErrRequest, "request"}, {ErrOutcome, "outcome"},
	{ErrRespSig, "response-signature"}, {ErrURL, "url"}, {ErrQuery, "query"}, {ErrNotTaken, "not-taken"},
	// The transaction rules (admit.go).
	{ErrShape, "shape"}, {ErrBEEF, "beef"}, {ErrMineable, "mineable"}, {ErrUnlock, "unlock"},
	{ErrLock, "lock"}, {ErrFieldSig, "signature"}, {ErrFunding, "funding"}, {ErrUnmined, "unmined"},
	{ErrTokenInput, "token-input"}, {ErrBucketID, "bucket-id"}, {ErrTooMany, "too-many"},
	{ErrNotBstore, "not-bstore"}, {ErrPieceTx, "piece"}, {ErrHostConfig, "host-config"},
	// A buyer's checks (challenge.go).
	{ErrSignature, "signature"}, {ErrRespCount, "count"}, {ErrFetch, "fetch"}, {ErrSegment, "segment"},
	{ErrPath, "path"},
	// An answerer's refusals.
	{ErrSeed, "seed"}, {ErrUnknownRoot, "unknown-root"}, {ErrAdvertisement, "advertisement"},
	// The fold (keymap.go) and a reader's checks (reader.go).
	{ErrMapMismatch, "map-mismatch"}, {ErrMap, "map"}, {ErrRollback, "rollback"}, {ErrEntry, "entry"},
	{ErrWriter, "writer"}, {ErrRoot, "root"}, {ErrClosure, "closure"}, {ErrContent, "content"},
	{ErrProofRoot, "proof-root"},
}

// Rule is the codec's finer name for a refusal inside a label that covers
// several rules (a request inside `request`, a page check inside `map`); it
// is pinned by the vectors but is not a label of the contract.
func Rule(err error) string {
	var r *ruleError
	if errors.As(err, &r) {
		return r.rule
	}
	return ""
}

type ruleError struct {
	label error
	rule  string
}

func (e *ruleError) Error() string { return e.label.Error() + " (" + e.rule + ")" }
func (e *ruleError) Unwrap() error { return e.label }

func ruled(label error, rule string) error { return &ruleError{label: label, rule: rule} }
