package vectors

import (
	"crypto/sha256"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// challengePriv is a party's derive(identity, "challenge") private key.
func challengePriv(p Party) *ec.PrivateKey {
	k, err := wallet.NewKeyDeriver(p.Priv).DerivePrivateKey(storerec.Protocol, storerec.KeyChallenge, pushdrop.Anyone())
	if err != nil {
		panic(err)
	}
	return k
}

// The fixed values the sample records use.
var (
	firstOutpoint = label("first-token-outpoint")
	bucketID      = storerec.BucketID(firstOutpoint, 0)
	content       = []byte("hello, bstore")
	contentBuilt  = must(storerec.Build(content))
	holdTxid      = label("hold-tx")
)

func sampleBucket0() *storerec.Bucket {
	return &storerec.Bucket{Bucket: Bucket, Owner: Owner.Pub(), ID: bucketID, Seq: 0, Writers: [][]byte{},
		Changes: []storerec.Change{}, Count: 0, MapRoot: storerec.EmptyHash, Created: Created, Consumed: [][32]byte{}}
}

func sampleBucket1() *storerec.Bucket {
	m := storerec.Map{"a/1.txt": label("C a/1"), "b/2.txt": label("C b/2")}
	return &storerec.Bucket{Bucket: Bucket, Owner: Owner.Pub(), ID: bucketID, Seq: 1, Prev: label("token 0"),
		Writers: [][]byte{Alice.Pub()},
		Changes: []storerec.Change{{Key: "a/1.txt", C: label("C a/1")}, {Key: "b/2.txt", C: label("C b/2")}},
		Count:   2, MapRoot: m.Root(), Created: Created + 1000, Consumed: [][32]byte{label("consumed 1")}}
}

func sampleBucket2() *storerec.Bucket {
	m := storerec.Map{"b/2.txt": label("C b/2")}
	return &storerec.Bucket{Bucket: Bucket, Owner: Owner.Pub(), ID: bucketID, Seq: 2, Prev: label("token 1"),
		Writers: [][]byte{Alice.Pub()},
		Changes: []storerec.Change{{Key: "a/1.txt", C: label("D a/1"), Removal: true}},
		Count:   1, MapRoot: m.Root(), Created: Created + 2000, Consumed: [][32]byte{},
		Extra: cbor.Map{{Key: uint64(12), Val: "from a later version"}}}
}

func samplePut() *storerec.Entry {
	etag := "5d41402abc4b2a76b9719d911017c592"
	return &storerec.Entry{Bucket: Bucket, ID: bucketID, Writer: Alice.Pub(), Key: "photos/2026/cat.jpg", Kind: storerec.KindPut,
		Created: Created, Size: contentBuilt.Size, Root: contentBuilt.RootHash, ContentHash: contentBuilt.ContentHash,
		ProofRoot: contentBuilt.ProofRoot, ProofProfile: 1, ContentType: "image/jpeg", ETag: &etag,
		Meta: []storerec.MetaPair{{Key: "album", Value: "cats"}, {Key: "x-1", Value: ""}},
		Hold: &storerec.Hold{Txid: holdTxid, Vout: 0, Satoshis: 10000, T: 2, K: 5, Retention: 31536000, Deadline: 900000, CheckC: 30}}
}

func samplePutMinimal() *storerec.Entry {
	return &storerec.Entry{Bucket: Bucket, ID: bucketID, Writer: Owner.Pub(), Key: "k", Kind: storerec.KindPut,
		Created: storerec.MinTime, Size: contentBuilt.Size, Root: contentBuilt.RootHash, ContentHash: contentBuilt.ContentHash,
		ProofRoot: contentBuilt.ProofRoot, ProofProfile: 1, ContentType: "application/octet-stream"}
}

func sampleEmpty() *storerec.Entry {
	return &storerec.Entry{Bucket: Bucket, ID: bucketID, Writer: Alice.Pub(), Key: "empty", Kind: storerec.KindPut,
		Created: storerec.MaxTime, Size: 0, Root: storerec.EmptyRoot, ContentHash: storerec.EmptyHash,
		ProofRoot: storerec.EmptyHash, ProofProfile: 1, ContentType: "text/plain"}
}

func sampleDelete() *storerec.Entry {
	return &storerec.Entry{Bucket: Bucket, ID: bucketID, Writer: Alice.Pub(), Key: "photos/2026/cat.jpg", Kind: storerec.KindDelete,
		Created: Created + 5000, Extra: cbor.Map{{Key: uint64(16), Val: []byte{1, 2}}}}
}

func sampleUnknownKind() *storerec.Entry {
	return &storerec.Entry{Bucket: Bucket, ID: bucketID, Writer: Alice.Pub(), Key: "later", Kind: 7, Created: Created,
		Unknown: cbor.Map{{Key: uint64(7), Val: "not a size"}, {Key: uint64(15), Val: []cbor.Value{true}}}}
}

func samplePiece() *storerec.Piece {
	return &storerec.Piece{Bucket: Bucket, Writer: Alice.Pub(), Root: contentBuilt.RootHash, Piece: contentBuilt.Root}
}

func sampleFront() *storerec.Front {
	return &storerec.Front{Bucket: Bucket, Front: Carol.Pub(), Advertiser: Bob.Pub(), URL: "https://front.example/bstore/v1", Created: Created}
}

func sampleRequest() *storerec.Request {
	return &storerec.Request{Root: contentBuilt.RootHash, ProofRoot: contentBuilt.ProofRoot, Size: contentBuilt.Size,
		Provider: Bob.Pub(), Answerer: Bob.Pub(), SeedHeight: 900000, SeedHash: label("block 900000"),
		Nonce: label16("nonce"), C: 3}
}

func sampleRequestRange() *storerec.Request {
	q := sampleRequest()
	q.Size = 3*storerec.BlobSize + 5
	q.Range = &[2]uint64{1, 3}
	q.Provider, q.Answerer = Bob.Pub(), Carol.Pub()
	return q
}

// signedPass is Bob's signed response to the sample request, answered
// inline from the sample content, and its parts.
func signedPass() (reqRaw, body, signed []byte, sig []byte) {
	q := sampleRequest()
	reqRaw = must(q.Encode())
	t := storerec.NewProofTree(content)
	b := storerec.Answer(q, reqRaw, content, t, true)
	body = must(b.Encode())
	signed = must(storerec.SignResponse(challengePriv(Bob), reqRaw, body))
	_, sig, _ = storerec.SplitSigned(signed)
	return
}

func sampleOutcome(outcome uint64) *storerec.Outcome {
	reqRaw, body, _, sig := signedPass()
	o := &storerec.Outcome{Bucket: Bucket, Buyer: Carol.Pub(), Created: Created, Request: reqRaw, Outcome: outcome}
	if outcome <= storerec.OutcomeInvalid {
		h := sha256.Sum256(body)
		o.Response, o.Signature = &h, sig
	}
	return o
}
