// Package vectors generates the golden vectors under testdata/vectors from
// the codec in internal/storerec. Every value is derived from fixed
// stand-ins (public test keys, hashes of labels for anything a client draws
// at random), so the files are reproducible byte for byte; every check the
// generator makes fails the run when it stops holding.
package vectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/cbor"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// The vectors' buckets; each suffix is a fixed example.
const (
	Bucket      = "photos_qzvmkwerty"
	OtherBucket = "archive_tqpzmwxrvk"
	// Created is the vectors' fixed time, Unix milliseconds.
	Created = 1798761600000
)

// Party is one public test identity: 32 bytes of one value as the private
// key. Never for real use.
type Party struct {
	Name string
	Priv *ec.PrivateKey
}

func party(name string, b byte) Party {
	k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{b}, 32))
	return Party{Name: name, Priv: k}
}

// The parties: the bucket's owner, a writer, two providers, a front and
// buyer, and a stranger.
var (
	Owner    = party("owner", 0x61)
	Alice    = party("alice", 0x62)
	Bob      = party("bob", 0x63)
	Carol    = party("carol", 0x64)
	Dave     = party("dave", 0x65)
	Erin     = party("erin", 0x66)
	Parties  = []Party{Owner, Alice, Bob, Carol, Dave, Erin}
	anyonePK = storerec.AnyoneKey
)

// Pub is the party's compressed identity key.
func (p Party) Pub() []byte { return p.Priv.PubKey().Compressed() }

// Wallet is a BRC-100 wallet over the party's key.
func (p Party) Wallet() wallet.Interface {
	w, err := wallet.NewCompletedProtoWallet(p.Priv)
	if err != nil {
		panic(err)
	}
	return w
}

// label is the fixed stand-in for a random value: SHA-256 of a label.
func label(format string, a ...any) [32]byte {
	return sha256.Sum256([]byte("bstore/vector/" + fmt.Sprintf(format, a...)))
}

func label16(format string, a ...any) [16]byte {
	l := label(format, a...)
	return [16]byte(l[:16])
}

func hx(b []byte) string    { return hex.EncodeToString(b) }
func h32(b [32]byte) string { return hex.EncodeToString(b[:]) }

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// displayHex writes a hash-byte-order txid in display order.
func displayHex(h [32]byte) string {
	r := make([]byte, 32)
	for i := range h {
		r[i] = h[31-i]
	}
	return hx(r)
}

func enc(v cbor.Value) []byte {
	b, err := cbor.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Names returns the file names in a stable order.
func Names(files map[string][]byte) []string {
	out := make([]string, 0, len(files))
	for n := range files {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func toJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	e.SetIndent("", "  ")
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func put(files map[string][]byte, name string, v any) error {
	b, err := toJSON(v)
	if err != nil {
		return err
	}
	files[name] = b
	return nil
}

// Generate builds every vector file. It fails when a check stops holding.
func Generate() (map[string][]byte, error) {
	files := map[string][]byte{}
	for _, f := range []func(map[string][]byte) error{
		genDerivation, genHex, genRefusal, genScript, genMap, genProof, genSettle, genQuery, genClosure, genTransaction,
	} {
		if err := f(files); err != nil {
			return nil, err
		}
	}
	return files, nil
}

// reason is the label of err, "ok" for nil.
func reason(err error) string { return storerec.Reason(err) }

// must panics on an error: a generator input that fails is a generator bug.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }
