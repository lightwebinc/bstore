package storerec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"
)

// The tie key is SHA-256(txid in hash byte order || vout LE32 || C).
func TestTieKeyLayout(t *testing.T) {
	txid := sha256.Sum256([]byte("txid"))
	c := sha256.Sum256([]byte("entry"))
	var b []byte
	b = append(b, txid[:]...)
	b = binary.LittleEndian.AppendUint32(b, 0x01020304)
	b = append(b, c[:]...)
	if len(b) != 68 || TieKey(txid, 0x01020304, c) != sha256.Sum256(b) {
		t.Fatal("tie key layout")
	}
}

// Grinding a low txid no longer wins a tie in first sight: the tie goes to
// the lower SHA-256 of the outpoint with the entry's commitment, which no
// advertiser can compute before the entry exists.
func TestGroundTxidDoesNotWinTie(t *testing.T) {
	c := sha256.Sum256([]byte("an entry made after the advertisements"))
	var zero [32]byte // the lowest txid in either byte order
	ground := &Ad{Txid: zero, Advertiser: []byte{2, 1}}
	zk := TieKey(zero, 0, c)
	var other *Ad
	for i := 0; other == nil; i++ {
		tx := sha256.Sum256([]byte(fmt.Sprintf("other %d", i)))
		if k := TieKey(tx, 0, c); bytes.Compare(k[:], zk[:]) < 0 {
			other = &Ad{Txid: tx, Advertiser: []byte{3, 1}}
		}
	}
	got := Rank(c, []Candidate{{Ad: ground, FirstSeen: 7}, {Ad: other, FirstSeen: 7}})
	if got[0].Ad != other || got[1].Ad != ground {
		t.Fatal("the all-zero txid won the tie")
	}
	// Against many commitments the ground txid wins about half the ties,
	// as any txid does: its low value buys nothing.
	wins := 0
	for i := 0; i < 256; i++ {
		ci := sha256.Sum256([]byte(fmt.Sprintf("entry %d", i)))
		if Rank(ci, []Candidate{{Ad: other, FirstSeen: 1}, {Ad: ground, FirstSeen: 1}})[0].Ad == ground {
			wins++
		}
	}
	if wins < 64 || wins > 192 {
		t.Fatalf("the ground txid won %d of 256 ties", wins)
	}
	// First sight still decides before the tie key.
	if Rank(c, []Candidate{{Ad: other, FirstSeen: 8}, {Ad: ground, FirstSeen: 7}})[0].Ad != ground {
		t.Fatal("first sight")
	}
}
