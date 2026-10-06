package storerec

import (
	"bytes"
	"errors"
	"fmt"
)

// A reader's check labels (spec section 15.2), beside map (keymap.go) and
// the record labels.
var (
	ErrRollback  = errors.New("storerec: a token below the seq the reader holds")
	ErrEntry     = errors.New("storerec: the entry is not the one asked for")
	ErrWriter    = errors.New("storerec: the entry's writer is neither the token's owner nor one of its writers")
	ErrRoot      = errors.New("storerec: the root or the closure it names is not valid for the entry")
	ErrClosure   = errors.New("storerec: an object of the closure was not served as its hash names it")
	ErrContent   = errors.New("storerec: the content does not hash to contentHash")
	ErrProofRoot = errors.New("storerec: the proof root recomputed differs from the entry's")
)

// Link is one token of a walk: its txid (hash byte order), its record and,
// for the token at seq 0, the outpoint its chain-tree input spends.
type Link struct {
	Txid   [32]byte
	Bucket *Bucket
	Spends *Outpoint
}

// Outpoint is a txid in hash byte order and an output index.
type Outpoint struct {
	Txid [32]byte
	Vout uint32
}

// Held is the token a reader holds for a chain: the highest seq it
// verified, and that token's txid.
type Held struct {
	Seq  uint64
	Txid [32]byte
}

// CheckWalk is a reader's step 1 on tokens it verified against its own
// headers, latest first, walked back by prev to the token it holds or to
// seq 0 (spec section 15.1). held is nil when it holds none. It is rollback
// when the latest is below the seq held; else bucket when two neighbours do
// not link (prev is the next one's txid, seq one lower, the same bucket,
// owner and id), when the walk stops above seq 0 other than on the token
// held (the same seq and txid), or when it reaches seq 0 and the id is not
// the bucket id of the outpoint that token spends (bucket-id at admission).
func CheckWalk(held *Held, walk []Link) error {
	if len(walk) == 0 {
		return fmt.Errorf("%w: no token", ErrBucket)
	}
	top := walk[0].Bucket
	if held != nil && top.Seq < held.Seq {
		return ErrRollback
	}
	for i := 0; i+1 < len(walk); i++ {
		a, b := walk[i].Bucket, walk[i+1].Bucket
		if a.Prev != walk[i+1].Txid || a.Seq != b.Seq+1 || a.Bucket != b.Bucket || !bytes.Equal(a.Owner, b.Owner) || a.ID != b.ID {
			return fmt.Errorf("%w: token at seq %d does not link", ErrBucket, a.Seq)
		}
	}
	last := walk[len(walk)-1]
	if held != nil && last.Bucket.Seq == held.Seq {
		if last.Txid != held.Txid {
			return fmt.Errorf("%w: the walk ends on another token at the seq held", ErrBucket)
		}
		return nil
	}
	if last.Bucket.Seq != 0 {
		return fmt.Errorf("%w: the walk stops at seq %d", ErrBucket, last.Bucket.Seq)
	}
	if last.Spends == nil || BucketID(last.Spends.Txid, last.Spends.Vout) != last.Bucket.ID {
		return fmt.Errorf("%w: the first token's id is not its outpoint's", ErrBucket)
	}
	return nil
}

// CheckEntry is a reader's step 3 on a carrier it verified: the entry's
// bucket, id and key are what it asked for.
func CheckEntry(e *Entry, bucket string, id [32]byte, key string) error {
	if e.Bucket != bucket || e.ID != id || e.Key != key {
		return ErrEntry
	}
	return nil
}

// CheckWriter is a reader's step 4: the writer is the token's owner or one
// of its writers (the token the answer is proven against).
func CheckWriter(e *Entry, tok *Bucket) error {
	if bytes.Equal(e.Writer, tok.Owner) {
		return nil
	}
	for _, w := range tok.Writers {
		if bytes.Equal(e.Writer, w) {
			return nil
		}
	}
	return ErrWriter
}

// CheckKind is a reader's step 5: an unknown kind is unsupported, and a
// delete is never a key's value in a map.
func CheckKind(e *Entry) error {
	if e.Kind != KindPut {
		return fmt.Errorf("%w: kind %d", ErrKind, e.Kind)
	}
	return nil
}

// CheckHead is a reader's steps 2 to 5 for an object answer whose token it
// verified (step 1) and whose carrier it verified (SPV and the carrier
// rules): the context proves (key, C) or absence, the entry is the one
// asked for, by a writer of the token, and a put. A head (S3 HeadObject)
// stops here; it never asks a provider. It returns what the answer proves:
// found (with the entry), absent, or unheld (incomplete, never absent).
func CheckHead(tok *Bucket, key string, c *[32]byte, e *Entry, ctx *EntryContext, leaves []MapLeaf) (ObjectResult, error) {
	res, _, err := CheckObject(tok.Count, tok.MapRoot, key, c, ctx, leaves)
	if err != nil || res != ObjectFound {
		return res, err
	}
	if err := CheckEntry(e, tok.Bucket, tok.ID, key); err != nil {
		return 0, err
	}
	if err := CheckWriter(e, tok); err != nil {
		return 0, err
	}
	if err := CheckKind(e); err != nil {
		return 0, err
	}
	return ObjectFound, nil
}
