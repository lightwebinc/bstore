// Package backend is the hold-backend interface of spec section 17 and the
// native content-addressed backend on local disk.
//
// A provider is a market front over a hold backend. The front receives,
// validates, decides, advertises and answers; the backend stores and
// serves. The interface is frozen (frozen.md section 11) so that a backend
// built later for an incumbent UHRP host targets a surface that does not
// move: Put, Has, Get(hash, offset, length), ServeURL and Advertise.
package backend

import (
	"context"
	"errors"
)

// Advert is an advertisement a backend made the closure held under: the
// transaction that holds it, as BEEF, and where in it.
type Advert struct {
	Txid   string // display order
	Vout   uint32
	Expiry uint64
	// BEEF is the advertisement's transaction with what it needs to verify:
	// its own proof once mined, else its ancestry.
	BEEF []byte
}

// Backend is the hold-backend interface (spec section 17).
type Backend interface {
	// Put stores one CHIRP object under its SHA-256. It is idempotent and
	// refuses bytes that do not hash to hash.
	Put(hash [32]byte, data []byte) error
	// Has reports whether the backend can serve the object now.
	Has(hash [32]byte) bool
	// Get is a byte range of a stored object. The front verifies what it
	// gets (against the object hash, or a segment against the proof tree)
	// before it uses it.
	Get(hash [32]byte, offset, length uint64) ([]byte, error)
	// ServeURL is the HTTPS URL the root's advertisement names: the root
	// object's BRC-167 route at whoever serves the bytes.
	ServeURL(root [32]byte) string
	// Advertise makes the closure held through until (Unix seconds) and
	// returns the advertisement that says so: for a native backend the
	// front leases the closure and mints its own advertisement; for a
	// bridge backend the incumbent commits or renews through its own
	// routes and the bridge returns the incumbent's advertisement.
	Advertise(ctx context.Context, root [32]byte, until uint64) (*Advert, error)
}

// RangeReporter is optional: a backend that reads by reference implements
// it to be told when a ranged read came back as bytes other than the same
// range of the whole object, which held to its hash was right. The front
// then used the whole object; the backend may stop asking that server for
// ranges.
type RangeReporter interface {
	RangeWrong(hash [32]byte)
}

// Refusals.
var (
	// ErrHash is bytes that do not hash to the hash they are put under.
	ErrHash = errors.New("backend: the bytes do not hash to the object's hash")
	// ErrMissing is an object the backend does not hold.
	ErrMissing = errors.New("backend: no such object")
	// ErrRange is a range outside the object.
	ErrRange = errors.New("backend: the range is outside the object")
	// ErrTooLarge is an object over the backend's bound.
	ErrTooLarge = errors.New("backend: the object is over the bound")
)
