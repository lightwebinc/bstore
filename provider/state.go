package provider

import (
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/payee"
)

// State is what a provider keeps between runs, in state.json in its data
// directory, written whole to a temporary file, synced and renamed. Large
// per-root data (the proof tree's leaves, the closure's objects) lives in
// files of its own under roots/.
type State struct {
	path string

	Identity string `json:"identity"`

	// The funding trees advertisements and renewals spend.
	Tree         *funding.Tree  `json:"tree,omitempty"`
	Trees        []funding.Tree `json:"trees,omitempty"`
	Ahead        []funding.Tree `json:"ahead,omitempty"`
	PendingTrees []PendingTree  `json:"pendingTrees,omitempty"`

	// Holdings by root (64 hex digits); Entries by commitment (display
	// txid of the entry carrier).
	Holdings map[string]*Holding  `json:"holdings"`
	Entries  map[string]*EntryRec `json:"entries"`
	// Unclaimed are pieces no held closure names yet, by object hash.
	Unclaimed map[string]*Unclaimed `json:"unclaimed"`
	// Deletes are the newest delete entry seen per bucket and key: a put
	// made no later than it that arrives after it (the plane does not order
	// entries) is ended on arrival instead of waiting for a closure forever.
	Deletes map[string]*DeleteRec `json:"deletes,omitempty"`
	// PendingAds are advertisement and renewal transactions signed and not
	// yet recorded as mined: the journal a restart resumes from.
	PendingAds []PendingAd `json:"pendingAds,omitempty"`
	// Outbox is mined advertisements still to reach a host.
	Outbox []Outgoing `json:"outbox,omitempty"`
	// Free counts the free settlement challenges answered, by hold
	// outpoint ("<txid>.<vout>"): at most two each (spec section 9.4).
	Free map[string]int `json:"free"`
	// Payouts are the settlements found paying this provider, by entry
	// commitment: from a remittance or from the hold output's spend.
	Payouts map[string]*Payout `json:"payouts"`
	// MadeGood are lost fast remittances (unsettleable) whose entry's hold
	// output was later spent by a mined settlement paying this provider at
	// least as much, by the lost txid: the writer made the loss good, so it
	// is not held for confirmation on their account.
	MadeGood map[string]string `json:"madeGood,omitempty"`
	// Owed are renewals paid for and not made yet.
	Owed []Owed `json:"owed,omitempty"`
	// Held are challenge payments taken and held for confirmation, by
	// txid, with their class: sent again once mined, each buys its answer.
	Held map[string]HeldRec `json:"held,omitempty"`
	// Fundings is the table an earlier version kept here: read once, into
	// the funding log (fundingBook), and not written again.
	Fundings map[string]*Funding `json:"fundings,omitempty"`
	// Book is the payee's record of the ledger's payments settled.
	payee.Book
}

// PendingTree is a funding tree signed for a coin and not yet adopted
// (producer.Trees.Prepare); ReturnedOnce marks one whose coin came back
// once, kept for one more recovery.
type PendingTree struct {
	Tree         funding.Tree   `json:"tree"`
	Coin         bwallet.Output `json:"coin"`
	ReturnedOnce bool           `json:"returnedOnce,omitempty"`
}

// Holding is a closure the provider holds, or is deciding to.
type Holding struct {
	Root        string `json:"root"`
	Size        uint64 `json:"size"`
	ContentHash string `json:"contentHash"`
	ProofRoot   string `json:"proofRoot"`
	// RootLen is the root node's length: the advertisement's fileSize.
	RootLen uint64 `json:"rootLen"`
	// Objects and Blobs count the closure's distinct objects and its blob
	// positions; Bytes is what its distinct objects take on disk.
	Objects int    `json:"objects"`
	Blobs   int    `json:"blobs"`
	Bytes   uint64 `json:"bytes"`
	// Verified once the closure and its proof root checked out; Damaged
	// once an object read back wrong or went missing, after which the root
	// is neither served, answered for nor renewed.
	Verified bool `json:"verified"`
	Damaged  bool `json:"damaged,omitempty"`
	// Buckets and Entries name what it is held for.
	Buckets []string `json:"buckets"`
	Entries []string `json:"entries"`
	// Ad is the live advertisement; Lease is the expiry the closure is
	// kept through (an advertisement's, or a pending one's).
	Ad    *AdRec `json:"ad,omitempty"`
	Lease uint64 `json:"lease"`
	// At is when it was taken, Unix seconds.
	At int64 `json:"at"`
	// Handed is a front's: every object was handed to the backend and the
	// advertiser's advertisement adopted, and the front's own copy of the
	// bytes dropped (Bytes is then 0).
	Handed bool `json:"handed,omitempty"`
}

// AdRec is a mined advertisement.
type AdRec struct {
	Txid   string `json:"txid"`
	Vout   uint32 `json:"vout"`
	Expiry uint64 `json:"expiry"`
	Height uint32 `json:"height"`
	RawHex string `json:"rawHex"`
	Bump   string `json:"bump"`
	// First is the txid of the advertisement this one renews, through
	// every renewal: the one whose first sight a renewal keeps.
	First string `json:"first"`
}

// PendingAd is an advertisement or renewal transaction signed and
// persisted before it reaches the settlement leg. RawHex is the
// transaction alone: its inputs are the funding tree the state keeps and,
// for a renewal, the advertisement it renews, so its BEEF is rebuilt from
// those (pendingTx). BeefHex is what rows before 2026-11-05 kept: a BEEF
// with every unmined ancestor, 142 KB each on the lab once funding trees
// chained up behind a slow block, thousands of them the bulk of a 247 MB
// state written whole on every save.
type PendingAd struct {
	Root    string `json:"root"`
	Txid    string `json:"txid"`
	RawHex  string `json:"rawHex,omitempty"`
	BeefHex string `json:"beef,omitempty"`
	Expiry  uint64 `json:"expiry"`
	// Tree and Vout are the funding output it spends for its fee; Prev the
	// advertisement a renewal spends.
	Tree string `json:"tree"`
	Vout uint32 `json:"vout"`
	Prev *AdRec `json:"prev,omitempty"`
	// Sent once the settlement leg took it.
	Sent bool  `json:"sent,omitempty"`
	At   int64 `json:"at"`
}

// Outgoing is a mined advertisement still to reach one host on one topic.
type Outgoing struct {
	Host  string `json:"host"`
	Topic string `json:"topic"`
	Txid  string `json:"txid"`
	Beef  string `json:"beef"`
	Tries int    `json:"tries"`
	Next  int64  `json:"next"`
}

// EntryRec is an object entry the provider received, and what it decided.
type EntryRec struct {
	C      string `json:"c"`
	Bucket string `json:"bucket"`
	// Key is the entry's key in the bucket: what a later delete names.
	// Entries noted before it was kept have none and no delete reaches them.
	Key         string   `json:"key,omitempty"`
	Writer      string   `json:"writer"`
	Root        string   `json:"root"`
	Size        uint64   `json:"size"`
	ContentHash string   `json:"contentHash"`
	ProofRoot   string   `json:"proofRoot"`
	Created     uint64   `json:"created"`
	Hold        *HoldRec `json:"hold,omitempty"`
	// Decision is "", "held", or why it was declined; At when it arrived.
	Decision string `json:"decision"`
	At       int64  `json:"at"`
}

// HoldRec is an entry's hold terms and what the provider found of them.
type HoldRec struct {
	Txid      string `json:"txid"` // display order
	Vout      uint32 `json:"vout"`
	Sats      uint64 `json:"sats"`
	T         uint64 `json:"t"`
	K         uint64 `json:"k"`
	Retention uint64 `json:"retention"`
	Deadline  uint64 `json:"deadline"`
	CheckC    uint64 `json:"checkC"`
	// Height is the hold output's block, once checked.
	Height uint32 `json:"height,omitempty"`
}

// Outpoint is the hold output as "<txid>.<vout>".
func (h *HoldRec) Outpoint() string { return h.Txid + "." + itoa(uint64(h.Vout)) }

// DeleteRec is a delete entry remembered for the puts it may still meet.
type DeleteRec struct {
	Created uint64 `json:"created"`
	At      int64  `json:"at"`
}

// Unclaimed is a piece kept until an entry names its closure.
type Unclaimed struct {
	Root   string `json:"root"`
	Bucket string `json:"bucket"`
	// Writer is who sent it: the piece's writer, or the identity that
	// uploaded it.
	Writer string `json:"writer"`
	Size   uint64 `json:"size"`
	At     int64  `json:"at"`
}

// Owed is a renewal paid for: the root's advertisement is owed through
// Until.
type Owed struct {
	Root    string `json:"root"`
	Until   uint64 `json:"until"`
	Payment string `json:"payment"`
	// Held is a renewal whose payment is held for confirmation: made once
	// the payment's proof verifies, dropped if it is double-spent first.
	Held bool `json:"held,omitempty"`
}

// HeldRec is a challenge payment held for confirmation: its class and
// when it was taken (Unix seconds).
type HeldRec struct {
	Class string `json:"class"`
	At    int64  `json:"at"`
}

// Funding is the carrier a funding outpoint paid for, as an earlier version
// kept it.
type Funding struct {
	C  string `json:"c"`
	At int64  `json:"at"`
}

// Payout is a settlement found paying this provider for one entry.
type Payout struct {
	Settlement string `json:"settlement"`
	Rank       int    `json:"rank"`
	Satoshis   uint64 `json:"satoshis"`
	// Via is "remittance" or "scan".
	Via string `json:"via"`
	At  int64  `json:"at"`
}

func loadState(dir string) (*State, error) {
	p := filepath.Join(dir, "state.json")
	st := &State{path: p}
	raw, err := os.ReadFile(p)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if err := json.Unmarshal(raw, st); err != nil {
			return nil, err
		}
		st.path = p
	}
	if st.Holdings == nil {
		st.Holdings = map[string]*Holding{}
	}
	if st.Entries == nil {
		st.Entries = map[string]*EntryRec{}
	}
	if st.Unclaimed == nil {
		st.Unclaimed = map[string]*Unclaimed{}
	}
	if st.Deletes == nil {
		st.Deletes = map[string]*DeleteRec{}
	}
	if st.Free == nil {
		st.Free = map[string]int{}
	}
	if st.Payouts == nil {
		st.Payouts = map[string]*Payout{}
	}
	return st, nil
}

// snapshot is a copy of the state that shares nothing a later change
// touches, for a save to encode outside the provider's lock.
func (st *State) snapshot() *State {
	c := *st
	if st.Tree != nil {
		t := *st.Tree
		c.Tree = &t
	}
	c.Trees = slices.Clone(st.Trees)
	c.Ahead = slices.Clone(st.Ahead)
	c.PendingTrees = slices.Clone(st.PendingTrees)
	for i := range c.PendingTrees {
		c.PendingTrees[i].Coin.Derivation = copyOf(c.PendingTrees[i].Coin.Derivation)
	}
	c.Holdings = copyRows(st.Holdings, func(h *Holding) {
		h.Buckets = slices.Clone(h.Buckets)
		h.Entries = slices.Clone(h.Entries)
		h.Ad = copyOf(h.Ad)
	})
	c.Entries = copyRows(st.Entries, func(e *EntryRec) { e.Hold = copyOf(e.Hold) })
	c.Unclaimed = copyRows(st.Unclaimed, nil)
	c.Deletes = copyRows(st.Deletes, nil)
	c.PendingAds = slices.Clone(st.PendingAds)
	for i := range c.PendingAds {
		c.PendingAds[i].Prev = copyOf(c.PendingAds[i].Prev)
	}
	c.Outbox = slices.Clone(st.Outbox)
	c.Free = maps.Clone(st.Free)
	c.Payouts = copyRows(st.Payouts, nil)
	c.MadeGood = maps.Clone(st.MadeGood)
	c.Owed = slices.Clone(st.Owed)
	c.Held = maps.Clone(st.Held)
	c.Settled = slices.Clone(st.Settled)
	c.Unsettleable = slices.Clone(st.Unsettleable)
	return &c
}

// copyRows copies a table of rows, each through deep when it holds
// references of its own. The rows lie in one allocation.
func copyRows[T any](m map[string]*T, deep func(*T)) map[string]*T {
	if m == nil {
		return nil
	}
	rows := make([]T, 0, len(m))
	out := make(map[string]*T, len(m))
	for k, v := range m {
		rows = append(rows, *v)
		r := &rows[len(rows)-1]
		if deep != nil {
			deep(r)
		}
		out[k] = r
	}
	return out
}

func copyOf[T any](v *T) *T {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

// save writes the state whole.
func (st *State) save() error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writeFileAtomic(st.path, raw)
}

func writeFileAtomic(p string, raw []byte) error {
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(p))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (st *State) pendingTree(txid string) *PendingTree {
	for i := range st.PendingTrees {
		if st.PendingTrees[i].Tree.Txid == txid {
			return &st.PendingTrees[i]
		}
	}
	return nil
}

func (st *State) dropPendingTree(txid string) {
	st.PendingTrees = slices.DeleteFunc(st.PendingTrees, func(p PendingTree) bool { return p.Tree.Txid == txid })
}

func (st *State) mintedAhead(txid string) *funding.Tree {
	for i := range st.Ahead {
		if st.Ahead[i].Txid == txid {
			return &st.Ahead[i]
		}
	}
	return nil
}

func (st *State) adopted(txid string) bool {
	return st.Tree != nil && st.Tree.Txid == txid || slices.ContainsFunc(st.Trees, func(t funding.Tree) bool { return t.Txid == txid })
}

func (st *State) dropPendingAd(txid string) {
	st.PendingAds = slices.DeleteFunc(st.PendingAds, func(p PendingAd) bool { return p.Txid == txid })
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
