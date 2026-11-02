package provider

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// fundingBook is the funding outpoints carriers were taken on and the
// carrier each paid for: the newest maxFundings, for fundingMemory (spec
// section 13.2). It keeps a log of its own, fundings.log, and not a table in
// state.json: every carrier adds a row, and a state written whole carried
// 33 MB of them through every save. A claim is held in unsaved and appended
// with the next background pass, the durability it had as a row of the
// state; the log is written afresh once it is mostly forgotten records.
//
// A record is fundingRecord bytes: the outpoint's txid, its index, the
// carrier's commitment and when it was taken. A short tail, a write a crash
// cut off, is dropped at load.
type fundingBook struct {
	path    string
	marks   map[fundingKey]fundingMark
	order   []fundingKey // oldest first, from head
	head    int
	unsaved []byte
	logged  int // records in the log
}

type fundingKey struct {
	txid  chainhash.Hash
	index uint32
}

type fundingMark struct {
	c  [32]byte
	at int64
}

const fundingRecord = 32 + 4 + 32 + 8

// maxFundings bounds the funding outpoints remembered; past it the oldest
// is forgotten for the newest.
const maxFundings = 200000

// fundingMemory is how long a funding outpoint's carrier is remembered:
// longer than an unclaimed piece or an undecided entry is kept.
const fundingMemory = 7 * 24 * time.Hour

// loadFundings reads the log, and takes in what a state file of an earlier
// version kept.
func loadFundings(dir string, old map[string]*Funding, now time.Time) (*fundingBook, error) {
	b := &fundingBook{path: filepath.Join(dir, "fundings.log"), marks: map[fundingKey]fundingMark{}}
	raw, err := os.ReadFile(b.path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	whole := len(raw) - len(raw)%fundingRecord
	if whole != len(raw) {
		if err := os.Truncate(b.path, int64(whole)); err != nil {
			return nil, err
		}
	}
	cutoff := now.Add(-fundingMemory).Unix()
	for at := 0; at < whole; at += fundingRecord {
		k, m := decodeFunding(raw[at : at+fundingRecord])
		if m.at >= cutoff {
			b.remember(k, m)
		}
	}
	b.logged = whole / fundingRecord
	for _, o := range sortedFundings(old) {
		if o.mark.at >= cutoff {
			b.claim(o.key, o.mark.c, o.mark.at)
		}
	}
	return b, nil
}

type oldFunding struct {
	key  fundingKey
	mark fundingMark
}

// sortedFundings reads a state file's table, oldest first; a row it cannot
// read is passed over.
func sortedFundings(old map[string]*Funding) []oldFunding {
	var out []oldFunding
	for k, f := range old {
		txid, vout, ok := cutOutpoint(k)
		if !ok {
			continue
		}
		h, err := chainhash.NewHashFromHex(txid)
		if err != nil {
			continue
		}
		c, err := hex.DecodeString(f.C)
		if err != nil || len(c) != 32 {
			continue
		}
		out = append(out, oldFunding{key: fundingKey{txid: *h, index: vout}, mark: fundingMark{c: [32]byte(c), at: f.At}})
	}
	slices.SortFunc(out, func(a, b oldFunding) int { return int(a.mark.at - b.mark.at) })
	return out
}

func cutOutpoint(s string) (string, uint32, bool) {
	txid, vout, ok := strings.Cut(s, ".")
	if !ok {
		return "", 0, false
	}
	v, err := strconv.ParseUint(vout, 10, 32)
	return txid, uint32(v), err == nil
}

func encodeFunding(dst []byte, k fundingKey, m fundingMark) []byte {
	dst = append(dst, k.txid[:]...)
	dst = binary.BigEndian.AppendUint32(dst, k.index)
	dst = append(dst, m.c[:]...)
	return binary.BigEndian.AppendUint64(dst, uint64(m.at))
}

func decodeFunding(r []byte) (fundingKey, fundingMark) {
	var k fundingKey
	var m fundingMark
	copy(k.txid[:], r)
	k.index = binary.BigEndian.Uint32(r[32:])
	copy(m.c[:], r[36:])
	m.at = int64(binary.BigEndian.Uint64(r[68:]))
	return k, m
}

// free reports whether the outpoint paid for no other carrier.
func (b *fundingBook) free(k fundingKey, c [32]byte) bool {
	m, ok := b.marks[k]
	return !ok || m.c == c
}

// claim records the carrier the outpoint paid for; the first one stands.
func (b *fundingBook) claim(k fundingKey, c [32]byte, at int64) {
	if _, ok := b.marks[k]; ok {
		return
	}
	m := fundingMark{c: c, at: at}
	b.remember(k, m)
	b.unsaved = encodeFunding(b.unsaved, k, m)
}

func (b *fundingBook) remember(k fundingKey, m fundingMark) {
	if _, ok := b.marks[k]; ok {
		return
	}
	if len(b.marks) >= maxFundings {
		b.forgetOldest()
	}
	b.marks[k] = m
	b.order = append(b.order, k)
}

func (b *fundingBook) forgetOldest() {
	delete(b.marks, b.order[b.head])
	b.head++
	if b.head > len(b.order)/2 {
		b.order = b.order[:copy(b.order, b.order[b.head:])]
		b.head = 0
	}
}

// expire forgets the claims made before cutoff.
func (b *fundingBook) expire(cutoff int64) {
	for b.head < len(b.order) && b.marks[b.order[b.head]].at < cutoff {
		b.forgetOldest()
	}
}

// pending hands over what the log lacks: the claims to append, or, once the
// log is mostly forgotten records, every claim to write it afresh.
func (b *fundingBook) pending() (add []byte, whole bool) {
	add, b.unsaved = b.unsaved, nil
	if b.logged+len(add)/fundingRecord <= 2*max(len(b.marks), maxFundings/8) {
		return add, false
	}
	all := make([]byte, 0, len(b.marks)*fundingRecord)
	for _, k := range b.order[b.head:] {
		all = encodeFunding(all, k, b.marks[k])
	}
	return all, true
}

// write puts what pending handed over in the log; it touches the file only,
// so it runs outside the provider's lock, on one goroutine at a time.
func (b *fundingBook) write(add []byte, whole bool) error {
	if whole {
		return writeFileAtomic(b.path, add)
	}
	if len(add) == 0 {
		return nil
	}
	f, err := os.OpenFile(b.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(add); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// flushFundings brings the log up to date.
func (p *Provider) flushFundings() error {
	p.fundMu.Lock()
	defer p.fundMu.Unlock()
	p.mu.Lock()
	add, whole := p.fundings.pending()
	p.mu.Unlock()
	err := p.fundings.write(add, whole)
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case err != nil && !whole:
		p.fundings.unsaved = append(add, p.fundings.unsaved...)
	case err != nil:
		// The claims it carried are in no file: the next pass writes the
		// log afresh.
		p.fundings.logged = 2*maxFundings + 1
	case whole:
		p.fundings.logged = len(add) / fundingRecord
	default:
		p.fundings.logged += len(add) / fundingRecord
	}
	return err
}
