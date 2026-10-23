package module

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

// JournalFile is the journal's file name in the state directory.
const JournalFile = "bstore-rows.jsonl"

// Line is one journal line: what ls_bstore keeps beside the engine's
// storage (spec sections 12.2 to 12.5), as an append-only journal that
// retention compacts. The engine's storage holds the outputs a host still
// keeps; it cannot hold what must outlive them, or what it has no column
// for: which carrier a funding outpoint admitted first, the input outpoints
// of every admitted sweep, the retraction marks, when the host first saw
// each object, and which held outputs a later transaction spent. One JSON
// object a line:
//
//	{"k":"c","op":"<txid>.<vout>","c":"<C>","at":<ms>}          the first carrier admitted on a funding outpoint
//	{"k":"cf","op":"..."}                                      that row was dropped by retention
//	{"k":"a","b":"<bucket>","c":"<C>","at":<ms>}               a carrier, first seen
//	{"k":"af","c":"<C>"}                                       the carrier was dropped
//	{"k":"x","op":"...","key":"<owner>","at":<ms>,"life":<bool>,"b":"..."}
//	                                                           a retraction mark (b optional)
//	{"k":"xl","op":"..."}                                      the mark is kept for the host's life
//	{"k":"xf","op":"..."}                                      the mark was dropped by retention
//	{"k":"p","op":"...","s":"<sweep txid>","at":<ms>}          an input outpoint of an admitted sweep
//	{"k":"pf","op":"..."}                                      that outpoint was dropped by retention
//	{"k":"t","b":"...","txid":"<txid>","vout":<n>,"at":<ms>}   a bucket token, first seen
//	{"k":"tf","txid":"<txid>"}                                 the token was dropped
//	{"k":"o","b":"...","txid":"...","vout":<n>,"at":<ms>}      an advertisement, hold output or sweep output, first seen
//	{"k":"w","b":"...","txid":"...","vout":<n>}                a held output a later admitted transaction spent
//	{"k":"of","b":"...","txid":"...","vout":<n>}               the output was dropped
//
// Commitments are in hash byte order and txids in display order, both
// lowercase hex; keys are 66 hex digits; at is the host's time in Unix
// milliseconds. Replayed in order the lines rebuild the state; the first
// line about a thing sets it and a later one only forgets or upgrades it.
// The bytes are the TypeScript module's, key for key, so one journal reads
// under either.
type Line struct {
	K    string
	Op   string
	C    string
	B    string
	Key  string
	At   int64
	Life bool
	S    string
	Txid string
	Vout int64
}

// encode writes a line as JSON.stringify writes the TypeScript module's
// object: the keys in the order the module sets them.
func (l *Line) encode() ([]byte, error) {
	var v any
	switch l.K {
	case "c":
		v = struct {
			K  string `json:"k"`
			Op string `json:"op"`
			C  string `json:"c"`
			At int64  `json:"at"`
		}{l.K, l.Op, l.C, l.At}
	case "cf", "xl", "xf", "pf":
		v = struct {
			K  string `json:"k"`
			Op string `json:"op"`
		}{l.K, l.Op}
	case "a":
		v = struct {
			K  string `json:"k"`
			B  string `json:"b"`
			C  string `json:"c"`
			At int64  `json:"at"`
		}{l.K, l.B, l.C, l.At}
	case "af":
		v = struct {
			K string `json:"k"`
			C string `json:"c"`
		}{l.K, l.C}
	case "x":
		v = struct {
			K    string `json:"k"`
			Op   string `json:"op"`
			Key  string `json:"key"`
			At   int64  `json:"at"`
			Life bool   `json:"life"`
			B    string `json:"b,omitempty"`
		}{l.K, l.Op, l.Key, l.At, l.Life, l.B}
	case "p":
		v = struct {
			K  string `json:"k"`
			Op string `json:"op"`
			S  string `json:"s"`
			At int64  `json:"at"`
		}{l.K, l.Op, l.S, l.At}
	case "t", "o":
		v = struct {
			K    string `json:"k"`
			B    string `json:"b"`
			Txid string `json:"txid"`
			Vout int64  `json:"vout"`
			At   int64  `json:"at"`
		}{l.K, l.B, l.Txid, l.Vout, l.At}
	case "tf":
		v = struct {
			K    string `json:"k"`
			Txid string `json:"txid"`
		}{l.K, l.Txid}
	case "w", "of":
		v = struct {
			K    string `json:"k"`
			B    string `json:"b"`
			Txid string `json:"txid"`
			Vout int64  `json:"vout"`
		}{l.K, l.B, l.Txid, l.Vout}
	default:
		return nil, fmt.Errorf("journal: no line kind %q", l.K)
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

var (
	hexRE      = regexp.MustCompile(`^[0-9a-f]*$`)
	outpointRE = regexp.MustCompile(`^[0-9a-f]{64}\.(0|[1-9][0-9]{0,9})$`)
	bucketRE   = regexp.MustCompile(`^[a-z_]{12,36}$`)
)

// parseLine reads one journal line as the TypeScript module's isLine does:
// a JSON object whose kind's members are each of their type and shape.
func parseLine(raw []byte) (*Line, bool) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var o map[string]any
	if err := d.Decode(&o); err != nil || o == nil {
		return nil, false
	}
	l := &Line{}
	str := func(k string) (string, bool) { s, ok := o[k].(string); return s, ok }
	hx := func(k string, n int) (string, bool) {
		s, ok := str(k)
		return s, ok && len(s) == n && hexRE.MatchString(s)
	}
	num := func(k string) (int64, bool) {
		n, ok := o[k].(json.Number)
		if !ok {
			return 0, false
		}
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil || f < 0 || f != math.Trunc(f) || f > (1<<53-1) {
			return 0, false
		}
		return int64(f), true
	}
	op := func() bool {
		s, ok := str("op")
		l.Op = s
		return ok && outpointRE.MatchString(s)
	}
	bk := func() bool {
		s, ok := str("b")
		l.B = s
		return ok && bucketRE.MatchString(s)
	}
	k, _ := str("k")
	l.K = k
	ok := false
	switch k {
	case "c":
		var okC, okAt bool
		l.C, okC = hx("c", 64)
		l.At, okAt = num("at")
		ok = op() && okC && okAt
	case "cf", "xl", "xf", "pf":
		ok = op()
	case "a":
		var okC, okAt bool
		l.C, okC = hx("c", 64)
		l.At, okAt = num("at")
		ok = bk() && okC && okAt
	case "af":
		l.C, ok = hx("c", 64)
	case "x":
		var okK, okAt bool
		l.Key, okK = hx("key", 66)
		l.At, okAt = num("at")
		life, okL := o["life"].(bool)
		l.Life = life
		okB := true
		if _, has := o["b"]; has {
			okB = bk()
		}
		ok = op() && okK && okAt && okL && okB
	case "p":
		var okS, okAt bool
		l.S, okS = hx("s", 64)
		l.At, okAt = num("at")
		ok = op() && okS && okAt
	case "t", "o":
		var okT, okV, okAt bool
		l.Txid, okT = hx("txid", 64)
		l.Vout, okV = num("vout")
		l.At, okAt = num("at")
		ok = bk() && okT && okV && okAt
	case "tf":
		l.Txid, ok = hx("txid", 64)
	case "w", "of":
		var okT, okV bool
		l.Txid, okT = hx("txid", 64)
		l.Vout, okV = num("vout")
		ok = bk() && okT && okV
	}
	return l, ok
}

// Journal is where the state lives. Append and Rewrite are durable when
// they return.
type Journal interface {
	Load() ([]Line, error)
	Append(l Line) error
	// Rewrite replaces everything with lines, atomically.
	Rewrite(lines []Line) error
}

// MemoryJournal is a journal in memory, for tests and for a host that
// accepts losing it.
type MemoryJournal struct {
	mu    sync.Mutex
	Lines []Line
}

// Load implements Journal.
func (m *MemoryJournal) Load() ([]Line, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Line(nil), m.Lines...), nil
}

// Append implements Journal.
func (m *MemoryJournal) Append(l Line) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Lines = append(m.Lines, l)
	return nil
}

// Rewrite implements Journal.
func (m *MemoryJournal) Rewrite(lines []Line) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Lines = append([]Line(nil), lines...)
	return nil
}

// FileJournal is the journal as a file, bstore-rows.jsonl in a directory.
// Each line is written and flushed to disk before Append returns. A last
// line cut short by a crash is cut off on load; any other line that does
// not parse stops the load, because a mark or a sweep's outpoint silently
// skipped could let a retracted carrier be answered again. A rewrite goes
// to a new file that is flushed and then renamed over the old one, so a
// crash leaves one or the other whole.
type FileJournal struct {
	Dir  string
	Path string
	mu   sync.Mutex
	f    *os.File
}

// NewFileJournal makes the directory and names the file.
func NewFileJournal(dir string) (*FileJournal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &FileJournal{Dir: dir, Path: filepath.Join(dir, JournalFile)}, nil
}

// Load implements Journal.
func (j *FileJournal) Load() ([]Line, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	text, err := os.ReadFile(j.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(text) > 0 && text[len(text)-1] != '\n' {
		keep := bytes.LastIndexByte(text, '\n') + 1
		if err := os.Truncate(j.Path, int64(keep)); err != nil {
			return nil, err
		}
		text = text[:keep]
	}
	var out []Line
	for i, raw := range bytes.Split(text, []byte{'\n'}) {
		if len(raw) == 0 {
			continue
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("%s: line %d is not JSON", j.Path, i+1)
		}
		l, ok := parseLine(raw)
		if !ok {
			return nil, fmt.Errorf("%s: line %d is not a bstore row", j.Path, i+1)
		}
		out = append(out, *l)
	}
	return out, nil
}

// Append implements Journal.
func (j *FileJournal) Append(l Line) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := l.encode()
	if err != nil {
		return err
	}
	if j.f == nil {
		f, err := os.OpenFile(j.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		j.f = f
	}
	if _, err := j.f.Write(b); err != nil {
		return err
	}
	return j.f.Sync()
}

// Rewrite implements Journal.
func (j *FileJournal) Rewrite(lines []Line) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	tmp := j.Path + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	var chunk bytes.Buffer
	write := func() error {
		_, err := f.Write(chunk.Bytes())
		chunk.Reset()
		return err
	}
	for i := range lines {
		b, err := lines[i].encode()
		if err != nil {
			_ = f.Close()
			return err
		}
		chunk.Write(b)
		if chunk.Len() > 1<<20 {
			if err := write(); err != nil {
				_ = f.Close()
				return err
			}
		}
	}
	if err := write(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if j.f != nil {
		_ = j.f.Close()
		j.f = nil
	}
	if err := os.Rename(tmp, j.Path); err != nil {
		return err
	}
	if d, err := os.Open(j.Dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Close closes the file.
func (j *FileJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}
	err := j.f.Close()
	j.f = nil
	return err
}
