package reader

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/lightwebinc/bcommon/lookup"
)

// A walk's progress kept on disk (WalkDir): every token a walk verifies is
// appended, as the host answered it, to a file of the chain's, and a walk
// cut short by a restart finds the tokens it already had there, verifies
// each again against the reader's own headers (no host is asked), and
// goes on from the deepest one. The file goes once the walk completes and
// the token is held. Nothing in it is trusted: a token is used only when
// it verifies and is the one the walk asks for (its txid and seq).

// maxWalkFile bounds one chain's file; past it the walk is not recorded
// further (a restart then asks hosts for the rest again).
const maxWalkFile = 64 << 20

type walkFiles struct {
	mu   sync.Mutex
	open map[string]map[[32]byte]lookup.Output // chain key -> txid -> output, loaded
}

func (r *Reader) walkPath(c Chain) string {
	h := sha256.Sum256([]byte(c.key()))
	return filepath.Join(r.WalkDir, hex.EncodeToString(h[:16])+".jsonl")
}

// walkLoad reads a chain's file once per process.
func (r *Reader) walkLoad(c Chain) map[[32]byte]lookup.Output {
	r.wf.mu.Lock()
	defer r.wf.mu.Unlock()
	if r.wf.open == nil {
		r.wf.open = map[string]map[[32]byte]lookup.Output{}
	}
	if m, ok := r.wf.open[c.key()]; ok {
		return m
	}
	m := map[[32]byte]lookup.Output{}
	if f, err := os.Open(r.walkPath(c)); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
		for sc.Scan() {
			var rec struct {
				Txid string        `json:"txid"`
				Out  lookup.Output `json:"out"`
			}
			if json.Unmarshal(sc.Bytes(), &rec) != nil {
				continue
			}
			if b, err := hex.DecodeString(rec.Txid); err == nil && len(b) == 32 {
				m[[32]byte(b)] = rec.Out
			}
		}
		f.Close()
	}
	r.wf.open[c.key()] = m
	return m
}

// walkSaved is a token the chain's file holds as want at seq, verified
// again, or nil.
func (r *Reader) walkSaved(ctx context.Context, c Chain, seq uint64, want [32]byte) *Token {
	if r.WalkDir == "" {
		return nil
	}
	o, ok := r.walkLoad(c)[want]
	if !ok {
		return nil
	}
	t, err := r.verifyToken(ctx, c, o)
	if err != nil || t.Txid != want || t.Bucket.Seq != seq {
		return nil
	}
	return t
}

// walkKeep appends a verified token's output to the chain's file.
func (r *Reader) walkKeep(c Chain, txid [32]byte, o lookup.Output) {
	if r.WalkDir == "" {
		return
	}
	m := r.walkLoad(c)
	r.wf.mu.Lock()
	defer r.wf.mu.Unlock()
	if _, ok := m[txid]; ok {
		return
	}
	p := r.walkPath(c)
	if fi, err := os.Stat(p); err == nil && fi.Size() > maxWalkFile {
		return
	}
	line, err := json.Marshal(map[string]any{"txid": hex.EncodeToString(txid[:]), "out": o})
	if err != nil {
		return
	}
	if err := os.MkdirAll(r.WalkDir, 0o700); err != nil {
		return
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = f.Write(append(line, '\n'))
	f.Close()
	m[txid] = o
}

// walkDone removes a chain's file once its walk is held.
func (r *Reader) walkDone(c Chain) {
	if r.WalkDir == "" {
		return
	}
	r.wf.mu.Lock()
	defer r.wf.mu.Unlock()
	delete(r.wf.open, c.key())
	_ = os.Remove(r.walkPath(c))
}
