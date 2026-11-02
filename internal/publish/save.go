package publish

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// saver writes the state: one encode and one fsync of the hot part for the
// caller's change and every other change made while the write before it
// ran (a group commit), and a cold file for each transaction that is new
// or has mined since the last save. At eight puts a second the state (2.6
// MB of holds, trees and races) was encoded and synced once a put, under
// the lock every put takes: a third of the gateway's CPU went to the
// encoding and the puts queued on the lock (2026-11-01, the lab's ceiling
// of seven puts a second); at 25 puts a second, with 4,096 holds ahead in
// 1,024-output transactions, two thirds, most of it the hex strings and
// the garbage they made. The encode still runs under p.mu, once a batch,
// into buffers reused; the writes and the syncs run with it released.
type saver struct {
	once sync.Once
	mu   sync.Mutex
	cond *sync.Cond
	// dirty is the generation of the latest change, saved that of the last
	// write attempted, ok that of the last write that succeeded; err is the
	// last write's error.
	dirty, saved, ok uint64
	err              error
	kick             chan struct{}
	done             chan struct{}
	closed           bool
	// hot and rows are the encode buffers, refs the transactions the state
	// named at the last encode: the saver's own, reused, since it writes
	// what it encoded before it encodes again.
	hot  bytes.Buffer
	rows []byte
	refs map[string]txRecord
	// cold is each transaction kept under txs/, by txid, as last written or
	// read: what a save compares the state against. The loader fills it
	// before the saver starts; the saver's own after.
	cold map[string]coldKey
	// hotBytes and coldBytes are what the last write that succeeded wrote,
	// under mu, for tests.
	hotBytes, coldBytes int
}

// errSaverClosed is a save asked for after Close.
var errSaverClosed = errors.New("publish: closed; the state is not written")

func (s *saver) start(loop func()) {
	s.once.Do(func() {
		s.cond = sync.NewCond(&s.mu)
		s.kick = make(chan struct{}, 1)
		s.done = make(chan struct{})
		go loop()
	})
}

// saveLocked makes the state durable: the caller's change, with every other
// change made while the write before it was running, is one encode and one
// fsync. p.mu is released while the write runs and held again on return,
// so the caller reads nothing of the state after this call that it did not
// copy before it.
func (p *Publisher) saveLocked() error {
	gen := p.markDirtyLocked()
	p.mu.Unlock()
	err := p.awaitSave(gen)
	p.mu.Lock()
	return err
}

// markDirtyLocked notes a change of the state and returns its generation,
// for awaitSave. The caller holds p.mu.
func (p *Publisher) markDirtyLocked() uint64 {
	p.sv.start(p.saveLoop)
	p.sv.mu.Lock()
	defer p.sv.mu.Unlock()
	p.sv.dirty++
	if !p.sv.closed {
		select {
		case p.sv.kick <- struct{}{}:
		default:
		}
	}
	return p.sv.dirty
}

// awaitSave returns once a write covering generation gen has completed,
// with that write's error.
func (p *Publisher) awaitSave(gen uint64) error {
	p.sv.mu.Lock()
	defer p.sv.mu.Unlock()
	for p.sv.saved < gen && !p.sv.closed {
		p.sv.cond.Wait()
	}
	if p.sv.saved < gen {
		return errSaverClosed
	}
	if p.sv.ok < gen {
		return p.sv.err
	}
	return nil
}

// saveLoop is the saver: a write per kick, covering every change made
// before its encode.
func (p *Publisher) saveLoop() {
	defer close(p.sv.done)
	for range p.sv.kick {
		p.mu.Lock()
		p.sv.mu.Lock()
		gen := p.sv.dirty
		p.sv.mu.Unlock()
		hot, err := p.sv.encode(p.st)
		p.mu.Unlock()
		var cold int
		if err == nil {
			cold, err = p.sv.write(p.cfg.Dir, hot)
		}
		p.sv.mu.Lock()
		p.sv.saved = gen
		if err == nil {
			p.sv.ok = gen
			p.sv.hotBytes, p.sv.coldBytes = len(hot), cold
		}
		p.sv.err = err
		p.sv.cond.Broadcast()
		p.sv.mu.Unlock()
	}
}

// encode is the hot part's bytes for st, valid until the next encode, and
// notes in s.refs every transaction st keeps. The caller holds p.mu.
func (s *saver) encode(st *state) ([]byte, error) {
	if s.refs == nil {
		s.refs = map[string]txRecord{}
	}
	clear(s.refs)
	rows, err := st.refs(s.rows[:0], s.refs)
	if err != nil {
		return nil, err
	}
	s.rows = rows
	s.hot.Reset()
	if err := json.NewEncoder(&s.hot).Encode(st.hot(rows)); err != nil {
		return nil, err
	}
	return s.hot.Bytes(), nil
}

// write makes the encode durable: first a cold file for each transaction
// in s.refs that is new or changed since it was last written or read, then
// the hot part, each synced under a temp-file rename, then the cold files
// of the transactions the state no longer names are removed. It returns
// the cold bytes written. p.mu is not held.
func (s *saver) write(dir string, hot []byte) (int, error) {
	tmp := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return 0, err
	}
	if s.cold == nil {
		s.cold = map[string]coldKey{}
	}
	n, made := 0, false
	for txid, r := range s.refs {
		k := r.key()
		if have, ok := s.cold[txid]; ok && have == k {
			continue
		}
		path, err := coldPath(dir, txid)
		if err != nil {
			return n, err
		}
		b, err := json.Marshal(r)
		if err != nil {
			return n, err
		}
		if !made {
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return n, err
			}
			made = true
		}
		if err := writeFileSync(tmp, path, b); err != nil {
			return n, err
		}
		s.cold[txid] = k
		n += len(b)
	}
	if err := writeFileSync(tmp, filepath.Join(dir, "publisher.json"), hot); err != nil {
		return n, err
	}
	for txid := range s.cold {
		if _, named := s.refs[txid]; named {
			continue
		}
		path, err := coldPath(dir, txid)
		if err == nil {
			err = os.Remove(path)
		}
		if err == nil || errors.Is(err, os.ErrNotExist) {
			delete(s.cold, txid)
		}
	}
	return n, nil
}

// flushSaves writes any change not yet written and stops the saver; for
// Close.
func (p *Publisher) flushSaves() {
	p.sv.mu.Lock()
	started := p.sv.kick != nil
	gen := p.sv.dirty
	p.sv.mu.Unlock()
	if !started {
		return
	}
	_ = p.awaitSave(gen)
	p.sv.mu.Lock()
	p.sv.closed = true
	close(p.sv.kick)
	p.sv.cond.Broadcast()
	p.sv.mu.Unlock()
	<-p.sv.done
}
