package publish

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// saver writes publisher.json: one encode and one fsync for the caller's
// change and every other change made while the write before it ran (a
// group commit). At eight puts a second the state (2.6 MB of holds, trees
// and races) was encoded and synced once a put, under the lock every put
// takes: a third of the gateway's CPU went to the encoding and the puts
// queued on the lock (2026-11-01, the lab's ceiling of seven puts a
// second). The encode still runs under p.mu, once a batch; the write and
// the sync run with it released.
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
		raw, err := json.Marshal(p.st.compact())
		p.mu.Unlock()
		if err == nil {
			tmp := filepath.Join(p.cfg.Dir, "tmp")
			if err = os.MkdirAll(tmp, 0o700); err == nil {
				err = writeFileSync(tmp, filepath.Join(p.cfg.Dir, "publisher.json"), raw)
			}
		}
		p.sv.mu.Lock()
		p.sv.saved = gen
		if err == nil {
			p.sv.ok = gen
		}
		p.sv.err = err
		p.sv.cond.Broadcast()
		p.sv.mu.Unlock()
	}
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
