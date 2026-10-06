package auth104

import (
	"container/list"
	"sync"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
)

// session is one BRC-104 session the server answered a handshake for.
type session struct {
	nonce     string // ours: the session nonce, the client's yourNonce
	peerNonce string // the client's initial nonce
	peer      *ec.PublicKey
	peerHex   string
	used      time.Time
	el        *list.Element
	// seen holds the request identifiers of every request this session
	// was answered, for as long as the session is kept: a request can only
	// verify within its own session, so remembering them per session and
	// forgetting a session whole refuses every replay exactly.
	seen map[string]bool
}

// sessions are the sessions the server keeps, bounded. A handshake is
// unauthenticated, so anyone could grow an unbounded store without end:
// this one keeps at most max sessions and forgets one idle for ttl; past
// the cap, and past the cap on remembered requests, it forgets the least
// recently used session whole. A client whose session was forgotten is
// refused (401) and shakes hands again.
type sessions struct {
	max     int
	maxSeen int
	ttl     time.Duration
	now     func() time.Time

	mu      sync.Mutex
	order   *list.List // *session, least recently used at the front
	byNonce map[string]*session
	seen    int // remembered request identifiers, all sessions
	evicted func(why string)
}

func newSessions(max, maxSeen int, ttl time.Duration, now func() time.Time, evicted func(string)) *sessions {
	if evicted == nil {
		evicted = func(string) {}
	}
	return &sessions{max: max, maxSeen: maxSeen, ttl: ttl, now: now, order: list.New(), byNonce: map[string]*session{}, evicted: evicted}
}

func (s *sessions) add(nonce, peerNonce string, peer *ec.PublicKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	if old, ok := s.byNonce[nonce]; ok {
		s.forgetLocked(old)
	}
	ss := &session{nonce: nonce, peerNonce: peerNonce, peer: peer, peerHex: hexKey(peer), used: s.now(), seen: map[string]bool{}}
	ss.el = s.order.PushBack(ss)
	s.byNonce[nonce] = ss
	for s.order.Len() > s.max {
		s.forgetLocked(s.order.Front().Value.(*session))
		s.evicted("cap")
	}
}

// get is the session of nonce, touched, or nil.
func (s *sessions) get(nonce string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	ss, ok := s.byNonce[nonce]
	if !ok {
		return nil
	}
	ss.used = s.now()
	s.order.MoveToBack(ss.el)
	return ss
}

// answered reports whether the session already took requestID, and
// whether the session is still kept.
func (s *sessions) answered(nonce, requestID string) (seen, kept bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.byNonce[nonce]
	if !ok {
		return false, false
	}
	return ss.seen[requestID], true
}

// take records requestID as taken by its session, once it verified; it
// reports false when it was taken before (two copies verified at once) or
// the session is gone.
func (s *sessions) take(nonce, requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.byNonce[nonce]
	if !ok || ss.seen[requestID] {
		return false
	}
	ss.seen[requestID] = true
	s.seen++
	for s.seen > s.maxSeen && s.order.Len() > 0 {
		front := s.order.Front().Value.(*session)
		s.forgetLocked(front)
		s.evicted("requests")
		if front == ss {
			return false
		}
	}
	return true
}

func (s *sessions) forgetLocked(ss *session) {
	s.order.Remove(ss.el)
	delete(s.byNonce, ss.nonce)
	s.seen -= len(ss.seen)
}

func (s *sessions) expireLocked() {
	cutoff := s.now().Add(-s.ttl)
	for e := s.order.Front(); e != nil; e = s.order.Front() {
		ss := e.Value.(*session)
		if ss.used.After(cutoff) {
			return
		}
		s.forgetLocked(ss)
		s.evicted("idle")
	}
}

func (s *sessions) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}
