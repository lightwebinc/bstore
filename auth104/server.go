// Package auth104 is the server side of BRC-103 mutual authentication over
// HTTP (BRC-104), as a provider's routes need it: the handshake at
// /.well-known/auth, and each authenticated request checked and its answer
// signed, the way the SDK's clients (authhttp in Go, AuthFetch in
// TypeScript) send and check them. It holds the hardening every bstore route
// shares: a handshake budget per address and for the route, checked before
// any signature work; bounded sessions; a budget of signed answers per
// session, taken before a request is verified; and a request sent twice
// refused before any work. go-sdk carries the client and the Peer but no
// HTTP server, so this is that server, built on the SDK's own payload
// encoding (authpayload) and nonce rules.
package auth104

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/auth"
	"github.com/bsv-blockchain/go-sdk/auth/authpayload"
	"github.com/bsv-blockchain/go-sdk/auth/brc104"
	"github.com/bsv-blockchain/go-sdk/auth/utils"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"
)

// Wallet is what the server signs and checks with: the SDK's ProtoWallet,
// or any BRC-100 wallet.
type Wallet interface {
	wallet.KeyOperations
}

// Config is a server's settings. Zero fields take the defaults below.
type Config struct {
	Wallet Wallet
	// MaxSessions bounds the sessions kept, SessionTTL how long one idle is
	// kept, and MaxRemembered the request identifiers remembered across all
	// sessions (past it the least recently used session is forgotten whole).
	MaxSessions   int
	SessionTTL    time.Duration
	MaxRemembered int
	// Handshakes and PerAddress are the handshake budget; Responses the
	// signed answers one session is given.
	Handshakes Budget
	PerAddress Budget
	Responses  Budget
	// MaxHandshake bounds a handshake's body.
	MaxHandshake int64
	Now          func() time.Time
	// ClientAddr, when set, is the remote address a request is budgeted
	// under (a client behind a trusted proxy); RemoteAddr otherwise.
	ClientAddr func(*http.Request) string
	// Count, when set, is told each outcome: "handshake" with accepted,
	// failed, limited_address or limited_global; "request" with replayed,
	// limited, failed or accepted; "session_evicted" with cap, idle or
	// requests.
	Count func(what, result string)
}

// The defaults, as the bstore host's terms route has them.
var (
	DefaultHandshakes = Budget{Rate: 4, Burst: 8}
	DefaultPerAddress = Budget{Rate: 1, Burst: 4}
	DefaultResponses  = Budget{Rate: 5, Burst: 20}
)

const (
	DefaultMaxSessions   = 10000
	DefaultSessionTTL    = 10 * time.Minute
	DefaultMaxRemembered = 1 << 18
	DefaultMaxHandshake  = 16 << 10
)

// Server answers BRC-104 handshakes and authenticated requests.
type Server struct {
	c        Config
	identity *ec.PublicKey
	sess     *sessions
	shakes   *Handshakes
	answers  *Keyed
}

// New is a server over cfg.
func New(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Wallet == nil {
		return nil, errors.New("auth104: no wallet")
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = DefaultMaxSessions
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = DefaultSessionTTL
	}
	if cfg.MaxRemembered == 0 {
		cfg.MaxRemembered = DefaultMaxRemembered
	}
	if cfg.Handshakes == (Budget{}) {
		cfg.Handshakes = DefaultHandshakes
	}
	if cfg.PerAddress == (Budget{}) {
		cfg.PerAddress = DefaultPerAddress
	}
	if cfg.Responses == (Budget{}) {
		cfg.Responses = DefaultResponses
	}
	if cfg.MaxHandshake == 0 {
		cfg.MaxHandshake = DefaultMaxHandshake
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.ClientAddr == nil {
		cfg.ClientAddr = func(r *http.Request) string { return r.RemoteAddr }
	}
	if cfg.Count == nil {
		cfg.Count = func(string, string) {}
	}
	if cfg.MaxSessions < 1 || cfg.MaxRemembered < 1 || cfg.SessionTTL <= 0 {
		return nil, errors.New("auth104: sessions, their life and the requests remembered must be positive")
	}
	id, err := cfg.Wallet.GetPublicKey(ctx, wallet.GetPublicKeyArgs{IdentityKey: true}, "bstore")
	if err != nil {
		return nil, fmt.Errorf("auth104: identity key: %w", err)
	}
	shakes, err := NewHandshakes(cfg.Handshakes, cfg.PerAddress, cfg.Now)
	if err != nil {
		return nil, err
	}
	answers, err := NewKeyed(cfg.Responses, cfg.MaxSessions, cfg.Now)
	if err != nil {
		return nil, err
	}
	s := &Server{c: cfg, identity: id.PublicKey, shakes: shakes, answers: answers}
	s.sess = newSessions(cfg.MaxSessions, cfg.MaxRemembered, cfg.SessionTTL, cfg.Now, func(why string) { cfg.Count("session_evicted", why) })
	return s, nil
}

// Identity is the server's identity key.
func (s *Server) Identity() *ec.PublicKey { return s.identity }

// Sessions is how many sessions are kept.
func (s *Server) Sessions() int { return s.sess.len() }

// HandshakePath is where the SDK's clients send a handshake: the origin's
// /.well-known/auth.
const HandshakePath = "/.well-known/auth"

// Reply is an answer before it is written, signed when its request was
// authenticated.
type Reply struct {
	Status  int
	Headers http.Header
	Body    []byte
}

// JSON is a reply of v as JSON.
func JSON(status int, v any, headers ...string) Reply {
	b, _ := json.Marshal(v)
	h := http.Header{"Content-Type": {"application/json"}}
	for i := 0; i+1 < len(headers); i += 2 {
		h.Set(headers[i], headers[i+1])
	}
	return Reply{Status: status, Headers: h, Body: b}
}

// Error is the JSON error reply a route answers.
func Error(status int, code, description string, headers ...string) Reply {
	return JSON(status, map[string]string{"status": "error", "code": code, "description": description}, headers...)
}

// Write writes r.
func Write(w http.ResponseWriter, r Reply) {
	for k, vs := range r.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(r.Body)))
	w.WriteHeader(r.Status)
	_, _ = w.Write(r.Body)
}

// Caller is who sent a request: the identity key BRC-103 authenticated,
// or nil for a request without authentication.
type Caller struct {
	Key *ec.PublicKey
	Hex string
}

// Handler answers one request whose body is already read and bounded.
type Handler func(ctx context.Context, r *http.Request, body []byte, who *Caller) Reply

// Handshake answers a POST to /.well-known/auth. The budget is taken before
// the body is read and before any signature work.
func (s *Server) Handshake(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		Write(w, Error(http.StatusMethodNotAllowed, "ERR_METHOD", "a handshake is a POST"))
		return
	}
	if ok, limit, retry := s.shakes.Take(s.c.ClientAddr(r)); !ok {
		s.c.Count("handshake", "limited_"+limit)
		Write(w, Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many handshakes; try again later", "Retry-After", fmt.Sprint(retry)))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.c.MaxHandshake))
	if err != nil {
		s.c.Count("handshake", "failed")
		Write(w, Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the handshake is too large"))
		return
	}
	reply, err := s.handshake(r.Context(), body)
	if err != nil {
		s.c.Count("handshake", "failed")
		Write(w, Error(http.StatusUnauthorized, "ERR_AUTH_FAILED", err.Error()))
		return
	}
	s.c.Count("handshake", "accepted")
	Write(w, reply)
}

func (s *Server) handshake(ctx context.Context, body []byte) (Reply, error) {
	var m auth.AuthMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return Reply{}, errors.New("the handshake is not an authentication message")
	}
	if m.Version != auth.AUTH_VERSION || m.MessageType != auth.MessageTypeInitialRequest || m.IdentityKey == nil {
		return Reply{}, errors.New("only an initial request of version 0.1 is accepted here")
	}
	theirs, err := base64.StdEncoding.DecodeString(m.InitialNonce)
	if err != nil || len(theirs) < 16 || len(theirs) > 256 {
		return Reply{}, errors.New("the initial nonce is not base64 of 16 to 256 bytes")
	}
	ours, err := utils.CreateNonce(ctx, s.c.Wallet, wallet.Counterparty{Type: wallet.CounterpartyTypeSelf})
	if err != nil {
		return Reply{}, err
	}
	oursRaw, err := base64.StdEncoding.DecodeString(ours)
	if err != nil {
		return Reply{}, err
	}
	sig, err := s.c.Wallet.CreateSignature(ctx, wallet.CreateSignatureArgs{
		EncryptionArgs: wallet.EncryptionArgs{
			ProtocolID:   wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryAppAndCounterparty, Protocol: auth.AUTH_PROTOCOL_ID},
			KeyID:        m.InitialNonce + " " + ours,
			Counterparty: wallet.Counterparty{Type: wallet.CounterpartyTypeOther, Counterparty: m.IdentityKey},
		},
		Data: append(append([]byte(nil), theirs...), oursRaw...),
	}, "bstore")
	if err != nil {
		return Reply{}, err
	}
	s.sess.add(ours, m.InitialNonce, m.IdentityKey)
	resp := &auth.AuthMessage{
		Version: auth.AUTH_VERSION, MessageType: auth.MessageTypeInitialResponse, IdentityKey: s.identity,
		Nonce: ours, YourNonce: m.InitialNonce, InitialNonce: ours, Signature: sig.Signature.Serialize(),
		RequestedCertificates: utils.RequestedCertificateSet{Certifiers: []*ec.PublicKey{}, CertificateTypes: utils.RequestedCertificateTypeIDAndFieldList{}},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return Reply{}, err
	}
	h := http.Header{"Content-Type": {"application/json"}}
	h.Set(brc104.HeaderVersion, resp.Version)
	h.Set(brc104.HeaderMessageType, string(resp.MessageType))
	h.Set(brc104.HeaderIdentityKey, hexKey(s.identity))
	h.Set(brc104.HeaderYourNonce, resp.YourNonce)
	h.Set(brc104.HeaderSignature, hex.EncodeToString(resp.Signature))
	return Reply{Status: http.StatusOK, Headers: h, Body: b}, nil
}

// Serve reads a request's body up to maxBody bytes and answers it with h:
// unauthenticated when it carries no BRC-104 headers (who is nil), and
// otherwise only once it verifies, with its answer signed. A request that
// carries some authentication headers and not others is refused.
func (s *Server) Serve(w http.ResponseWriter, r *http.Request, maxBody int64, h Handler) {
	if r.Header.Get(brc104.HeaderRequestID) == "" {
		for k := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), brc104.AuthHeaderPrefix) {
				Write(w, Error(http.StatusBadRequest, "ERR_AUTH_PARTIAL", "partial BRC-104 authentication headers"))
				return
			}
		}
		body, ok := readBody(w, r, maxBody)
		if !ok {
			return
		}
		Write(w, h(r.Context(), r, body, nil))
		return
	}
	s.authenticated(w, r, maxBody, h)
}

func readBody(w http.ResponseWriter, r *http.Request, maxBody int64) ([]byte, bool) {
	if r.ContentLength > maxBody {
		Write(w, Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the request body is too large"))
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		Write(w, Error(http.StatusRequestEntityTooLarge, "ERR_TOO_LARGE", "the request body is too large, or could not be read"))
		return nil, false
	}
	return body, true
}

func (s *Server) authenticated(w http.ResponseWriter, r *http.Request, maxBody int64, h Handler) {
	version := r.Header.Get(brc104.HeaderVersion)
	idHex := r.Header.Get(brc104.HeaderIdentityKey)
	nonce := r.Header.Get(brc104.HeaderNonce)
	yourNonce := r.Header.Get(brc104.HeaderYourNonce)
	sigHex := r.Header.Get(brc104.HeaderSignature)
	requestID := r.Header.Get(brc104.HeaderRequestID)
	rid, err := base64.StdEncoding.Strict().DecodeString(requestID)
	if version != auth.AUTH_VERSION || idHex == "" || nonce == "" || yourNonce == "" || sigHex == "" || err != nil || len(rid) != brc104.RequestIDLength {
		s.c.Count("request", "failed")
		Write(w, Error(http.StatusUnauthorized, "ERR_AUTH_FAILED", "the BRC-104 authentication headers are incomplete"))
		return
	}
	// A request answered before is refused before any work: sent again
	// byte for byte it would otherwise cost a signature each time and its
	// sender nothing.
	if seen, _ := s.sess.answered(yourNonce, requestID); seen {
		s.c.Count("request", "replayed")
		Write(w, Error(http.StatusUnauthorized, "ERR_AUTH_REPLAYED", "this request was already answered"))
		return
	}
	// The session's budget is taken before any verification, by every
	// request naming the session, so a stream of forged requests costs the
	// session its own budget and not the server its CPU.
	if ok, retry := s.answers.Take(yourNonce); !ok {
		s.c.Count("request", "limited")
		Write(w, Error(http.StatusTooManyRequests, "ERR_RATE_LIMITED", "too many requests on this session; try again later", "Retry-After", fmt.Sprint(retry)))
		return
	}
	body, ok := readBody(w, r, maxBody)
	if !ok {
		return
	}
	ss, err := s.verify(r, body, rid, idHex, nonce, yourNonce, sigHex)
	if err != nil {
		s.c.Count("request", "failed")
		Write(w, Error(http.StatusUnauthorized, "ERR_AUTH_FAILED", err.Error()))
		return
	}
	// Remembered only once it verified, so nobody fills the memory with
	// requests it cannot sign; a copy that verified at the same instant is
	// refused here.
	if !s.sess.take(yourNonce, requestID) {
		s.c.Count("request", "replayed")
		Write(w, Error(http.StatusUnauthorized, "ERR_AUTH_REPLAYED", "this request was already answered"))
		return
	}
	s.c.Count("request", "accepted")
	reply := h(r.Context(), r, body, &Caller{Key: ss.peer, Hex: ss.peerHex})
	signed, err := s.sign(r.Context(), ss, rid, requestID, reply)
	if err != nil {
		Write(w, Error(http.StatusInternalServerError, "ERR_INTERNAL", "the answer could not be signed"))
		return
	}
	Write(w, signed)
}

func (s *Server) verify(r *http.Request, body, rid []byte, idHex, nonce, yourNonce, sigHex string) (*session, error) {
	ss := s.sess.get(yourNonce)
	if ss == nil {
		return nil, errors.New("no such session; shake hands again")
	}
	if !strings.EqualFold(idHex, ss.peerHex) {
		return nil, errors.New("the identity key is not the session's")
	}
	if len(nonce) > 256 {
		return nil, errors.New("the request nonce is too long")
	}
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil || len(sigBytes) > 80 {
		return nil, errors.New("the signature is not hex")
	}
	sig, err := ec.ParseSignature(sigBytes)
	if err != nil {
		return nil, errors.New("the signature does not parse")
	}
	rr := r.Clone(r.Context())
	rr.Body = io.NopCloser(bytes.NewReader(body))
	payload, err := authpayload.FromHTTPRequest(rid, rr)
	if err != nil {
		return nil, fmt.Errorf("the request cannot be read as it was signed: %v", err)
	}
	res, err := s.c.Wallet.VerifySignature(r.Context(), wallet.VerifySignatureArgs{
		EncryptionArgs: wallet.EncryptionArgs{
			ProtocolID:   wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryAppAndCounterparty, Protocol: auth.AUTH_PROTOCOL_ID},
			KeyID:        nonce + " " + ss.nonce,
			Counterparty: wallet.Counterparty{Type: wallet.CounterpartyTypeOther, Counterparty: ss.peer},
		},
		Data:      payload,
		Signature: sig,
	}, "bstore")
	if err != nil || !res.Valid {
		return nil, errors.New("the request does not verify")
	}
	return ss, nil
}

func (s *Server) sign(ctx context.Context, ss *session, rid []byte, requestID string, reply Reply) (Reply, error) {
	if reply.Headers == nil {
		reply.Headers = http.Header{}
	}
	payload, err := authpayload.FromResponse(rid, authpayload.SimplifiedHttpResponse{StatusCode: reply.Status, Header: reply.Headers, Body: reply.Body})
	if err != nil {
		return Reply{}, err
	}
	nonce := string(utils.RandomBase64(32))
	sig, err := s.c.Wallet.CreateSignature(ctx, wallet.CreateSignatureArgs{
		EncryptionArgs: wallet.EncryptionArgs{
			ProtocolID:   wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryAppAndCounterparty, Protocol: auth.AUTH_PROTOCOL_ID},
			KeyID:        nonce + " " + ss.peerNonce,
			Counterparty: wallet.Counterparty{Type: wallet.CounterpartyTypeOther, Counterparty: ss.peer},
		},
		Data: payload,
	}, "bstore")
	if err != nil {
		return Reply{}, err
	}
	reply.Headers.Set(brc104.HeaderVersion, auth.AUTH_VERSION)
	reply.Headers.Set(brc104.HeaderIdentityKey, hexKey(s.identity))
	reply.Headers.Set(brc104.HeaderNonce, nonce)
	reply.Headers.Set(brc104.HeaderYourNonce, ss.peerNonce)
	reply.Headers.Set(brc104.HeaderSignature, hex.EncodeToString(sig.Signature.Serialize()))
	reply.Headers.Set(brc104.HeaderRequestID, requestID)
	return reply, nil
}

func hexKey(k *ec.PublicKey) string { return hex.EncodeToString(k.Compressed()) }
