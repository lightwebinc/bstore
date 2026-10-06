package provider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/internal/acceptcfg"
	"github.com/lightwebinc/bstore/storerec"
)

// Config is a provider's configuration file, JSON. Every field has a
// default but Data, PublicURL, Node, Settle, Headers and Buckets.
type Config struct {
	// Data is the data directory: the wallet, the state, the store, the
	// payment ledger. Absolute.
	Data string `json:"data"`
	// Listen is the address the routes are served on; TLSCert and TLSKey,
	// when both set, serve them over TLS (otherwise a TLS proxy in front
	// terminates it).
	Listen  string `json:"listen"`
	TLSCert string `json:"tls_cert,omitempty"`
	TLSKey  string `json:"tls_key,omitempty"`
	// PublicURL is the https URL the routes are reached at from outside:
	// objects at <PublicURL>/chirp/v1/..., the front at
	// <PublicURL>/bstore/v1 (spec section 10.1). No trailing slash, no
	// query, no fragment, no user information.
	PublicURL string `json:"public_url"`
	// Node is a Teranode asset API (proofs, spends, block hashes); Settle
	// the settlement leg, tcp:<host:port>, rpc:<url> or arcade:<url>, with
	// RPCUser, RPCPass and ArcadeKey as it needs; Headers a header source
	// (bcommon headers: woc:main, chaintracks:<url> or a bridge URL).
	Node      string `json:"node"`
	Settle    string `json:"settle"`
	RPCUser   string `json:"rpc_user,omitempty"`
	RPCPass   string `json:"rpc_pass,omitempty"`
	ArcadeKey string `json:"arcade_key,omitempty"`
	Headers   string `json:"headers"`
	// Hosts are the overlay hosts advertisements are submitted to, each
	// directly; Facade, when set, is a plane facade they are submitted to
	// once as well. UHRPTopic also submits each to tm_uhrp, as BRC-26 has
	// it, where a host carries that topic.
	Hosts     []string `json:"hosts"`
	Facade    string   `json:"facade,omitempty"`
	UHRPTopic bool     `json:"uhrp_topic"`
	// Buckets are the buckets the provider takes objects for.
	Buckets []BucketConfig `json:"buckets"`
	// Backend selects the hold backend: "native" (the content-addressed
	// store under Data/store), or "bridge" when a bridge runs the provider
	// as a front for another advertiser and supplies the backend (Front).
	// Export, when set, keeps a read-only view of every held closure there
	// for an NFS export (docs/provider.md); a bridge keeps no closure.
	Backend string `json:"backend"`
	Export  string `json:"export,omitempty"`
	// QuotaBytes bounds the store; UnclaimedBytes the part of it pieces no
	// entry has claimed yet may take, and UnclaimedSeconds how long such a
	// piece is kept.
	QuotaBytes       uint64 `json:"quota_bytes"`
	UnclaimedBytes   uint64 `json:"unclaimed_bytes"`
	UnclaimedSeconds uint64 `json:"unclaimed_seconds"`
	// Policy is what the provider takes, Terms what it publishes and
	// charges, Limits its routes' hardening.
	Policy Policy `json:"policy"`
	Terms  Terms  `json:"terms"`
	Limits Limits `json:"limits"`
	// TreeCount and TreeSats size the funding trees advertisements and
	// renewals spend; FeeRate and FeeFloor are the fee policy.
	TreeCount int    `json:"tree_count"`
	TreeSats  uint64 `json:"tree_sats"`
	FeeRate   uint64 `json:"fee_sat_per_byte"`
	FeeFloor  uint64 `json:"fee_floor"`
	// Mainnet selects mainnet addresses for the fund address.
	Mainnet bool `json:"mainnet"`
	// TrustedProxies are the addresses (CIDR) of proxies in front of the
	// provider whose X-Forwarded-For names the client: every per-address
	// budget is the client's, not the proxy's.
	TrustedProxies []string `json:"trusted_proxies,omitempty"`
	// MetricsListen, when set, serves /metrics there, and only there.
	MetricsListen string `json:"metrics_listen,omitempty"`
	// Accept is the payment acceptance rule (accept_threshold_sats and the
	// other accept_ keys, docs/provider.md).
	acceptcfg.Config
}

// BucketConfig is one bucket the provider subscribes to.
type BucketConfig struct {
	Bucket string `json:"bucket"`
	// Writers, when set, are the only writers whose pieces and entries are
	// taken, 66 hex digits each.
	Writers []string `json:"writers,omitempty"`
}

// Policy is the provider's hold policy (spec section 9.3; not frozen).
type Policy struct {
	// MaxObject is the largest object taken, bytes.
	MaxObject uint64 `json:"max_object"`
	// MinDeadlineBlocks is how far ahead of the tip an entry's deadline
	// must be for its bounty to be raced for.
	MinDeadlineBlocks uint64 `json:"min_deadline_blocks"`
	// MaxRetentionSeconds is the longest an entry may ask an advertisement
	// to last from the entry's created time.
	MaxRetentionSeconds uint64 `json:"max_retention_seconds"`
	// HoldUnbountied takes an entry with no bounty, or one whose bounty
	// does not check out, for UnbountiedSeconds.
	HoldUnbountied    bool   `json:"hold_unbountied"`
	UnbountiedSeconds uint64 `json:"unbountied_seconds"`
	// AnswerInline answers challenges with the segments (the default); off,
	// every sample is by reference, fetched by the buyer from the
	// advertised URL.
	AnswerInline *bool `json:"answer_inline,omitempty"`
}

// Terms are the provider's published terms (spec section 10.1), and the
// limits of what it sells.
type Terms struct {
	ChallengeBase      uint64 `json:"challenge_base"`
	ChallengePerSample uint64 `json:"challenge_per_sample"`
	FreeC              uint64 `json:"free_c"`
	MinBounty          uint64 `json:"min_bounty"`
	MinAge             uint64 `json:"min_age"`
	RenewPerGiBDay     uint64 `json:"renew_per_gib_day"`
	RenewMinimum       uint64 `json:"renew_minimum"`
	// MaxRenewSeconds is the furthest ahead of now a renewal may run.
	MaxRenewSeconds uint64 `json:"max_renew_seconds"`
}

// Limits are the routes' hardening; zero takes the default.
type Limits struct {
	HandshakesPerSec        float64 `json:"handshakes_per_sec"`
	HandshakeBurst          float64 `json:"handshake_burst"`
	HandshakesPerAddrPerSec float64 `json:"handshakes_per_addr_per_sec"`
	HandshakeAddrBurst      float64 `json:"handshake_addr_burst"`
	ResponsesPerSec         float64 `json:"responses_per_sec"`
	ResponseBurst           float64 `json:"response_burst"`
	Sessions                int     `json:"sessions"`
	SessionTTLSeconds       int     `json:"session_ttl_seconds"`
	// ChallengesPerBuyerPerMin and ChallengeBurst bound the challenges one
	// buyer (identity key) is answered; FreePerWriterPerMin the free
	// settlement challenges one writer is.
	ChallengesPerBuyerPerMin float64 `json:"challenges_per_buyer_per_min"`
	ChallengeBurst           float64 `json:"challenge_burst"`
	FreePerWriterPerMin      float64 `json:"free_per_writer_per_min"`
	// ChallengeWorkers bounds the challenges answered at once.
	ChallengeWorkers int `json:"challenge_workers"`
	// RenewalsPerRootPerHour bounds paid renewals of one root.
	RenewalsPerRootPerHour float64 `json:"renewals_per_root_per_hour"`
	// SubmitsPerAddrPerSec and SubmitBurst bound submissions from one
	// address (the plane's listener, or a writer's unicast).
	SubmitsPerAddrPerSec float64 `json:"submits_per_addr_per_sec"`
	SubmitBurst          float64 `json:"submit_burst"`
	// ObjectReaders bounds the object reads served at once;
	// ObjectReadsPerAddrPerSec and ObjectReadBurst one address's reads.
	ObjectReaders            int     `json:"object_readers"`
	ObjectReadsPerAddrPerSec float64 `json:"object_reads_per_addr_per_sec"`
	ObjectReadBurst          float64 `json:"object_read_burst"`
	// UploadSessions bounds the open upload sessions, and their life.
	UploadSessions       int    `json:"upload_sessions"`
	UploadSessionSeconds uint64 `json:"upload_session_seconds"`
}

func (c *Config) defaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8095"
	}
	if c.Backend == "" {
		c.Backend = "native"
	}
	if c.QuotaBytes == 0 {
		c.QuotaBytes = 64 << 30
	}
	if c.UnclaimedBytes == 0 {
		c.UnclaimedBytes = c.QuotaBytes / 8
	}
	if c.UnclaimedSeconds == 0 {
		c.UnclaimedSeconds = 3600
	}
	p := &c.Policy
	if p.MaxObject == 0 {
		p.MaxObject = 1 << 30
	}
	if p.MinDeadlineBlocks == 0 {
		p.MinDeadlineBlocks = 6
	}
	if p.MaxRetentionSeconds == 0 {
		p.MaxRetentionSeconds = 31 * 86400
	}
	if p.UnbountiedSeconds == 0 {
		p.UnbountiedSeconds = 86400
	}
	t := &c.Terms
	if t.ChallengeBase == 0 && t.ChallengePerSample == 0 {
		t.ChallengeBase, t.ChallengePerSample = 10, 1
	}
	if t.MinBounty == 0 {
		t.MinBounty = 1000
	}
	if t.RenewPerGiBDay == 0 {
		t.RenewPerGiBDay = 100
	}
	if t.RenewMinimum == 0 {
		t.RenewMinimum = 500
	}
	if t.FreeC == 0 {
		t.FreeC = 30
	}
	if t.MaxRenewSeconds == 0 {
		t.MaxRenewSeconds = 366 * 86400
	}
	l := &c.Limits
	def := func(v *float64, d float64) {
		if *v == 0 {
			*v = d
		}
	}
	def(&l.HandshakesPerSec, 32)
	def(&l.HandshakeBurst, 64)
	def(&l.HandshakesPerAddrPerSec, 1)
	def(&l.HandshakeAddrBurst, 4)
	def(&l.ResponsesPerSec, 5)
	def(&l.ResponseBurst, 20)
	def(&l.ChallengesPerBuyerPerMin, 6)
	def(&l.ChallengeBurst, 6)
	def(&l.FreePerWriterPerMin, 12)
	def(&l.RenewalsPerRootPerHour, 4)
	def(&l.SubmitsPerAddrPerSec, 64)
	def(&l.SubmitBurst, 256)
	def(&l.ObjectReadsPerAddrPerSec, 50)
	def(&l.ObjectReadBurst, 200)
	if l.Sessions == 0 {
		l.Sessions = 10000
	}
	if l.SessionTTLSeconds == 0 {
		l.SessionTTLSeconds = 600
	}
	if l.ChallengeWorkers == 0 {
		l.ChallengeWorkers = 4
	}
	if l.ObjectReaders == 0 {
		l.ObjectReaders = 32
	}
	if l.UploadSessions == 0 {
		l.UploadSessions = 64
	}
	if l.UploadSessionSeconds == 0 {
		l.UploadSessionSeconds = 3600
	}
	if c.TreeCount == 0 {
		c.TreeCount = 16
	}
	if c.TreeSats == 0 {
		c.TreeSats = 1000
	}
	if c.FeeRate == 0 {
		c.FeeRate = 1
	}
	if c.FeeFloor == 0 {
		c.FeeFloor = 250
	}
}

// LoadConfig reads a configuration file, takes the defaults and checks it.
// An unknown key is refused, so a misspelt one is not silently a default.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if err := c.Check(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &c, nil
}

// Check takes the defaults and refuses a configuration that cannot run.
func (c *Config) Check() error {
	c.defaults()
	if c.Data == "" || !filepath.IsAbs(c.Data) {
		return errors.New("data must be an absolute directory")
	}
	if err := checkPublicURL(c.PublicURL); err != nil {
		return err
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("tls_cert and tls_key go together")
	}
	if c.Node == "" || c.Settle == "" || c.Headers == "" {
		return errors.New("node, settle and headers are required")
	}
	if kind, _, _ := strings.Cut(c.Settle, ":"); kind != "tcp" && kind != "rpc" && kind != "arcade" {
		return errors.New("settle must be tcp:<host:port>, rpc:<url> or arcade:<url>")
	}
	for _, h := range append(append([]string(nil), c.Hosts...), c.Facade) {
		if h == "" {
			continue
		}
		if u, err := url.Parse(h); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("host %q is not an http or https URL", h)
		}
	}
	if len(c.Buckets) == 0 {
		return errors.New("buckets: at least one")
	}
	seen := map[string]bool{}
	for _, b := range c.Buckets {
		if err := storerec.CheckBucket(b.Bucket); err != nil {
			return fmt.Errorf("bucket %q: %w", b.Bucket, err)
		}
		if seen[b.Bucket] {
			return fmt.Errorf("bucket %q named twice", b.Bucket)
		}
		seen[b.Bucket] = true
		for _, w := range b.Writers {
			if _, err := guard.ParsePubKeyHex(w); err != nil {
				return fmt.Errorf("bucket %s: writer %q: %w", b.Bucket, w, err)
			}
		}
	}
	if c.Backend != "native" && c.Backend != "bridge" {
		return fmt.Errorf("backend %q: the backends are native and bridge", c.Backend)
	}
	if c.Backend == "bridge" && c.Export != "" {
		return errors.New("export: a bridge keeps no closure to export")
	}
	if c.Backend == "bridge" && c.Policy.AnswerInline != nil && *c.Policy.AnswerInline {
		return errors.New("policy.answer_inline: a front answers every sample by reference (spec section 7.5)")
	}
	if c.Export != "" && !filepath.IsAbs(c.Export) {
		return errors.New("export must be an absolute directory")
	}
	if c.UnclaimedBytes > c.QuotaBytes {
		return errors.New("unclaimed_bytes is above quota_bytes")
	}
	if c.UnclaimedBytes/4 < c.Policy.MaxObject {
		return errors.New("one writer may fill a quarter of unclaimed_bytes, which must hold policy.max_object: raise unclaimed_bytes or lower max_object")
	}
	if c.Policy.MaxObject > storerec.MaxSize {
		return errors.New("policy.max_object is above 2^43")
	}
	t := c.Terms
	if t.FreeC < 30 || t.FreeC > storerec.MaxC {
		return errors.New("terms.free_c is 30 to 1024 (spec section 9.4)")
	}
	if t.MinAge > 1008 {
		return errors.New("terms.min_age is 0 to 1008 blocks")
	}
	for _, v := range []uint64{t.ChallengeBase, t.ChallengePerSample, t.MinBounty, t.RenewPerGiBDay, t.RenewMinimum} {
		if v > storerec.MaxSafe {
			return errors.New("a price is above 2^53 - 1")
		}
	}
	if t.RenewMinimum < c.FeeFloor {
		return errors.New("terms.renew_minimum must cover a renewal's fee: at least fee_floor")
	}
	if t.MinBounty < c.FeeFloor {
		return errors.New("terms.min_bounty must cover an advertisement's fee: at least fee_floor")
	}
	for _, cidr := range c.TrustedProxies {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("trusted_proxies: %q is not a CIDR", cidr)
		}
	}
	if c.TreeCount < 1 || c.TreeCount > 1023 {
		return errors.New("tree_count is 1 to 1023")
	}
	if c.TreeSats < c.FeeFloor*2 {
		return errors.New("tree_sats must cover an advertisement's fee: at least twice fee_floor")
	}
	return nil
}

// checkPublicURL holds the public URL to what an advertisement's url and a
// derived front need: https, a host, no user information, query or
// fragment, no trailing slash, and nothing a CHIRP url could be mistaken
// for.
func checkPublicURL(s string) error {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		strings.HasSuffix(s, "/") || strings.Contains(s, "/chirp/v1/") || strings.ContainsAny(s, "?# ") {
		return fmt.Errorf("public_url %q must be https://host[/path] with no user, query, fragment or trailing slash", s)
	}
	return nil
}

// FrontURL is the front a provider's advertisement derives: the public URL
// followed by /bstore/v1.
func (c *Config) FrontURL() string { return c.PublicURL + storerec.FrontSuffix }

// answerInline reports whether challenges are answered with the segments.
func (c *Config) answerInline() bool {
	return c.Backend != "bridge" && (c.Policy.AnswerInline == nil || *c.Policy.AnswerInline)
}
