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

	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/chaincfg"
	"github.com/lightwebinc/bstore/internal/acceptcfg"
	"github.com/lightwebinc/bstore/storerec"
)

// Config is a provider's configuration file, JSON. Every field has a
// default but Data, PublicURL and Buckets.
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
	// Chain is the chain view (proofs, spends, transactions):
	// nodeapi.ParseChain's woc:main, woc:test, asset:<url> or a list;
	// default woc:main or woc:test by Mainnet. Node, the older key, a
	// node's asset API, is chain asset:<url>. Settle is the settlement leg,
	// arcade:main, arcade:test, arcade:<url>, arc:<url>, rpc:<url> or
	// tcp:<host:port> (default GorillaPool's arcade), with RPCUser, RPCPass
	// and ArcadeKey as it needs; Headers a header source (bcommon headers:
	// woc:main, bhs:<url>, arcade:<url>, chaintracks:<url> or a bridge
	// URL; default woc:main or woc:test), HeadersToken its bearer token.
	// WoCKey and WoCRate are a WhatsOnChain API key and its requests a
	// second: a busy provider exceeds the free 3 a second and needs a key,
	// or a node. On a regtest chain set chain, settle and headers.
	Chain        string  `json:"chain,omitempty"`
	Node         string  `json:"node,omitempty"`
	Settle       string  `json:"settle,omitempty"`
	RPCUser      string  `json:"rpc_user,omitempty"`
	RPCPass      string  `json:"rpc_pass,omitempty"`
	ArcadeKey    string  `json:"arcade_key,omitempty"`
	Headers      string  `json:"headers,omitempty"`
	HeadersToken string  `json:"headers_token,omitempty"`
	WoCKey       string  `json:"woc_key,omitempty"`
	WoCRate      float64 `json:"woc_rate,omitempty"`
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
	// renewals spend; an advertisement pays all of its output in fee, so
	// TreeSats is the most one costs. Fee is the miner fee policy (feepolicy.Config;
	// default 100 satoshis per 1000 bytes, floor 100, capped at 100/1000); FeeRate and FeeFloor
	// are its older keys fee_sat_per_byte and fee_floor, still read.
	TreeCount int               `json:"tree_count"`
	TreeSats  uint64            `json:"tree_sats"`
	Fee       *feepolicy.Config `json:"fee,omitempty"`
	FeeRate   uint64            `json:"fee_sat_per_byte,omitempty"`
	FeeFloor  uint64            `json:"fee_floor,omitempty"`
	// Mainnet selects mainnet addresses for the fund address, and the
	// mainnet services by default.
	Mainnet bool `json:"mainnet"`
	// RefuseUnmined makes fund -beef take mined payments only.
	RefuseUnmined bool `json:"refuse_unmined,omitempty"`
	// TrustedProxies are the addresses (CIDR) of proxies in front of the
	// provider whose X-Forwarded-For names the client: every per-address
	// budget is the client's, not the proxy's.
	TrustedProxies []string `json:"trusted_proxies,omitempty"`
	// Feeders are the addresses (CIDR) of the plane's local feeders: the
	// overlay bridge that hands this provider what the plane delivers on its
	// topics. A submission whose connection comes from one skips the
	// per-address submission budgets, which a feed at plane speed would
	// otherwise trip and lose pieces to; the global bounds still apply.
	Feeders []string `json:"feeders,omitempty"`
	// MetricsListen, when set, serves /metrics there, and only there.
	MetricsListen string `json:"metrics_listen,omitempty"`
	// PprofListen, when set, serves Go's net/http/pprof there (a loopback
	// address: the profiles are the process's internals).
	PprofListen string `json:"pprof_listen,omitempty"`
	// FeedListen, when set, is a loopback address where the plane's local
	// feeder (an overlay bridge on the same host) submits: POST /submit and
	// /healthz only, plain HTTP, no BRC-104, and no per-address budgets, the
	// way a bridge reaches its engine. Every other route stays on Listen.
	FeedListen string `json:"feed_listen,omitempty"`
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

// maxVerifyWorkers bounds limits.verify_workers: each verification reads
// one 4 MiB blob at a time and its walk keeps at most 4 MiB ahead. The
// same workers decide: a hold is a few chain reads and a journal sync,
// so more workers hold more small entries a second.
const maxVerifyWorkers = 64

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
	// VerifyWorkers bounds the roots seen through at once for entries:
	// each verified and then decided (held, advertised) by one worker.
	VerifyWorkers int `json:"verify_workers"`
	// AdSends bounds the advertisements on their way to the settlement leg
	// at once, behind the workers.
	AdSends int `json:"ad_sends"`
	// AdBatch is the most roots one advertisement transaction carries (an
	// output each, spec section 12.1 admits up to 1,024 a transaction), and
	// AdBatchMs how long the minter collects roots behind the first before
	// it signs; 1 mints one a transaction, as before.
	AdBatch   int `json:"ad_batch"`
	AdBatchMs int `json:"ad_batch_ms"`
	// AdVerdict is what an advertisement's send waits for on an arcade leg:
	// "proof" (the default) returns once arcade has taken the transaction,
	// the Merkle proof the background watches for being the evidence;
	// "network" waits for the network's verdict first, which bounds the
	// rate at what the network announces (about 150 a second on one node).
	AdVerdict string `json:"ad_verdict"`
	// RenewalsPerRootPerHour bounds paid renewals of one root.
	RenewalsPerRootPerHour float64 `json:"renewals_per_root_per_hour"`
	// SubmitsPerAddrPerSec and SubmitBurst bound submissions from one
	// address (the plane's listener, or a writer's unicast).
	SubmitsPerAddrPerSec float64 `json:"submits_per_addr_per_sec"`
	SubmitBurst          float64 `json:"submit_burst"`
	// InFlightPerAddr bounds the submissions and uploads one address has
	// read or processed at once; one past it waits up to inflightWait for
	// a slot before a 429 (a feeder skips it, as it skips the rate).
	InFlightPerAddr int `json:"in_flight_per_addr"`
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
	// The chain keys take the zero-infrastructure profile here as well as
	// in chaincfg, so a configuration past Check names what it uses and a
	// module that embeds Config reads woc:<network> and arcade:<network>
	// from it (a set node key stays the chain view, asset:<node>). Mainnet
	// or testnet, so none of these can fail.
	s := c.Spec()
	if c.Chain == "" && c.Node == "" {
		c.Chain, _ = s.ChainSpec()
	}
	if c.Settle == "" {
		c.Settle, _ = s.SettleSpec()
	}
	if c.Headers == "" {
		c.Headers, _ = s.HeadersSpec()
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8095"
	}
	if c.Backend == "" {
		c.Backend = "native"
	}
	if c.QuotaBytes == 0 {
		c.QuotaBytes = 128 << 30
	}
	if c.UnclaimedBytes == 0 {
		c.UnclaimedBytes = c.QuotaBytes / 8
	}
	if c.UnclaimedSeconds == 0 {
		c.UnclaimedSeconds = 3600
	}
	p := &c.Policy
	if p.MaxObject == 0 {
		p.MaxObject = 4 << 30
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
		t.RenewPerGiBDay = 250
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
	def(&l.SubmitsPerAddrPerSec, 1024)
	def(&l.SubmitBurst, 4096)
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
	if l.InFlightPerAddr == 0 {
		l.InFlightPerAddr = 32
	}
	if l.VerifyWorkers == 0 {
		l.VerifyWorkers = 4
	}
	if l.AdSends == 0 {
		l.AdSends = 64
	}
	if l.AdBatch == 0 {
		l.AdBatch = 64
	}
	if l.AdBatchMs == 0 {
		l.AdBatchMs = 20
	}
	if l.AdVerdict == "" {
		l.AdVerdict = "proof"
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
		c.TreeCount = 1023
	}
	if c.TreeSats == 0 {
		c.TreeSats = 200
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
	if err := c.Spec().Check(); err != nil {
		return err
	}
	fees, err := c.Spec().Fees()
	if err != nil {
		return err
	}
	floor := fees.Floor
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
	if c.Limits.VerifyWorkers < 1 || c.Limits.VerifyWorkers > maxVerifyWorkers {
		return fmt.Errorf("limits.verify_workers is 1 to %d", maxVerifyWorkers)
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
	if t.RenewMinimum < floor {
		return errors.New("terms.renew_minimum must cover a renewal's fee: at least the fee floor")
	}
	if t.MinBounty < floor {
		return errors.New("terms.min_bounty must cover an advertisement's fee: at least the fee floor")
	}
	for _, cidr := range c.TrustedProxies {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("trusted_proxies: %q is not a CIDR", cidr)
		}
	}
	for _, cidr := range c.Feeders {
		if _, _, err := net.ParseCIDR(cidr); err != nil {
			return fmt.Errorf("feeders: %q is not a CIDR", cidr)
		}
	}
	if c.FeedListen != "" {
		host, _, err := net.SplitHostPort(c.FeedListen)
		if err != nil {
			return fmt.Errorf("feed_listen: %w", err)
		}
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("feed_listen: %q is not a loopback address: submissions there skip the per-address budgets (name a remote bridge in feeders instead)", c.FeedListen)
		}
	}
	if c.Limits.InFlightPerAddr < 1 {
		return errors.New("limits.in_flight_per_addr is at least 1")
	}
	if c.Limits.AdSends < 1 || c.Limits.AdSends > 1024 {
		return errors.New("limits.ad_sends is 1 to 1024")
	}
	if c.Limits.AdBatch < 1 || c.Limits.AdBatch > storerec.MaxAdsPerTx {
		return fmt.Errorf("limits.ad_batch is 1 to %d", storerec.MaxAdsPerTx)
	}
	if c.Limits.AdBatchMs < 1 || c.Limits.AdBatchMs > 10000 {
		return errors.New("limits.ad_batch_ms is 1 to 10000")
	}
	if c.Limits.AdVerdict != "proof" && c.Limits.AdVerdict != "network" {
		return errors.New(`limits.ad_verdict is "proof" or "network"`)
	}
	if c.TreeCount < 1 || c.TreeCount > 1023 {
		return errors.New("tree_count is 1 to 1023")
	}
	if c.TreeSats < floor*2 {
		return errors.New("tree_sats must cover an advertisement's fee: at least twice the fee floor")
	}
	return nil
}

// Spec is the chain, settlement and fee settings, for chaincfg.
func (c *Config) Spec() chaincfg.Spec {
	n := "test"
	if c.Mainnet {
		n = "main"
	}
	return chaincfg.Spec{Network: n, Chain: c.Chain, Node: c.Node, Settle: c.Settle, ArcadeKey: c.ArcadeKey, RPCUser: c.RPCUser,
		RPCPass: c.RPCPass, RPCID: "bstore-provider", Headers: c.Headers, HeadersToken: c.HeadersToken, WoCKey: c.WoCKey,
		WoCRate: c.WoCRate, Fee: c.Fee, FeeRate: c.FeeRate, FeeFloor: c.FeeFloor}
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
