package s3d

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/chaincfg"
	"github.com/lightwebinc/bstore/internal/acceptcfg"
	"github.com/lightwebinc/bstore/internal/publish"

	"github.com/lightwebinc/bstore/storerec"
)

// Config is the gateway's configuration file, JSON; an unknown key is
// refused. Every field has a default but Data, Hosts, Identities,
// AccessKeys and Buckets.
type Config struct {
	// Data is the data directory: identities/<name>/ (each a wallet, its
	// funding trees and its outbox), store/ (the local closures), uploads/,
	// state.json. Absolute.
	Data string `json:"data"`
	// Listen is the address S3 is served on; TLSCert and TLSKey, when both
	// set, serve it over TLS (otherwise a proxy in front terminates it).
	Listen  string `json:"listen"`
	TLSCert string `json:"tls_cert,omitempty"`
	TLSKey  string `json:"tls_key,omitempty"`
	// ETagMD5 makes ETags S3's MD5s (a part's MD5; the MD5 of the parts'
	// MD5s for a multipart upload), at the cost of an MD5 over every body.
	// Off, an object's ETag is its content hash (its SHA-256, what
	// x-amz-checksum-sha256 carries) and a part's its SHA-256.
	ETagMD5 bool `json:"etag_md5,omitempty"`
	// Region is the SigV4 region clients sign for; Domain, when set, also
	// serves virtual-host addressing (<bucket>.<domain>).
	Region string `json:"region"`
	Domain string `json:"domain,omitempty"`
	// Chain is the chain view (nodeapi.ParseChain: woc:main, woc:test,
	// asset:URL; default woc:main or woc:test by Mainnet); Node, the older
	// key, a node's asset API, is chain asset:URL. Settle is the settlement
	// leg (publish.ParseSettler: arcade:main, arcade:URL, arc:URL, rpc:URL,
	// tcp:ADDR; default GorillaPool's arcade), with RPCUser, RPCPass,
	// ArcadeKey; Headers a header source (bcommon headers; default
	// woc:main or woc:test), HeadersToken its bearer token. WoCKey and
	// WoCRate are a WhatsOnChain API key and its requests a second: a busy
	// gateway needs one, or a node. On a regtest chain set chain, settle
	// and headers.
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
	// Hosts are the overlay hosts asked and written to; Facade, when set,
	// is the plane; Providers are providers pieces and entries are
	// unicast to (their submit route). Quorum is how many hosts must take
	// an entry when there is no facade.
	Hosts     []string `json:"hosts"`
	Facade    string   `json:"facade,omitempty"`
	Providers []string `json:"providers,omitempty"`
	Quorum    int      `json:"quorum,omitempty"`
	// CA, when set, is a PEM file of certificates trusted, besides the
	// system's, for TLS to hosts, providers and advertisers.
	CA string `json:"ca,omitempty"`
	// FetchPrivate lets the gateway fetch from advertised URLs whose host
	// resolves to a loopback, private or link-local address; off, such an
	// advertisement is never fetched from.
	FetchPrivate bool `json:"fetch_private,omitempty"`
	// Identities are the writer identities, each with a wallet under
	// identities/<name>; AccessKeys map S3 access keys to them; Buckets are
	// the chains served.
	Identities []string       `json:"identities"`
	AccessKeys []AccessKey    `json:"access_keys"`
	Buckets    []BucketConfig `json:"buckets"`
	// Bounty, when set, is the hold bounty every put asks for; HoldAhead is
	// how many mined hold outputs of it each identity keeps ready.
	Bounty    *BountyConfig `json:"bounty,omitempty"`
	HoldAhead int           `json:"hold_ahead,omitempty"`
	// HoldTree is how many hold outputs one transaction carries: 16 by
	// default, at most what hosts admit of a mined transaction (1,024).
	HoldTree int    `json:"hold_tree,omitempty"`
	Limits   Limits `json:"limits"`
	// PieceTree and EntryTree size funding trees. Fee is the miner fee
	// policy (feepolicy.Config; default 100 satoshis per 1000 bytes, floor
	// 100, capped at 100/1000); FeeRate and FeeFloor are its older keys,
	// still read and held to the same cap.
	PieceTree int               `json:"piece_tree"`
	EntryTree int               `json:"entry_tree"`
	Fee       *feepolicy.Config `json:"fee,omitempty"`
	FeeRate   uint64            `json:"fee_sat_per_byte,omitempty"`
	FeeFloor  uint64            `json:"fee_floor,omitempty"`
	Mainnet   bool              `json:"mainnet"`
	// RefuseUnmined makes fund -beef take mined payments only.
	RefuseUnmined bool `json:"refuse_unmined,omitempty"`
	// The payment acceptance rule a settlement is remitted by (accept_
	// keys, docs/s3.md).
	acceptcfg.Config
	// PieceWorkers is how many piece carriers a put mints and sends at once
	// (default 4); PieceAhead how many piece outputs are kept ready ahead
	// of the puts, across the current tree, the trees held for a switch
	// and the trees being minted, with more trees minted in the background
	// when fewer are left (default piece_tree: one tree always ready);
	// EntryAhead the same for entry trees (default half of entry_tree). A
	// writer is held to the entry outputs ready per block interval: set
	// entry_ahead to the puts one block interval must absorb, and the
	// gateway keeps that many ready across as many trees of entry_tree
	// outputs as it takes, each entry carrier carrying only its own tree.
	PieceWorkers int `json:"piece_workers,omitempty"`
	PieceAhead   int `json:"piece_ahead,omitempty"`
	EntryAhead   int `json:"entry_ahead,omitempty"`
	// Race is the hold race, settlement and reclaim the gateway runs in the
	// background for its own puts.
	Race RaceConfig `json:"race"`
	// StoreBytes bounds the local closures kept for reading the gateway's
	// own writes and for copies.
	StoreBytes int64 `json:"store_bytes"`
	// OutboxBytes bounds what each identity's outbox may owe its targets
	// (a provider that is down) before a put is refused with 503
	// SlowDown; 0 is 8 GiB.
	OutboxBytes int64 `json:"outbox_bytes,omitempty"`
	// MetricsListen, when set, serves /metrics there, and only there.
	MetricsListen string `json:"metrics_listen,omitempty"`
	// PprofListen, when set, serves Go's net/http/pprof there (a loopback
	// address: the profiles are the process's internals), with mutex and
	// block profiling on.
	PprofListen string `json:"pprof_listen,omitempty"`
}

// RaceConfig is the gateway's background race (spec section 9.4): for each
// identity's own puts with a bounty, the race judged, settled by rank once
// closed, the remittances sent, and the hold output reclaimed after the
// deadline when nothing settled, as `bstore holds`, `settle` and `reclaim`
// would. On by default.
type RaceConfig struct {
	// Off leaves races to the command line, run against a stopped gateway.
	Off bool `json:"off,omitempty"`
	// WindowSeconds is the race window (default an hour); TickSeconds the
	// pause between passes (default 10).
	WindowSeconds int64 `json:"window_seconds,omitempty"`
	TickSeconds   int   `json:"tick_seconds,omitempty"`
	// Budget is the most paid for one settlement challenge a provider
	// prices (default 0: only free answers).
	Budget uint64 `json:"budget,omitempty"`
	// NoPublish keeps failed challenges unpublished as outcome records.
	NoPublish bool `json:"no_publish,omitempty"`
	// Parallel is how many open races a pass steps at once (default 8).
	Parallel int `json:"parallel,omitempty"`
}

// AccessKey is one S3 access key.
type AccessKey struct {
	ID         string   `json:"id"`
	Secret     string   `json:"secret,omitempty"`
	SecretFile string   `json:"secret_file,omitempty"`
	Identity   string   `json:"identity"`
	Buckets    []string `json:"buckets,omitempty"`
}

// BucketConfig is one bucket chain the gateway serves: the identifier, the
// owner's key and the bucket id (spec sections 3.3 and 16.1).
type BucketConfig struct {
	Bucket string `json:"bucket"`
	Owner  string `json:"owner"`
	ID     string `json:"id"`
}

// BountyConfig is the hold bounty a put asks for (spec section 4.3).
type BountyConfig struct {
	Satoshis         uint64 `json:"satoshis"`
	T                uint64 `json:"t"`
	K                uint64 `json:"k"`
	RetentionSeconds uint64 `json:"retention_seconds"`
	DeadlineBlocks   uint64 `json:"deadline_blocks"`
	CheckC           uint64 `json:"check_c"`
}

// Limits are the gateway's hardening; zero takes the default.
type Limits struct {
	// MaxObject bounds a PutObject body; MaxUpload a multipart upload's
	// assembled object; MaxPart one part; MaxParts the part numbers.
	MaxObject int64 `json:"max_object"`
	MaxUpload int64 `json:"max_upload"`
	MaxPart   int64 `json:"max_part"`
	MaxParts  int   `json:"max_parts"`
	// Uploads open at once, in all and per access key; UploadSeconds how
	// long one may stay open.
	Uploads       int `json:"uploads"`
	UploadsPerKey int `json:"uploads_per_key"`
	UploadSeconds int `json:"upload_seconds"`
	// Writes are puts, copies and completions at once; PartWrites part
	// uploads at once; Reads object reads at once. Past each the gateway
	// answers 503 SlowDown.
	Writes     int `json:"writes"`
	PartWrites int `json:"part_writes"`
	Reads      int `json:"reads"`
	// PartWriters is how many part blobs are written to the disk at once,
	// across every upload: parts in flight are hashed as they stream in
	// and queue for a writer, so the drive sees this many streams, not one
	// per part. Default 2.
	PartWriters int `json:"part_writers"`
	// Stages are put and copy bodies streaming in at once; Lookups the
	// heads, gets and listings asking hosts at once.
	Stages  int `json:"stages"`
	Lookups int `json:"lookups"`
	// MinRate is the slowest a request body may arrive, bytes a second
	// averaged after its first IdleSeconds; slower is cut off.
	MinRate int64 `json:"min_rate"`
	// ReplayPerKey bounds the signatures one access key has remembered
	// against replay; past it that key's writes are refused with SlowDown.
	// A signature is remembered for the 15 minutes it could be replayed,
	// so this bounds a key's sustained writes to ReplayPerKey / 900 a
	// second. The default is one replay window at RequestsPerSec, so the
	// request rate, not this, is what holds a key back.
	ReplayPerKey int `json:"replay_per_key"`
	// RequestsPerSec and Burst bound the requests of one access key.
	RequestsPerSec float64 `json:"requests_per_sec"`
	Burst          float64 `json:"burst"`
	// IdleSeconds is the longest a request body or a response may stall.
	IdleSeconds int `json:"idle_seconds"`
	// DeleteKeys bounds one DeleteObjects request.
	DeleteKeys int `json:"delete_keys"`
}

func (l *Limits) defaults() {
	set := func(p *int64, v int64) {
		if *p <= 0 {
			*p = v
		}
	}
	seti := func(p *int, v int) {
		if *p <= 0 {
			*p = v
		}
	}
	set(&l.MaxObject, 5<<30)
	set(&l.MaxUpload, 100<<30)
	set(&l.MaxPart, 5<<30)
	seti(&l.MaxParts, 10000)
	seti(&l.Uploads, 1000)
	seti(&l.UploadsPerKey, 100)
	seti(&l.UploadSeconds, 86400)
	// 64: an S3 client's default concurrency (10 for the aws CLI, more for
	// SDK transfer managers) is not refused at the door.
	seti(&l.Writes, 256)
	seti(&l.PartWrites, 64)
	seti(&l.PartWriters, 2)
	seti(&l.Reads, 32)
	seti(&l.Stages, 64)
	seti(&l.Lookups, 64)
	set(&l.MinRate, 16<<10)
	if l.RequestsPerSec <= 0 {
		l.RequestsPerSec = 500
	}
	seti(&l.ReplayPerKey, int(math.Ceil(l.RequestsPerSec*maxSkew.Seconds())))
	if l.Burst <= 0 {
		l.Burst = 1000
	}
	seti(&l.IdleSeconds, 60)
	seti(&l.DeleteKeys, 1000)
	if l.MaxParts > 10000 {
		l.MaxParts = 10000
	}
	if l.DeleteKeys > 1000 {
		l.DeleteKeys = 1000
	}
}

// LoadConfig reads and checks a configuration file.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.Check(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

// Spec is the chain, settlement and fee settings, for chaincfg.
func (c *Config) Spec() chaincfg.Spec {
	n := "test"
	if c.Mainnet {
		n = "main"
	}
	return chaincfg.Spec{Network: n, Chain: c.Chain, Node: c.Node, Settle: c.Settle, ArcadeKey: c.ArcadeKey, RPCUser: c.RPCUser,
		RPCPass: c.RPCPass, RPCID: "bstore-s3d", Headers: c.Headers, HeadersToken: c.HeadersToken, WoCKey: c.WoCKey, WoCRate: c.WoCRate,
		Fee: c.Fee, FeeRate: c.FeeRate, FeeFloor: c.FeeFloor}
}

// IdentityDir is an identity's directory.
func (c *Config) IdentityDir(name string) string { return filepath.Join(c.Data, "identities", name) }

// Check holds the configuration to its rules and fills the defaults.
func (c *Config) Check() error {
	if !filepath.IsAbs(c.Data) {
		return errors.New("data must be an absolute directory")
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:9000"
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("tls_cert and tls_key go together")
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if err := c.Spec().Check(); err != nil {
		return err
	}
	if len(c.Hosts) == 0 {
		return errors.New("at least one host is required")
	}
	if c.Facade == "" && len(c.Providers) == 0 {
		return errors.New("a facade or at least one provider is required to deliver pieces")
	}
	if c.Quorum < 0 || c.Quorum > len(c.Hosts) {
		return errors.New("quorum is at most the number of hosts")
	}
	names := map[string]bool{}
	for _, n := range c.Identities {
		if !validName(n) || names[n] {
			return fmt.Errorf("identity %q: a name of 1 to 32 of a-z, 0-9, - and _, once", n)
		}
		names[n] = true
	}
	if len(names) == 0 {
		return errors.New("at least one identity is required")
	}
	s3names := map[string]bool{}
	for i := range c.Buckets {
		b := &c.Buckets[i]
		if err := storerec.CheckBucket(b.Bucket); err != nil {
			return fmt.Errorf("bucket %q: not a bucket identifier", b.Bucket)
		}
		n, ok := storerec.S3Name(b.Bucket)
		if !ok {
			return fmt.Errorf("bucket %q: its S3 name begins with the reserved prefix sthree-", b.Bucket)
		}
		if s3names[n] {
			return fmt.Errorf("bucket %q: served twice", b.Bucket)
		}
		s3names[n] = true
		owner, err := hex.DecodeString(b.Owner)
		if err != nil || len(b.Owner) != 66 || strings.ToLower(b.Owner) != b.Owner {
			return fmt.Errorf("bucket %q: owner is a key in 66 lowercase hex digits", b.Bucket)
		}
		if _, err := guard.ParsePubKey(owner); err != nil || bytes.Equal(owner, storerec.AnyoneKey) {
			return fmt.Errorf("bucket %q: owner is not a valid key", b.Bucket)
		}
		id, err := hex.DecodeString(b.ID)
		if err != nil || len(id) != 32 || strings.ToLower(b.ID) != b.ID {
			return fmt.Errorf("bucket %q: id is 64 lowercase hex digits", b.Bucket)
		}
	}
	if len(c.Buckets) == 0 {
		return errors.New("at least one bucket is required")
	}
	ids := map[string]bool{}
	for i := range c.AccessKeys {
		k := &c.AccessKeys[i]
		if len(k.ID) < 3 || len(k.ID) > 128 || strings.ContainsAny(k.ID, "/ \t\r\n,=") || ids[k.ID] {
			return fmt.Errorf("access key %q: 3 to 128 characters, no slash, space, comma or equals sign, once", k.ID)
		}
		ids[k.ID] = true
		if (k.Secret == "") == (k.SecretFile == "") {
			return fmt.Errorf("access key %s: exactly one of secret and secret_file", k.ID)
		}
		if k.SecretFile != "" {
			b, err := os.ReadFile(k.SecretFile)
			if err != nil {
				return fmt.Errorf("access key %s: %w", k.ID, err)
			}
			k.Secret = strings.TrimSpace(string(b))
			k.SecretFile = ""
		}
		if len(k.Secret) < 16 || len(k.Secret) > 256 {
			return fmt.Errorf("access key %s: the secret is 16 to 256 characters", k.ID)
		}
		if !names[k.Identity] {
			return fmt.Errorf("access key %s: no identity %q", k.ID, k.Identity)
		}
		for _, b := range k.Buckets {
			if !s3names[b] {
				return fmt.Errorf("access key %s: bucket %q is not served (an S3 bucket name)", k.ID, b)
			}
		}
	}
	if len(ids) == 0 {
		return errors.New("at least one access key is required")
	}
	if b := c.Bounty; b != nil {
		if b.Satoshis < 1 || b.T < 1 || b.T > 16 || b.K < b.T || b.K > 16 || b.RetentionSeconds < 1 || b.RetentionSeconds > 1<<32-1 ||
			b.DeadlineBlocks < 1 || b.CheckC < 1 || b.CheckC > 1024 {
			return errors.New("bounty: satoshis at least 1, 1 <= t <= k <= 16, retention_seconds and deadline_blocks at least 1, check_c 1 to 1024")
		}
	}
	if c.HoldAhead <= 0 {
		c.HoldAhead = publish.DefaultHoldAhead
	}
	if c.PieceTree < 0 || c.EntryTree < 0 || c.PieceAhead < 0 || c.EntryAhead < 0 || c.PieceWorkers < 0 {
		return errors.New("piece_tree, entry_tree, piece_ahead, entry_ahead and piece_workers are not negative")
	}
	if c.PieceTree > 1023 || c.EntryTree > 1023 {
		return errors.New("piece_tree and entry_tree are at most 1023")
	}
	if c.PieceWorkers > 32 {
		return errors.New("piece_workers is at most 32")
	}
	if c.Race.WindowSeconds < 0 || c.Race.TickSeconds < 0 || c.Race.Parallel < 0 {
		return errors.New("race: window_seconds, tick_seconds and parallel are not negative")
	}
	if c.Race.Parallel == 0 {
		c.Race.Parallel = 8
	}
	if c.Race.WindowSeconds == 0 {
		c.Race.WindowSeconds = 3600
	}
	if c.Race.TickSeconds == 0 {
		c.Race.TickSeconds = 10
	}
	if c.StoreBytes <= 0 {
		c.StoreBytes = 8 << 30
	}
	c.Limits.defaults()
	if c.PieceTree == 0 {
		// A tree holds the largest PutObject's pieces, so no put waits
		// for one to mine: up to 1,023 outputs (about 4 GiB of content);
		// a larger put spans trees.
		c.PieceTree = min(max(publish.PiecesFor(uint64(c.Limits.MaxObject)), 64), 1023)
	}
	if c.EntryTree == 0 {
		c.EntryTree = publish.DefaultEntryTree
	}
	if c.PieceAhead == 0 {
		c.PieceAhead = publish.DefaultTreesAhead * c.PieceTree
	}
	if c.PieceWorkers == 0 {
		c.PieceWorkers = publish.DefaultPieceWorkers
	}
	return nil
}

func validName(n string) bool {
	if len(n) < 1 || len(n) > 32 {
		return false
	}
	for i := 0; i < len(n); i++ {
		ch := n[i]
		if !(ch >= 'a' && ch <= 'z') && !(ch >= '0' && ch <= '9') && ch != '-' && ch != '_' {
			return false
		}
	}
	return true
}
