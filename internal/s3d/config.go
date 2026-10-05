package s3d

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lightwebinc/bcommon/guard"

	"github.com/lightwebinc/bstore/internal/storerec"
)

// Config is the gateway's configuration file, JSON; an unknown key is
// refused. Every field has a default but Data, Node, Settle, Headers,
// Hosts, Identities, AccessKeys and Buckets.
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
	// Region is the SigV4 region clients sign for; Domain, when set, also
	// serves virtual-host addressing (<bucket>.<domain>).
	Region string `json:"region"`
	Domain string `json:"domain,omitempty"`
	// Node is a Teranode asset API; Settle the settlement leg (tcp:,
	// rpc: or arcade:, with RPCUser, RPCPass, ArcadeKey); Headers a header
	// source (bcommon headers).
	Node      string `json:"node"`
	Settle    string `json:"settle"`
	RPCUser   string `json:"rpc_user,omitempty"`
	RPCPass   string `json:"rpc_pass,omitempty"`
	ArcadeKey string `json:"arcade_key,omitempty"`
	Headers   string `json:"headers"`
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
	// Bounty, when set, is the hold bounty every put asks for.
	Bounty *BountyConfig `json:"bounty,omitempty"`
	Limits Limits        `json:"limits"`
	// PieceTree and EntryTree size funding trees; FeeRate and FeeFloor are
	// the fee policy.
	PieceTree int    `json:"piece_tree"`
	EntryTree int    `json:"entry_tree"`
	FeeRate   uint64 `json:"fee_sat_per_byte"`
	FeeFloor  uint64 `json:"fee_floor"`
	Mainnet   bool   `json:"mainnet"`
	// StoreBytes bounds the local closures kept for reading the gateway's
	// own writes and for copies.
	StoreBytes int64 `json:"store_bytes"`
	// MetricsListen, when set, serves /metrics there, and only there.
	MetricsListen string `json:"metrics_listen,omitempty"`
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
	seti(&l.Writes, 8)
	seti(&l.PartWrites, 32)
	seti(&l.Reads, 32)
	if l.RequestsPerSec <= 0 {
		l.RequestsPerSec = 200
	}
	if l.Burst <= 0 {
		l.Burst = 400
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
	if c.Node == "" || c.Settle == "" || c.Headers == "" {
		return errors.New("node, settle and headers are required")
	}
	kind, _, _ := strings.Cut(c.Settle, ":")
	if kind != "tcp" && kind != "rpc" && kind != "arcade" {
		return errors.New("settle is tcp:<host:port>, rpc:<url> or arcade:<url>")
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
	if c.PieceTree <= 0 {
		c.PieceTree = 64
	}
	if c.EntryTree <= 0 {
		c.EntryTree = 16
	}
	if c.PieceTree > 1023 || c.EntryTree > 1023 {
		return errors.New("piece_tree and entry_tree are at most 1023")
	}
	if c.FeeRate == 0 {
		c.FeeRate = 1
	}
	if c.FeeFloor == 0 {
		c.FeeFloor = 250
	}
	if c.StoreBytes <= 0 {
		c.StoreBytes = 8 << 30
	}
	c.Limits.defaults()
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
