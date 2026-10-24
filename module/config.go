// Package module is the bstore overlay module in Go: `tm_bstore_<bucket>`
// for each configured bucket, `tm_bstoreblob_<bucket>` where the host's
// topics name one, and `ls_bstore` for all of them, written to bcommon's
// engine-neutral contract (github.com/lightwebinc/bcommon/module) so that
// any Go overlay host that carries the adapter mounts it. It is the twin of
// the TypeScript module under host/src (frozen), built from the Go core:
// the admission rules are storerec.Admit, the fold and the map answers
// storerec's, and the questions storerec's classes.
//
// Configuration is the TypeScript module's, read from the environment when
// the host calls the factory (docs/host.md): BSTORE_BUCKETS,
// BSTORE_STATE_DIR, BSTORE_HEADERS_URL, BSTORE_SERVED, BSTORE_KEEP_DAYS,
// BSTORE_PENDING_DAYS, BSTORE_OTHER_HOURS, BSTORE_MAX_BEEF,
// BSTORE_MAX_SWEEP_INPUTS, BSTORE_LISTEN, BSTORE_PRICES, BSTORE_PAYEE_KEY,
// BSTORE_SESSIONS, BSTORE_SESSION_TTL, the handshake and response budgets,
// BSTORE_ARCADE_URL, BSTORE_ASSET_URL, BSTORE_CHAIN, BSTORE_WOC_KEY and the
// BSTORE_ACCEPT_* rule, and one of its own, BSTORE_JOURNAL_SYNC_MS (the rows
// journal's group commit window). Every mistake is an error, and the host
// refuses to start.
package module

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/termsroute"

	"github.com/lightwebinc/bstore/storerec"
)

// The retention floors (spec section 12.3): the long floor, the floor of
// unnamed entries, and everything else.
const (
	FloorKeepDays    = 31
	FloorPendingDays = 7
	FloorOtherHours  = 24
)

// The rows journal's group commit window (BSTORE_JOURNAL_SYNC_MS): lines
// are written at once and fsynced together this often, off the lookup
// service's lock; 0 fsyncs each line before the admission goes on. The
// default and the bound are the engine store's (OVERLAY_STORE_WAL_SYNC_MS).
const (
	DefaultJournalSyncMs = 100
	MaxJournalSyncMs     = 60_000
)

// DefaultListenPort is the terms route's port on the Go host. The
// TypeScript host's is 8094; the Go engine runs beside it on the same
// hosts until a cutover, so the Go route takes its own.
const DefaultListenPort = 8294

// Config is the module's configuration.
type Config struct {
	Buckets    []string
	StateDir   string
	HeadersURL string
	// Served are the chains this host serves: <bucket>|<owner hex>|<id hex>.
	Served         []string
	KeepDays       int
	PendingDays    int
	OtherHours     int
	MaxBEEF        int
	MaxSweepInputs int
	// JournalSync is the rows journal's group commit window; 0 syncs every
	// line.
	JournalSync time.Duration
	// Listen is the terms route's host:port; empty for none.
	Listen   string
	Prices   map[string]uint64
	PayeeKey *ec.PrivateKey
	Sessions termsroute.SessionBound
	Budget   termsroute.BudgetConfig
	Response termsroute.ResponseBudgetConfig
	Accept   termsroute.AcceptConfig
}

// Classes are the lookup classes as the route prices them, in the order of
// spec section 11.1.
var Classes = func() []termsroute.PricedClass {
	out := make([]termsroute.PricedClass, len(storerec.Classes))
	for i, c := range storerec.Classes {
		out[i] = termsroute.PricedClass{Name: c.Name, Free: c.Free}
	}
	return out
}()

// ParseBuckets parses BSTORE_BUCKETS: bucket identifiers, once each.
func ParseBuckets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("BSTORE_BUCKETS is required: name each bucket this host carries")
	}
	var out []string
	for _, e := range strings.Split(raw, ",") {
		b := strings.TrimSpace(e)
		if storerec.CheckBucket(b) != nil {
			return nil, fmt.Errorf("BSTORE_BUCKETS: %q is not a bucket identifier <name>_<suffix>", b)
		}
		for _, x := range out {
			if x == b {
				return nil, fmt.Errorf("BSTORE_BUCKETS names %s twice", b)
			}
		}
		out = append(out, b)
	}
	return out, nil
}

var (
	ownerRE = regexp.MustCompile(`^[0-9a-f]{66}$`)
	idRE    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func isKeyHex(s string) bool {
	if !ownerRE.MatchString(s) {
		return false
	}
	b, _ := hex.DecodeString(s)
	_, err := storerec.CheckKey(b)
	return err == nil
}

// ParseServed parses BSTORE_SERVED: <bucket>:<owner>:<id>, each of a
// carried bucket, once. The result is <bucket>|<owner>|<id>.
func ParseServed(raw string, buckets []string) ([]string, error) {
	var out []string
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	for _, entry := range strings.Split(raw, ",") {
		e := strings.TrimSpace(entry)
		parts := strings.Split(e, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("BSTORE_SERVED: %q is not <bucket>:<owner>:<id>", e)
		}
		b, owner, id := parts[0], parts[1], parts[2]
		carried := false
		for _, x := range buckets {
			carried = carried || x == b
		}
		if !carried {
			return nil, fmt.Errorf("BSTORE_SERVED: %s is not a bucket BSTORE_BUCKETS names", b)
		}
		if !isKeyHex(owner) {
			return nil, fmt.Errorf("BSTORE_SERVED: %s is not an owner's key, 66 lowercase hex digits", owner)
		}
		if !idRE.MatchString(id) {
			return nil, fmt.Errorf("BSTORE_SERVED: %s is not a bucket id, 64 lowercase hex digits", id)
		}
		key := b + "|" + owner + "|" + id
		for _, x := range out {
			if x == key {
				return nil, fmt.Errorf("BSTORE_SERVED names %s twice", e)
			}
		}
		out = append(out, key)
	}
	return out, nil
}

var digits = regexp.MustCompile(`^[0-9]+$`)
var rateRE = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

func integer(name, raw string, def, min int) (int, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if !digits.MatchString(v) || err != nil || n > termsroute.MaxSafe || n < int64(min) {
		return 0, fmt.Errorf("%s: %q is not an integer of at least %d", name, raw, min)
	}
	return int(n), nil
}

// rate is a positive rate, decimals allowed, at most 1e6.
func rate(name, raw string, def float64) (float64, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if !rateRE.MatchString(v) || err != nil || !(f > 0) || f > 1e6 {
		return 0, fmt.Errorf("%s: %q is not a positive number of at most 1000000", name, raw)
	}
	return f, nil
}

func isNativeSource(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

var listenRE = regexp.MustCompile(`^(.+):([0-9]{1,5})$`)

// ParseListen parses BSTORE_LISTEN, host:port, an IPv6 host in brackets or
// not, as the TypeScript module reads it; the answer is host:port as Go
// listens on it.
func ParseListen(raw string) (string, error) {
	m := listenRE.FindStringSubmatch(raw)
	if m == nil {
		return "", fmt.Errorf("BSTORE_LISTEN: %q is not host:port", raw)
	}
	port, _ := strconv.Atoi(m[2])
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("BSTORE_LISTEN: %q is not host:port", raw)
	}
	host := m[1]
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + strconv.Itoa(port), nil
	}
	return host + ":" + strconv.Itoa(port), nil
}

var payeeRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ParseConfig reads the whole configuration through getenv.
func ParseConfig(getenv func(string) string) (*Config, error) {
	buckets, err := ParseBuckets(getenv("BSTORE_BUCKETS"))
	if err != nil {
		return nil, err
	}
	c := &Config{Buckets: buckets, StateDir: getenv("BSTORE_STATE_DIR")}
	if c.StateDir == "" || !filepath.IsAbs(c.StateDir) {
		return nil, errors.New("BSTORE_STATE_DIR is required: an absolute directory kept for the host's life, where the rows journal and the payment ledger live")
	}
	own := strings.TrimSpace(getenv("BSTORE_HEADERS_URL"))
	host := strings.TrimSpace(getenv("OVERLAY_CHAIN_TRACKER_URL"))
	switch {
	case own != "":
		if !isNativeSource(own) {
			return nil, fmt.Errorf("BSTORE_HEADERS_URL: %q is not an http or https URL", own)
		}
		c.HeadersURL = own
	case host != "" && isNativeSource(host):
		c.HeadersURL = host
	default:
		return nil, errors.New("BSTORE_HEADERS_URL is required: a native header source (/v1/root/<height>, /v1/tip) that token and mined-object proofs are checked against; OVERLAY_CHAIN_TRACKER_URL is not one")
	}
	if c.Served, err = ParseServed(getenv("BSTORE_SERVED"), buckets); err != nil {
		return nil, err
	}
	ints := []struct {
		dst      *int
		name     string
		def, min int
	}{
		{&c.KeepDays, "BSTORE_KEEP_DAYS", FloorKeepDays, FloorKeepDays},
		{&c.PendingDays, "BSTORE_PENDING_DAYS", FloorPendingDays, FloorPendingDays},
		{&c.OtherHours, "BSTORE_OTHER_HOURS", FloorOtherHours, FloorOtherHours},
		{&c.MaxBEEF, "BSTORE_MAX_BEEF", storerec.DefaultMaxBEEF, storerec.DefaultMaxBEEF},
		{&c.MaxSweepInputs, "BSTORE_MAX_SWEEP_INPUTS", storerec.DefaultMaxSweepInputs, storerec.DefaultMaxSweepInputs},
	}
	for _, x := range ints {
		if *x.dst, err = integer(x.name, getenv(x.name), x.def, x.min); err != nil {
			return nil, err
		}
	}
	syncMs, err := integer("BSTORE_JOURNAL_SYNC_MS", getenv("BSTORE_JOURNAL_SYNC_MS"), DefaultJournalSyncMs, 0)
	if err != nil {
		return nil, err
	}
	if syncMs > MaxJournalSyncMs {
		return nil, fmt.Errorf("BSTORE_JOURNAL_SYNC_MS: %d is over %d", syncMs, MaxJournalSyncMs)
	}
	c.JournalSync = time.Duration(syncMs) * time.Millisecond
	if c.Prices, err = termsroute.ParsePrices(getenv("BSTORE_PRICES"), Classes, "BSTORE_PRICES"); err != nil {
		return nil, err
	}
	maxSessions, err := integer("BSTORE_SESSIONS", getenv("BSTORE_SESSIONS"), termsroute.DefaultMaxSessions, 1)
	if err != nil {
		return nil, err
	}
	ttl, err := integer("BSTORE_SESSION_TTL", getenv("BSTORE_SESSION_TTL"), termsroute.DefaultSessionTTL, 1)
	if err != nil {
		return nil, err
	}
	c.Sessions = termsroute.SessionBound{Max: maxSessions, TTL: time.Duration(ttl) * time.Second}
	c.Budget = termsroute.DefaultBudget
	if c.Budget.PerSec, err = rate("BSTORE_HANDSHAKES_PER_SEC", getenv("BSTORE_HANDSHAKES_PER_SEC"), termsroute.DefaultBudget.PerSec); err != nil {
		return nil, err
	}
	if c.Budget.Burst, err = integer("BSTORE_HANDSHAKE_BURST", getenv("BSTORE_HANDSHAKE_BURST"), termsroute.DefaultBudget.Burst, 1); err != nil {
		return nil, err
	}
	if c.Budget.PerAddressPerSec, err = rate("BSTORE_HANDSHAKES_PER_ADDR_PER_SEC", getenv("BSTORE_HANDSHAKES_PER_ADDR_PER_SEC"), termsroute.DefaultBudget.PerAddressPerSec); err != nil {
		return nil, err
	}
	if c.Budget.AddressBurst, err = integer("BSTORE_HANDSHAKE_ADDR_BURST", getenv("BSTORE_HANDSHAKE_ADDR_BURST"), termsroute.DefaultBudget.AddressBurst, 1); err != nil {
		return nil, err
	}
	c.Response = termsroute.DefaultResponseBudget
	if c.Response.PerSec, err = rate("BSTORE_RESPONSES_PER_SEC", getenv("BSTORE_RESPONSES_PER_SEC"), termsroute.DefaultResponseBudget.PerSec); err != nil {
		return nil, err
	}
	if c.Response.Burst, err = integer("BSTORE_RESPONSE_BURST", getenv("BSTORE_RESPONSE_BURST"), termsroute.DefaultResponseBudget.Burst, 1); err != nil {
		return nil, err
	}
	if c.Accept, err = termsroute.ParseAcceptConfig("BSTORE", getenv); err != nil {
		return nil, err
	}
	if listen := strings.TrimSpace(getenv("BSTORE_LISTEN")); listen != "" {
		if c.Listen, err = ParseListen(listen); err != nil {
			return nil, err
		}
	}
	if k := strings.TrimSpace(getenv("BSTORE_PAYEE_KEY")); k != "" {
		if !payeeRE.MatchString(k) {
			return nil, errors.New("BSTORE_PAYEE_KEY is not 64 lowercase hex characters")
		}
		b, _ := hex.DecodeString(k)
		c.PayeeKey, _ = ec.PrivateKeyFromBytes(b)
		if c.PayeeKey == nil {
			return nil, errors.New("BSTORE_PAYEE_KEY is not a private key")
		}
	}
	charged := false
	for _, p := range c.Prices {
		charged = charged || p > 0
	}
	if charged && (c.Listen == "" || c.PayeeKey == nil) {
		return nil, errors.New("BSTORE_PRICES prices a class: BSTORE_LISTEN and BSTORE_PAYEE_KEY are required")
	}
	if len(c.Prices) > 0 && c.Listen == "" {
		return nil, errors.New("BSTORE_PRICES needs BSTORE_LISTEN, where the terms document is served")
	}
	return c, nil
}

// CheckNoSync refuses a host configured to synchronise a tm_bstore_ or
// tm_bstoreblob_ topic with a peer (OVERLAY_SYNC_PEERS,
// topic=url,...;topic=...): that synchronisation carries only unspent
// outputs, so it never carries a chain's spent tokens, and it drops what a
// topic manager refuses without trying it again. A host joins a bucket by
// replay (spec section 12.6).
func CheckNoSync(raw string) error {
	for _, entry := range strings.Split(raw, ";") {
		t := strings.TrimSpace(strings.SplitN(entry, "=", 2)[0])
		if strings.HasPrefix(t, storerec.RecordsPrefix) || strings.HasPrefix(t, storerec.BlobPrefix) {
			return fmt.Errorf("OVERLAY_SYNC_PEERS names %s: a bstore topic is never synchronised as unspent outputs; a host joins a bucket by replay", t)
		}
	}
	return nil
}
