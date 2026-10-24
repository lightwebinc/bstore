package module

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/headers"
	bmodule "github.com/lightwebinc/bcommon/module"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/replay"
	"github.com/lightwebinc/bcommon/termsroute"

	"github.com/lightwebinc/bstore/storerec"
)

// Name is the module's name, as a host's module list names it.
const Name = "bstore"

// How often retention runs; how often drops still waiting are tried again
// (and the payment watch, at most every WatchEvery).
const (
	PruneEvery = time.Hour
	FlushEvery = 2 * time.Second
	WatchEvery = 30 * time.Second
)

// Build is the version the host logs; a binary sets it.
var Build = "unversioned"

// Mounted is the module for one configuration: the contract's Module, the
// lookup service, and what Start runs.
type Mounted struct {
	Module bmodule.Module
	LS     *Service
	// Front is the terms route, when configured.
	Front   *Front
	config  *Config
	host    bmodule.Host
	ledger  *termsroute.Ledger
	journal Journal
}

// Front is the terms route and where it listens.
type Front struct {
	*termsroute.Front
	Addr string
}

// chainView is the configured node view as a replay's chain source (the
// parent of a chain's first token, which no host keeps): the node's asset
// API (BSTORE_ASSET_URL), else WhatsOnChain (BSTORE_CHAIN), else none.
func chainView(a termsroute.AcceptConfig) replay.TreeSource {
	switch {
	case a.AssetURL != "":
		return &nodeapi.Asset{Base: a.AssetURL, Client: &http.Client{Timeout: 30 * time.Second}}
	case a.Chain != "":
		if woc, err := nodeapi.NewWoC(a.Chain, a.WocKey); err == nil {
			return woc
		}
	}
	return nil
}

// Mount builds the module for c. overlayTopics is OVERLAY_TOPICS, when
// known: a blob topic is mounted only where it names one. journal replaces
// the state directory's, hdrs the configured header source and now the
// host's clock, for tests.
func Mount(host bmodule.Host, c *Config, overlayTopics []string, journal Journal, hdrs chaintracker.ChainTracker, now func() time.Time) (*Mounted, error) {
	records := map[string]bool{}
	blobs := map[string]bool{}
	for _, b := range c.Buckets {
		records[storerec.RecordsPrefix+b] = true
		blobs[storerec.BlobPrefix+b] = true
	}
	for _, t := range overlayTopics {
		if strings.HasPrefix(t, storerec.BlobPrefix) && !blobs[t] {
			return nil, fmt.Errorf("OVERLAY_TOPICS names %s but BSTORE_BUCKETS does not configure its bucket; it would be admitted by the default manager", t)
		}
		if strings.HasPrefix(t, storerec.RecordsPrefix) && !strings.HasPrefix(t, storerec.BlobPrefix) && !records[t] {
			return nil, fmt.Errorf("OVERLAY_TOPICS names %s but BSTORE_BUCKETS does not configure it; it would be admitted by the default manager", t)
		}
	}
	m := host.Metrics()
	for _, kind := range AdmitKinds {
		m.Preset("bstore_admitted_total", map[string]string{"kind": kind})
		m.Preset("bstore_admitted_repeats_total", map[string]string{"kind": kind})
	}
	for _, reason := range Reasons {
		m.Preset("bstore_refused_total", map[string]string{"reason": reason})
	}
	for _, status := range TokenStatuses {
		m.Preset("bstore_token_status_total", map[string]string{"status": status})
	}
	for _, why := range DropReasons {
		m.Preset("bstore_dropped_total", map[string]string{"why": why})
	}
	m.Preset("bstore_retractions_total", nil)
	m.Preset("bstore_journal_lines_total", nil)
	m.Preset("bstore_journal_wait_seconds_total", nil)
	for _, result := range []string{"ok", "error"} {
		m.Preset("bstore_journal_syncs_total", map[string]string{"result": result})
	}
	m.Preset("bstore_journal_sync_seconds_total", nil)
	priced := map[string]bool{}
	for name, p := range c.Prices {
		if p > 0 {
			priced[name] = true
		}
	}
	supplied := hdrs != nil
	if hdrs == nil {
		hdrs = headers.New(c.HeadersURL)
	}
	if now == nil {
		now = time.Now
	}
	if journal == nil {
		fj, err := NewFileJournal(c.StateDir)
		if err != nil {
			return nil, err
		}
		fj.SyncEvery = c.JournalSync
		fj.OnSync = journalSyncs(host)
		journal = fj
	}
	ls, err := NewService(Options{Buckets: c.Buckets, Served: c.Served, Host: host, Journal: journal, Priced: priced, KeepDays: c.KeepDays,
		PendingDays: c.PendingDays, OtherHours: c.OtherHours, Headers: hdrs, Now: now, Trees: chainView(c.Accept),
		MaxBEEF: c.MaxBEEF, MaxSweepInputs: c.MaxSweepInputs})
	if err != nil {
		return nil, err
	}
	m.Gauge("bstore_buckets", func() float64 { return float64(len(c.Buckets)) })
	m.Gauge("bstore_tokens", func() float64 { return float64(ls.Counts().Tokens) })
	m.Gauge("bstore_tokens_pending", func() float64 { return float64(ls.Counts().PendingTokens) })
	m.Gauge("bstore_entries", func() float64 { return float64(ls.Counts().Entries) })
	m.Gauge("bstore_outcomes", func() float64 { return float64(ls.Counts().Outcomes) })
	m.Gauge("bstore_fronts", func() float64 { return float64(ls.Counts().Fronts) })
	m.Gauge("bstore_advertisements", func() float64 { return float64(ls.Counts().Ads) })
	m.Gauge("bstore_hold_outputs", func() float64 { return float64(ls.Counts().Holds) })
	m.Gauge("bstore_sweeps", func() float64 { return float64(ls.Counts().Sweeps) })
	m.Gauge("bstore_sweep_outpoints", func() float64 { return float64(ls.Counts().SweepOutpoints) })
	m.Gauge("bstore_retraction_marks", func() float64 { return float64(ls.Counts().Marks) })
	m.Gauge("bstore_outpoint_rows", func() float64 { return float64(ls.Counts().Rows) })
	m.Gauge("bstore_pending_drops", func() float64 { return float64(ls.Counts().PendingDrops) })
	// As the last retention pass counted them.
	m.Gauge("bstore_maps_complete", func() float64 { return float64(ls.Counts().Tally.MapsComplete) })
	m.Gauge("bstore_maps_incomplete", func() float64 { return float64(ls.Counts().Tally.MapsIncomplete) })
	m.Gauge("bstore_chains_broken", func() float64 { return float64(ls.Counts().Tally.ChainsBroken) })
	m.Gauge("bstore_anchored_entries", func() float64 { return float64(ls.Counts().Tally.Anchored) })
	topics := map[string]bmodule.TopicManager{}
	var recordNames []string
	for _, b := range c.Buckets {
		t := storerec.RecordsPrefix + b
		tm, err := NewRecordsTopicManager(t, host, hdrs, c.MaxBEEF, c.MaxSweepInputs)
		if err != nil {
			return nil, err
		}
		topics[t] = tm
		recordNames = append(recordNames, t)
	}
	var mountedBlobs []string
	for _, t := range overlayTopics {
		if !blobs[t] || topics[t] != nil {
			continue
		}
		tm, err := NewBlobTopicManager(t, host)
		if err != nil {
			return nil, err
		}
		topics[t] = tm
		mountedBlobs = append(mountedBlobs, t)
	}
	out := &Mounted{Module: bmodule.Module{Topics: topics, Lookups: map[string]bmodule.LookupService{storerec.LookupService: ls}}, LS: ls, config: c, host: host, journal: journal}
	var gate *termsroute.Gate
	if c.Listen != "" {
		var receiver termsroute.Receiver
		if len(priced) > 0 {
			ledger, err := termsroute.OpenLedger(c.StateDir, termsroute.LayoutPayee, now)
			if err != nil {
				return nil, err
			}
			out.ledger = ledger
			receiver = ledger
			gate = termsroute.GateFor("bstore", host, hdrs, c.Accept, c.StateDir)
		}
		f, err := termsroute.New(termsroute.Options{App: "bstore", Service: storerec.LookupService, Classes: Classes, PaymentProtocol: storerec.SettleProtocol,
			LS: RouteAnswerer{LS: ls}, Host: host, Prices: c.Prices, Payee: c.PayeeKey, Receiver: receiver, Headers: hdrs, Gate: gate,
			Sessions: c.Sessions, Budget: c.Budget, Responses: c.Response, ResponseBudgetBeforeVerify: true, Now: now})
		if err != nil {
			return nil, err
		}
		if out.ledger != nil {
			f.RestoreFast(out.ledger.Fast)
		}
		out.Front = &Front{Front: f, Addr: c.Listen}
	}
	terms, sessions, handshakes, responses, payments := "none", "none", "none", "none", "none"
	if c.Listen != "" {
		terms = c.Listen
		sessions = fmt.Sprintf("%d, idle %ds", c.Sessions.Max, int(c.Sessions.TTL/time.Second))
		handshakes = fmt.Sprintf("%g/s burst %d, per address %g/s burst %d", c.Budget.PerSec, c.Budget.Burst, c.Budget.PerAddressPerSec, c.Budget.AddressBurst)
		responses = fmt.Sprintf("%g/s burst %d a session", c.Response.PerSec, c.Response.Burst)
	}
	if gate != nil {
		if gate.Networked() {
			payments = fmt.Sprintf("fast to %d sat, payer %d, total %d", c.Accept.Policy.ThresholdSats, c.Accept.Policy.PayerLimit, c.Accept.Policy.TotalLimit)
		} else {
			payments = "held until mined (no arcade or asset URL)"
		}
	}
	pricedNames := make([]string, 0, len(priced))
	for _, cl := range Classes {
		if priced[cl.Name] {
			pricedNames = append(pricedNames, cl.Name)
		}
	}
	blobLine := "none"
	if len(mountedBlobs) > 0 {
		blobLine = strings.Join(mountedBlobs, ",") + " (refuses everything: a host holds no pieces)"
	}
	headersLine := c.HeadersURL
	if supplied {
		headersLine = "supplied"
	}
	journalLine := "each line"
	if c.JournalSync > 0 {
		journalLine = fmt.Sprintf("every %s", c.JournalSync)
	}
	host.Log("bstore module mounted", map[string]any{"topics": strings.Join(recordNames, ","), "blobs": blobLine, "lookup": storerec.LookupService,
		"served": len(c.Served), "headers": headersLine, "floors": fmt.Sprintf("keep %dd, pending %dd, other %dh", ls.keep/day, ls.pending/day, ls.other/hour), "journal_sync": journalLine,
		"terms": terms, "priced": orNone(pricedNames), "sessions": sessions, "handshakes": handshakes, "responses": responses, "payments": payments, "build": Build})
	return out, nil
}

// journalSyncs counts the rows journal's timed fsyncs and logs the first
// failure of a run of them and the recovery after it.
func journalSyncs(host bmodule.Host) func(lines int, took time.Duration, err error) {
	var mu sync.Mutex
	failing := false
	return func(lines int, took time.Duration, err error) {
		m := host.Metrics()
		m.Inc("bstore_journal_sync_seconds_total", nil, took.Seconds())
		result := "ok"
		if err != nil {
			result = "error"
		}
		m.Inc("bstore_journal_syncs_total", map[string]string{"result": result}, 1)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err != nil && !failing:
			failing = true
			host.Log("ls_bstore could not sync the journal; its lines stay pending and the sync is tried again", map[string]any{"lines": lines, "err": err.Error()})
		case err == nil && failing:
			failing = false
			host.Log("ls_bstore journal synced again", map[string]any{"lines": lines})
		}
	}
}

// Close flushes the rows journal and closes it (Start does when its context
// ends). A host that stops without it leaves at most the last sync window's
// lines in the page cache: written, not yet on disk.
func (m *Mounted) Close() error {
	if c, ok := m.journal.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func orNone(a []string) string {
	if len(a) == 0 {
		return "none"
	}
	return strings.Join(a, ",")
}

// Start serves the terms route (when configured) until ctx ends, and runs
// the background tick: drops still waiting every FlushEvery, the payment
// watch every WatchEvery, and retention once the index is restored and
// every PruneEvery after. It returns when ctx ends, after closing the rows
// journal (Close).
func (m *Mounted) Start(ctx context.Context) error {
	defer func() { _ = m.Close() }()
	go func() {
		var pruned, watched time.Time
		t := time.NewTicker(FlushEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if m.Front != nil && m.Front.Gate != nil && time.Since(watched) >= WatchEvery {
				watched = time.Now()
				m.Front.Tick(ctx)
			}
			if !m.LS.Restored() {
				continue
			}
			if time.Since(pruned) >= PruneEvery {
				pruned = time.Now()
				m.LS.Prune(ctx)
				continue
			}
			m.LS.Flush(ctx)
		}
	}()
	if m.Front == nil {
		<-ctx.Done()
		return nil
	}
	return m.Front.Front.Listen(ctx, m.Front.Addr)
}

// New is the factory a host calls: the configuration from getenv, checked
// against OVERLAY_TOPICS and OVERLAY_SYNC_PEERS.
func New(host bmodule.Host, getenv func(string) string) (*Mounted, error) {
	if getenv == nil {
		return nil, errors.New("bstore: no environment")
	}
	var topics []string
	for _, t := range strings.Split(getenv("OVERLAY_TOPICS"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			topics = append(topics, t)
		}
	}
	if err := CheckNoSync(getenv("OVERLAY_SYNC_PEERS")); err != nil {
		return nil, err
	}
	c, err := ParseConfig(getenv)
	if err != nil {
		return nil, err
	}
	return Mount(host, c, topics, nil, nil, nil)
}
