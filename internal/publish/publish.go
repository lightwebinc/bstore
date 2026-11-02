// Package publish is bstore's publisher (spec sections 5, 9, 13 and 14): it
// builds an object's CHIRP closure and proof root while the content streams
// (Store.Stage), mints a hold output when a bounty is asked for, makes the
// piece carriers and the entry, and delivers them over the plane, by
// unicast to the hosts and providers configured, or by BRC-167 upload
// session; it judges each bounty's race (race.go), settles it by rank or
// reclaims it after its deadline (settle.go), challenges providers
// (challenge.go), publishes outcome records, and sweeps its funding trees
// (sweep.go). The S3 gateway and the command line both write through it.
//
// Save before send. Every carrier spends one output of a mined funding
// tree, and a carrier rebuilt differently on the same output would be a
// second carrier the first's acknowledgement never names (spec section
// 13.2). So the outputs a put will spend are recorded used, and saved,
// before any carrier is signed; each carrier is written to the outbox,
// synced, before it is sent; and what a target did not take stays in the
// outbox and is sent again, the same bytes, by the background run and on
// the next start. Funding trees are recorded with their fee coin before
// they reach the settlement leg (bcommon producer Prepare) and settled on
// the next start (Recover), as the provider does. A settlement, a reclaim
// and a sweep are signed and recorded before they are sent, and a run
// that stops part way sends the same bytes again. An identity's directory
// is locked while a Publisher has it open.
package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/acceptance"
	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"
	bpub "github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/chaincfg"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// Profile is a publisher identity's embedded wallet: the fund key under
// [1, "bstore"] key id fund, counterparty self, and the registered basket.
var Profile = bwallet.Profile{
	FundProtocol:   storerec.Protocol,
	FundKeyID:      storerec.KeyFund,
	FundBasket:     "bstore fund",
	Version:        "bstore-publisher-1",
	LegacyPoolFile: "bstore-publisher-pool.json",
}

// WalletDir is an identity's wallet under its directory.
func WalletDir(dir string) string { return filepath.Join(dir, "wallet") }

// Init creates an identity's wallet under dir and returns its key.
func Init(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	w, err := bwallet.Create(WalletDir(dir), Profile)
	if err != nil {
		return "", err
	}
	return w.Signer().IdentityHex(), nil
}

// Config is one publisher identity.
type Config struct {
	// Dir is the identity's directory: wallet/, publisher.json, outbox/.
	Dir string
	// Chain is the chain view transactions, proofs and spends are asked
	// of (a node, or WhatsOnChain: chaincfg); Known says whether a
	// transaction sent is held, mined or not (nil: never, so one not
	// mined is sent again); Tip is the header source's height and its
	// header by height. Settler is the settlement leg transactions are
	// handed to.
	Chain   nodeapi.Chain
	Known   nodeapi.KnownSource
	Tip     chaincfg.Tip
	Settler bpub.Settler
	// Fees is the fee policy, or FeeSource, when set, answers it for each
	// build (a broadcaster's live policy).
	Fees      mint.Fees
	FeeSource feepolicy.Source
	// Accept is the payment acceptance rule a settlement is remitted by:
	// one whose largest payout is at or below its threshold is remitted on
	// Status's word that the network took it, before it mines. With no
	// Status (or the zero Accept) every settlement waits for its block.
	Accept  acceptance.Policy
	Status  acceptance.StatusSource
	Mainnet bool
	// PieceTree and EntryTree are the outputs of a new piece or entry
	// funding tree (spec section 19.2: small trees cost less per piece).
	// A put of up to PieceTree pieces never waits for a block: the next
	// tree of each kind is minted ahead, in the background, once the
	// current one has PieceAhead (EntryAhead) outputs or fewer left.
	// PieceAhead zero is PieceTree, so one tree is always ready; EntryAhead
	// zero is half of EntryTree.
	PieceTree  int
	EntryTree  int
	PieceAhead int
	EntryAhead int
	// PieceWorkers is how many piece carriers are minted and sent at once;
	// zero is 4.
	PieceWorkers int
	// Poll is how often a wait for a transaction to mine asks the node;
	// zero is a second.
	Poll time.Duration
	// LockWait is how long Open waits for an identity another process has
	// open; zero refuses at once (ErrInUse).
	LockWait time.Duration
	// HoldAhead is how many mined hold outputs of each bounty value are kept
	// ready, so a put never waits for a block; HoldWait is how long a put
	// waits for one when none is ready.
	HoldAhead int
	HoldWait  time.Duration
	// Store is the local closure store, shared by every identity.
	Store *Store
	// Delivery reaches the plane, the hosts and the providers.
	Delivery *Delivery
	// Reader, when set, asks the hosts and verifies what they answer: the
	// race's advertisements and fronts, and closures the store lacks.
	Reader *reader.Reader
	// FrontHTTP reaches the fronts that advertisements and statements name
	// (challenges, terms, remittances): addresses a stranger chose, so it
	// should refuse private ones. Nil uses Delivery's client.
	FrontHTTP *http.Client
	Logf      func(format string, args ...any)
	// OutboxMax bounds the bytes the outbox may owe before a put is
	// refused (ErrOutboxFull); 0 is DefaultOutboxMax.
	OutboxMax int64
	Now       func() time.Time
}

// Publisher is one identity's writer.
type Publisher struct {
	cfg    Config
	wallet *bwallet.Embedded
	signer *bwallet.Signer

	mu sync.Mutex // st and every save
	st *state
	sv saver // writes st: see save.go

	// spendMu, one a tree kind, holds a spend from the library's answer to
	// the state's record of it, so that two puts never take the same
	// outputs; puts otherwise run side by side.
	spendMu [3]sync.Mutex
	// tip is the chain height the payer judges coinbase maturity at,
	// learned by whichever put or settlement asks last.
	tip atomic.Uint32
	// run is the Publisher's own lifetime: what a put starts in the
	// background (a funding tree minted ahead) outlives the request.
	run    context.Context
	stop   context.CancelFunc
	holdMu sync.Mutex // one hold fill runs at a time
	kept   *producer.Kept
	payer  *producer.Payer
	pieces *producer.Trees
	ents   *producer.Trees
	outs   *producer.Trees
	outbox *Outbox
	lock   *dirLock
	// auth reaches the providers configured (upload sessions); frontAuth
	// reaches the fronts advertisements and statements name, through
	// FrontHTTP, a client a front.
	auth      *authClient
	frontAuth *authPool
	// payMu is held while a small payment coin is taken or minted.
	payMu   sync.Mutex
	splitMu sync.Mutex
	coins   *bwallet.Pool // the payment coins, under payMu
}

type state struct {
	Identity string        `json:"identity"`
	Piece    treeSet       `json:"piece"`
	Entry    treeSet       `json:"entry"`
	Outcome  treeSet       `json:"outcome"`
	Pending  []pendingTree `json:"pendingTrees,omitempty"`
	Holds    []HoldRecord  `json:"holds,omitempty"`
	// HoldTxs is each hold transaction once, by txid, as the state is
	// saved (compact); in memory it is empty and every record carries its
	// transaction (expand).
	HoldTxs map[string]holdTx `json:"holdTxs,omitempty"`
	// Buckets are the buckets this identity published carriers in: where
	// a sweep of its trees is published (spec section 13.5).
	Buckets []string `json:"buckets,omitempty"`
	// Sweeps are the kill switch's sweeps, each recorded before it is
	// sent.
	Sweeps []SweepRecord `json:"sweeps,omitempty"`
	// Uploads are the unicast upload sessions owed a closure.
	Uploads []UploadRecord `json:"uploads,omitempty"`
	// Payments are BRC-105 payments made and not yet seen mined.
	Payments []PaymentRecord `json:"payments,omitempty"`
	// Intents are payment coins taken for calls in flight, recorded before
	// the call (a stop mid-call leaves them to ReconcilePayments).
	Intents []PaymentRecord `json:"intents,omitempty"`
}

type treeSet struct {
	Tree  *funding.Tree  `json:"tree,omitempty"`
	Trees []funding.Tree `json:"trees,omitempty"`
	Ahead []funding.Tree `json:"ahead,omitempty"`
}

type pendingTree struct {
	Kind         string         `json:"kind"`
	Tree         funding.Tree   `json:"tree"`
	Coin         bwallet.Output `json:"coin"`
	ReturnedOnce bool           `json:"returnedOnce,omitempty"`
}

// HoldRecord is a hold output this identity minted (spec section 9.2): what
// a publisher client settles by rank or reclaims after the deadline.
type HoldRecord struct {
	Txid    string `json:"txid"`
	Vout    uint32 `json:"vout"`
	Sats    uint64 `json:"sats"`
	RawHex  string `json:"rawHex,omitempty"`
	BumpHex string `json:"bumpHex,omitempty"`
	Height  uint32 `json:"height,omitempty"`
	// Used marks a hold output a put has taken, saved before its entry is
	// signed, so no two entries name one hold output.
	Used bool `json:"used,omitempty"`
	// Entry is the commitment of the entry that names it (display order),
	// once that entry is minted.
	Entry string `json:"entry,omitempty"`
	// Sent is when it was handed to the settlement leg (Unix seconds).
	Sent int64 `json:"sent,omitempty"`
	// Reclaim is its spend back to the fund key when no entry names it,
	// recorded before it is sent.
	Reclaim *ReclaimRecord `json:"reclaim,omitempty"`
}

// holdTx is a hold transaction as the state keeps it once, for the records
// of its outputs: its bytes, its proof once mined, and the height.
type holdTx struct {
	Raw    string `json:"raw"`
	Bump   string `json:"bump,omitempty"`
	Height uint32 `json:"height,omitempty"`
}

const (
	kindPiece   = "piece"
	kindEntry   = "entry"
	kindOutcome = "outcome"
)

// Open opens an identity, settles the funding trees a previous run left
// signed and unadopted, and resumes its outbox.
func Open(ctx context.Context, cfg Config) (*Publisher, error) {
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.PieceTree <= 0 {
		cfg.PieceTree = DefaultPieceTree
	}
	if cfg.EntryTree <= 0 {
		cfg.EntryTree = 16
	}
	cfg.PieceTree, cfg.EntryTree = min(cfg.PieceTree, mint.MaxFundingOutputs), min(cfg.EntryTree, mint.MaxFundingOutputs)
	if cfg.PieceAhead <= 0 {
		cfg.PieceAhead = cfg.PieceTree
	}
	if cfg.EntryAhead <= 0 {
		cfg.EntryAhead = max(cfg.EntryTree/2, 1)
	}
	if cfg.PieceWorkers <= 0 {
		cfg.PieceWorkers = 4
	}
	cfg.PieceWorkers = min(cfg.PieceWorkers, 32)
	if cfg.Poll <= 0 {
		cfg.Poll = time.Second
	}
	if cfg.HoldAhead <= 0 {
		cfg.HoldAhead = 8
	}
	if cfg.HoldWait <= 0 {
		cfg.HoldWait = 30 * time.Second
	}
	if cfg.Fees == (mint.Fees{}) {
		cfg.Fees = mint.DefaultFees
	}
	if cfg.Store == nil || cfg.Delivery == nil || cfg.Chain == nil || cfg.Tip == nil || cfg.Settler == nil {
		return nil, errors.New("publish: a store, a delivery, a chain view, a header source and a settlement leg are required")
	}
	lk, err := lockDirWait(ctx, cfg.Dir, cfg.LockWait)
	if err != nil {
		return nil, err
	}
	w, err := bwallet.Open(WalletDir(cfg.Dir), Profile)
	if err != nil {
		lk.release()
		return nil, fmt.Errorf("wallet: %w", err)
	}
	w.Mainnet = cfg.Mainnet
	w.Originator = "bstore-publisher"
	p := &Publisher{cfg: cfg, wallet: w, signer: w.Signer(), lock: lk}
	p.run, p.stop = context.WithCancel(context.WithoutCancel(ctx))
	ok := false
	defer func() {
		if !ok {
			p.stop()
			lk.release()
		}
	}()
	if p.st, err = loadState(cfg.Dir); err != nil {
		return nil, err
	}
	if p.st.Identity == "" {
		p.st.Identity = p.signer.IdentityHex()
	} else if p.st.Identity != p.signer.IdentityHex() {
		return nil, fmt.Errorf("publish: the state in %s belongs to %s, not this wallet's %s", cfg.Dir, p.st.Identity, p.signer.IdentityHex())
	}
	if n := p.repairProofs(); n > 0 {
		cfg.Logf("publisher state: %d proof(s) recorded by an earlier run cut to the minimal path", n)
	}
	p.kept = &producer.Kept{Load: p.loadKept}
	p.payer = &producer.Payer{Pool: w.Pool, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
		Kept: p.kept, Settler: cfg.Settler, Chain: cfg.Chain, Fees: p.fees(), Note: p.note, Poll: cfg.Poll, TipFunc: p.tip.Load}
	p.pieces = p.newTrees(kindPiece, storerec.PieceDerivation, cfg.PieceTree, cfg.PieceAhead)
	p.ents = p.newTrees(kindEntry, storerec.EntryDerivation, cfg.EntryTree, cfg.EntryAhead)
	p.outs = p.newTrees(kindOutcome, storerec.OutcomeDerivation, cfg.EntryTree, cfg.EntryAhead)
	if p.outbox, err = openOutbox(filepath.Join(cfg.Dir, "outbox"), cfg.Delivery, cfg.Store, cfg.Logf, cfg.Now); err != nil {
		return nil, err
	}
	p.outbox.Max = cfg.OutboxMax
	p.auth = newAuthClient(p, cfg.Delivery.HTTP)
	fh := cfg.FrontHTTP
	if fh == nil {
		fh = cfg.Delivery.HTTP
	}
	p.frontAuth = newAuthPool(p, fh)
	p.mergePayPools()
	if err := p.save(); err != nil {
		return nil, err
	}
	if err := p.recoverTrees(ctx); err != nil {
		return nil, err
	}
	p.holdAhead()
	ok = true
	return p, nil
}

// holdAhead gives the library every tree an earlier run minted ahead and
// did not use (the state's trees ahead), so that the spends switch to them
// before more are minted.
func (p *Publisher) holdAhead() {
	for _, kind := range []string{kindPiece, kindEntry, kindOutcome} {
		p.mu.Lock()
		ahead := slices.Clone(p.set(kind).Ahead)
		p.mu.Unlock()
		trees := p.treesOf(kind)
		for _, t := range ahead {
			if t.IdentityKeyHex != p.signer.IdentityHex() || t.Remaining() == 0 {
				continue
			}
			if err := trees.Hold(t); err != nil {
				p.cfg.Logf("funding tree %s, minted ahead by an earlier run: %v", short(t.Txid), err)
			}
		}
	}
}

// Close stops what the identity runs in the background (a funding tree
// minted ahead is settled on the next start from its record) and releases
// the identity's directory for another process.
func (p *Publisher) Close() {
	// A split of payment coins started by a call is let reach the leg:
	// once it has, its outputs are recorded.
	p.waitFill(30 * time.Second)
	if p.stop != nil {
		p.stop()
	}
	p.flushSaves()
	if p.lock != nil {
		p.lock.release()
		p.lock = nil
	}
}

// Dir is the identity's directory.
func (p *Publisher) Dir() string { return p.cfg.Dir }

// Chain is the chain view the identity asks.
func (p *Publisher) Chain() nodeapi.Chain { return p.cfg.Chain }

// bestTip is the header source's height, in the shape a node's best
// header had.
type bestTip struct{ Height uint32 }

func (p *Publisher) bestHeader(ctx context.Context) (bestTip, error) {
	h, err := p.cfg.Tip.CurrentHeight(ctx)
	return bestTip{Height: h}, err
}

// fees is the fee policy now.
func (p *Publisher) fees() mint.Fees {
	if p.cfg.FeeSource != nil {
		if f, err := p.cfg.FeeSource.Fees(p.run); err == nil {
			return f
		}
	}
	return p.cfg.Fees
}

// floor is the least fee a transaction pays.
func (p *Publisher) floor() uint64 { return p.fees().Floor }

// dust is the least change a transaction built here keeps as an output,
// as mint keeps its own: Fees.Dust, or the floor when Dust is zero.
func (p *Publisher) dust() uint64 { return dustOf(p.fees()) }

func dustOf(f mint.Fees) uint64 {
	if f.Dust == 0 {
		return f.Floor
	}
	return f.Dust
}

// feeAt is the fee a transaction of an estimated size pays, as
// mint.Fees.For prices it: the rate (capped at MaxRate) rounded up, at
// least the floor. Never the floor and the rate added together.
func (p *Publisher) feeAt(size uint64) uint64 { return feeAt(p.fees(), size) }

func feeAt(f mint.Fees, size uint64) uint64 {
	g := f
	g.Floor, g.Max = 0, 0
	fee, err := g.For(int(min(size, 1<<40))) //nolint:gosec // bounded above
	if err != nil {
		fee = 0
	}
	return max(fee, f.Floor)
}

// capFee holds a transaction built here rather than by package mint (the
// hold outputs, the payment coin split, a settlement, a reclaim) to the fee
// policy's per-transaction maximum (fee.max_tx), as mint holds its own:
// mint.Fees.For at the signed size, and the fee actually paid, change too
// small to keep included, each at most fees.Max. Overpaying is lost money,
// so it fails closed with mint.ErrFeeTooHigh before anything is sent.
func capFee(fees mint.Fees, tx *transaction.Transaction) error {
	if _, err := fees.For(tx.Size()); err != nil {
		return err
	}
	if fees.Max == 0 {
		return nil
	}
	paid, err := tx.GetFee()
	if err != nil {
		return err
	}
	if paid > fees.Max {
		return fmt.Errorf("%w: %d satoshis paid for %d bytes, maximum %d", mint.ErrFeeTooHigh, paid, tx.Size(), fees.Max)
	}
	return nil
}

// Delivery is how the identity reaches hosts and providers.
func (p *Publisher) Delivery() *Delivery { return p.cfg.Delivery }

// Store is the local closure store.
func (p *Publisher) Store() *Store { return p.cfg.Store }

// treesOf is the funding trees of kind.
func (p *Publisher) treesOf(kind string) *producer.Trees {
	switch kind {
	case kindPiece:
		return p.pieces
	case kindOutcome:
		return p.outs
	}
	return p.ents
}

// noteBucket records a bucket this identity publishes carriers in; the
// caller holds p.mu and saves.
func (p *Publisher) noteBucketLocked(bucket string) {
	if !slices.Contains(p.st.Buckets, bucket) {
		p.st.Buckets = append(p.st.Buckets, bucket)
		slices.Sort(p.st.Buckets)
	}
}

// Identity is the identity key, 33 bytes.
func (p *Publisher) Identity() []byte { return p.signer.Identity.Compressed() }

// IdentityHex is the identity key, 66 hex digits.
func (p *Publisher) IdentityHex() string { return p.signer.IdentityHex() }

// Wallet is the identity's wallet.
func (p *Publisher) Wallet() *bwallet.Embedded { return p.wallet }

// Outbox is what the identity has not delivered yet.
func (p *Publisher) Outbox() *Outbox { return p.outbox }

// Holds are the hold outputs this identity minted.
func (p *Publisher) Holds() []HoldRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.st.Holds)
}

func (p *Publisher) note(format string, args ...any) {
	if strings.HasPrefix(format, "funding tree published") {
		return
	}
	p.cfg.Logf(format, args...)
}

func carrierParams(d pushdrop.Derivation) carrier.Params {
	return carrier.Params{Derivation: d, FundingTag: storerec.TagFunding, ValidatePayload: func([]byte) error { return nil },
		ErrIdentity: storerec.ErrIdentity}
}

// DefaultPieceTree is a piece funding tree's outputs when none is set: a
// put of up to 1 GiB (256 pieces of 4 MiB) spends one tree, and the tree
// adds about 12.5 KB to each piece carrier's BEEF.
const DefaultPieceTree = 256

// PiecesFor is how many pieces a closure of size bytes has: its blobs, its
// branches and its root (CHIRP Profile 1, at most 256 children a node).
func PiecesFor(size uint64) int {
	n := storerec.Blobs(size)
	total := uint64(1)
	for n > 256 {
		n = (n + 255) / 256
		total += n
	}
	total += storerec.Blobs(size)
	if total > 1<<30 {
		return 1 << 30
	}
	return int(total)
}

func (p *Publisher) newTrees(kind string, d pushdrop.Derivation, count, ahead int) *producer.Trees {
	return &producer.Trees{Payer: p.payer, State: treeState{p, kind}, Identity: p.signer.IdentityHex(),
		Count: count, Sats: 1, Funder: "pool", Ahead: uint32(ahead),
		Lock: func(ctx context.Context) (*script.Script, error) {
			return carrier.FundingLock(ctx, p.wallet, p.wallet.Originator, carrierParams(d))
		},
		Change: p.signer.FundScript, Facade: keptFacade(), Topic: "tm_bstore_kept",
		Prepare: func(tree funding.Tree, coin bwallet.Output) error { return p.prepareTree(kind, tree, coin) }}
}

// set is the tree set of kind; the caller holds p.mu.
func (p *Publisher) set(kind string) *treeSet {
	switch kind {
	case kindPiece:
		return &p.st.Piece
	case kindOutcome:
		return &p.st.Outcome
	}
	return &p.st.Entry
}

type treeState struct {
	p    *Publisher
	kind string
}

func (t treeState) Current() *funding.Tree {
	t.p.mu.Lock()
	defer t.p.mu.Unlock()
	s := t.p.set(t.kind)
	if s.Tree == nil {
		return nil
	}
	c := *s.Tree
	return &c
}

func (t treeState) Adopt(tr funding.Tree) error {
	p := t.p
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.set(t.kind)
	s.Tree = &tr
	s.Trees = producer.Index(s.Trees, &tr)
	s.Ahead = slices.DeleteFunc(s.Ahead, func(a funding.Tree) bool { return a.Txid == tr.Txid })
	p.dropPending(tr.Txid)
	return p.saveLocked()
}

func (p *Publisher) dropPending(txid string) {
	p.st.Pending = slices.DeleteFunc(p.st.Pending, func(pt pendingTree) bool { return pt.Tree.Txid == txid })
}

func (p *Publisher) prepareTree(kind string, tree funding.Tree, coin bwallet.Output) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropPending(tree.Txid)
	p.st.Pending = append(p.st.Pending, pendingTree{Kind: kind, Tree: tree, Coin: coin})
	if err := p.saveLocked(); err != nil {
		p.dropPending(tree.Txid)
		return err
	}
	return nil
}

// recoverTrees settles every tree a run signed and did not adopt, before
// anything is spent (bcommon producer Recover).
func (p *Publisher) recoverTrees(ctx context.Context) error {
	p.mu.Lock()
	pending := slices.Clone(p.st.Pending)
	p.mu.Unlock()
	for _, pt := range pending {
		trees := p.treesOf(pt.Kind)
		id := pt.Tree.Txid
		if slices.ContainsFunc(trees.Held(), func(h funding.Tree) bool { return h.Txid == id }) {
			continue
		}
		p.mu.Lock()
		s := p.set(pt.Kind)
		adopted := slices.ContainsFunc(s.Trees, func(t funding.Tree) bool { return t.Txid == id })
		if adopted {
			p.dropPending(id)
			err := p.saveLocked()
			p.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		p.mu.Unlock()
		r, err := trees.Recover(ctx, pt.Tree, pt.Coin)
		if errors.Is(err, producer.ErrPublish) {
			err = nil // adopted; its publish sends nothing
		}
		if err != nil {
			p.cfg.Logf("funding tree %s was signed by a run that stopped before recording it, and what became of it could not be settled now (%v): its record is kept", short(id), err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		p.mu.Lock()
		switch r.Outcome {
		case producer.TreeAdopted, producer.CoinSpent:
			p.dropPending(id)
		case producer.TreeHeld:
			s := p.set(pt.Kind)
			s.Ahead = producer.Index(s.Ahead, &r.Tree)
		case producer.CoinReturned:
			for i := range p.st.Pending {
				if p.st.Pending[i].Tree.Txid != id {
					continue
				}
				if p.st.Pending[i].ReturnedOnce {
					p.dropPending(id)
					p.cfg.Logf("funding tree %s: the node still does not know it, so its record is dropped", short(id))
				} else {
					p.st.Pending[i].ReturnedOnce = true
					p.cfg.Logf("funding tree %s: its coin is back in the pool and its record kept for one more start", short(id))
				}
				break
			}
		}
		err = p.saveLocked()
		p.mu.Unlock()
		if err != nil {
			return err
		}
		p.cfg.Logf("funding tree %s: %s", short(id), r.Outcome)
	}
	return nil
}

// spend reserves need outputs of a mined tree of kind and records them used
// before it returns: no carrier is signed on an output the state does not
// already hold spent.
func (p *Publisher) spend(ctx context.Context, kind string, need uint32) (*transaction.Transaction, uint32, error) {
	trees := p.treesOf(kind)
	if h, err := p.bestHeader(ctx); err == nil {
		p.tip.Store(h.Height)
	}
	// The library answers the state's next outputs and the state records
	// them spent here: one spend of a kind at a time between the two, or
	// two puts would be answered the same outputs.
	tree, vout, gen, err := func() (*transaction.Transaction, uint32, uint64, error) {
		mu := p.spendLock(kind)
		mu.Lock()
		defer mu.Unlock()
		if err := p.promoteAhead(kind); err != nil {
			return nil, 0, 0, err
		}
		// The run's context, not the request's: a spend that leaves the tree
		// low starts the next one in the background, and that mint must
		// outlive this put. A spend that has to wait for a tree is bounded by
		// the payer's own timeout.
		tree, vout, err := trees.Spend(p.run, need)
		if errors.Is(err, producer.ErrPublish) {
			tree, vout, err = trees.Spend(p.run, need)
		}
		p.keepHeld(kind)
		if err != nil {
			return nil, 0, 0, feeError(fmt.Errorf("%s funding tree: %w", kind, err))
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		s := p.set(kind)
		id := tree.TxID().String()
		if s.Tree == nil || s.Tree.Txid != id {
			return nil, 0, 0, fmt.Errorf("publish: the %s tree spent is not the current one", kind)
		}
		if s.Tree.Next < vout+need {
			s.Tree.Next = vout + need
			s.Trees = producer.Index(s.Trees, s.Tree)
		}
		return tree, vout, p.markDirtyLocked(), nil
	}()
	if err != nil {
		return nil, 0, err
	}
	// The outputs are this put's once recorded; the record is durable
	// before a carrier is signed on them, and the spends of other puts do
	// not wait for this put's write (the saver batches them).
	if err := p.awaitSave(gen); err != nil {
		return nil, 0, err
	}
	return tree, vout, nil
}

// spendLock is the lock a spend of kind holds.
func (p *Publisher) spendLock(kind string) *sync.Mutex {
	switch kind {
	case kindPiece:
		return &p.spendMu[0]
	case kindOutcome:
		return &p.spendMu[2]
	}
	return &p.spendMu[1]
}

// keepHeld records every tree the library holds for a switch (a tree
// minted ahead, settled and mined) in the state's trees ahead, and drops its
// Prepare record: a crash before the switch leaves it where a sweep, and
// the next start's promoteAhead, find it. The library adopts it itself at
// the switch, which takes it out of the trees ahead again.
func (p *Publisher) keepHeld(kind string) {
	held := p.treesOf(kind).Held()
	if len(held) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.set(kind)
	changed := false
	for _, h := range held {
		if s.Tree != nil && s.Tree.Txid == h.Txid {
			continue
		}
		if !slices.ContainsFunc(s.Ahead, func(a funding.Tree) bool { return a.Txid == h.Txid }) {
			s.Ahead = append(s.Ahead, h)
			changed = true
		}
		if slices.ContainsFunc(p.st.Pending, func(pt pendingTree) bool { return pt.Tree.Txid == h.Txid }) {
			p.dropPending(h.Txid)
			changed = true
		}
	}
	if changed {
		if err := p.saveLocked(); err != nil {
			p.cfg.Logf("publisher state: %v", err)
		}
	}
}

// pieceRoom is how many outputs the current piece tree has left for this
// identity: a put spends them before it switches to the next tree.
func (p *Publisher) pieceRoom() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	t := p.st.Piece.Tree
	if t == nil || t.IdentityKeyHex != p.signer.IdentityHex() {
		return 0
	}
	return t.Remaining()
}

// promoteAhead makes a tree a previous run left held the current one when
// the current one is used up.
func (p *Publisher) promoteAhead(kind string) error {
	trees := p.treesOf(kind)
	p.mu.Lock()
	s := p.set(kind)
	cur := s.Tree
	if cur != nil && cur.Remaining() > 0 {
		p.mu.Unlock()
		return nil
	}
	ahead := slices.Clone(s.Ahead)
	p.mu.Unlock()
	held := trees.Held()
	for _, t := range ahead {
		if slices.ContainsFunc(held, func(h funding.Tree) bool { return h.Txid == t.Txid }) {
			continue
		}
		if t.IdentityKeyHex == p.signer.IdentityHex() && t.Remaining() > 0 {
			return treeState{p, kind}.Adopt(t)
		}
	}
	return nil
}

func (p *Publisher) loadKept(txid string) (*transaction.Transaction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range []*treeSet{&p.st.Piece, &p.st.Entry, &p.st.Outcome} {
		for _, list := range [][]funding.Tree{s.Trees, s.Ahead} {
			for _, t := range list {
				if t.Txid == txid {
					return rebuilt(funding.Rebuild(t.RawHex, t.BumpHex, t.BeefHex))
				}
			}
		}
		if s.Tree != nil && s.Tree.Txid == txid {
			return rebuilt(funding.Rebuild(s.Tree.RawHex, s.Tree.BumpHex, s.Tree.BeefHex))
		}
	}
	for _, pt := range p.st.Pending {
		if pt.Tree.Txid == txid {
			return rebuilt(funding.Rebuild(pt.Tree.RawHex, pt.Tree.BumpHex, pt.Tree.BeefHex))
		}
	}
	return nil, fmt.Errorf("publish: %s is not a transaction this identity keeps", txid)
}

// rebuilt is a kept transaction with its txid cached: the library hands the
// one object to every carrier spending it (producer.Kept), and each asks
// its txid (AddInputFromTx), which would otherwise serialize and hash the
// tree's outputs again per piece. A kept transaction does not change.
func rebuilt(tx *transaction.Transaction, err error) (*transaction.Transaction, error) {
	if err == nil {
		tx.SetTxHash(tx.TxID())
	}
	return tx, err
}

func (p *Publisher) save() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saveLocked()
}

// compact is the state as it is saved: each hold transaction once, in
// HoldTxs, and the records of its outputs without their copies of it. The
// state is saved on every take, so its size is what a put pays for; a hold
// transaction of 16 outputs kept 16 times over was most of it.
func (s *state) compact() *state {
	c := *s
	c.HoldTxs = make(map[string]holdTx, len(s.Holds)/8+1)
	c.Holds = make([]HoldRecord, len(s.Holds))
	for i, h := range s.Holds {
		if h.RawHex != "" {
			if t, ok := c.HoldTxs[h.Txid]; !ok || (t.Bump == "" && h.BumpHex != "") {
				c.HoldTxs[h.Txid] = holdTx{Raw: h.RawHex, Bump: h.BumpHex, Height: h.Height}
			}
			h.RawHex, h.BumpHex, h.Height = "", "", 0
		}
		c.Holds[i] = h
	}
	return &c
}

// expand gives each hold record its transaction back from HoldTxs, so that
// in memory every record carries it, as records saved by earlier builds
// do. Called once the state is loaded.
func (s *state) expand() {
	for i := range s.Holds {
		h := &s.Holds[i]
		if h.RawHex != "" {
			continue
		}
		if t, ok := s.HoldTxs[h.Txid]; ok {
			h.RawHex, h.BumpHex, h.Height = t.Raw, t.Bump, t.Height
		}
	}
	s.HoldTxs = nil
}

func loadState(dir string) (*state, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "publisher.json"))
	if errors.Is(err, os.ErrNotExist) {
		return &state{}, nil
	}
	if err != nil {
		return nil, err
	}
	var st state
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("publisher.json: %w", err)
	}
	st.expand()
	return &st, nil
}

// feeError words the library's refusals to find a fee input.
func feeError(err error) error {
	var nc *producer.NoCoinError
	if errors.As(err, &nc) {
		if nc.Held > 0 || nc.Minting > 0 {
			return fmt.Errorf("fee input: %w: change from %d unproven transaction(s) and %d tree(s) minting; it becomes spendable once mined", nc.Err, nc.Held, nc.Minting)
		}
		return fmt.Errorf("fee input: %w (fund the identity)", nc.Err)
	}
	return err
}

func short(txid string) string {
	if len(txid) > 12 {
		return txid[:12]
	}
	return txid
}
