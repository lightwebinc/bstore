// Package chaincfg builds what every bstore program reads the chain through
// from its configuration: the chain view (transactions, proofs, spends), the
// header source and the tip, the settlement leg, and the miner fee policy.
// The writer's home, the S3 gateway and the provider share it, so each
// backend is configuration and the same keys mean the same thing in each.
//
// Nothing here needs a node. On mainnet and testnet the defaults are the
// zero-infrastructure profile: WhatsOnChain for the chain view and the
// headers (every header held to its proof of work, every proof to those
// headers) and GorillaPool's public arcade to broadcast. A node is still the
// better chain view ("asset:URL"): it removes the one trust WhatsOnChain
// adds, its word that an output is unspent.
package chaincfg

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	bpub "github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/bstore/internal/efleg"
)

// Spec is the chain, settlement and fee settings of one configuration.
// Every field but Network is optional on mainnet and testnet.
type Spec struct {
	// Network is main, test or regtest. A regtest chain has no public
	// services: it needs Chain (or Node), Settle and Headers.
	Network string
	// Chain is a nodeapi.ParseChain specification (woc:main, woc:test,
	// asset:URL, or a list); empty is woc:<network>. Node is the older
	// key, a node's asset API URL, read as asset:<Node>; both set is
	// refused.
	Chain string
	Node  string
	// Settle is a publish.ParseSettler specification; empty is
	// publish.DefaultSettle(network), GorillaPool's arcade. ArcadeKey is
	// the bearer token of an arcade or ARC leg; RPCUser, RPCPass and RPCID
	// authenticate an rpc: leg (RPCID default "bstore").
	Settle    string
	ArcadeKey string
	RPCUser   string
	RPCPass   string
	RPCID     string
	// Headers is a headers.New specification; empty is woc:<network>.
	// HeadersToken is a bearer token for a block-headers-service.
	Headers      string
	HeadersToken string
	// WoCKey is a WhatsOnChain API key and WoCRate its requests a second
	// (zero is the free tier's 3). A busy program needs one, or a node.
	WoCKey  string
	WoCRate float64
	// Fee is the fee block; FeeRate and FeeFloor the older keys
	// fee_sat_per_byte and fee_floor (zero: absent), folded in by
	// feepolicy.MergeLegacy.
	Fee      *feepolicy.Config
	FeeRate  uint64
	FeeFloor uint64
	// Client, when set, is every backend's HTTP client.
	Client *http.Client
}

// ErrSpec is a configuration that cannot be used.
var ErrSpec = errors.New("chaincfg")

func (s Spec) network() (string, error) {
	switch s.Network {
	case "main", "test", "regtest":
		return s.Network, nil
	}
	return "", fmt.Errorf("%w: network is main, test or regtest, not %q", ErrSpec, s.Network)
}

// ChainSpec is the chain view specification in force.
func (s Spec) ChainSpec() (string, error) {
	n, err := s.network()
	if err != nil {
		return "", err
	}
	switch {
	case s.Chain != "" && s.Node != "":
		return "", fmt.Errorf("%w: chain and node are both set; node is the older spelling of chain asset:URL, keep one", ErrSpec)
	case s.Chain != "":
		return s.Chain, nil
	case s.Node != "":
		return "asset:" + strings.TrimRight(s.Node, "/"), nil
	case n == "regtest":
		return "", fmt.Errorf("%w: a regtest chain has no public chain view: set chain asset:URL (your node)", ErrSpec)
	}
	return "woc:" + n, nil
}

// SettleSpec is the settlement specification in force.
func (s Spec) SettleSpec() (string, error) {
	n, err := s.network()
	if err != nil {
		return "", err
	}
	if s.Settle != "" {
		return s.Settle, nil
	}
	if n == "regtest" {
		return "", fmt.Errorf("%w: a regtest chain has no public broadcaster: set settle rpc:URL (your node)", ErrSpec)
	}
	return bpub.DefaultSettle(n)
}

// HeadersSpec is the header source specification in force.
func (s Spec) HeadersSpec() (string, error) {
	n, err := s.network()
	if err != nil {
		return "", err
	}
	if s.Headers != "" {
		return s.Headers, nil
	}
	if n == "regtest" {
		return "", fmt.Errorf("%w: a regtest chain has no public header source: set headers", ErrSpec)
	}
	return "woc:" + n, nil
}

// FeeConfig is the fee block with the older keys folded in.
func (s Spec) FeeConfig() (*feepolicy.Config, error) {
	var rate, floor *uint64
	if s.FeeRate != 0 {
		rate = &s.FeeRate
	}
	if s.FeeFloor != 0 {
		floor = &s.FeeFloor
	}
	return feepolicy.MergeLegacy(s.Fee, rate, floor)
}

// Fees is the static fees the configuration names (a live policy's
// fallback), over mint.DefaultFees.
func (s Spec) Fees() (mint.Fees, error) {
	c, err := s.FeeConfig()
	if err != nil {
		return mint.Fees{}, err
	}
	return c.Fees(mint.DefaultFees)
}

// Check refuses a configuration that cannot be opened, reaching nothing.
func (s Spec) Check() error {
	cs, err := s.ChainSpec()
	if err != nil {
		return err
	}
	if _, err := nodeapi.ParseChain(cs, nodeapi.ChainOptions{Headers: headers.New("woc:main")}); err != nil {
		return err
	}
	ss, err := s.SettleSpec()
	if err != nil {
		return err
	}
	if _, _, err := bpub.ParseSettler(ss, bpub.SettleOptions{}); err != nil {
		return err
	}
	hs, err := s.HeadersSpec()
	if err != nil {
		return err
	}
	if _, err := headers.NewSource(hs); err != nil {
		return fmt.Errorf("headers: %w", err)
	}
	_, err = s.feeSource(ss)
	return err
}

// feeSource builds the fee policy. A live policy with no URLs of its own
// reads the policy of the arcade or ARC the leg settles through.
func (s Spec) feeSource(settle string) (feepolicy.Source, error) {
	c, err := s.FeeConfig()
	if err != nil {
		return nil, err
	}
	if c.Source != "" && c.Source != feepolicy.SourceStatic && len(c.PolicyURLs) == 0 {
		_, arcade, err := bpub.ParseSettler(settle, bpub.SettleOptions{})
		if err != nil || arcade == nil {
			return nil, fmt.Errorf("%w: fee.source %s reads a broadcaster's policy: set fee.policy_urls, or settle through arcade or arc", ErrSpec, c.Source)
		}
		c.PolicyURLs = []string{arcade.Base}
	}
	src, err := c.Build(mint.DefaultFees)
	if err != nil {
		return nil, err
	}
	if a, ok := src.(*feepolicy.ARC); ok {
		a.Key, a.Client = s.ArcadeKey, s.Client
	}
	return src, nil
}

// Tip is the chain's height and a block's header by height, from the
// header source.
type Tip interface {
	CurrentHeight(ctx context.Context) (uint32, error)
	HeaderAt(ctx context.Context, height uint32) (*headers.Header, error)
}

// spendTTL is how long one read of a transaction's outputs answers for them.
const spendTTL = 2 * time.Second

// CacheSpends is for a long-running service, whose answers may be spendTTL
// old in exchange for reading a transaction once. A hold transaction holds up
// to 1,024 outputs and a node answers for all of them at once: one read serves
// every entry on that transaction, where a read per entry kept a node's cores
// busy with no put running. A command that acts once on what it reads keeps
// the plain view.
func (v *View) CacheSpends() { v.Chain.CacheSpends(spendTTL) }

// View is an opened configuration.
type View struct {
	Network string
	// ChainSpec, SettleSpec and HeadersSpec are the specifications in
	// force, for a status line.
	ChainSpec, SettleSpec, HeadersSpec string
	// Chain is the chain view: arcade's proofs of what it was sent first
	// when the leg is arcade or ARC, then the configured backends, every
	// proof checked against Headers.
	Chain *nodeapi.Sources
	// Known says whether the network holds a transaction, mined or not:
	// the chain view's word, as a node's was. Not arcade's: arcade knows
	// what it was sent even when the network refused it, and a
	// transaction refused must not read as one waiting to mine.
	Known nodeapi.KnownSource
	// Asset is the node, when the chain view names one (asset:URL):
	// coinbase funding on a regtest chain needs it.
	Asset *nodeapi.Asset
	// Headers is the header source; Tip reads the height and a header by
	// height from it.
	Headers *headers.Client
	Tip     Tip
	// Settler is the settlement leg, every input given its source
	// transaction where the leg wants one; Arcade the leg itself when it
	// is arcade or ARC, RPC when it is a node's.
	Settler bpub.Settler
	Arcade  *bpub.Arcade
	RPC     *nodeapi.RPC
	// FeeSource answers the fees a build uses: static, or a broadcaster's
	// live policy (fee.source arc).
	FeeSource feepolicy.Source
}

// Open builds the backends a configuration names. It reaches nothing.
func Open(s Spec) (*View, error) {
	if err := s.Check(); err != nil {
		return nil, err
	}
	cs, _ := s.ChainSpec()
	ss, _ := s.SettleSpec()
	hs, _ := s.HeadersSpec()
	hc, err := headers.NewSource(hs)
	if err != nil {
		return nil, fmt.Errorf("headers: %w", err)
	}
	hc.Token = s.HeadersToken
	if s.Client != nil {
		hc.HTTP = s.Client
	}
	chain, err := nodeapi.ParseChain(cs, nodeapi.ChainOptions{WoCKey: s.WoCKey, WoCRate: s.WoCRate, Client: s.Client, Headers: hc})
	if err != nil {
		return nil, err
	}
	v := &View{Network: s.Network, ChainSpec: cs, SettleSpec: ss, HeadersSpec: hs, Chain: chain, Known: chain, Headers: hc}
	for _, t := range chain.Tx {
		if a, ok := t.(*nodeapi.Asset); ok {
			v.Asset = a
			break
		}
	}
	v.Tip = tip{h: hc, asset: v.Asset}
	if s.RPCID == "" {
		s.RPCID = "bstore"
	}
	leg, arcade, err := bpub.ParseSettler(ss, bpub.SettleOptions{Key: s.ArcadeKey, RPCUser: s.RPCUser, RPCPass: s.RPCPass, RPCID: s.RPCID,
		Client: s.Client, Spends: chain})
	if err != nil {
		return nil, err
	}
	v.Arcade = arcade
	if r, ok := leg.(*bpub.RPCSettler); ok {
		v.RPC = r.RPC
		v.Settler = leg
	} else {
		// arcade and a fabric ingress take extended format: each input
		// with its source transaction.
		v.Settler = efleg.Wrap(leg, chain)
	}
	if arcade != nil {
		chain.Proofs = append([]nodeapi.ProofSource{nodeapi.Checked{Source: arcade, Headers: hc}}, chain.Proofs...)
	}
	if v.FeeSource, err = s.feeSource(ss); err != nil {
		return nil, err
	}
	return v, nil
}

// Fees is the fee policy now. A live policy never fails on the network: it
// answers its last good value, then the static fees.
func (v *View) Fees(ctx context.Context) mint.Fees {
	f, err := v.FeeSource.Fees(ctx)
	if err != nil {
		return mint.DefaultFees
	}
	return f
}

// tip reads the header source. A bridge header source serves no header
// fields, so a header by height is then the node's, when there is one.
type tip struct {
	h     *headers.Client
	asset *nodeapi.Asset
}

func (t tip) CurrentHeight(ctx context.Context) (uint32, error) { return t.h.CurrentHeight(ctx) }

func (t tip) HeaderAt(ctx context.Context, height uint32) (*headers.Header, error) {
	if t.h.Kind == headers.Native && t.asset != nil {
		s, err := t.asset.HashAtHeight(ctx, height)
		if err != nil {
			return nil, err
		}
		hash, err := chainhash.NewHashFromHex(s)
		if err != nil {
			return nil, fmt.Errorf("the node's hash at height %d: %w", height, err)
		}
		return &headers.Header{Height: height, Hash: *hash}, nil
	}
	return t.h.HeaderAt(ctx, height)
}

// probeTxid names no transaction: a chain view that answers "not known"
// for it is answering.
const probeTxid = "0000000000000000000000000000000000000000000000000000000000000001"

// Line is one line of a status report.
type Line struct{ Label, Text string }

// Report reads, and spends nothing: whether the chain view, the header
// source and the settlement leg answer, and the fee policy in force.
func (v *View) Report(ctx context.Context) []Line {
	var out []Line
	switch {
	case v.Asset != nil:
		if h, err := v.Asset.BestHeader(ctx); err != nil {
			out = append(out, Line{"chain", v.ChainSpec + ": " + firstLine(err)})
		} else {
			out = append(out, Line{"chain", fmt.Sprintf("%s: node tip %d", v.ChainSpec, h.Height)})
		}
	default:
		if _, err := v.Chain.Known(ctx, probeTxid); err != nil {
			out = append(out, Line{"chain", v.ChainSpec + ": " + firstLine(err)})
		} else {
			out = append(out, Line{"chain", v.ChainSpec + ": answers"})
		}
	}
	if h, err := v.Headers.CurrentHeight(ctx); err != nil {
		out = append(out, Line{"headers", v.HeadersSpec + ": " + firstLine(err)})
	} else {
		out = append(out, Line{"headers", fmt.Sprintf("%s: height %d", v.HeadersSpec, h)})
	}
	switch {
	case v.Arcade != nil:
		if err := v.Arcade.Ping(ctx); err != nil {
			out = append(out, Line{"settle", v.SettleSpec + ": " + firstLine(err)})
		} else {
			out = append(out, Line{"settle", v.SettleSpec + ": answers"})
		}
	default:
		out = append(out, Line{"settle", v.SettleSpec})
	}
	return append(out, v.feeLine(ctx))
}

// feeLine is the fee policy in force: the rate paid, a rate above the cap
// shown lowered to it.
func (v *View) feeLine(ctx context.Context) Line {
	f := v.Fees(ctx)
	rate := f.Rate
	if rate.IsZero() {
		rate.Sats, rate.Bytes = f.SatPerByte, 1
	}
	if !f.MaxRate.IsZero() && rate.Cmp(f.MaxRate) > 0 {
		rate = f.MaxRate
	}
	src := feepolicy.SourceStatic
	if a, ok := v.FeeSource.(*feepolicy.ARC); ok {
		st := a.Status()
		src = st.Source
		if st.Err != nil {
			src += " (" + firstLine(st.Err) + ")"
		}
	}
	return Line{"fee", fmt.Sprintf("%s satoshis/bytes, floor %d, %s", rate, f.Floor, src)}
}

func firstLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}
