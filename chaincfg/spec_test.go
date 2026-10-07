package chaincfg

import (
	"context"
	"strings"
	"testing"

	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
)

// With nothing configured, mainnet and testnet need no node: WhatsOnChain
// for the chain view and the headers, GorillaPool's arcade to broadcast,
// the network's fee rate.
func TestZeroInfrastructureDefaults(t *testing.T) {
	for _, n := range []string{"main", "test"} {
		v, err := Open(Spec{Network: n})
		if err != nil {
			t.Fatal(err)
		}
		if v.ChainSpec != "woc:"+n || v.HeadersSpec != "woc:"+n || v.SettleSpec != "arcade:"+n {
			t.Fatalf("%s: chain %s headers %s settle %s", n, v.ChainSpec, v.HeadersSpec, v.SettleSpec)
		}
		if v.Arcade == nil || v.Asset != nil || v.RPC != nil {
			t.Fatalf("%s: arcade %v asset %v rpc %v", n, v.Arcade, v.Asset, v.RPC)
		}
		if _, ok := v.Chain.Spends.(*nodeapi.WoC); !ok {
			t.Fatalf("%s: spends from %T", n, v.Chain.Spends)
		}
		if v.Known != v.Chain || v.Arcade.Spends != v.Chain {
			t.Fatalf("%s: known %T, arcade's spend view %T", n, v.Known, v.Arcade.Spends)
		}
		if f := v.Fees(context.Background()); f != mint.DefaultFees {
			t.Fatalf("%s: fees %+v", n, f)
		}
	}
}

func TestSpecs(t *testing.T) {
	// node is the older spelling of chain asset:URL.
	v, err := Open(Spec{Network: "regtest", Node: "http://node:8090/", Settle: "rpc:http://node:9292", Headers: "http://node:8090"})
	if err != nil {
		t.Fatal(err)
	}
	if v.ChainSpec != "asset:http://node:8090" || v.Asset == nil || v.Asset.Base != "http://node:8090" || v.RPC == nil || v.Known != v.Chain {
		t.Fatalf("legacy node: %+v", v)
	}
	// A WhatsOnChain key and rate reach the client.
	v, err = Open(Spec{Network: "main", WoCKey: "k", WoCRate: 20})
	if err != nil {
		t.Fatal(err)
	}
	if w := v.Chain.Spends.(*nodeapi.WoC); w.Key != "k" || w.Rate != 20 {
		t.Fatalf("woc %+v", w)
	}
	r := mint.Rate{Sats: 1, Bytes: 1}
	for name, s := range map[string]Spec{
		"no network":          {},
		"chain and node":      {Network: "main", Chain: "woc:main", Node: "http://n"},
		"regtest chain":       {Network: "regtest", Settle: "rpc:http://n", Headers: "http://n"},
		"regtest settle":      {Network: "regtest", Chain: "asset:http://n", Headers: "http://n"},
		"regtest headers":     {Network: "regtest", Chain: "asset:http://n", Settle: "rpc:http://n"},
		"bad chain":           {Network: "main", Chain: "junglebus:main"},
		"bad settle":          {Network: "main", Settle: "ftp:x"},
		"bad headers":         {Network: "main", Headers: "nope:x"},
		"fee twice":           {Network: "main", FeeRate: 1, Fee: &feepolicy.Config{Rate: &r}},
		"live fee, no arcade": {Network: "main", Settle: "tcp:h:1", Fee: &feepolicy.Config{Source: "arc"}},
		"fee source":          {Network: "main", Fee: &feepolicy.Config{Source: "oracle"}},
	} {
		if err := s.Check(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The older fee keys still mean what they meant; a live policy with no URLs
// reads the arcade the leg settles through.
func TestFees(t *testing.T) {
	v, err := Open(Spec{Network: "main", FeeRate: 1, FeeFloor: 300})
	if err != nil {
		t.Fatal(err)
	}
	if f := v.Fees(context.Background()); f.Rate != (mint.Rate{Sats: 1, Bytes: 1}) || f.Floor != 300 {
		t.Fatalf("legacy fees %+v", f)
	}
	v, err = Open(Spec{Network: "test", Fee: &feepolicy.Config{Source: "arc"}, ArcadeKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	a, ok := v.FeeSource.(*feepolicy.ARC)
	if !ok || len(a.URLs) != 1 || !strings.Contains(a.URLs[0], "testnet.arcade.gorillapool.io") || a.Key != "k" {
		t.Fatalf("live policy %+v", v.FeeSource)
	}
}
