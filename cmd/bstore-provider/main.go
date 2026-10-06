// Command bstore-provider is a bstore storage provider: the market front of
// spec section 17 over the native content-addressed hold backend. See
// docs/provider.md.
//
//	bstore-provider init -data DIR       create the provider's identity and wallet
//	bstore-provider address -config FILE the fund address fees are paid from
//	bstore-provider rescan -config FILE -from H [-to H]
//	                                     take coin paid to the fund address into the pool
//	bstore-provider fund -config FILE -blocks N
//	                                     a test network only: mine N blocks to the fund address
//	bstore-provider run -config FILE     serve until interrupted
//	bstore-provider version              print the version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/provider"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bstore-provider:", err)
		var u usageError
		if errors.As(err, &u) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

type usageError struct{ error }

// version is stamped by the build; "dev" otherwise.
var version = "dev"

const usage = `usage: bstore-provider <command> [flags]

commands:
  init -data DIR                 create the identity and wallet under DIR
  address -config FILE           print the identity key and the fund address
  rescan -config FILE -from H [-to H]
                                 take coin paid to the fund address in blocks H.. into the pool
  fund -config FILE -blocks N    a test network only: mine N blocks to the fund address
                                 through the node's RPC (settle rpc:<url>) and pool the coinbase
  run -config FILE               serve until interrupted
  version                        print the version

docs/provider.md is the operator's guide: the configuration file, the
terms, the limits and the metrics.`

func run(args []string) error {
	if len(args) == 0 {
		return usageError{errors.New(usage)}
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		fmt.Println(usage)
		return nil
	case "version", "-version", "--version":
		fmt.Println("bstore-provider", version)
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	data := fs.String("data", "", "data directory (init)")
	cfgPath := fs.String("config", "", "configuration file")
	from := fs.Uint("from", 0, "first block height to rescan")
	to := fs.Uint("to", 0, "last block height to rescan (default the tip)")
	blocks := fs.Int("blocks", 0, "blocks to mine (fund)")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return usageError{err}
	}
	switch args[0] {
	case "init":
		if *data == "" {
			return usageError{errors.New("init needs -data")}
		}
		id, err := provider.Init(*data)
		if err != nil {
			return err
		}
		fmt.Println("identity", id)
		return nil
	case "address", "rescan", "fund", "run":
	default:
		return usageError{fmt.Errorf("unknown command %q\n%s", args[0], usage)}
	}
	if *cfgPath == "" {
		return usageError{errors.New(args[0] + " needs -config")}
	}
	cfg, err := provider.LoadConfig(*cfgPath)
	if err != nil {
		return usageError{err}
	}
	switch args[0] {
	case "address":
		w, err := bwallet.Open(provider.WalletDir(cfg.Data), provider.Profile)
		if err != nil {
			return err
		}
		addr, err := w.Signer().FundAddress(cfg.Mainnet)
		if err != nil {
			return err
		}
		fmt.Println("identity", w.Signer().IdentityHex())
		fmt.Println("fund address", addr)
		fmt.Printf("pool %d output(s), %d satoshis\n", w.Pool.Count(), w.Pool.Balance())
		return nil
	case "rescan", "fund":
		w, err := bwallet.Open(provider.WalletDir(cfg.Data), provider.Profile)
		if err != nil {
			return err
		}
		asset := &nodeapi.Asset{Base: cfg.Node}
		if args[0] == "fund" {
			kind, addr, _ := strings.Cut(cfg.Settle, ":")
			if kind != "rpc" || *blocks < 1 {
				return usageError{errors.New("fund needs settle rpc:<url> and -blocks N (a test network's node)")}
			}
			rpc := &nodeapi.RPC{URL: addr, User: cfg.RPCUser, Pass: cfg.RPCPass, ID: "bstore-provider"}
			n, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, rpc, asset, *blocks, 50)
			if err != nil {
				return err
			}
			fmt.Printf("%d coinbase output(s) pooled; pool %d satoshis\n", n, w.Pool.Balance())
			return nil
		}
		top := uint32(*to)
		if top == 0 {
			h, err := asset.BestHeader(ctx)
			if err != nil {
				return err
			}
			top = h.Height
		}
		n, err := bwallet.Rescan(ctx, w.Signer(), w.Pool, asset, uint32(*from), top)
		if err != nil {
			return err
		}
		fmt.Printf("%d output(s) found; pool %d satoshis\n", n, w.Pool.Balance())
		return nil
	}
	p, err := provider.New(ctx, cfg, provider.Options{})
	if err != nil {
		return err
	}
	if err := p.Start(ctx); err != nil {
		return err
	}
	fmt.Printf("bstore-provider %s listening on %s, front %s\n", p.Identity(), p.Addr(), cfg.FrontURL())
	<-ctx.Done()
	return p.Close()
}
