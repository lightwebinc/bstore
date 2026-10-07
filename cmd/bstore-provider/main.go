// Command bstore-provider is a bstore storage provider: the market front of
// spec section 17 over the native content-addressed hold backend. See
// docs/provider.md.
//
//	bstore-provider init -data DIR       create the provider's identity and wallet
//	bstore-provider address -config FILE the fund address fees are paid from
//	bstore-provider fund -config FILE -txid TXID
//	                                     import a mined payment you sent to the fund address
//	bstore-provider rescan -config FILE -from H [-to H]
//	                                     coinbase: only on a regtest chain you run (development
//	                                     and tests); take coinbase paying the fund address into the pool
//	bstore-provider fund -config FILE -blocks N
//	                                     coinbase: only on a regtest chain you run (development
//	                                     and tests); mine N blocks to the fund address
//	bstore-provider run -config FILE     serve until interrupted
//	bstore-provider doctor -config FILE [-ca FILE]
//	                                     what the provider holds and whether everything answers; reads only
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
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/internal/fundimport"
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
  fund -config FILE -txid TXID   import a mined payment you sent to the fund address: every
                                 output paying it that the node shows unspent, its proof checked
                                 against the header source (mainnet, testnet)
  rescan -config FILE -from H [-to H]
                                 coinbase: only on a regtest chain you run (development and tests);
                                 take coinbase paying the fund address in blocks H.. into the pool
  fund -config FILE -blocks N    coinbase: only on a regtest chain you run (development and tests);
                                 mine N blocks to the fund address through the node's RPC
                                 (settle rpc:<url>) and pool the coinbase
  run -config FILE               serve until interrupted
  doctor -config FILE [-ca FILE] the identity, the pool, the store, and whether the node, the header
                                 source, the hosts and the provider's own front answer; reads only
                                 (-ca: a PEM file trusted for the front's certificate)
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
	txid := fs.String("txid", "", "import the outputs of a mined payment to the fund address (fund; mainnet, testnet)")
	from := fs.Uint("from", 0, "coinbase: only on a regtest chain you run (development and tests): first block height to rescan for coinbase")
	to := fs.Uint("to", 0, "last block height to rescan for coinbase (default the tip)")
	blocks := fs.Int("blocks", 0, "coinbase: only on a regtest chain you run (development and tests): blocks to mine (fund)")
	caFile := fs.String("ca", "", "a PEM file trusted for the front's certificate (doctor)")
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
	case "address", "rescan", "fund", "run", "doctor":
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
	case "doctor":
		return provider.Doctor(ctx, cfg, *caFile, os.Stdout)
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
		if args[0] == "fund" && *txid != "" {
			hc, err := headers.NewSource(cfg.Headers)
			if err != nil {
				return usageError{fmt.Errorf("headers: %w", err)}
			}
			res, err := fundimport.Import(ctx, w.Signer(), w.Pool, asset, hc, *txid)
			if err != nil {
				return err
			}
			fmt.Printf("%s mined at height %d: %d output(s) to the fund address, %d imported (%d satoshis), %d already held, %d spent\n",
				res.TxID, res.Height, res.Paying, res.Added, res.Satoshis, res.Held, res.Spent)
			fmt.Printf("pool %d output(s), %d satoshis\n", w.Pool.Count(), w.Pool.Balance())
			return nil
		}
		if args[0] == "fund" {
			kind, addr, _ := strings.Cut(cfg.Settle, ":")
			if kind != "rpc" || *blocks < 1 {
				return usageError{errors.New("fund needs -txid TXID, or (coinbase: only on a regtest chain you run) settle rpc:<url> and -blocks N")}
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
