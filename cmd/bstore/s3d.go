// The S3 gateway, `bstore s3d` (spec section 16, docs/s3.md):
//
//	bstore s3d init -config FILE              create every identity's wallet the configuration names
//	bstore s3d address -config FILE -identity NAME
//	bstore s3d rescan -config FILE -identity NAME -from H [-to H]
//	bstore s3d fund -config FILE -identity NAME -blocks N   a test network only
//	bstore s3d run -config FILE               serve until interrupted
package main

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/s3d"
)

const s3dUsage = `usage: bstore s3d <command> [flags]

commands:
  init -config FILE                       create the wallet of every identity the configuration names
  address -config FILE -identity NAME     print an identity's key, fund address and pool
  rescan -config FILE -identity NAME -from H [-to H]
                                          take coin paid to the fund address in blocks H.. into the pool
  fund -config FILE -identity NAME -blocks N
                                          a test network only: mine N blocks to the fund address
                                          through the node's RPC (settle rpc:<url>) and pool the coinbase
  run -config FILE                        serve S3 until interrupted`

// cmdS3D is `bstore s3d`, the S3 gateway.
func cmdS3D(ctx context.Context, g *global, args []string) error {
	if len(args) < 1 {
		return usage("%s", s3dUsage)
	}
	if isHelp(args[0]) {
		g.say("%s", s3dUsage)
		return nil
	}
	cmd := args[0]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(g.stderr)
	cfgPath := fs.String("config", "", "configuration file")
	ident := fs.String("identity", "", "identity name")
	from := fs.Uint("from", 0, "first block height to rescan")
	to := fs.Uint("to", 0, "last block height to rescan (default the tip)")
	blocks := fs.Int("blocks", 0, "blocks to mine (fund)")
	if err := fs.Parse(args[1:]); err != nil {
		return helpOrUsage(err)
	}
	switch cmd {
	case "init", "address", "rescan", "fund", "run":
	default:
		return usage("unknown s3d command %q\n%s", cmd, s3dUsage)
	}
	if *cfgPath == "" {
		return usage("%s needs -config", cmd)
	}
	cfg, err := s3d.LoadConfig(*cfgPath)
	if err != nil {
		return usage("%v", err)
	}
	switch cmd {
	case "init":
		for _, name := range cfg.Identities {
			dir := cfg.IdentityDir(name)
			if w, err := bwallet.Open(publish.WalletDir(dir), publish.Profile); err == nil {
				fmt.Fprintln(g.stdout, "identity", name, w.Signer().IdentityHex(), "(exists)")
				continue
			}
			id, err := publish.Init(dir)
			if err != nil {
				return err
			}
			fmt.Fprintln(g.stdout, "identity", name, id)
		}
		return nil
	case "address", "rescan", "fund":
		if !slices.Contains(cfg.Identities, *ident) {
			return usage("%s needs -identity, one of %s", cmd, strings.Join(cfg.Identities, ", "))
		}
		w, err := bwallet.Open(publish.WalletDir(cfg.IdentityDir(*ident)), publish.Profile)
		if err != nil {
			return fmt.Errorf("wallet (bstore s3d init creates it): %w", err)
		}
		asset := &nodeapi.Asset{Base: cfg.Node}
		switch cmd {
		case "address":
			addr, err := w.Signer().FundAddress(cfg.Mainnet)
			if err != nil {
				return err
			}
			fmt.Fprintln(g.stdout, "identity", w.Signer().IdentityHex())
			fmt.Fprintln(g.stdout, "fund address", addr)
			fmt.Fprintf(g.stdout, "pool %d output(s), %d satoshis\n", w.Pool.Count(), w.Pool.Balance())
			return nil
		case "fund":
			kind, addr, _ := strings.Cut(cfg.Settle, ":")
			if kind != "rpc" || *blocks < 1 {
				return usage("fund needs settle rpc:<url> and -blocks N (a test network's node)")
			}
			rpc := &nodeapi.RPC{URL: addr, User: cfg.RPCUser, Pass: cfg.RPCPass, ID: "bstore-s3d"}
			n, _, err := bwallet.FundFromCoinbase(ctx, w.Signer(), w.Pool, rpc, asset, *blocks, 50)
			if err != nil {
				return err
			}
			fmt.Fprintf(g.stdout, "%d coinbase output(s) pooled; pool %d satoshis\n", n, w.Pool.Balance())
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
		fmt.Fprintf(g.stdout, "%d output(s) found; pool %d satoshis\n", n, w.Pool.Balance())
		return nil
	}
	// A command holding an identity (bstore holds against a stopped
	// gateway) is waited for, not raced: the gateway starts once it ends.
	s, err := s3d.New(ctx, cfg, s3d.Options{LockWait: 10 * time.Minute})
	if err != nil {
		return err
	}
	if err := s.Start(ctx); err != nil {
		return err
	}
	for _, name := range cfg.Identities {
		fmt.Fprintf(g.stdout, "bstore s3d identity %s %s\n", name, s.Identity(name))
	}
	fmt.Fprintf(g.stdout, "bstore s3d listening on %s\n", s.Addr())
	<-ctx.Done()
	return s.Close()
}
