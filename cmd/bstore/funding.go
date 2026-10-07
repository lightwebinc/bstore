package main

import (
	"errors"
	"fmt"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/internal/chaincfg"
)

// fundError is an import's error with its exit code: not mined yet is
// incomplete; a payment that pays nothing here, or whose proof or parents
// do not check, is refused.
func fundError(err error, hint string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, chaincfg.ErrUnmined):
		return incomplete("%v: import it once it has a block, or import the BEEF your wallet gave you (%s)", err, hint)
	case errors.Is(err, bwallet.ErrPaysNothing), errors.Is(err, bwallet.ErrUnprovenParent), errors.Is(err, nodeapi.ErrProofRefused):
		return refused("%v", err)
	}
	return err
}

// readBEEF reads a payment's BEEF from a file, or standard input for "-".
func readBEEF(g *global, path string) ([]byte, error) { return chaincfg.ReadBEEF(path, g.stdin) }

// feeFlags is the fee block the -fee flags describe; nil when none is set.
func feeFlags(rate, source string, floor uint64, maxRate string, maxTx uint64) (*feepolicy.Config, error) {
	if rate == "" && source == "" && floor == 0 && maxRate == "" && maxTx == 0 {
		return nil, nil
	}
	c := &feepolicy.Config{Source: source, MaxTx: maxTx}
	if rate != "" {
		r, err := mint.ParseRate(rate)
		if err != nil {
			return nil, fmt.Errorf("-fee-rate: %w", err)
		}
		c.Rate = &r
	}
	if maxRate != "" {
		r, err := mint.ParseRate(maxRate)
		if err != nil {
			return nil, fmt.Errorf("-fee-max-rate: %w", err)
		}
		c.MaxRate = &r
	}
	if floor != 0 {
		c.Floor = &floor
	}
	return c, nil
}
