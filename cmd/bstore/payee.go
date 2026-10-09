package main

import (
	"context"
	"path/filepath"
	"time"

	"github.com/lightwebinc/bcommon/payee"
	"github.com/lightwebinc/bcommon/payeecmd"
	"github.com/lightwebinc/bcommon/purse"

	"github.com/lightwebinc/bstore/internal/publish"
)

// payeeBookFile is the home's record, as a host's payee, of what payee
// settle took into the pool and what the network refused.
const payeeBookFile = "payee.json"

// cmdPayee is bcommon's payee verbs (package payeecmd) over this home: the
// identity of its wallet is the payee key, and its pool the payee's.
func cmdPayee(ctx context.Context, g *global, args []string) error {
	c := g.payeeCommand()
	fs := g.flagSet("payee", c.Help())
	f := c.Bind(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	return c.Run(ctx, f, pos)
}

func (g *global) payeeCommand() *payeecmd.Command {
	return &payeecmd.Command{App: "bstore", KeyDir: publish.WalletDir(g.home), Stdout: g.stdout, Stderr: g.stderr, Note: g.say,
		Usage: usage, Refused: refused, Open: g.openPayee}
}

// openPayee opens the home for payee settle: a purse over its wallet, the
// chain view, the header source and the settlement leg config.json names,
// and the book in payeeBookFile.
func (g *global) openPayee(ctx context.Context) (*payeecmd.Session, error) {
	h, err := g.open(ctx)
	if err != nil {
		return nil, err
	}
	book, err := payee.OpenBookFile(filepath.Join(g.home, payeeBookFile))
	if err != nil {
		h.close()
		return nil, err
	}
	w := h.pub.Wallet()
	p := &purse.Purse{Embedded: w, Settler: h.view.Settler, Chain: h.view.Chain, Headers: h.view.Headers, Wait: 10 * time.Minute}
	return &payeecmd.Session{Payer: p, Record: book.Record(), Pool: w.Pool, Close: h.close}, nil
}
