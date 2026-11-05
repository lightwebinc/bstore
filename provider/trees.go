package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/publish"
)

// A provider pays for its advertisements and renewals from funding trees:
// one mined transaction of TreeCount outputs, each locked to the wallet's
// fund key, each paying one advertisement's fee. bcommon's producer runs a
// tree's life. A tree is recorded with its fee coin before it reaches the
// settlement leg (Trees.Prepare), so a run that stops between the leg and
// the adoption leaves a record the next start settles (Trees.Recover):
// adopted, held, its coin returned (kept for one more start, then dropped)
// or its coin spent elsewhere. A tree the library already holds is never
// recovered a second time. A tree is never published to a host: the
// advertisement is what a host admits, and it is published on its own.

func (p *Provider) newTrees() {
	p.kept = &producer.Kept{Load: p.loadKept}
	p.payer = p.newPayer()
	// A tree goes to the leg without waiting for a block: an advertisement
	// spends its output at once, and is itself what has to mine.
	p.payer.Async = true
	p.trees = &producer.Trees{Payer: p.payer, State: treeState{p}, Identity: p.signer.IdentityHex(),
		Count: p.cfg.TreeCount, Sats: p.cfg.TreeSats, Funder: "pool",
		Lock: func(context.Context) (*script.Script, error) { return p.signer.FundScript() }, Change: p.signer.FundScript,
		Facade: keptFacade(), Topic: "tm_bstore_kept", Ahead: uint32(max(1, p.cfg.TreeCount/4)), Prepare: p.prepareTree}
}

func (p *Provider) newPayer() *producer.Payer {
	pay := &producer.Payer{Pool: p.pool, Keys: map[string]*bwallet.Signer{p.signer.IdentityHex(): p.signer},
		Kept: p.kept, Settler: p.settler, Chain: p.chain, Fees: p.fees(), Note: p.libraryNote, Poll: time.Second}
	pay.Tip = p.lastTip.Load()
	return pay
}

func (p *Provider) libraryNote(format string, args ...any) {
	if strings.HasPrefix(format, "funding tree published") {
		return
	}
	p.logf(format, args...)
}

type treeState struct{ p *Provider }

func (t treeState) Current() *funding.Tree {
	t.p.mu.Lock()
	defer t.p.mu.Unlock()
	if t.p.st.Tree == nil {
		return nil
	}
	c := *t.p.st.Tree
	return &c
}

func (t treeState) Adopt(tr funding.Tree) error {
	p := t.p
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.st
	st.Tree = &tr
	st.Trees = producer.Index(st.Trees, &tr)
	st.Ahead = slices.DeleteFunc(st.Ahead, func(a funding.Tree) bool { return a.Txid == tr.Txid })
	st.dropPendingTree(tr.Txid)
	return p.save()
}

// prepareTree records a tree signed for coin before it reaches the leg; a
// record that cannot be saved aborts the mint.
func (p *Provider) prepareTree(tree funding.Tree, coin bwallet.Output) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.st.dropPendingTree(tree.Txid)
	p.st.PendingTrees = append(p.st.PendingTrees, PendingTree{Tree: tree, Coin: coin})
	if err := p.save(); err != nil {
		p.st.dropPendingTree(tree.Txid)
		return err
	}
	return nil
}

// recoverTrees settles every tree a run signed and did not adopt, before
// anything is spent. A tree the library holds now (minted ahead or found
// by an earlier recovery in this run) is skipped: it is the library's.
func (p *Provider) recoverTrees(ctx context.Context) error {
	p.mu.Lock()
	pending := slices.Clone(p.st.PendingTrees)
	p.mu.Unlock()
	held := p.trees.Held()
	for _, pt := range pending {
		id := pt.Tree.Txid
		if slices.ContainsFunc(held, func(h funding.Tree) bool { return h.Txid == id }) {
			continue
		}
		p.mu.Lock()
		ahead := p.st.mintedAhead(id)
		done := p.st.adopted(id) || ahead != nil && ahead.Remaining() == 0
		if done {
			p.st.dropPendingTree(id)
			err := p.save()
			p.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		p.mu.Unlock()
		if ahead != nil {
			continue
		}
		r, err := p.trees.Recover(ctx, pt.Tree, pt.Coin)
		if errors.Is(err, producer.ErrPublish) {
			// Adopted; its publish sends nothing (keptFacade), so nothing
			// is left to repeat.
			err = nil
		}
		if err != nil {
			p.logf("funding tree %s was signed by a run that stopped before recording it, and what became of it could not be settled now (%v): its record is kept", short(id), err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		p.mu.Lock()
		switch r.Outcome {
		case producer.TreeAdopted, producer.CoinSpent:
			p.st.dropPendingTree(id)
		case producer.TreeHeld:
			p.st.Ahead = producer.Index(p.st.Ahead, &r.Tree)
		case producer.CoinReturned:
			// Kept once, in case the tree reaches the node after all, and
			// dropped the second time; the record is kept once only.
			if rec := p.st.pendingTree(id); rec != nil {
				if rec.ReturnedOnce {
					p.st.dropPendingTree(id)
					p.logf("funding tree %s: the node still does not know it, so its record is dropped", short(id))
				} else {
					rec.ReturnedOnce = true
					p.logf("funding tree %s: its coin is back in the pool and its record kept for one more start", short(id))
				}
			}
		}
		err = p.save()
		p.mu.Unlock()
		if err != nil {
			return err
		}
		p.logf("funding tree %s: %s", short(id), r.Outcome)
	}
	return nil
}

// spendTree is the funding output the next advertisement spends, its tree
// marked used past it in the same save. The caller holds mintMu. The tip
// is the one last read (every bounty check and background pass reads it):
// a round trip here was one more per entry, in a row.
func (p *Provider) spendTree(ctx context.Context) (*transaction.Transaction, uint32, error) {
	p.payer.Tip = p.lastTip.Load()
	if err := p.promoteAhead(); err != nil {
		return nil, 0, err
	}
	tree, vout, err := p.trees.Spend(ctx, 1)
	if errors.Is(err, producer.ErrPublish) {
		// Adopted and current; Spend answers it.
		tree, vout, err = p.trees.Spend(ctx, 1)
	}
	if err != nil {
		return nil, 0, feeError(fmt.Errorf("funding tree: %w", err))
	}
	return tree, vout, nil
}

// markSpent records that vout of tree is used; the provider's lock is held.
func (st *State) markSpent(tree string, vout uint32) {
	if st.Tree != nil && st.Tree.Txid == tree && st.Tree.Next <= vout {
		st.Tree.Next = vout + 1
		st.Trees = producer.Index(st.Trees, st.Tree)
	}
}

// promoteAhead makes a tree a previous run minted ahead the current one
// when the current one is used up: the library keeps one only in memory.
func (p *Provider) promoteAhead() error {
	p.mu.Lock()
	cur := p.st.Tree
	if cur != nil && cur.Remaining() > 0 {
		p.mu.Unlock()
		return nil
	}
	ahead := slices.Clone(p.st.Ahead)
	p.mu.Unlock()
	held := p.trees.Held()
	for _, t := range ahead {
		if slices.ContainsFunc(held, func(h funding.Tree) bool { return h.Txid == t.Txid }) {
			continue
		}
		if t.IdentityKeyHex == p.signer.IdentityHex() && t.Remaining() > 0 {
			return treeState{p}.Adopt(t)
		}
	}
	return nil
}

// feeError words the library's refusals to find a fee input.
func feeError(err error) error {
	var nc *producer.NoCoinError
	if errors.As(err, &nc) {
		switch {
		case nc.Held > 0 || nc.Minting > 0:
			return fmt.Errorf("fee input: %w: change from %d unproven transaction(s) and %d tree(s) minting; it becomes spendable once mined", nc.Err, nc.Held, nc.Minting)
		}
		return fmt.Errorf("fee input: %w (fund the provider: bstore-provider address)", nc.Err)
	}
	return err
}

// keptFacade is a facade that sends nothing: the library publishes each
// tree it adopts through one, and a provider's tree goes to no host.
func keptFacade() *publish.Facade {
	return &publish.Facade{Base: "http://kept.invalid", HTTP: keptClient}
}

func short(txid string) string {
	if len(txid) > 12 {
		return txid[:12]
	}
	return txid
}

// keptClient answers every submit with nothing admitted.
var keptClient = &http.Client{Transport: keptTransport{}}

type keptTransport struct{}

func (keptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	raw, _ := json.Marshal(map[string]any{req.Header.Get("x-topics"): map[string]any{"outputsToAdmit": []int{}, "coinsToRetain": []int{}}})
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)),
		ContentLength: int64(len(raw)), Request: req}, nil
}
