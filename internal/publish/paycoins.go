package publish

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/producer"
)

// Small payment coins. A BRC-105 payment whose call is not answered may
// still be broadcast by its payee, so its coin is held until the node
// shows it was not (paymentWait), unless the payee signs that it did not
// take it (row C30). So that a refused payment holds little, calls that
// may pay take a small coin of their own: coins split off the pool and
// kept in a pool of their own (wallet/paycoins.json), out of reach of the
// funding trees, hold outputs and fees the identity's pool pays. A call
// takes the smallest that covers it, alone in a pool for the call; its
// change goes to the identity's pool, and an unspent coin back to the
// payment coins. When fewer than half are ready, a coin of the identity's
// pool is split into more in the background, and a call that finds none
// pays from the identity's pool meanwhile.

// payCoins is how many small payment coins are kept ready.
const payCoins = 8

// payCoinValue is the value of a payment coin for calls that pay up to
// maxPay: two payments and their fees.
func (p *Publisher) payCoinValue(maxPay uint64) uint64 {
	return 2 * (maxPay + p.cfg.Fees.Floor)
}

// payCoinPool is the pool of small payment coins.
func (p *Publisher) payCoinPool() (*bwallet.Pool, error) {
	if p.coins == nil {
		c, err := bwallet.LoadPool(filepath.Join(WalletDir(p.cfg.Dir), "paycoins.json"))
		if err != nil {
			return nil, err
		}
		p.coins = c
	}
	return p.coins, nil
}

// smallCoins are the payment coins that pay up to maxPay; the caller holds
// payMu.
func (p *Publisher) smallCoins(tip uint32, maxPay uint64) []bwallet.Output {
	coins, err := p.payCoinPool()
	if err != nil {
		return nil
	}
	need := maxPay + p.cfg.Fees.Floor
	var out []bwallet.Output
	for _, o := range coins.Outputs() {
		if o.Spendable(tip) && o.Satoshis >= need {
			out = append(out, o)
		}
	}
	return out
}

// PayCoins is how many payment coins are held, and their value.
func (p *Publisher) PayCoins() (int, uint64) {
	p.payMu.Lock()
	defer p.payMu.Unlock()
	coins, err := p.payCoinPool()
	if err != nil {
		return 0, 0
	}
	return coins.Count(), coins.Balance()
}

// payPool is the pool a call that pays up to maxPay pays from: a pool of
// one payment coin when one is ready, and the identity's pool otherwise.
// done puts back what that pool holds after the call: the coin, unspent,
// to the payment coins; a payment's change to the identity's pool.
func (p *Publisher) payPool(ctx context.Context, maxPay uint64) (*bwallet.Pool, func(), *bwallet.Output) {
	main := p.wallet.Pool
	tip := uint32(0)
	if h, err := p.cfg.Asset.BestHeader(ctx); err == nil {
		tip = h.Height
	}
	p.payMu.Lock()
	small := p.smallCoins(tip, maxPay)
	if len(small) <= payCoins/2 {
		go p.FillPayCoins(p.run, maxPay)
	}
	if len(small) == 0 {
		p.payMu.Unlock()
		return main, func() {}, nil
	}
	pick := small[0]
	for _, o := range small[1:] {
		if o.Satoshis < pick.Satoshis {
			pick = o
		}
	}
	var r [8]byte
	_, _ = rand.Read(r[:])
	path := filepath.Join(p.cfg.Dir, "tmp", "paypool-"+hex.EncodeToString(r[:])+".json")
	one, err := bwallet.LoadPool(path)
	if err == nil {
		_, err = one.Add(pick)
	}
	if err == nil {
		var removed bool
		removed, err = p.coins.Remove(pick)
		if err == nil && !removed {
			err = errors.New("the coin left the payment coins")
		}
	}
	p.payMu.Unlock()
	if err != nil {
		_ = os.Remove(path)
		return main, func() {}, nil
	}
	return one, func() { p.putBack(one, path, pick.Outpoint()) }, &pick
}

// putBack empties a call's pool: the payment coin it was given, if still
// there, to the payment coins; anything else (a payment's change) to the
// identity's pool.
func (p *Publisher) putBack(one *bwallet.Pool, path, coin string) {
	p.payMu.Lock()
	defer p.payMu.Unlock()
	var back, change []bwallet.Output
	for _, o := range one.Outputs() {
		if o.Outpoint() == coin {
			back = append(back, o)
		} else {
			change = append(change, o)
		}
	}
	if coins, err := p.payCoinPool(); err == nil && len(back) > 0 {
		if _, err := coins.Add(back...); err != nil {
			p.cfg.Logf("payment pool %s: %v; it is merged on the next start", filepath.Base(path), err)
			return
		}
	}
	if len(change) > 0 {
		if _, err := p.wallet.Pool.Add(change...); err != nil {
			p.cfg.Logf("payment pool %s: %v; it is merged on the next start", filepath.Base(path), err)
			return
		}
	}
	_ = os.Remove(path)
}

// mergePayPools puts back what the call pools of a run that stopped
// mid-call held: to the payment coins, since a payment's change is never
// written to a call pool before its coin has left it.
func (p *Publisher) mergePayPools() {
	paths, _ := filepath.Glob(filepath.Join(p.cfg.Dir, "tmp", "paypool-*.json"))
	p.payMu.Lock()
	defer p.payMu.Unlock()
	coins, err := p.payCoinPool()
	if err != nil {
		return
	}
	// A payment coin also in the identity's pool (a split stopped between
	// its two steps) stays a payment coin.
	for _, c := range coins.Outputs() {
		for _, o := range p.wallet.Pool.Outputs() {
			if o.Outpoint() == c.Outpoint() {
				_, _ = p.wallet.Pool.Remove(o)
			}
		}
	}
	for _, path := range paths {
		one, err := bwallet.LoadPool(path)
		if err != nil {
			continue
		}
		var back, change []bwallet.Output
		for _, o := range one.Outputs() {
			if o.Unproven {
				change = append(change, o)
			} else {
				back = append(back, o)
			}
		}
		if _, err := coins.Add(back...); err != nil {
			continue
		}
		if _, err := p.wallet.Pool.Add(change...); err == nil {
			_ = os.Remove(path)
		}
	}
}

// proveCoins collects the proof of payment coins not yet mined.
func (p *Publisher) proveCoins(ctx context.Context) {
	p.payMu.Lock()
	coins, err := p.payCoinPool()
	var txids []string
	if err == nil {
		txids = coins.UnprovenTxids()
	}
	p.payMu.Unlock()
	for _, txid := range txids {
		mp, height, err := p.cfg.Asset.Proof(ctx, txid)
		if err != nil {
			continue
		}
		p.payMu.Lock()
		_, _ = coins.Prove(txid, funding.BumpHex(mp), height)
		p.payMu.Unlock()
	}
}

// waitFill waits, up to wait, for a split of payment coins in flight.
func (p *Publisher) waitFill(wait time.Duration) {
	done := make(chan struct{})
	go func() {
		p.splitMu.Lock()
		p.splitMu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(wait):
	}
}

// returnCoin puts a payment's fee coin back where it came from: the
// payment coins for a small one, the identity's pool otherwise.
func (p *Publisher) returnCoin(c bwallet.Output, small bool) {
	if small {
		p.payMu.Lock()
		coins, err := p.payCoinPool()
		if err == nil {
			_, err = coins.Add(c)
		}
		p.payMu.Unlock()
		if err == nil {
			return
		}
	}
	_ = p.wallet.Pool.Return(c)
}

// FillPayCoins splits one coin of the pool into small payment coins for
// calls that pay up to maxPay, until payCoins are held (proven or on their
// way). One runs at a time; a call while one runs returns at once.
func (p *Publisher) FillPayCoins(ctx context.Context, maxPay uint64) {
	if maxPay == 0 || !p.splitMu.TryLock() {
		return
	}
	defer p.splitMu.Unlock()
	pay := p.newPayer(ctx)
	p.payMu.Lock()
	held := 0
	if coins, err := p.payCoinPool(); err == nil {
		for _, o := range coins.Outputs() {
			if o.Satoshis >= maxPay+p.cfg.Fees.Floor && (o.Unproven || o.Spendable(pay.Tip)) {
				held++
			}
		}
	}
	p.payMu.Unlock()
	need := payCoins - held
	if need <= 0 {
		return
	}
	if err := p.splitCoin(ctx, pay, p.payCoinValue(maxPay), need); err != nil {
		p.cfg.Logf("payment coins: %v", feeError(err))
	}
}

func (p *Publisher) splitCoin(ctx context.Context, pay *producer.Payer, value uint64, n int) error {
	fee := p.cfg.Fees.Floor + p.cfg.Fees.SatPerByte*uint64(200+34*n)
	coin, err := pay.TakeAtLeast(ctx, uint64(n)*value+fee+p.cfg.Fees.Floor)
	if err != nil {
		return err
	}
	fs, err := p.signer.FundScript()
	if err != nil {
		pay.GiveBack()
		return err
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(coin.Tx, coin.Vout, coin.Unlocker)
	for range n {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: value, LockingScript: fs})
	}
	in := coin.Tx.Outputs[coin.Vout].Satoshis
	if change := in - uint64(n)*value - fee; change >= p.cfg.Fees.Floor {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: change, LockingScript: fs})
	}
	if err := tx.Sign(); err != nil {
		pay.GiveBack()
		return err
	}
	// Every output pays the fund key. The split is not waited for: once the
	// leg takes it, its outputs are in the pools unproven, and become
	// spendable when their proof is collected (CollectChange), so a command
	// that ends meanwhile loses nothing.
	pay.Async = true
	if _, _, err := pay.Settle(ctx, "payment coins", tx); err != nil {
		pay.GiveBack()
		return err
	}
	pay.Change(tx, 0, nil)
	// The coins of value go from the identity's pool to the payment coins:
	// added there first, then removed here, so a stop between leaves them
	// in both, and the next start keeps them with the payment coins.
	txid := tx.TxID().String()
	var moved []bwallet.Output
	for _, o := range p.wallet.Pool.Outputs() {
		if o.TxID == txid && o.Vout < uint32(n) {
			moved = append(moved, o)
		}
	}
	p.payMu.Lock()
	coins, err := p.payCoinPool()
	if err == nil {
		_, err = coins.Add(moved...)
	}
	p.payMu.Unlock()
	if err != nil {
		return err
	}
	for _, o := range moved {
		_, _ = p.wallet.Pool.Remove(o)
	}
	p.cfg.Logf("payment coins: %d of %d satoshis split off (%s); spendable once mined", len(moved), value, short(txid))
	return nil
}
