package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// fees is the network rate with no padding (100 sat a kB, a floor of one).
var fees = mint.NetworkFees

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	names := fs.String("names", "perf_a,perf_b", "bucket names; each gets a random suffix")
	providers := fs.Int("providers", 3, "advertising providers")
	_ = fs.Parse(args)
	if *root == "" {
		return errors.New("init: -root")
	}
	path := filepath.Join(*root, "ids.json")
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%s exists", path)
	}
	if err := os.MkdirAll(*root, 0o700); err != nil {
		return err
	}
	f := idsFile{Owner: randomHex(32), Writer: randomHex(32), Buyer: randomHex(32)}
	for range *providers {
		f.Providers = append(f.Providers, randomHex(32))
	}
	for _, n := range strings.Split(*names, ",") {
		b := n + "_" + randomSuffix()
		if err := storerec.CheckBucket(b); err != nil {
			return fmt.Errorf("%s: %w", b, err)
		}
		f.Buckets = append(f.Buckets, b)
	}
	if err := writeJSON(path, f); err != nil {
		return err
	}
	id, err := loadIDs(*root)
	if err != nil {
		return err
	}
	fmt.Printf("buckets %s\nowner %s\nwriter %s\nbuyer %s\n", strings.Join(f.Buckets, ","), id.Owner.PubHex(), id.Writer.PubHex(), id.Buyer.PubHex())
	return nil
}

// coins hands out the fan-out outputs in order.
type coins struct {
	fee   *feeKey
	outs  []mint.Input
	next  int
	value uint64
	big   []mint.Input // for tokens: an anchor of a hundred changes pays more than a coin holds
	nbig  int
}

func (c *coins) takeBig() (mint.Input, error) {
	if c.nbig >= len(c.big) {
		return mint.Input{}, errors.New("big coins exhausted")
	}
	in := c.big[c.nbig]
	c.nbig++
	return in, nil
}

func (c *coins) take() (mint.Input, error) {
	if c.next >= len(c.outs) {
		return mint.Input{}, errors.New("fan-out coins exhausted")
	}
	in := c.outs[c.next]
	c.next++
	return in, nil
}

// planner holds what plan signs.
type planner struct {
	ctx   context.Context
	id    *ids
	fee   *feeKey
	coins *coins
	p     *planFile
	all   []*transaction.Transaction
	level map[string]int
}

func (pl *planner) add(tx *transaction.Transaction, level int) {
	pl.all = append(pl.all, tx)
	pl.level[tx.TxID().String()] = level
}

func adPriv(p *ident) *ec.PrivateKey {
	return must(wallet.NewKeyDeriver(p.Priv).DerivePrivateKey(storerec.AdProtocol, storerec.AdKeyID, pushdrop.Anyone()))
}

func adURL(i int, root [32]byte) string {
	return fmt.Sprintf("https://perf-provider-%d.invalid/store", i+1) + storerec.CHIRPURLSuffix(root)
}

func cmdPlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	root := fs.String("root", "", "corpus root (ids.json, wallet.json, funding.json)")
	entryTrees := fs.Int("entry-trees", 512, "the writer's entry funding trees")
	outcomeTrees := fs.Int("outcome-trees", 64, "the buyer's outcome funding trees")
	frontTrees := fs.Int("front-trees", 64, "the buyer's front-statement funding trees")
	treeSize := fs.Int("tree-size", 16, "funding outputs per tree (the publisher's default is 16)")
	adverts := fs.Int("adverts", 96000, "mined advertisements for the advert shape")
	holds := fs.Int("holds", 96000, "mined hold outputs for the hold shape")
	mixPuts := fs.Int("mix-puts", 19200, "puts in the mix: each an entry, its hold output, one advertisement per provider")
	anchorEvery := fs.Int("anchor-every", 100, "mix: the owner anchors a bucket after this many of its puts")
	coin := fs.Uint64("coin", 200, "satoshis per fan-out coin")
	holdSats := fs.Uint64("hold-sats", 100, "satoshis in each hold output")
	fanMax := fs.Int("fan-max", 34000, "coins per fan-out transaction")
	bigCoin := fs.Uint64("big-coin", 5000, "satoshis per token coin (an anchor naming a hundred entries is several kB)")
	var homes []string
	fs.Func("home", "ADDRESS: an asker's home fund address (repeat); paid -home-coins outputs of -home-sats", func(s string) error {
		homes = append(homes, s)
		return nil
	})
	homeCoins := fs.Int("home-coins", 30, "outputs per home")
	homeSats := fs.Uint64("home-sats", 20000, "satoshis per home output")
	tipAhead := fs.Uint64("deadline", 5000, "hold deadline: this many blocks above the funding height")
	_ = fs.Parse(args)
	if *root == "" {
		return errors.New("plan: -root")
	}
	if _, err := os.Stat(filepath.Join(*root, "chain", "plan.json")); err == nil {
		return errors.New("plan: chain/plan.json exists; a plan is signed once")
	}
	id, err := loadIDs(*root)
	if err != nil {
		return err
	}
	fk, err := loadFeeKey(*root)
	if err != nil {
		return err
	}
	fnd, ftx, err := loadFunding(*root)
	if err != nil {
		return err
	}
	if fnd.Address != fk.Addr.AddressString {
		return fmt.Errorf("funding pays %s, the wallet is %s", fnd.Address, fk.Addr.AddressString)
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	nb := len(id.Buckets)
	nprov := len(id.Providers)
	anchors := 0
	perBucket := (*mixPuts + nb - 1) / nb
	anchors = nb * ((perBucket + *anchorEvery - 1) / *anchorEvery)
	need := *entryTrees + *outcomeTrees + *frontTrees + nb + *adverts + *holds + *mixPuts*(1+nprov)
	fmt.Printf("plan: %d coins (%d trees, %d chain trees and first tokens, %d anchors, %d adverts, %d holds, mix %d puts x %d)\n",
		need, *entryTrees+*outcomeTrees+*frontTrees, 2*nb, anchors, *adverts, *holds, *mixPuts, 1+nprov)

	pl := &planner{ctx: ctx, id: id, fee: fk, level: map[string]int{},
		p: &planFile{Created: time.Now().UTC().Format(time.RFC3339), FeeAddr: fk.Addr.AddressString,
			Params: planParams{TreeSize: *treeSize, Coin: *coin, HoldSats: *holdSats, AnchorEvery: *anchorEvery, Providers: nprov}}}

	// Level 0: the askers' homes, paid from funding output 0; its change
	// feeds the token coins (level 1) and a fan-out (level 2).
	type src struct {
		tx   *transaction.Transaction
		vout uint32
		sats uint64
	}
	var sources []src
	for i, o := range fnd.Outputs {
		if i == 0 && len(homes) > 0 {
			h, change, err := pl.homesTx(ftx, o.Vout, o.Satoshis, homes, *homeCoins, *homeSats)
			if err != nil {
				return err
			}
			pl.add(h, 0)
			pl.p.Homes = h.TxID().String()
			sources = append(sources, src{h, uint32(len(h.Outputs) - 1), change})
			continue
		}
		sources = append(sources, src{ftx, o.Vout, o.Satoshis})
	}
	cs := &coins{fee: fk, value: *coin}
	// One transaction of big coins for the tokens, from the first source.
	nbig := 2*nb + anchors
	{
		s0 := sources[0]
		bigTx, err := pl.fanOut(s0.tx, s0.vout, s0.sats, nbig, *bigCoin)
		if err != nil {
			return err
		}
		pl.add(bigTx, 1)
		for v := range nbig {
			cs.big = append(cs.big, mint.Input{Tx: bigTx, Vout: uint32(v), Unlocker: fk.unlocker()})
		}
		sources[0] = src{bigTx, uint32(nbig), bigTx.Outputs[nbig].Satoshis}
	}
	for _, s := range sources {
		if len(cs.outs) >= need {
			break
		}
		// What the source covers: n coins, the fee at 100 sat a kB, change.
		fit := int((s.sats - 200) * 1000 / (*coin*1000 + 3400))
		n := min(*fanMax, need-len(cs.outs), fit)
		fan, err := pl.fanOut(s.tx, s.vout, s.sats, n, *coin)
		if err != nil {
			return err
		}
		pl.add(fan, 2)
		for v := range n {
			cs.outs = append(cs.outs, mint.Input{Tx: fan, Vout: uint32(v), Unlocker: fk.unlocker()})
		}
	}
	if len(cs.outs) < need {
		return fmt.Errorf("plan: the funding gives %d coins of %d sat, the plan needs %d", len(cs.outs), *coin, need)
	}
	pl.coins = cs

	// Level 3: funding trees, chain trees (level 2 is the fan-outs).
	tree := func(who *ident, d pushdrop.Derivation, n int) (*transaction.Transaction, error) {
		lock, err := carrier.FundingLock(ctx, who.W, "", carrierParams(d))
		if err != nil {
			return nil, err
		}
		c, err := cs.take()
		if err != nil {
			return nil, err
		}
		t, err := mint.FundingTree(lock, n, 1, c, fk.Lock, fees)
		if err != nil {
			return nil, err
		}
		pl.add(t, 3)
		return t, nil
	}
	var entryT, outcomeT []*transaction.Transaction
	for range *entryTrees {
		t, err := tree(id.Writer, storerec.EntryDerivation, *treeSize)
		if err != nil {
			return err
		}
		entryT = append(entryT, t)
		pl.p.Trees.Entry = append(pl.p.Trees.Entry, t.TxID().String())
	}
	for range *outcomeTrees {
		t, err := tree(id.Buyer, storerec.OutcomeDerivation, *treeSize)
		if err != nil {
			return err
		}
		outcomeT = append(outcomeT, t)
		pl.p.Trees.Outcome = append(pl.p.Trees.Outcome, t.TxID().String())
	}
	for range *frontTrees {
		t, err := tree(id.Buyer, storerec.FrontDerivation, *treeSize)
		if err != nil {
			return err
		}
		pl.p.Trees.Front = append(pl.p.Trees.Front, t.TxID().String())
	}
	_ = outcomeT

	// Each bucket: a chain tree (level 3) and its first token (level 4),
	// naming the writer.
	type chainState struct {
		b       *storerec.Bucket
		token   *transaction.Transaction
		m       storerec.Map
		named   map[[32]byte]bool
		pending []storerec.Applicable
	}
	chains := make([]*chainState, nb)
	now := uint64(time.Now().UnixMilli())
	for i, name := range id.Buckets {
		ct, err := tree(id.Owner, storerec.ChainDerivation, 1)
		if err != nil {
			return err
		}
		bid := storerec.BucketID([32]byte(*ct.TxID()), 0)
		b0 := &storerec.Bucket{Bucket: name, Owner: id.Owner.Pub(), ID: bid, Seq: 0, Writers: [][]byte{id.Writer.Pub()},
			Changes: []storerec.Change{}, Count: 0, MapRoot: storerec.EmptyHash, Created: now, Consumed: [][32]byte{}}
		t0, err := pl.token(b0, ct, 0, 4)
		if err != nil {
			return fmt.Errorf("%s first token: %w", name, err)
		}
		chains[i] = &chainState{b: b0, token: t0, m: storerec.Map{}, named: map[[32]byte]bool{}}
		pl.p.Buckets = append(pl.p.Buckets, planBucket{Name: name, Topic: storerec.RecordsTopic(name), ID: hex.EncodeToString(bid[:]),
			ChainTree: ct.TxID().String(), Tokens: []string{t0.TxID().String()}})
	}

	// The 1 MiB closures the advertisements and the mix's puts name.
	refs, err := closures(*root, "1m", 1<<20, 64)
	if err != nil {
		return err
	}
	pl.p.Closures = refs

	// Level 3: the standalone advertisements and hold outputs, signed in
	// parallel over pre-assigned coins.
	adKeys := make([]*ec.PrivateKey, nprov)
	for i, p := range id.Providers {
		adKeys[i] = adPriv(p)
	}
	holdKey, err := storerec.LockingKey(storerec.HoldDerivation, id.Writer.Pub())
	if err != nil {
		return err
	}
	holdScript := script.Script(storerec.HoldScript(holdKey))
	expiry := uint64(time.Now().Add(60 * 24 * time.Hour).Unix())
	adTx := func(i, prov int, ref closureRef, c mint.Input) (*transaction.Transaction, error) {
		f, err := storerec.AdvertisementFields(id.Providers[prov].Pub(), adKeys[prov], ref.root(), adURL(prov, ref.root()), expiry+uint64(i), uint64(ref.RootLen))
		if err != nil {
			return nil, err
		}
		s := script.Script(storerec.AdvertisementScript(adKeys[prov].PubKey(), f))
		return mint.Transition(&s, storerec.AdvertisementMin, nil, c, fk.Lock, fees)
	}
	holdTx := func(c mint.Input) (*transaction.Transaction, error) {
		return mint.Transition(&holdScript, *holdSats, nil, c, fk.Lock, fees)
	}
	signAll := func(n int, f func(i int, c mint.Input) (*transaction.Transaction, error)) ([]*transaction.Transaction, error) {
		in := make([]mint.Input, n)
		for i := range n {
			c, err := cs.take()
			if err != nil {
				return nil, err
			}
			in[i] = c
		}
		out := make([]*transaction.Transaction, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		w := runtime.NumCPU()
		for k := range w {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := k; i < n; i += w {
					out[i], errs[i] = f(i, in[i])
				}
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	ads, err := signAll(*adverts, func(i int, c mint.Input) (*transaction.Transaction, error) {
		return adTx(i, i%nprov, refs[i%len(refs)], c)
	})
	if err != nil {
		return fmt.Errorf("adverts: %w", err)
	}
	for _, t := range ads {
		pl.add(t, 3)
		pl.p.Adverts = append(pl.p.Adverts, t.TxID().String())
	}
	hs, err := signAll(*holds, func(_ int, c mint.Input) (*transaction.Transaction, error) { return holdTx(c) })
	if err != nil {
		return fmt.Errorf("holds: %w", err)
	}
	for _, t := range hs {
		pl.add(t, 3)
		pl.p.Holds = append(pl.p.Holds, t.TxID().String())
	}

	// The mix: per put, its hold output, its entry (a carrier, never
	// broadcast), one advertisement per provider; per bucket an anchor
	// after every -anchor-every of its puts and at the end.
	mixHolds, err := signAll(*mixPuts, func(_ int, c mint.Input) (*transaction.Transaction, error) { return holdTx(c) })
	if err != nil {
		return err
	}
	mixAds, err := signAll(*mixPuts*nprov, func(i int, c mint.Input) (*transaction.Transaction, error) {
		return adTx(*adverts+i, i%nprov, refs[(i/nprov)%len(refs)], c)
	})
	if err != nil {
		return err
	}
	deadline := uint64(fnd.Height) + *tipAhead
	first, err := serials(*root, *mixPuts)
	if err != nil {
		return err
	}
	type mixPut struct {
		entry *transaction.Transaction
		app   storerec.Applicable
	}
	puts := make([]mixPut, *mixPuts)
	errs := make([]error, *mixPuts)
	var wg sync.WaitGroup
	w := runtime.NumCPU()
	for k := range w {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := k; j < *mixPuts; j += w {
				b := j % nb
				ref := refs[j%len(refs)]
				h := mixHolds[j]
				serial := first + uint64(j)
				e := &storerec.Entry{Bucket: id.Buckets[b], ID: chains[b].b.ID, Writer: id.Writer.Pub(),
					Key: fmt.Sprintf("mix/photos/%08d.jpg", serial), Kind: storerec.KindPut, Created: now + serial,
					Size: ref.Size, Root: ref.root(), ContentHash: ref.contentHash(), ProofRoot: ref.proofRoot(), ProofProfile: 1,
					ContentType: "image/jpeg",
					Hold: &storerec.Hold{Txid: [32]byte(*h.TxID()), Vout: 0, Satoshis: *holdSats, T: 3, K: 3,
						Retention: 7 * 24 * 3600, Deadline: deadline, CheckC: storerec.DefaultC}}
				rec, err := e.Encode()
				if err != nil {
					errs[j] = err
					continue
				}
				t := entryT[j%len(entryT)]
				vout := uint32((j / len(entryT)) % *treeSize)
				c, err := carrier.Mint(ctx, id.Writer.W, "", carrierParams(storerec.EntryDerivation), rec, t, vout)
				if err != nil {
					errs[j] = err
					continue
				}
				puts[j] = mixPut{entry: c, app: storerec.Applicable{C: carrier.Commitment(c), Key: e.Key, Kind: e.Kind, Created: e.Created}}
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("mix entries: %w", err)
		}
	}
	anchor := func(b int) error {
		ch := chains[b]
		if len(ch.pending) == 0 {
			return nil
		}
		changes, consumed := storerec.ResolveBatch(ch.m, ch.named, ch.pending)
		next := ch.m.Clone()
		for _, c := range changes {
			if c.Removal {
				delete(next, c.Key)
			} else {
				next[c.Key] = c.C
			}
			ch.named[c.C] = true
		}
		for _, x := range consumed {
			ch.named[x] = true
		}
		if changes == nil {
			changes = []storerec.Change{}
		}
		if consumed == nil {
			consumed = [][32]byte{}
		}
		nbk := &storerec.Bucket{Bucket: ch.b.Bucket, Owner: id.Owner.Pub(), ID: ch.b.ID, Seq: ch.b.Seq + 1, Prev: [32]byte(*ch.token.TxID()),
			Writers: ch.b.Writers, Changes: changes, Count: uint64(len(next)), MapRoot: next.Root(),
			Created: uint64(time.Now().UnixMilli()), Consumed: consumed}
		t, err := pl.token(nbk, ch.token, 0, 4+int(nbk.Seq))
		if err != nil {
			return fmt.Errorf("anchor %s seq %d: %w", ch.b.Bucket, nbk.Seq, err)
		}
		ch.b, ch.token, ch.m, ch.pending = nbk, t, next, nil
		pl.p.Buckets[b].Tokens = append(pl.p.Buckets[b].Tokens, t.TxID().String())
		pl.p.Mix = append(pl.p.Mix, mixItem{Kind: "token", Txid: t.TxID().String(), Bucket: b})
		return nil
	}
	var entries []*transaction.Transaction
	for j, mp := range puts {
		b := j % nb
		pl.p.Mix = append(pl.p.Mix, mixItem{Kind: "entry", Txid: mp.entry.TxID().String(), Bucket: b})
		entries = append(entries, mp.entry)
		pl.add(mixHolds[j], 3)
		pl.p.Mix = append(pl.p.Mix, mixItem{Kind: "hold", Txid: mixHolds[j].TxID().String(), Bucket: b})
		for k := range nprov {
			a := mixAds[j*nprov+k]
			pl.add(a, 3)
			pl.p.Mix = append(pl.p.Mix, mixItem{Kind: "ad", Txid: a.TxID().String(), Bucket: b})
		}
		chains[b].pending = append(chains[b].pending, mp.app)
		if len(chains[b].pending) >= *anchorEvery {
			if err := anchor(b); err != nil {
				return err
			}
		}
	}
	for b := range chains {
		if err := anchor(b); err != nil {
			return err
		}
	}

	// Broadcast levels, then the store and the plan.
	byLevel := map[int][]string{}
	maxL := 0
	for txid, l := range pl.level {
		byLevel[l] = append(byLevel[l], txid)
		maxL = max(maxL, l)
	}
	for l := 0; l <= maxL; l++ {
		ids := byLevel[l]
		sort.Strings(ids)
		pl.p.Levels = append(pl.p.Levels, ids)
	}
	if err := st.addTxs(append(pl.all, entries...)); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(*root, "chain", "plan.json"), pl.p); err != nil {
		return err
	}
	fmt.Printf("plan: %d transactions to broadcast in %d levels, %d coins used of %d; %d mix entries (offline); %d mix objects\n",
		len(pl.all), len(pl.p.Levels), cs.next, len(cs.outs), len(entries), len(pl.p.Mix))
	for _, b := range pl.p.Buckets {
		fmt.Printf("bucket %s id %s tokens %d\n", b.Name, b.ID, len(b.Tokens))
	}
	return nil
}

// token mints a bucket token under the owner's bucket key, spending
// prev:vout (the chain tree at seq 0, the previous token after) and a coin.
func (pl *planner) token(b *storerec.Bucket, prev *transaction.Transaction, vout uint32, level int) (*transaction.Transaction, error) {
	rec, err := b.Encode()
	if err != nil {
		return nil, err
	}
	o := pl.id.Owner
	lock, err := storerec.BucketDerivation.Lock(pl.ctx, o.W, "", [][]byte{storerec.TagToken, rec}, true)
	if err != nil {
		return nil, err
	}
	unlock := storerec.ChainDerivation.Unlocker(pl.ctx, o.W, "")
	if b.Seq > 0 {
		unlock = storerec.BucketDerivation.Unlocker(pl.ctx, o.W, "")
	}
	c, err := pl.coins.takeBig()
	if err != nil {
		return nil, err
	}
	t, err := mint.Transition(lock, 1, &mint.Input{Tx: prev, Vout: vout, Unlocker: unlock}, c, pl.fee.Lock, fees)
	if err != nil {
		return nil, err
	}
	pl.add(t, level)
	return t, nil
}

// fanOut splits one output into n coins of coin sats to the fee key, the
// rest back to it as change.
func (pl *planner) fanOut(src *transaction.Transaction, vout uint32, sats uint64, n int, coin uint64) (*transaction.Transaction, error) {
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(src, vout, pl.fee.unlocker())
	for range n {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: coin, LockingScript: pl.fee.Lock})
	}
	size := uint64(10 + 150 + (n+1)*34)
	fee := size*100/1000 + 1
	if sats < uint64(n)*coin+fee+1 {
		return nil, fmt.Errorf("fan-out of %d x %d sat needs %d sat, the source has %d", n, coin, uint64(n)*coin+fee+1, sats)
	}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats - uint64(n)*coin - fee, LockingScript: pl.fee.Lock})
	if err := tx.Sign(); err != nil {
		return nil, err
	}
	return tx, nil
}

// homesTx pays each home address n outputs of sats, the rest back to the
// fee key as the last output.
func (pl *planner) homesTx(src *transaction.Transaction, vout uint32, in uint64, homes []string, n int, sats uint64) (*transaction.Transaction, uint64, error) {
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(src, vout, pl.fee.unlocker())
	var out uint64
	for _, h := range homes {
		addr, err := script.NewAddressFromString(h)
		if err != nil {
			return nil, 0, fmt.Errorf("home %s: %w", h, err)
		}
		lock, err := p2pkh.Lock(addr)
		if err != nil {
			return nil, 0, err
		}
		for range n {
			tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: lock})
			out += sats
		}
	}
	size := uint64(10 + 150 + (len(tx.Outputs)+1)*34)
	fee := size*100/1000 + 1
	if in < out+fee+1 {
		return nil, 0, fmt.Errorf("homes need %d sat, the funding output has %d", out+fee+1, in)
	}
	change := in - out - fee
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: change, LockingScript: pl.fee.Lock})
	if err := tx.Sign(); err != nil {
		return nil, 0, err
	}
	return tx, change, nil
}
