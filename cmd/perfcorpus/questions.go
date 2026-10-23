package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// question is one ls_bstore question about the perf buckets, as the lookup
// driver asks it: free at a host's /lookup, priced at its terms route.
type question struct {
	Class string            `json:"class"`
	Paid  bool              `json:"paid"`
	Q     map[string]string `json:"q"`
}

// cmdQuestions prints questions over every free class and the five priced
// ones, about what the plan minted: the anchored keys of the mix, its
// entries, the 1 MiB roots, the providers, the buyer, the holds.
func cmdQuestions(args []string) error {
	fs := flag.NewFlagSet("questions", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	keys := fs.Int("keys", 64, "mix keys (and entries) per bucket to ask about")
	_ = fs.Parse(args)
	id, err := loadIDs(*root)
	if err != nil {
		return err
	}
	var p planFile
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), &p); err != nil {
		return err
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	owner := id.Owner.PubHex()
	enc := json.NewEncoder(os.Stdout)
	emit := func(class string, paid bool, q map[string]string) {
		_ = enc.Encode(question{Class: class, Paid: paid, Q: q})
	}
	// The mix's entries and their keys, per bucket.
	type ent struct{ txid, key string }
	per := map[int][]ent{}
	for _, it := range p.Mix {
		if it.Kind != "entry" || len(per[it.Bucket]) >= *keys {
			continue
		}
		tx, err := st.tx(it.Txid)
		if err != nil {
			return err
		}
		rec, ok := pushdrop.FirstPush(*tx.Outputs[0].LockingScript)
		if !ok {
			return errors.New("mix entry without a record")
		}
		e, err := storerec.DecodeEntry(rec)
		if err != nil {
			return err
		}
		per[it.Bucket] = append(per[it.Bucket], ent{it.Txid, e.Key})
	}
	nowMS := strconv.FormatInt(time.Now().Add(24*time.Hour).UnixMilli(), 10)
	ff := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	for bi, b := range p.Buckets {
		base := func(extra map[string]string) map[string]string {
			q := map[string]string{"bucket": b.Name, "owner": owner, "id": b.ID}
			for k, v := range extra {
				q[k] = v
			}
			return q
		}
		emit("bucket", false, base(nil))
		for s := 0; s < len(b.Tokens); s += 8 {
			emit("bucket-seq", false, base(map[string]string{"seq": strconv.Itoa(s)}))
		}
		emit("list", false, base(map[string]string{"prefix": "mix/photos/", "after": ""}))
		emit("list", false, base(map[string]string{"prefix": "", "after": ""}))
		emit("unanchored", false, base(map[string]string{"writer": id.Writer.PubHex(), "after": ""}))
		for _, e := range per[bi] {
			emit("object", false, base(map[string]string{"key": e.key}))
			emit("pending", false, base(map[string]string{"pending": e.key}))
			emit("entry", false, map[string]string{"bucket": b.Name, "entry": e.txid})
		}
		for k, r := range p.Closures[:min(16, len(p.Closures))] {
			emit("ads", false, map[string]string{"bucket": b.Name, "root": r.Root, "after": ""})
			emit("ad", false, map[string]string{"bucket": b.Name, "root": r.Root, "advertiser": id.Providers[k%len(id.Providers)].PubHex()})
		}
		for _, h := range p.Holds[:min(32, len(p.Holds))] {
			emit("hold", false, map[string]string{"bucket": b.Name, "hold": h + ".0"})
		}
		for _, pv := range id.Providers {
			a := pv.PubHex()
			emit("fronts", false, map[string]string{"bucket": b.Name, "advertiser": a, "after": ""})
			emit("front", false, map[string]string{"bucket": b.Name, "advertiser": a, "front": id.Buyer.PubHex()})
			emit("outcomes", false, map[string]string{"bucket": b.Name, "answerer": a})
			emit("outcomes-of", false, map[string]string{"bucket": b.Name, "provider": a})
			emit("outcomes-by", false, map[string]string{"bucket": b.Name, "answerer": a, "buyer": id.Buyer.PubHex()})
			emit("outcomes-of-by", false, map[string]string{"bucket": b.Name, "provider": a, "buyer": id.Buyer.PubHex()})
			emit("outcome-history", true, map[string]string{"bucket": b.Name, "outcome-history": a, "before": nowMS + ":" + ff})
			emit("outcome-history-of", true, map[string]string{"bucket": b.Name, "outcome-history-of": a, "before": nowMS + ":" + ff})
		}
		for _, e := range per[bi][:min(8, len(per[bi]))] {
			emit("history", true, base(map[string]string{"history": e.key, "before": "99"}))
		}
		emit("chain", true, map[string]string{"bucket": b.Name, "owner": owner, "chain": b.ID, "after": "0"})
		emit("export", true, map[string]string{"bucket": b.Name, "owner": owner, "export": b.ID, "after": ""})
	}
	fmt.Fprintf(os.Stderr, "questions about %d buckets\n", len(p.Buckets))
	return nil
}

// cmdFirstTokens writes each bucket's first token as a host admits it (the
// token and its chain tree) to <out>/<topic>.beef, for a direct /submit to
// the hosts once they carry the topic (what `bstore sync` does for a
// bucket minted before its hosts).
func cmdFirstTokens(args []string) error {
	fs := flag.NewFlagSet("first-tokens", flag.ExitOnError)
	root := fs.String("root", "", "corpus root")
	out := fs.String("out", ".", "directory")
	_ = fs.Parse(args)
	var p planFile
	if err := readJSON(filepath.Join(*root, "chain", "plan.json"), &p); err != nil {
		return err
	}
	st, err := openStore(*root)
	if err != nil {
		return err
	}
	b := &builder{st: st}
	for _, bk := range p.Buckets {
		beef, err := b.tokenBEEF(0, bk.Tokens[0])
		if err != nil {
			return err
		}
		path := filepath.Join(*out, bk.Topic+".beef")
		if err := os.WriteFile(path, beef, 0o600); err != nil {
			return err
		}
		fmt.Println(path)
	}
	return nil
}
