package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/storerec"
)

const putHelp = `usage: bstore put KEY [FILE|-] [flags]

Publish an object under KEY (spec sections 5, 9.2 and 14): its CHIRP
closure and proof root built while it streams, every object of it kept in
the home's store; then a hold output when a bounty is asked for (taken
from the ones minted ahead, marked taken before the entry is signed); then
the closure on the bucket's blob topic as piece carriers, or by BRC-167
upload session to each provider of config.json's uploads when there is no
facade; then the put entry, to the facade or the hosts and every provider.
It answers once the facade or a quorum of the hosts took the entry; what a
target did not take stays in the outbox (bstore sync). A put is pending
until the owner's next token anchors it. With no FILE, or -, the content
is read from standard input.

The bounty is config.json's unless -bounty names one (-no-bounty: none).
Its race is judged by bstore holds and paid by bstore settle.`

func cmdPut(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("put", putHelp)
	ctype := fs.String("type", "", "the content type (default from the file name, else application/octet-stream)")
	var meta listFlag
	fs.Var(&meta, "meta", "user metadata k=v (repeatable)")
	bf := bountyFlags(fs)
	maxBytes := fs.Uint64("max", storerec.MaxSize, "the most bytes read")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 || len(pos) > 2 {
		return usage("put KEY [FILE|-]")
	}
	key := pos[0]
	if err := storerec.CheckObjectKey(key); err != nil {
		return usage("key: %v", err)
	}
	src := "-"
	if len(pos) == 2 {
		src = pos[1]
	}
	pairs, err := metaPairs(meta)
	if err != nil {
		return usage("%v", err)
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	ch, err := h.chain()
	if err != nil {
		return err
	}
	b, err := bf.bounty(h.cfg)
	if err != nil {
		return usage("%v", err)
	}
	var r io.Reader = g.stdin
	name := ""
	if src != "-" {
		f, err := os.Open(src)
		if err != nil {
			return usage("%v", err)
		}
		defer f.Close()
		r, name = f, src
	}
	sum := md5.New()
	staged, err := h.store.Stage(ctx, io.TeeReader(r, sum), *maxBytes)
	if err != nil {
		return err
	}
	defer staged.Release()
	etag := hex.EncodeToString(sum.Sum(nil))
	ct := contentType(*ctype, name)
	rc, err := h.pub.Put(ctx, publish.Fields{Bucket: ch.Bucket, ID: ch.ID, Key: key, ContentType: ct, ETag: &etag, Meta: pairs},
		publish.Object{Root: staged.Root, Size: staged.Size, ContentHash: staged.ContentHash, ProofRoot: staged.ProofRoot,
			Closure: publish.StoreClosure{Store: h.store, Manifest: staged.Manifest}}, b)
	return h.reportPut(rc, err)
}

func (h *home) reportPut(rc *publish.Receipt, err error) error {
	g := h.g
	if rc != nil {
		g.out("entry  %s", rc.Txid)
		g.out("key    %s", rc.Entry.Key)
		if rc.Entry.Kind == storerec.KindPut {
			g.out("root   %s", hex.EncodeToString(rc.Entry.Root[:]))
			g.out("size   %d", rc.Entry.Size)
		}
		if rc.Hold != nil {
			g.out("hold   %s.%d, %d satoshis, t %d, K %d, deadline %d", rc.Hold.Txid, rc.Hold.Vout, rc.Hold.Sats,
				rc.Entry.Hold.T, rc.Entry.Hold.K, rc.Entry.Hold.Deadline)
		}
		for _, u := range rc.Uploads {
			if u.Err != nil {
				g.say("upload to %s: %v (bstore sync sends it again)", u.Provider, u.Err)
			} else {
				g.out("upload %s: %d (%d object(s) sent)", u.Provider, u.Status, u.Staged)
			}
		}
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, publish.ErrNotSent):
		return usage("%v: the outbox keeps sending it (bstore sync)", err)
	case errors.Is(err, publish.ErrNoHold):
		return incomplete("%v: run put again once the hold outputs being minted have mined (bstore sync)", err)
	}
	return err
}

// bountyF are a put's bounty flags.
type bountyF struct {
	sats, t, k, retention, deadline, checkC *uint64
	none                                    *bool
}

func bountyFlags(fs interface {
	Uint64(string, uint64, string) *uint64
	Bool(string, bool, string) *bool
}) *bountyF {
	return &bountyF{
		sats:      fs.Uint64("bounty", 0, "the bounty in satoshis (default config.json's)"),
		t:         fs.Uint64("t", 0, "distinct advertisers that must pass before anything is paid"),
		k:         fs.Uint64("k", 0, "the most advertisers paid"),
		retention: fs.Uint64("retention", 0, "seconds an eligible advertisement lasts at least"),
		deadline:  fs.Uint64("deadline", 0, "blocks ahead of the tip by which the bounty is settled"),
		checkC:    fs.Uint64("check-c", 0, "samples of the settlement challenge (30 recommended)"),
		none:      fs.Bool("no-bounty", false, "no bounty"),
	}
}

func (f *bountyF) bounty(c *Config) (*publish.Bounty, error) {
	if *f.none {
		return nil, nil
	}
	var b publish.Bounty
	if c.Bounty != nil {
		b = publish.Bounty{Satoshis: c.Bounty.Satoshis, T: c.Bounty.T, K: c.Bounty.K, Retention: c.Bounty.RetentionSeconds,
			DeadlineBlocks: c.Bounty.DeadlineBlocks, CheckC: c.Bounty.CheckC}
	}
	set := func(dst *uint64, v *uint64) {
		if *v != 0 {
			*dst = *v
		}
	}
	set(&b.Satoshis, f.sats)
	set(&b.T, f.t)
	set(&b.K, f.k)
	set(&b.Retention, f.retention)
	set(&b.DeadlineBlocks, f.deadline)
	set(&b.CheckC, f.checkC)
	if b.Satoshis == 0 {
		return nil, nil
	}
	if b.T == 0 {
		b.T = 1
	}
	if b.K == 0 {
		b.K = b.T
	}
	if b.Retention == 0 {
		b.Retention = 30 * 24 * 3600
	}
	if b.DeadlineBlocks == 0 {
		b.DeadlineBlocks = 144
	}
	if b.CheckC == 0 {
		b.CheckC = storerec.MinFreeC
	}
	if err := checkTerms(&storerec.Hold{Satoshis: b.Satoshis, T: b.T, K: b.K, Retention: b.Retention, CheckC: b.CheckC}); err != nil {
		return nil, fmt.Errorf("bounty: %w", err)
	}
	return &b, nil
}

func contentType(flagged, name string) string {
	ct := flagged
	if ct == "" && name != "" {
		ct = mime.TypeByExtension(filepath.Ext(name))
	}
	ct = strings.ToLower(strings.TrimSpace(strings.SplitN(ct, ";", 2)[0]))
	if storerec.CheckContentType(ct) != nil {
		return "application/octet-stream"
	}
	return ct
}

func metaPairs(kv []string) ([]storerec.MetaPair, error) {
	var out []storerec.MetaPair
	seen := map[string]bool{}
	for _, x := range kv {
		k, v, ok := strings.Cut(x, "=")
		k = strings.ToLower(k)
		if !ok || !storerec.CheckMetaKey(k) || !storerec.OneLine(v) || len(v) > 1024 {
			return nil, fmt.Errorf("meta %q: k=v, k of a-z, 0-9 and -, v one line of at most 1024 bytes", x)
		}
		if seen[k] {
			return nil, fmt.Errorf("meta %s given twice", k)
		}
		seen[k] = true
		out = append(out, storerec.MetaPair{Key: k, Value: v})
	}
	return out, nil
}

const getHelp = `usage: bstore get KEY [OUT|-] [-proof-root]

Read an object (spec section 15.2): the chain's latest token from the hosts,
walked back to seq 0 and checked unspent; the map's proof of the key; the
entry, its writer and its kind; the root and every branch, held to their
hashes and to profile 1's construction; every blob from the advertisers of
the root, each held to its hash before a byte of it is written, the next
advertiser asked when one fails and the one that served wrong bytes named;
and the content hash before the last blob is released. -proof-root also
recomputes the proof root. OUT is written whole or not at all; - (the
default) streams to standard output.

A key the map proves absent is NoSuchKey (exit 1); a key no host can
produce the entry of is incomplete (exit 3).`

func cmdGet(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("get", getHelp)
	pr := fs.Bool("proof-root", false, "recompute the proof root (step 9)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 1 || len(pos) > 2 {
		return usage("get KEY [OUT|-]")
	}
	key := pos[0]
	if err := storerec.CheckObjectKey(key); err != nil {
		return usage("key: %v", err)
	}
	out := "-"
	if len(pos) == 2 {
		out = pos[1]
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	ch, err := h.chain()
	if err != nil {
		return err
	}
	f, err := h.rd.Object(ctx, ch, key)
	if err != nil {
		return err
	}
	if f.Result == storerec.ObjectAbsent {
		return refused("NoSuchKey: %s is absent from %s, proven against the token at seq %d", key, ch.Bucket, f.Token.Bucket.Seq)
	}
	e := f.Entry.Entry
	obj, err := h.rd.Open(ctx, e, h.store)
	if err != nil {
		return err
	}
	var w io.Writer = g.stdout
	var tmp *os.File
	if out != "-" {
		if tmp, err = os.CreateTemp(filepath.Dir(out), ".bstore-get-*"); err != nil {
			return err
		}
		defer func() {
			if tmp != nil {
				tmp.Close()
				os.Remove(tmp.Name())
			}
		}()
		w = tmp
	}
	var built *storerec.Streaming
	if *pr {
		built = storerec.NewStreaming(func([]byte) error { return nil })
		w = io.MultiWriter(w, built)
	}
	if err := obj.WriteRange(ctx, w, 0, e.Size); err != nil {
		return err
	}
	if built != nil {
		b, err := built.Finish()
		if err != nil {
			return err
		}
		if b.ProofRoot != e.ProofRoot {
			return fmt.Errorf("%w: the writer's entry %s", storerec.ErrProofRoot, f.Entry.Txid())
		}
	}
	if tmp != nil {
		if err := tmp.Sync(); err != nil {
			return err
		}
		if err := tmp.Close(); err != nil {
			return err
		}
		if err := os.Rename(tmp.Name(), out); err != nil {
			return err
		}
		tmp = nil
		g.say("%s: %d bytes, entry %s, token seq %d", key, e.Size, f.Entry.Txid(), f.Token.Bucket.Seq)
	}
	return nil
}

const lsHelp = `usage: bstore ls [PREFIX] [-after KEY] [-max N]

List the keys of the bucket under PREFIX, each page proven complete
against the chain's latest token (spec section 11.3); a key whose entry a
host holds no copy of is read from another host, and is incomplete (exit
3) when none does. Each line: size, created (the writer's claim), key.`

func cmdLs(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("ls", lsHelp)
	after := fs.String("after", "", "list keys after this one")
	maxN := fs.Int("max", 0, "the most keys listed (0: all)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) > 1 {
		return usage("ls [PREFIX]")
	}
	prefix := ""
	if len(pos) == 1 {
		prefix = pos[0]
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	ch, err := h.chain()
	if err != nil {
		return err
	}
	n := 0
	a := *after
	for {
		pg, err := h.rd.List(ctx, ch, prefix, a)
		if err != nil {
			return err
		}
		for _, e := range pg.Entries {
			created := time.UnixMilli(int64(e.Entry.Created)).UTC().Format(time.RFC3339)
			g.out("%d\t%s\t%s", e.Entry.Size, created, e.Entry.Key)
			n++
			if *maxN > 0 && n >= *maxN {
				return nil
			}
		}
		if pg.Done || pg.Next == "" {
			return nil
		}
		a = pg.Next
	}
}

const rmHelp = `usage: bstore rm KEY

Publish a delete entry for KEY. The key leaves the map at the owner's next
token, as a removal; a delete of a key the map does not hold is named in
that token's consumed, never applied again.`

func cmdRm(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("rm", rmHelp)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("rm KEY")
	}
	if err := storerec.CheckObjectKey(pos[0]); err != nil {
		return usage("key: %v", err)
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	ch, err := h.chain()
	if err != nil {
		return err
	}
	rc, err := h.pub.Delete(ctx, ch.Bucket, ch.ID, pos[0], time.Time{})
	return h.reportPut(rc, err)
}

const cpHelp = `usage: bstore cp SRC DST [-to BUCKET] [flags]

A new put entry for DST naming SRC's content: the root, size, content hash,
proof root, content type, ETag and metadata of SRC's anchored entry, read
and checked as get reads it. Within one bucket nothing is published again;
to another bucket (-to) the closure is published on its blob topic, from
the home's store or from SRC's advertisers, every object held to its hash.
A bounty may be set as on put.`

func cmdCp(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("cp", cpHelp)
	to := fs.String("to", "", "the destination bucket (default the same)")
	bf := bountyFlags(fs)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usage("cp SRC DST")
	}
	for _, k := range pos {
		if err := storerec.CheckObjectKey(k); err != nil {
			return usage("key: %v", err)
		}
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	src, err := h.chain()
	if err != nil {
		return err
	}
	dst := src
	if *to != "" {
		save := g.bucket
		g.bucket = *to
		dst, err = h.chain()
		g.bucket = save
		if err != nil {
			return err
		}
	}
	b, err := bf.bounty(h.cfg)
	if err != nil {
		return usage("%v", err)
	}
	f, err := h.rd.Object(ctx, src, pos[0])
	if err != nil {
		return err
	}
	if f.Result == storerec.ObjectAbsent {
		return refused("NoSuchKey: %s is absent from %s", pos[0], src.Bucket)
	}
	e := f.Entry.Entry
	obj := publish.Object{Root: e.Root, Size: e.Size, ContentHash: e.ContentHash, ProofRoot: e.ProofRoot}
	if dst.Bucket != src.Bucket && e.Size > 0 {
		if m, err := h.store.Manifest(e.Root); err == nil {
			obj.Closure = publish.StoreClosure{Store: h.store, Manifest: m}
		} else {
			o, err := h.rd.Open(ctx, e, h.store)
			if err != nil {
				return err
			}
			obj.Closure = o
		}
	}
	rc, err := h.pub.Put(ctx, publish.Fields{Bucket: dst.Bucket, ID: dst.ID, Key: pos[1], ContentType: e.ContentType, ETag: e.ETag, Meta: e.Meta}, obj, b)
	return h.reportPut(rc, err)
}
