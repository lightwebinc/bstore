package main

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/lightwebinc/bstore/internal/anchor"
	"github.com/lightwebinc/bstore/storerec"
)

const bucketHelp = `usage: bstore bucket new NAME [-writers K1,K2]
       bstore bucket add BUCKET -owner KEY -id HEX
       bstore bucket list

new creates a bucket chain this identity owns (spec section 3): the
identifier NAME_<10 random letters>, a chain tree mined under
derive(owner, "chain"), and the first token spending it, naming the
writers (identity keys, besides the owner, whose entries the owner means
to anchor). Each is recorded before it is sent; a run that stops part way
is finished by running new again with the identifier it printed. It prints
the line a host's operator adds to BSTORE_SERVED, and adds the chain to
config.json.

add names a chain another owner keeps, so this home can read it and, as a
writer the owner named, write to it. list prints the chains config.json
names and the ones this identity owns.`

func cmdBucket(ctx context.Context, g *global, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		g.say("%s", bucketHelp)
		if len(args) == 0 {
			return &exitError{code: exitUsage}
		}
		return nil
	}
	sub := args[0]
	fs := g.flagSet("bucket "+sub, bucketHelp)
	var writers listFlag
	fs.Var(&writers, "writers", "writer keys, comma separated (new)")
	owner := fs.String("owner", "", "the owner's identity key (add)")
	id := fs.String("id", "", "the bucket id, 64 hex digits (add)")
	pos, err := parse(fs, args[1:])
	if err != nil {
		return err
	}
	switch sub {
	case "new", "add", "list":
	default:
		return usage("bucket new, add or list")
	}
	h, err := g.open(ctx)
	if err != nil {
		return err
	}
	defer h.close()
	switch sub {
	case "list":
		if len(pos) != 0 {
			return usage("bucket list")
		}
		for _, b := range h.cfg.Buckets {
			mark := ""
			if b.Bucket == h.cfg.Bucket {
				mark = " (default)"
			}
			if b.Owner == h.pub.IdentityHex() {
				mark += " (owned)"
			}
			g.out("%s owner %s id %s%s", b.Bucket, b.Owner, b.ID, mark)
		}
		return nil
	case "add":
		if len(pos) != 1 {
			return usage("bucket add BUCKET -owner KEY -id HEX")
		}
		ref := BucketRef{Bucket: strings.ReplaceAll(pos[0], "-", "_"), Owner: strings.ToLower(*owner), ID: strings.ToLower(*id)}
		if _, err := ref.chain(); err != nil {
			return usage("%v", err)
		}
		return h.addBucket(ref)
	}
	if len(pos) != 1 {
		return usage("bucket new NAME [-writers K1,K2]")
	}
	name := pos[0]
	bucket := name
	if storerec.CheckBucket(name) != nil {
		// A bare name: a fresh suffix.
		if bucket, err = anchor.NewIdentifier(name); err != nil {
			return usage("bucket name %q: 1 to 25 lowercase letters and single underscores, starting and ending with a letter", name)
		}
	}
	c, err := h.tool.New(ctx, bucket, writers)
	if c != nil && c.ID != "" {
		if aerr := h.addBucket(BucketRef{Bucket: c.Bucket, Owner: h.pub.IdentityHex(), ID: c.ID}); aerr != nil && err == nil {
			err = aerr
		}
		g.out("bucket %s", c.Bucket)
		g.out("owner  %s", h.pub.IdentityHex())
		g.out("id     %s", c.ID)
		g.out("served %s:%s:%s", c.Bucket, h.pub.IdentityHex(), c.ID)
		g.out("topics %s %s", storerec.RecordsTopic(c.Bucket), storerec.BlobTopic(c.Bucket))
	}
	switch {
	case errors.Is(err, anchor.ErrRefused):
		return refused("%v (a host that refused the first token does not serve this chain; when the reason is the token itself, make a fresh bucket with bucket new and a bare name)", err)
	case errors.Is(err, anchor.ErrIncomplete):
		return incomplete("%v (bucket new %s finishes a token not yet mined; bstore sync delivers a mined one)", err, bucket)
	}
	return err
}

func (h *home) addBucket(ref BucketRef) error {
	for i, b := range h.cfg.Buckets {
		if b.Bucket == ref.Bucket && b.Owner == ref.Owner {
			h.cfg.Buckets[i] = ref
			return saveConfig(h.g.configPath(), h.cfg)
		}
	}
	h.cfg.Buckets = append(h.cfg.Buckets, ref)
	if h.cfg.Bucket == "" {
		h.cfg.Bucket = ref.Bucket
	}
	return saveConfig(h.g.configPath(), h.cfg)
}

const anchorHelp = `usage: bstore anchor [-add-writer KEY] [-remove-writer KEY] [-dry-run]

The owner's anchoring tooling (spec sections 3.4 and 4.4), exactly as
frozen. It rebuilds the chain from the hosts (the rooted tokens it
verifies against its own headers, never a local cache) and the set of
entries the chain has named; walks each writer's unanchored entries at
every host, every page from the start; holds every entry to the writer,
bucket and id asked for and to its carrier's rules, and its funding
outpoint unspent by positive evidence from the node; folds the batch
against the current map (one key's entries ordered by created then
commitment, the last deciding; a delete of an absent key, a put of the
value the key names, and a put superseded in the batch go to consumed;
nothing the chain named is applied again); and mints the tokens that carry
it, split at 65,536 bytes. A host whose answer does not verify is named
and the run is incomplete (exit 3); a chain whose fold is broken is
refused (exit 1).

-add-writer and -remove-writer change the writers the next token names; a
writer is removed only when the map names none of its entries.`

func cmdAnchor(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("anchor", anchorHelp)
	var add, remove listFlag
	fs.Var(&add, "add-writer", "a writer key to add (repeatable)")
	fs.Var(&remove, "remove-writer", "a writer key to remove (repeatable)")
	dry := fs.Bool("dry-run", false, "plan and report; mint nothing")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("anchor takes flags only")
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
	if hex.EncodeToString(ch.Owner) != h.pub.IdentityHex() {
		return usage("bucket %s is owned by %s, not this identity: only its owner anchors it", ch.Bucket, short(hex.EncodeToString(ch.Owner)))
	}
	c, err := h.tool.Store.Find(ch.Bucket, hex.EncodeToString(ch.ID[:]))
	if err != nil {
		return usage("%v", err)
	}
	rep, err := h.tool.Anchor(ctx, c, anchor.Options{AddWriters: add, RemoveWriters: remove, DryRun: *dry})
	if rep != nil {
		g.out("chain %s at seq %d: %d entr(ies) found, %d already named", ch.Bucket, rep.FromSeq, rep.Entries, rep.Named)
		if rep.Plan != nil {
			verb := "minted"
			if *dry {
				verb = "would mint"
			}
			g.out("%s %d token(s): %d change(s), %d consumed", verb, len(rep.Plan.Steps), rep.Applied, rep.Consumed)
		}
		for _, t := range rep.Tokens {
			g.out("token %s", t)
		}
		for _, s := range rep.Swept {
			g.say("skipped, swept: %s", s)
		}
		for _, u := range rep.Unvouched {
			g.say("skipped, its funding outpoint has no evidence either way: %s", u)
		}
		if rep.Strangers > 0 {
			g.say("%d answer(s) named another writer, bucket or chain: not applied", rep.Strangers)
		}
		for _, x := range rep.Hosts {
			g.say("%s", x)
		}
		for _, k := range rep.Deferred {
			g.say("deferred, the map is at its bound: %s", k)
		}
	}
	return err
}
