package publish

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lightwebinc/bcommon/carrier"

	"github.com/lightwebinc/bstore/storerec"
)

// The identity keeps the entries it made (entries/<txid>.json and .beef),
// written before the entry is sent, so that it can replay one to a host
// that lacks it (fill) and say which of its entries a token has named
// (sync). An entry a token of its chain named is marked so, and kept,
// until the key is deleted: a delete's race pass drops the key's earlier
// entries (retire.go), which nothing replays or syncs again.

// EntryLog is one entry this identity made.
type EntryLog struct {
	Txid    string `json:"txid"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
	Kind    uint64 `json:"kind"`
	Created uint64 `json:"created"`
	Record  string `json:"record"` // hex
	// Named is the seq of the token that named it, once one has.
	Named *uint64 `json:"named,omitempty"`
}

func (p *Publisher) entriesDir() string { return filepath.Join(p.cfg.Dir, "entries") }

func (p *Publisher) logEntry(e *storerec.Entry, rec []byte, txid string, beef []byte) error {
	dir := p.entriesDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(p.cfg.Dir, "tmp")
	if err := writeFileSync(tmp, filepath.Join(dir, txid+".beef"), beef); err != nil {
		return err
	}
	raw, _ := json.Marshal(EntryLog{Txid: txid, Bucket: e.Bucket, Key: e.Key, Kind: e.Kind, Created: e.Created, Record: hex.EncodeToString(rec)})
	if err := writeFileSync(tmp, filepath.Join(dir, txid+".json"), raw); err != nil {
		return err
	}
	p.indexEntry(e, txid)
	return nil
}

// Entries are the entries this identity made, oldest claim first.
func (p *Publisher) Entries() []EntryLog {
	ents, _ := os.ReadDir(p.entriesDir())
	var out []EntryLog
	for _, d := range ents {
		if !strings.HasSuffix(d.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(p.entriesDir(), d.Name()))
		if err != nil {
			continue
		}
		var l EntryLog
		if json.Unmarshal(raw, &l) == nil && l.Txid+".json" == d.Name() {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Created != out[j].Created {
			return out[i].Created < out[j].Created
		}
		return out[i].Txid < out[j].Txid
	})
	return out
}

// EntryBEEF is an entry's carrier as it was sent.
func (p *Publisher) EntryBEEF(txid string) ([]byte, error) {
	if !isHex64(txid) {
		return nil, errors.New("publish: not a txid")
	}
	return os.ReadFile(filepath.Join(p.entriesDir(), txid+".beef"))
}

// EntryRecord is an entry this identity made, decoded.
func (p *Publisher) EntryRecord(txid string) (*storerec.Entry, *EntryLog, error) {
	if !isHex64(txid) {
		return nil, nil, errors.New("publish: not a txid")
	}
	raw, err := os.ReadFile(filepath.Join(p.entriesDir(), txid+".json"))
	if err != nil {
		return nil, nil, err
	}
	var l EntryLog
	if err := json.Unmarshal(raw, &l); err != nil {
		return nil, nil, err
	}
	rec, err := hex.DecodeString(l.Record)
	if err != nil {
		return nil, nil, err
	}
	e, err := storerec.DecodeEntry(rec)
	if err != nil {
		return nil, nil, err
	}
	return e, &l, nil
}

// MarkNamed records that the token at seq named the entry.
func (p *Publisher) MarkNamed(txid string, seq uint64) error {
	_, l, err := p.EntryRecord(txid)
	if err != nil {
		return err
	}
	if l.Named != nil && *l.Named == seq {
		return nil
	}
	l.Named = &seq
	raw, _ := json.Marshal(l)
	return writeFileSync(filepath.Join(p.cfg.Dir, "tmp"), filepath.Join(p.entriesDir(), txid+".json"), raw)
}

// Resend offers a BEEF to the given targets through the outbox, as any
// object this identity minted: written before it is sent, sent again
// until every target took it. It returns the targets that took it.
func (p *Publisher) Resend(ctx context.Context, id, what string, beef []byte, targets []Target) []Target {
	if len(targets) == 0 {
		return nil
	}
	it, err := p.outbox.add(id, what, beef, targets)
	if err != nil {
		p.cfg.Logf("%s %s: %v", what, short(id), err)
		return nil
	}
	return p.outbox.send(ctx, it, beef)
}

// ResendAll is Resend, and also returns the targets that refused the BEEF.
// A refusal is final: the outbox never offers those bytes to them again.
func (p *Publisher) ResendAll(ctx context.Context, id, what string, beef []byte, targets []Target) (ok, refused []Target) {
	if len(targets) == 0 {
		return nil, nil
	}
	it, err := p.outbox.add(id, what, beef, targets)
	if err != nil {
		p.cfg.Logf("%s %s: %v", what, short(id), err)
		return nil, nil
	}
	return p.outbox.sendAll(ctx, it, beef)
}

// HostTarget is one host's submit route for a bucket's records topic.
func HostTarget(host, bucket string) Target {
	return Target{Base: trim(host), Topic: storerec.RecordsTopic(bucket), Host: true}
}

// RecordTargets are where a mined object or a record of bucket goes: the
// facade, when there is one, and every host.
func (p *Publisher) RecordTargets(bucket string) []Target {
	return p.cfg.Delivery.recordTargets(bucket, false)
}

// PublishOutcome publishes an outcome record (spec section 8.1) on the
// bucket's records topic: a carrier on an outcome funding tree output
// recorded used first, written to the outbox before it is sent. It returns
// the carrier's txid.
func (p *Publisher) PublishOutcome(ctx context.Context, o *storerec.Outcome) (string, error) {
	o.Buyer = p.Identity()
	if o.Created == 0 {
		o.Created = uint64(p.cfg.Now().UnixMilli())
	}
	rec, err := o.Encode()
	if err != nil {
		return "", err
	}
	targets := p.cfg.Delivery.recordTargets(o.Bucket, false)
	if len(targets) == 0 {
		return "", errors.New("no facade or host to publish the outcome to")
	}
	if p.Killed() {
		return "", ErrKilled
	}
	if err := p.noteBucket(o.Bucket); err != nil {
		return "", err
	}
	tree, vout, err := p.spend(ctx, kindOutcome, 1)
	if err != nil {
		return "", err
	}
	defer p.releaseTree(tree)
	tx, err := carrier.Mint(ctx, p.wallet, p.wallet.Originator, carrierParams(storerec.OutcomeDerivation), rec, tree, vout)
	if err != nil {
		return "", err
	}
	beef, err := tx.AtomicBEEF(false)
	if err != nil {
		return "", err
	}
	txid := tx.TxID().String()
	it, err := p.outbox.add(txid, "outcome", beef, targets)
	if err != nil {
		return "", err
	}
	if ok := p.outbox.send(ctx, it, beef); len(ok) == 0 {
		return txid, fmt.Errorf("outcome %s: no target took it yet; it is sent again", short(txid))
	}
	return txid, nil
}

func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}
