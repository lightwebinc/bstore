package module

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/bstore/storerec"
)

// Reasons are every label a host counts a refused submission by: the
// record rules of spec sections 3.2, 4.2, 8.1 and 8.2 and the transaction
// rules of section 12.1. A reader's labels and query are never an
// admission verdict.
var Reasons = []string{
	"too-large", "cbor", "key-type", "magic", "missing", "type", "bucket", "identity", "seq", "writers", "changes", "count", "created",
	"consumed", "first", "objkey", "kind", "size", "profile", "content-type", "etag", "meta", "hold", "empty", "request", "outcome",
	"response-signature", "url", "shape", "beef", "mineable", "unlock", "lock", "signature", "funding", "unmined", "token-input",
	"bucket-id", "too-many", "piece", "not-bstore", "host-config", "range", "list",
}

// AdmitKinds are what an admitted transaction was.
var AdmitKinds = []string{"entry", "outcome", "front", "bucket-token", "mined", "sweep"}

// RecentAdmissions is how many admitted txids a manager remembers to count
// each once.
const RecentAdmissions = 4096

// Refused is raised for every refusal. The engine counts it as a failed
// topic for this submission and stores nothing.
type Refused struct {
	Reason string
	Detail string
}

func (r *Refused) Error() string { return "bstore: refused, " + r.Reason + ": " + r.Detail }

// ErrNoSubmission is a host that hands the manager a parsed BEEF without
// the submission's bytes (bcommon module.WithSubmission): the manager
// decides on the wire form (which transactions a BEEF carries, in what
// order, with which proofs), which a parsed and merged BEEF no longer
// shows.
var ErrNoSubmission = errors.New("bstore: the host did not hand the manager the submission's bytes (bcommon module.WithSubmission)")

// RecordsTopicManager is tm_bstore_<bucket>, the records topic of one
// bucket (spec section 12.1). The rules are storerec.Admit's; this is the
// engine seam: it runs them on the submission and turns the verdict into
// admittance instructions.
//
// The engine records every transaction a topic manager returns
// instructions for as applied, empty instructions included, and a later
// submission of the same txid is a duplicate no topic manager is shown
// again. A refusal returned as empty instructions would therefore be
// permanent for that txid: anyone could suppress a genuine object by
// submitting it first in a different BEEF, and a token refused because the
// host's headers lagged would stay refused. So every refusal is raised, and
// the engine records nothing and marks nothing spent. No rule reads another
// topic, another record or anything this topic holds; the previous coins
// decide only which held outputs an accepted transaction retains: every
// one, so no admitted output is deleted when something spends it.
type RecordsTopicManager struct {
	Topic  string
	Bucket string
	host   bmodule.Host
	h      storerec.Host

	mu     sync.Mutex
	recent map[string]bool
	order  []string
}

var (
	_ bmodule.TopicManager    = (*RecordsTopicManager)(nil)
	_ bmodule.InputIdentifier = (*RecordsTopicManager)(nil)
	_ bmodule.TopicManager    = (*BlobTopicManager)(nil)
	_ bmodule.InputIdentifier = (*BlobTopicManager)(nil)
)

// NewRecordsTopicManager makes the records manager of one topic; a bound
// below its floor is refused.
func NewRecordsTopicManager(topic string, host bmodule.Host, headers chaintracker.ChainTracker, maxBEEF, maxSweepInputs int) (*RecordsTopicManager, error) {
	if !strings.HasPrefix(topic, storerec.RecordsPrefix) || strings.HasPrefix(topic, storerec.BlobPrefix) {
		return nil, fmt.Errorf("bstore: %s is not a tm_bstore_ records topic", topic)
	}
	bucket := topic[len(storerec.RecordsPrefix):]
	if storerec.CheckBucket(bucket) != nil {
		return nil, fmt.Errorf("bstore: %s is not a bucket identifier <name>_<suffix>", bucket)
	}
	if maxBEEF < storerec.DefaultMaxBEEF {
		return nil, fmt.Errorf("bstore: a BEEF bound of %d is below %d", maxBEEF, storerec.DefaultMaxBEEF)
	}
	if maxSweepInputs < storerec.DefaultMaxSweepInputs {
		return nil, fmt.Errorf("bstore: a sweep input bound of %d is below %d", maxSweepInputs, storerec.DefaultMaxSweepInputs)
	}
	return &RecordsTopicManager{Topic: topic, Bucket: bucket, host: host,
		h: storerec.Host{Bucket: bucket, Headers: headers, MaxBEEF: maxBEEF, MaxSweepInputs: maxSweepInputs}, recent: map[string]bool{}}, nil
}

// Admit applies the rules to the submission's bytes with the held inputs,
// as the TypeScript manager's identifyAdmissibleOutputs does. A verdict
// Precheck left in ctx for these bytes is finished with held instead.
func (t *RecordsTopicManager) Admit(ctx context.Context, raw []byte, held []int) (overlay.AdmittanceInstructions, error) {
	var a *storerec.Admission
	var err error
	if pc, ok := ctx.Value(precheckKey{t}).(*precheck); ok && bytes.Equal(pc.raw, raw) {
		a, err = pc.p.Admit(held)
	} else {
		a, err = storerec.Admit(ctx, raw, held, t.h)
	}
	if err != nil {
		reason := storerec.Reason(err)
		if reason == "other" {
			return overlay.AdmittanceInstructions{}, err
		}
		t.host.Metrics().Inc("bstore_refused_total", map[string]string{"reason": reason}, 1)
		detail := strings.TrimPrefix(err.Error(), "storerec: ")
		t.host.Log("tm_bstore refused", map[string]any{"topic": t.Topic, "reason": reason, "detail": detail})
		return overlay.AdmittanceInstructions{}, &Refused{Reason: reason, Detail: detail}
	}
	t.count(string(a.Kind), chainhash.Hash(a.Txid).String())
	ai := overlay.AdmittanceInstructions{OutputsToAdmit: make([]uint32, 0, len(a.Outputs)), CoinsToRetain: make([]uint32, 0, len(a.Retain))}
	for _, o := range a.Outputs {
		ai.OutputsToAdmit = append(ai.OutputsToAdmit, uint32(o)) //nolint:gosec // bounded by the transaction's outputs
	}
	for _, r := range a.Retain {
		ai.CoinsToRetain = append(ai.CoinsToRetain, uint32(r)) //nolint:gosec // bounded by the transaction's inputs
	}
	return ai, nil
}

// IdentifyAdmissibleOutputs implements bcommon's module.TopicManager over
// the submission's bytes in ctx.
func (t *RecordsTopicManager) IdentifyAdmissibleOutputs(ctx context.Context, _ *transaction.Beef, _ *chainhash.Hash, previousCoins []uint32) (overlay.AdmittanceInstructions, error) {
	raw, ok := bmodule.Submission(ctx)
	if !ok {
		return overlay.AdmittanceInstructions{}, ErrNoSubmission
	}
	held := make([]int, 0, len(previousCoins))
	for _, c := range previousCoins {
		held = append(held, int(c))
	}
	return t.Admit(ctx, raw, held)
}

// precheck is a submission's verdict decided before the host took the
// topic's lock.
type precheck struct {
	raw []byte
	p   *storerec.Pending
}

type precheckKey struct{ t *RecordsTopicManager }

// Precheck decides the submission in ctx (bcommon module.WithSubmission) on
// every rule but the range of its previous coins (storerec.Prepare) and
// returns ctx carrying the verdict, which Admit finishes under the lock.
// No rule reads what the topic holds, so a host that runs this before it
// takes the topic's lock (a Go host's admission pipeline can) holds the lock
// only for what the topic holds. It counts and logs nothing: Admit does,
// when the host shows it the transaction.
func (t *RecordsTopicManager) Precheck(ctx context.Context) context.Context {
	raw, ok := bmodule.Submission(ctx)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, precheckKey{t}, &precheck{raw: raw, p: storerec.Prepare(ctx, raw, t.h)})
}

// IdentifyNeededInputs implements module.InputIdentifier: nothing is
// needed, since a carrier carries its funding tree, a token its parent and
// a mined object its own proof.
func (*RecordsTopicManager) IdentifyNeededInputs(context.Context, *transaction.Beef, *chainhash.Hash) ([]*transaction.Outpoint, error) {
	return nil, nil
}

// count counts an admission once per transaction. The engine shows a
// manager a txid it already applied only when two submissions of it race
// past its duplicate check, or when the index dropped it and evicted the
// engine's record, so bstore_admitted_total counts transactions not seen
// lately and bstore_admitted_repeats_total the rest.
func (t *RecordsTopicManager) count(kind, txid string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.recent[txid] {
		t.host.Metrics().Inc("bstore_admitted_repeats_total", map[string]string{"kind": kind}, 1)
		return
	}
	t.recent[txid] = true
	t.order = append(t.order, txid)
	if len(t.order) > RecentAdmissions {
		delete(t.recent, t.order[0])
		t.order = t.order[1:]
	}
	t.host.Metrics().Inc("bstore_admitted_total", map[string]string{"kind": kind}, 1)
}

// GetDocumentation implements module.TopicManager.
func (t *RecordsTopicManager) GetDocumentation() string {
	return strings.Join([]string{
		"# " + t.Topic,
		"",
		"The records topic of the bstore bucket " + t.Bucket + " (spec section 12.1): object",
		"entries, outcome records and front statements, each on a carrier that spends one output of",
		"its owner's mined funding tree; bucket tokens, each mined and published with its parent;",
		"advertisements and hold outputs, mined; and output 0 of mined sweeps. Each is admitted on",
		"its own bytes and the host's headers; no other record and nothing the topic holds is read.",
		"Pieces belong on the blob topic and are never admitted. Every refusal is raised.",
	}, "\n")
}

// GetMetaData implements module.TopicManager.
func (t *RecordsTopicManager) GetMetaData() *overlay.MetaData {
	return &overlay.MetaData{Name: t.Topic, Description: "Admits the records of the bstore bucket " + t.Bucket + "."}
}

// BlobTopicManager is a bucket's blob topic on a host: it carries pieces
// only, and no host admits a piece or holds bytes (spec sections 5 and
// 12.1), so every submission is refused, raised and counted as piece.
// Mounted only where the operator names the topic, so that the host's
// default manager never admits anything on it.
type BlobTopicManager struct {
	Topic  string
	Bucket string
	host   bmodule.Host
}

// NewBlobTopicManager makes the blob manager of one topic.
func NewBlobTopicManager(topic string, host bmodule.Host) (*BlobTopicManager, error) {
	if !strings.HasPrefix(topic, storerec.BlobPrefix) {
		return nil, fmt.Errorf("bstore: %s is not a tm_bstoreblob_ topic", topic)
	}
	bucket := topic[len(storerec.BlobPrefix):]
	if storerec.CheckBucket(bucket) != nil {
		return nil, fmt.Errorf("bstore: %s is not a bucket identifier <name>_<suffix>", bucket)
	}
	return &BlobTopicManager{Topic: topic, Bucket: bucket, host: host}, nil
}

// IdentifyAdmissibleOutputs implements module.TopicManager: refused, piece.
func (t *BlobTopicManager) IdentifyAdmissibleOutputs(context.Context, *transaction.Beef, *chainhash.Hash, []uint32) (overlay.AdmittanceInstructions, error) {
	t.host.Metrics().Inc("bstore_refused_total", map[string]string{"reason": "piece"}, 1)
	return overlay.AdmittanceInstructions{}, &Refused{Reason: "piece", Detail: "a host admits nothing on a blob topic: pieces go to providers"}
}

// IdentifyNeededInputs implements module.InputIdentifier.
func (*BlobTopicManager) IdentifyNeededInputs(context.Context, *transaction.Beef, *chainhash.Hash) ([]*transaction.Outpoint, error) {
	return nil, nil
}

// GetDocumentation implements module.TopicManager.
func (t *BlobTopicManager) GetDocumentation() string {
	return "# " + t.Topic + "\n\nThe blob topic of the bstore bucket " + t.Bucket + ": pieces, which providers hold. A host admits nothing here."
}

// GetMetaData implements module.TopicManager.
func (t *BlobTopicManager) GetMetaData() *overlay.MetaData {
	return &overlay.MetaData{Name: t.Topic, Description: "Refuses everything: the blob topic of " + t.Bucket + " is for providers."}
}
