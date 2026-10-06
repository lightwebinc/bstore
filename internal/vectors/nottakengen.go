package vectors

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/lightwebinc/bcommon/cbor"

	"github.com/lightwebinc/bstore/storerec"
)

// genNotTaken writes the payment-not-taken note's vectors (frozen.md row
// C30): notes as a provider signs them, every step of the body's and the
// signed note's refusal order, and the buyer's check against its own
// payment, request and answerer.
func genNotTaken(files map[string][]byte) error {
	req := must((&storerec.Request{Root: label("note root"), ProofRoot: label("note proof root"), Size: 104857600,
		Provider: Bob.Pub(), Answerer: Bob.Pub(), SeedHeight: 38000, SeedHash: label("note seed"), Nonce: label16("note nonce"), C: 30}).Encode())
	pay := label("note payment")
	base := &storerec.NotTaken{Payment: pay, Request: sha256.Sum256(req), Reason: "unknown-root", Time: 1798761600, Answerer: Bob.Pub()}
	sign := func(p Party, body []byte) []byte { return must(storerec.SignNotTaken(challengePriv(p), body)) }

	type note struct {
		Name     string         `json:"name"`
		Request  string         `json:"request"`
		Body     string         `json:"body"`
		Signed   string         `json:"signed"`
		View     map[string]any `json:"view"`
		Signer   string         `json:"signer"`
		KeyHex   string         `json:"challengeKey"`
		PayDispl string         `json:"paymentTxid"`
	}
	var notes []note
	for _, c := range []struct {
		name   string
		reason string
		time   uint64
		who    Party
	}{
		{"unknown-root at the vectors' time", "unknown-root", 1798761600, Bob},
		{"busy, time 0", "busy", 0, Bob},
		{"seed, time at its bound", "seed", storerec.MaxU32, Bob},
		{"a front's note (the answerer is the front)", "request", 1798761600, Dave},
		{"a reason of 64 characters", strings.Repeat("a", 63) + "-", 1798761600, Carol},
	} {
		n := *base
		n.Reason, n.Time, n.Answerer = c.reason, c.time, c.who.Pub()
		body := must(n.Encode())
		signed := sign(c.who, body)
		got, err := storerec.VerifyNotTaken(signed)
		if err != nil || got.Reason != c.reason || got.Time != c.time {
			return fmt.Errorf("note %q does not verify: %v", c.name, err)
		}
		notes = append(notes, note{Name: c.name, Request: hx(req), Body: hx(body), Signed: hx(signed), View: noteView(&n),
			Signer: c.who.Name, KeyHex: hx(must(storerec.ChallengeKey(c.who.Pub())).Compressed()), PayDispl: displayHex(pay)})
	}

	// The body's refusals, in order.
	valid := cbor.Map{
		{Key: storerec.NKMagic, Val: storerec.MagicNotTaken},
		{Key: storerec.NKPayment, Val: pay[:]},
		{Key: storerec.NKRequest, Val: base.Request[:]},
		{Key: storerec.NKReason, Val: base.Reason},
		{Key: storerec.NKTime, Val: base.Time},
		{Key: storerec.NKAnswerer, Val: Bob.Pub()},
	}
	type refusal struct {
		Kind  string `json:"kind"`
		Name  string `json:"name"`
		Hex   string `json:"hex"`
		Label string `json:"label"`
		Rule  string `json:"rule,omitempty"`
	}
	var refs []refusal
	body := func(name string, b []byte, rule string) {
		refs = append(refs, refusal{Kind: "body", Name: name, Hex: hx(b), Label: "not-taken", Rule: rule})
	}
	bm := func(name string, m cbor.Map, rule string) { body(name, enc(m), rule) }
	// The record steps every bstore record takes, each with its label as
	// the note's rule.
	for _, c := range recordSteps("note", valid, storerec.MaxNotTaken, storerec.MagicNotTaken, storerec.NKLast) {
		body(c.Name, unhex(c.Hex), c.Label)
	}
	bm("a key above 5", with(valid, uint64(6), uint64(0)), "unknown-key")
	bm("payment missing", without(valid, storerec.NKPayment), "missing")
	bm("payment of 31 bytes", with(valid, storerec.NKPayment, pay[:31]), "type")
	bm("request missing", without(valid, storerec.NKRequest), "missing")
	bm("request as text", with(valid, storerec.NKRequest, "r"), "type")
	bm("reason missing", without(valid, storerec.NKReason), "missing")
	bm("reason as bytes", with(valid, storerec.NKReason, []byte("seed")), "type")
	bm("reason empty", with(valid, storerec.NKReason, ""), "reason")
	bm("reason in capitals", with(valid, storerec.NKReason, "Seed"), "reason")
	bm("reason with a space", with(valid, storerec.NKReason, "not taken"), "reason")
	bm("reason of 65 characters", with(valid, storerec.NKReason, strings.Repeat("a", 65)), "reason")
	bm("time missing", without(valid, storerec.NKTime), "missing")
	bm("time as text", with(valid, storerec.NKTime, "1"), "type")
	bm("time past 2^32 - 1", with(valid, storerec.NKTime, uint64(storerec.MaxU32)+1), "time")
	bm("answerer missing", without(valid, storerec.NKAnswerer), "missing")
	bm("answerer as text", with(valid, storerec.NKAnswerer, "02"), "type")
	for _, bk := range badKeys {
		bm("answerer "+bk.name, with(valid, storerec.NKAnswerer, bk.k), "identity")
	}
	okBody := enc(valid)
	refs = append(refs, refusal{Kind: "body", Name: "the valid body", Hex: hx(okBody), Label: "ok"})

	// The signed note's refusals.
	signed := func(name string, b []byte, label, rule string) {
		refs = append(refs, refusal{Kind: "signed", Name: name, Hex: hx(b), Label: label, Rule: rule})
	}
	good := sign(Bob, okBody)
	okDigest := sha256.Sum256(okBody)
	sig := must(challengePriv(Bob).Sign(okDigest[:]))
	otherDigest := sha256.Sum256(enc(with(valid, storerec.NKReason, "seed")))
	respDigest := storerec.SignedDigest(base.Request, okDigest)
	signed("a signed note", good, "ok", "")
	signed("over 512 bytes", enc([]cbor.Value{okBody, make([]byte, storerec.MaxNotTaken)}), "not-taken", "too-large")
	signed("not CBOR", []byte{0xff}, "not-taken", "shape")
	signed("one element", enc([]cbor.Value{okBody}), "not-taken", "shape")
	signed("the signature as text", enc([]cbor.Value{okBody, "sig"}), "not-taken", "shape")
	signed("a body that does not decode", enc([]cbor.Value{[]byte{0xff}, sig.Serialize()}), "not-taken", "cbor")
	signed("signed by another identity's challenge key", sign(Carol, okBody), "not-taken", "signature")
	signed("signed by the identity key itself", must(storerec.SignNotTaken(Bob.Priv, okBody)), "not-taken", "signature")
	signed("a high-S signature", enc([]cbor.Value{okBody, highS(sig.Serialize())}), "not-taken", "signature")
	signed("a signature over another body", enc([]cbor.Value{okBody, must(challengePriv(Bob).Sign(otherDigest[:])).Serialize()}), "not-taken", "signature")
	signed("a response's digest signed instead", enc([]cbor.Value{okBody, must(challengePriv(Bob).Sign(respDigest[:])).Serialize()}), "not-taken", "signature")
	for i, r := range refs {
		var err error
		if r.Kind == "body" {
			_, err = storerec.DecodeNotTakenBody(unhex(r.Hex))
		} else {
			_, err = storerec.VerifyNotTaken(unhex(r.Hex))
		}
		if got := reason(err); got != r.Label {
			return fmt.Errorf("note refusal %q: got %s (%v), want %s", r.Name, got, err, r.Label)
		}
		if got := storerec.Rule(err); got != r.Rule {
			return fmt.Errorf("note refusal %q: rule %q, want %q", r.Name, got, r.Rule)
		}
		refs[i] = r
	}

	// The buyer's check.
	type check struct {
		Name     string `json:"name"`
		Signed   string `json:"signed"`
		Payment  string `json:"payment"`
		Request  string `json:"request"`
		Answerer string `json:"answerer"`
		Label    string `json:"label"`
		Rule     string `json:"rule,omitempty"`
	}
	var checks []check
	add := func(name string, s []byte, p [32]byte, rq, ans []byte, label, rule string) {
		_, err := storerec.CheckNotTaken(s, p, rq, ans)
		if got := reason(err); got != label || storerec.Rule(err) != rule {
			panic(fmt.Sprintf("note check %q: got %s/%s (%v), want %s/%s", name, got, storerec.Rule(err), err, label, rule))
		}
		checks = append(checks, check{Name: name, Signed: hx(s), Payment: h32(p), Request: hx(rq), Answerer: hx(ans), Label: label, Rule: rule})
	}
	add("its own payment, request and answerer", good, pay, req, Bob.Pub(), "ok", "")
	add("another payment", good, label("another payment"), req, Bob.Pub(), "not-taken", "payment")
	add("the payment in display order", good, [32]byte(unhex(displayHex(pay))), req, Bob.Pub(), "not-taken", "payment")
	add("another request", good, pay, append([]byte(nil), req[:len(req)-1]...), Bob.Pub(), "not-taken", "request")
	add("another answerer asked", good, pay, req, Carol.Pub(), "not-taken", "answerer")
	add("a note that does not verify", sign(Carol, okBody), pay, req, Bob.Pub(), "not-taken", "signature")

	return put(files, "nottaken-v1.json", map[string]any{
		"note": "the payment-not-taken note (frozen.md row C30, pending the owner's confirmation of its bytes): notes as signed, " +
			"every step of the body's and the signed note's refusal order (label not-taken, rule the step), and the buyer's check",
		"notes":    notes,
		"refusals": refs,
		"checks":   checks,
	})
}

func noteView(n *storerec.NotTaken) map[string]any {
	return map[string]any{"payment": h32(n.Payment), "request": h32(n.Request), "reason": n.Reason, "time": n.Time, "answerer": hx(n.Answerer)}
}
