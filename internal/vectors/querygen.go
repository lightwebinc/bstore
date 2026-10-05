package vectors

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/lightwebinc/bstore/internal/storerec"
)

func genQuery(files map[string][]byte) error {
	b := Bucket
	o := hx(Owner.Pub())
	i := h32(bucketID)
	w := hx(Alice.Pub())
	txid := displayHex(label("some txid"))
	root := h32(contentBuilt.RootHash)
	op := txid + ".0"
	tc := "1798761600000:" + txid
	valid := map[string]map[string]string{
		"bucket":             {"bucket": b, "owner": o, "id": i},
		"bucket-seq":         {"bucket": b, "owner": o, "id": i, "seq": "0"},
		"object":             {"bucket": b, "owner": o, "id": i, "key": "photos/cat.jpg"},
		"list":               {"bucket": b, "owner": o, "id": i, "prefix": "photos/", "after": ""},
		"pending":            {"bucket": b, "owner": o, "id": i, "pending": "photos/cat.jpg"},
		"unanchored":         {"bucket": b, "owner": o, "id": i, "writer": w, "after": ""},
		"entry":              {"bucket": b, "entry": txid},
		"ads":                {"bucket": b, "root": root, "after": ""},
		"ad":                 {"bucket": b, "root": root, "advertiser": hx(Bob.Pub())},
		"fronts":             {"bucket": b, "advertiser": hx(Bob.Pub()), "after": txid},
		"front":              {"bucket": b, "advertiser": hx(Bob.Pub()), "front": hx(Carol.Pub())},
		"hold":               {"bucket": b, "hold": op},
		"outcomes":           {"bucket": b, "answerer": hx(Bob.Pub())},
		"outcomes-of":        {"bucket": b, "provider": hx(Bob.Pub())},
		"outcomes-by":        {"bucket": b, "answerer": hx(Bob.Pub()), "buyer": hx(Carol.Pub())},
		"outcomes-of-by":     {"bucket": b, "provider": hx(Bob.Pub()), "buyer": hx(Carol.Pub())},
		"sweep":              {"bucket": b, "spent": op},
		"history":            {"bucket": b, "owner": o, "id": i, "history": "photos/cat.jpg", "before": "9007199254740991"},
		"chain":              {"bucket": b, "owner": o, "chain": i, "after": "0"},
		"export":             {"bucket": b, "owner": o, "export": i, "after": ""},
		"outcome-history":    {"bucket": b, "outcome-history": hx(Bob.Pub()), "before": tc},
		"outcome-history-of": {"bucket": b, "outcome-history-of": hx(Bob.Pub()), "before": tc},
	}
	type qcase struct {
		Name  string `json:"name"`
		JSON  string `json:"json"`
		Class string `json:"class"`
		Label string `json:"label"`
	}
	var cases []qcase
	add := func(name string, raw string) {
		q, err := storerec.ParseQuery([]byte(raw), nil, nil)
		c := qcase{Name: name, JSON: raw, Label: reason(err)}
		if q != nil {
			c.Class = q.Class
		}
		cases = append(cases, c)
	}
	js := func(m map[string]string) string {
		bb, _ := json.Marshal(m)
		return string(bb)
	}
	edit := func(class, member, v string) string {
		m := map[string]string{}
		for k, x := range valid[class] {
			m[k] = x
		}
		m[member] = v
		return js(m)
	}
	drop := func(class, member string) string {
		m := map[string]string{}
		for k, x := range valid[class] {
			if k != member {
				m[k] = x
			}
		}
		return js(m)
	}
	for _, c := range storerec.Classes {
		add(c.Name+": valid", js(valid[c.Name]))
		for _, mem := range c.Members {
			add(c.Name+": "+mem+" missing", drop(c.Name, mem))
		}
		add(c.Name+": an extra member", edit(c.Name, "extra", "x"))
	}
	// Every value rule.
	for _, v := range []struct{ class, member, value, note string }{
		{"bucket", "bucket", "Photos_qzvmkwerty", "uppercase"}, {"bucket", "bucket", "photos-qzvmkwerty", "hyphen"},
		{"bucket", "id", strings.ToUpper(i), "uppercase hex"}, {"bucket", "id", i[:62], "62 hex"},
		{"bucket", "owner", o[:64], "64 hex"}, {"bucket", "owner", strings.ToUpper(o), "uppercase hex"},
		{"bucket", "owner", hx(anyonePK), "the anyone key"}, {"bucket", "owner", hx(keyAlias), "x at or above the prime"},
		{"bucket", "owner", hx(keyOffCrv), "off the curve"},
		{"bucket-seq", "seq", "01", "a leading zero"}, {"bucket-seq", "seq", "9007199254740991", "2^53 - 1"},
		{"bucket-seq", "seq", "9007199254740992", "2^53"}, {"bucket-seq", "seq", "-1", "negative"}, {"bucket-seq", "seq", "1.0", "a fraction"},
		{"bucket-seq", "seq", "", "empty"},
		{"object", "key", "", "an empty key"}, {"object", "key", "a\nb", "a newline"}, {"object", "key", strings.Repeat("k", 1024), "1024 bytes"},
		{"object", "key", strings.Repeat("k", 1025), "1025 bytes"}, {"object", "key", "a ", "U+2028"},
		{"list", "prefix", strings.Repeat("p", 1024), "a prefix of 1024 bytes"}, {"list", "prefix", strings.Repeat("p", 1025), "a prefix of 1025 bytes"},
		{"list", "after", "a\tb", "an after with a tab"}, {"list", "after", "photos/z", "an after"},
		{"export", "after", "", "export from the start"}, {"export", "after", "\u0085", "export after U+0085"},
		{"unanchored", "after", txid, "a txid cursor"}, {"unanchored", "after", strings.ToUpper(txid), "an uppercase txid"},
		{"unanchored", "writer", hx(anyonePK), "the anyone key as writer"},
		{"entry", "entry", "", "an empty entry"}, {"entry", "entry", txid + "0", "65 hex"},
		{"ads", "root", strings.ToUpper(root), "an uppercase root"}, {"ads", "after", "x", "a bad cursor"},
		{"fronts", "after", "", "fronts from the start"},
		{"hold", "hold", txid + ".4294967295", "vout 2^32 - 1"}, {"hold", "hold", txid + ".4294967296", "vout 2^32"},
		{"hold", "hold", txid + ".01", "vout with a leading zero"}, {"hold", "hold", txid, "no vout"}, {"hold", "hold", txid + ".", "an empty vout"},
		{"hold", "hold", strings.ToUpper(txid) + ".0", "an uppercase txid"},
		{"sweep", "spent", txid + ".-1", "a negative vout"},
		{"history", "before", "0", "before 0"}, {"history", "before", "9007199254740992", "before 2^53"}, {"history", "before", tc, "a time cursor where a seq is wanted"},
		{"chain", "after", "", "an empty after"}, {"chain", "chain", strings.ToUpper(i), "uppercase"},
		{"outcome-history", "before", "0:" + txid, "a time cursor at 0"}, {"outcome-history", "before", "01:" + txid, "a leading zero"},
		{"outcome-history", "before", "1798761600000:" + strings.ToUpper(txid), "an uppercase txid"},
		{"outcome-history", "before", "1798761600000", "no txid"}, {"outcome-history", "before", "9007199254740992:" + txid, "past 2^53 - 1"},
		{"outcome-history-of", "outcome-history-of", hx(anyonePK), "the anyone key"},
		{"outcomes-by", "buyer", hx(key04), "prefix 04"},
	} {
		add(fmt.Sprintf("%s: %s %s", v.class, v.member, v.note), edit(v.class, v.member, v.value))
	}
	// Shapes.
	add("not an object", `["bucket"]`)
	add("null", `null`)
	add("not JSON", `{`)
	add("a number member", `{"bucket":"`+b+`","entry":1}`)
	add("a member given twice takes its last value (valid last)", `{"bucket":"x","entry":"`+txid+`","bucket":"`+b+`"}`)
	add("a member given twice takes its last value (invalid last)", `{"bucket":"`+b+`","entry":"`+txid+`","bucket":"x"}`)
	add("__proto__ is a member like any other", `{"bucket":"`+b+`","entry":"`+txid+`","__proto__":"x"}`)
	add("a lone surrogate in a key", `{"bucket":"`+b+`","owner":"`+o+`","id":"`+i+`","key":"a\ud800b"}`)
	add("an escaped newline in a key", `{"bucket":"`+b+`","owner":"`+o+`","id":"`+i+`","key":"a\nb"}`)
	add("an escaped character", `{"bucket":"`+b+`","owner":"`+o+`","id":"`+i+`","key":"café"}`)
	add("a nested object", `{"bucket":"`+b+`","entry":{"x":1}}`)
	return put(files, "query-v1.json", map[string]any{
		"note":    "every class with its exact members, every member missing or extra, every value rule; a question is refused (query) or names its class; a member given twice takes its last value",
		"classes": storerec.Classes,
		"cases":   cases,
	})
}
