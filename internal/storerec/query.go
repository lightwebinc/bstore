package storerec

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Class is one lookup question class of spec section 11.1: its members,
// whether it is free, and whether it answers only for a served chain.
type Class struct {
	Name    string
	Members []string
	Free    bool
	Served  bool
}

// Classes are every class ls_bstore answers. No priced class is a superset
// of a free class: each carries a member no free class carries.
var Classes = []Class{
	{"bucket", []string{"bucket", "owner", "id"}, true, false},
	{"bucket-seq", []string{"bucket", "owner", "id", "seq"}, true, false},
	{"object", []string{"bucket", "owner", "id", "key"}, true, true},
	{"list", []string{"bucket", "owner", "id", "prefix", "after"}, true, true},
	{"pending", []string{"bucket", "owner", "id", "pending"}, true, true},
	{"unanchored", []string{"bucket", "owner", "id", "writer", "after"}, true, true},
	{"entry", []string{"bucket", "entry"}, true, false},
	{"ads", []string{"bucket", "root", "after"}, true, false},
	{"ad", []string{"bucket", "root", "advertiser"}, true, false},
	{"fronts", []string{"bucket", "advertiser", "after"}, true, false},
	{"front", []string{"bucket", "advertiser", "front"}, true, false},
	{"hold", []string{"bucket", "hold"}, true, false},
	{"outcomes", []string{"bucket", "answerer"}, true, false},
	{"outcomes-of", []string{"bucket", "provider"}, true, false},
	{"outcomes-by", []string{"bucket", "answerer", "buyer"}, true, false},
	{"outcomes-of-by", []string{"bucket", "provider", "buyer"}, true, false},
	{"sweep", []string{"bucket", "spent"}, true, false},
	{"history", []string{"bucket", "owner", "id", "history", "before"}, false, true},
	{"chain", []string{"bucket", "owner", "chain", "after"}, false, false},
	{"export", []string{"bucket", "owner", "export", "after"}, false, true},
	{"outcome-history", []string{"bucket", "outcome-history", "before"}, false, false},
	{"outcome-history-of", []string{"bucket", "outcome-history-of", "before"}, false, false},
}

// Question is a validated question: its class and its members as given.
type Question struct {
	Class  string
	Values map[string]string
}

func classOf(names []string) (Class, bool) {
	sort.Strings(names)
	for _, c := range Classes {
		m := append([]string(nil), c.Members...)
		sort.Strings(m)
		if strings.Join(m, ",") == strings.Join(names, ",") {
			return c, true
		}
	}
	return Class{}, false
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// decimal is a decimal of 0 to MaxSafe with no leading zero.
func decimal(s string) (uint64, bool) {
	if len(s) == 0 || len(s) > 16 || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil && n <= MaxSafe
}

func keyHex(s string) bool {
	if !isLowerHex(s, 66) {
		return false
	}
	b, _ := hex.DecodeString(s)
	_, err := CheckKey(b)
	return err == nil
}

// timeCursor is <created>:<txid>, created a decimal with no leading zero,
// the txid 64 lowercase hex digits in display order.
func timeCursor(s string) bool {
	c, t, ok := strings.Cut(s, ":")
	_, okN := decimal(c)
	return ok && okN && isLowerHex(t, 64)
}

// outpoint is BRC-100's OutpointString: a txid of 64 lowercase hex digits,
// a dot, and the output index in decimal with no leading zero, below 2^32.
func outpoint(s string) bool {
	t, v, ok := strings.Cut(s, ".")
	if !ok || !isLowerHex(t, 64) || len(v) == 0 || len(v) > 10 || (len(v) > 1 && v[0] == '0') {
		return false
	}
	for i := 0; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	n, err := strconv.ParseUint(v, 10, 64)
	return err == nil && n <= MaxU32
}

// checkValue is one member's rule in its class (spec section 11.2).
func checkValue(class, name, v string) bool {
	switch name {
	case "bucket":
		return CheckBucket(v) == nil
	case "id", "export", "chain", "root":
		return isLowerHex(v, 64)
	case "owner", "writer", "advertiser", "front", "answerer", "provider", "buyer", "outcome-history", "outcome-history-of":
		return keyHex(v)
	case "key", "pending", "history":
		return CheckObjectKey(v) == nil
	case "prefix":
		return len(v) <= MaxObjectKey && OneLine(v)
	case "entry":
		return isLowerHex(v, 64)
	case "hold", "spent":
		return outpoint(v)
	case "seq":
		_, ok := decimal(v)
		return ok
	case "before":
		if class == "history" {
			_, ok := decimal(v)
			return ok
		}
		return timeCursor(v)
	case "after":
		switch class {
		case "list", "export":
			return len(v) <= MaxObjectKey && OneLine(v)
		case "unanchored", "ads", "fronts":
			return v == "" || isLowerHex(v, 64)
		case "chain":
			_, ok := decimal(v)
			return ok
		}
	}
	return false
}

// ParseQuery validates a BRC-24 query given as JSON text, as parsed: a
// member given twice takes its last value. It must be an object carrying
// exactly the members of one class, each a string meeting its rule, naming
// a bucket the host carries (carries nil admits every bucket) and, for a
// served class, a chain the host serves (serves nil admits every chain);
// anything else is refused, never answered empty and never ignored.
func ParseQuery(raw []byte, carries func(bucket string) bool, serves func(bucket, owner, id string) bool) (*Question, error) {
	var q map[string]any
	if err := json.Unmarshal(raw, &q); err != nil || q == nil {
		return nil, fmt.Errorf("%w: not an object", ErrQuery)
	}
	return CheckQuery(q, carries, serves)
}

// CheckQuery is ParseQuery on a parsed object.
func CheckQuery(q map[string]any, carries func(bucket string) bool, serves func(bucket, owner, id string) bool) (*Question, error) {
	names := make([]string, 0, len(q))
	for k := range q {
		names = append(names, k)
	}
	class, ok := classOf(names)
	if !ok {
		return nil, fmt.Errorf("%w: the members of no class", ErrQuery)
	}
	out := &Question{Class: class.Name, Values: map[string]string{}}
	for _, n := range names {
		s, ok := q[n].(string)
		if !ok {
			return nil, fmt.Errorf("%w: %s is not a string", ErrQuery, n)
		}
		if !checkValue(class.Name, n, s) {
			return nil, fmt.Errorf("%w: %s breaks its rule", ErrQuery, n)
		}
		out.Values[n] = s
	}
	if carries != nil && !carries(out.Values["bucket"]) {
		return nil, fmt.Errorf("%w: a bucket this host does not carry", ErrQuery)
	}
	if class.Served && serves != nil {
		id := out.Values["id"]
		if class.Name == "export" {
			id = out.Values["export"]
		}
		if !serves(out.Values["bucket"], out.Values["owner"], id) {
			return nil, fmt.Errorf("%w: a chain this host does not serve", ErrQuery)
		}
	}
	return out, nil
}
