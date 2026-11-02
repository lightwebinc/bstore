package provider

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

func fundingAt(n byte, index uint32) fundingKey {
	var h chainhash.Hash
	h[0] = n
	return fundingKey{txid: h, index: index}
}

func TestFundingsOneCarrierAnOutpoint(t *testing.T) {
	b, err := loadFundings(t.TempDir(), nil, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	k := fundingAt(1, 2)
	a, other := [32]byte{1}, [32]byte{2}
	if !b.free(k, a) {
		t.Fatal("an outpoint nothing claimed is not free")
	}
	b.claim(k, a, 1000)
	b.claim(k, other, 1000)
	if !b.free(k, a) || b.free(k, other) {
		t.Fatal("the first carrier does not stand alone on its outpoint")
	}
}

func TestFundingsSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(1_000_000, 0)
	b, err := loadFundings(dir, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	b.claim(fundingAt(1, 0), [32]byte{7}, now.Unix())
	add, whole := b.pending()
	if whole {
		t.Fatal("a short log is appended to")
	}
	if err := b.write(add, whole); err != nil {
		t.Fatal(err)
	}
	// A record a crash cut off goes; the one before it stays, and what is
	// appended after it lines up.
	f, err := os.OpenFile(filepath.Join(dir, "fundings.log"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, fundingRecord/2)); err != nil {
		t.Fatal(err)
	}
	f.Close()
	b, err = loadFundings(dir, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if b.free(fundingAt(1, 0), [32]byte{8}) {
		t.Fatal("a claim did not survive the restart")
	}
	b.claim(fundingAt(2, 0), [32]byte{9}, now.Unix())
	add, whole = b.pending()
	if err := b.write(add, whole); err != nil {
		t.Fatal(err)
	}
	b, err = loadFundings(dir, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if b.free(fundingAt(1, 0), [32]byte{8}) || b.free(fundingAt(2, 0), [32]byte{8}) {
		t.Fatal("the log does not line up after a torn tail")
	}
}

func TestFundingsTakenFromAnOldState(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	var h chainhash.Hash
	h[3] = 9
	c := [32]byte{5}
	old := map[string]*Funding{
		h.String() + ".4": {C: hex.EncodeToString(c[:]), At: now.Unix()},
		"not an outpoint": {C: hex.EncodeToString(c[:]), At: now.Unix()},
		h.String() + ".5": {C: hex.EncodeToString(c[:]), At: now.Add(-fundingMemory - time.Hour).Unix()},
	}
	b, err := loadFundings(t.TempDir(), old, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.marks) != 1 || b.free(fundingKey{txid: h, index: 4}, [32]byte{6}) {
		t.Fatalf("taken in: %d rows", len(b.marks))
	}
	if len(b.unsaved) != fundingRecord {
		t.Fatal("what an old state held does not reach the log")
	}
}

func TestFundingsForgetTheOldestFirst(t *testing.T) {
	b, err := loadFundings(t.TempDir(), nil, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	key := func(i int) fundingKey { return fundingKey{index: uint32(i)} }
	for i := range maxFundings + 10 {
		b.claim(key(i), [32]byte{1}, 1000)
	}
	if len(b.marks) != maxFundings {
		t.Fatalf("%d rows kept", len(b.marks))
	}
	if !b.free(key(0), [32]byte{2}) || !b.free(key(9), [32]byte{2}) {
		t.Fatal("the oldest are still held")
	}
	if b.free(key(10), [32]byte{2}) || b.free(key(maxFundings+9), [32]byte{2}) {
		t.Fatal("a newer claim was forgotten")
	}
	b.expire(2000)
	if len(b.marks) != 0 {
		t.Fatalf("%d rows past their memory", len(b.marks))
	}
}

func TestFundingLogWrittenAfreshWhenMostlyForgotten(t *testing.T) {
	dir := t.TempDir()
	b, err := loadFundings(dir, nil, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	for i := range maxFundings / 4 {
		b.claim(fundingKey{index: uint32(i)}, [32]byte{1}, 1000)
	}
	add, whole := b.pending()
	if whole {
		t.Fatal("a log no longer than its claims was written afresh")
	}
	if err := b.write(add, whole); err != nil {
		t.Fatal(err)
	}
	b.logged = len(add) / fundingRecord
	b.expire(2000)
	b.claim(fundingKey{index: 1 << 30}, [32]byte{1}, 3000)
	add, whole = b.pending()
	if !whole || len(add) != fundingRecord {
		t.Fatalf("whole %v, %d bytes", whole, len(add))
	}
}

// filled sets every exported field the type reaches, so a reference field
// added later is seen by the snapshot test without the test naming it.
func filled(v reflect.Value) {
	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		filled(v.Elem())
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		k, e := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		filled(k)
		filled(e)
		v.SetMapIndex(k, e)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 2, 2))
		filled(v.Index(0))
		filled(v.Index(1))
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				filled(v.Field(i))
			}
		}
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	}
}

// shared names a reference both values reach.
func shared(a, b reflect.Value, path string) string {
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return ""
		}
		if a.Pointer() == b.Pointer() {
			return path
		}
		return shared(a.Elem(), b.Elem(), path+".*")
	case reflect.Map:
		if a.Len() > 0 && a.Pointer() == b.Pointer() {
			return path
		}
		for _, k := range a.MapKeys() {
			if s := shared(a.MapIndex(k), b.MapIndex(k), path+"["+k.String()+"]"); s != "" {
				return s
			}
		}
	case reflect.Slice:
		if a.Len() > 0 && a.Pointer() == b.Pointer() {
			return path
		}
		for i := range a.Len() {
			if s := shared(a.Index(i), b.Index(i), path+"[]"); s != "" {
				return s
			}
		}
	case reflect.Struct:
		for i := range a.NumField() {
			f := a.Type().Field(i)
			if f.IsExported() {
				if s := shared(a.Field(i), b.Field(i), path+"."+f.Name); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

func TestSnapshotSharesNothing(t *testing.T) {
	st := &State{}
	filled(reflect.ValueOf(st).Elem())
	st.Fundings = nil
	snap := st.snapshot()
	if !reflect.DeepEqual(st, snap) {
		t.Fatal("the snapshot is not the state")
	}
	if s := shared(reflect.ValueOf(st).Elem(), reflect.ValueOf(snap).Elem(), "state"); s != "" {
		t.Fatalf("the snapshot shares %s", strings.TrimPrefix(s, "state."))
	}
}
