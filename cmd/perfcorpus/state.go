package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/bstore/storerec"
)

// ident is one identity of the corpus: a key and a BRC-100 wallet over it.
type ident struct {
	Name string
	Priv *ec.PrivateKey
	W    wallet.Interface
}

func (i *ident) Pub() []byte { return i.Priv.PubKey().Compressed() }

func (i *ident) PubHex() string { return hex.EncodeToString(i.Pub()) }

func newIdent(name, privHex string) (*ident, error) {
	k, err := ec.PrivateKeyFromHex(privHex)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	w, err := wallet.NewCompletedProtoWallet(k)
	if err != nil {
		return nil, err
	}
	return &ident{Name: name, Priv: k, W: w}, nil
}

// idsFile is ids.json: the identities' private keys (lab only) and the
// perf buckets' names. Identities: owner (the buckets' owner), writer (the
// one writer the first token names), buyer (outcomes, and the front of the
// front statements) and three providers (advertisements).
type idsFile struct {
	Owner     string   `json:"owner"`
	Writer    string   `json:"writer"`
	Buyer     string   `json:"buyer"`
	Providers []string `json:"providers"`
	Buckets   []string `json:"buckets"`
}

type ids struct {
	Owner, Writer, Buyer *ident
	Providers            []*ident
	Buckets              []string
}

func loadIDs(root string) (*ids, error) {
	var f idsFile
	if err := readJSON(filepath.Join(root, "ids.json"), &f); err != nil {
		return nil, err
	}
	out := &ids{Buckets: f.Buckets}
	var err error
	if out.Owner, err = newIdent("owner", f.Owner); err != nil {
		return nil, err
	}
	if out.Writer, err = newIdent("writer", f.Writer); err != nil {
		return nil, err
	}
	if out.Buyer, err = newIdent("buyer", f.Buyer); err != nil {
		return nil, err
	}
	for i, p := range f.Providers {
		id, err := newIdent(fmt.Sprintf("provider-%d", i+1), p)
		if err != nil {
			return nil, err
		}
		out.Providers = append(out.Providers, id)
	}
	if len(out.Providers) == 0 || len(out.Buckets) == 0 {
		return nil, errors.New("ids.json names no provider or no bucket")
	}
	return out, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func randomSuffix() string {
	b := make([]byte, storerec.SuffixLen)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = 'a' + b[i]%26
	}
	return string(b)
}

// feeKey is the root's txmint wallet: the key every fee coin pays.
type feeKey struct {
	Priv *ec.PrivateKey
	Addr *script.Address
	Lock *script.Script
}

func loadFeeKey(root string) (*feeKey, error) {
	var w struct {
		WIF string `json:"wif"`
	}
	if err := readJSON(filepath.Join(root, "wallet.json"), &w); err != nil {
		return nil, err
	}
	k, err := ec.PrivateKeyFromWif(w.WIF)
	if err != nil {
		return nil, err
	}
	addr, err := script.NewAddressFromPublicKey(k.PubKey(), false)
	if err != nil {
		return nil, err
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		return nil, err
	}
	return &feeKey{Priv: k, Addr: addr, Lock: lock}, nil
}

func (f *feeKey) unlocker() transaction.UnlockingScriptTemplate {
	u, err := p2pkh.Unlock(f.Priv, nil)
	if err != nil {
		panic(err)
	}
	return u
}

// funding is funding.json as `txmint corpus fund` writes it.
type funding struct {
	TxID    string `json:"txid"`
	Raw     string `json:"raw"`
	Bump    string `json:"bump"`
	Height  uint32 `json:"height"`
	Address string `json:"address"`
	Outputs []struct {
		Vout     uint32 `json:"vout"`
		Satoshis uint64 `json:"satoshis"`
	} `json:"outputs"`
}

func loadFunding(root string) (*funding, *transaction.Transaction, error) {
	var f funding
	if err := readJSON(filepath.Join(root, "funding.json"), &f); err != nil {
		return nil, nil, err
	}
	raw, err := hex.DecodeString(f.Raw)
	if err != nil {
		return nil, nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, nil, err
	}
	bump, err := hex.DecodeString(f.Bump)
	if err != nil {
		return nil, nil, err
	}
	if tx.MerklePath, err = transaction.NewMerklePathFromBinary(bump); err != nil {
		return nil, nil, err
	}
	if tx.TxID().String() != f.TxID {
		return nil, nil, errors.New("funding.json: raw does not hash to its txid")
	}
	return &f, tx, nil
}

// ---- the plan: every signed transaction and how the corpus uses it.

// planFile is chain/plan.json.
type planFile struct {
	Created  string       `json:"created"`
	FeeAddr  string       `json:"fee_address"`
	Buckets  []planBucket `json:"buckets"`
	Trees    planTrees    `json:"trees"`
	Adverts  []string     `json:"adverts"` // mined advertisement txids (standalone shape)
	Holds    []string     `json:"holds"`   // mined hold-output txids (standalone shape)
	Mix      []mixItem    `json:"mix"`
	Homes    string       `json:"homes,omitempty"` // payment funding the askers' homes
	Levels   [][]string   `json:"levels"`          // broadcast order
	Closures []closureRef `json:"closures_1m"`     // the 1 MiB roots the advertisements and mix puts name
	Params   planParams   `json:"params"`
}

type planParams struct {
	TreeSize    int    `json:"tree_size"`
	Coin        uint64 `json:"coin_sats"`
	HoldSats    uint64 `json:"hold_sats"`
	AnchorEvery int    `json:"anchor_every"`
	Providers   int    `json:"providers"`
}

type planBucket struct {
	Name      string   `json:"name"`
	Topic     string   `json:"topic"`
	ID        string   `json:"id"` // hex, hash order as BSTORE_SERVED names it
	ChainTree string   `json:"chain_tree"`
	Tokens    []string `json:"tokens"` // seq 0, 1, ...
}

type planTrees struct {
	Entry   []string `json:"entry"`   // the writer's entry trees
	Outcome []string `json:"outcome"` // the buyer's outcome trees
	Front   []string `json:"front"`   // the buyer's front-statement trees
}

// mixItem is one object of the mix shape, in order.
type mixItem struct {
	Kind   string `json:"k"` // entry, hold, ad, token
	Txid   string `json:"t"`
	Bucket int    `json:"b"`
}

type closureRef struct {
	Root        string `json:"root"`
	ContentHash string `json:"content_hash"`
	ProofRoot   string `json:"proof_root"`
	Size        uint64 `json:"size"`
	RootLen     int    `json:"root_len"`
}

// txStore is chain/txs.jsonl (every signed transaction, raw) and
// chain/proofs.jsonl (each mined one's minimal BUMP), appended to.
type txStore struct {
	dir    string
	mu     sync.Mutex
	raw    map[string][]byte
	proofs map[string][]byte
}

type txLine struct {
	Txid string `json:"txid"`
	Raw  string `json:"raw,omitempty"`
	Bump string `json:"bump,omitempty"`
}

func openStore(root string) (*txStore, error) {
	s := &txStore{dir: filepath.Join(root, "chain"), raw: map[string][]byte{}, proofs: map[string][]byte{}}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, err
	}
	if err := eachLine(filepath.Join(s.dir, "txs.jsonl"), func(l txLine) error {
		b, err := hex.DecodeString(l.Raw)
		s.raw[l.Txid] = b
		return err
	}); err != nil {
		return nil, err
	}
	if err := eachLine(filepath.Join(s.dir, "proofs.jsonl"), func(l txLine) error {
		b, err := hex.DecodeString(l.Bump)
		s.proofs[l.Txid] = b
		return err
	}); err != nil {
		return nil, err
	}
	return s, nil
}

func eachLine(path string, f func(txLine) error) error {
	fh, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var l txLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := f(l); err != nil {
			return err
		}
	}
	return sc.Err()
}

func appendLines(path string, lines []txLine) error {
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(fh, 1<<20)
	for _, l := range lines {
		b, _ := json.Marshal(l)
		w.Write(append(b, '\n'))
	}
	if err := w.Flush(); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	return fh.Close()
}

func (s *txStore) addTxs(txs []*transaction.Transaction) error {
	lines := make([]txLine, 0, len(txs))
	s.mu.Lock()
	for _, tx := range txs {
		id := tx.TxID().String()
		if _, ok := s.raw[id]; ok {
			continue
		}
		b := tx.Bytes()
		s.raw[id] = b
		lines = append(lines, txLine{Txid: id, Raw: hex.EncodeToString(b)})
	}
	s.mu.Unlock()
	return appendLines(filepath.Join(s.dir, "txs.jsonl"), lines)
}

func (s *txStore) addProofs(p map[string]*transaction.MerklePath) error {
	lines := make([]txLine, 0, len(p))
	s.mu.Lock()
	for id, mp := range p {
		b := mp.Bytes()
		s.proofs[id] = b
		lines = append(lines, txLine{Txid: id, Bump: hex.EncodeToString(b)})
	}
	s.mu.Unlock()
	return appendLines(filepath.Join(s.dir, "proofs.jsonl"), lines)
}

// tx parses a stored transaction, with its proof when it has one.
func (s *txStore) tx(id string) (*transaction.Transaction, error) {
	s.mu.Lock()
	raw, ok := s.raw[id]
	bump := s.proofs[id]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no transaction %s in the plan", id)
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, err
	}
	if bump != nil {
		if tx.MerklePath, err = transaction.NewMerklePathFromBinary(bump); err != nil {
			return nil, err
		}
	}
	return tx, nil
}

func (s *txStore) proven(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.proofs[id]
	return ok
}

// ---- small helpers

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func carrierParams(d pushdrop.Derivation) carrier.Params {
	return carrier.Params{Derivation: d, FundingTag: storerec.TagFunding,
		ValidatePayload: func([]byte) error { return nil }, ErrIdentity: storerec.ErrIdentity}
}

// serials allocates n serials from <root>/serial.json under a lock: every
// record built from one root carries a distinct serial, so no two objects
// share a txid and a set is never a replay of another.
func serials(root string, n int) (uint64, error) {
	path := filepath.Join(root, "serial.json")
	lf, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return 0, err
	}
	var st struct {
		Next uint64 `json:"next"`
	}
	if err := readJSON(path, &st); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	first := st.Next
	st.Next += uint64(n)
	return first, writeJSON(path, st)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

var bg = context.Background()
