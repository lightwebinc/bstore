// Command devchain is a local stand-in chain for trying bstore without a
// node, a settlement service or any coin: it serves a node's JSON-RPC
// (generatetoaddress, sendrawtransaction, getinfo) at /rpc, a node's asset
// API under /api/v1/, and a header source (/v1/root/<height>, /v1/tip) that
// the bstore commands, the providers and the overlay hosts check proofs against.
// Every transaction it accepts is mined at once, in a block of its own.
//
// It is bcommon's testchain package served over HTTP. It checks what it is
// sent the way a node would (inputs exist and are unspent, coinbase is
// mature, scripts verify, nothing non-final is mined) but it has no proof
// of work and no peers: its proofs mean nothing outside it, and nothing it
// mines is money. It is for a laptop, never for a network.
//
// With -journal FILE every accepted RPC call is appended to FILE and
// replayed at the next start, so the chain survives a restart with the same
// blocks, and the hosts' and the homes' state still matches it.
//
// `devchain certs -dir DIR -names a,b` writes a throwaway certificate
// authority and a server certificate for each name, and exits: a
// provider's advertised URL must be https, so a laptop needs one. The CA's
// key is not kept. Never for a network.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/bcommon/testchain"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "certs" {
		fs := flag.NewFlagSet("certs", flag.ExitOnError)
		dir := fs.String("dir", "", "the directory to write ca.pem and <name>.pem, <name>-key.pem into")
		names := fs.String("names", "", "the server names, comma separated")
		_ = fs.Parse(os.Args[2:])
		if *dir == "" || *names == "" {
			log.Fatal("devchain certs: -dir and -names are required")
		}
		if err := writeCerts(*dir, strings.Split(*names, ",")); err != nil {
			log.Fatalf("devchain certs: %v", err)
		}
		return
	}
	listen := flag.String("listen", ":8080", "address to serve on")
	start := flag.Uint("height", 700, "the height of the tip before anything is mined")
	journal := flag.String("journal", "", "append every accepted RPC call to this file and replay it at start")
	showVer := flag.Bool("version", false, "print the version and exit")
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `usage: devchain [-listen ADDR] [-height N] [-journal FILE]

A local stand-in chain for trying bstore: a node's RPC at /rpc, its asset
API under /api/v1/, and a header source at /v1/root/<height> and /v1/tip.
Every transaction is mined at once. No proof of work, no peers: for a
laptop, never for a network.

       devchain certs -dir DIR -names NAME,...

writes a throwaway CA (ca.pem) and a server certificate and key for each
name, and exits.

`)
		flag.PrintDefaults()
	}
	flag.Parse()
	if *showVer {
		fmt.Println("devchain", version)
		return
	}
	chain := testchain.New(uint32(*start)) //nolint:gosec // a flag value
	h, err := newJournaled(chain, *journal)
	if err != nil {
		log.Fatalf("devchain: %v", err)
	}
	srv := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Printf("devchain %s: serving on %s, tip %d (a local stand-in chain; nothing here is money)", version, *listen, chain.Height())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("devchain: %v", err)
	}
}

// journaled serves the chain and appends each RPC call it accepted to a
// file, so a restart replays the same calls into the same chain.
type journaled struct {
	chain http.Handler
	mu    sync.Mutex
	f     *os.File
}

func newJournaled(chain *testchain.Chain, path string) (*journaled, error) {
	j := &journaled{chain: chain}
	if path == "" {
		return j, nil
	}
	if err := j.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	j.f = f
	return j, nil
}

// replay sends each journal line to the chain again, in order. A line the
// chain refuses now means the journal is not this chain's.
func (j *journaled) replay(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	n := 0
	for sc.Scan() {
		n++
		rec := httptest.NewRecorder()
		j.chain.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(sc.Bytes())))
		if !accepted(rec.Body.Bytes()) {
			return fmt.Errorf("journal %s line %d: the chain refuses it now: %s", path, n, rec.Body.String())
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if n > 0 {
		log.Printf("devchain: replayed %d call(s) from %s", n, path)
	}
	return nil
}

func accepted(body []byte) bool {
	var r struct {
		Error any `json:"error"`
	}
	return json.Unmarshal(body, &r) == nil && r.Error == nil
}

func (j *journaled) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if j.f == nil || r.Method != http.MethodPost || r.URL.Path != "/rpc" {
		j.chain.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var call struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(body, &call)
	// One call at a time, so the journal's order is the chain's.
	j.mu.Lock()
	defer j.mu.Unlock()
	rec := httptest.NewRecorder()
	r.Body = io.NopCloser(bytes.NewReader(body))
	j.chain.ServeHTTP(rec, r)
	if call.Method != "getinfo" && accepted(rec.Body.Bytes()) {
		var line bytes.Buffer
		if err := json.Compact(&line, body); err != nil {
			http.Error(w, "journal: "+err.Error(), http.StatusInternalServerError)
			return
		}
		line.WriteByte('\n')
		if _, err := j.f.Write(line.Bytes()); err != nil {
			http.Error(w, "journal: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := j.f.Sync(); err != nil {
			http.Error(w, "journal: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}
