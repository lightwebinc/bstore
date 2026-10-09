package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bstore/internal/anchor"
	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

func runCLI(t *testing.T, home string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(context.Background(), append([]string{"-home", home}, args...), strings.NewReader(""), &out, &errb, func(string) string { return "" })
	return out.String(), errb.String(), code
}

// Exit codes (spec section 15.4) for what each layer returns.
func TestClassify(t *testing.T) {
	for _, c := range []struct {
		err  error
		code int
	}{
		{fmt.Errorf("x: %w", anchor.ErrBroken), exitRefused},
		{fmt.Errorf("x: %w", reader.ErrRefused), exitRefused},
		{reader.ErrContentHash, exitRefused},
		{fmt.Errorf("%w: seq", storerec.ErrRollback), exitRefused},
		{storerec.ErrProofRoot, exitRefused},
		{fmt.Errorf("x: %w", anchor.ErrIncomplete), exitIncomplete},
		{fmt.Errorf("x: %w", publish.ErrNotYet), exitIncomplete},
		{reader.ErrUnavailable, exitIncomplete},
		{reader.ErrUnheld, exitIncomplete},
		{reader.ErrNoSource, exitIncomplete},
		{reader.ErrNoChain, exitIncomplete},
		{publish.ErrOverBudget, exitUsage},
		{fmt.Errorf("%w: x", publish.ErrCannotPay), exitUsage},
		{errors.New("connection refused"), exitUsage},
		{usage("x"), exitUsage},
		{refused("NoSuchKey"), exitRefused},
		{incomplete("x"), exitIncomplete},
	} {
		if got, _ := classify(c.err); got != c.code {
			t.Errorf("%v: exit %d, want %d", c.err, got, c.code)
		}
	}
}

// Usage and configuration errors exit 2, before anything is touched.
func TestUsageErrors(t *testing.T) {
	home := t.TempDir()
	for _, args := range [][]string{
		{},
		{"no-such-command"},
		{"put"},
		{"get"},
		{"put", "bad\x01key", "-"},
		{"challenge", "k", "-provider", "zz"},
		{"challenge", "k", "-provider", "02" + strings.Repeat("11", 32), "-c", "0"},
		{"renew", "k"},
		{"renew", "k", "-provider", "zz", "-days", "1"},
		{"renew", "k", "-provider", "02" + strings.Repeat("11", 32)},
		{"renew", "k", "-provider", "02" + strings.Repeat("11", 32), "-days", "1", "-until", "5"},
		{"kill"},
		{"kill", "-confirm", "yes"},
		{"ls"},  // no home
		{"due"}, // no home
		{"bucket"},
		{"bucket", "frob"},
	} {
		if _, _, code := runCLI(t, filepath.Join(home, "none"), args...); code != exitUsage {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
	}
	if _, _, code := runCLI(t, home, "-h"); code != exitOK {
		t.Error("-h")
	}
	if out, _, code := runCLI(t, home, "version"); code != exitOK || !strings.HasPrefix(out, "bstore ") {
		t.Error("version")
	}
}

// Text from someone else (a key, a host's message, a record) reaches the
// terminal filtered: no escape sequence, no control character, no
// bidirectional override survives.
func TestTerminalInjection(t *testing.T) {
	evil := "photos/\x1b]0;owned\x07\x1b[2J‮key\r"
	_, errOut, _ := runCLI(t, t.TempDir(), evil)
	var out, errb bytes.Buffer
	g := &global{stdout: &out, stderr: &errb}
	g.out("key %s", evil)
	g.say("host said %s", evil)
	if code, msg := classify(fmt.Errorf("host: %s", evil)); code != exitUsage || msg == "" {
		t.Fatal("classify")
	}
	for name, s := range map[string]string{"unknown command": errOut, "out": out.String(), "say": errb.String()} {
		if strings.ContainsAny(s, "\x1b\x07\r‮") {
			t.Errorf("%s reached the terminal unfiltered: %q", name, s)
		}
	}
}

// payee key writes this home's wallet identity as BSTORE_PAYEE_KEY, once;
// payee settle reads the ledgers before it opens the home, and a home with
// no config.json is a usage error. The verbs themselves are bcommon's
// payeecmd, tested there.
func TestPayee(t *testing.T) {
	home := t.TempDir()
	k, err := ec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(publish.WalletDir(home), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(publish.WalletDir(home), "identity.json"), []byte(`{"wif":"`+k.Wif()+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	line := "BSTORE_PAYEE_KEY=" + hex.EncodeToString(k.Serialize()) + "\n"
	keyFile := filepath.Join(t.TempDir(), "payee.env")
	if _, errOut, code := runCLI(t, home, "payee", "key", "-out", keyFile); code != exitOK {
		t.Fatalf("key: %d %s", code, errOut)
	}
	if raw, err := os.ReadFile(keyFile); err != nil || string(raw) != line {
		t.Fatalf("key file: %q %v", raw, err)
	}
	if _, errOut, code := runCLI(t, home, "payee", "key", "-out", keyFile); code != exitUsage || !strings.Contains(errOut, "never writes over a file") {
		t.Fatalf("over a file: %d %s", code, errOut)
	}
	if out, _, code := runCLI(t, home, "payee", "key", "-out", "-"); code != exitOK || out != line {
		t.Fatalf("-out -: %d %q", code, out)
	}
	ledger := filepath.Join(t.TempDir(), "payments.jsonl")
	if err := os.WriteFile(ledger, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, errOut, code := runCLI(t, home, "payee", "settle", ledger); code != exitUsage || !strings.Contains(errOut, "bstore init creates it") {
		t.Fatalf("settle with no config.json: %d %s", code, errOut)
	}
	for _, args := range [][]string{{"payee"}, {"payee", "key"}, {"payee", "settle"}, {"payee", "settle", ledger, "-in-flight", "0"},
		{"payee", "settle", filepath.Join(home, "none.jsonl")}} {
		if _, _, code := runCLI(t, home, args...); code != exitUsage {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
	}
	if _, err := os.Stat(filepath.Join(home, payeeBookFile)); !os.IsNotExist(err) {
		t.Fatalf("a refused settle wrote the book: %v", err)
	}
}
