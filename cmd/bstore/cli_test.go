package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

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
