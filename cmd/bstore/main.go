// Command bstore is bstore's command line: the publisher client (spec
// sections 9 and 14), the bucket owner's tooling (sections 3.4 to 3.6 and
// 4.4), a verified reader (section 15) and the S3 gateway (`bstore s3d`,
// section 16). docs/usage.md is the reference.
//
// A home (-home, default ~/.bstore) holds one identity: its wallet, its
// funding trees, its outbox, its races, the chains it owns and the local
// closures it built, with config.json naming the node, the settlement leg,
// the header source, the hosts and the providers. One process at a time
// uses a home.
//
// Exit codes (spec section 15.4): 0 done or verified; 1 refused (a check
// that names the owner or the writer failed, a broken chain, an invalid
// signed response, a key proven absent); 2 usage, configuration, local or
// transport error, a quorum not met, a price over the budget; 3 incomplete
// (hosts disagree or answered what does not verify, a key no host
// produces, a challenge with no valid signed answer, something still due),
// which proves nothing either way.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/bstore/internal/anchor"
	"github.com/lightwebinc/bstore/internal/publish"
	"github.com/lightwebinc/bstore/internal/reader"
	"github.com/lightwebinc/bstore/storerec"
)

// version is stamped by the build; "dev" otherwise.
var version = "dev"

// The exit codes.
const (
	exitOK         = 0
	exitRefused    = 1
	exitUsage      = 2
	exitIncomplete = 3
)

const usageText = `usage: bstore [global flags] <command> [flags] [arguments]

an identity (a home):
  init -node URL -settle LEG -headers URL -hosts URLS [flags]
                              create the home: wallet and config.json
  fund                        print the fund address and the pool
  fund -blocks N              a test network only: mine N blocks to the fund address
  fund -from H [-to H]        take coin paid to the fund address in blocks H.. into the pool
  doctor                      the home, what is owed, and whether everything answers; reads only
  terms URL                   a provider's front terms, or a host's ls_bstore terms

the bucket's owner:
  bucket new NAME [-writers K1,K2]
                              create a bucket chain: NAME_<random suffix>, its first token
  bucket add BUCKET -owner KEY -id HEX
                              name a chain another owner keeps, to read it or write to it
  bucket list                 the buckets this home knows
  anchor [-add-writer K] [-remove-writer K] [-dry-run]
                              fold every writer's unanchored entries into the next tokens

a writer:
  put KEY [FILE|-]            publish an object: closure, hold output, entry
  rm KEY                      publish a delete entry
  cp SRC DST [-to BUCKET]     a new entry naming SRC's content
  holds [-entry TXID]         watch and judge each open hold race; report
  settle [-entry TXID] [-close]
                              pay a closed race by rank; resume a settlement in flight
  reclaim [-entry TXID] [-unused]
                              spend a hold output back after its deadline
  kill -confirm KILL [-yes]   sweep every funding tree: every unanchored entry retracted

anyone:
  get KEY [OUT|-]             read an object, every check in order, from its advertisers
  ls [PREFIX] [-after KEY]    list keys, every page proven complete
  challenge KEY -provider K [-c N] [-budget SATS]
                              a paid possession challenge; its outcome recorded
  renew KEY -provider K (-until T | -days N) [-budget SATS]
                              buy a later expiry of a provider's advertisement
  fill                        replay what this home made to every host that lacks it
  sync                        send what is owed and record what the chain anchored
  due                         what is owed: exit 3 when anything is

the S3 gateway:
  s3d <command> -config FILE  (bstore s3d -h)

  version                     print the version

global flags:
  -home DIR       the identity's home (default $BSTORE_HOME, else ~/.bstore)
  -bucket B       the bucket: its identifier, or its S3 name (default config.json's)
  -hosts URLS     overlay hosts, comma separated (default config.json's)
  -v              verbose: the log on standard error

Run 'bstore <command> -h' for a command's flags. Exit codes: 0 done or
verified, 1 refused, 2 usage or transport error, 3 incomplete.
`

// exitError carries an exit code with a message.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

func usage(format string, args ...any) error {
	return &exitError{exitUsage, fmt.Sprintf(format, args...)}
}

func refused(format string, args ...any) error {
	return &exitError{exitRefused, fmt.Sprintf(format, args...)}
}

func incomplete(format string, args ...any) error {
	return &exitError{exitIncomplete, fmt.Sprintf(format, args...)}
}

func isHelp(arg string) bool {
	return arg == "help" || arg == "-h" || arg == "-help" || arg == "--help"
}

// helpOrUsage answers a flag-parsing failure: a help request exits 0 (the
// FlagSet has printed its text), anything else is a usage error.
func helpOrUsage(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return &exitError{exitOK, ""}
	}
	return &exitError{exitUsage, ""}
}

// global is the resolved home and the shared flags.
type global struct {
	home    string
	bucket  string
	hosts   string
	verbose bool
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	getenv  func(string) string
}

// say writes a note to standard error, filtered for the terminal: a note
// may quote what a host, a node or a record said.
func (g *global) say(format string, args ...any) {
	fmt.Fprintln(g.stderr, termsafe.Text(fmt.Sprintf(format, args...)))
}

// out writes a line to standard output, filtered for the terminal.
func (g *global) out(format string, args ...any) {
	fmt.Fprintln(g.stdout, termsafe.Text(fmt.Sprintf(format, args...)))
}

// logf is the log: standard error when verbose, else nothing.
func (g *global) logf(format string, args ...any) {
	if g.verbose {
		g.say(format, args...)
	}
}

type command func(ctx context.Context, g *global, args []string) error

var commands map[string]command

func init() {
	commands = map[string]command{
		"anchor":    cmdAnchor,
		"bucket":    cmdBucket,
		"challenge": cmdChallenge,
		"cp":        cmdCp,
		"doctor":    cmdDoctor,
		"due":       cmdDue,
		"fill":      cmdFill,
		"fund":      cmdFund,
		"get":       cmdGet,
		"holds":     cmdHolds,
		"init":      cmdInit,
		"kill":      cmdKill,
		"ls":        cmdLs,
		"put":       cmdPut,
		"reclaim":   cmdReclaim,
		"renew":     cmdRenew,
		"rm":        cmdRm,
		"s3d":       cmdS3D,
		"settle":    cmdSettle,
		"sync":      cmdSync,
		"terms":     cmdTerms,
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

// run is the whole command, with its streams and environment passed in so
// tests drive it in process.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("bstore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usageText) }
	var (
		home    = fs.String("home", "", "")
		bucket  = fs.String("bucket", "", "")
		hosts   = fs.String("hosts", "", "")
		verbose = fs.Bool("v", false, "")
		showVer = fs.Bool("version", false, "")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if *showVer {
		fmt.Fprintln(stdout, "bstore", version)
		return exitOK
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return exitUsage
	}
	switch {
	case isHelp(rest[0]):
		fmt.Fprint(stdout, usageText)
		return exitOK
	case rest[0] == "version":
		fmt.Fprintln(stdout, "bstore", version)
		return exitOK
	}
	cmd, ok := commands[rest[0]]
	if !ok {
		fmt.Fprintf(stderr, "bstore: unknown command %q\n", termsafe.Text(abbrev(rest[0])))
		fs.Usage()
		return exitUsage
	}
	h := *home
	if h == "" {
		h = getenv("BSTORE_HOME")
	}
	if h == "" {
		if d, err := os.UserHomeDir(); err == nil {
			h = d + "/.bstore"
		}
	}
	g := &global{home: h, bucket: *bucket, hosts: *hosts, verbose: *verbose, stdin: stdin, stdout: stdout, stderr: stderr, getenv: getenv}
	err := cmd(ctx, g, rest[1:])
	if err == nil {
		return exitOK
	}
	code, msg := classify(err)
	if msg != "" {
		fmt.Fprintln(stderr, "bstore:", termsafe.Text(msg))
	}
	return code
}

// classify maps an error to its exit code (spec section 15.4).
func classify(err error) (int, string) {
	var ee *exitError
	if errors.As(err, &ee) {
		return ee.code, ee.msg
	}
	msg := err.Error()
	switch {
	case errors.Is(err, anchor.ErrBroken), errors.Is(err, reader.ErrRefused), errors.Is(err, reader.ErrContentHash),
		errors.Is(err, storerec.ErrRollback), errors.Is(err, storerec.ErrProofRoot):
		return exitRefused, msg
	case errors.Is(err, anchor.ErrIncomplete), errors.Is(err, publish.ErrNotYet), errors.Is(err, reader.ErrUnavailable),
		errors.Is(err, reader.ErrUnheld), errors.Is(err, reader.ErrNoSource), errors.Is(err, reader.ErrNoChain):
		return exitIncomplete, msg
	case errors.Is(err, publish.ErrOverBudget):
		return exitUsage, msg + " (raise -budget to pay it)"
	case errors.Is(err, publish.ErrCannotPay):
		return exitUsage, msg + " (nothing was recorded about the provider; try again once the pool's change is mined, or fund the home)"
	}
	return exitUsage, msg
}

func abbrev(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

// flagSet is a command's FlagSet, printing its help text on -h.
func (g *global) flagSet(name, help string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(g.stderr)
	fs.BoolVar(&g.verbose, "v", g.verbose, "verbose (also a global flag)")
	fs.StringVar(&g.bucket, "bucket", g.bucket, "the bucket (also a global flag)")
	fs.Usage = func() {
		fmt.Fprintln(g.stderr, strings.TrimSpace(help))
		fmt.Fprintln(g.stderr, "\nflags:")
		fs.PrintDefaults()
	}
	return fs
}

// parse parses a command's flags, allowing them after its positional
// arguments too, and returns the positionals.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, helpOrUsage(err)
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		if args[0] == "--" {
			return append(pos, args[1:]...), nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

// listFlag collects a repeatable flag.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error {
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			*l = append(*l, x)
		}
	}
	return nil
}
