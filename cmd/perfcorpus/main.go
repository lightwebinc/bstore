// Command perfcorpus builds a replayable corpus of real bstore records for
// step-load runs on a lab (lab and tests only, never for real use).
//
// The objects are what bstore's clients publish on a records topic, built
// from the record codecs in storerec and bcommon: object entries (puts of a
// few sizes, deletes), outcome records and front statements on carriers,
// mined advertisements and hold outputs, and bucket tokens that anchor
// entries. The format lets most of it be built offline:
//
//   - A carrier needs only its funding tree to be mined (the carrier itself
//     is never mineable), so after one block of funding trees every entry,
//     outcome and front statement is signed offline, as many as wanted.
//     Carriers reuse the trees' outputs: admission reads each submission's
//     own bytes and no host sees a double spend of a funding output that is
//     never broadcast.
//   - Advertisements, hold outputs and bucket tokens are admitted only when
//     mined, so those are built and broadcast once, at plan time, each spent
//     from a fan-out of one funding payment, and proven before the set is
//     written.
//
// Subcommands, in order:
//
//	perfcorpus init  -root DIR                   identities and two perf buckets' names
//	perfcorpus plan  -root DIR [counts]          sign every on-chain transaction (nothing sent)
//	perfcorpus send  -root DIR -propagation URL  broadcast the plan level by level
//	perfcorpus prove -root DIR -asset URL        wait for the blocks, record each minimal proof
//	perfcorpus build -root DIR -name SET -shapes ... -count N
//	                                             write a txmint replay set (offline carriers,
//	                                             the mined objects, the mix)
//	perfcorpus check -set DIR -asset URL [-n K]  admit the first K objects of each shape
//	                                             with storerec.Admit against the chain's headers
//
// The root holds wallet.json and funding.json as `txmint keygen` and
// `txmint corpus fund` write them: the fee key and the one mined payment
// every on-chain transaction descends from. The set directory is what
// `txmint replay` reads: <shape>.bin of length-prefixed BRC-149 submission
// records, manifest.json, and replay's cursor.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const usage = `perfcorpus: a replayable corpus of real bstore records (lab only)

  init  -root DIR                     identities and two perf bucket names
  plan  -root DIR [counts]            sign the on-chain part (nothing is sent)
  send  -root DIR -propagation URL    broadcast the plan, level by level
  prove -root DIR -asset URL          wait for the blocks, record minimal proofs (one request each)
  prove-local -root DIR [-from H]     the same, computed from each block's subtrees
  build -root DIR -name SET -shapes S -count N
  check -set DIR -asset URL [-n K]    admit the first K of each shape locally
  info  -root DIR                     what the plan holds and what is proven
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	args := os.Args[2:]
	switch os.Args[1] {
	case "init":
		err = cmdInit(args)
	case "plan":
		err = cmdPlan(ctx, args)
	case "send":
		err = cmdSend(ctx, args)
	case "prove":
		err = cmdProve(ctx, args)
	case "prove-local":
		err = cmdProveLocal(ctx, args)
	case "build":
		err = cmdBuild(ctx, args)
	case "check":
		err = cmdCheck(ctx, args)
	case "info":
		err = cmdInfo(args)
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "perfcorpus:", err)
		os.Exit(1)
	}
}
