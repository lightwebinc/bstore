// Command devchain is a local stand-in chain for trying bstore without a
// node, a settlement service or any coin: bcommon devkit's devchain (a
// node's JSON-RPC at /rpc, its asset API under /api/v1/ and a header source
// at /v1/root/<height> and /v1/tip, every transaction mined at once). No
// proof of work, no peers: its proofs mean nothing outside it, and nothing
// it mines is money. Coinbase: only on a regtest chain you run, for
// development and tests. It is for a laptop, never for a network.
package main

import (
	"os"

	"github.com/lightwebinc/bcommon/devkit/devchain"
)

var version = "dev"

func main() {
	os.Exit(devchain.Main(devchain.Options{App: "bstore", Version: version, Certs: true}, os.Args[1:], os.Stdout, os.Stderr))
}
