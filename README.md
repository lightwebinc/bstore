# bstore

[![CI](https://github.com/lightwebinc/bstore/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/bstore/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/bstore)](https://github.com/lightwebinc/bstore/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/bstore.svg)](https://pkg.go.dev/github.com/lightwebinc/bstore)
[![Go version](https://img.shields.io/github/go-mod/go-version/lightwebinc/bstore)](go.mod)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> [!WARNING]
> **Experimental software.** bstore is part of [bstack](https://github.com/lightwebinc/bstack),
> the applications and patterns built on [BSV Layered Multicast](https://github.com/lightwebinc/bsv-multicast).
> It is published to be built on and improved. Interfaces, formats and behavior may change
> rapidly between releases; pin an exact version.

Object storage as a market on Bitcoin. A writer publishes an object once;
storage providers compete to hold it for a bounty; a reader proves every
byte it gets. Readers and writers may speak S3.

> The TypeScript host module under `host/` is frozen: bug fixes only. New behavior goes to its Go counterpart under `module/`, for Go overlay hosts ([docs/host.md](docs/host.md#the-go-module)). The `bstore-host` image runs the TypeScript module.

## How it works

- **Publish once, many providers.** An object is a CHIRP (BRC-167) closure
  sent to every provider of its bucket, with a signed entry on the
  bucket's records topic. Providers advertise what they hold with ordinary
  UHRP (BRC-26) advertisements.
- **A bounty races the write.** A writer may commit a hold bounty that its
  own client pays, by rank, to the first providers to advertise and pass a
  challenge, with BRC-178's square-root split.
- **Anyone can buy time or proof.** A renewal extends a provider's holding;
  a challenge samples 4 KiB segments and proves each into the object's
  proof root.
- **The bucket proves its own listing.** The owner's chain of mined tokens
  commits to the whole key map, so a reader proves what a key names, that a
  key is absent and that a listing page is complete.
- **Existing UHRP hosts join through a bridge**
  ([bstore-bridge](https://github.com/lightwebinc/bstore-bridge)) that
  speaks their HTTP routes. Current uhrp-server-basic releases need a
  one-line fix to commit CHIRP roots; the bridge checks before it pays.
- **Honest about limits.** Availability is a market outcome, keys are not
  copies, and enforcement is reputation in this version.

[docs/architecture.md](docs/architecture.md) shows the pieces and how they
fit.

## Install

```console
$ go install github.com/lightwebinc/bstore/cmd/bstore@latest            # the command line and the S3 gateway
$ go install github.com/lightwebinc/bstore/cmd/bstore-provider@latest   # a storage provider
```

Release tarballs for Linux and macOS (amd64, arm64), each with its
`SHA256SUMS` entry, are on the
[releases page](https://github.com/lightwebinc/bstore/releases). The
container images are built from the [Dockerfile](Dockerfile)
(`ghcr.io/lightwebinc/bstore`, `bstore-provider`, `bstore-devchain`); see
[docs/configuration.md](docs/configuration.md#container-images).

## On mainnet

No node is needed. By default the chain is read from WhatsOnChain (every
header held to its proof of work, every proof to those headers) and
transactions are broadcast through GorillaPool's public arcade; your own
node (`-chain asset:URL`) is the better chain view, and every backend is a
setting ([docs/configuration.md](docs/configuration.md#networks-and-endpoints)).
You need only the overlay hosts that carry your bucket.

```console
$ bstore init -network main \
    -hosts https://overlay-a.example.com,https://overlay-b.example.com
identity 02...
$ bstore fund
identity     02...
fund address 1...
pool         0 output(s), 0 satoshis
```

Send a small amount from your own wallet to the fund address (100,000
satoshis, 0.001 BSV, is ample to try every role but a bounty) and import
the payment: as the BEEF your wallet hands over (no lookup, and it need
not have mined), or by its txid once it has a block:

```console
$ bstore fund -beef payment.beef
$ bstore fund -txid <txid of your payment>
<txid> mined at height <h>: 1 output(s) to the fund address, 1 imported (100000 satoshis), 0 already held, 0 spent
pool 1 output(s), 100000 satoshis
$ bstore bucket new photos          # prints the "served" line your hosts add
$ bstore put photos/cat.jpg ./cat.jpg
```

Reading needs no coin: name the bucket (its identifier, owner key and id,
from its owner) and read, every byte proven:

```console
$ bstore bucket add photos_hmqzvtkwrx -owner 02... -id 5f...
$ bstore -bucket photos_hmqzvtkwrx get photos/cat.jpg ./cat.jpg -proof-root
```

Testnet is the same with `-network test`.
[docs/examples.md](docs/examples.md) walks each role on a real network,
with what each step costs.

## Documentation

| If you want to | Read |
| --- | --- |
| Do a task on mainnet or testnet: fund, make a bucket, put, read, challenge, renew | [docs/examples.md](docs/examples.md) |
| Learn the ideas and the day-to-day use, by role | [docs/user-guide.md](docs/user-guide.md) |
| See the components, the data flow and the code layout | [docs/architecture.md](docs/architecture.md) |
| Look up a flag, an environment variable, a config key or an image | [docs/configuration.md](docs/configuration.md) |
| Look up a command and what it does, step by step | [docs/usage.md](docs/usage.md) |
| Know the bounds, what each operation costs, and the known limits | [docs/limits.md](docs/limits.md) |
| Run a storage provider | [docs/provider.md](docs/provider.md) |
| Run the S3 gateway, and what S3 clients can do through it | [docs/s3.md](docs/s3.md) |
| Run an overlay host that carries buckets | [docs/host.md](docs/host.md) |
| Try everything on one machine: a local development sandbox (no coin needed, a private regtest chain) | [QUICKSTART.md](QUICKSTART.md) |
| Implement a compatible writer, provider, host or reader | [docs/spec.md](docs/spec.md) (the specification) |
| Know what cannot change once published | [docs/frozen.md](docs/frozen.md) |
| Use or check the codec and its golden vectors | [docs/codec.md](docs/codec.md) |

## Building

```console
$ cd host && npm ci && cd ..
$ make verify NODE=/path/to/node24
$ make bstore bstore-provider       # bin/: the command line (and bstore s3d), the provider
$ make module NODE=/path/to/node24  # host/bundle/bstore-module.js
$ make docker-build                 # the images
```

Go 1.27.2 and Node 24. bstore pins go-sdk v1.7.1 and bcommon v0.33.4

## License

Apache License 2.0. See [LICENSE](LICENSE), and [NOTICE](NOTICE) for the
third-party software and standards.
