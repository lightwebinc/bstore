# Architecture

bstore is object storage as a market. A writer publishes an object once;
every provider that subscribes to the bucket receives it, decides whether
to hold it, and competes for the writer's bounty by advertising on chain and
passing a sampled proof of possession. Readers verify everything themselves
and see S3 through a gateway.

This page names the parts, what each holds and trusts, how an object moves
through them, and where each lives in the code. The contract, every byte
format and rule a compatible implementation must meet, is
[spec.md](spec.md); this page is the map.

## The parts

```mermaid
flowchart LR
  subgraph clients["Clients (each a home: wallet + config.json)"]
    O["owner<br/>bstore bucket / anchor"]
    W["writer<br/>bstore put / holds / settle"]
    R["reader<br/>bstore get / ls / challenge / renew"]
    G["S3 gateway<br/>bstore s3d"]
  end
  S3["S3 client"] -->|"SigV4"| G
  subgraph overlay["Overlay hosts (bstore module)"]
    H1["host"]
    H2["host"]
  end
  subgraph providers["Providers"]
    P1["bstore-provider<br/>native store"]
    P2["bstore-bridge<br/>front for an incumbent UHRP host"]
  end
  O -->|"bucket tokens"| H1
  O --> H2
  W -->|"entries"| H1
  W -->|"pieces / upload sessions"| P1
  W --> P2
  G -->|"entries"| H2
  G -->|"pieces"| P1
  P1 -->|"advertisements"| H1
  P2 -->|"advertisements"| H2
  R -->|"lookups"| H1
  R -->|"blobs, challenges, renewals"| P1
  C[("BSV blockchain<br/>chain view, settlement leg, header source")]
  O -.-> C
  W -.-> C
  P1 -.-> C
  R -.-> C
  %% The last node is an invisible spacer in the rightmost rank: the rendered
  %% viewer places its zoom and pan controls over the bottom-right corner.
  P1 ~~~ pad
  pad["&nbsp;<br/>&nbsp;<br/>&nbsp;<br/>&nbsp;<br/>&nbsp;"]:::pad
  classDef pad fill:none,stroke:none,color:transparent
```

| Part | Binary or artifact | Holds | Trusts |
| --- | --- | --- | --- |
| A home (owner, writer, reader) | `bstore` | one identity: its wallet, funding trees, outbox, hold races, the chains it owns, the closures it built | its own header source; everything else is checked |
| The S3 gateway | `bstore s3d` | one home per writer identity, a local store of its own writes, pending entries | as a home; S3 clients by their access keys |
| A provider | `bstore-provider` | the closures it chose to hold, its advertisements, its payment ledger | its header source and chain view; a writer's pieces only once checked |
| A bridge | `bstore-bridge` (its own repository) | segment hashes only; the incumbent keeps the bytes | the incumbent's answers, each checked against the closure |
| A host | an overlay host loading `host/bundle/bstore-module.js` (or the `bstore-host` image) | the bucket's records topic: tokens, entries, advertisements, outcomes, front statements | its header source |
| The chain | a chain view (WhatsOnChain by default, or a node's asset API), a settlement leg (GorillaPool's arcade by default), a header source (WhatsOnChain by default) | mined funding trees, hold outputs, bucket tokens, advertisements, settlements | |

Nobody is believed on their word. A host is a replica of records anyone
can check. A provider's bytes are held to their hashes. A reader proves a
key against the owner's latest token and its own headers.

## What moves where

Two topics carry a bucket. The records topic (`tm_bstore_<bucket>`) carries
the small, self-verifying records: tokens, entries, advertisements, outcomes,
front statements. The blob topic (`tm_bstoreblob_<bucket>`) carries the
bytes, as pieces, and only providers subscribe to it. A directory host is
never delivered, or billed for, an object's bytes.

```mermaid
sequenceDiagram
  participant C as S3 client
  participant G as gateway (bstore s3d)
  participant F as facade (plane)
  participant P as provider
  participant H as host
  participant B as chain
  C->>G: PutObject (SigV4, body streams)
  G->>G: closure + proof root while the body streams
  G-->>C: 200 once the put is committed to the outbox
  G->>F: pieces (blob topic), one publish
  F->>P: every subscribed provider receives the pieces
  G->>F: entry (records topic)
  F->>H: hosts take the entry
  P->>P: check the closure, recompute the proof root, hold
  P->>B: advertisement (mined)
  P->>H: advertisement with its proof
  G->>P: settlement challenge (c samples)
  P-->>G: signed response
  G->>B: settle the hold output by rank
```

## An object's life

1. **Bucket.** The owner's `bstore bucket new` mines a chain tree and the
   first token, naming the writers. A host serves the chain once its
   operator adds the `served` line.
2. **Put.** The writer's client streams the content into a CHIRP closure
   (4 MiB blobs under a root node) and a proof root over 4 KiB segments.
   Each piece rides an unmined carrier spending one mined funding output;
   the signed entry rides another. Pieces go to the providers (by the
   plane, as piece carriers to each provider's `/submit`, or by BRC-167
   upload session); the entry goes to the hosts. With a bounty, a hold
   output minted ahead is named in the entry.
   The S3 gateway answers once the put is committed to its outbox: the hold
   taken, a funding output reserved and recorded used for each piece, a
   record to mint each piece from written, the entry minted and written,
   all synced. It mints and sends the pieces from those records in the
   background and the entry after them ([s3.md](s3.md)). A restart mints
   what is left on the outputs reserved, never fresh ones. `bstore put`
   waits for the first send.
3. **Hold.** Each provider checks the closure and the proof root, decides
   by its policy, and advertises (a mined BRC-26 advertisement).
4. **Race.** The writer's client ranks advertisers by first sight,
   challenges each at its front, and once `K` pass (or the window closes
   with `t`), settles: one transaction spends the hold output to each
   winner's derived key by BRC-178's square-root split, and each winner is
   sent its remittance.
5. **Anchor.** The owner's `bstore anchor` folds the writers' entries into
   the next token, which commits to the whole key map.
6. **Read.** A reader proves the key against the latest token, fetches
   every blob from the root's advertisers, holds each to its hash, and
   writes the file whole or not at all.
7. **Time and proof.** Anyone buys a challenge (samples proven into the
   proof root) or a renewal (a successor advertisement spending the old).

```mermaid
flowchart LR
  E["entry published<br/>(hold terms name a bounty)"] --> S["providers advertise<br/>first sight ranks them"]
  S --> Q["settlement challenge<br/>min(checkC, freeC) samples, free"]
  Q -->|pass| K{"K passed, or the<br/>window closed with t?"}
  Q -->|fail| N["next candidate"] --> Q
  K -->|yes| T["settle: one transaction,<br/>sqrt-weighted by rank"]
  K -->|deadline, under t| L["bounty lapses:<br/>the writer reclaims"]
```

## Money

Every mined transaction is paid from the pool of the identity that makes
it. A pool is funded by paying the identity's fund address from the user's
own wallet and importing the payment with `fund -txid`
([examples.md](examples.md)). Carriers are never mined: a put costs its
funding outputs, the hold output's transaction and the bounty, not block
space for its bytes. [limits.md](limits.md) gives measured sizes and fees.

| Who pays | For | To |
| --- | --- | --- |
| The owner | bucket tokens | miners |
| The writer | funding trees, hold outputs, settlements, the bounty | miners; the first `k` advertisers to pass |
| The provider | advertisements and renewals | miners |
| Anyone | a challenge or a renewal | the provider, by a 402 |
| Every subscriber | delivered bytes | the plane operator |

The `-blocks` and `-from` funding flags, the `devchain` binary and
`internal/testwriter` are coinbase: only on a regtest chain you run
(development and tests).

## Three commands

*Create a bucket, fund the owner, and name a gateway's identity as a writer.*

```bash
bstore init -network main -hosts https://overlay.example.com
bstore fund -txid <txid>
bstore bucket new photos -writers 03...
```

*Put an object through the S3 gateway with the AWS CLI; the answer comes
once the gateway holds the put durably.*

```bash
aws --endpoint-url https://s3.example.com s3 cp ./photo.jpg s3://photos-hmqzvtkwrx/albums/photo.jpg
```

*Buy a proof that a provider still holds an object: 30 sampled segments,
each proven into the entry's proof root.*

```bash
bstore challenge albums/photo.jpg -provider 02... -c 30
```

## The code

| Path | What |
| --- | --- |
| `cmd/bstore` | the command line: homes, owner tooling, writer, reader, and `bstore s3d` |
| `cmd/bstore-provider` | the provider's command |
| `cmd/devchain` | the local stand-in chain of the development sandbox (bcommon `testchain` over HTTP) |
| `cmd/vectors` | writes and checks the golden vectors under `testdata/vectors` |
| `storerec` | the byte-level contract: records, entries, pieces, outcomes, front statements, challenges, the key map, settlement, advertisements, a reader's checks |
| `provider` | the provider: receiving, checking, the hold policy, advertising, challenges, renewals, settlement, serving |
| `backend` | the hold-backend interface (frozen) and the native content-addressed store |
| `chaincfg` | the chain view, header source, settlement leg and fee policy every configuration names (a provider's from `provider.Config.Spec`), and importing a payment (BEEF or txid) to a fund address into a pool; exported for a module that embeds a provider |
| `auth104` | the server side of BRC-103/104 mutual authentication, with its hardening |
| `internal/publish` | the publisher: staging, carriers, delivery, the outbox, the race, settlement, reclaim, paid calls |
| `internal/anchor` | the owner's tooling: chain creation and anchoring |
| `internal/reader` | the verified reader |
| `internal/s3d` | the S3 gateway |
| `internal/efleg` | attaching inputs' source transactions for Extended Format settlement legs |
| `internal/acceptcfg` | the payment acceptance settings shared by every configuration |
| `host/` | the TypeScript host module: topic managers, `ls_bstore`, the terms route, retention |
| `module/` | the Go host module, the same contract for Go overlay hosts |
| `deploy/quickstart` | the development sandbox's Compose file |

bstore builds on [bcommon](https://github.com/lightwebinc/bcommon) (wallet,
carriers, minting, fee policy, headers, the chain views, payment acceptance) and
[go-sdk](https://github.com/bsv-blockchain/go-sdk). The Go codec in
`storerec` and the TypeScript twin in `host/src` read the same golden
vectors ([codec.md](codec.md)).

## Where to go next

| To | Read |
| --- | --- |
| run a provider and sell storage | [provider.md](provider.md) |
| put an S3 front on a bucket | [s3.md](s3.md) |
| carry a bucket's records on an overlay host | [host.md](host.md) |
| look up a flag, a key or an image | [configuration.md](configuration.md) |
| know what an object may be and what a put costs | [limits.md](limits.md) |
| read the contract | [spec.md](spec.md), [frozen.md](frozen.md) |
