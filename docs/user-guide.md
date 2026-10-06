# bstore: user guide

bstore is object storage as a market: a writer publishes an object once,
storage providers compete to hold it for a bounty, and a reader proves
every byte it gets. This guide explains the ideas and the day-to-day use
for each role: the bucket's owner, a writer, a reader, a provider's
operator, a bridge's operator and an S3 user. [usage.md](usage.md) is the
reference for every command, [provider.md](provider.md), [s3.md](s3.md)
and [host.md](host.md) for the servers, [limits.md](limits.md) for every
bound and what each operation costs, [spec.md](spec.md) the contract, and
[../QUICKSTART.md](../QUICKSTART.md) a run of most of it on one machine.

## 1. The ideas

**An object is a CHIRP closure** (BRC-167): its content cut into 4 MiB
blobs, named by a root node that lists their hashes. The root's hash names
the object everywhere; whoever serves its bytes, a reader holds each blob to
its hash before it uses a byte.

**A bucket is a chain of mined tokens its owner signs.** Each token names
the bucket's writers and folds the writers' entries since the last token
into a key map, and commits to the root of the whole map. So a reader
proves what a key names, that a key is absent, and that a listing page is
complete, against the latest token and its own block headers. Hosts
replicate the chain; none is believed on its word.

**A write is an entry and its pieces.** The writer signs an entry (a put or
a delete of a key) and publishes it on the bucket's records topic; the
object's pieces go to the providers. Entries ride unmined carriers, each
spending one mined funding output, so a writer pays per object, not per
byte of block space. An entry is pending until the owner's next token
anchors it.

**Providers race for a bounty.** A put may carry a hold output: satoshis
the writer locks for the providers. Each provider that checks the closure
advertises it at once (a BRC-26 advertisement); the writer's own client
ranks advertisers by when it first sees them, challenges each, and pays the
first `K` that pass, by rank, with BRC-178's square-root split. `t` is how
many distinct advertisers the writer needs before it pays anyone.

**A challenge proves possession.** It samples 4 KiB segments chosen from a
block hash, the buyer's nonce and the provider's key; the provider answers
each with the segment and its path to the object's proof root, signed.
Anyone may buy one, and publish the outcome for everyone to read.

**Anyone can buy time.** An advertisement expires; a renewal, bought from
the provider by anyone (a reader above all), extends it with a successor
that spends the old one.

**Incumbent UHRP hosts join unchanged** through a bridge that speaks their
own HTTP routes: the bridge is the market front, the incumbent keeps the
bytes and is paid by its own key.

## 2. What it protects, and what it does not

It protects:

- **what a key names:** every read is proven against the owner's latest
  token, and a host that answers what does not verify is named;
- **every byte:** each blob is held to its hash before it is written or
  sent, the content hash before the last blob, and with `-proof-root` the
  proof root too; a provider serving wrong bytes is named and skipped;
- **who wrote it:** an entry is signed by its writer, and the owner anchors
  only the writers its token names;
- **who is paid:** settlement pays each advertiser's own key, derived per
  entry and rank; nobody but the advertiser can spend its share.

It does not:

- **guarantee availability.** It is a market outcome: the contract says
  how many providers advertise a root and lets a writer raise the bounty
  until enough do;
- **make keys copies.** `t` advertisers may be one disk, or one provider
  serving another's bytes (limits.md, known limit 1);
- **enforce by deposit.** A provider that fails is caught by its buyer and
  named in a published outcome; nothing is slashed;
- **hide anything.** Entries, keys and sizes are public on the records
  topic, and the bytes are served to anyone who asks. Encrypt before you
  put what must stay private.

[spec.md](spec.md) section 20 lists everything the contract does not do.

## 3. Install

```console
$ make bstore            # bin/bstore: the command line and the S3 gateway
$ make bstore-provider   # bin/bstore-provider
$ (cd host && npm ci) && make module NODE=/path/to/node24   # the host module
$ make docker-build      # the images: bstore, bstore-provider, bstore-devchain
```

The bridge is its own repository, `bstore-bridge` (`make bstore-bridge`
there).

## 4. Words

| Word | Means |
| --- | --- |
| home | a directory (`~/.bstore`) holding one identity: its wallet, funding trees, outbox, races and the chains it owns |
| bucket | `<name>_<10 letters>`: two topics (`tm_bstore_` records, `tm_bstoreblob_` pieces) and the owner's chain on them |
| owner | the identity that made the bucket and alone mints its tokens |
| writer | an identity the owner's token names; its entries are anchored |
| entry | a signed put or delete of one key |
| piece | one object of a closure (a blob, or a root or branch node), on its own carrier |
| hold output | the bounty, mined and locked to the writer's hold key, named by the entry |
| advertisement | a provider's BRC-26 statement that it holds a root, until an expiry |
| front | a provider's routes for challenges, renewals and remittances (`<url>/bstore/v1`) |
| outcome | a published record of a challenge's result |
| anchor | the owner's next token, folding writers' entries into the map |

## 5. Who runs what

| Who | Runs |
| --- | --- |
| The bucket's owner | `bstore` with its own home: `bucket new`, `anchor` |
| A writer | `bstore` with its own home (`put`, `rm`, `cp`, the race), or writes through an S3 gateway that holds its identity |
| A reader | `bstore` with any home (`get`, `ls`), or an S3 client at a gateway; a funded home for paid challenges and renewals |
| A provider's operator | `bstore-provider` ([provider.md](provider.md)) |
| A bridge's operator | `bstore-bridge` in front of an incumbent UHRP host |
| A host's operator | an overlay host with the bstore module ([host.md](host.md)) |
| A gateway's operator | `bstore s3d` ([s3.md](s3.md)) |

## 6. The bucket's owner

```console
$ bstore init -network main -node https://node.example.com \
    -settle arcade:https://arcade.example.com -headers https://headers.example.com \
    -hosts https://overlay-a.example.com,https://overlay-b.example.com
$ bstore fund                       # the fund address; pay it, then
$ bstore fund -from <height>        # take the payment into the pool
$ bstore bucket new photos -writers 02...
bucket photos_hmqzvtkwrx
owner  02...
id     5f...
served photos_hmqzvtkwrx:02...:5f...
topics tm_bstore_photos_hmqzvtkwrx tm_bstoreblob_photos_hmqzvtkwrx
```

Give the hosts' operators the `served` line and the records topic, and the
providers' operators the bucket. A host serves a chain only if its
`BSTORE_SERVED` names it, and it reads that at start, so a bucket is made
first and its first token delivered once the hosts carry it: `bucket new`
then exits 3 with the token kept, and `bstore sync` sends it later. A first
token a host refuses outright is reported as refused (exit 1): that host
will never serve the chain, and when the reason is the token itself the way
on is a fresh bucket.

**Anchor** whenever you want writers' entries to become the bucket's:

```console
$ bstore anchor -add-writer 03...   # name another writer in the next token
$ bstore anchor                     # fold every writer's unanchored entries
$ bstore anchor -dry-run            # plan only
```

Each run rebuilds the chain from the hosts, walks every writer's entries at
every host, checks each, and mints at most one token a block. Until the next
token a put is pending: readable at the writing gateway, a claim elsewhere.

## 7. A writer

```console
$ bstore put photos/cat.jpg ./cat.jpg -bounty 20000 -t 2 -k 3 -deadline 144
$ bstore rm photos/old.jpg
$ bstore cp photos/cat.jpg photos/cat-copy.jpg
```

`put` streams the file into its closure and proof root, takes a hold output
minted ahead, sends the pieces to the plane or to each provider, and
answers once the entry is taken. Everything is written to the home before
it is sent, and `bstore sync` sends what a target has not taken. A put with
no bounty (`-no-bounty`) is held only by providers that hold without one.

**The race and settlement.** Your own client judges it:

```console
$ bstore holds -watch 10m   # look, challenge each candidate, report
$ bstore settle             # pay by rank once K passed, or the window closed with t
$ bstore reclaim            # take back a bounty nobody won, after its deadline
```

Each candidate gets a free settlement challenge of `min(checkC, freeC)`
samples at its front; the first `K` that pass are paid, one transaction
spending the hold output to each advertiser's derived key, and each
provider is sent its remittance. With an arcade settlement leg and every
payout at or below `accept_threshold_sats` (25,000,000 sat by default), the
remittances go out as soon as the network has taken the settlement, before
it mines (the race shows `remitted`), and a later `settle` collects its
proof; a larger settlement is remitted once mined. `settle` resumes a
settlement in flight and never builds a second. A payee that passed through a bridge is told by
the bridge, so its remittance is listed as forwarded and is not owed; any
payee whose output the node shows spent has its share.

`bstore due` lists what is owed and exits 3 while anything is.

## 8. A reader

```console
$ bstore bucket add photos_hmqzvtkwrx -owner 02... -id 5f...
$ bstore ls photos/
$ bstore get photos/cat.jpg ./cat.jpg -proof-root
```

A read needs no coin. `get` makes every check in order and writes the file
whole or not at all; a key proven absent is `NoSuchKey` (exit 1), and one
no host produces is incomplete (exit 3).

**Challenges and renewals** need a funded home:

```console
$ bstore challenge photos/cat.jpg -provider 03... -c 113 -budget 2000
$ bstore renew photos/cat.jpg -provider 03... -days 30 -budget 5000
```

A challenge's outcome is kept in the home and published on the records
topic: a pass, a failure (an invalid signed answer, exit 1), or no answer
(exit 3). At `c = 113` one dropped blob of a 100 MiB object is caught 99
times in 100. A challenge of an incumbent behind a bridge names the
bridge's front and key: `-front URL -answerer KEY`, from its front
statement; a renewal of one names `-front`. A paid challenge pays from a
small coin split off the pool; one refused with the provider's signed note
that it did not take the payment gives the coin back at once, and one
refused without a note keeps it for an hour (limits.md, known limit 5).

## 9. An S3 user

A gateway's operator gives you an endpoint, a region, an access key and a
secret. Bucket names are the identifier with `-` for `_`:

```console
$ export AWS_ACCESS_KEY_ID=EXAMPLEACCESSKEY AWS_SECRET_ACCESS_KEY=...
$ aws --endpoint-url https://s3.example.com s3 cp ./cat.jpg s3://photos-hmqzvtkwrx/photos/cat.jpg
$ aws --endpoint-url https://s3.example.com s3 ls s3://photos-hmqzvtkwrx/photos/
$ aws --endpoint-url https://s3.example.com s3api get-object --bucket photos-hmqzvtkwrx --key photos/cat.jpg cat.jpg
```

A `200` on a put means the entry was taken, not that anyone holds the
object yet; it reads back at once at the gateway that wrote it, and
elsewhere once the owner anchors it. Every read is proven before a byte is
sent. Conditional writes answer `501`, so a client that wants a lock (the
Terraform S3 backend's `use_lockfile`) stops rather than believing it holds
one. [s3.md](s3.md) has the compatibility matrix.

## 10. A provider's operator

```console
$ bstore-provider init -data /var/lib/bstore-provider
$ bstore-provider address -config provider.json      # pay the fund address
$ bstore-provider rescan -config provider.json -from <height>
$ bstore-provider run -config provider.json
$ bstore-provider doctor -config provider.json       # reads only: does everything answer
```

The configuration names the public `https` URL the routes are reached at,
the node, the settlement leg, the header source, the hosts and the buckets
you serve ([provider.md](provider.md)). You set your terms: the price of a
challenge (`base`, `perSample`), the free challenges you owe a bounty
(`freeC`, `minBounty`, `minAge`), and the price of a renewal. Fund the
wallet: every advertisement and renewal is a mined transaction you pay
for. Bounties, paid challenges and renewals land in `payments.jsonl` and
are taken into the pool once mined. Watch `bstore_provider_damaged_total`:
a root marked damaged is no longer served, and the next challenge of it
fails. Back up the data directory: the wallet is the identity your
payments go to.

## 11. A bridge's operator

The bridge joins an incumbent UHRP host (an existing BRC-26 server) to the
market without changing it. It runs a bstore provider front with a backend
that speaks the incumbent's HTTP routes: it uploads each closure it takes
to the incumbent by upload session and pays the incumbent's commit, adopts
the incumbent's own advertisement and publishes it to the bucket, answers
challenges by reading each sample from the incumbent's URL, and sells
renewals it makes at the incumbent's `/renew`. It keeps no bytes, only
segment hashes.

```console
$ bstore-bridge init -data /var/lib/bstore-bridge
$ bstore-bridge run -config bridge.json
$ bstore-bridge payouts -config bridge.json          # settlements forwarded for the incumbent
```

A bounty pays the incumbent's key, never the bridge's: the bridge forwards
each settlement to `remittances.jsonl`, and the incumbent's operator, where
its key is, runs `bstore-bridge claim -remittances <copy> -key-file <key>`
to list what its wallet takes in. The bridge earns challenge fees and
renewal margins, and spends its own coin on commits; a daily cap
(`max_sats_per_day`) bounds that. Set renewal terms at least the
incumbent's own price. The bridge's `docs/bridge.md` is the reference.

## 12. A host's operator

Load the module (or run the `bstore-host` image), carry each bucket's
records topic, and name the chains you serve:

```sh
BSTORE_BUCKETS=photos_hmqzvtkwrx
BSTORE_SERVED=photos_hmqzvtkwrx:02...:5f...
BSTORE_STATE_DIR=/var/lib/bstore
```

A directory host holds no bytes and should not carry the blob topics.
Price the bulk questions on the module's own route (`BSTORE_LISTEN`,
`BSTORE_PRICES`, `BSTORE_PAYEE_KEY`); the payee's ledger is settled with
`bsecret payee settle`. Back up the database and the state directory
together ([host.md](host.md)).

## 13. Plane or unicast

On the plane, a writer submits each piece and record once to its facade and
every subscribed provider is delivered the pieces. In unicast the writer
sends records to each host and pieces to each provider itself (`providers`
for piece carriers, `uploads` for upload sessions). This version delivers
pieces by unicast (limits.md, Plane and unicast).

## 14. The kill switch

```console
$ bstore kill -confirm KILL
```

Sweeps every funding tree the home holds, so every unanchored entry it made
is retracted at every honest host and never anchored. It is irreversible:
the identity publishes nothing after it. Keys already anchored stay, hold
outputs are left to `reclaim`, and a copy anyone kept stays a copy.

## 15. Limits

Every bound, what each operation costs, and the known limits are in
[limits.md](limits.md). The ones to know first: an object is at most 8 TiB
(a provider takes 1 GiB by default); a bucket holds 1,048,576 keys; the
owner anchors at most one token a block; a 100 MiB put took 19 to 26 s
through the S3 gateway on a test network; a gateway's races are settled
through the command line with the gateway stopped.

## 16. If something goes wrong

| You see | Means | Do |
| --- | --- | --- |
| exit 3, `incomplete` | something is still owed or a host answered what does not verify (named) | `bstore sync`, then the command again; `bstore due` |
| `bucket new` exits 3 | the hosts do not carry the bucket yet | give the operators the `served` line; `bstore sync` once they do |
| `bucket new` exits 1, refused | a host refused the first token | read the reason (`-v`); make a fresh bucket if it is the token's |
| a put answers `503 SlowDown` | no hold output ready, or the entry not taken in time | wait and retry; a retry after the entry was made makes a second entry |
| `challenge` exits 2, cannot pay | the pool's change waits for a proof | `bstore sync` after a block, or fund the home |
| `due` lists a forwarded remittance | a bridged incumbent's share, forwarded by its bridge | nothing: it clears when the payee spends its output |
| a provider named as serving wrong bytes | its copy is damaged or it lies | challenge it; publish the outcome |

## 17. Where to go next

- [../QUICKSTART.md](../QUICKSTART.md): the whole flow on one machine.
- [usage.md](usage.md): every command, flag and setting.
- [limits.md](limits.md): bounds, costs and known limits.
- [spec.md](spec.md) and [frozen.md](frozen.md): the contract.
