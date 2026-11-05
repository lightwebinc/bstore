# The command line

`bstore` is the publisher client, the bucket owner's tooling and a verified
reader ([spec.md](spec.md) sections 3, 9, 14 and 15). `bstore s3d` is the
S3 gateway ([s3.md](s3.md)), and `bstore cp` with an `s3://` side is an S3
client for it. This page covers everything but the gateway.

*Build the command and list every verb.*

```bash
go build -o bstore ./cmd/bstore
./bstore -h
```

| Verb | Does |
| --- | --- |
| `init`, `fund`, `doctor`, `terms` | make a home, fund it, check that everything answers, read a provider's or a host's terms |
| `bucket new`, `bucket add`, `anchor` | the owner: make a bucket, name one you do not own, fold entries into the next token |
| `put`, `rm`, `cp` | a writer: publish, delete, copy |
| `holds`, `settle`, `reclaim` | the hold race: watch it, pay the winners, take back a bounty nobody won |
| `get`, `ls` | a reader: a verified file, a proven listing |
| `challenge`, `renew` | buy proof of possession, buy more time |
| `sync`, `fill`, `due` | send what is owed, replay what a host lacks, list what is still owed |
| `payee key`, `payee settle` | a host's payee: its key line, settling its ledger |
| `kill` | the kill switch |
| `cp`, `ls`, `rm`, `bench` with `s3://` | an S3 client with timings |

## A home

A home is one identity: its wallet, its funding trees, its outbox, its
hold races, the bucket chains it owns, the closures it built and
`config.json`. It is `-home DIR`, else `$BSTORE_HOME`, else `~/.bstore`.

One process at a time uses a home (an advisory lock on its directory): two
processes writing one identity's records would each overwrite what the
other spent. Back the home up: the wallet is the identity.

No node is needed: on mainnet and testnet the home reads the chain from
WhatsOnChain and broadcasts through GorillaPool's public arcade unless
`config.json` names other backends. A node of your own (`-chain
asset:URL`) is the better chain view; see
[configuration.md](configuration.md#networks-and-endpoints).

*Make a home naming an overlay host and a plane facade, print the fund address, import a payment.*

```bash
bstore init -network main -hosts https://overlay.example.com -facade https://plane.example.com
bstore fund
bstore fund -beef payment.beef
bstore fund -txid <txid>
```

```text
identity 02...
identity     02...
fund address 1...
pool         0 output(s), 0 satoshis
```

### Funding

Send a small amount from your own wallet to the fund address and import
the payment, either way:

| Command | Takes |
| --- | --- |
| `fund -beef FILE` (or `-` for standard input) | the payment as the BEEF your wallet handed over, with no lookup. A mined payment's proof is checked against the header source. One not mined yet is taken when every transaction it spends is proven; it is sent to the settlement leg in case the wallet did not broadcast it, and its outputs wait in the pool until it mines. `-refuse-unmined` (or `refuse_unmined` in `config.json`) takes mined payments only |
| `fund -txid TXID` | a mined payment by its txid, fetched with its proof from the chain view and the proof checked against the header source. One not mined yet exits 3: import it once it has a block |

Every output that pays the fund address, that the pool does not hold and
that the chain view does not show spent goes into the pool. Importing the
same payment again adds nothing. A payment that pays the fund address
nothing is refused (exit 1). On testnet use `-network test`.

`fund -blocks N` and `fund -from H [-to H]` are **coinbase: only on a
regtest chain you run (development and tests)**, with `chain asset:URL`.
`-blocks` mines `N` blocks to the fund address through the node's RPC
(`settle rpc:URL`); `-from` takes the coinbase outputs paying the fund
address in blocks `H` to the tip into the pool, and finds no ordinary
payment.

### Checks

`doctor` reports the home, the fee policy in force and whether everything
it names answers (the chain view, the header source, the settlement leg,
the hosts and the providers), and changes nothing. `terms URL` prints a
provider's terms (its front, ending `/bstore/v1`) or a host's `ls_bstore`
terms.

### config.json

`init` writes it; an unknown key is refused. Every key, its default and an
example are in [configuration.md](configuration.md#configjson).

## The owner

*Make a bucket naming two writers.*

```bash
bstore bucket new photos -writers 03...,02...
```

```text
bucket photos_hmqzvtkwrx
owner  02...
id     5f...
served photos_hmqzvtkwrx:02...:5f...
topics tm_bstore_photos_hmqzvtkwrx tm_bstoreblob_photos_hmqzvtkwrx
```

`bucket new` draws the identifier's ten random letters, mines a chain tree
of one output under `derive(owner, "chain")` and the first token spending
it, naming the writers, and adds the chain to `config.json`. Each is
recorded before it is sent; a run that stops part way is finished by
running `bucket new` again with the identifier it printed.

A mined first token a host has not taken yet (it does not carry the
bucket's topic yet) exits 3 and is delivered by `sync`. One a host refuses
exits 1 and is never sent there again, since a refusal of the same bytes
is final. The `served` line is what a host's operator adds to its served
chains. `bucket add BUCKET -owner KEY -id HEX` names a chain someone else
owns, to read it or write to it.

### Anchoring

`anchor` folds the writers' entries into the next tokens, exactly as
frozen (spec sections 3.4 and 4.4):

1. **The chain is rebuilt from the hosts every run.** The highest token
   any host answers that verifies against this home's headers and that
   the chain view shows unspent, walked back by `prev` through
   `bucket-seq` to seq 0, every link and the first token's bucket id
   checked. Its fold must be consistent; a broken chain is refused (exit
   1) and the way on is a new chain. The set of commitments the chain has
   named comes from those tokens, never from a local record.
2. **Each writer's entries are walked** (the owner's and those of the
   writers the next token names) with `unanchored` at every host, every
   page from the start. Every entry an answer returns must verify (the
   carrier's rules and its funding tree's proof) and carry the writer,
   bucket and id asked for; a host whose answer does not, or whose pages
   are out of order, is named and the run is incomplete (exit 3). A host
   that withholds an entry delays it; another host's answer still finds
   it.
3. **Each entry's funding outpoint must be unspent** by positive evidence
   from the chain view: a swept entry is skipped for good, and one the
   chain view cannot vouch for waits for the next run (exit 3).
4. **The batch is folded against the current map** before anything is
   minted: one key's entries ordered by `created`, then commitment, the
   last deciding and the rest consumed; a delete of a key the map does
   not hold, and a put of the value a key already names, go to
   `consumed`; an entry any token named is skipped whatever a host says.
5. **The changes are split over tokens** at the record's 65,536 bytes,
   1024 changes and 1024 consumed; one key's change is carried by one
   token, its superseded entries spilling into tokens of `consumed` alone
   before it. Each token is recorded before it is sent, mined, then
   published with its parent to the facade and every host. A token
   another instance minted first at the same seq is detected and dropped,
   and the next run builds on the winner.

`-add-writer` and `-remove-writer` change the writers the next token
names; a writer is removed only when the map names none of its entries.
`-dry-run` plans and mints nothing.

## Writing

*Put with a bounty, delete, copy within the bucket, copy to another bucket.*

```bash
bstore put photos/cat.jpg ./cat.jpg -bounty 20000 -t 2 -k 3 -deadline 144
bstore rm photos/old.jpg
bstore cp photos/cat.jpg photos/cat-copy.jpg
bstore cp photos/cat.jpg photos/cat.jpg -to example_backup_qwhzkmvtrx
```

```text
entry  9c...
key    photos/cat.jpg
root   41...
size   1048576
hold   7a....0, 20000 satoshis, t 2, K 3, deadline 851000
```

`put` builds the closure and the proof root while the content streams,
then takes a mined hold output of the bounty (minted ahead, `hold_tree` to
a transaction, marked taken before the entry is signed; with none ready it
waits up to 30 seconds, then exits 3 while more mine), publishes the
closure and the entry, and answers once the facade or a quorum of the
hosts took the entry.

The closure goes to the plane on the blob topic, or, with no facade, as
piece carriers to each provider's submit route, or by BRC-167 upload
session to each provider of `uploads` (session opened, each object staged
once, root committed; the commit answers `202` until the entry reaches the
provider).

Save before send: funding outputs are recorded used before any carrier is
signed, every carrier and every entry is written to the home before it is
sent, and what a target did not take is sent again, the same bytes, by
`sync`. A put is pending until the owner's next token anchors it.

`rm` publishes a delete entry. `cp` publishes a put naming the source's
anchored content; to another bucket it publishes the closure again on
that bucket's blob topic, from the home's store or from the source's
advertisers.

## The hold race and settlement

The writer's own client judges the race (spec section 9.4).

*Watch the race for ten minutes, then pay the winners.*

```bash
bstore holds -watch 10m
bstore settle
```

```text
race 9c...  key photos/cat.jpg  open
  hold 7a....0  20000 satoshis  t 2  K 3  checkC 30  deadline 851000
   1. 03...  first seen 10:02:11.412  passed  last: pass c=30
   2. 02...  first seen 10:02:11.412  passed  last: pass c=30
   3. 02...  first seen 10:04:52.090  seen
```

### First sight

Each look asks every host for the root's advertisements. One that is
mined in this home's headers, names the root, is not by the anyone key,
expires no earlier than the entry's retention, has the root node's length
as `fileSize` and a CHIRP URL is eligible, and its advertiser's first
sight is the time of that look.

Advertisements seen in one look tie, and the lower tie key ranks first
(`SHA-256` of the advertisement's outpoint with the entry's commitment,
spec section 9.4), which no provider can grind before the entry exists. A
renewal keeps the original's sight. No provider can make this client see
an advertisement before it exists and is mined, and an advertisement says
nothing about the bytes: possession is what the challenge proves.

### The settlement challenge

In rank order, as many candidates as passes are still missing are
challenged at once, each at its front (derived from its advertisement's
URL; a front statement naming it, four at most, only once its own front
has not answered) with `min(checkC, freeC)` samples. `freeC` comes from
its terms (30, the floor every provider's `freeC` meets, when its terms
cannot be read). The request is seeded two blocks below the tip with a
fresh nonce, and the signed response is checked sample-major,
by-reference samples fetched from the advertised URL and rechecked from
the whole blob.

### Draws

A candidate gets two requests (draws) over the whole race, every front
counted. A draw is recorded before it is sent, and one not answered (a
timeout, a server error, a price) is sent again unchanged, the same nonce,
until its seed leaves the answerer's window or three sends have failed;
only then is it spent. So a provider cannot answer the draws it can serve
and refuse the rest: refusing only delays it.

A refusal of the request itself, a signed answer, or a front with no
challenge route (`404`, `410`) spends the draw at once; after a front with
no route the next draw goes to the next front in the same look. One
exception (row C31): a `404` with code `ERR_ROUTE_NOT_FOUND` from the
advertiser's own derived front is not a draw (an incumbent behind a bridge
has no bstore route there), so that front is dropped and both draws go to
fronts that state they answer for the advertiser.

### The draw log

What became of each draw (its answer, a draw spent and why, one not
counted, the candidate failing) is kept in the race apart from the
progress notes ("waiting", "priced", "not asked"), which a step writes
every few seconds. `bstore holds` prints each candidate's draw log under
it, and `bstore doctor` names, for every race not yet settled, each
candidate's last draw outcome and its reason.

### Whose failure

A failure is the answerer's (spec section 7.4): at the advertiser's own
front it fails the candidate; at a front that is not the advertiser it is
the front's, that front is not asked again, and the candidate is not
failed. An unretrievable sample, which is about the advertiser's URL,
fails it whoever answered.

A provider whose `minAge` the hold output is not yet deep enough for waits
for a later look; one whose `minBounty` the bounty is below is priced, and
is paid only up to `-budget`; neither is passed or failed. A failure at a
signed answer is published as an outcome record. One candidate takes at
most four minutes of a look.

### Close

The race closes when `K` have passed, when the window
(`race_window_seconds`) ends with at least `t` passed, or with `settle
-close`. With fewer than `t` when the window ends nothing is paid.

### Settlement

`settle` pays by rank (spec section 9.5). It first checks each payee's
advertisement unspent by positive evidence from the chain view, a renewal
successor counting as unspent; a canceled one is dropped, the ranks below
move up, the next passed candidate fills the last place, and `t` is
counted again.

One transaction spends the hold output and pays rank `i` the square-root
share `floor(R * sqrt(k - i + 1) / S)`, the remainder to rank 1, as a
P2PKH output to the key BRC-29 derives for its advertiser with prefix the
entry's commitment and suffix the two-byte rank. The payouts sum to
exactly the hold output and the fee comes from another coin. A payee's
advertisement is followed through sixteen renewals at most, one successor
a spend; a longer chain is taken as not live.

The settlement is signed and recorded before it reaches the settlement
leg: a `settle` killed at any point is finished by running it again, which
sends the same bytes and never builds a second settlement for the hold
output. Once it mines, each payee's remittance goes, unicast, to the
`settle` route of the advertiser's own front, derived from its
advertisement, never to a front that answered for it; `settle` again
resends any a payee has not taken.

A payee whose output the chain view shows spent has its share and is owed
nothing more, remitted or not. A payee that passed at a front other than
its own (a bridge in front of an incumbent UHRP host) has no settle route
of its own: the front forwards the settlement to it (C28), so its
remittance is reported as forwarded and not counted as owed. A settlement
that can never mine (a fee coin spent elsewhere) is set aside and the next
`settle` builds another; a settlement a miner re-encoded is taken as the
one sent. A settlement is made only while the tip is below the deadline.

### Reclaim

`reclaim` spends a hold output nobody won back to the fund key once the
tip reaches its deadline: a race closed with fewer than `t`, or one whose
deadline came first. A hold output with a settlement recorded (and not
set aside as never able to mine) is never reclaimed, and a reclaimed one
is never settled. A put that stops before its entry is made gives its hold
output back to the next put; `reclaim -unused` spends back every mined
hold output no entry names, each reclaim recorded whole before it is sent.

### The gateway's races

The gateway's puts with a bounty are judged, settled and reclaimed the
same way, by the gateway itself in the background ([s3.md](s3.md),
`race`). With `race.off`, or to look at a race, run `bstore holds` and
`bstore settle` with `-home` at the identity's directory
(`<data>/identities/<name>`, with a `config.json` beside its wallet) while
the gateway is stopped: a running gateway holds the directory and a
command there is refused.

## Reading

*Read a file with every check including the proof root; list a prefix.*

```bash
bstore get photos/cat.jpg ./cat.jpg -proof-root
bstore ls photos/
```

```text
1048576	<created>	photos/cat.jpg
```

`get` makes every check of spec section 15.2 in order: the chain's latest
token from the hosts, walked back to seq 0 and checked unspent; the map's
proof of the key; the entry, its writer and its kind; the root and every
branch, held to their hashes and to profile 1's construction; every blob
from the root's advertisers, each held to its hash before a byte of it is
written, the next advertiser asked when one fails and one serving wrong
bytes named; the content hash before the last blob is released; with
`-proof-root` the proof root too.

A file is written whole or not at all. A key the map proves absent is
`NoSuchKey` (exit 1); a key whose entry no host produces is incomplete
(exit 3). `ls` lists keys under a prefix, every page proven complete.

## Challenges

*Buy a 113-sample possession challenge of one provider, paying at most 2,000 satoshis.*

```bash
bstore challenge photos/cat.jpg -provider 03... -c 113 -budget 2000
```

```text
challenge of 03... at https://provider.example.com/store/bstore/v1: pass (c = 113, paid 123)
```

A possession challenge (spec section 7) of one provider's advertisement:
`c` samples chosen from the tip block's hash, a fresh nonce (`-nonce`
fixes one) and the provider's key, paid through the provider's 402 up to
`-budget` (a price over it is refused, exit 2). The outcome is appended to
`outcomes.jsonl` in the home and published as an outcome record on the
bucket's records topic unless `-no-publish`.

| Exit | Means |
| --- | --- |
| 0 | a pass |
| 1 | an invalid signed response, which anyone holding it can check again |
| 2 | the home's pool cannot pay the price now (its change waits for a proof); nothing is recorded about the provider |
| 3 | no valid signed answer (a refusal, a timeout, a provider that found its own copy damaged), or a sample the advertised URL did not serve |

One dropped blob of a 100 MiB object is caught by `c = 113` 99 times in
100. A provider answering for another advertiser (a bridge in front of an
incumbent UHRP host) is asked with `-front` and `-answerer`: the front's
URL and key, from its front statement.

## Renewals

*Buy thirty more days from one provider, paying at most 5,000 satoshis.*

```bash
bstore renew photos/cat.jpg -provider 03... -days 30 -budget 5000
```

```text
renewal of 03... at https://provider.example.com/store/bstore/v1: 200 (paid 500)
  advertisement 7a..., expiry 1790000000, until 1792592000
  successor 4c....0 spends 7a..., expiry 1792592000: mined, its proof verified in this home's headers
```

Anyone may buy more time from an advertiser (spec section 10.3), a reader
above all: the provider's live advertisement of the object's root is
found at the hosts, and the renewal is posted to its front (`-front` for a
front that answers for it), paid through the provider's 402 up to
`-budget`. `-until` is the new expiry in Unix seconds, `-days` moves the
current one later.

The answer is checked: an output advertising the same root by the same
provider through exactly the expiry asked, in a transaction that spends
the advertisement it renews; confirmed once its proof verifies in the
home's headers, a promise until then.

| Exit | Means |
| --- | --- |
| 0 | a successor that checks |
| 1 | an answer that does not check |
| 2 | a price over the budget |
| 3 | a renewal paid and owed (the provider answered `202`), or no live advertisement |

## Keeping up

| Command | Does |
| --- | --- |
| `sync` | sends everything owed: the outbox, closures owed to an upload session, hold transactions not yet seen mined, the change of payments made; then records which of this identity's entries a token has named |
| `fill` | replays to every host what this home made and the host lacks: its entries no token has named yet, and every token of the chains it owns (spec section 12.6) |
| `due` | lists what is owed, and exits 3 when anything is: undelivered objects, open races with the blocks left to their deadline, settlements not mined or not remitted, bounties to reclaim, entries no token has named, tokens recorded and not published |

A BRC-105 payment is kept once it has left this process, answered or not,
since the payee may broadcast it; one the chain view has not seen in an
hour, whose coin the chain view shows unspent, gives its coin back. A
refusal that carries the payee's signed note that it did not take the
payment (spec section 10.4) gives the coin back at once.

Paid calls take a small payment coin of their own: coins split off the
pool into `wallet/paycoins.json` (eight, each two payments of the call's
budget and their fees), so a payment a payee refuses and keeps holds only
that coin. Funding trees and fees never take them. When fewer than half
are left more are split in the background; a call that finds none pays
from the pool.

A settlement remitted before it mines whose proof the chain view has not
served for an hour is named by `due` as an `ALARM` (by `doctor` too): it
may never mine. `due` asks the chain view about each unremitted payee's
output first: a spent one is done. A forwarded payee's share (above) is
listed and not owed. A claim by an incumbent's operator (`bstore-bridge
claim`) is not visible from here: it changes nothing on chain until the
incumbent's wallet spends the output.

## A host's payee

A host that prices its bulk questions (`BSTORE_PRICES`) is paid to its
payee key, `BSTORE_PAYEE_KEY`: each payment is recorded in the host's
`payments.jsonl` and is money only once settled ([host.md](host.md)).

| Command | Does |
| --- | --- |
| `payee key -out FILE` | writes `BSTORE_PAYEE_KEY=<this home's identity private key>` to FILE (mode 0600, never over a file), or with `-out -` to standard output: the home is then the payee's wallet |
| `payee settle <payments.jsonl>... [-in-flight N]` | takes every payment in the ledgers that this home has not settled into its pool: checked to pay the key the identity derives, verified against the headers, broadcast, waited for until it mines, pooled, and recorded in `payee.json` in the home. A payment the network refuses for good is reported once and passed over after. Run it on a schedule. It holds the home's lock, so run it from a home no gateway is using |

The verbs are bcommon's `payeecmd`, the same in every application.

## The kill switch

*Plan the sweep, then run it without a prompt. Irreversible.*

```bash
bstore kill -confirm KILL
bstore kill -confirm KILL -yes
```

`kill` sweeps every funding tree this home holds (pieces, entries,
outcomes), one mined transaction a tree, every output used or not, each
recorded before it is sent, and publishes each sweep to the records topic
of every bucket this identity published in. Once it mines, every carrier
those outputs funded is a double spend: honest hosts retract it, and the
owner's anchoring skips it by its spend check even where a host never saw
the sweep.

It is irreversible, and the identity publishes nothing after it. Keys a
token anchored stay anchored (a delete entry and the owner's next token
remove a key, not a sweep), hold outputs are left to `reclaim`, and a copy
anyone kept stays a copy.

## An S3 client

*Put a 4 GiB file over TLS with timings, get it back as JSON, list, delete, and run a benchmark.*

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
bstore cp ./big.bin s3://example-photos-hmqzvtkwrx/albums/big.bin -endpoint https://s3.example.com
bstore cp s3://example-photos-hmqzvtkwrx/albums/big.bin ./big.bin -endpoint https://s3.example.com -json
bstore ls s3://example-photos-hmqzvtkwrx/albums/ -endpoint https://s3.example.com
bstore rm s3://example-photos-hmqzvtkwrx/albums/big.bin -endpoint https://s3.example.com
bstore bench put s3://example-photos-hmqzvtkwrx/bench -n 4 -size 1GiB -endpoint https://s3.example.com
```

```text
put s3://example-photos-hmqzvtkwrx/albums/big.bin
bytes    4294967296
elapsed  3.912s
rate     1097.9 MB/s
parts    67108864 bytes, 16 at a time
etag     "9b...-64"
crc32c   Zm9vYg==
{"op":"get","bucket":"example-photos-hmqzvtkwrx","key":"albums/big.bin","bytes":4294967296,...}
```

`cp`, `ls` and `rm` with an `s3://bucket/key` argument speak S3 to the
gateway ([s3.md](s3.md)), or to any S3, through the AWS SDK for Go v2:
path-style addressing, Signature Version 4, credentials from
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE` or
`~/.aws`, the endpoint from `-endpoint` or `AWS_ENDPOINT_URL`.

The point is speed with timings: the body is never hashed before it
moves. A request is signed with `UNSIGNED-PAYLOAD`, and the checksum the
gateway verifies rides as a trailer over TLS. A file above one part
(`-part`, default 64 MiB) is a multipart upload with `-conc` (default 16)
parts in flight, each read from the file in place. A download is read in
`-conc` ranges at once into the file. Over plain HTTP the SDK computes the
checksum first, so measure over TLS.

| Checksum (`-checksum`) | Cost |
| --- | --- |
| `crc32c` (the default) | the SSE4.2 instruction on both ends, no visible cost |
| `crc64` | table-driven in Go, about 9% of the gateway's ingest CPU at 1.1 GB/s |
| `sha256` | the hash |
| `none` | nothing |

On completion: the bytes, the elapsed time, the rate in MB/s (decimal),
the ETag and every checksum the gateway returned; `-json` prints the same
as one object. With `-` for the local side the object's bytes are standard
output (or standard input) and the result goes to standard error, so a
pipe carries the object alone. `-ca FILE` trusts a PEM besides the
system's, `-insecure` trusts anything. No progress bars, no recursive
sync: the AWS CLI does those.

`bench put` sends `-n` objects of `-size` fresh random bytes one after
another under the prefix, prints each object's result and the aggregate,
then deletes them (`-keep` leaves them); `bench get` reads the objects
`bench put -keep` left, in ranges, and discards the bytes. Every S3 flag
applies.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | done; everything asked verified; a challenge passed; a renewal checked |
| 1 | refused: a check that names the owner or the writer failed, a broken chain, an invalid signed response, a key proven absent (`NoSuchKey`) |
| 2 | usage, configuration, local or transport error; a quorum not met; a price over the budget; a home in use by another process |
| 3 | incomplete: a host answered what does not verify (named), a key no host produces, a challenge with no valid signed answer, a spend the chain view cannot vouch for, something still owed. It proves nothing either way |

Everything printed that someone else wrote (keys, URLs, a host's or a
provider's words) is filtered for the terminal (bcommon `termsafe`).

## Limits

Every bound, what each operation costs and the known limits are in
[limits.md](limits.md); the command line's own:

| What | Bound |
| --- | --- |
| An object | `put -max`, at most 2^43 bytes; a few 4 MiB blobs and one segment in memory while it streams |
| A race | candidates without bound; at most three free answers asked of one candidate that gives no signed answer; 64 `ads` pages a host a look |
| A settlement | `K` paid ranks; payouts sum to the hold output exactly |
| Anchoring | 16,384 `unanchored` pages a writer and host a run; 1,048,576 keys in a map (a put past it waits) |
| The outbox | an object is sent again for a week, then given up and said so |
| A request | an authenticated call is retried after `429` twice, waiting what `Retry-After` asks up to 10 seconds |
| A provider's bounds | a provider bounds the BRC-103 handshakes and the upload sessions one address opens ([provider.md](provider.md), Limits); each command opens its own sessions |
| An authenticated answer | 9 MiB (a signed response at its bound and room); fronts named by advertisements and statements are reached through the same private-address guard as advertised URLs |
| Anchoring time | the walk of every writer's unanchored entries takes ten minutes at most a run; a host whose page does not verify is not asked for more that run |

What the race cannot tell:

| Blind spot | Why |
| --- | --- |
| An advertiser may point its URL at another's bytes | A by-reference sample proves the advertised URL served the bytes, not whose disk they are on: an advertiser that proxies another provider's blob route passes. As with keys that are not copies (spec section 9.7), independence is the writer's judgment |
| A front that is not the advertiser | It is asked only when the advertiser's own front has not answered; then a hostile one can spend the candidate's draws, though never fail it by its own answer |

## Test it end to end

*Run the publisher's end-to-end suite against a reference overlay host.*

```bash
make publisher-e2e REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
```

On a reference overlay host loading the bstore module, over MySQL
(Docker), on a local test chain, with two providers, through this command
line alone:

1. An owner creates a bucket while the host is down and syncs the first
   token once it is up.
2. A writer it names puts 100 MiB with a bounty of `t = 2`, `K = 2`, its
   closure going to both providers by upload session.
3. The race: each provider passes a free settlement challenge of `c =
   30`. The settlement is killed the instant it reaches the settlement
   leg and finished by running `settle` again (one settlement, the same
   bytes); both providers are paid by remittance.
4. The owner anchors. The object is read back from the providers through
   a home that holds no copy, content hash and proof root checked.
5. A blob dropped from one provider is caught by a paid challenge of
   `c = 113`, its no-answer outcome recorded and published, while the
   other provider passes. A copy reads.
6. `rm`, anchoring and `NoSuchKey`. A bounty nobody can win lapses at its
   deadline and is reclaimed, and is never settled after.
7. The kill switch sweeps every tree, after which the writer's unanchored
   entry is retracted, never anchored, and no further put is published.
