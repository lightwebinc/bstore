# The command line

`bstore` is the publisher client, the bucket owner's tooling and a verified
reader ([spec.md](spec.md) sections 3, 9, 14 and 15); `bstore s3d` is the S3
gateway ([s3.md](s3.md)). This page covers everything but the gateway.

```console
$ go build -o bstore ./cmd/bstore
$ ./bstore -h
```

## A home

A home is one identity: its wallet, its funding trees, its outbox, its hold
races, the bucket chains it owns, the closures it built and `config.json`.
It is `-home DIR`, else `$BSTORE_HOME`, else `~/.bstore`. One process at a
time uses a home (an advisory lock on its directory): two processes writing
one identity's records would each overwrite what the other spent. Back the
home up: the wallet is the identity.

```console
$ bstore init -network main -node https://node.example.com \
    -settle arcade:https://arcade.example.com -headers https://headers.example.com \
    -hosts https://overlay.example.com -facade https://plane.example.com
identity 02...
$ bstore fund
identity     02...
fund address 1...
pool         0 output(s), 0 satoshis
$ bstore fund -from 850000
```

`fund -from H` takes what was paid to the fund address in blocks `H` to the
tip into the pool; on a test network `fund -blocks N` mines `N` blocks to it
through the node's RPC. `doctor` reports the home and whether everything it
names answers, and changes nothing. `terms URL` prints a provider's terms
(its front, ending `/bstore/v1`) or a host's `ls_bstore` terms.

### config.json

An unknown key is refused.

| Key | Default | Meaning |
| --- | --- | --- |
| `network` | required | `main`, `test` or `regtest` |
| `node`, `settle` (`rpc_user`, `rpc_pass`, `arcade_key`), `headers` | required | the node's asset API (proofs, spends, the tip), the settlement leg (`rpc:`, `tcp:` or `arcade:`), the header source |
| `hosts` | required | the overlay hosts asked and written to |
| `facade` | none | the plane: pieces and records are submitted to it once |
| `providers` | none | providers' submit routes: without a facade, entries (and piece carriers) go to each |
| `uploads` | none | providers' public URLs: without a facade, a closure goes to each by BRC-167 upload session instead of as piece carriers |
| `quorum` | 1 | hosts that must take an entry when there is no facade |
| `ca`, `fetch_private` | none, false | certificates trusted besides the system's; whether advertised URLs at loopback, private or link-local addresses are reached |
| `bounty` | none | what a put asks for unless told otherwise: `satoshis`, `t`, `k`, `retention_seconds`, `deadline_blocks`, `check_c` |
| `hold_ahead` | 8 | mined hold outputs of a bounty kept ready |
| `race_window_seconds` | 3600 | the writer's race window |
| `publish_outcomes` | true | publish failed settlement challenges and every challenge's outcome as outcome records |
| `piece_tree`, `entry_tree` | 64, 16 | outputs of a new funding tree (outcome trees take `entry_tree`) |
| `fee_sat_per_byte`, `fee_floor` | 1, 250 | the fee policy |
| `buckets`, `bucket` | none | the chains this home reads and writes (`{"bucket", "owner", "id"}`), and the one used unless `-bucket` names another |

## The owner

```console
$ bstore bucket new photos -writers 03...,02...
bucket photos_hmqzvtkwrx
owner  02...
id     5f...
served photos_hmqzvtkwrx:02...:5f...
topics tm_bstore_photos_hmqzvtkwrx tm_bstoreblob_photos_hmqzvtkwrx
```

`bucket new` draws the identifier's ten random letters, mines a chain tree
of one output under `derive(owner, "chain")` and the first token spending
it, naming the writers, and adds the chain to `config.json`. Each is
recorded before it is sent; a run that stops part way is finished by running
`bucket new` again with the identifier it printed. The `served` line is what
a host's operator adds to its served chains. `bucket add BUCKET -owner KEY
-id HEX` names a chain someone else owns, to read it or write to it.

`anchor` folds the writers' entries into the next tokens, exactly as frozen
(spec sections 3.4 and 4.4):

1. The chain is rebuilt from the hosts every run: the highest token any host
   answers that verifies against this home's headers and that the node shows
   unspent, walked back by `prev` through `bucket-seq` to seq 0, every link
   and the first token's bucket id checked. Its fold must be consistent; a
   broken chain is refused (exit 1) and the way on is a new chain. The set of
   commitments the chain has named comes from those tokens, never from a
   local record.
2. Each writer's entries (the owner's and those of the writers the next
   token names) are walked with `unanchored` at every host, every page from
   the start. Every entry an answer returns must verify (the carrier's rules
   and its funding tree's proof) and carry the writer, bucket and id asked
   for; a host whose answer does not, or whose pages are out of order, is
   named and the run is incomplete (exit 3). A host that withholds an entry
   delays it; another host's answer still finds it.
3. Each entry's funding outpoint must be unspent by positive evidence from
   the node: a swept entry is skipped for good, and one the node cannot vouch
   for waits for the next run (exit 3).
4. The batch is folded against the current map before anything is minted:
   one key's entries ordered by `created`, then commitment, the last deciding
   and the rest consumed; a delete of a key the map does not hold, and a put
   of the value a key already names, go to `consumed`; an entry any token
   named is skipped whatever a host says.
5. The changes are split over tokens at the record's 65,536 bytes, 1024
   changes and 1024 consumed; one key's change is carried by one token, its
   superseded entries spilling into tokens of `consumed` alone before it.
   Each token is recorded before it is sent, mined, then published with its
   parent to the facade and every host. A token another instance minted
   first at the same seq is detected and dropped, and the next run builds on
   the winner.

`-add-writer` and `-remove-writer` change the writers the next token names;
a writer is removed only when the map names none of its entries. `-dry-run`
plans and mints nothing.

## Writing

```console
$ bstore put photos/cat.jpg ./cat.jpg -bounty 20000 -t 2 -k 3 -deadline 144
entry  9c...
key    photos/cat.jpg
root   41...
size   1048576
hold   7a....0, 20000 satoshis, t 2, K 3, deadline 851000
$ bstore rm photos/old.jpg
$ bstore cp photos/cat.jpg photos/cat-copy.jpg
$ bstore cp photos/cat.jpg photos/cat.jpg -to example_backup_qwhzkmvtrx
```

`put` builds the closure and the proof root while the content streams, then
takes a mined hold output of the bounty (minted ahead, sixteen to a
transaction, marked taken before the entry is signed; with none ready it
waits up to 30 seconds, then exits 3 while more mine), publishes the closure
and the entry, and answers once the facade or a quorum of the hosts took the
entry. The closure goes to the plane on the blob topic, or, with no facade,
as piece carriers to each provider's submit route, or by BRC-167 upload
session to each provider of `uploads` (session opened, each object staged
once, root committed; the commit answers `202` until the entry reaches the
provider). Save before send: funding outputs are recorded used before any
carrier is signed, every carrier and every entry is written to the home
before it is sent, and what a target did not take is sent again, the same
bytes, by `sync`. A put is pending until the owner's next token anchors it.

`rm` publishes a delete entry. `cp` publishes a put naming the source's
anchored content; to another bucket it publishes the closure again on that
bucket's blob topic, from the home's store or from the source's advertisers.

## The hold race and settlement

The writer's own client judges the race (spec section 9.4):

```console
$ bstore holds -watch 10m
race 9c...  key photos/cat.jpg  open
  hold 7a....0  20000 satoshis  t 2  K 3  checkC 30  deadline 851000
   1. 03...  first seen 10:02:11.412  passed  last: pass c=30
   2. 02...  first seen 10:02:11.412  passed  last: pass c=30
   3. 02...  first seen 10:04:52.090  seen
$ bstore settle
```

- **First sight.** Each look asks every host for the root's advertisements.
  One that is mined in this home's headers, names the root, is not by the
  anyone key, expires no earlier than the entry's retention, has the root
  node's length as `fileSize` and a CHIRP URL is eligible, and its
  advertiser's first sight is the time of that look; advertisements seen in
  one look tie, and the lower txid ranks first. A renewal keeps the
  original's sight. No provider can make this client see an advertisement
  before it exists and is mined, and an advertisement says nothing about the
  bytes: possession is what the challenge proves.
- **The settlement challenge.** In rank order, each candidate is challenged
  at its front (derived from its advertisement's URL; a front statement
  naming it, four at most, only once its own front has not answered) with
  `min(checkC, freeC)` samples, `freeC` from its terms (30, the floor every
  provider's `freeC` meets, when its terms cannot be read), seeded two
  blocks below the tip with a fresh nonce, and the signed response is
  checked sample-major, by-reference samples fetched from the advertised URL
  and rechecked from the whole blob.
- **Draws.** A candidate gets two requests (draws) over the whole race,
  every front counted. A draw is recorded before it is sent, and one not
  answered (a timeout, a server error, a price) is sent again unchanged,
  the same nonce, until its seed leaves the answerer's window or three sends
  have failed; only then is it spent. So a provider cannot answer the draws
  it can serve and refuse the rest: refusing only delays it. A refusal of
  the request itself, or a signed answer, spends the draw.
- **Whose failure.** A failure is the answerer's (spec section 7.4): at the
  advertiser's own front it fails the candidate; at a front that is not the
  advertiser it is the front's, that front is not asked again, and the
  candidate is not failed (an unretrievable sample, which is about the
  advertiser's URL, fails it whoever answered). A provider whose `minAge`
  the hold output is not yet deep enough for waits for a later look; one
  whose `minBounty` the bounty is below is priced, and is paid only up to
  `-budget`; neither is passed or failed. A failure at a signed answer is
  published as an outcome record. One candidate takes at most four minutes
  of a look.
- **Close.** The race closes when `K` have passed, when the window
  (`race_window_seconds`) ends with at least `t` passed, or with `settle
  -close`. With fewer than `t` when the window ends nothing is paid.

`settle` pays by rank (spec section 9.5). It first checks each payee's
advertisement unspent by positive evidence from the node, a renewal
successor counting as unspent; a cancelled one is dropped, the ranks below
move up, the next passed candidate fills the last place, and `t` is counted
again. One transaction spends the hold output and pays rank `i` the
square-root share `floor(R * sqrt(k - i + 1) / S)`, the remainder to rank 1,
as a P2PKH output to the key BRC-29 derives for its advertiser with prefix
the entry's commitment and suffix the two-byte rank; the payouts sum to
exactly the hold output and the fee comes from another coin. A payee's
advertisement is followed through sixteen renewals at most, one successor a
spend; a longer chain is taken as not live. The
settlement is signed and recorded before it reaches the settlement leg: a
`settle` killed at any point is finished by running it again, which sends
the same bytes and never builds a second settlement for the hold output.
Once it mines, each payee's remittance goes, unicast, to the `settle` route
of the advertiser's own front, derived from its advertisement, never to a
front that answered for it; `settle` again resends any a payee has not
taken. A settlement that can never mine (a fee coin spent elsewhere) is set
aside and the next `settle` builds another; a settlement a miner re-encoded
is taken as the one sent. A settlement is made only
while the tip is below the deadline.

`reclaim` spends a hold output nobody won back to the fund key once the tip
reaches its deadline: a race closed with fewer than `t`, or one whose
deadline came first. A hold output with a settlement recorded (and not set
aside as never able to mine) is never reclaimed, and a reclaimed one is
never settled. A put that stops before its entry is made gives its hold
output back to the next put; `reclaim -unused` spends back every mined hold
output no entry names, each reclaim recorded whole before it is sent.

The gateway's puts with a bounty are judged and settled the same way: run
`bstore holds` and `bstore settle` with `-home` at the identity's directory
(`<data>/identities/<name>`, with a `config.json` beside its wallet) while
the gateway is stopped.

## Reading

```console
$ bstore get photos/cat.jpg ./cat.jpg -proof-root
$ bstore ls photos/
1048576	<created>	photos/cat.jpg
```

`get` makes every check of spec section 15.2 in order: the chain's latest
token from the hosts, walked back to seq 0 and checked unspent; the map's
proof of the key; the entry, its writer and its kind; the root and every
branch, held to their hashes and to profile 1's construction; every blob
from the root's advertisers, each held to its hash before a byte of it is
written, the next advertiser asked when one fails and one serving wrong
bytes named; the content hash before the last blob is released; with
`-proof-root` the proof root too. A file is written whole or not at all. A
key the map proves absent is `NoSuchKey` (exit 1); a key whose entry no host
produces is incomplete (exit 3). `ls` lists keys under a prefix, every page
proven complete.

## Challenges

```console
$ bstore challenge photos/cat.jpg -provider 03... -c 113 -budget 2000
challenge of 03... at https://provider.example.com/store/bstore/v1: pass (c = 113, paid 123)
```

A possession challenge (spec section 7) of one provider's advertisement:
`c` samples chosen from the tip block's hash, a fresh nonce (`-nonce` fixes
one) and the provider's key, paid through the provider's 402 up to
`-budget` (a price over it is refused, exit 2). The outcome is appended to
`outcomes.jsonl` in the home and published as an outcome record on the
bucket's records topic unless `-no-publish`. A pass exits 0; an invalid
signed response, which anyone holding it can check again, exits 1; no valid
signed answer (a refusal, a timeout, a provider that found its own copy
damaged) or a sample the advertised URL did not serve exits 3. One dropped
blob of a 100 MiB object is caught by `c = 113` 99 times in 100.

## Keeping up

- `sync` sends everything owed: the outbox, closures owed to an upload
  session, hold transactions not yet seen mined, the change of payments
  made; then records which of this identity's entries a token has named. A
  BRC-105 payment is kept once it has left this process, answered or not,
  since the payee may broadcast it; one the node has not seen in an hour,
  whose coin the node shows unspent, gives its coin back.
- `fill` replays to every host what this home made and the host lacks: its
  entries no token has named yet, and every token of the chains it owns
  (spec section 12.6).
- `due` lists what is owed, and exits 3 when anything is: undelivered
  objects, open races with the blocks left to their deadline, settlements
  not mined or not remitted, bounties to reclaim, entries no token has
  named, tokens recorded and not published.

## The kill switch

```console
$ bstore kill -confirm KILL
$ bstore kill -confirm KILL -yes
```

`kill` sweeps every funding tree this home holds (pieces, entries,
outcomes), one mined transaction a tree, every output used or not, each
recorded before it is sent, and publishes each sweep to the records topic
of every bucket this identity published in. Once it mines, every carrier
those outputs funded is a double spend: honest hosts retract it, and the
owner's anchoring skips it by its spend check even where a host never saw
the sweep. It is irreversible, and the identity publishes nothing after it.
Keys a token anchored stay anchored (a delete entry and the owner's next
token remove a key, not a sweep), hold outputs are left to `reclaim`, and a
copy anyone kept stays a copy.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | done; everything asked verified; a challenge passed |
| 1 | refused: a check that names the owner or the writer failed, a broken chain, an invalid signed response, a key proven absent (`NoSuchKey`) |
| 2 | usage, configuration, local or transport error; a quorum not met; a price over the budget; a home in use by another process |
| 3 | incomplete: a host answered what does not verify (named), a key no host produces, a challenge with no valid signed answer, a spend the node cannot vouch for, something still owed. It proves nothing either way |

Everything printed that someone else wrote (keys, URLs, a host's or a
provider's words) is filtered for the terminal (bcommon `termsafe`).

## Limits

| What | Bound |
| --- | --- |
| An object | `put -max`, at most 2^43 bytes; one 4 MiB blob and one segment in memory while it streams |
| A race | candidates without bound; at most three free answers asked of one candidate that gives no signed answer; 64 `ads` pages a host a look |
| A settlement | `K` paid ranks; payouts sum to the hold output exactly |
| Anchoring | 16,384 `unanchored` pages a writer and host a run; 1,048,576 keys in a map (a put past it waits) |
| The outbox | an object is sent again for a week, then given up and said so |
| A request | an authenticated call is retried after `429` twice, waiting what `Retry-After` asks up to 10 seconds |
| A provider's bounds | a provider bounds the BRC-103 handshakes and the upload sessions one address opens ([provider.md](provider.md), Limits); each command opens its own sessions |
| An authenticated answer | 9 MiB (a signed response at its bound and room); fronts named by advertisements and statements are reached through the same private-address guard as advertised URLs |
| Anchoring time | the walk of every writer's unanchored entries takes ten minutes at most a run; a host whose page does not verify is not asked for more that run |

What the race cannot tell:

- **An advertiser may point its URL at another's bytes.** A by-reference
  sample proves the advertised URL served the bytes, not whose disk they are
  on: an advertiser that proxies another provider's blob route passes. As
  with keys that are not copies (spec section 9.7), independence is the
  writer's judgement.
- **Ties go to the lower txid.** Advertisements first seen in one look tie,
  and the lower advertisement txid ranks first (spec section 9.4, row C18):
  a provider can grind its txid, and every advertisement that existed
  before the race ties on the first look.
- **A front that is not the advertiser** is asked only when the advertiser's
  own front has not answered; then a hostile one can spend the candidate's
  draws, though never fail it by its own answer.

## Test it end to end

```console
$ make publisher-e2e REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
```

On a reference overlay host loading the bstore module, over MySQL (Docker),
on a local test chain, with two providers, through this command line alone:
an owner creates a bucket while the host is down and syncs the first token
once it is up; a writer it names puts 100 MiB with a bounty of `t = 2`,
`K = 2`, its closure going to both providers by upload session; the race,
each provider passing a free settlement challenge of `c = 30`; the
settlement killed the instant it reaches the settlement leg and finished
by running `settle` again (one settlement, the same bytes), both providers
paid by remittance; the owner anchors; the object is read back from the
providers through a home that holds no copy, content hash and proof root
checked; a blob dropped from one provider is caught by a paid challenge of
`c = 113`, its no-answer outcome recorded and published, while the other
provider passes; a copy reads; `rm`, anchoring and `NoSuchKey`; a bounty
nobody can win lapses at its deadline and is reclaimed, and is never
settled after; and the kill switch sweeps every tree, after which the
writer's unanchored entry is retracted, never anchored, and no further put
is published.
