# Limits

What an object may be, what a put, a race, a challenge and a renewal cost,
how fast the pieces move, and what bstore does not do. The record bounds
every reader accepts ([spec.md](spec.md) section 19.1) are the contract's
and sit above everything here. The rest is policy: defaults and caps of the
command line ([usage.md](usage.md)), the provider
([provider.md](provider.md)), the S3 gateway ([s3.md](s3.md)) and the host
module ([host.md](host.md)), none of them a wire format or a frozen bound
([frozen.md](frozen.md)); a later version may change any of them.

Each number below is one of three kinds, said in its row:

- **measured**: a run produced it, on a test network whose blocks came
  about every 30 seconds, with three providers (two native, one bridged
  incumbent), two hosts and two S3 gateways, over the exit demonstration
  run twice and four drills (section [Where the numbers come
  from](#where-the-numbers-come-from));
- **set**: a default or a cap this version chooses;
- **contract**: a record bound or arithmetic from the signed contract.

## Objects

| Limit | Value | Kind |
| --- | --- | --- |
| An object's size | 0 to 2^43 bytes (8 TiB) | contract |
| A blob, a segment | 4 MiB (4,194,304 bytes) of content; 4 KiB proof segments, 1,024 to a blob | contract |
| Pieces of an object | one per blob, plus the root node, plus a branch node per 256 blobs above 256: a 100 MiB object is 25 blobs and a 1,077-byte root, 26 pieces; a 20 MiB object 5 blobs and a 257-byte root, 6 pieces; 1 TiB is 263,173 pieces | contract (the 100 MiB and 20 MiB roots measured: a provider's store held exactly the content plus 6,976 bytes for six 100 MiB and two 20 MiB objects) |
| A piece carrier | 4,259,840 bytes of BEEF at most: the blob, its record and the funding tree's path | contract |
| A key, an entry's `meta` | 1,024 bytes, 2,048 bytes; an entry 8,192 bytes | contract |
| `bstore put` | `-max`, default 2^43 bytes; one 4 MiB blob and one segment in memory while it streams | set |
| S3 `PutObject`; a multipart object | 5 GiB; 100 GiB, 10,000 parts | set (`limits.max_object`, `limits.max_upload`) |
| What a provider takes | `policy.max_object`, default 1 GiB | set |

## A put

The writer pays three things: the funding outputs the carriers spend (mined,
shared by many puts), the hold output's transaction (mined, minted ahead),
and the bounty itself. Pieces and entries are carriers: never mined, each
spending one mined funding output.

| What | Cost | Kind |
| --- | --- | --- |
| A funding output | a 64-output piece funding tree is 3,328 bytes, 52 bytes an output, so about 5.2 satoshis at the network's rate, 100 per 1000 bytes (measured at 1 satoshi a byte, bcommon `mint.LegacyFees`: about 52); the contract's arithmetic says about 57 bytes | measured |
| A 100 MiB put's funding | 26 piece outputs and one entry output: about 1.4 KB mined, about 140 satoshis of fee, plus each output's 1 satoshi of value (a carrier spends it and is never mined) | measured |
| An entry carrier | 1,915 to 2,176 bytes of BEEF on a 16-output entry tree | measured (8 entries) |
| A piece carrier | the blob plus about 1.2 KB and 49 bytes per output of its funding tree: about 4.3 KB over the blob on the default 64-output piece tree, so a 100 MiB put delivers about 0.1 % above its content to each provider | contract arithmetic |
| The hold output's transaction | 240 and 241 bytes, 100 satoshis of fee (the floor; 495 when measured at the older 1 satoshi a byte, floor 250), for one hold output (up to 16 a transaction when minted ahead) | measured |
| The bounty | the writer's choice, at least each provider's `terms.min_bounty` (default 1,000) or that provider does not race; the test runs used 30,000 and 1,000 satoshis, `t = 3`, `K = 3` | set by the writer |
| A 100 MiB `PutObject` through the gateway | 7.9 s from the first byte to the `200`, with no block wait (hold outputs and funding trees minted ahead; staging hashed on several cores) | measured (v0.5.0; 9.8 s on v0.3.0) |
| Staging 100 MiB (closure, proof root, store writes) | 0.6 to 0.8 s locally, was 2.9 to 3.3 s on one core | measured (v0.5.0) |
| A provider's closure check once the entry arrives, 100 MiB | 0.05 to 0.08 s, the closure walked as the pieces arrived (about 1.7 s spread over the piece requests); 2.6 to 2.9 s when every piece is read back, as after a restart | measured (v0.8.0) |

## The hold race and the bounty

| What | Value | Kind |
| --- | --- | --- |
| Hold terms | `t` 1 to 16 distinct advertisers, `K` `t` to 16 paid ranks, `checkC` 1 to 1024 | contract |
| Payout shares | rank `i` of `k` paid gets `floor(R * sqrt(k - i + 1) / S)`, the remainder to rank 1, summing to exactly the hold output | contract |
| Shares measured | 12,533 / 10,232 / 7,235 of 30,000 for three ranks (also 11,716 / 8,284 of 20,000 for two in the end-to-end test) | measured |
| The settlement transaction | 406 and 407 bytes, 100 satoshis of fee (the floor), three payees; the fee from another coin, never the hold output | measured |
| The race, three advertisers seen in one look, each passing a free settlement challenge of `c = 30` | 0.9 s for `holds -watch`, the race already judged in the background by the gateway; `settle` 18.9 s, one block wait | measured (v0.3.0) |
| A settlement challenge's `c` | `min(checkC, freeC)`; 30 when a provider's terms cannot be read | contract (C19) |
| Draws | two per candidate over the whole race, each sent again unchanged until its seed leaves the window | contract (C23) |
| Race window | `race_window_seconds`, default 3,600 | set |
| Free settlement challenges a provider owes | two per hold outpoint and advertiser, `c` at most `min(checkC, freeC)`, `freeC` at least 30, the bounty at least the provider's `minBounty` and `minAge` deep | contract |
| A bridge's commit at an incumbent | the incumbent's price, paid by the bridge from its own coin: 500 satoshis a commit at the stand-in incumbent; the bridge never receives a bounty | measured |

## Challenges

A challenge samples `c` segments chosen from a block hash two below the tip,
the buyer's nonce and the provider's key; each sample is a 4 KiB segment
with its proof path, about 4,576 bytes for a 100 MiB object.

| What | Value | Kind |
| --- | --- | --- |
| Price | the provider's `base + perSample * c`: 40 at `c = 30` and 123 at `c = 113` on terms of 10 + 1; 70 at `c = 30` at a bridge pricing 10 + 2 | measured |
| A response at `c = 30`, `c = 113` | about 138 KB and 517 KB for a 100 MiB object; at most 8 MiB at the bound `c = 1024` | contract arithmetic |
| Time | 1.8 s at `c = 30` through a bridge, every sample by reference from the incumbent's own URL (60 ranged reads: the bridge's and the reader's); 2.2 s at `c = 113` at a native provider; 0.9 s for a refusal of a dropped blob (`unknown-root`, nothing paid) | measured |
| A challenge's payment | built on the provider's 402 from a small payment coin; on a refusal released at once when the provider signs it did not take it, else kept for an hour (see known limit 5) | set |

**Detection.** A provider missing a fraction `f` of the segments passes one
challenge of `c` samples with probability `(1 - f)^c` (spec section 7.6).
Percent of challenges that catch it:

| `c` | 50 % lost | 25 % | 10 % | 5 % | 1 % | 0.1 % |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 50.0 | 25.0 | 10.0 | 5.0 | 1.0 | 0.1 |
| 8 | 99.6 | 90.0 | 57.0 | 33.7 | 7.7 | 0.8 |
| 16 | >99.9 | 99.0 | 81.5 | 56.0 | 14.9 | 1.6 |
| 30 | >99.9 | >99.9 | 95.8 | 78.5 | 26.0 | 3.0 |
| 60 | >99.9 | >99.9 | 99.8 | 95.4 | 45.3 | 5.8 |
| 100 | >99.9 | >99.9 | >99.9 | 99.4 | 63.4 | 9.5 |
| 230 | >99.9 | >99.9 | >99.9 | >99.9 | 90.1 | 20.6 |
| 459 | >99.9 | >99.9 | >99.9 | >99.9 | 99.0 | 36.8 |
| 1000 | >99.9 | >99.9 | >99.9 | >99.9 | >99.9 | 63.2 |

One dropped blob of a 100 MiB object is 4 % of it: a `c = 30` challenge
catches it 70.6 % of the time and `c = 113` 99 % of the time (contract).
Every dropped blob in the test runs was caught at `c = 113` on the first
try, three times at native providers and once through the bridge at the
incumbent (measured); a provider that passed a challenge and then lost a
blob failed the next one, and both outcome records stand side by side at
every host.

## Renewals

| What | Value | Kind |
| --- | --- | --- |
| Price | the provider's `perGiBDay` for the object's size and the time added, never below its `minimum`: 7 days of 100 MiB cost 500 satoshis (the minimum) at a native provider and 2,000 (its minimum) at a bridge | measured |
| The successor | 650 to 656 bytes, 100 satoshis of fee (the floor), paid by the provider; it spends the advertisement it renews | measured size |
| Time | 7.6 s to an answer (the successor a promise until it mines); 87.9 s through a bridge that renewed at the incumbent and waited for the successor to mine | measured |
| How far | `terms.max_renew_seconds`, default 366 days | set |

## The bucket chain and anchoring

| What | Value | Kind |
| --- | --- | --- |
| Keys in a bucket | 1,048,576; a put past it waits | contract |
| A token | 65,536 bytes encoded, 16 writers, 1,024 changes and 1,024 consumed; `anchor` splits a larger interval | contract |
| A token on chain | 616 to 961 bytes, 100 satoshis (the floor) | measured |
| Cadence | when the owner runs `anchor`, at most one token a block: each spends the last and is minted only once the last is mined in the owner's headers. Until then a put is pending: readable at the writing gateway, and as a claim by `pending` | contract |
| An anchoring run | 19.7 s for one change, 26.8 s for a delete (v0.3.0; each a block and both hosts) | measured |
| Its walk | 16,384 `unanchored` pages a writer and host, ten minutes in all | set |

## Reading

| What | Value | Kind |
| --- | --- | --- |
| A 100 MiB `GetObject` at a gateway with no copy, every blob held to its hash before it is sent | 3.4 s | measured (v0.3.0) |
| `bstore get -proof-root` of the same object | 3.7 s | measured |
| `HeadObject`, `ListObjectsV2` at that gateway | 1.7 s, 1.6 s (the chain walked and proven each time) | measured |
| Advertisers asked per root | 256 (four pages); one serving wrong bytes, failing or hanging is named and asked last | set |
| Listing pages | 256 keys, every page proven complete | contract |

## Providers

| What | Value | Kind |
| --- | --- | --- |
| Native store | the content plus each root and branch node: 671,095,616 bytes for six 100 MiB and two 20 MiB objects | measured |
| Segment hashes kept | 32 bytes per 4 KiB, 1/128 of the content (819,200 bytes for 100 MiB) | contract arithmetic |
| A bridge's store | no bytes: it keeps segment hashes only, and reads samples from the incumbent | measured |
| Quota | `quota_bytes`, default 64 GiB; unclaimed pieces an eighth of it, a quarter of that per writer, kept `unclaimed_seconds` (3,600) | set |
| Every other bound | the provider's table ([provider.md](provider.md), Limits) | set |

## Hosts

| What | Value | Kind |
| --- | --- | --- |
| Retention floors | a served chain's tokens for the host's life; anchored entries while named, then 31 days; entries no token names 7 days; advertisements, hold outputs, outcomes, front statements and sweep outpoints 31 days; retraction marks for served parties for life; everything else 24 hours ([host.md](host.md), Retention) | contract floors, set upward by `BSTORE_KEEP_DAYS`, `BSTORE_PENDING_DAYS`, `BSTORE_OTHER_HOURS` |
| Priced questions | the host's `BSTORE_PRICES`: the test hosts priced `history` 5, `chain` 10, `export` 20, `outcome-history` 3, `outcome-history-of` 4 satoshis; every other class is free | set |
| How long a written journal line may wait for its fsync (the Go module) | 100 ms (`BSTORE_JOURNAL_SYNC_MS`, 0 to 60000; 0 flushes each line before the admission completes, under the lookup service's lock): one fsync covers every line of the window, so a power loss loses at most the window and a process that dies loses nothing ([host.md](host.md)) | set |
| What a directory host holds for a 100 MiB object | under 10 KB: the entry, the hold transaction, one advertisement a provider, the token's share | contract arithmetic |

## Plane and unicast

On the plane a writer submits each piece and each record once, to its
facade, and every provider subscribed to the bucket's blob topic is
delivered the pieces, billed by the byte to that provider's operator, while
hosts take the records topic only. In unicast the writer uploads each piece
to each provider itself, as piece carriers to `/submit` or by a BRC-167
upload session, and each record to each host: an object costs the writer
one upload a provider. Records are the same either way.

A piece reaches the plane through a facade that routes its bucket's blob
topic to a provider on that host, because a host holds no bytes: the provider
holds the piece to its rules first, so the facade publishes only valid pieces
(provider.md). Pieces are larger than a plane's open object bound, so the
facade's site must publish as an authenticated source. A provider on the
plane is fed by its own bridge; a provider that refuses a piece gets no second
copy, so its quota has to hold what it serves. A 100 MiB put is about 104.9 MB to
each provider either way; the difference is who sends it and who pays for the
delivery: one publish and each receiver's delivery on the plane, one upload a
provider in unicast.

## Known limits

These are not settings. Each is something the tooling or the contract does
not do, stated so that nobody finds it by accident.

1. **An advertiser can point its URL at another provider's bytes.** A
   by-reference sample proves the advertised URL served the bytes, not
   whose disk they are on: an advertiser that proxies another provider's
   blob route passes its challenges. As with keys that are not copies
   (spec section 9.7), independence is the writer's judgment. No
   clarification closes it: a sample can show that bytes were served, never
   whose copy they are, short of a sealed copy per advertiser, which is a
   new proof format, not a reading of this one.
2. **Tie-key grinding is still open to a late advertiser.** Ties in first
   sight break by `SHA-256(txid || vout || C)` (frozen.md A1), which nobody
   can grind before the entry exists. A provider that advertises only after
   the entry is published can still grind its advertisement's txid, and
   wins only ties with advertisements first seen in that same look. Closing
   it means a tie key the advertiser cannot know when it mints (a later
   block's hash, say), which changes amendment A1's rule and its vectors: an
   amendment, not a clarification, and the gain is a rank among ties in one
   look only.
3. **A bridged incumbent's derived front must answer `404`.** Clarification
   C31 ([frozen.md](frozen.md) section 14, signed) does not count a `404
   ERR_ROUTE_NOT_FOUND` from the advertiser's own derived front as a draw,
   so a bridged incumbent gets two draws at the bridge's front, as a native
   provider does. An incumbent whose derived front does not answer at all
   (or refuses the BRC-104 handshake the client sends) still spends a draw
   after three tries (C23, C29): the client's BRC-104 handshake is go-sdk's,
   and until the go-sdk bcommon pins sends the wire shape a TypeScript
   server accepts, such an incumbent gets one draw.
4. **Incumbents that ignore `Range` cost 4 MiB a sample.** A bridge reads
   each sampled segment from the incumbent's URL; when the incumbent
   answers a ranged request with the whole blob, a challenge of `c` reads
   up to `c` x 4 MiB there, and is priced per sample all the same. The
   bridge probes `Range` after each commit and daily, and states what it
   found. Prices are the provider's own terms, not the contract's, so this
   needs no clarification: a bridge in front of such an incumbent prices
   its samples for what they cost it.
5. **A refused paid challenge without a note holds a coin for an hour.**
   The buyer builds its payment on the provider's 402; a provider that then
   refuses (`unknown-root`) takes nothing. This version's providers say so
   in a signed payment-not-taken note (spec section 10.4, row C30), and the buyer releases the coin
   at once. A provider that sends no note leaves the client keeping the
   payment as possibly broadcast, since the payee could still broadcast it,
   and giving the coin back only when the chain view has not seen it in an hour.
   Paid calls take a small payment coin of their own, split off the pool,
   so such a hold costs little and never blocks the identity's other coin.
6. **A bridged incumbent's remittance is never taken.** Its own front has
   no settle route; the bridge forwards the settlement to the incumbent's
   operator instead (C28). `bstore settle` and `bstore due` therefore list
   such a payee, while its output is unspent, as forwarded by the front
   that answered for it and do not count it as owed; once the chain view shows
   the payee's output spent, the payee is done. A claim alone is not
   visible: `bstore-bridge claim` prints what the incumbent's wallet takes
   in, and nothing reaches the chain until that wallet spends the output.
7. **A chain walk cut short by a restart resumes, but only from what it
   kept.** A gateway walks a bucket's chain back from its latest token in
   the background, and keeps every token it verifies in a file of the
   chain's (`walks/` under its data directory, at most 64 MiB a chain)
   until the walk completes. A restarted gateway verifies those tokens
   again against its own headers, without asking a host, and asks only for
   the rest. A chain longer than the file holds asks hosts again for the
   part past it.
8. **A retry is the same entry only while it is recent and the key's
   newest.** A put whose entry was not taken in time answers `503
   SlowDown` and the entry stays in the outbox, to be delivered. The same
   write sent again (same identity, key, content, content type, ETag and
   metadata) within an hour, while that entry is still the key's newest
   pending one at the gateway, is answered `200` from it, and no second
   entry is made. Only an entry answered `503` is reused: a write answered
   `200` and sent again is a new write. A retry after another write to
   the key at this gateway, after the hour, after a gateway restart, or at
   another gateway makes a second entry for the same content; the later
   one decides the key, and the owner's anchor consumes the earlier. A
   retry answered from its entry does not make it newer than a write to
   the key made at another gateway in between.
9. **A provider that stays down stops the gateway's puts at a bound.**
   Every piece a target has not taken is kept and sent again, the same
   bytes, for a week, pinned in the local store while it is owed. Past
   `outbox_bytes` owed (default 8 GiB per identity, counted at most every
   5 s, so puts started together may pass it by their own sizes) a put is
   refused before anything is spent, `503 SlowDown`, and the gateway's log
   names the target that owes most; puts are taken again once that target
   takes what it is owed (or the week passes). The bound counts what is
   owed, so one provider that is down for long enough stops every put of
   the identity, by design: the alternative is a store that grows without
   bound.
10. **The reference host gives a module no engine handle.** So:
    - **no evict:** the module drops an object through the storage the host
      hands it after the engine has recorded the admission, never by an
      eviction call;
    - **no resubmit:** a token or advertisement a host refused because its
      block was not yet in the host's headers is not retried by the host;
      the publisher's own retries and `fill` cover it;
    - **no per-submitter limits:** the module cannot see who submits, and
      funding does not bound a stranger's carriers. A host that cannot
      accept that runs behind a plane or a front that limits submissions.
11. **The blob topics carry nothing yet.** Pieces travel by unicast (Plane
    and unicast, above), so a blob topic a host or a consumer subscribes
    to is delivered nothing; a directory host should not subscribe to one.
12. **A settlement remitted before it mines can stay unmined.** A
    settlement at or below the acceptance threshold is remitted on arcade's
    word that the network took it (race state `remitted`). When the chain
    view serves no proof of it for an hour, the gateway's
    `bstore_s3d_races_remitted_stale` gauge counts it and `bstore doctor`
    names it as an alarm. If it never mines because its fee coin went
    elsewhere, the next settlement of the race is remitted only once it
    mines; its payees hold the writer's later remittances for confirmation
    until a mined settlement pays them at least what the lost one said
    (provider.md, Payment acceptance).
13. **The incumbent in the tests is a stand-in.** The bridge is tested
    against `internal/fakeuhrp` in bstore-bridge, written from the
    incumbent server's published HTTP descriptions, not against the
    incumbent server itself, whose wallet storage, chain services and
    advertisement broadcast reach only public networks.
14. **The images are not published yet.** The command, provider, host and
    local chain images are built from a checkout (`make docker-build`,
    `make docker-build-host`) until the repositories are public.
15. **The fees were measured at the older rate.** The sizes here were
    measured at 1 satoshi a byte, ten times the 100 satoshis per 1000
    bytes mainnet miners take. The default is now the network's rate,
    with a floor of 100 satoshis a transaction and a cap of 100 per 1000
    bytes (the `fee` setting), so every transaction under 1,000 bytes
    pays the floor; the fees quoted here are recomputed at that default
    from the measured sizes.
16. **Funding is by import.** A pool takes coin through `fund -beef` (the
    BEEF the user's wallet hands over; it may not have mined yet) or
    `fund -txid` (a mined payment); `fund -from` and `rescan -from` find
    coinbase only (coinbase: only on a regtest chain you run, development
    and tests).
17. **The chain view's "unspent" is trusted.** With no node, transactions
    and proofs from WhatsOnChain are checked, but its word that an output
    is unspent, or that a transaction is not known or not mined, is
    taken as given. A node of your own (`chain asset:<url>`) removes
    that trust. Its free tier allows 3 requests a second: a busy provider
    or gateway needs an API key (`woc_key`) or a node.

See [spec.md](spec.md) section 20 for what the contract itself does not do.

## Where the numbers come from

**Measured** on a test network with blocks about every 30 seconds: the exit
demonstration run twice in a row (a bucket owner, a writer behind one S3
gateway and a reader behind another; a 100 MiB object put through S3 with
the AWS CLI v2, held by three advertisers, one a bridged incumbent; the
race, settlement, anchoring, a verified read, a renewal, a dropped blob
caught at `c = 113`, a delete and `NoSuchKey`) and four drills (a dropped
cold blob, a pass then a failure of one provider, a renewal through the
bridge, a by-reference pass of the incumbent). Transaction sizes and fees
are from the node's own copies (fee from the inputs' values), BEEF sizes
from the writer's records, store sizes from the providers' metrics, times
from the wall clock of each command. On a network mining a block every ten
minutes, every step that waits for a block waits that long instead.

**Contract**: [spec.md](spec.md) sections 7.6, 19.1 and 19.2, and
[frozen.md](frozen.md); its arithmetic is held by the codec's tests and
vectors (`make verify`).

**Set**: every default in the provider's, the gateway's and the command
line's configuration (`provider/config.go`, `internal/s3d`,
`cmd/bstore`) and the host module's environment (`host/src`).
