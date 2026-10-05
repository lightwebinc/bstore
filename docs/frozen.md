# Frozen at first publish

Every item below is hashed into a key or an identifier, written into a
record, named by a host or a provider, or checked by a reader against bytes
already published. Once one record using it is on a public host, readers
verify against it for as long as anyone reads that record, so changing it
afterwards is a new identifier or a new record version, never an edit.

**Status: signed, with twenty-two clarifications pending owner confirmation
(section 14, rows C1 to C22).** Every other row below is signed with the
value it gives, and none is provisional. The rows marked "vectors" are pinned byte for byte by
the golden vectors the codec generates ([spec.md](spec.md) section 22); a
vector that disagrees with a row is a defect in the codec, not a change to
the row. The registry rows of section 1 belong in bcommon's registry
(`docs/registry.md`), where they are checked against every registered row;
they are registered there before anything is published under them, and this
repository does not edit that registry.

Public copy: spec sections 7.7 and 21's reserved tag `st` `0x04` describe a
later output that pays on a challenge proof. No public document describes
them, beyond "reserved", until the owner's patent check clears them.

## 1. Registry entries

Checked against the registered prefixes (`bf`, `gw`, `bl`, `bb`, `bo`, `se`,
`bc`, `vx` for tests, and `bs`, unassigned and never to be assigned): `st` is none of them, and the
registry already reserves it for a storage application. `[1, "bstore"]` is no
registered protocol; `tm_bstore_`, `tm_bstoreblob_` and `ls_bstore` are no
registered names; no basket starts with `bstore`.

| Kind | Value | Use | Status |
| --- | --- | --- | --- |
| Protocol | `[1, "bstore"]` | every bstore derivation (counterparty anyone) and the funding key | SIGNED |
| Key ids | `bucket`, `chain`, `entry`, `piece`, `outcome`, `front`, `hold`, `challenge`, `fund` | spec section 13.1 | SIGNED |
| Tag prefix | `st` (`73 74`) | owned by bstore | SIGNED |
| Tags | `st` `0x01` bucket token; `st` `0x02` funding output; `st` `0x03` hold output; `st` `0x04` reserved, unassigned | spec section 21 | SIGNED |
| Record magics | `stb`, `ste`, `stp`, `sto`, `stf`, `stq`, `str`, each `0x01` | bucket record, entry, piece, outcome, front statement, request, response body | SIGNED |
| Never-assigned tag bytes | `0x62`, `0x65`, `0x66`, `0x6f`, `0x70`, `0x71`, `0x72` (they begin record magics), and `0x00` | spec section 21 | SIGNED |
| Topics | `tm_bstore_<name>_<suffix>` (records), `tm_bstoreblob_<name>_<suffix>` (blobs); `<name>` 1 to 25 bytes, `<suffix>` 10 random lowercase letters fixed at creation; both namespaces bstore's | spec section 3.1 | SIGNED |
| Lookup service | `ls_bstore` | one per host, all buckets | SIGNED |
| Host route | `<base>/ls_bstore/terms` | spec section 11.4 | SIGNED |
| Provider routes | `<front>/challenge`, `/renew`, `/settle`, `/terms`; a derived front ends `/bstore/v1` after the advertisement URL's prefix | spec section 10.1 | SIGNED |
| Baskets | `bstore fund`, `bstore entry funding`, `bstore piece funding`, `bstore outcome funding`, `bstore front funding`, `bstore chain funding`, `bstore chain`, `bstore hold`, `bstore kill tombstone` | wallet baskets | SIGNED |
| Domain string | `bstore bucket id v1` | spec section 3.3 | SIGNED |
| Borrowed, not registered | `[2, "uhrp advertisement"]` key id `1`; `[2, "3241645161d8"]` | BRC-26 as the SDK has it; BRC-29 | SIGNED (as a note in the registry) |
| Certificate types | none | operator independence is the writer's policy (spec section 9.7) | SIGNED |

## 2. Mechanism shared with the substrate

| Item | Value | Status |
| --- | --- | --- |
| Derivation setting | BRC-42, counterparty `anyone`, `forSelf = true` on the writer's side | SIGNED |
| PushDrop layout | lock-before, field signature last, over `SHA-256` of the fields concatenated | SIGNED |
| Carrier non-final device | `nLockTime = 4102444800`, input `nSequence = 0`; hosts accept any `nLockTime >= 4102444800` and any `nSequence` below `0xFFFFFFFF` | SIGNED |
| Carrier shape and refusal order | spec section 12.1: `shape`, `beef`, record rules, `bucket`, `mineable`, `unlock`, `lock`, `signature`, `funding` | SIGNED (vectors) |
| Token shape and refusal order | spec section 12.1: `shape`, `beef`, record rules, `bucket`, `lock`, `signature`, `unmined`, `token-input`, `bucket-id`; admitted whether or not the predecessor is held | SIGNED (vectors) |
| Classifier | spec section 12.1: claiming outputs by push data; funding-shaped `21 <33> ac 03 73 74 02 75`; hold-shaped `21 <33> ac 03 73 74 03 75`; advertisements as the SDK validates them; two claiming outputs refuse (`shape`); a piece is refused (`piece`) | SIGNED (vectors) |
| Mined objects | advertisement and hold-shaped outputs admitted when mined, at most 16 a transaction (`beef`, `unmined`, `too-many`); a sweep is a transaction of at most two outputs whose output 0 is funding-shaped; its input outpoints kept at least 31 days once per host, its transaction only where it matched a held carrier; anything else `not-bstore`, a transaction that only spends held outputs included; settlements and reclaims never published | SIGNED (vectors) |
| BEEF ceilings | entry, outcome and front carriers 81,920 bytes; tokens and sweeps 196,608; other mined objects 65,536; piece carriers 4,259,840; a host's own bound at least 262,144 | SIGNED |
| Admission | a verdict depends only on the submission's bytes and the host's headers; the engine's previous coins decide retention only, never acceptance; never on another topic, on what the topic holds, or on arrival order; an allow-list is static operator configuration, never computed from any topic; a topic manager returns instructions only for what it admits and raises for every refusal | SIGNED |
| Hosts hold no bytes | no host admits a piece; no host fetches anything a record names; no broadcaster | SIGNED |
| Coin retention | every accepted transaction retains every held output it spends | SIGNED |
| Joining | by replay; no unordered synchronisation of unspent outputs on `tm_bstore_` topics | SIGNED |
| Commitment | `C = txid(K)`, hash byte order in records, display order in JSON | SIGNED |
| CBOR | RFC 8949 section 4.2.1, bcommon's subset, enforced on decode; invalid UTF-8 refused at `cbor` | SIGNED |
| Funding trees | at most 1023 funding outputs; any change outputs; sized by the writer so every carrier fits its ceiling | SIGNED |

## 3. Buckets

| Item | Value | Status |
| --- | --- | --- |
| Bucket identifier | `<name>_<suffix>`, 12 to 36 bytes: `<name>` 1 to 25 bytes of `[a-z]` and single underscores, a letter at each end; `<suffix>` 10 letters drawn at random once | SIGNED |
| Bucket record keys | 0 `magic`, 1 `bucket`, 2 `owner`, 3 `id`, 4 `seq`, 5 `prev`, 6 `writers`, 7 `changes`, 8 `count`, 9 `mapRoot`, 10 `created`, 11 `consumed`, all required; 12 and above unknown | SIGNED |
| Bucket record bounds | 65,536 bytes encoded, which governs; 16 writers; 1024 changes; 1024 consumed; `count` at most 1,048,576 | SIGNED |
| Refusal order | spec section 3.2, ending in `first` | SIGNED (vectors) |
| Bucket id | `SHA-256("bstore bucket id v1" \|\| txid \|\| uint32be(vout))` of the outpoint the first token spends | SIGNED (vectors) |
| Chain identity | a chain is `(owner, id)`; questions, served configuration and reader memory name both; a token of another owner never displaces the owner's in any cap | SIGNED |
| The map | changes applied in order: a put `[key, C]` sets, a removal `[key, Z, D]` (`D` the delete entry's commitment) removes; `consumed` lists the commitments applied without a change (superseded in the batch, or no-ops); a token names an entry by its commitment in `changes` or `consumed`; every change must change the map and no commitment is named twice in a chain (`map-mismatch`); tooling never applies an entry any token named, resolves one key's batch by `created` then commitment, and moves no-ops to `consumed` | SIGNED (vectors) |
| Map root | RFC 6962 Merkle Tree Hash over `uint16be(len(key)) \|\| key \|\| C`, ascending by key bytes; empty map `SHA-256("")` | SIGNED (vectors) |
| Token statuses | `pending`, `map-mismatch` (sticky), `consistent`; only rooted, consistent tokens answered as latest | SIGNED |
| Writers | the owner and up to 16 keys a token names; a reader refuses an entry by anyone else, checked against the latest token (`writer`); tooling never removes a writer while the map names its entries | SIGNED |

## 4. The object entry

| Item | Value | Status |
| --- | --- | --- |
| Keys | 0 `magic`, 1 `bucket`, 2 `id`, 3 `writer`, 4 `key`, 5 `kind`, 6 `created` for every kind; 7 `size`, 8 `root`, 9 `contentHash`, 10 `proofRoot`, 11 `proofProfile`, 12 `contentType` for a put; 13 `etag`, 14 `meta`, 15 `hold` optional for a put; 16 and above unknown | SIGNED |
| Kinds | 1 put, 2 delete; 3 to 255 admitted as unknown, unsupported for readers | SIGNED |
| Bounds | record 8,192 bytes; key 1 to 1024 bytes, one line; `size` up to 2^43; `contentType` 3 to 127 bytes, lowercase; `etag` 1 to 64; `meta` 32 pairs, 2,048 bytes | SIGNED |
| The empty object | `size` 0 with the CHIRP empty root, `contentHash` and `proofRoot` `SHA-256("")`, no hold | SIGNED (vectors) |
| Refusal order | spec section 4.2, ending in `kind`, `empty` | SIGNED (vectors) |
| Hold terms | `[txid, vout, satoshis, t, K, retention, deadline, checkC]`, bounds of spec section 4.3 | SIGNED |
| Anchored and pending | the latest token's map decides; pending entries are claims any reader may ask for (`pending`: only those no token names; no clock decides it), and the writing gateway serves its own at once; anchoring tooling MUST, and pending readers SHOULD, check an entry's funding outpoint unspent by positive evidence | SIGNED |

## 5. Pieces

| Item | Value | Status |
| --- | --- | --- |
| Keys | 0 `magic`, 1 `bucket`, 2 `writer`, 3 `root`, 4 `piece`; 5 and above unknown | SIGNED |
| Bounds | `piece` 1 to 4,194,304 bytes; record 4,194,560 | SIGNED |
| Closure | CHIRP Profile 1 (BRC-167) unchanged; one piece per distinct object of the closure; a bstore root carries no extension | SIGNED |
| The blob topic | pieces only, never admitted by a host | SIGNED |

## 6. The proof tree

| Item | Value | Status |
| --- | --- | --- |
| Proof profile | 1 | SIGNED |
| Segments | 4,096 bytes from offset 0; the last holds the remainder, unpadded | SIGNED |
| Hashes | leaf `SHA-256(0x00 \|\| segment)`, node `SHA-256(0x01 \|\| left \|\| right)`, single SHA-256 | SIGNED (vectors) |
| Shape | RFC 6962 Merkle Tree Hash over the segments in order (equivalently: per-blob subtrees, odd node carried up, then the same over blob roots); empty object `SHA-256("")` | SIGNED (vectors) |
| Path | RFC 6962 audit path, leaf to root; directions and length from the index and the segment count, never sent | SIGNED (vectors) |

## 7. Challenges

| Item | Value | Status |
| --- | --- | --- |
| Request keys | 0 `magic`, 1 `root`, 2 `proofRoot`, 3 `size`, 4 `provider`, 5 `answerer`, 6 `seedHeight`, 7 `seedHash`, 8 `nonce` (16 bytes), 9 `c`, 10 `range` optional; no other key; at most 512 bytes | SIGNED (vectors) |
| `c` | 1 to 1024 | SIGNED |
| Index | `SHA-256(seedHash \|\| proofRoot \|\| provider \|\| nonce \|\| uint32le(k)) mod m`, the digest read little-endian; with `range`, offset by `1024 * first`; with replacement | SIGNED (vectors) |
| Freshness | `tip - 6 <= seedHeight <= tip`, checked by the provider when it answers and by the buyer when it sends; at verification, `seedHash` held at `seedHeight` on any branch the verifier has seen | SIGNED |
| Answerer's refusals | 422 `seed`, `request`, `unknown-root` (a `proofRoot` or `size` not its own computation for `root`) | SIGNED |
| Response body | 0 `magic`, 1 `request` (its hash), 2 `samples` (`c` entries `[segment or null, siblings]`, every segment `null` when the answerer is not the provider), 3 `answerer`; no other key | SIGNED (vectors) |
| Signed response | CBOR `[body, signature]`; signature over `SHA-256(request) \|\| SHA-256(body)` under `derive(answerer, "challenge")`; at most 8,388,608 bytes | SIGNED (vectors) |
| Buyer's check order | steps 1 to 3 once (`signature`, `request`, `count` including every path's length), then per sample in `k` order `fetch`, `segment`, `path`; failing `signature` is no-answer; `request`, `count`, or an inline sample's failure invalid; a fetched sample's failure rechecked from the whole blob: blob verifies and segment then passes, pass; blob verifies and segment still fails, invalid; blob fails, unretrievable; outcomes the answerer's; a reorged seed accepted on any branch seen | SIGNED (vectors) |
| By-reference samples | fetched by the buyer from the tested URL (the provider's live advertisement for the root with the latest expiry, ties to the lower txid), blob route, with `Range`, or the whole blob | SIGNED |
| The settlement challenge | the writer uses `min(checkC, freeC)` per candidate; not priced for the entry's writer, before `deadline`, when the provider's own bounty checks pass and the bounty meets its minimum, `c` at most `min(checkC, freeC)` with `freeC` at least 30, at most two per hold outpoint and advertiser, the hold output at least the provider's published `minAge` blocks deep (0 to 1008; `tip - h + 1`, so 0 and 1 both mean mined) | SIGNED |
| Default `c` | 30 for a paid challenge and as the recommended `checkC`; policy, not frozen | (not frozen) |

## 8. Outcomes and front statements

| Item | Value | Status |
| --- | --- | --- |
| Outcome keys | 0 `magic`, 1 `bucket`, 2 `buyer`, 3 `created`, 4 `request`, 5 `outcome` (1 pass, 2 invalid, 3 no-answer, 4 unretrievable), 6 `response` and 7 `signature` for 1 and 2; 8 and above unknown; 4,096 bytes | SIGNED |
| Outcome admission | the answerer's signature checked at admission for outcomes 1 and 2 (`response-signature`) | SIGNED (vectors) |
| Outcome attribution | to the answerer; to the provider only when they are one key; unretrievable to the provider's URL | SIGNED |
| Front statement keys | 0 `magic`, 1 `bucket`, 2 `front`, 3 `advertiser`, 4 `url`, 5 `created`; 6 and above unknown; 2,048 bytes | SIGNED |

## 9. Holding and settlement

| Item | Value | Status |
| --- | --- | --- |
| Advertisements | BRC-26 exactly as the SDK validates it (its signature canonical DER, any S); CHIRP URL form for a root; never the anyone key | SIGNED |
| Hold output | `<derive(writer, "hold")> OP_CHECKSIG <73 74 03> OP_DROP`, mined, value the bounty | SIGNED |
| Eligibility | spec section 9.3 | SIGNED |
| Ranking | first sight at the writer's client, ties to the lower advertisement txid; candidates must pass a settlement challenge; the race closes at `K` passed, the writer's window, or `deadline`; settled only if `t` passed; payees' advertisements unspent by positive evidence, a renewal successor counting as unspent and keeping the original's first sight; a cancelled payee dropped, ranks below moved up, `t` counted again | SIGNED (vectors) |
| Payouts | BRC-178's square-root weights in binary64, floors, remainder to rank 1, no zero output; payouts sum to the hold output's value | SIGNED (vectors) |
| Settlement derivation | BRC-29 to the advertiser's key; prefix `base64(C)`, suffix `base64(uint16be(rank))` | SIGNED (vectors) |
| Payee | always the advertisement's identity key | SIGNED |
| Remittance | unicast to the payee's `settle` route, naming the settled entry; recoverable from public data; the settlement never on the plane; one hold outpoint settles one entry | SIGNED |

## 10. Names and questions

| Item | Value | Status |
| --- | --- | --- |
| Classes | free `bucket`, `bucket-seq`, `object`, `list`, `pending`, `unanchored`, `entry`, `ads`, `ad`, `fronts`, `front`, `hold`, `outcomes`, `outcomes-of`, `outcomes-by`, `outcomes-of-by`, `sweep`; priceable `history`, `chain`, `export`, `outcome-history`, `outcome-history-of`; chain classes name `(owner, id)`; map classes for served chains only; no priced class a superset of a free class | SIGNED |
| Query values | every member a string; the value rules of spec section 11.2; an unknown member, a missing member or an unknown bucket refused | SIGNED |
| Contexts | an entry's `[index, path]`; the leading token's array of at most two leaves `[role, index, key, C, path]` (0 before, 1 after, 2 unheld); pages stop at an unheld leaf and always complete; the reader's page checks of spec section 11.3 (role 0 strictly before the range: at or before `a`, or before `p`; role 1 strictly after it: after `a`, after `p`, not starting with `p`); page maxima 256 (`list`) and 512 (`export`) as the checks use them; 1,024 and 4,096 bytes | SIGNED (vectors) |
| Order | `list` and `export` by key bytes; `ads`, `unanchored` and `fronts` ascending txid (display order) after a txid cursor; `pending`, `outcomes`, `outcomes-of`, `outcome-history`, `outcome-history-of` newest `created` first; `history` by token, newest first; never dated after the host's time | SIGNED |
| Free window | `outcomes` within 31 days of admission | SIGNED |
| Terms and payment | `<base>/ls_bstore/terms`; the 402 authoritative; payment output 0 of the BRC-105 transaction, P2PKH to the BRC-29 key | SIGNED |
| Retention floors | spec section 12.3 | SIGNED (floors: a later version may lengthen them, never shorten) |

## 11. The S3 contract and the backend seam

| Item | Value | Status |
| --- | --- | --- |
| S3 bucket names | the identifier with `_` replaced by `-`; the gateway names `(owner, id)` per S3 bucket | SIGNED |
| Authentication | SigV4 gateway mode only, one identity per access key | SIGNED |
| ETag | the entry's `etag` (MD5, or S3's multipart form), else `"<root hex>-1"` | SIGNED |
| Headers | `x-amz-checksum-sha256` the content hash (`FULL_OBJECT`); `x-amz-meta-bstore-root`, `bstore-` metadata keys reserved; `Content-Type` lowercased without parameters, default `application/octet-stream`; `HeadObject` never asks a provider; `sthree-` names refused | SIGNED |
| Writes | `Content-MD5` checked (`BadDigest`); `If-None-Match` and `If-Match` refused with `501 NotImplemented`; a multipart upload belongs to one gateway; `CopyObject` keeps `contentType`, `etag` and `meta` (`COPY`) or takes the request's (`REPLACE`), republishing pieces across buckets | SIGNED |
| Not found | `NoSuchKey` only on a proven absence, or at the writing gateway on its own newer pending delete; `503 SlowDown` when no host answers or the entry is unheld | SIGNED |
| Provider route bodies | the JSON bodies of `challenge`, `renew`, `settle` and `terms` (spec section 10.1); BRC-105 on every priced route, BRC-121 optional | SIGNED |
| Hold-backend interface | `Put`, `Has`, `Get(hash, offset, length)`, `ServeURL`, `Advertise`, as spec section 17 | SIGNED |

## 12. Bounds that are a floor

Readers accept every record within these, and a later version may raise
them but never lower them: bucket record 65,536 bytes, 16 writers, 1024
changes, 1024 consumed, 1,048,576 keys; entry 8,192 bytes, key 1,024 bytes, `meta` 2,048
bytes; object 2^43 bytes; piece 4,194,304 bytes; outcome 4,096 and front
statement 2,048 bytes; request 512 bytes, `c` 1024, signed response
8,388,608 bytes; contexts 1,024 and 4,096 bytes; `t` and `K` 16, `checkC`
1024; a host's BEEF bound at least 262,144 bytes and its sweep input bound at
least 1024. As ceilings, not floors: the BEEF ceilings of section 2.

## 13. Not frozen

- Funding tree sizes and output values; fee sources.
- The owner's anchoring cadence, and which entries it anchors.
- Provider policy: which buckets, objects and bounties it takes, its prices,
  its storage layout (per-blob roots or every segment hash), how long it
  keeps unclaimed pieces, its deadline for answering.
- The writer's policy: bounty sizes, `t`, `K`, `checkC`, retention,
  deadlines, its race window, which operators it treats as independent.
- Host policy: retention beyond the floors, allow-lists, rate limits,
  prices, the handshake budget, metric names.
- The answer limits other than the `list` and `export` page maxima, which a later version may raise.
- The buyer's `c` beyond the default and its timeout.
- Gateway policy: which buckets and identities it serves, its default
  bounty, part buffering.

## 14. Clarifications

A clarification rewords a signed rule where the signed wording leaves a
case open. Each below is pending the owner's confirmation; the codec and its
vectors (C1 to C7), the host module (C8), the provider (C9 to C11), the S3
gateway (C13 to C16) and the command line (C17 to C22) already follow it.
C12 rewords how the contract describes the challenge seed, and binds a later
on-chain form.

| Row | Item | Clarification | Status |
| --- | --- | --- | --- |
| C1 | Closure, spec section 5 | A root node of CHIRP version 1 with a minor version above 0 is refused (`root`): bstore reads version 1.0 alone, as bcommon's decoder does, and never reads a later minor version as 1.0 | PENDING owner confirmation |
| C2 | Closure, spec section 5 | bstore checks that a root carries no extension itself: a root with any extension (`len(Root.Extensions) != 0`), advisory or not, is refused (`root`), because bcommon's decoder keeps an advisory extension rather than refusing it | PENDING owner confirmation |
| C3 | Closure, spec section 5 | The depth bound of a closure (16, bcommon `chirp.MaxDepth`) counts the objects on the path from the root to an object, both included: a blob under fourteen branches is sixteen objects deep and within the bound; under fifteen it is seventeen and refused (`root`) | PENDING owner confirmation |
| C4 | Closure, spec sections 5, 9.3 and 15.2 | A closure is checked by bcommon `chirp.Verify` with `Limits{MaxLength: 1<<43}` and the default bound on references, in its refusal order: missing, hash, the node checks and the length bound for the root; then, depth first in child order, the references bound, depth, cycle and missing for each reference, a branch's hash, node checks and length, a blob's length then hash; then content-hash; then canonical. A fetch takes only bytes that hash to the object asked for. An object not served, or served only as other bytes, the root included, is `closure` (spec section 15.2 step 6's "the root node's bytes do not hash to `root`" is read as step 7's "no advertiser served it": bytes an advertiser serves for the root that hash to something else are not the root); content-hash is `content`; every other refusal, and every rule of rows C1 and C2, is `root` | PENDING owner confirmation |
| C5 | Vectors, spec section 22 | The `chirp-v1.json` of section 22 is bcommon's file of that name at the tag go.mod pins, read from that module by bstore's tests; bstore's own vectors of its closure checks are `closure-v1.json`, so that no two vector files of one name differ | PENDING owner confirmation |
| C6 | Challenge request, spec section 7.1 | `seedHeight` is at most 2^32 - 1, the range of a block height in a BUMP (BRC-74); a request with a larger one is refused (`request`) | PENDING owner confirmation |
| C7 | Front statement, spec section 8.2 | The url's authority, from after `https://` to the first `/`, is not empty; `https:///x` is refused (`url`) | PENDING owner confirmation |
| C8 | The index, spec section 12.2 | The latest tokens of a chain are those of the branch on the current chain: walking down from the highest `seq` that holds a rooted (and, for a served chain, consistent) token, the tokens at the first `seq` where any token's own proof verifies in the host's headers, and among them the lowest txid where one answer needs one token. A longer branch that a reorganisation orphaned is not answered as latest, and retention follows the branch on the chain. When no token verifies within 64 `seq`s of the highest (the header source does not answer), the tokens at the highest `seq` | PENDING owner confirmation |
| C9 | Unicast delivery, spec sections 9.3 and 14 | A provider advertises only for an entry it takes, so a BRC-167 upload-session commit checks the closure and stages it, answering `202` until an entry naming the root reaches the provider and it takes it, and `201` with the advertised location after; a writer that unicasts submits its entry to each provider's submit route as it does to a host | PENDING owner confirmation |
| C10 | The settlement challenge, spec section 9.4 | "At most two per hold outpoint and advertiser" counts the free answers given: a signed response to a request answered free. A request refused (`422`) or not answered counts nothing, and a challenge paid for is not one of the two | PENDING owner confirmation |
| C11 | Answerer's refusals, spec sections 7.2 and 10.1 | An answerer refuses with `422` `request` a request whose `answerer` is another key, or, answering for itself, whose `provider` is another key: it signs only as itself, and only for advertisers it answers for | PENDING owner confirmation |
| C12 | Challenge seed, spec sections 1 (introduction) and 7.2 | The seed of a challenge's indices is a recent block's hash, the buyer's nonce and the provider's key. The buyer draws its nonce after the block exists, so the buyer can try nonces against a known block and keep one whose samples it likes; the provider controls none of the seed. "A seed no single party controls" is read as "a seed the provider does not control", and section 7.2's "Neither party picks the seed alone" the same way. A later on-chain form (section 7.7) MUST NOT let the prover supply the nonce, or choose among nonces, since the prover would then choose which segments it is asked for | PENDING owner confirmation |
| C13 | Checksum header, spec section 16.3 | `x-amz-checksum-sha256` and `x-amz-checksum-type` describe the whole object: `HeadObject` and a whole `GetObject` carry them; a ranged answer (`206`) carries neither, since a client that checks them would hold a partial body to the whole object's hash | PENDING owner confirmation |
| C14 | Listing cursor, spec section 16.2 | With a `Delimiter`, a page may end on a common prefix. Its `ContinuationToken` is then the base64 of the greatest object key under that prefix (a version 1 listing's `NextMarker` is that key itself): the prefix padded to 1,024 bytes with the greatest code points that fit (U+10FFFF, then U+FFFF, U+07FF or `~` for the last one to three bytes), so the next page resumes after every key the prefix folded. Without a common prefix it is the base64 of the last key, as written | PENDING owner confirmation |
| C15 | A reader's walk, spec sections 15.1 and 15.2 step 1 | A host answers a token stored alone with its proof, without its parent, so a reader checks `bucket-id` at `seq` 0 by finding the first token's input whose outpoint's bucket id (section 3.3) is the token's `id`; a first token with no such input fails step 1 (`bucket`). Whether the output that input spends is locked to `derive(owner, "chain")` is admission's `token-input`, which a reader without the parent does not repeat; the outpoint is unique, so the id still names one first token | PENDING owner confirmation |
| C16 | Not found, spec section 16.3 | When no host answers anything that verifies, the writing gateway's own newest pending entry for a key still answers a read if it is a put (its own write, read at once); if it is a delete, the answer is `503 SlowDown`, not `NoSuchKey`, because `NoSuchKey` needs a proven absence or an anchored entry the delete is newer than | PENDING owner confirmation |
| C17 | Deadline, spec sections 4.3, 9.5 and 9.6 | A writer settles only while its tip is below `deadline`, so the settlement mines at or before `deadline`; it reclaims only once its tip has reached `deadline`, so the reclaim mines after it. At a tip equal to `deadline` the bounty has lapsed. A hold output with a settlement recorded is never reclaimed, and a reclaimed one is never settled | PENDING owner confirmation |
| C18 | First sight, spec sections 9.3 and 9.4 | The writer's client takes first sight per look at the hosts: the eligible advertisements first seen in one look share that look's time and tie, the lower advertisement txid (display order) ranking first. A renewal successor keeps its original's sight, and the advertisement checked unspent before settlement is the one first seen, followed through its renewal successors | PENDING owner confirmation |
| C19 | The settlement challenge, spec section 9.4 | When a candidate's terms cannot be read, the writer challenges it with `min(checkC, 30)` samples, 30 being the least `freeC` a provider may publish. A candidate that would price its answer (its `minBounty` above the bounty, the hold output not yet `minAge` deep, its two free answers given) is neither passed nor failed: the writer waits, or pays as its own policy allows | PENDING owner confirmation |
| C20 | Exit codes, spec section 15.4 | A key the map proves absent, asked for by name, exits 1 (`NoSuchKey`): what was asked for is proven not to be there. A key whose entry no host produces stays 3 | PENDING owner confirmation |
| C21 | Anchoring, spec sections 3.4, 3.5 and 4.4 | The tooling walks the unanchored entries of the owner and of the writers the token it mints names (a writer it adds included, against which a reader then checks that token's entries). It splits a batch over tokens so that one token carries each key's change, the key's superseded entries going to that token's `consumed` or to tokens of `consumed` alone before it. It mints only on a latest token whose output the spend source shows unspent | PENDING owner confirmation |
| C22 | Remittance, spec sections 9.5 and 10.1 | The `settle` remittance carries the settlement as Atomic BEEF with its proof: the writer sends it once the settlement has mined, and a payee takes it only once it verifies against the payee's headers | PENDING owner confirmation |
