# Frozen at first publish

Every item below is hashed into a key or an identifier, written into a
record, named by a host or a provider, or checked by a reader against bytes
already published. Once one record using it is on a public host, readers
verify against it for as long as anyone reads that record, so changing it
afterwards is a new identifier or a new record version, never an edit.

**Status: draft, nothing signed.** Every row is PROVISIONAL and gives the
recommended value. The rows marked "vectors" will be pinned byte for byte by
the golden vectors the codec generates ([spec.md](spec.md) section 22). The
registry rows of section 1 belong in bcommon's registry
(`docs/registry.md`); they are proposed here and registered there in their
own change, and this repository does not edit that registry.

## 1. Registry entries

Checked against the registered prefixes (`bf`, `gw`, `bl`, `bb`, `bo`, `se`,
`bc`, `vx` for tests, and `bs`, retired): `st` is none of them, and the
registry already reserves it for a storage application. `[1, "bstore"]` is no
registered protocol; `tm_bstore_`, `tm_bstoreblob_` and `ls_bstore` are no
registered names; no basket starts with `bstore`.

| Kind | Value | Use | Status |
| --- | --- | --- | --- |
| Protocol | `[1, "bstore"]` | every bstore derivation (counterparty anyone) and the funding key | PROVISIONAL |
| Key ids | `bucket`, `chain`, `entry`, `piece`, `outcome`, `front`, `hold`, `challenge`, `fund` | spec section 13.1 | PROVISIONAL |
| Tag prefix | `st` (`73 74`) | owned by bstore | PROVISIONAL |
| Tags | `st` `0x01` bucket token; `st` `0x02` funding output; `st` `0x03` hold output; `st` `0x04` reserved, unassigned | spec section 21 | PROVISIONAL |
| Record magics | `stb`, `ste`, `stp`, `sto`, `stf`, `stq`, `str`, each `0x01` | bucket record, entry, piece, outcome, front statement, request, response body | PROVISIONAL |
| Never-assigned tag bytes | `0x62`, `0x65`, `0x66`, `0x6f`, `0x70`, `0x71`, `0x72`, and `0x00` | they begin record magics | PROVISIONAL |
| Topics | `tm_bstore_<name>_<suffix>` (records), `tm_bstoreblob_<name>_<suffix>` (blobs); `<name>` 1 to 25 bytes, `<suffix>` 10 random lowercase letters fixed at creation; both namespaces bstore's | spec section 3.1 | PROVISIONAL |
| Lookup service | `ls_bstore` | one per host, all buckets | PROVISIONAL |
| Host route | `<base>/ls_bstore/terms` | spec section 11.4 | PROVISIONAL |
| Provider routes | `<front>/challenge`, `/renew`, `/settle`, `/terms`; a derived front ends `/bstore/v1` after the advertisement URL's prefix | spec section 10.1 | PROVISIONAL |
| Baskets | `bstore fund`, `bstore entry funding`, `bstore piece funding`, `bstore outcome funding`, `bstore front funding`, `bstore chain funding`, `bstore chain`, `bstore hold`, `bstore kill tombstone` | wallet baskets | PROVISIONAL |
| Domain string | `bstore bucket id v1` | spec section 3.3 | PROVISIONAL |
| Borrowed, not registered | `[2, "uhrp advertisement"]` key id `1`; `[2, "3241645161d8"]` | BRC-26 as the SDK has it; BRC-29 | PROVISIONAL (as a note in the registry) |
| Certificate types | none | operator independence is the writer's policy (spec section 9.7) | PROVISIONAL |

## 2. Mechanism shared with the substrate

| Item | Value | Status |
| --- | --- | --- |
| Derivation setting | BRC-42, counterparty `anyone`, `forSelf = true` on the writer's side | PROVISIONAL |
| PushDrop layout | lock-before, field signature last, over `SHA-256` of the fields concatenated | PROVISIONAL |
| Carrier non-final device | `nLockTime = 4102444800`, input `nSequence = 0`; hosts accept any `nLockTime >= 4102444800` and any `nSequence` below `0xFFFFFFFF` | PROVISIONAL |
| Carrier shape and refusal order | spec section 12.1: `shape`, `beef`, record rules, `bucket`, `mineable`, `unlock`, `lock`, `signature`, `funding` | PROVISIONAL (vectors) |
| Token shape and refusal order | spec section 12.1: `shape`, `beef`, record rules, `bucket`, `lock`, `signature`, `unmined`, `token-input`, `bucket-id`; admitted whether or not the predecessor is held | PROVISIONAL (vectors) |
| Classifier | spec section 12.1: claiming outputs by push data; funding-shaped `21 <33> ac 03 73 74 02 75`; hold-shaped `21 <33> ac 03 73 74 03 75`; advertisements as the SDK validates them; two claiming outputs refuse (`shape`); a piece is refused (`piece`) | PROVISIONAL (vectors) |
| Mined objects | advertisement and hold-shaped outputs admitted when mined, at most 16 a transaction; sweeps as bcommon `carrier.Sweep`; a spend of held outputs accepted, admitting nothing, retaining them; anything else `not-bstore` | PROVISIONAL (vectors) |
| BEEF ceilings | entry, outcome and front carriers 81,920 bytes; tokens and sweeps 196,608; other mined objects 65,536; piece carriers 4,259,840; a host's own bound at least 262,144 | PROVISIONAL |
| Admission | a verdict depends only on the submission's bytes, the host's headers and, for retention only, the engine's previous coins; never on another topic, on what the topic holds, or on arrival order; an allow-list is static operator configuration, never computed from any topic; a topic manager returns instructions only for what it admits and raises for every refusal | PROVISIONAL |
| Hosts hold no bytes | no host admits a piece; no host fetches anything a record names; no broadcaster | PROVISIONAL |
| Coin retention | every accepted transaction retains every held output it spends | PROVISIONAL |
| Joining | by replay; no unordered synchronisation of unspent outputs on `tm_bstore_` topics | PROVISIONAL |
| Commitment | `C = txid(K)`, hash byte order in records, display order in JSON | PROVISIONAL |
| CBOR | RFC 8949 section 4.2.1, bcommon's subset, enforced on decode; invalid UTF-8 refused at `cbor` | PROVISIONAL |
| Funding trees | at most 1023 funding outputs; any change outputs; sized by the writer so every carrier fits its ceiling | PROVISIONAL |

## 3. Buckets

| Item | Value | Status |
| --- | --- | --- |
| Bucket identifier | `<name>_<suffix>`, 12 to 36 bytes: `<name>` 1 to 25 bytes of `[a-z]` and single underscores, a letter at each end; `<suffix>` 10 letters drawn at random once | PROVISIONAL |
| Bucket record keys | 0 `magic`, 1 `bucket`, 2 `owner`, 3 `id`, 4 `seq`, 5 `prev`, 6 `writers`, 7 `changes`, 8 `count`, 9 `mapRoot`, 10 `created`, all required; 11 and above unknown | PROVISIONAL |
| Bucket record bounds | 65,536 bytes encoded, which governs; 16 writers; 1024 changes; `count` at most 1,048,576 | PROVISIONAL |
| Refusal order | spec section 3.2, ending in `first` | PROVISIONAL (vectors) |
| Bucket id | `SHA-256("bstore bucket id v1" \|\| txid \|\| uint32be(vout))` of the outpoint the first token spends | PROVISIONAL (vectors) |
| The map | changes applied in order; a zero `C` removes; every change must change the map | PROVISIONAL |
| Map root | RFC 6962 Merkle Tree Hash over `uint16be(len(key)) \|\| key \|\| C`, ascending by key bytes; empty map `SHA-256("")` | PROVISIONAL (vectors) |
| Token statuses | `pending`, `map-mismatch` (sticky), `consistent`; only rooted, consistent tokens answered as latest | PROVISIONAL |
| Writers | the owner and up to 16 keys a token names; a reader refuses an anchored entry by anyone else (`writer`) | PROVISIONAL |

## 4. The object entry

| Item | Value | Status |
| --- | --- | --- |
| Keys | 0 `magic`, 1 `bucket`, 2 `id`, 3 `writer`, 4 `key`, 5 `kind`, 6 `created` for every kind; 7 `size`, 8 `root`, 9 `contentHash`, 10 `proofRoot`, 11 `proofProfile`, 12 `contentType` for a put; 13 `etag`, 14 `meta`, 15 `hold` optional for a put; 16 and above unknown | PROVISIONAL |
| Kinds | 1 put, 2 delete; 3 to 255 admitted as unknown, unsupported for readers | PROVISIONAL |
| Bounds | record 8,192 bytes; key 1 to 1024 bytes, one line; `size` up to 2^43; `contentType` 3 to 127 bytes; `etag` 1 to 64; `meta` 32 pairs, 2,048 bytes | PROVISIONAL |
| The empty object | `size` 0 with the CHIRP empty root, `contentHash` and `proofRoot` `SHA-256("")`, no hold | PROVISIONAL (vectors) |
| Refusal order | spec section 4.2, ending in `kind`, `empty` | PROVISIONAL (vectors) |
| Hold terms | `[txid, vout, satoshis, t, K, retention, deadline, checkC]`, bounds of spec section 4.3 | PROVISIONAL |
| Anchored and pending | the latest token's map decides; pending entries are claims, served at once by the writing gateway only | PROVISIONAL |

## 5. Pieces

| Item | Value | Status |
| --- | --- | --- |
| Keys | 0 `magic`, 1 `bucket`, 2 `writer`, 3 `root`, 4 `piece`; 5 and above unknown | PROVISIONAL |
| Bounds | `piece` 1 to 4,194,304 bytes; record 4,194,560 | PROVISIONAL |
| Closure | CHIRP Profile 1 (BRC-167) unchanged; one piece per distinct object of the closure | PROVISIONAL |
| The blob topic | pieces only, never admitted by a host | PROVISIONAL |

## 6. The proof tree

| Item | Value | Status |
| --- | --- | --- |
| Proof profile | 1 | PROVISIONAL |
| Segments | 4,096 bytes from offset 0; the last holds the remainder, unpadded | PROVISIONAL |
| Hashes | leaf `SHA-256(0x00 \|\| segment)`, node `SHA-256(0x01 \|\| left \|\| right)`, single SHA-256 | PROVISIONAL (vectors) |
| Shape | RFC 6962 Merkle Tree Hash over the segments in order (equivalently: per-blob subtrees, odd node carried up, then the same over blob roots); empty object `SHA-256("")` | PROVISIONAL (vectors) |
| Path | RFC 6962 audit path, leaf to root; directions and length from the index and the segment count, never sent | PROVISIONAL (vectors) |

## 7. Challenges

| Item | Value | Status |
| --- | --- | --- |
| Request keys | 0 `magic`, 1 `root`, 2 `proofRoot`, 3 `size`, 4 `provider`, 5 `answerer`, 6 `seedHeight`, 7 `seedHash`, 8 `nonce` (16 bytes), 9 `c`, 10 `range` optional; no other key; at most 512 bytes | PROVISIONAL (vectors) |
| `c` | 1 to 1024 | PROVISIONAL |
| Index | `SHA-256(seedHash \|\| proofRoot \|\| provider \|\| nonce \|\| uint32le(k)) mod m`, big-endian; with `range`, offset by `1024 * first`; with replacement | PROVISIONAL (vectors) |
| Freshness | `tip - 6 <= seedHeight <= tip`, checked by the provider and the buyer, each with its own headers | PROVISIONAL |
| Response body | 0 `magic`, 1 `request` (its hash), 2 `samples` (`c` entries `[segment or null, siblings]`), 3 `answerer`; no other key | PROVISIONAL (vectors) |
| Signed response | CBOR `[body, signature]`; signature over `SHA-256(request) \|\| SHA-256(body)` under `derive(answerer, "challenge")`; at most 8,388,608 bytes | PROVISIONAL (vectors) |
| Buyer's check order | `signature`, `request`, `count`, `segment`, `path`, `fetch`; failing `signature` is no-answer, any later step invalid | PROVISIONAL (vectors) |
| By-reference samples | fetched by the buyer from the advertised blob route with `Range`, or the whole blob | PROVISIONAL |
| The settlement challenge | free to the entry's writer for an entry with hold terms, `c` at most `checkC`, until `deadline`, at most two per entry and advertiser | PROVISIONAL |
| Default `c` | 30 for a paid challenge and as the recommended `checkC`; policy, not frozen | (not frozen) |

## 8. Outcomes and front statements

| Item | Value | Status |
| --- | --- | --- |
| Outcome keys | 0 `magic`, 1 `bucket`, 2 `buyer`, 3 `created`, 4 `request`, 5 `outcome` (1 pass, 2 invalid, 3 no-answer), 6 `response` and 7 `signature` for 1 and 2; 8 and above unknown; 4,096 bytes | PROVISIONAL |
| Outcome admission | the answerer's signature checked at admission for outcomes 1 and 2 (`response-signature`) | PROVISIONAL (vectors) |
| Front statement keys | 0 `magic`, 1 `bucket`, 2 `front`, 3 `advertiser`, 4 `url`, 5 `created`; 6 and above unknown; 2,048 bytes | PROVISIONAL |

## 9. Holding and settlement

| Item | Value | Status |
| --- | --- | --- |
| Advertisements | BRC-26 exactly as the SDK validates it; CHIRP URL form for a root | PROVISIONAL |
| Hold output | `<derive(writer, "hold")> OP_CHECKSIG <73 74 03> OP_DROP`, mined, value the bounty | PROVISIONAL |
| Eligibility | spec section 9.3 | PROVISIONAL |
| Ranking | first sight at the writer's client, ties to the lower advertisement txid; candidates must pass a settlement challenge | PROVISIONAL |
| Payouts | BRC-178's square-root weights in binary64, floors, remainder to rank 1, no zero output; payouts sum to the hold output's value | PROVISIONAL (vectors) |
| Settlement derivation | BRC-29 to the advertiser's key; prefix `base64(C)`, suffix `base64(uint16be(rank))` | PROVISIONAL (vectors) |
| Payee | always the advertisement's identity key | PROVISIONAL |
| Remittance | unicast to the payee's `settle` route; recoverable from public data | PROVISIONAL |

## 10. Names and questions

| Item | Value | Status |
| --- | --- | --- |
| Classes | free `bucket`, `bucket-seq`, `object`, `list`, `pending`, `entry`, `ads`, `fronts`, `hold`, `outcomes`, `sweep`; priceable `history`, `chain`, `export`, `outcome-history`; no priced class a superset of a free class | PROVISIONAL |
| Query values | every member a string; the value rules of spec section 11.2; an unknown member, a missing member or an unknown bucket refused | PROVISIONAL |
| Contexts | an entry's `[index, path]`; the leading token's `[before, after]` neighbours `[index, key, C, path]`; 1,024 and 4,096 bytes | PROVISIONAL (vectors) |
| Order | `list` and `export` by key bytes; `ads` lowest txid first; `pending`, `fronts`, `outcomes`, `outcome-history` newest `created` first, never dated after the host's time | PROVISIONAL |
| Free window | `outcomes` within 31 days of admission | PROVISIONAL |
| Terms and payment | `<base>/ls_bstore/terms`; the 402 authoritative; payment output 0 of the BRC-105 transaction, P2PKH to the BRC-29 key | PROVISIONAL |
| Retention floors | spec section 12.3 | PROVISIONAL (floors: a later version may lengthen them, never shorten) |

## 11. The S3 contract and the backend seam

| Item | Value | Status |
| --- | --- | --- |
| S3 bucket names | the identifier with `_` replaced by `-` | PROVISIONAL |
| Authentication | SigV4 gateway mode only, one identity per access key | PROVISIONAL |
| ETag | the entry's `etag` (MD5, or S3's multipart form), else `"<root hex>-1"` | PROVISIONAL |
| Headers | `x-amz-checksum-sha256` the content hash (`FULL_OBJECT`); `x-amz-meta-bstore-root` | PROVISIONAL |
| Not found | `NoSuchKey` only on a proven absence or a delete; `503 SlowDown` when no host answers | PROVISIONAL |
| Hold-backend interface | `Put`, `Has`, `Get(hash, offset, length)`, `ServeURL`, `Advertise`, as spec section 17 | PROVISIONAL |

## 12. Bounds that are a floor

Readers accept every record within these, and a later version may raise
them but never lower them: bucket record 65,536 bytes, 16 writers, 1024
changes, 1,048,576 keys; entry 8,192 bytes, key 1,024 bytes, `meta` 2,048
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
- The answer limits, which a later version may raise.
- The buyer's `c` beyond the default and its timeout.
- Gateway policy: which buckets and identities it serves, its default
  bounty, part buffering.
