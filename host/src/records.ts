/**
 * The records: the bucket record, the object entry, the piece, the front
 * statement and the outcome, twins of the Go bucket.go, entry.go, piece.go
 * and outcome.go, each refused in its spec section's order and viewed as
 * the vectors view it.
 */
import { CborMap, compareBytes, encode, recordReader, type Pair, type RecordFields, type Value } from '@lightwebinc/bcommon'
import { checkBucket, checkContentType, checkETag, checkFrontURL, checkKey, isMetaKey, isObjectKey, oneLine } from './names.js'
import {
  BK, EK, EmptyHash, EmptyRoot, FK, KindDelete, KindPut, MagicBucket, MagicEntry, MagicFront, MagicOutcome, MagicPiece, MaxBucketRecord,
  MaxChanges, MaxCheckC, MaxConsumed, MaxEntry, MaxFront, MaxKeysInMap, MaxKind, MaxMetaBytes, MaxMetaPairs, MaxMetaValue, MaxOutcome,
  MaxPiece, MaxPieceRecord, MaxSafe, MaxSatoshis, MaxSize, MaxT, MaxTime, MaxU32, MaxWriters, MinTime, OK, OutcomeInvalid, OutcomePass,
  OutcomeUnretrievable, PK, ProofProfile, StoreError, refuse,
} from './storerec.js'
import { compareText, equal, sha256, toHex, utf8 } from './util.js'
import { decodeRequest, verifyChallengeSig, type Request } from './challenge.js'

export const rr = recordReader((reason, detail) => new StoreError(reason, '', detail))

/** A record's known keys read with bstore's labels. */
export class Fields {
  constructor(readonly f: RecordFields) {}
  static read(b: Uint8Array, max: number, last: number, magic: Uint8Array): Fields {
    return new Fields(rr.decode(b, max, last, magic))
  }
  has(k: number): boolean {
    return this.f.known.has(k)
  }
  get extra(): Pair[] {
    return this.f.extra
  }
  need(k: number): Value {
    return rr.need(this.f, k)
  }
  bytes(k: number): Uint8Array {
    return rr.bytes(this.f, k)
  }
  bytesN(k: number, n: number): Uint8Array {
    const v = this.bytes(k)
    if (v.length !== n) throw refuse('type', '', `key ${k}`)
    return v
  }
  text(k: number): string {
    return rr.text(this.f, k)
  }
  uint(k: number, lo: number, hi: number, label: string): number {
    const v = this.need(k)
    if (typeof v !== 'bigint' || v < 0n) throw refuse('type', '', `key ${k}`)
    if (v < BigInt(lo) || v > BigInt(hi)) throw refuse(label, '', `key ${k}`)
    return Number(v)
  }
  array(k: number): Value[] {
    const v = this.need(k)
    if (!Array.isArray(v)) throw refuse('type', '', `key ${k}`)
    return v
  }
  key(k: number): Uint8Array {
    const v = this.bytes(k)
    checkKey(v)
    return v
  }
  bucket(k: number): string {
    const s = this.text(k)
    checkBucket(s)
    return s
  }
  created(k: number): number {
    return this.uint(k, MinTime, MaxTime, 'created')
  }
}

export function encodeRecord(m: Pair[], extra: Pair[], last: number, max: number): Uint8Array {
  rr.checkExtra(extra, last, m.length)
  return rr.encode(m, extra, max)
}

const u = (n: number): bigint => BigInt(n)
const p = (k: number, v: Value): Pair => ({ key: u(k), val: v })

export function extraHex(extra: Pair[]): string {
  return extra.length === 0 ? '' : toHex(encode(new CborMap(extra)))
}

// ---- the bucket record ----

export interface Change {
  key: string
  c: Uint8Array
  removal: boolean
}

export interface Bucket {
  bucket: string
  owner: Uint8Array
  id: Uint8Array
  seq: number
  prev: Uint8Array
  writers: Uint8Array[]
  changes: Change[]
  count: number
  mapRoot: Uint8Array
  created: number
  consumed: Uint8Array[]
  extra: Pair[]
}

const zero32 = new Uint8Array(32)

export function changeValue(c: Change): Value {
  return c.removal ? [c.key, zero32, c.c] : [c.key, c.c]
}

export function encodeBucket(r: Bucket): Uint8Array {
  const out = encodeRecord([
    p(BK.magic, MagicBucket), p(BK.bucket, r.bucket), p(BK.owner, r.owner), p(BK.id, r.id), p(BK.seq, u(r.seq)), p(BK.prev, r.prev),
    p(BK.writers, r.writers), p(BK.changes, r.changes.map(changeValue)), p(BK.count, u(r.count)), p(BK.mapRoot, r.mapRoot),
    p(BK.created, u(r.created)), p(BK.consumed, r.consumed),
  ], r.extra, BK.last, MaxBucketRecord)
  decodeBucket(out)
  return out
}

export function decodeBucket(raw: Uint8Array): Bucket {
  const f = Fields.read(raw, MaxBucketRecord, BK.last, MagicBucket)
  const bucket = f.bucket(BK.bucket)
  const owner = f.key(BK.owner)
  const id = f.bytesN(BK.id, 32)
  const seq = f.uint(BK.seq, 0, MaxSafe, 'seq')
  const prev = f.bytesN(BK.prev, 32)
  const writers = readWriters(f, owner)
  const changes = readChanges(f)
  const count = f.uint(BK.count, 0, MaxKeysInMap, 'count')
  const mapRoot = f.bytesN(BK.mapRoot, 32)
  const created = f.created(BK.created)
  const consumed = readConsumed(f, changes)
  if (seq === 0) {
    if (!equal(prev, zero32) || changes.length !== 0 || consumed.length !== 0 || count !== 0 || !equal(mapRoot, EmptyHash)) throw refuse('first')
  } else if (equal(prev, zero32)) {
    throw refuse('first')
  }
  return { bucket, owner, id, seq, prev, writers, changes, count, mapRoot, created, consumed, extra: f.extra }
}

function readWriters(f: Fields, owner: Uint8Array): Uint8Array[] {
  const a = f.array(BK.writers)
  if (a.length > MaxWriters) throw refuse('writers')
  const out = a.map((e) => {
    if (!(e instanceof Uint8Array)) throw refuse('identity')
    checkKey(e)
    return e
  })
  for (let i = 0; i < out.length; i++) {
    if (i > 0 && compareBytes(out[i - 1]!, out[i]!) >= 0) throw refuse('writers')
    if (equal(out[i]!, owner)) throw refuse('writers')
  }
  return out
}

function readChange(e: Value): Change | undefined {
  if (!Array.isArray(e) || (e.length !== 2 && e.length !== 3)) return undefined
  const key = e[0]
  if (typeof key !== 'string' || !isObjectKey(key)) return undefined
  const last = e[e.length - 1]
  if (!(last instanceof Uint8Array) || last.length !== 32 || equal(last, zero32)) return undefined
  if (e.length === 3) {
    const z = e[1]
    if (!(z instanceof Uint8Array) || z.length !== 32 || !equal(z, zero32)) return undefined
  }
  return { key, c: last, removal: e.length === 3 }
}

function readChanges(f: Fields): Change[] {
  const a = f.array(BK.changes)
  if (a.length > MaxChanges) throw refuse('changes')
  const out: Change[] = []
  for (const e of a) {
    const c = readChange(e)
    if (c === undefined) throw refuse('changes')
    if (out.length > 0 && compareText(out[out.length - 1]!.key, c.key) >= 0) throw refuse('changes')
    out.push(c)
  }
  return out
}

function readConsumed(f: Fields, changes: Change[]): Uint8Array[] {
  const a = f.array(BK.consumed)
  if (a.length > MaxConsumed) throw refuse('consumed')
  const named = new Set(changes.map((c) => toHex(c.c)))
  const out: Uint8Array[] = []
  for (const e of a) {
    if (!(e instanceof Uint8Array) || e.length !== 32) throw refuse('consumed')
    if (out.length > 0 && compareBytes(out[out.length - 1]!, e) >= 0) throw refuse('consumed')
    if (named.has(toHex(e))) throw refuse('consumed')
    out.push(e)
  }
  return out
}

export function bucketView(r: Bucket): Record<string, unknown> {
  return {
    bucket: r.bucket, owner: toHex(r.owner), id: toHex(r.id), seq: r.seq, prev: toHex(r.prev), writers: r.writers.map(toHex),
    changes: r.changes.map((c) => (c.removal ? [c.key, toHex(zero32), toHex(c.c)] : [c.key, toHex(c.c)])), count: r.count,
    mapRoot: toHex(r.mapRoot), created: r.created, consumed: r.consumed.map(toHex), extra: extraHex(r.extra),
  }
}

// ---- the object entry ----

export interface Hold {
  txid: Uint8Array
  vout: number
  satoshis: number
  t: number
  K: number
  retention: number
  deadline: number
  checkC: number
}

export interface Entry {
  bucket: string
  id: Uint8Array
  writer: Uint8Array
  key: string
  kind: number
  created: number
  size: number
  root: Uint8Array
  contentHash: Uint8Array
  proofRoot: Uint8Array
  proofProfile: number
  contentType: string
  etag?: string
  meta?: [string, string][]
  hold?: Hold
  /** A kind from 3 to 255: its keys 7 to 15, unread. */
  unknown: Pair[]
  extra: Pair[]
}

export const holdValue = (h: Hold): Value => [h.txid, u(h.vout), u(h.satoshis), u(h.t), u(h.K), u(h.retention), u(h.deadline), u(h.checkC)]

export function encodeEntry(e: Entry): Uint8Array {
  const m: Pair[] = [p(EK.magic, MagicEntry), p(EK.bucket, e.bucket), p(EK.id, e.id), p(EK.writer, e.writer), p(EK.key, e.key),
    p(EK.kind, u(e.kind)), p(EK.created, u(e.created))]
  if (e.kind === KindPut) {
    m.push(p(EK.size, u(e.size)), p(EK.root, e.root), p(EK.contentHash, e.contentHash), p(EK.proofRoot, e.proofRoot),
      p(EK.proofProfile, u(e.proofProfile)), p(EK.contentType, e.contentType))
    if (e.etag !== undefined) m.push(p(EK.etag, e.etag))
    if (e.meta !== undefined) m.push(p(EK.meta, new CborMap(e.meta.map(([k, v]) => ({ key: k, val: v })))))
    if (e.hold !== undefined) m.push(p(EK.hold, holdValue(e.hold)))
  } else if (e.kind !== KindDelete) {
    m.push(...e.unknown)
  }
  const out = encodeRecord(m, e.extra, EK.last, MaxEntry)
  decodeEntry(out)
  return out
}

export function decodeEntry(raw: Uint8Array): Entry {
  const f = Fields.read(raw, MaxEntry, EK.last, MagicEntry)
  const bucket = f.bucket(EK.bucket)
  const id = f.bytesN(EK.id, 32)
  const writer = f.key(EK.writer)
  const key = f.text(EK.key)
  if (!isObjectKey(key)) throw refuse('objkey')
  const kind = f.uint(EK.kind, 1, MaxKind, 'kind')
  const created = f.created(EK.created)
  const e: Entry = {
    bucket, id, writer, key, kind, created, size: 0, root: zero32, contentHash: zero32, proofRoot: zero32, proofProfile: 0,
    contentType: '', unknown: [], extra: f.extra,
  }
  if (kind === KindPut) {
    readPut(f, e)
  } else if (kind === KindDelete) {
    for (let k = EK.size; k <= EK.last; k++) if (f.has(k)) throw refuse('kind')
  } else {
    for (let k = EK.size; k <= EK.last; k++) if (f.has(k)) e.unknown.push({ key: u(k), val: f.need(k) })
  }
  return e
}

function readPut(f: Fields, e: Entry): void {
  if (f.has(EK.size)) e.size = f.uint(EK.size, 0, MaxSize, 'size')
  if (f.has(EK.root)) e.root = f.bytesN(EK.root, 32)
  if (f.has(EK.contentHash)) e.contentHash = f.bytesN(EK.contentHash, 32)
  if (f.has(EK.proofRoot)) e.proofRoot = f.bytesN(EK.proofRoot, 32)
  if (f.has(EK.proofProfile)) e.proofProfile = f.uint(EK.proofProfile, ProofProfile, ProofProfile, 'profile')
  if (f.has(EK.contentType)) {
    e.contentType = f.text(EK.contentType)
    checkContentType(e.contentType)
  }
  if (f.has(EK.etag)) {
    e.etag = f.text(EK.etag)
    checkETag(e.etag)
  }
  if (f.has(EK.meta)) e.meta = readMeta(f.need(EK.meta))
  if (f.has(EK.hold)) e.hold = readHold(f.array(EK.hold))
  for (let k = EK.size; k <= EK.contentType; k++) if (!f.has(k)) throw refuse('kind')
  if (e.size === 0) {
    if (!equal(e.root, EmptyRoot) || !equal(e.contentHash, EmptyHash) || !equal(e.proofRoot, EmptyHash) || e.hold !== undefined) throw refuse('empty')
  } else if (equal(e.root, EmptyRoot)) {
    throw refuse('empty')
  }
}

function readMeta(v: Value): [string, string][] {
  if (!(v instanceof CborMap)) throw refuse('type', '', 'key 14')
  if (v.entries.length < 1 || v.entries.length > MaxMetaPairs) throw refuse('meta')
  const out: [string, string][] = []
  for (const { key, val } of v.entries) {
    if (typeof key !== 'string' || !isMetaKey(key)) throw refuse('meta')
    if (typeof val !== 'string' || utf8(val).length > MaxMetaValue || !oneLine(val)) throw refuse('meta')
    out.push([key, val])
  }
  if (encode(v).length > MaxMetaBytes) throw refuse('meta')
  return out
}

export function readHold(a: Value[]): Hold {
  if (a.length !== 8) throw refuse('hold')
  const txid = a[0]
  if (!(txid instanceof Uint8Array) || txid.length !== 32) throw refuse('hold')
  const bounds: [number, number][] = [[0, MaxU32], [1, MaxSatoshis], [1, MaxT], [1, MaxT], [1, MaxU32], [1, MaxU32], [1, MaxCheckC]]
  const n = bounds.map(([lo, hi], i) => {
    const x = a[i + 1]
    if (typeof x !== 'bigint' || x < BigInt(lo) || x > BigInt(hi)) throw refuse('hold')
    return Number(x)
  })
  const h: Hold = { txid, vout: n[0]!, satoshis: n[1]!, t: n[2]!, K: n[3]!, retention: n[4]!, deadline: n[5]!, checkC: n[6]! }
  if (h.K < h.t) throw refuse('hold')
  return h
}

export const holdView = (h: Hold): Record<string, unknown> => ({
  txid: toHex(h.txid), vout: h.vout, satoshis: h.satoshis, t: h.t, K: h.K, retention: h.retention, deadline: h.deadline, checkC: h.checkC,
})

export function entryView(e: Entry): Record<string, unknown> {
  const v: Record<string, unknown> = {
    bucket: e.bucket, id: toHex(e.id), writer: toHex(e.writer), key: e.key, kind: e.kind, created: e.created, extra: extraHex(e.extra),
    unknown: extraHex(e.unknown),
  }
  if (e.kind === KindPut) {
    Object.assign(v, {
      size: e.size, root: toHex(e.root), contentHash: toHex(e.contentHash), proofRoot: toHex(e.proofRoot), proofProfile: e.proofProfile,
      contentType: e.contentType,
    })
    if (e.etag !== undefined) v.etag = e.etag
    if (e.meta !== undefined) v.meta = e.meta
    if (e.hold !== undefined) v.hold = holdView(e.hold)
  }
  return v
}

// ---- the piece ----

export interface Piece {
  bucket: string
  writer: Uint8Array
  root: Uint8Array
  piece: Uint8Array
  extra: Pair[]
}

export function encodePiece(x: Piece): Uint8Array {
  const out = encodeRecord([p(PK.magic, MagicPiece), p(PK.bucket, x.bucket), p(PK.writer, x.writer), p(PK.root, x.root), p(PK.piece, x.piece)],
    x.extra, PK.last, MaxPieceRecord)
  decodePiece(out)
  return out
}

export function decodePiece(raw: Uint8Array): Piece {
  const f = Fields.read(raw, MaxPieceRecord, PK.last, MagicPiece)
  const bucket = f.bucket(PK.bucket)
  const writer = f.key(PK.writer)
  const root = f.bytesN(PK.root, 32)
  const piece = f.bytes(PK.piece)
  if (piece.length < 1 || piece.length > MaxPiece) throw refuse('piece')
  return { bucket, writer, root, piece, extra: f.extra }
}

export const pieceView = (x: Piece): Record<string, unknown> => ({
  bucket: x.bucket, writer: toHex(x.writer), root: toHex(x.root), pieceHash: toHex(sha256(x.piece)), pieceLength: x.piece.length,
  extra: extraHex(x.extra),
})

// ---- the front statement ----

export interface Front {
  bucket: string
  front: Uint8Array
  advertiser: Uint8Array
  url: string
  created: number
  extra: Pair[]
}

export function encodeFront(s: Front): Uint8Array {
  const out = encodeRecord([p(FK.magic, MagicFront), p(FK.bucket, s.bucket), p(FK.front, s.front), p(FK.advertiser, s.advertiser),
    p(FK.url, s.url), p(FK.created, u(s.created))], s.extra, FK.last, MaxFront)
  decodeFront(out)
  return out
}

export function decodeFront(raw: Uint8Array): Front {
  const f = Fields.read(raw, MaxFront, FK.last, MagicFront)
  const bucket = f.bucket(FK.bucket)
  const front = f.key(FK.front)
  const advertiser = f.key(FK.advertiser)
  const url = f.text(FK.url)
  checkFrontURL(url)
  const created = f.created(FK.created)
  return { bucket, front, advertiser, url, created, extra: f.extra }
}

export const frontView = (s: Front): Record<string, unknown> => ({
  bucket: s.bucket, front: toHex(s.front), advertiser: toHex(s.advertiser), url: s.url, created: s.created, extra: extraHex(s.extra),
})

// ---- the outcome record ----

export interface Outcome {
  bucket: string
  buyer: Uint8Array
  created: number
  request: Uint8Array
  req: Request
  outcome: number
  response?: Uint8Array
  signature?: Uint8Array
  extra: Pair[]
}

export function encodeOutcome(o: Outcome): Uint8Array {
  const m: Pair[] = [p(OK.magic, MagicOutcome), p(OK.bucket, o.bucket), p(OK.buyer, o.buyer), p(OK.created, u(o.created)),
    p(OK.request, o.request), p(OK.outcome, u(o.outcome))]
  if (o.response !== undefined) m.push(p(OK.response, o.response))
  if (o.signature !== undefined) m.push(p(OK.signature, o.signature))
  const out = encodeRecord(m, o.extra, OK.last, MaxOutcome)
  decodeOutcome(out)
  return out
}

export function decodeOutcome(raw: Uint8Array): Outcome {
  const f = Fields.read(raw, MaxOutcome, OK.last, MagicOutcome)
  const bucket = f.bucket(OK.bucket)
  const buyer = f.key(OK.buyer)
  const created = f.created(OK.created)
  const request = f.bytes(OK.request)
  const req = decodeRequest(request)
  const outcome = f.uint(OK.outcome, OutcomePass, OutcomeUnretrievable, 'outcome')
  const o: Outcome = { bucket, buyer, created, request, req, outcome, extra: f.extra }
  if (f.has(OK.response)) o.response = f.bytesN(OK.response, 32)
  if (f.has(OK.signature)) o.signature = f.bytes(OK.signature)
  const signed = outcome === OutcomePass || outcome === OutcomeInvalid
  if (signed !== f.has(OK.response) || signed !== f.has(OK.signature)) throw refuse('kind')
  if (signed && !verifyChallengeSig(req.answerer, sha256(request), o.response!, o.signature!)) throw refuse('response-signature')
  return o
}

export function outcomeView(o: Outcome): Record<string, unknown> {
  const v: Record<string, unknown> = { bucket: o.bucket, buyer: toHex(o.buyer), created: o.created, request: toHex(o.request), outcome: o.outcome, extra: extraHex(o.extra) }
  if (o.response !== undefined) v.response = toHex(o.response)
  if (o.signature !== undefined) v.signature = toHex(o.signature)
  return v
}

