/**
 * Challenges: the request, the response body and its signature, the sample
 * indices, the buyer's check in sample-major order with the whole-blob
 * recheck, the answerer's refusals, and the proof tree a provider answers
 * from; the twin of the Go challenge.go and proof.go.
 */
import { CborMap, decodeValue, encode, hashLeaf, pathLength, proveLeafHashes, readerLockingKey, rootOfLeafHashes, verifyAt, verifyField, type Pair, type Value } from '@lightwebinc/bcommon'
import { isKey } from './names.js'
import {
  BlobSize, KeyChallenge, MagicRequest, MagicResponse, MaxC, MaxRequest, MaxSignedResponse, MaxSize, MaxU32, NonceLen, OutcomeInvalid,
  OutcomeNoAnswer, OutcomePass, OutcomeUnretrievable, Protocol, QK, RK, SeedWindow, SegmentSize, SegmentsPerBlob, StoreError, refuse,
} from './storerec.js'
import { Fields, rr } from './records.js'
import { concat, equal, littleEndian, sha256, toHex } from './util.js'

export interface Request {
  root: Uint8Array
  proofRoot: Uint8Array
  size: number
  provider: Uint8Array
  answerer: Uint8Array
  seedHeight: number
  seedHash: Uint8Array
  nonce: Uint8Array
  c: number
  range?: [number, number]
}

const u = (n: number): bigint => BigInt(n)
const p = (k: number, v: Value): Pair => ({ key: u(k), val: v })

export function encodeRequest(q: Request): Uint8Array {
  const m: Pair[] = [p(QK.magic, MagicRequest), p(QK.root, q.root), p(QK.proofRoot, q.proofRoot), p(QK.size, u(q.size)),
    p(QK.provider, q.provider), p(QK.answerer, q.answerer), p(QK.seedHeight, u(q.seedHeight)), p(QK.seedHash, q.seedHash),
    p(QK.nonce, q.nonce), p(QK.c, u(q.c))]
  if (q.range !== undefined) m.push(p(QK.range, [u(q.range[0]), u(q.range[1])]))
  const out = rr.encode(m, [], MaxRequest)
  decodeRequest(out)
  return out
}

/** A refusal inside a request: request, the inner label or rule as its rule. */
function asRequest<T>(f: () => T): T {
  try {
    return f()
  } catch (e) {
    if (e instanceof StoreError) throw new StoreError('request', e.rule !== '' ? e.rule : e.label, e.message)
    throw e
  }
}

export const segments = (size: number): number => Math.ceil(size / SegmentSize)
export const blobs = (size: number): number => Math.ceil(size / BlobSize)
export const segmentLen = (i: number, size: number): number => (i === segments(size) - 1 ? size - SegmentSize * (segments(size) - 1) : SegmentSize)

/** Reads a request (spec section 7.1); every refusal is request, its rule the step. */
export function decodeRequest(raw: Uint8Array): Request {
  return asRequest(() => {
    const f = Fields.read(raw, MaxRequest, QK.last, MagicRequest)
    if (f.extra.length > 0) throw refuse('unknown-key')
    const root = f.bytesN(QK.root, 32)
    const proofRoot = f.bytesN(QK.proofRoot, 32)
    const size = f.uint(QK.size, 1, MaxSize, 'size')
    const provider = f.key(QK.provider)
    const answerer = f.key(QK.answerer)
    const seedHeight = f.uint(QK.seedHeight, 0, MaxU32, 'seed-height')
    const seedHash = f.bytesN(QK.seedHash, 32)
    const nonce = f.bytesN(QK.nonce, NonceLen)
    const c = f.uint(QK.c, 1, MaxC, 'c')
    const q: Request = { root, proofRoot, size, provider, answerer, seedHeight, seedHash, nonce, c }
    if (f.has(QK.range)) {
      const a = f.array(QK.range)
      if (a.length !== 2) throw refuse('range')
      const [first, last] = a
      if (typeof first !== 'bigint' || typeof last !== 'bigint' || first < 0n || last < 0n || first > last || last >= BigInt(blobs(size))) throw refuse('range')
      q.range = [Number(first), Number(last)]
    }
    return q
  })
}

export function requestView(q: Request): Record<string, unknown> {
  const v: Record<string, unknown> = {
    root: toHex(q.root), proofRoot: toHex(q.proofRoot), size: q.size, provider: toHex(q.provider), answerer: toHex(q.answerer),
    seedHeight: q.seedHeight, seedHash: toHex(q.seedHash), nonce: toHex(q.nonce), c: q.c,
  }
  if (q.range !== undefined) v.range = q.range
  return v
}

/** The segments the indices are drawn from, and the first of them. */
export function domain(q: Request): [number, number] {
  const n = segments(q.size)
  if (q.range === undefined) return [n, 0]
  const lo = SegmentsPerBlob * q.range[0]
  const hi = Math.min(SegmentsPerBlob * (q.range[1] + 1), n)
  return [hi - lo, lo]
}

export const indexDigest = (q: Request, k: number): Uint8Array => sha256(q.seedHash, q.proofRoot, q.provider, q.nonce, littleEndian(k, 4))

/** The segment sample k asks for: the digest read little-endian, modulo the domain, offset. */
export function sampleIndex(q: Request, k: number): number {
  const d = indexDigest(q, k)
  let v = 0n
  for (let i = 31; i >= 0; i--) v = (v << 8n) | BigInt(d[i]!)
  const [m, off] = domain(q)
  return off + Number(v % BigInt(m))
}

export interface Sample {
  segment: Uint8Array | null
  siblings: Uint8Array[]
}

export interface Body {
  request: Uint8Array
  samples: Sample[]
  answerer: Uint8Array
}

export function encodeBody(r: Body): Uint8Array {
  return rr.encode([p(RK.magic, MagicResponse), p(RK.request, r.request), p(RK.samples, r.samples.map((s) => [s.segment, s.siblings])),
    p(RK.answerer, r.answerer)], [], MaxSignedResponse)
}

/** Reads a response body for its shape alone (the record steps, unknown-key, missing, type, samples). */
export function decodeBody(raw: Uint8Array): Body {
  const f = Fields.read(raw, MaxSignedResponse, RK.last, MagicResponse)
  if (f.extra.length > 0) throw refuse('unknown-key')
  const request = f.bytesN(RK.request, 32)
  const samples = f.array(RK.samples).map((e): Sample => {
    if (!Array.isArray(e) || e.length !== 2) throw refuse('samples')
    const [seg, sib] = e
    if (seg !== null && !(seg instanceof Uint8Array)) throw refuse('samples')
    if (!Array.isArray(sib)) throw refuse('samples')
    return {
      segment: seg as Uint8Array | null,
      siblings: sib.map((h) => {
        if (!(h instanceof Uint8Array) || h.length !== 32) throw refuse('samples')
        return h
      }),
    }
  })
  const answerer = f.bytes(RK.answerer)
  return { request, samples, answerer }
}

export const bodyView = (r: Body): Record<string, unknown> => ({
  request: toHex(r.request),
  samples: r.samples.map((s) => ({ segment: s.segment === null ? null : toHex(s.segment), siblings: s.siblings.map(toHex) })),
  answerer: toHex(r.answerer),
})

/** derive(answerer, "challenge") as compressed bytes. */
export const challengeKey = (answerer: Uint8Array): Uint8Array =>
  Uint8Array.from(readerLockingKey(Protocol, KeyChallenge, toHex(answerer)).encode(true) as number[])

/** A strict low-S DER signature over SHA-256(request) || response hash under derive(answerer, "challenge"). */
export function verifyChallengeSig(answerer: Uint8Array, requestHash: Uint8Array, responseHash: Uint8Array, sig: Uint8Array): boolean {
  if (!isKey(answerer)) return false
  return verifyField(challengeKey(answerer), concat(requestHash, responseHash), sig)
}

/** A signed response [body, signature] as byte strings, or undefined. */
export function splitSigned(signed: Uint8Array): [Uint8Array, Uint8Array] | undefined {
  if (signed.length > MaxSignedResponse) return undefined
  let v: Value
  try {
    v = decodeValue(signed)
  } catch {
    return undefined
  }
  if (!Array.isArray(v) || v.length !== 2 || !(v[0] instanceof Uint8Array) || !(v[1] instanceof Uint8Array)) return undefined
  return [v[0], v[1]]
}

export interface Fetcher {
  range(blob: number, offset: number, length: number): Promise<Uint8Array>
  blob(blob: number): Promise<Uint8Array>
}

export interface Verdict {
  outcome: number
  label: string
  rule: string
  sample: number
  recheck: boolean
  rechecked: number[]
}

function checkSample(seg: Uint8Array | undefined, want: number, i: number, n: number, siblings: Uint8Array[], proofRoot: Uint8Array): string | undefined {
  if (seg === undefined || seg.length !== want) return 'segment'
  if (!verifyAt(hashLeaf(seg), i, n, siblings, proofRoot)) return 'path'
  return undefined
}

/** The buyer's check of spec section 7.4, sample-major, with the whole-blob recheck. */
export async function verifyResponse(q: Request, requestRaw: Uint8Array, signed: Uint8Array, blobHash: (blob: number) => Uint8Array, fetch: Fetcher): Promise<Verdict> {
  const fail = (outcome: number, label: string, rule: string, sample: number): Verdict => ({ outcome, label, rule, sample, recheck: false, rechecked: [] })
  const parts = splitSigned(signed)
  if (parts === undefined) return fail(OutcomeNoAnswer, 'signature', 'shape', -1)
  const [bodyRaw, sig] = parts
  if (!verifyChallengeSig(q.answerer, sha256(requestRaw), sha256(bodyRaw), sig)) return fail(OutcomeNoAnswer, 'signature', 'verify', -1)
  let body: Body
  try {
    body = decodeBody(bodyRaw)
  } catch (e) {
    if (e instanceof StoreError) return fail(OutcomeInvalid, 'request', `body-${e.label}`, -1)
    throw e
  }
  if (!equal(body.request, sha256(requestRaw))) return fail(OutcomeInvalid, 'request', 'request-hash', -1)
  if (!equal(body.answerer, q.answerer)) return fail(OutcomeInvalid, 'request', 'answerer', -1)
  if (!equal(q.answerer, q.provider) && body.samples.some((s) => s.segment !== null)) return fail(OutcomeInvalid, 'request', 'inline-from-front', -1)
  if (body.samples.length !== q.c) return fail(OutcomeInvalid, 'count', 'samples', -1)
  const n = segments(q.size)
  const idx: number[] = []
  for (let k = 0; k < body.samples.length; k++) {
    idx.push(sampleIndex(q, k))
    if (body.samples[k]!.siblings.length !== pathLength(idx[k]!, n)) return fail(OutcomeInvalid, 'count', 'path-length', k)
  }
  const v: Verdict = { outcome: OutcomePass, label: 'ok', rule: '', sample: -1, recheck: false, rechecked: [] }
  for (let k = 0; k < body.samples.length; k++) {
    const s = body.samples[k]!
    const i = idx[k]!
    const want = segmentLen(i, q.size)
    const blob = Math.floor(i / SegmentsPerBlob)
    const off = (i % SegmentsPerBlob) * SegmentSize
    if (s.segment !== null) {
      const r = checkSample(s.segment, want, i, n, s.siblings, q.proofRoot)
      if (r !== undefined) return fail(OutcomeInvalid, r, '', k)
      continue
    }
    let first: string | undefined
    try {
      first = checkSample(await fetch.range(blob, off, want), want, i, n, s.siblings, q.proofRoot)
    } catch {
      first = 'fetch'
    }
    if (first === undefined) continue
    let whole: Uint8Array
    try {
      whole = await fetch.blob(blob)
    } catch {
      return { outcome: OutcomeUnretrievable, label: first, rule: '', sample: k, recheck: true, rechecked: [] }
    }
    if (!equal(sha256(whole), blobHash(blob))) return { outcome: OutcomeUnretrievable, label: first, rule: '', sample: k, recheck: true, rechecked: [] }
    const from = off + want <= whole.length ? whole.subarray(off, off + want) : undefined
    const r = checkSample(from, want, i, n, s.siblings, q.proofRoot)
    if (r !== undefined) return { outcome: OutcomeInvalid, label: r, rule: '', sample: k, recheck: true, rechecked: [] }
    v.rechecked.push(k)
  }
  return v
}

/** An answerer's refusals before it answers: request, seed, unknown-root. */
export function answerCheck(requestRaw: Uint8Array, tip: number, headerAt: (h: number) => Uint8Array | undefined,
  own: (root: Uint8Array) => [Uint8Array, number] | undefined): Request {
  const q = decodeRequest(requestRaw)
  const h = headerAt(q.seedHeight)
  if (h === undefined || !equal(h, q.seedHash) || q.seedHeight > tip || q.seedHeight + SeedWindow < tip) throw refuse('seed')
  const o = own(q.root)
  if (o === undefined || !equal(o[0], q.proofRoot) || o[1] !== q.size) throw refuse('unknown-root')
  return q
}

/** Every segment's leaf hash of an object: what a bridge front keeps. */
export class ProofTree {
  readonly leaves: Uint8Array[] = []
  constructor(readonly size: number, leafHashes: Uint8Array[]) {
    this.leaves = leafHashes
  }
  static of(content: Uint8Array): ProofTree {
    const leaves: Uint8Array[] = []
    for (let off = 0; off < content.length; off += SegmentSize) leaves.push(hashLeaf(content.subarray(off, Math.min(off + SegmentSize, content.length))))
    return new ProofTree(content.length, leaves)
  }
  root(): Uint8Array {
    return rootOfLeafHashes(this.leaves)
  }
  path(i: number): Uint8Array[] {
    return proveLeafHashes(this.leaves, i).map((s) => s.hash)
  }
}

/** The response body for a request from the content: inline, or every segment by reference. */
export function answer(q: Request, requestRaw: Uint8Array, content: Uint8Array, t: ProofTree, inline: boolean): Body {
  const samples: Sample[] = []
  for (let k = 0; k < q.c; k++) {
    const i = sampleIndex(q, k)
    const off = i * SegmentSize
    samples.push({ segment: inline ? content.slice(off, off + segmentLen(i, q.size)) : null, siblings: t.path(i) })
  }
  return { request: sha256(requestRaw), samples, answerer: q.answerer }
}

export const encodeSigned = (body: Uint8Array, sig: Uint8Array): Uint8Array => encode([body, sig])
export { CborMap }
