/**
 * BRC-26 advertisements as the SDK decodes and verifies them (ts-stack
 * @bsv/sdk storage/UHRPAdvertisementValidation.ts, which the SDK this
 * package pins does not carry), step for step, and bstore's eligibility of
 * an advertisement for an entry's bounty; the twin of the Go advert.go. The
 * url is parsed by the WHATWG URL parser, as the SDK parses it.
 */
import { Signature } from '@bsv/sdk'
import { chirpIdentifier, readerLockingKey, strictPublicKey } from '@lightwebinc/bcommon'
import { AnyoneKey } from './names.js'
import { AdKeyID, AdProtocol, KindPut, MaxAdURL, MaxSafe, refuse } from './storerec.js'
import type { Entry } from './records.js'
import { concat, equal, sha256, toHex } from './util.js'

export const MaxAdPayload = MaxAdURL + 256

export interface Advertisement {
  host: Uint8Array
  hash: Uint8Array
  url: string
  expiry: number
  fileSize: number
  signature: Uint8Array
}

const adErr = (rule: string) => refuse('advertisement', rule)

function minimalPush(d: Uint8Array): Uint8Array {
  if (d.length === 0 || (d.length === 1 && d[0] === 0)) return Uint8Array.of(0)
  if (d.length === 1 && d[0]! >= 1 && d[0]! <= 16) return Uint8Array.of(0x50 + d[0]!)
  if (d.length === 1 && d[0] === 0x81) return Uint8Array.of(0x4f)
  if (d.length <= 75) return concat(Uint8Array.of(d.length), d)
  if (d.length <= 0xff) return concat(Uint8Array.of(0x4c, d.length), d)
  if (d.length <= 0xffff) return concat(Uint8Array.of(0x4d, d.length & 0xff, d.length >> 8), d)
  return concat(Uint8Array.of(0x4e, d.length & 0xff, (d.length >> 8) & 0xff, (d.length >> 16) & 0xff, d.length >>> 24), d)
}

function readPush(s: Uint8Array, i: number): [Uint8Array, number] | undefined {
  if (i >= s.length) return undefined
  const op = s[i]!
  let n: number
  let hdr = 1
  if (op === 0) return [Uint8Array.of(0), i + 1]
  if (op >= 0x51 && op <= 0x60) return [Uint8Array.of(op - 0x50), i + 1]
  if (op === 0x4f) return [Uint8Array.of(0x81), i + 1]
  if (op >= 1 && op <= 75) n = op
  else if (op === 0x4c && i + 1 < s.length) [n, hdr] = [s[i + 1]!, 2]
  else if (op === 0x4d && i + 2 < s.length) [n, hdr] = [s[i + 1]! | (s[i + 2]! << 8), 3]
  else if (op === 0x4e && i + 4 < s.length) [n, hdr] = [(s[i + 1]! | (s[i + 2]! << 8) | (s[i + 3]! << 16)) + s[i + 4]! * 2 ** 24, 5]
  else return undefined
  if (i + hdr + n > s.length) return undefined
  return [s.subarray(i + hdr, i + hdr + n), i + hdr + n]
}

function strictCompact(f: Uint8Array): number | undefined {
  if (f.length === 0) return undefined
  let v: bigint
  let n: number
  const le = (from: number, len: number): bigint => {
    let x = 0n
    for (let i = from + len - 1; i >= from; i--) x = (x << 8n) | BigInt(f[i]!)
    return x
  }
  if (f[0] === 0xfd) {
    if (f.length < 3) return undefined
    ;[v, n] = [le(1, 2), 3]
    if (v < 0xfdn) return undefined
  } else if (f[0] === 0xfe) {
    if (f.length < 5) return undefined
    ;[v, n] = [le(1, 4), 5]
    if (v <= 0xffffn) return undefined
  } else if (f[0] === 0xff) {
    if (f.length < 9) return undefined
    ;[v, n] = [le(1, 8), 9]
    if (v <= 0xffffffffn || v > BigInt(MaxSafe)) return undefined
  } else {
    ;[v, n] = [BigInt(f[0]!), 1]
  }
  return n === f.length && v >= 1n ? Number(v) : undefined
}

/** A DER signature Signature.fromDER reads and toDER writes back unchanged. */
function canonicalDER(sig: Uint8Array): Signature | undefined {
  if (sig.length < 8 || sig.length > 80) return undefined
  try {
    const s = Signature.fromDER(Array.from(sig))
    return equal(Uint8Array.from(s.toDER() as number[]), sig) ? s : undefined
  } catch {
    return undefined
  }
}

const controlRe = /\p{Cc}/u

/** The SDK's url check: strict UTF-8 (one BOM dropped), no control, a credential-free https URL with no fragment. */
export function adURL(raw: Uint8Array): string | undefined {
  let u: string
  try {
    u = new TextDecoder('utf-8', { fatal: true }).decode(raw)
  } catch {
    return undefined
  }
  if (controlRe.test(u)) return undefined
  let loc: URL
  try {
    loc = new URL(u)
  } catch {
    return undefined
  }
  if (loc.protocol !== 'https:' || loc.hostname.length === 0 || loc.username.length !== 0 || loc.password.length !== 0 || loc.hash.length !== 0) return undefined
  return u
}

/** Decodes and verifies one advertisement output's locking script, in the Go order. */
export function decodeAdvertisement(script: Uint8Array): Advertisement {
  if (script.length < 35 || script[0] !== 0x21 || script[34] !== 0xac) throw adErr('pushdrop')
  const lockBytes = script.subarray(1, 34)
  if (strictPublicKey(lockBytes) === undefined) throw adErr('pushdrop')
  const fields: Uint8Array[] = []
  let i = 35
  while (fields.length < 6) {
    const r = readPush(script, i)
    if (r === undefined) throw adErr('pushdrop')
    fields.push(r[0])
    i = r[1]
  }
  let total = 0
  for (const f of fields) {
    if (f.length > MaxAdURL) throw adErr('field-size')
    total += f.length
  }
  if (total > MaxAdPayload) throw adErr('field-size')
  const want = concat(Uint8Array.of(0x21), lockBytes, Uint8Array.of(0xac), ...fields.map(minimalPush), Uint8Array.of(0x6d, 0x6d, 0x6d))
  if (!equal(script, want)) throw adErr('pushdrop')
  const host = fields[0]!
  if (host.length !== 33 || (host[0] !== 2 && host[0] !== 3) || strictPublicKey(host) === undefined) throw adErr('host')
  if (fields[1]!.length !== 32) throw adErr('hash')
  if (fields[2]!.length < 1 || fields[2]!.length > MaxAdURL) throw adErr('url')
  const url = adURL(fields[2]!)
  if (url === undefined) throw adErr('url')
  const expiry = strictCompact(fields[3]!)
  if (expiry === undefined) throw adErr('expiry')
  const fileSize = strictCompact(fields[4]!)
  if (fileSize === undefined) throw adErr('file-size')
  const sig = canonicalDER(fields[5]!)
  if (sig === undefined) throw adErr('signature')
  const lock = readerLockingKey(AdProtocol, AdKeyID, toHex(host))
  if (!equal(Uint8Array.from(lock.encode(true) as number[]), lockBytes)) throw adErr('lock')
  if (!sig.verify(Array.from(concat(...fields.slice(0, 5))), lock)) throw adErr('verify')
  return { host, hash: fields[1]!, url, expiry, fileSize, signature: fields[5]! }
}

export const chirpURLSuffix = (root: Uint8Array): string => {
  const id = chirpIdentifier(root)
  return `/chirp/v1/${id}/objects/${id}`
}

/** Why an advertisement does not count for an entry's bounty; "" when it does. */
export function eligibility(ad: Advertisement, e: Entry, rootLen: number): string {
  if (e.kind !== KindPut || e.hold === undefined) return 'no-hold'
  if (!equal(ad.hash, e.root)) return 'root'
  if (equal(ad.host, AnyoneKey)) return 'anyone'
  if (ad.expiry < Math.floor(e.created / 1000) + e.hold.retention) return 'expiry'
  if (ad.fileSize !== rootLen) return 'file-size'
  if (!ad.url.endsWith(chirpURLSuffix(e.root))) return 'url'
  return ''
}

export { sha256 }
