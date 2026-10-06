/** Names and identifiers, the twin of the Go names.go. */
import { strictPublicKey } from '@lightwebinc/bcommon'
import { BlobPrefix, DomainBucketID, FrontSuffix, MaxBucket, MaxContentType, MaxETag, MaxMetaKey, MaxName, MaxObjectKey, MaxURL, MinBucket, MinContentType, MinURL, RecordsPrefix, SuffixLen, refuse } from './storerec.js'
import { bigEndian, concat, equal, fromHex, sha256, utf8 } from './util.js'

/** BRC-43's anyone counterparty: the generator point. */
export const AnyoneKey = fromHex('0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798')

/** The key rule of spec section 2, or identity. */
export function checkKey(b: Uint8Array): void {
  if (strictPublicKey(b) === undefined) throw refuse('identity', '', 'not a canonical compressed key')
  if (equal(b, AnyoneKey)) throw refuse('identity', '', 'the anyone key')
}

export const isKey = (b: Uint8Array): boolean => {
  try {
    checkKey(b)
    return true
  } catch {
    return false
  }
}

const lower = (c: number): boolean => c >= 0x61 && c <= 0x7a

/** The bucket identifier grammar of spec section 3.1, or bucket. */
export function checkBucket(s: string): void {
  if (!isBucket(s)) throw refuse('bucket')
}

export function isBucket(s: string): boolean {
  const b = utf8(s)
  if (b.length < MinBucket || b.length > MaxBucket || b.length !== s.length) return false
  const i = s.lastIndexOf('_')
  if (i < 0) return false
  const name = b.subarray(0, i)
  const suffix = b.subarray(i + 1)
  if (suffix.length !== SuffixLen || !suffix.every(lower)) return false
  if (name.length < 1 || name.length > MaxName || !lower(name[0]!) || !lower(name[name.length - 1]!)) return false
  for (let j = 0; j < name.length; j++) {
    const c = name[j]!
    if (c === 0x5f) {
      if (name[j - 1] === 0x5f) return false
      continue
    }
    if (!lower(c)) return false
  }
  return true
}

export const recordsTopic = (bucket: string): string => RecordsPrefix + bucket
export const blobTopic = (bucket: string): string => BlobPrefix + bucket

/** The S3 bucket name, and whether the gateway takes it (no sthree- prefix). */
export function s3Name(bucket: string): [string, boolean] {
  const n = bucket.replaceAll('_', '-')
  return [n, !n.startsWith('sthree-')]
}

/** SHA-256("bstore bucket id v1" || txid || uint32be(vout)), txid in hash byte order. */
export const bucketID = (txid: Uint8Array, vout: number): Uint8Array => sha256(utf8(DomainBucketID), txid, bigEndian(vout, 4))

/** No control character (U+0000 to U+001F, U+007F to U+009F), U+2028 or U+2029. */
export function oneLine(s: string): boolean {
  for (const ch of s) {
    const r = ch.codePointAt(0)!
    if (r <= 0x1f || (r >= 0x7f && r <= 0x9f) || r === 0x2028 || r === 0x2029) return false
  }
  return true
}

/** An object key: 1 to 1024 bytes, one line. A lone surrogate counts as U+FFFD. */
export function isObjectKey(k: string): boolean {
  const n = utf8(k).length
  return n >= 1 && n <= MaxObjectKey && oneLine(k)
}

export function checkContentType(s: string): void {
  const b = utf8(s)
  if (b.length < MinContentType || b.length > MaxContentType) throw refuse('content-type')
  let slashes = 0
  for (const c of b) {
    if (c < 0x21 || c > 0x7e || (c >= 0x41 && c <= 0x5a) || c === 0x3b) throw refuse('content-type')
    if (c === 0x2f) slashes++
  }
  if (slashes !== 1) throw refuse('content-type')
}

export function checkETag(s: string): void {
  const b = utf8(s)
  if (b.length < 1 || b.length > MaxETag) throw refuse('etag')
  for (const c of b) if (c < 0x20 || c > 0x7e || c === 0x22) throw refuse('etag')
}

export function isMetaKey(s: string): boolean {
  const b = utf8(s)
  return b.length >= 1 && b.length <= MaxMetaKey && b.every((c) => lower(c) || (c >= 0x30 && c <= 0x39) || c === 0x2d)
}

/** A front statement's url (spec section 8.2), or url. */
export function checkFrontURL(s: string): void {
  const b = utf8(s)
  if (b.length < MinURL || b.length > MaxURL || !s.startsWith('https://') || s.endsWith('/')) throw refuse('url')
  for (const c of b) if (c < 0x21 || c > 0x7e || c === 0x3f || c === 0x23) throw refuse('url')
  let auth = s.slice('https://'.length)
  const i = auth.indexOf('/')
  if (i >= 0) auth = auth.slice(0, i)
  if (auth === '' || auth.includes('@')) throw refuse('url')
  // The WHATWG parse, as the advertisement check runs it, with its host
  // ASCII after percent-decoding and no label beginning "xn--", so that no
  // internationalised name is mapped (UTS #46) and Go decides alike.
  let loc: URL
  try {
    loc = new URL(s)
  } catch {
    throw refuse('url')
  }
  if (loc.protocol !== 'https:' || loc.hostname.length === 0 || loc.username.length !== 0 || loc.password.length !== 0 || loc.hash.length !== 0) throw refuse('url')
  const j = auth.indexOf('\\')
  if (/%[89a-fA-F][0-9a-fA-F]/.test(j >= 0 ? auth.slice(0, j) : auth)) throw refuse('url')
  if (loc.hostname.split('.').some((l) => l.startsWith('xn--'))) throw refuse('url')
}

/** The derived front of an advertisement url, or undefined. */
export function derivedFront(adURL: string): string | undefined {
  const i = adURL.lastIndexOf('/chirp/v1/')
  return i < 0 ? undefined : adURL.slice(0, i) + FrontSuffix
}

export const concatBytes = concat
