/** Byte helpers shared by the codec. */
import { Hash } from '@bsv/sdk'
import { compareBytes } from '@lightwebinc/bcommon'

export const fromHex = (s: string): Uint8Array => Uint8Array.from(s.match(/../g) ?? [], (b) => parseInt(b, 16))
export const toHex = (b: Uint8Array | number[]): string => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')

export function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let at = 0
  for (const p of parts) {
    out.set(p, at)
    at += p.length
  }
  return out
}

export const sha256 = (...parts: Uint8Array[]): Uint8Array => Uint8Array.from(Hash.sha256(Array.from(concat(...parts))))

const text = new TextEncoder()
/** The UTF-8 bytes of a string; a lone surrogate becomes U+FFFD, as Go's JSON reading does. */
export const utf8 = (s: string): Uint8Array => text.encode(s)

/** Compares two strings by their UTF-8 bytes, as Go compares strings. */
export const compareText = (a: string, b: string): number => compareBytes(utf8(a), utf8(b))

/** Whether s is n lowercase hex digits. */
export const isLowerHex = (s: string, n: number): boolean => s.length === n && /^[0-9a-f]*$/.test(s)

/** The display txid of a hash-order txid: its bytes reversed, in hex. */
export const displayTxid = (c: Uint8Array): string => toHex(Uint8Array.from(c).reverse())

/** The hash-order bytes of a display txid. */
export const hashOrder = (txid: string): Uint8Array => fromHex(txid).reverse()

/** n as size bytes big-endian. */
export function bigEndian(n: number | bigint, size: number): Uint8Array {
  const out = new Uint8Array(size)
  let v = BigInt(n)
  for (let i = size - 1; i >= 0; i--) {
    out[i] = Number(v & 0xffn)
    v >>= 8n
  }
  return out
}

/** n as size bytes little-endian. */
export function littleEndian(n: number | bigint, size: number): Uint8Array {
  return bigEndian(n, size).reverse()
}

export const equal = (a: Uint8Array, b: Uint8Array): boolean => a.length === b.length && a.every((x, i) => x === b[i])
