/**
 * The key map of one branch of a served chain, as the host folds it (spec
 * sections 3.4 and 12.2): the fold of keymap.ts, the codec's, done in place
 * on flat buffers so that a token costs its changes and one tree over the
 * map, and kept with that tree so that an answer's paths cost a lookup each.
 *
 * keymap.ts's fold clones the map and sorts it again for every token, and
 * its paths rebuild the tree for every leaf; it is the reference, and the
 * tests hold this one to it token by token and page by page. Keys sort by
 * code point, which is their UTF-8 byte order (cmpKey); a key's leaf hash
 * is SHA-256(0x00 || uint16be(len(key)) || key || C), a node
 * SHA-256(0x01 || left || right), the root of no leaves SHA-256("").
 */
import { createHash } from 'node:crypto'
import { MaxKeysInMap } from './storerec.js'
import { RoleAfter, RoleBefore, RoleUnheld, type EntryContext, type MapLeaf, type PageEntry } from './keymap.js'
import type { Bucket } from './records.js'
import { utf8 } from './util.js'

const H = 32

function sha(...parts: Uint8Array[]): Buffer {
  const h = createHash('sha256')
  for (const p of parts) h.update(p)
  return h.digest()
}

const Zero = Uint8Array.of(0)
const One = Uint8Array.of(1)
/** SHA-256 of no bytes: the root of an empty map. */
export const EmptyMapRoot = sha()

/**
 * Compares two strings by code point, which for well-formed strings is the
 * order of their UTF-8 bytes (compareText), without encoding either: a
 * surrogate (U+D800 to U+DFFF, half of a code point above U+FFFF) sorts
 * after every other UTF-16 unit.
 */
export function cmpKey(a: string, b: string): number {
  const n = Math.min(a.length, b.length)
  for (let i = 0; i < n; i++) {
    let x = a.charCodeAt(i)
    let y = b.charCodeAt(i)
    if (x === y) continue
    if (x >= 0xd800) x = x <= 0xdfff ? x + 0x2000 : x - 0x800
    if (y >= 0xd800) y = y <= 0xdfff ? y + 0x2000 : y - 0x800
    return x - y
  }
  return a.length - b.length
}

/** A key's leaf hash. */
export function leafHash(key: string, c: Uint8Array): Buffer {
  const k = utf8(key)
  return sha(Zero, Uint8Array.of(k.length >> 8, k.length & 0xff), k, c)
}

/** Every level of the RFC 6962 tree over n leaf hashes, leaves first; an odd node is carried up unchanged. */
function levelsOf(hs: Buffer, n: number): Buffer[] {
  const out = [hs]
  let cur = hs
  let m = n
  while (m > 1) {
    const up = Math.ceil(m / 2)
    const next = Buffer.allocUnsafe(up * H)
    for (let i = 0; i + 1 < m; i += 2) sha(One, cur.subarray(i * H, i * H + H), cur.subarray(i * H + H, i * H + 2 * H)).copy(next, (i >> 1) * H)
    if (m % 2 === 1) cur.copy(next, (up - 1) * H, (m - 1) * H, m * H)
    out.push(next)
    cur = next
    m = up
  }
  return out
}

/** A commitment as the key of the named set: its 32 bytes as a latin1 string. */
const bin = (c: Uint8Array): string => Buffer.from(c.buffer, c.byteOffset, c.byteLength).toString('latin1')

/** Why a token does not fold, as keymap.ts names it, or undefined. */
export type FoldReason = 'named-twice' | 'removal-of-absent' | 'put-unchanged' | 'count' | 'too-many-keys' | 'map-root'

interface Edit {
  key: string
  /** The commitment the key names after the token; null for a removal. */
  c: Uint8Array | null
  /** Where the key is in the map before the token, or where it would go. */
  at: number
  found: boolean
}

export class MapState {
  /** The keys, ascending by code point. */
  keys: string[] = []
  /** The commitment of each key, 32 bytes each, in key order. */
  private cs: Buffer = Buffer.alloc(0)
  /** The tree: levels[0] the leaf hashes in key order, the last level the root. */
  private levels: Buffer[] = [Buffer.alloc(0)]
  /** Every commitment a token of the branch named, as latin1 strings of its 32 bytes. */
  private readonly named = new Set<string>()

  get size(): number {
    return this.keys.length
  }

  /** The map root. */
  get root(): Uint8Array {
    return this.keys.length === 0 ? EmptyMapRoot : this.levels[this.levels.length - 1]!.subarray(0, H)
  }

  /** Whether a token of the branch named commitment c. */
  isNamed(c: Uint8Array): boolean {
    return this.named.has(bin(c))
  }

  /** The index of the first key at or above k. */
  lower(k: string): number {
    let lo = 0
    let hi = this.keys.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (cmpKey(this.keys[mid]!, k) < 0) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  /** The index of the first key above k. */
  upper(k: string): number {
    let lo = 0
    let hi = this.keys.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (cmpKey(this.keys[mid]!, k) <= 0) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  /** The commitment at index i. */
  cAt(i: number): Uint8Array {
    return this.cs.subarray(i * H, i * H + H)
  }

  /** The commitment key k names, or undefined. */
  get(k: string): Uint8Array | undefined {
    const i = this.lower(k)
    return this.keys[i] === k ? this.cAt(i) : undefined
  }

  /** Every commitment the map holds, in key order. */
  *commitments(): Generator<Uint8Array> {
    for (let i = 0; i < this.keys.length; i++) yield this.cAt(i)
  }

  /** The audit path of leaf i, leaf to root (RFC 6962 section 2.1.1). */
  path(i: number): Uint8Array[] {
    const out: Uint8Array[] = []
    let m = this.keys.length
    for (let l = 0; m > 1; l++) {
      const lv = this.levels[l]!
      const sib = i ^ 1
      if (sib < m) out.push(Uint8Array.from(lv.subarray(sib * H, sib * H + H)))
      i >>= 1
      m = Math.ceil(m / 2)
    }
    return out
  }

  /** The leaf at index i, in full, with its path. */
  leafAt(role: number, i: number): MapLeaf {
    return { role, index: i, key: this.keys[i]!, c: Uint8Array.from(this.cAt(i)), path: this.path(i) }
  }

  /**
   * Folds one token onto the map (spec section 3.4), as keymap.ts's fold
   * does: every change must change the map, no commitment may be named
   * twice in the branch, and the map after must hold `count` keys under
   * `mapRoot`. Returns why it does not fold, and leaves the map as it was;
   * or applies it and returns undefined.
   */
  apply(t: Bucket): FoldReason | undefined {
    const seen = new Set<string>()
    const name = (c: Uint8Array): boolean => {
      const k = bin(c)
      if (this.named.has(k) || seen.has(k)) return false
      seen.add(k)
      return true
    }
    const edits: Edit[] = []
    let size = this.keys.length
    for (const ch of t.changes) {
      if (!name(ch.c)) return 'named-twice'
      const at = this.lower(ch.key)
      const found = this.keys[at] === ch.key
      const cur = found ? this.cAt(at) : undefined
      if (ch.removal) {
        if (cur === undefined) return 'removal-of-absent'
        edits.push({ key: ch.key, c: null, at, found })
        size--
        continue
      }
      if (cur !== undefined && Buffer.compare(cur, ch.c) === 0) return 'put-unchanged'
      edits.push({ key: ch.key, c: ch.c, at, found })
      if (!found) size++
    }
    for (const c of t.consumed) if (!name(c)) return 'named-twice'
    if (size !== t.count) return 'count'
    if (size > MaxKeysInMap) return 'too-many-keys'
    const keys: string[] = new Array<string>(size)
    const cs = Buffer.allocUnsafe(size * H)
    const hs = Buffer.allocUnsafe(size * H)
    const old = this.levels[0]!
    let from = 0
    let to = 0
    const copy = (until: number): void => {
      for (let i = from; i < until; i++) keys[to + i - from] = this.keys[i]!
      this.cs.copy(cs, to * H, from * H, until * H)
      old.copy(hs, to * H, from * H, until * H)
      to += until - from
      from = until
    }
    // The changes are ascending by key (the record's rule), so their places are too.
    for (const e of edits) {
      copy(e.at)
      if (e.found) from++
      if (e.c === null) continue
      keys[to] = e.key
      cs.set(e.c, to * H)
      leafHash(e.key, e.c).copy(hs, to * H)
      to++
    }
    copy(this.keys.length)
    const levels = levelsOf(hs, size)
    const root = size === 0 ? EmptyMapRoot : levels[levels.length - 1]!.subarray(0, H)
    if (Buffer.compare(root, t.mapRoot) !== 0) return 'map-root'
    this.keys = keys
    this.cs = cs
    this.levels = levels
    for (const k of seen) this.named.add(k)
    return undefined
  }
}

// ---- answers: the honest object answer and page (keymap.ts proveObject and provePage, over the kept tree)

const startsWith = (k: string, p: string): boolean => k.startsWith(p)
const inRange = (k: string, p: string, a: string): boolean => startsWith(k, p) && cmpKey(k, a) > 0

/** The honest object answer for k: the entry's context, or the leaves that prove k absent or name its entry unheld. */
export function proveObjectFast(m: MapState, k: string, held: (c: Uint8Array) => boolean): [EntryContext | undefined, MapLeaf[]] {
  const i = m.lower(k)
  if (m.keys[i] === k) {
    if (held(m.cAt(i))) return [{ index: i, path: m.path(i) }, []]
    return [undefined, [m.leafAt(RoleUnheld, i)]]
  }
  const out: MapLeaf[] = []
  if (i > 0) out.push(m.leafAt(RoleBefore, i - 1))
  if (i < m.size) out.push(m.leafAt(RoleAfter, i))
  return [undefined, out]
}

/** The honest page for prefix p after a, at most max entries, stopping at the first entry the host cannot produce. */
export function provePageFast(m: MapState, p: string, a: string, max: number, held: (c: Uint8Array) => boolean): [PageEntry[], MapLeaf[]] {
  const n = m.size
  // The first key that is neither at or before a nor before p.
  const s = Math.max(m.upper(a), m.lower(p))
  const entries: PageEntry[] = []
  const leaves: MapLeaf[] = []
  if (s > 0) leaves.push(m.leafAt(RoleBefore, s - 1))
  let i = s
  for (; i < n && entries.length < max && inRange(m.keys[i]!, p, a); i++) {
    const c = m.cAt(i)
    if (!held(c)) return [entries, [...leaves, m.leafAt(RoleUnheld, i)]]
    entries.push({ key: m.keys[i]!, c: Uint8Array.from(c), ctx: { index: i, path: m.path(i) } })
  }
  if (entries.length < max && i < n) leaves.push(m.leafAt(RoleAfter, i))
  return [entries, leaves]
}
