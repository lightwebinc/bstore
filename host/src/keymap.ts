/**
 * The key map: leaves and roots, the fold of a chain, the owner's batch
 * resolution, contexts, object answers and the page checks of spec
 * section 11.3; the twin of the Go keymap.go. Keys compare by their UTF-8
 * bytes, as Go compares strings.
 */
import { compareBytes, decodeValue, encode, hashLeaf, proveBytes, rootOfBytes, verifyAt, type Value } from '@lightwebinc/bcommon'
import { isObjectKey } from './names.js'
import { KindDelete, KindPut, MaxEntryContext, MaxKeysInMap, MaxMapPath, MaxTokenContext, StoreError, refuse } from './storerec.js'
import type { Bucket, Change } from './records.js'
import { bigEndian, compareText, concat, equal, fromHex, toHex, utf8 } from './util.js'

export const leaf = (key: string, c: Uint8Array): Uint8Array => {
  const k = utf8(key)
  return concat(bigEndian(k.length, 2), k, c)
}

/** A key map: key to commitment. */
export class KeyMap {
  readonly m = new Map<string, Uint8Array>()
  constructor(pairs: [string, Uint8Array][] = []) {
    for (const [k, c] of pairs) this.m.set(k, c)
  }
  clone(): KeyMap {
    return new KeyMap([...this.m.entries()])
  }
  get size(): number {
    return this.m.size
  }
  keys(): string[] {
    return [...this.m.keys()].sort(compareText)
  }
  leaves(): Uint8Array[] {
    return this.keys().map((k) => leaf(k, this.m.get(k)!))
  }
  root(): Uint8Array {
    return rootOfBytes(this.leaves())
  }
  path(keys: string[], i: number): Uint8Array[] {
    return proveBytes(keys.map((k) => leaf(k, this.m.get(k)!)), i).map((s) => s.hash)
  }
  leafAt(keys: string[], role: number, i: number): MapLeaf {
    return { role, index: i, key: keys[i]!, c: this.m.get(keys[i]!)!, path: this.path(keys, i) }
  }
}

// ---- the fold ----

export interface Fold {
  statuses: string[]
  reasons: string[]
  map: KeyMap
  named: Set<string>
}

function apply(m: KeyMap, named: Set<string>, t: Bucket): [KeyMap, Set<string>] | string {
  const next = m.clone()
  const seen = new Set(named)
  const name = (c: Uint8Array): boolean => {
    const k = toHex(c)
    if (seen.has(k)) return false
    seen.add(k)
    return true
  }
  for (const c of t.changes) {
    if (!name(c.c)) return 'named-twice'
    const cur = next.m.get(c.key)
    if (c.removal) {
      if (cur === undefined) return 'removal-of-absent'
      next.m.delete(c.key)
      continue
    }
    if (cur !== undefined && equal(cur, c.c)) return 'put-unchanged'
    next.m.set(c.key, c.c)
  }
  for (const c of t.consumed) if (!name(c)) return 'named-twice'
  if (next.size !== t.count) return 'count'
  if (next.size > MaxKeysInMap) return 'too-many-keys'
  if (!equal(next.root(), t.mapRoot)) return 'map-root'
  return [next, seen]
}

/** Folds a chain's rooted tokens in seq order from 0; map-mismatch is sticky. */
export function foldChain(tokens: Bucket[]): Fold {
  const f: Fold = { statuses: [], reasons: [], map: new KeyMap(), named: new Set() }
  let broken = false
  tokens.forEach((t, i) => {
    if (t.seq !== i) throw new Error(`token ${i} has seq ${t.seq}`)
    if (broken) {
      f.statuses.push('map-mismatch')
      f.reasons.push('')
      return
    }
    const r = apply(f.map, f.named, t)
    if (typeof r === 'string') {
      broken = true
      f.statuses.push('map-mismatch')
      f.reasons.push(r)
      return
    }
    ;[f.map, f.named] = r
    f.statuses.push('consistent')
    f.reasons.push('')
  })
  return f
}

export interface Applicable {
  c: Uint8Array
  key: string
  kind: number
  created: number
}

/** The owner's tooling: changes ascending by key, and consumed ascending. */
export function resolveBatch(current: KeyMap, named: Set<string>, batch: Applicable[]): [Change[], Uint8Array[]] {
  const byKey = new Map<string, Applicable[]>()
  const taken = new Set<string>()
  for (const e of batch) {
    const h = toHex(e.c)
    if (named.has(h) || taken.has(h) || (e.kind !== KindPut && e.kind !== KindDelete)) continue
    taken.add(h)
    byKey.set(e.key, [...(byKey.get(e.key) ?? []), e])
  }
  const changes: Change[] = []
  const consumed: Uint8Array[] = []
  for (const k of [...byKey.keys()].sort(compareText)) {
    const es = byKey.get(k)!.sort((a, b) => (a.created !== b.created ? a.created - b.created : compareBytes(a.c, b.c)))
    for (const e of es.slice(0, -1)) consumed.push(e.c)
    const last = es[es.length - 1]!
    const cur = current.m.get(k)
    if ((last.kind === KindDelete && cur === undefined) || (last.kind === KindPut && cur !== undefined && equal(cur, last.c))) consumed.push(last.c)
    else changes.push({ key: k, c: last.c, removal: last.kind === KindDelete })
  }
  consumed.sort(compareBytes)
  return [changes, consumed]
}

// ---- contexts ----

export const RoleBefore = 0
export const RoleAfter = 1
export const RoleUnheld = 2

export interface EntryContext {
  index: number
  path: Uint8Array[]
}

export interface MapLeaf {
  role: number
  index: number
  key: string
  c: Uint8Array
  path: Uint8Array[]
}

const mapErr = (rule: string): StoreError => refuse('map', rule)

export const encodeEntryContext = (c: EntryContext): Uint8Array => encode([BigInt(c.index), c.path])
export const encodeTokenContext = (ls: MapLeaf[]): Uint8Array => encode(ls.map((l) => [BigInt(l.role), BigInt(l.index), l.key, l.c, l.path]))

function readPath(v: Value | undefined): Uint8Array[] | undefined {
  if (!Array.isArray(v) || v.length > MaxMapPath) return undefined
  if (!v.every((e) => e instanceof Uint8Array && e.length === 32)) return undefined
  return v as Uint8Array[]
}

function decodeCtx(raw: Uint8Array, max: number): Value {
  if (raw.length > max) throw mapErr('too-large')
  try {
    return decodeValue(raw)
  } catch {
    throw mapErr('cbor')
  }
}

export function decodeEntryContext(raw: Uint8Array): EntryContext {
  const v = decodeCtx(raw, MaxEntryContext)
  if (!Array.isArray(v) || v.length !== 2) throw mapErr('shape')
  const [idx, p] = v
  const path = readPath(p)
  if (typeof idx !== 'bigint' || idx < 0n || idx >= BigInt(MaxKeysInMap) || path === undefined) throw mapErr('shape')
  return { index: Number(idx), path }
}

export function decodeTokenContext(raw: Uint8Array): MapLeaf[] {
  const v = decodeCtx(raw, MaxTokenContext)
  if (!Array.isArray(v) || v.length > 2) throw mapErr('shape')
  const out: MapLeaf[] = []
  for (const e of v) {
    if (!Array.isArray(e) || e.length !== 5) throw mapErr('shape')
    const [role, idx, key, c, p] = e
    const path = readPath(p)
    if (typeof role !== 'bigint' || typeof idx !== 'bigint' || typeof key !== 'string' || !(c instanceof Uint8Array) || path === undefined ||
      role < 0n || role > 2n || idx < 0n || idx >= BigInt(MaxKeysInMap) || c.length !== 32 || !isObjectKey(key)) throw mapErr('shape')
    const l: MapLeaf = { role: Number(role), index: Number(idx), key, c, path }
    if (out.length > 0 && out[out.length - 1]!.role >= l.role) throw mapErr('shape')
    out.push(l)
  }
  if (out.length === 2 && out[0]!.role === RoleAfter && out[1]!.role === RoleUnheld) throw mapErr('shape')
  return out
}

const proves = (key: string, c: Uint8Array, index: number, path: Uint8Array[], count: number, root: Uint8Array): boolean =>
  verifyAt(hashLeaf(leaf(key, c)), index, count, path, root)

export type ObjectResult = 'found' | 'absent' | 'unheld'

/** A reader's step 2 for an object answer. */
export function checkObject(count: number, root: Uint8Array, k: string, entry: Uint8Array | undefined, ctx: EntryContext | undefined,
  leaves: MapLeaf[]): [ObjectResult, Uint8Array | undefined] {
  if (entry !== undefined) {
    if (leaves.length !== 0 || ctx === undefined) throw mapErr('object-shape')
    if (!proves(k, entry, ctx.index, ctx.path, count, root)) throw mapErr('path')
    return ['found', entry]
  }
  for (const l of leaves) if (!proves(l.key, l.c, l.index, l.path, count, root)) throw mapErr('path')
  if (leaves.length === 1 && leaves[0]!.role === RoleUnheld) {
    if (leaves[0]!.key !== k) throw mapErr('object-unheld')
    return ['unheld', leaves[0]!.c]
  }
  let r0: MapLeaf | undefined
  let r1: MapLeaf | undefined
  for (const l of leaves) {
    if (l.role === RoleBefore) r0 = l
    else if (l.role === RoleAfter) r1 = l
    else throw mapErr('object-absence')
  }
  let next = 0
  if (r0 !== undefined) {
    if (compareText(r0.key, k) >= 0) throw mapErr('object-absence')
    next = r0.index + 1
  }
  if (r1 !== undefined) {
    if (r1.index !== next || compareText(r1.key, k) <= 0) throw mapErr('object-absence')
  } else if (next !== count) {
    throw mapErr('object-absence')
  }
  if (r0 === undefined && next !== 0) throw mapErr('object-absence')
  return ['absent', undefined]
}

export interface PageEntry {
  key: string
  c: Uint8Array
  ctx: EntryContext
}

export interface PageKey {
  index: number
  key: string
}

const startsWith = (k: string, p: string): boolean => {
  const kb = utf8(k)
  const pb = utf8(p)
  return pb.length <= kb.length && equal(kb.subarray(0, pb.length), pb)
}
const inRange = (k: string, p: string, a: string): boolean => startsWith(k, p) && compareText(k, a) > 0
const below = (k: string, p: string, a: string): boolean => compareText(k, a) <= 0 || compareText(k, p) < 0
const above = (k: string, p: string, a: string): boolean => compareText(k, a) > 0 && !startsWith(k, p) && compareText(k, p) > 0

/** Checks 1 to 4 of spec section 11.3 on positions whose paths are proven. */
export function checkPageOrder(count: number, p: string, a: string, max: number, answered: PageKey[], r0?: PageKey, r1?: PageKey, r2?: PageKey): void {
  const s = answered.length > 0 ? answered[0]!.index : r2 !== undefined ? r2.index : r1 !== undefined ? r1.index : count
  answered.forEach((e, i) => {
    if (!inRange(e.key, p, a) || e.index !== s + i) throw mapErr('check-1')
  })
  if ((r0 !== undefined) !== s > 0) throw mapErr('check-2')
  if (r0 !== undefined && (r0.index !== s - 1 || !below(r0.key, p, a))) throw mapErr('check-2')
  const next = s + answered.length
  if (r2 !== undefined && (r2.index !== next || !inRange(r2.key, p, a) || r1 !== undefined)) throw mapErr('check-3')
  if (answered.length !== max && r2 === undefined) {
    if ((r1 !== undefined) !== next < count) throw mapErr('check-4')
    if (r1 !== undefined && (r1.index !== next || !above(r1.key, p, a))) throw mapErr('check-4')
  }
}

/** A reader's check of a list or export page: every path, then checks 1 to 4. */
export function checkPage(count: number, root: Uint8Array, p: string, a: string, max: number, entries: PageEntry[], leaves: MapLeaf[]): void {
  for (const e of entries) if (!proves(e.key, e.c, e.ctx.index, e.ctx.path, count, root)) throw mapErr('path')
  let r0: PageKey | undefined
  let r1: PageKey | undefined
  let r2: PageKey | undefined
  for (const l of leaves) {
    if (!proves(l.key, l.c, l.index, l.path, count, root)) throw mapErr('path')
    const pk = { index: l.index, key: l.key }
    if (l.role === RoleBefore) r0 = pk
    else if (l.role === RoleAfter) r1 = pk
    else r2 = pk
  }
  checkPageOrder(count, p, a, max, entries.map((e) => ({ index: e.ctx.index, key: e.key })), r0, r1, r2)
}

/** The honest page for prefix p after a, stopping at the first entry held refuses. */
export function provePage(m: KeyMap, p: string, a: string, max: number, held: (c: Uint8Array) => boolean): [PageEntry[], MapLeaf[]] {
  const keys = m.keys()
  const n = keys.length
  let s = keys.findIndex((k) => !below(k, p, a))
  if (s < 0) s = n
  const entries: PageEntry[] = []
  const leaves: MapLeaf[] = []
  if (s > 0) leaves.push(m.leafAt(keys, RoleBefore, s - 1))
  let i = s
  for (; i < n && entries.length < max && inRange(keys[i]!, p, a); i++) {
    if (!held(m.m.get(keys[i]!)!)) return [entries, [...leaves, m.leafAt(keys, RoleUnheld, i)]]
    entries.push({ key: keys[i]!, c: m.m.get(keys[i]!)!, ctx: { index: i, path: m.path(keys, i) } })
  }
  if (entries.length < max && i < n) leaves.push(m.leafAt(keys, RoleAfter, i))
  return [entries, leaves]
}

/** The honest object answer for k. */
export function proveObject(m: KeyMap, k: string, held: (c: Uint8Array) => boolean): [EntryContext | undefined, MapLeaf[]] {
  const keys = m.keys()
  let i = keys.findIndex((x) => compareText(x, k) >= 0)
  if (i < 0) i = keys.length
  if (i < keys.length && keys[i] === k) {
    if (held(m.m.get(k)!)) return [{ index: i, path: m.path(keys, i) }, []]
    return [undefined, [m.leafAt(keys, RoleUnheld, i)]]
  }
  const out: MapLeaf[] = []
  if (i > 0) out.push(m.leafAt(keys, RoleBefore, i - 1))
  if (i < keys.length) out.push(m.leafAt(keys, RoleAfter, i))
  return [undefined, out]
}

export const mapFromView = (v: [string, string][]): KeyMap => new KeyMap(v.map(([k, c]) => [k, fromHex(c)]))
