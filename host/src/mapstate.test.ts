/**
 * The host's map state against the codec's (keymap.ts), which the golden
 * vectors hold: the same fold verdicts token by token, the same roots, and
 * byte-identical object answers and pages, each passing a reader's checks.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { KeyMap, checkObject, checkPage, encodeEntryContext, encodeTokenContext, foldChain, proveObject, provePage, resolveBatch } from './keymap.js'
import { MapState, cmpKey, proveObjectFast, provePageFast } from './mapstate.js'
import type { Bucket, Change } from './records.js'
import { compareText, sha256, toHex, utf8 } from './util.js'

/** A deterministic generator, so a failure is reproducible. */
function rng(seed: number): () => number {
  let s = seed >>> 0
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0
    return s / 2 ** 32
  }
}

const alphabet = ['a', 'b', 'c', 'é', 'ß', '中', '\u{1F600}', '\u{FF21}', '\u{E000}', '/', 'A', '0']

function key(r: () => number): string {
  let k = ''
  const n = 1 + Math.floor(r() * 4)
  for (let i = 0; i < n; i++) k += alphabet[Math.floor(r() * alphabet.length)]!
  return k
}

let counter = 0
const fresh = (): Uint8Array => sha256(utf8(`c/${counter++}`))

test('cmpKey orders strings as their UTF-8 bytes do', () => {
  const r = rng(7)
  for (let i = 0; i < 20000; i++) {
    const a = key(r)
    const b = key(r)
    assert.equal(Math.sign(cmpKey(a, b)), Math.sign(compareText(a, b)), `${a} ${b}`)
  }
})

/** A chain of honest tokens over random puts and removals, with a dishonest one now and then. */
function chain(seed: number, tokens: number): Bucket[] {
  const r = rng(seed)
  const out: Bucket[] = []
  let m = new KeyMap()
  let named = new Set<string>()
  for (let seq = 0; seq < tokens; seq++) {
    const b: Bucket = {
      bucket: 'photos_qzvmkwerty', owner: new Uint8Array(33), id: new Uint8Array(32), seq, prev: new Uint8Array(32), writers: [], changes: [], count: 0,
      mapRoot: m.root(), created: 1, consumed: [], extra: [],
    }
    if (seq > 0) {
      const batch = []
      for (let i = 0; i < 1 + Math.floor(r() * 6); i++) {
        const k = r() < 0.6 || m.size === 0 ? key(r) : m.keys()[Math.floor(r() * m.size)]!
        batch.push({ c: fresh(), key: k, kind: r() < 0.25 ? 2 : 1, created: Math.floor(r() * 5) })
      }
      const [changes, consumed] = resolveBatch(m, named, batch)
      const next = m.clone()
      for (const ch of changes) {
        if (ch.removal) next.m.delete(ch.key)
        else next.m.set(ch.key, ch.c)
        named.add(toHex(ch.c))
      }
      for (const c of consumed) named.add(toHex(c))
      b.changes = changes
      b.consumed = consumed
      b.count = next.size
      b.mapRoot = next.root()
      // Now and then a dishonest token: each reason the fold names.
      const bad = r()
      if (bad < 0.04 && changes.length > 0) b.mapRoot = sha256(utf8('wrong'))
      else if (bad < 0.07) b.count++
      else if (bad < 0.09 && m.size > 0) b.changes = [...changes.filter((c) => c.key !== m.keys()[0]), { key: m.keys()[0]!, c: m.m.get(m.keys()[0]!)!, removal: false }].sort((x, y) => compareText(x.key, y.key))
      else if (bad < 0.11) b.changes = [...changes.filter((c) => c.key !== 'zzz-absent'), { key: 'zzz-absent', c: fresh(), removal: true } as Change].sort((x, y) => compareText(x.key, y.key))
      else if (bad < 0.13 && out.length > 1 && out[1]!.changes.length > 0) b.consumed = [...consumed, out[1]!.changes[0]!.c].sort((x, y) => (toHex(x) < toHex(y) ? -1 : 1))
      else {
        m = next
      }
      if (bad >= 0.13) named = new Set(named)
    }
    out.push(b)
  }
  return out
}

test('the fold reaches the codec verdict, reason and root on every token of a thousand chains', () => {
  const reasons = new Set<string>()
  for (let s = 1; s <= 1000; s++) {
    const tokens = chain(s, 12)
    const want = foldChain(tokens)
    for (const x of want.reasons) if (x !== '') reasons.add(x)
    const m = new MapState()
    let broken = false
    tokens.forEach((t, i) => {
      if (broken) {
        assert.equal(want.statuses[i], 'map-mismatch')
        return
      }
      const r = m.apply(t)
      assert.equal(r ?? '', want.reasons[i], `chain ${s} seq ${i}`)
      assert.equal(r === undefined ? 'consistent' : 'map-mismatch', want.statuses[i], `chain ${s} seq ${i}`)
      if (r !== undefined) broken = true
    })
    assert.equal(toHex(m.root), toHex(want.map.root()), `chain ${s}`)
    assert.deepEqual(m.keys, want.map.keys(), `chain ${s}`)
  }
  // Every reason a chain can reach was reached.
  assert.deepEqual([...reasons].sort(), ['count', 'map-root', 'named-twice', 'removal-of-absent'])
})

test('object answers and pages are byte for byte the codec honest ones, and pass the reader checks', () => {
  const r = rng(99)
  for (let s = 0; s < 300; s++) {
    const n = Math.floor(r() * 40)
    const pairs = new Map<string, Uint8Array>()
    for (let i = 0; i < n; i++) pairs.set(key(r), fresh())
    const km = new KeyMap([...pairs])
    const keys = km.keys()
    const st = new MapState()
    assert.equal(st.apply({ bucket: 'b', owner: new Uint8Array(33), id: new Uint8Array(32), seq: 1, prev: new Uint8Array(32), writers: [], changes: keys.map((k) => ({ key: k, c: km.m.get(k)!, removal: false })), count: keys.length, mapRoot: km.root(), created: 1, consumed: [], extra: [] }), undefined)
    const unheld = new Set(keys.filter(() => r() < 0.1).map((k) => toHex(km.m.get(k)!)))
    const held = (c: Uint8Array): boolean => !unheld.has(toHex(c))
    for (let i = 0; i < 20; i++) {
      const k = r() < 0.5 && keys.length > 0 ? keys[Math.floor(r() * keys.length)]! : key(r)
      const [a1, l1] = proveObject(km, k, held)
      const [a2, l2] = proveObjectFast(st, k, held)
      assert.equal(toHex(encodeTokenContext(l2)), toHex(encodeTokenContext(l1)))
      assert.equal(a2 === undefined ? '' : toHex(encodeEntryContext(a2)), a1 === undefined ? '' : toHex(encodeEntryContext(a1)))
      checkObject(km.size, km.root(), k, a2 === undefined ? undefined : km.m.get(k), a2, l2)
      const p = r() < 0.4 ? '' : [...key(r)][0]!
      const a = r() < 0.5 ? '' : key(r)
      const max = 1 + Math.floor(r() * 6)
      const [e1, p1] = provePage(km, p, a, max, held)
      const [e2, p2] = provePageFast(st, p, a, max, held)
      assert.equal(toHex(encodeTokenContext(p2)), toHex(encodeTokenContext(p1)), `page ${p} ${a} ${max}`)
      assert.deepEqual(e2.map((e) => [e.key, toHex(e.c), toHex(encodeEntryContext(e.ctx))]), e1.map((e) => [e.key, toHex(e.c), toHex(encodeEntryContext(e.ctx))]))
      checkPage(km.size, km.root(), p, a, max, e2, p2)
    }
  }
})
