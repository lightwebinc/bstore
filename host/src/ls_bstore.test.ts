/**
 * ls_bstore's classes (spec section 11.1) through the real engine: tokens
 * in any order, the fold of a served chain, object answers and listing
 * pages checked as a reader checks them, pending and unanchored, entries,
 * advertisements, fronts, holds, outcomes, sweeps, history and chain, and
 * the questions it refuses.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { MaxKeysInMap } from './storerec.js'
import { decodeAdvertisement } from './advert.js'
import { Buyer, Chain, T0, commitment, sweep, type Posted } from './testmint.js'
import { beefOf, byName, txOf, txVectors } from './testutil.js'
import { answer, bucket, outputs, refused, submit, submitTx } from './testhost.js'
import { Day, list, object, readPage, world } from './testworld.js'
import { KeyEntry, KeyFront, KeyOutcome, OutcomePass } from './storerec.js'
import { toHex } from './util.js'

test('tokens: pending until rooted, latest by seq, every token at a seq, and chain in seq order, whatever order they arrive in', async () => {
  const w = await world()
  const e1 = await w.alice.put(w.owner.id, 'a.txt')
  const t0 = await w.owner.token()
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: [e1] })
  const t2 = await w.owner.token()
  assert.deepEqual(await submitTx(w.r, e1.tx), [0])
  assert.deepEqual(await submit(w.r, t2.beef), [0])
  assert.deepEqual(await submit(w.r, t1.beef), [0])
  assert.equal(w.r.ls.tokenStatus(t2.txid), 'pending')
  assert.deepEqual(await answer(w.r, w.chainQ), [], 'nothing is rooted yet')
  assert.deepEqual(await submit(w.r, t0.beef), [0])
  for (const t of [t0, t1, t2]) assert.equal(w.r.ls.tokenStatus(t.txid), 'consistent')
  assert.deepEqual(await answer(w.r, w.chainQ), [t2.txid])
  assert.deepEqual(await answer(w.r, { ...w.chainQ, seq: '1' }), [t1.txid])
  assert.deepEqual(await answer(w.r, { bucket, owner: w.owner.hex, chain: w.owner.idHex, after: '0' }), [t1.txid, t2.txid])
  assert.deepEqual(await answer(w.r, { bucket, owner: w.owner.hex, chain: w.owner.idHex, after: '2' }), [])
  assert.equal(w.r.host.count('bstore_token_status_total', { status: 'consistent' }), 3)
})

test('a token that does not fold is map-mismatch, sticky; latest stays the last consistent one, and the map is its map', async () => {
  const w = await world()
  const e1 = await w.alice.put(w.owner.id, 'a.txt')
  const e2 = await w.alice.put(w.owner.id, 'b.txt')
  const t0 = await w.owner.token()
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: [e1] })
  // The next token claims a root its changes do not reproduce.
  const bad = await w.owner.token({ anchor: [e2], mutate: (b) => (b.mapRoot = new Uint8Array(32).fill(7)) })
  const after = await w.owner.token()
  for (const x of [e1, e2]) await submitTx(w.r, x.tx)
  for (const t of [t0, t1, bad, after]) assert.deepEqual(await submit(w.r, t.beef), [0])
  assert.equal(w.r.ls.tokenStatus(bad.txid), 'map-mismatch')
  assert.equal(w.r.ls.tokenStatus(after.txid), 'map-mismatch')
  assert.deepEqual(await answer(w.r, w.chainQ), [t1.txid])
  const o = await object(w, 'b.txt')
  assert.deepEqual([o?.result, o?.token], ['absent', t1.txid], 'b.txt is absent from the map of the last consistent token')
  assert.equal(w.r.host.lines.some((l) => l.msg === 'ls_bstore token status' && l.extra?.['reason'] === 'map-root'), true)
  // An unserved chain is rooted, never folded.
  const u = await world({ served: false, seed: 'u' })
  const u0 = await u.owner.token()
  const u1 = await u.owner.token({ mutate: (b) => (b.count = 5) })
  for (const t of [u0, u1]) await submit(u.r, t.beef)
  assert.equal(u.r.ls.tokenStatus(u1.txid), 'rooted')
  assert.deepEqual(await answer(u.r, u.chainQ), [u1.txid])
  assert.match(await refused(u.r, { ...u.chainQ, key: 'a' }), /does not serve/)
})

test('object: found, absent and unheld, each proven against the latest token and passing a reader check', async () => {
  const w = await world()
  const es = []
  for (const k of ['a.txt', 'c.txt', 'é/x', 'z']) es.push(await w.alice.put(w.owner.id, k))
  await submit(w.r, (await w.owner.token()).beef)
  for (const e of es.slice(0, 3)) await submitTx(w.r, e.tx)
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: es })
  await submit(w.r, t1.beef)
  assert.deepEqual(await object(w, 'c.txt'), { result: 'found', entry: es[1]!.txid, token: t1.txid })
  assert.deepEqual(await object(w, 'b.txt'), { result: 'absent', token: t1.txid })
  assert.deepEqual(await object(w, '0'), { result: 'absent', token: t1.txid })
  assert.deepEqual(await object(w, 'zz'), { result: 'absent', token: t1.txid })
  // z's entry was never submitted here: the key exists and this host cannot produce it.
  assert.deepEqual(await object(w, 'z'), { result: 'unheld', token: t1.txid })
  await submitTx(w.r, es[3]!.tx)
  assert.deepEqual(await object(w, 'z'), { result: 'found', entry: es[3]!.txid, token: t1.txid })
  // A delete removes the key at the next token.
  const d = await w.alice.del(w.owner.id, 'c.txt', { created: T0 + 1 })
  await submitTx(w.r, d.tx)
  const t2 = await w.owner.token({ anchor: [d] })
  await submit(w.r, t2.beef)
  assert.deepEqual(await object(w, 'c.txt'), { result: 'absent', token: t2.txid })
})

test('list and export: complete pages, a role 2 leaf where an entry is not held, the next page after it', async () => {
  const w = await world()
  const keys = Array.from({ length: 300 }, (_, i) => `k/${i.toString().padStart(3, '0')}`)
  const es = []
  for (const k of keys) es.push(await w.alice.put(w.owner.id, k))
  es.push(await w.alice.put(w.owner.id, 'j'))
  es.push(await w.alice.put(w.owner.id, 'l'))
  await submit(w.r, (await w.owner.token()).beef)
  const missing = es[10]!
  for (const e of es) if (e !== missing) await submitTx(w.r, e.tx)
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: es })
  await submit(w.r, t1.beef)
  // The first page stops at the entry this host cannot produce, k/010.
  const p1 = await list(w, 'k/', '')
  assert.deepEqual(p1.keys, keys.slice(0, 10))
  assert.deepEqual(p1.leaves, [[0, 'j'], [2, 'k/010']])
  // Asked after it: a full page of 256.
  const p2 = await list(w, 'k/', 'k/010')
  assert.deepEqual(p2.keys, keys.slice(11, 11 + 256))
  assert.deepEqual(p2.leaves, [[0, 'k/010']])
  // The last page: role 1 strictly after the range.
  const p3 = await list(w, 'k/', keys[266]!)
  assert.deepEqual(p3.keys, keys.slice(267))
  assert.deepEqual(p3.leaves, [[0, keys[266]!], [1, 'l']])
  // An empty range brackets nothing.
  const empty = await list(w, 'm/', '')
  assert.deepEqual([empty.keys, empty.leaves], [[], [[0, 'l']]])
  // export: as list with no prefix, 512 a page, priced classes answered through the engine where the host prices nothing.
  await submitTx(w.r, missing.tx)
  const ex = readPage(await outputs(w.r, { bucket, owner: w.owner.hex, export: w.owner.idHex, after: '' }), '', '', true)
  assert.deepEqual(ex.keys, ['j', ...keys, 'l'])
  assert.deepEqual(ex.leaves, [])
})

test('pending: entries for a key no token names, by the owner or the latest writers, newest first, never dated after the host time', async () => {
  const w = await world()
  const t0 = await w.owner.token()
  await submit(w.r, t0.beef)
  const a1 = await w.alice.put(w.owner.id, 'p', { created: T0 })
  const a2 = await w.alice.put(w.owner.id, 'p', { created: T0 + 2 })
  const o1 = await w.owned.put(w.owner.id, 'p', { created: T0 + 1 })
  const s1 = await w.bob.put(w.owner.id, 'p', { created: T0 + 3 })
  const future = await w.owned.put(w.owner.id, 'p', { created: T0 + 2 * Day })
  const other = await w.alice.put(w.owner.id, 'q')
  for (const e of [a1, a2, o1, s1, future, other]) await submitTx(w.r, e.tx)
  const q = { ...w.chainQ, pending: 'p' }
  assert.deepEqual(await answer(w.r, q), [o1.txid], 'alice is not a writer yet; bob never; the future one never')
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: [] })
  await submit(w.r, t1.beef)
  assert.deepEqual(await answer(w.r, q), [a2.txid, o1.txid, a1.txid], "alice's too, now the latest token names her")
  const t2 = await w.owner.token({ anchor: [a1, o1] })
  await submit(w.r, t2.beef)
  assert.deepEqual(await answer(w.r, q), [a2.txid], 'named entries are not pending, whatever the token did with them')
  assert.equal((await object(w, 'p'))?.entry, o1.txid, 'the last by created won the batch')
})

test('unanchored: by writer, no token names it, within 7 days of admission, ascending by txid after the cursor', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const es = []
  for (let i = 0; i < 70; i++) es.push(await w.alice.put(w.owner.id, `u/${i}`))
  for (const e of es) await submitTx(w.r, e.tx)
  const sorted = es.map((e) => e.txid).sort()
  const q = { ...w.chainQ, writer: w.alice.hex, after: '' }
  assert.deepEqual(await answer(w.r, q), sorted.slice(0, 64))
  assert.deepEqual(await answer(w.r, { ...q, after: sorted[63]! }), sorted.slice(64))
  // Anchoring names them.
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: es.slice(0, 60) })
  await submit(w.r, t1.beef)
  const rest = es.slice(60).map((e) => e.txid).sort()
  assert.deepEqual(await answer(w.r, q), rest)
  // Admitted more than 7 days ago: no longer answered.
  w.clock.advance(8 * Day)
  assert.deepEqual(await answer(w.r, q), [])
  assert.deepEqual(await answer(w.r, { ...w.chainQ, writer: w.bob.hex, after: '' }), [])
})

test('entry and hold: by txid and by outpoint; ads and ad: live, unspent, unexpired, by txid after a cursor, a transaction never split', async () => {
  const w = await world()
  const h = w.alice.hold(5000)
  const e = await w.alice.put(w.owner.id, 'held', { hold: h.terms })
  await submitTx(w.r, e.tx)
  assert.deepEqual(await submitTx(w.r, h.tx), [0])
  assert.deepEqual(await answer(w.r, { bucket, entry: e.txid }), [e.txid])
  assert.deepEqual(await answer(w.r, { bucket, hold: h.op }), [h.tx.id('hex')])
  assert.deepEqual(await answer(w.r, { bucket, hold: `${h.tx.id('hex')}.1` }), [])
  const root = e.e.root
  const r = toHex(root)
  const exp = Math.floor(w.clock.ms / 1000) + 3600
  const ads = []
  for (let i = 0; i < 5; i++) ads.push(await w.prov.advertise([root, root, root, root, root, root, root, root, root, root, root, root, root, root, root, root], exp + i))
  for (const tx of ads) assert.equal((await submitTx(w.r, tx))?.length, 16)
  const byTxid = ads.map((tx) => tx.id('hex')).sort()
  const page = await outputs(w.r, { bucket, root: r, after: '' })
  // 64 outputs a page, four whole transactions.
  assert.deepEqual([...new Set(page.map((o) => o.txid))], byTxid.slice(0, 4))
  assert.equal(page.length, 64)
  assert.deepEqual([...new Set(await answer(w.r, { bucket, root: r, after: byTxid[3]! }))], byTxid.slice(4))
  // ad: by advertiser, at most 8; another advertiser's are not answered.
  const other = await w.prov2.advertise([root], exp)
  await submitTx(w.r, other)
  assert.deepEqual(await answer(w.r, { bucket, root: r, advertiser: w.prov2.hex }), [other.id('hex')])
  assert.deepEqual(await answer(w.r, { bucket, root: r, advertiser: w.prov.hex }), Array<string>(8).fill(byTxid[0]!), 'the first eight, at most')
  // A renewal spends the advertisement it renews: the old one is no longer answered.
  const renewed = await w.prov2.renew(other, 0, root, exp + 7200)
  assert.deepEqual(await submitTx(w.r, renewed), [0])
  assert.deepEqual(await answer(w.r, { bucket, root: r, advertiser: w.prov2.hex }), [renewed.id('hex')])
  // Expired by the host's clock: not answered.
  w.clock.advance(3 * 3600_000)
  assert.deepEqual(await answer(w.r, { bucket, root: r, after: '' }), [])
})

test("the anyone key's advertisement is admitted, as the SDK validates it, and never answered", async () => {
  const v = txVectors()
  const w = await world({ setup: { chain: new Chain(900000, v.headers) } })
  const t = byName(v, 'advertisement-anyone-key')
  assert.deepEqual(await submit(w.r, beefOf(t)), t.admits!.outputsToAdmit)
  const ad = decodeAdvertisement(Uint8Array.from(txOf(t).outputs[t.admits!.outputsToAdmit[0]!]!.lockingScript.toBinary()))
  assert.deepEqual(await answer(w.r, { bucket, root: toHex(ad.hash), after: '' }), [])
})

test('fronts and front: statements naming an advertiser by txid after a cursor, and by one front newest first', async () => {
  const w = await world()
  const fs = []
  for (let i = 0; i < 10; i++) fs.push(await w.front.statement(w.prov.party.id, { created: T0 + i }))
  const later = await w.front.statement(w.prov.party.id, { created: T0 + 2 * Day })
  for (const f of [...fs, later]) assert.deepEqual(await submitTx(w.r, f.tx), [0])
  const sorted = [...fs, later].map((f) => f.txid).sort()
  assert.deepEqual(await answer(w.r, { bucket, advertiser: w.prov.hex, after: '' }), sorted.slice(0, 8))
  assert.deepEqual(await answer(w.r, { bucket, advertiser: w.prov.hex, after: sorted[7]! }), sorted.slice(8))
  assert.deepEqual(await answer(w.r, { bucket, advertiser: w.prov.hex, front: w.front.hex }), fs.slice(2).reverse().map((f) => f.txid), 'newest first, never one dated after the host time')
})

test('outcomes and their kin: newest first, within 31 days of admission, never dated after the host time; history without a window, before a cursor', async () => {
  const w = await world()
  const root = new Uint8Array(32).fill(1)
  const os: Posted[] = []
  for (let i = 0; i < 3; i++) os.push(await w.buyer.outcome({ root, provider: w.prov.party, created: T0 + i }))
  const pass = await w.buyer.outcome({ root, provider: w.prov.party, answerer: w.front.party, outcome: OutcomePass, created: T0 + 10 })
  const future = await w.buyer.outcome({ root, provider: w.prov.party, created: T0 + 2 * Day })
  const byBob = await new Buyer(bucket, 'outcome bob', w.chain).outcome({ root, provider: w.prov.party, created: T0 + 5 })
  for (const o of [...os, pass, future, byBob]) assert.deepEqual(await submitTx(w.r, o.tx), [0])
  const newest = (xs: typeof os): string[] => xs.map((x) => x.txid)
  assert.deepEqual(await answer(w.r, { bucket, answerer: w.prov.hex }), newest([byBob, os[2]!, os[1]!, os[0]!]))
  assert.deepEqual(await answer(w.r, { bucket, answerer: w.front.hex }), [pass.txid])
  assert.deepEqual(await answer(w.r, { bucket, provider: w.prov.hex }), newest([pass, byBob, os[2]!, os[1]!, os[0]!]))
  assert.deepEqual(await answer(w.r, { bucket, answerer: w.prov.hex, buyer: w.buyer.hex }), newest([os[2]!, os[1]!, os[0]!]))
  assert.deepEqual(await answer(w.r, { bucket, provider: w.prov.hex, buyer: w.buyer.hex }), newest([pass, os[2]!, os[1]!, os[0]!]))
  // outcome-history: before a cursor, no window.
  const cursor = `${T0 + 2}:${os[2]!.txid}`
  assert.deepEqual(await answer(w.r, { bucket, 'outcome-history': w.prov.hex, before: cursor }), newest([os[1]!, os[0]!]))
  assert.deepEqual(await answer(w.r, { bucket, 'outcome-history-of': w.prov.hex, before: `${T0 + 3 * Day}:${'0'.repeat(64)}` }), newest([pass, byBob, os[2]!, os[1]!, os[0]!]))
  w.clock.advance(32 * Day)
  assert.deepEqual(await answer(w.r, { bucket, answerer: w.prov.hex }), [], 'admitted more than 31 days ago')
  assert.deepEqual(await answer(w.r, { bucket, 'outcome-history': w.prov.hex, before: cursor }), newest([os[1]!, os[0]!]))
})

test('history: the tokens below a seq whose changes set or remove a key, newest first, each followed by the entry its change names', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const v1 = await w.alice.put(w.owner.id, 'h', { label: 'one' })
  const v2 = await w.alice.put(w.owner.id, 'h', { label: 'two', created: T0 + 1 })
  const d = await w.alice.del(w.owner.id, 'h', { created: T0 + 2 })
  const other = await w.alice.put(w.owner.id, 'x')
  for (const e of [v1, v2, d, other]) await submitTx(w.r, e.tx)
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: [v1] })
  const t2 = await w.owner.token({ anchor: [other] })
  const t3 = await w.owner.token({ anchor: [v2] })
  const t4 = await w.owner.token({ anchor: [d] })
  for (const t of [t1, t2, t3, t4]) await submit(w.r, t.beef)
  const q = { ...w.chainQ, history: 'h', before: '9' }
  assert.deepEqual(await answer(w.r, q), [t4.txid, d.txid, t3.txid, v2.txid, t1.txid, v1.txid])
  assert.deepEqual(await answer(w.r, { ...q, before: '4' }), [t3.txid, v2.txid, t1.txid, v1.txid])
})

test('sweep: a swept carrier is retracted and never answered; the sweep is answered by the outpoint it spent', async () => {
  const w = await world()
  const e = await w.alice.put(w.owner.id, 's')
  await submitTx(w.r, e.tx)
  const sw = await sweep(e.tree, [e.vout, 40], w.alice.party, KeyEntry, w.chain)
  assert.deepEqual(await submitTx(w.r, sw), [0])
  assert.deepEqual(await answer(w.r, { bucket, entry: e.txid }), [])
  assert.deepEqual(await answer(w.r, { bucket, spent: e.op }), [sw.id('hex')])
  assert.deepEqual(await answer(w.r, { bucket, spent: `${e.tree.id('hex')}.40` }), [sw.id('hex')])
  assert.equal(w.r.host.count('bstore_retractions_total'), 1)
  // The replay: admitted by the manager, which reads nothing the topic holds, and dropped by the index at once.
  assert.deepEqual(await submitTx(w.r, e.tx), [0])
  assert.deepEqual(await answer(w.r, { bucket, entry: e.txid }), [])
  assert.equal(await w.r.storage.findOutput(e.txid, 0), null, 'deleted again')
  assert.equal(await w.r.storage.doesAppliedTransactionExist({ txid: e.txid, topic: `tm_bstore_${bucket}` }), false, 'and evicted, so a replay is decided afresh')
  // A sweep that matched nothing: its transaction is dropped, its outpoints kept, and a carrier that comes later is retracted.
  const late = await w.buyer.outcome({ root: new Uint8Array(32), provider: w.prov.party })
  const sw2 = await sweep(late.tree, [late.vout], w.buyer.party, KeyOutcome, w.chain)
  assert.deepEqual(await submitTx(w.r, sw2), [0])
  assert.deepEqual(await answer(w.r, { bucket, spent: late.op }), [], 'its transaction is not kept')
  assert.equal(w.r.ls.swept(late.op), true)
  assert.deepEqual(await submitTx(w.r, late.tx), [0])
  assert.deepEqual(await answer(w.r, { bucket, answerer: w.prov.hex }), [])
  assert.notEqual(w.r.ls.mark(late.op), undefined)
})

test('one carrier per funding outpoint keeps a floor: a later one on it is dropped, unless a served chain names it', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const first = await w.alice.put(w.owner.id, 'k', { created: T0 })
  const second = await w.alice.put(w.owner.id, 'k', { created: T0 + 1, vout: first.vout })
  const third = await w.alice.put(w.owner.id, 'k', { created: T0 + 2, vout: first.vout })
  assert.equal(second.op, first.op)
  for (const e of [first, second]) assert.deepEqual(await submitTx(w.r, e.tx), [0])
  assert.equal(w.r.ls.firstOn(first.op), first.c)
  assert.deepEqual(await answer(w.r, { bucket, entry: second.txid }), [])
  assert.equal(w.r.host.count('bstore_dropped_total', { why: 'second-carrier' }), 1)
  // Once a served chain names it, the same carrier offered again is held.
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: [third] })
  await submit(w.r, t1.beef)
  assert.deepEqual(await submitTx(w.r, third.tx), [0])
  assert.deepEqual(await answer(w.r, { bucket, entry: third.txid }), [third.txid])
  assert.equal((await object(w, 'k'))?.entry, third.txid)
})

test('questions: one class exactly, every value checked, a bucket this host carries, a chain it serves for a map class', async () => {
  const w = await world()
  const bad = [
    {},
    { bucket },
    { bucket: 'other_qzvmkwerty', entry: 'a'.repeat(64) },
    { bucket, entry: 'A'.repeat(64) },
    { bucket, entry: 'a'.repeat(64), extra: '1' },
    { bucket, root: 'a'.repeat(64), after: 'x' },
    { ...w.chainQ, owner: '02' + 'ab'.repeat(32), key: 'a' },
    { ...w.chainQ, key: 'a\n' },
    { bucket, owner: w.owner.hex, export: '0'.repeat(64), after: '' },
    { bucket, hold: `${'a'.repeat(64)}.01` },
    { bucket, answerer: '0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798' },
  ]
  for (const q of bad) assert.match(await refused(w.r, q), /ls_bstore: refused/, JSON.stringify(q))
  await assert.rejects(w.r.ls.lookup({ service: 'ls_bchat', query: { bucket } }), /this service is ls_bstore/)
  assert.ok(MaxKeysInMap > 0)
  void KeyFront
  void commitment
})
