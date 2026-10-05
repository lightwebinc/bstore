/**
 * The independent review's findings, each held by a test: a history page
 * never splits a seq; a hold output keeps its floor as long as its entry
 * keeps the pending one; a mined output the engine marked spent before the
 * spender was journaled is restored and pruned; a sweep is kept only in
 * the bucket whose carrier it matched; the latest tokens follow the branch
 * whose proofs verify, a longer orphaned branch notwithstanding; chain
 * walks only seqs that hold a rooted token.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { KeyEntry } from './storerec.js'
import { T0, Writer, sha, sweep } from './testmint.js'
import { answer, bucket, outputs, restart, submit, submitTx, topic } from './testhost.js'
import { Day, Hour, object, world } from './testworld.js'

test('history never splits a seq: a page that would end inside one ends before it', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const f = w.owner.fork()
  const eB = await w.owned.put(w.owner.id, 'k', { label: 'fork', created: T0 + 999 })
  await submitTx(w.r, eB.tx)
  const B1 = await f.token({ anchor: [eB] })
  assert.deepEqual(await submit(w.r, B1.beef), [0])
  const tokens = new Set<string>([B1.txid])
  for (let i = 1; i <= 64; i++) {
    const e = await w.owned.put(w.owner.id, 'k', { label: `v${i}`, created: T0 + i })
    await submitTx(w.r, e.tx)
    const t = await w.owner.token({ anchor: [e] })
    assert.deepEqual(await submit(w.r, t.beef), [0])
    tokens.add(t.txid)
  }
  const seen = new Set<string>()
  let before = '1000'
  for (;;) {
    const toks = (await answer(w.r, { ...w.chainQ, history: 'k', before })).filter((x) => tokens.has(x))
    if (toks.length === 0) break
    for (const x of toks) seen.add(x)
    before = String(Math.min(...toks.map((x) => (w.owner.history.find((m) => m.txid === x) ?? f.history.find((m) => m.txid === x)!).rec.seq)))
  }
  assert.equal(seen.size, tokens.size)
})

test('a hold named by an entry named only by an unrooted token survives until the entry is anchored', async () => {
  const w = await world()
  const t0 = await w.owner.token()
  const h = w.alice.hold(1000)
  const e = await w.alice.put(w.owner.id, 'k', { hold: h.terms })
  await submitTx(w.r, h.tx)
  await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id], anchor: [e] })).beef)
  w.clock.advance(25 * Hour)
  await w.r.ls.prune()
  assert.equal((await answer(w.r, { bucket, entry: e.txid })).length, 1)
  await submit(w.r, t0.beef)
  assert.equal((await answer(w.r, { bucket, hold: h.op })).length, 1)
  w.clock.advance(29 * Day)
  await w.r.ls.prune()
  assert.equal((await answer(w.r, { bucket, hold: h.op })).length, 1, 'anchored now: 31 days')
})

test('an advertisement the engine marked spent before the spender was journaled is restored and pruned', async () => {
  const w = await world()
  const ad = await w.prov.advertise([sha('r')], Math.floor((w.clock.ms + 400 * Day) / 1000))
  await submitTx(w.r, ad)
  await w.r.storage.markUTXOAsSpent(ad.id('hex'), 0, topic)
  const r2 = await restart(w.r)
  assert.equal(r2.ls.adCount, 1, 'restored, though storage hands only unspent rows')
  assert.deepEqual(await answer(r2, { bucket, root: Buffer.from(sha('r')).toString('hex'), after: '' }), [], 'and known spent')
  r2.clock.advance(400 * Day)
  await r2.ls.prune()
  assert.equal(await r2.storage.findOutput(ad.id('hex'), 0, topic), null)
})

test('a sweep is kept only in the bucket whose carrier it matched, whichever bucket it reaches first', async () => {
  const other = 'archive_qzvmkwerty'
  const w = await world({ setup: { config: { buckets: [bucket, other] } } })
  const there = new Writer(other, 'w alice', w.chain)
  const e = await there.put(w.owner.id, 'x')
  assert.deepEqual(await submitTx(w.r, e.tx, `tm_bstore_${other}`), [0])
  const sw = await sweep(e.tree, [e.vout], there.party, KeyEntry, w.chain)
  assert.deepEqual(await submitTx(w.r, sw), [0], 'admitted in this bucket first')
  assert.deepEqual(await answer(w.r, { bucket: other, entry: e.txid }), [], 'and it retracts the carrier of the other')
  assert.deepEqual(await answer(w.r, { bucket, spent: e.op }), [], 'but is not kept here, where nothing matched')
  assert.deepEqual(await submitTx(w.r, sw, `tm_bstore_${other}`), [0])
  assert.deepEqual(await answer(w.r, { bucket: other, spent: e.op }), [sw.id('hex')], 'kept in the bucket whose carrier it retracted')
  const again = await restart(w.r)
  assert.deepEqual(await answer(again, { bucket: other, spent: e.op }), [sw.id('hex')])
})

test('the latest token is the tip of the branch whose proofs verify, though an orphaned branch is longer (row C8)', async () => {
  const w = await world()
  const a = await w.alice.put(w.owner.id, 'a')
  const b = await w.alice.put(w.owner.id, 'b')
  for (const e of [a, b]) await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token()).beef)
  const other = w.owner.fork()
  const a1 = await w.owner.token({ writers: [w.alice.id], anchor: [a] })
  const a2 = await w.owner.token()
  const b1 = await other.token({ writers: [w.alice.id], anchor: [b] })
  for (const t of [a1, a2, b1]) assert.deepEqual(await submit(w.r, t.beef), [0])
  assert.deepEqual(await answer(w.r, w.chainQ), [a2.txid], 'both branches on the chain: the longer')
  // A reorganisation takes branch A's blocks out of the best chain.
  for (const t of [a1, a2]) w.chain.orphan(t.tx.merklePath!.blockHeight)
  const again = await restart(w.r)
  assert.deepEqual(await answer(again, w.chainQ), [b1.txid])
  assert.deepEqual(await object({ ...w, r: again }, 'b'), { result: 'found', entry: b.txid, token: b1.txid })
  // Retention follows the branch on the chain: b stays anchored, a is no longer named by it.
  again.clock.advance(400 * Day)
  await again.ls.prune()
  assert.deepEqual(await answer(again, { bucket, entry: b.txid }), [b.txid])
})

test("chain walks only the seqs that hold a rooted token: a stranger's unrooted tokens cost it nothing", async () => {
  const u = await world({ served: false, seed: 'unrooted' })
  await u.owner.token()
  const ts = []
  for (let i = 0; i < 200; i++) ts.push(await u.owner.token())
  for (const t of ts) await submit(u.r, t.beef)
  const v = (u.r.ls as unknown as { chains: Map<string, { bySeq: Map<number, unknown>; rootedSeqs: { sorted: { size: number } } }> }).chains.get(`${bucket}|${u.owner.hex}|${u.owner.idHex}`)!
  assert.equal(v.bySeq.size, 200)
  assert.equal(v.rootedSeqs.sorted.size, 0)
  assert.deepEqual(await outputs(u.r, { bucket, owner: u.owner.hex, chain: u.owner.idHex, after: '0' }), [])
})
