/**
 * Retention (spec section 12.3) at the host's clock: each floor holds, and
 * what passes its floor is dropped: deleted from the topic and evicted, so
 * the same object offered again is decided afresh.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { KeyEntry, KeyFront, KeyOutcome } from './storerec.js'
import { Buyer, FrontSim, T0, sweep } from './testmint.js'
import { answer, bucket, submit, submitTx, topic } from './testhost.js'
import { Day, Hour, object, world, type World } from './testworld.js'
import { toHex } from './util.js'

const held = async (w: World, txid: string): Promise<boolean> => (await w.r.storage.findOutput(txid, 0, topic)) !== null
const entry = async (w: World, txid: string): Promise<boolean> => (await answer(w.r, { bucket, entry: txid })).length === 1
const prune = async (w: World, ms: number): Promise<void> => {
  w.clock.advance(ms)
  await w.r.ls.prune()
}

test('entries: anchored kept, replaced 31 days after the token that replaced it, pending 7 days for the owner and writers, a stranger 24 hours', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const v1 = await w.alice.put(w.owner.id, 'k', { label: 'one' })
  const v2 = await w.alice.put(w.owner.id, 'k', { label: 'two', created: T0 + 1 })
  const pending = await w.owned.put(w.owner.id, 'p')
  const stranger = await w.bob.put(w.owner.id, 's')
  for (const e of [v1, v2, pending, stranger]) await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id], anchor: [v1] })).beef)
  await prune(w, 23 * Hour)
  assert.deepEqual([await entry(w, stranger.txid), await entry(w, pending.txid)], [true, true], 'within 24 hours, everything')
  await prune(w, 2 * Hour)
  assert.deepEqual([await entry(w, stranger.txid), await held(w, stranger.txid)], [false, false], "a stranger's entry goes at 24 hours")
  await prune(w, 5 * Day)
  // v1 is replaced by v2 now: 31 days from this token's first sight.
  await submit(w.r, (await w.owner.token({ anchor: [v2] })).beef)
  await prune(w, 2 * Day)
  assert.deepEqual([await entry(w, pending.txid), await held(w, pending.txid)], [false, false], "the owner's unnamed entry goes at 7 days")
  await prune(w, 28 * Day)
  assert.equal(await entry(w, v1.txid), true, 'replaced 30 days ago: kept')
  await prune(w, 1 * Day + Hour)
  assert.equal(await entry(w, v1.txid), false, 'replaced over 31 days ago: dropped')
  await prune(w, 400 * Day)
  assert.equal((await object(w, 'k'))?.entry, v2.txid, 'the anchored entry is kept')
  // Dropped means evicted: offered again, the stranger's entry is admitted afresh and answered.
  assert.deepEqual(await submitTx(w.r, stranger.tx), [0])
  assert.equal(await entry(w, stranger.txid), true)
  assert.ok(w.r.host.count('bstore_dropped_total', { why: 'retention' }) >= 3)
})

test('an entry named only by a token that is not rooted is kept as a pending one is', async () => {
  const w = await world()
  const t0 = await w.owner.token()
  const e = await w.bob.put(w.owner.id, 'k')
  await submitTx(w.r, e.tx)
  const t1 = await w.owner.token({ anchor: [e] })
  await submit(w.r, t1.beef)
  assert.equal(w.r.ls.tokenStatus(t1.txid), 'pending')
  await prune(w, 6 * Day)
  assert.equal(await entry(w, e.txid), true, 'bob is no writer, but the owner named his entry')
  await submit(w.r, t0.beef)
  await prune(w, 400 * Day)
  assert.equal(await entry(w, e.txid), true, 'anchored once the chain is rooted, and kept')
  // A reader refuses it, since bob is no writer of the token it is proven against (spec section 15.2 step 4).
  await assert.rejects(object(w, 'k'), /writer/)
})

test("tokens: a served chain's for the host's life; any other 24 hours after first sight", async () => {
  const w = await world()
  const u = await world({ served: false, seed: 'stranger', setup: { chain: w.chain } })
  const s0 = await w.owner.token()
  const s1 = await w.owner.token()
  const x0 = await u.owner.token()
  const x1 = await u.owner.token()
  for (const t of [s0, s1, x0, x1]) await submit(w.r, t.beef)
  await prune(w, 25 * Hour)
  assert.deepEqual(await answer(w.r, w.chainQ), [s1.txid])
  assert.deepEqual(await answer(w.r, u.chainQ), [])
  assert.equal(await w.r.storage.findOutput(x1.txid, 0, topic), null)
  await prune(w, 400 * Day)
  assert.deepEqual(await answer(w.r, { ...w.chainQ, seq: '0' }), [s0.txid])
})

test('outcomes: 31 days when the buyer is a party and the root is one the map names; any other 24 hours', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const e = await w.alice.put(w.owner.id, 'k')
  await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id], anchor: [e] })).beef)
  const asOwner = new Buyer(bucket, 'w owner', w.chain)
  const kept = await asOwner.outcome({ root: e.e.root, provider: w.prov.party })
  const otherRoot = await asOwner.outcome({ root: new Uint8Array(32).fill(9), provider: w.prov.party })
  const stranger = await w.buyer.outcome({ root: e.e.root, provider: w.prov.party })
  for (const o of [kept, otherRoot, stranger]) await submitTx(w.r, o.tx)
  await prune(w, 25 * Hour)
  assert.deepEqual([await held(w, kept.txid), await held(w, otherRoot.txid), await held(w, stranger.txid)], [true, false, false])
  await prune(w, 30 * Day)
  assert.equal(await held(w, kept.txid), false)
})

test('front statements: the first 16 by first sight per advertiser of a root the map names, 31 days; the rest 24 hours', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const e = await w.alice.put(w.owner.id, 'k')
  await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id], anchor: [e] })).beef)
  await submitTx(w.r, await w.prov.advertise([e.e.root], Math.floor(w.clock.ms / 1000) + 90 * 86400))
  const fronts = []
  for (let i = 0; i < 17; i++) {
    const f = new FrontSim(bucket, `front ${i}`, w.chain)
    const s = await f.statement(w.prov.party.id)
    await submitTx(w.r, s.tx)
    fronts.push(s)
    w.clock.advance(1000)
  }
  const otherAdvertiser = await w.front.statement(w.prov2.party.id)
  await submitTx(w.r, otherAdvertiser.tx)
  await prune(w, 25 * Hour)
  const kept = []
  for (const f of fronts) kept.push(await held(w, f.txid))
  assert.deepEqual(kept, [...Array<boolean>(16).fill(true), false])
  assert.equal(await held(w, otherAdvertiser.txid), false)
  await prune(w, 31 * Day)
  assert.equal(await held(w, fronts[0]!.txid), false)
  void KeyFront
})

test('advertisements: of a root the map names, until the earlier of expiry and 31 days, plus 24 hours; any other 24 hours', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const e = await w.alice.put(w.owner.id, 'k')
  await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id], anchor: [e] })).beef)
  const now = Math.floor(w.clock.ms / 1000)
  const soon = await w.prov.advertise([e.e.root], now + 10 * 86400)
  const long = await w.prov2.advertise([e.e.root], now + 90 * 86400)
  const other = await w.prov.advertise([new Uint8Array(32).fill(3)], now + 90 * 86400)
  for (const tx of [soon, long, other]) await submitTx(w.r, tx)
  await prune(w, 25 * Hour)
  assert.deepEqual([await held(w, soon.id('hex')), await held(w, long.id('hex')), await held(w, other.id('hex'))], [true, true, false])
  await prune(w, 10 * Day)
  assert.equal(await held(w, soon.id('hex')), false, 'expired over 24 hours ago')
  assert.deepEqual(await answer(w.r, { bucket, root: toHex(e.e.root), after: '' }), [long.id('hex')])
  await prune(w, 20 * Day)
  assert.equal(await held(w, long.id('hex')), true, '31 days after first sight, plus 24 hours, has not passed')
  await prune(w, 2 * Day)
  assert.equal(await held(w, long.id('hex')), false)
})

test('hold outputs: named by an anchored or a pending entry of the chain, 31 days; any other 24 hours', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const hA = w.alice.hold(1000)
  const hP = w.owned.hold(1000)
  const hX = w.bob.hold(1000)
  const a = await w.alice.put(w.owner.id, 'a', { hold: hA.terms })
  const p = await w.owned.put(w.owner.id, 'p', { hold: hP.terms })
  const x = await w.bob.put(w.owner.id, 'x', { hold: hX.terms })
  for (const tx of [hA.tx, hP.tx, hX.tx, a.tx, p.tx, x.tx]) await submitTx(w.r, tx)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id], anchor: [a] })).beef)
  await prune(w, 25 * Hour)
  const holds = async (): Promise<number[]> => [(await answer(w.r, { bucket, hold: hA.op })).length, (await answer(w.r, { bucket, hold: hP.op })).length, (await answer(w.r, { bucket, hold: hX.op })).length]
  assert.deepEqual(await holds(), [1, 1, 0])
  await prune(w, 30 * Day)
  assert.deepEqual(await holds(), [0, 0, 0])
})

test("retraction: a party's mark is kept for the host's life, a stranger's and a sweep's outpoints 31 days; a sweep's transaction as long as its outpoints", async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id] })).beef)
  const mine = await w.alice.put(w.owner.id, 'm')
  const theirs = await w.bob.put(w.owner.id, 's')
  for (const e of [mine, theirs]) await submitTx(w.r, e.tx)
  const s1 = await sweep(mine.tree, [mine.vout], w.alice.party, KeyEntry, w.chain)
  const s2 = await sweep(theirs.tree, [theirs.vout], w.bob.party, KeyEntry, w.chain)
  for (const s of [s1, s2]) assert.deepEqual(await submitTx(w.r, s), [0])
  assert.deepEqual([w.r.ls.mark(mine.op)?.life, w.r.ls.mark(theirs.op)?.life], [true, false])
  await prune(w, 30 * Day)
  assert.deepEqual(await answer(w.r, { bucket, spent: mine.op }), [s1.id('hex')])
  await prune(w, 2 * Day)
  assert.deepEqual(await answer(w.r, { bucket, spent: mine.op }), [], "the sweep's transaction went with its outpoints")
  assert.deepEqual([w.r.ls.mark(mine.op) !== undefined, w.r.ls.mark(theirs.op) !== undefined, w.r.ls.swept(theirs.op)], [true, false, false])
  // The party's carrier stays retracted whenever it is offered; the stranger's, once both have aged out, is a carrier like any other.
  for (const e of [mine, theirs]) assert.deepEqual(await submitTx(w.r, e.tx), [0])
  assert.deepEqual([await entry(w, mine.txid), await entry(w, theirs.txid)], [false, true])
  void KeyOutcome
})

test('rows: the first carrier of an outpoint is remembered while one is held, and until 24 hours after its first sight', async () => {
  const w = await world()
  const first = await w.owned.put(w.owner.id, 'a')
  const second = await w.owned.put(w.owner.id, 'a', { vout: first.vout, created: T0 + 5 })
  assert.equal(second.op, first.op)
  await submitTx(w.r, first.tx)
  await prune(w, 6 * Day)
  assert.equal(w.r.ls.firstOn(first.op), first.c, 'held: its row stays')
  assert.deepEqual(await submitTx(w.r, second.tx), [0])
  assert.equal(await entry(w, second.txid), false, 'a later carrier on the outpoint has no floor')
  await prune(w, 2 * Day)
  assert.equal(await entry(w, first.txid), false)
  assert.equal(w.r.ls.firstOn(first.op), undefined, 'nothing held on it and past 24 hours: forgotten')
})
