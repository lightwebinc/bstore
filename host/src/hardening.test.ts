/**
 * What keeps the index honest across crashes, restarts and races: the
 * journal file, restore in any order to the same answers (spent tokens,
 * renewed advertisements and spent sweeps included), retention never
 * against a restore in flight, drops that wait for the engine's record and
 * never delete what was offered again, the first carrier of an outpoint and
 * the retraction marks across a compaction and a restart, reorganisations,
 * and the bound on tokens at one seq.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FileJournal, JournalFile } from './journal.js'
import { KeyEntry, LookupService } from './storerec.js'
import { T0, sweep } from './testmint.js'
import { answer, bucket, outputs, restart, submit, submitTx, topic, type Rig } from './testhost.js'
import { MemoryStorage } from './testutil.js'
import { Day, Hour, object, world, type World } from './testworld.js'
import { toHex } from './util.js'

test('the journal file: a last line cut short is cut off, any other bad line stops the load, a rewrite replaces it whole', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bstore-journal-'))
  try {
    const j = new FileJournal(dir)
    j.append({ k: 'c', op: `${'a'.repeat(64)}.0`, c: 'b'.repeat(64), at: 1 })
    j.append({ k: 'x', op: `${'a'.repeat(64)}.1`, key: '02' + 'c'.repeat(64), at: 2, life: true })
    j.close()
    writeFileSync(join(dir, JournalFile), readFileSync(join(dir, JournalFile), 'utf8') + '{"k":"c","op":"')
    assert.equal(new FileJournal(dir).load().length, 2)
    assert.equal(readFileSync(join(dir, JournalFile), 'utf8').endsWith('}\n'), true, 'the cut line is gone from the file')
    writeFileSync(join(dir, JournalFile), readFileSync(join(dir, JournalFile), 'utf8') + '{"k":"zz"}\n{"k":"c"}\n')
    assert.throws(() => new FileJournal(dir).load(), /line 3 is not a bstore row/)
    writeFileSync(join(dir, JournalFile), '{"k":"x","op":"nope","key":"02","at":1,"life":true}\n')
    assert.throws(() => new FileJournal(dir).load(), /line 1 is not a bstore row/)
    const k = new FileJournal(dir)
    k.rewrite([{ k: 'p', op: `${'d'.repeat(64)}.7`, s: 'e'.repeat(64), at: 3 }])
    assert.deepEqual(k.load(), [{ k: 'p', op: `${'d'.repeat(64)}.7`, s: 'e'.repeat(64), at: 3 }])
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

/** A world with a bit of everything: tokens spent by their successors, entries anchored, replaced and pending, a renewed advertisement, fronts, outcomes, a hold, and two sweeps, the second spending the first's output. */
async function busy(): Promise<{ w: World; snapshot: (r: Rig) => Promise<unknown> }> {
  const w = await world()
  const h = w.alice.hold(5000)
  const e1 = await w.alice.put(w.owner.id, 'a', { hold: h.terms })
  const e2 = await w.alice.put(w.owner.id, 'b')
  const e3 = await w.alice.put(w.owner.id, 'a', { label: 'again', created: T0 + 1 })
  const p = await w.owned.put(w.owner.id, 'p')
  const doomed = await w.bob.put(w.owner.id, 'doomed')
  const now = Math.floor(w.clock.ms / 1000)
  const ad = await w.prov.advertise([e1.e.root], now + 30 * 86400)
  const renewed = await w.prov.renew(ad, 0, e1.e.root, now + 60 * 86400)
  const f = await w.front.statement(w.prov.party.id)
  const o = await w.buyer.outcome({ root: e1.e.root, provider: w.prov.party })
  for (const x of [e1, e2, e3, p, doomed, f, o]) await submitTx(w.r, x.tx)
  for (const tx of [h.tx, ad, renewed]) await submitTx(w.r, tx)
  const t0 = await w.owner.token()
  const t1 = await w.owner.token({ writers: [w.alice.id], anchor: [e1, e2] })
  const t2 = await w.owner.token({ anchor: [e3] })
  for (const t of [t0, t1, t2]) await submit(w.r, t.beef)
  const sw = await sweep(doomed.tree, [doomed.vout], w.bob.party, KeyEntry, w.chain)
  await submitTx(w.r, sw)
  const q = w.chainQ
  const snapshot = async (r: Rig): Promise<unknown> => ({
    bucket: await answer(r, q),
    seq0: await answer(r, { ...q, seq: '0' }),
    a: (await outputs(r, { ...q, key: 'a' })).map((x) => [x.txid, toHex(x.context ?? [])]),
    list: (await outputs(r, { ...q, prefix: '', after: '' })).map((x) => [x.txid, toHex(x.context ?? [])]),
    pending: await answer(r, { ...q, pending: 'p' }),
    unanchored: await answer(r, { ...q, writer: w.alice.hex, after: '' }),
    replaced: await answer(r, { bucket, entry: e1.txid }),
    doomed: await answer(r, { bucket, entry: doomed.txid }),
    ads: await answer(r, { bucket, root: toHex(e1.e.root), after: '' }),
    fronts: await answer(r, { bucket, advertiser: w.prov.hex, after: '' }),
    outcomes: await answer(r, { bucket, provider: w.prov.hex }),
    hold: await answer(r, { bucket, hold: h.op }),
    sweep: await answer(r, { bucket, spent: doomed.op }),
    history: await answer(r, { ...q, history: 'a', before: '9' }),
    chain: await answer(r, { bucket, owner: w.owner.hex, chain: w.owner.idHex, after: '0' }),
    statuses: [t0, t1, t2].map((t) => r.ls.tokenStatus(t.txid)),
  })
  return { w, snapshot }
}

test('restore in any order reaches the same answers: tokens spent by their successors, a renewed advertisement, a sweep', async () => {
  const { w, snapshot } = await busy()
  const before = await snapshot(w.r)
  assert.equal((before as { renewedOnly?: unknown; ads: string[] }).ads.length, 1)
  const again = await restart(w.r)
  assert.deepEqual(await snapshot(again), before)
  // Storage's rows in reverse, and shuffled: the same.
  const rows = await again.storage.findUTXOsForTopic(topic, undefined, undefined, true)
  for (const order of [[...rows].reverse(), [...rows].sort(() => Math.random() - 0.5)]) {
    await again.ls.restore(order, again.storage)
    assert.deepEqual(await snapshot(again), before)
  }
  // The renewed advertisement, though spent, is restored, and goes at its floor.
  w.clock.advance(70 * Day)
  await again.ls.prune()
  assert.equal(again.ls.adCount, 0)
})

test('retention never runs against a restore in flight, and an admission waits for it', async () => {
  const { w } = await busy()
  let release: () => void = () => {}
  const gate = new Promise<void>((r) => (release = r))
  const slow = Object.create(w.r.storage) as MemoryStorage
  slow.findOutput = async (...a: Parameters<MemoryStorage['findOutput']>) => {
    await gate
    return await MemoryStorage.prototype.findOutput.apply(w.r.storage, a)
  }
  const rows = await w.r.storage.findUTXOsForTopic(topic, undefined, undefined, true)
  const restoring = w.r.ls.restore(rows, slow)
  w.clock.advance(400 * Day)
  const pruning = w.r.ls.prune()
  const late = await w.alice.put(w.owner.id, 'late')
  const admitting = w.r.engine.submit({ beef: late.tx.toAtomicBEEF(), topics: [topic] })
  await new Promise((r) => setTimeout(r, 50))
  assert.equal(w.r.ls.restored, false)
  release()
  await restoring
  assert.ok((await pruning) > 0, 'retention ran once the restore was done')
  await admitting
  assert.deepEqual(await answer(w.r, { bucket, entry: late.txid }), [late.txid])
})

test("a drop decided while the engine admits the object waits for the engine's record; one offered again before it runs is not deleted", async () => {
  const w = await world()
  const e = await w.alice.put(w.owner.id, 's')
  await submitTx(w.r, e.tx)
  const sw = await sweep(e.tree, [e.vout], w.alice.party, KeyEntry, w.chain)
  await submitTx(w.r, sw)
  // The replay: the index drops it before the engine records the admission; the drop waits, then deletes and evicts.
  await w.r.engine.submit({ beef: e.tx.toAtomicBEEF(), topics: [topic] })
  assert.equal(w.r.ls.pendingDrops, 1)
  await w.r.ls.flush()
  assert.equal(w.r.ls.pendingDrops, 0)
  assert.equal(await w.r.storage.doesAppliedTransactionExist({ txid: e.txid, topic }), false)
  // A stranger's entry dropped by retention, offered again before the drop runs: it is held, and stays in storage.
  const s = await w.bob.put(w.owner.id, 'x')
  await submitTx(w.r, s.tx)
  w.clock.advance(25 * Hour)
  const flush = w.r.ls.flush.bind(w.r.ls)
  w.r.ls.flush = async () => {}
  await w.r.ls.prune()
  assert.equal(w.r.ls.pendingDrops, 1)
  w.r.ls.flush = flush
  await w.r.storage.deleteAppliedTransaction(s.txid, topic)
  await w.r.storage.deleteOutput(s.txid, 0, topic)
  await w.r.engine.submit({ beef: s.tx.toAtomicBEEF(), topics: [topic] })
  await w.r.ls.flush()
  assert.notEqual(await w.r.storage.findOutput(s.txid, 0, topic), null)
  assert.deepEqual(await answer(w.r, { bucket, entry: s.txid }), [s.txid])
})

test("the first carrier of an outpoint and the retraction marks hold across a compaction and a restart", async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  await submit(w.r, (await w.owner.token({ writers: [w.alice.id] })).beef)
  const first = await w.alice.put(w.owner.id, 'k')
  const second = await w.alice.put(w.owner.id, 'k', { vout: first.vout, created: T0 + 1 })
  const swept = await w.alice.put(w.owner.id, 's')
  for (const e of [first, swept]) await submitTx(w.r, e.tx)
  await submitTx(w.r, await sweep(swept.tree, [swept.vout], w.alice.party, KeyEntry, w.chain))
  await w.r.ls.prune()
  const again = await restart(w.r)
  assert.deepEqual(await submitTx(again, second.tx), [0])
  assert.deepEqual(await answer(again, { bucket, entry: second.txid }), [])
  assert.deepEqual(await submitTx(again, swept.tx), [0])
  assert.deepEqual(await answer(again, { bucket, entry: swept.txid }), [])
  assert.deepEqual(await answer(again, { bucket, entry: first.txid }), [first.txid])
})

test('a reorganisation: two rooted tokens at one seq are both latest; answers use the one whose proof verifies, else the lowest txid', async () => {
  const w = await world()
  const a = await w.alice.put(w.owner.id, 'a')
  const b = await w.alice.put(w.owner.id, 'b')
  for (const e of [a, b]) await submitTx(w.r, e.tx)
  await submit(w.r, (await w.owner.token()).beef)
  const other = w.owner.fork()
  const x = await w.owner.token({ writers: [w.alice.id], anchor: [a] })
  const y = await other.token({ writers: [w.alice.id], anchor: [b] })
  for (const t of [x, y]) assert.deepEqual(await submit(w.r, t.beef), [0])
  assert.deepEqual(await answer(w.r, w.chainQ), [x.txid, y.txid].sort())
  const low = x.txid < y.txid ? x : y
  const high = low === x ? y : x
  // Both verify: the lowest txid.
  assert.equal((await object(w, 'a'))?.token, low.txid)
  // The lower one's block leaves the best chain: the other, once the choice is made again.
  w.chain.orphan(low.tx.merklePath!.blockHeight)
  const again = await restart(w.r)
  const token = (await outputs(again, { ...w.chainQ, key: 'a' }))[0]!.txid
  assert.equal(token, high.txid)
})

test('at most 8 tokens of a chain at one seq: rooted first, then the lowest txids', async () => {
  const w = await world()
  await submit(w.r, (await w.owner.token()).beef)
  const forks = []
  for (let i = 0; i < 9; i++) forks.push(await w.owner.fork().token({ created: T0 + 600_000 + i }))
  for (const t of forks) assert.deepEqual(await submit(w.r, t.beef), [0])
  const held = await answer(w.r, { ...w.chainQ, seq: '1' })
  assert.deepEqual(held, forks.map((t) => t.txid).sort().slice(0, 8))
  assert.equal(w.r.host.lines.some((l) => l.msg === 'ls_bstore kept no more tokens at this seq'), true)
  void LookupService
})
