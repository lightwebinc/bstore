/**
 * The topic managers on the golden transaction vectors: through the manager
 * directly and through the real engine, every refusal raised and counted
 * by its label and nothing refused recorded; a worse copy cannot suppress
 * a genuine object; a token whose block the headers lack is admitted once
 * they catch up; pieces and the blob topic admit nothing.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Chain, OwnerSim } from './testmint.js'
import { answer, bucket, setup, submit, topic } from './testhost.js'
import { FakeHost, beefOf, byName, heldOf, tracker, txVectors } from './testutil.js'
import { BlobTopicManager, Reasons, RecordsTopicManager, Refused } from './tm_bstore.js'

const v = txVectors()

test('every transaction vector through the manager: the Go codec verdict, the label counted, every refusal raised', async () => {
  assert.equal(v.bucket, bucket)
  const host = new FakeHost()
  const tm = new RecordsTopicManager(topic, host, tracker(v.headers))
  const refused = new Map<string, number>()
  for (const t of v.cases) {
    if (t.verdict === 'admit') {
      const got = await tm.identifyAdmissibleOutputs(beefOf(t), heldOf(t))
      assert.deepEqual(got, { outputsToAdmit: t.admits!.outputsToAdmit, coinsToRetain: t.admits!.coinsToRetain }, t.name)
      continue
    }
    await assert.rejects(tm.identifyAdmissibleOutputs(beefOf(t), heldOf(t)), (e: unknown) => e instanceof Refused && e.reason === t.reason, t.name)
    refused.set(t.reason!, (refused.get(t.reason!) ?? 0) + 1)
  }
  for (const [reason, n] of refused) assert.equal(host.count('bstore_refused_total', { reason }), n, reason)
  assert.ok(refused.has('piece') && refused.has('not-bstore'), 'pieces and transactions of nothing bstore are among the refusals')
  for (const t of v.cases) if (t.reason !== undefined) assert.ok(Reasons.includes(t.reason), `${t.reason} is a preset label`)
  // Counted once per transaction, however often it is shown again.
  const put = byName(v, 'entry-put')
  const before = [host.count('bstore_admitted_total', { kind: 'entry' }), host.count('bstore_admitted_repeats_total', { kind: 'entry' })]
  await tm.identifyAdmissibleOutputs(beefOf(put), [])
  assert.deepEqual([host.count('bstore_admitted_total', { kind: 'entry' }), host.count('bstore_admitted_repeats_total', { kind: 'entry' })], [before[0], before[1]! + 1])
})

test('every transaction vector through the engine: admitted as the vectors say, nothing refused recorded', async () => {
  const r = await setup({ chain: new Chain(900000, v.headers) })
  const admitted = new Set(v.cases.filter((t) => t.verdict === 'admit').map((t) => t.txid))
  const done = new Set<string>()
  for (const t of v.cases) {
    const got = await submit(r, beefOf(t))
    if (t.verdict === 'admit') {
      // The same transaction in another admissible BEEF is a duplicate the engine does not show the manager again.
      if (!done.has(t.txid)) assert.deepEqual(got, t.admits!.outputsToAdmit, t.name)
      done.add(t.txid)
    } else if (!admitted.has(t.txid)) {
      assert.deepEqual(got ?? [], [], t.name)
      assert.equal(await r.storage.doesAppliedTransactionExist({ txid: t.txid, topic }), false, t.name)
    }
  }
  assert.equal(r.ls.tokenStatus(byName(v, 'token-first').txid), 'rooted')
})

test('suppression: a manager that records refusals loses a genuine carrier, token, advertisement and sweep to a copy in a worse BEEF; tm_bstore does not', async () => {
  for (const [worseName, genuineName] of [
    ['carrier-beef-tree-unproven', 'entry-beef-v1'],
    ['carrier-beef-extra-transaction', 'entry-beef-v1'],
    ['token-beef-atomic', 'token-v2'],
    ['token-beef-no-parent', 'token-v2'],
    ['advertisement-beef-with-parent', 'advertisement'],
    ['sweep-beef-extra-transaction', 'sweep'],
  ] as const) {
    const worse = byName(v, worseName)
    const genuine = byName(v, genuineName)
    assert.equal(worse.txid, genuine.txid)
    const naive = await setup({ chain: new Chain(900000, v.headers), naive: true })
    assert.deepEqual(await submit(naive, beefOf(worse)), [])
    assert.ok(await naive.storage.doesAppliedTransactionExist({ txid: genuine.txid, topic }), `${worseName}: the refusal is recorded as applied`)
    const before = naive.calls()
    assert.deepEqual(await submit(naive, beefOf(genuine)), [])
    assert.equal(naive.calls(), before, `${worseName}: the genuine object is a duplicate the manager never sees`)

    const real = await setup({ chain: new Chain(900000, v.headers) })
    assert.deepEqual((await submit(real, beefOf(worse))) ?? [], [], `${worseName}: nothing admitted`)
    assert.equal(await real.storage.doesAppliedTransactionExist({ txid: genuine.txid, topic }), false, `${worseName}: nothing recorded`)
    assert.deepEqual(await submit(real, beefOf(genuine)), genuine.admits!.outputsToAdmit, `${worseName}: the genuine object is admitted`)
  }
})

test("a token whose parent's block the host's headers lack is refused without a trace, and admitted once they catch up", async () => {
  const chain = new Chain(71000)
  const owner = new OwnerSim(bucket, 'lagging owner', chain)
  const t0 = await owner.token()
  const t1 = await owner.token()
  const height = t0.tx.merklePath!.blockHeight
  const root = chain.roots.get(height)!
  const r = await setup({ chain, config: { served: [owner.served] } })
  chain.orphan(height)
  assert.deepEqual(await submit(r, t1.beef), [], 'refused unmined')
  assert.equal(r.host.count('bstore_refused_total', { reason: 'unmined' }), 1)
  assert.equal(await r.storage.doesAppliedTransactionExist({ txid: t1.txid, topic }), false)
  chain.roots.set(height, root)
  assert.deepEqual(await submit(r, t1.beef), [0], 'decided again and admitted')
  assert.equal(r.ls.tokenStatus(t1.txid), 'pending')
  assert.deepEqual(await submit(r, t0.beef), [0])
  assert.equal(r.ls.tokenStatus(t1.txid), 'consistent')
  assert.deepEqual(await answer(r, { bucket, owner: owner.hex, id: owner.idHex }), [t1.txid])
})

test('the blob topic: every submission refused as piece and raised; pieces refused on the records topic too', async () => {
  const host = new FakeHost()
  const blob = new BlobTopicManager(`tm_bstoreblob_${bucket}`, host)
  for (const name of ['piece', 'entry-put', 'token-first', 'advertisement']) {
    await assert.rejects(blob.identifyAdmissibleOutputs(), (e: unknown) => e instanceof Refused && e.reason === 'piece', name)
  }
  assert.equal(host.count('bstore_refused_total', { reason: 'piece' }), 4)
  const records = new RecordsTopicManager(topic, host, tracker(v.headers))
  await assert.rejects(records.identifyAdmissibleOutputs(beefOf(byName(v, 'piece')), []), (e: unknown) => e instanceof Refused && e.reason === 'piece')
  // Mounted only where OVERLAY_TOPICS names the blob topic.
  const r = await setup({ chain: new Chain(900000, v.headers), overlayTopics: [topic, `tm_bstoreblob_${bucket}`] })
  assert.ok(r.module.topics![`tm_bstoreblob_${bucket}`] !== undefined)
  assert.deepEqual((await submit(r, beefOf(byName(v, 'piece')), `tm_bstoreblob_${bucket}`)) ?? [], [])
  assert.deepEqual((await submit(r, beefOf(byName(v, 'entry-put')), `tm_bstoreblob_${bucket}`)) ?? [], [])
  assert.equal(r.storage.applied.size, 0)
  assert.equal(r.storage.outputs.size, 0)
})

test('a manager refuses to be built for another topic family or below the floors', () => {
  const host = new FakeHost()
  const h = tracker(v.headers)
  assert.throws(() => new RecordsTopicManager(`tm_bstoreblob_${bucket}`, host, h), /not a tm_bstore_ records topic/)
  assert.throws(() => new RecordsTopicManager('tm_bchat_photos_qzvmkwerty', host, h), /not a tm_bstore_ records topic/)
  assert.throws(() => new RecordsTopicManager('tm_bstore_Photos_qzvmkwerty', host, h), /not a bucket identifier/)
  assert.throws(() => new RecordsTopicManager(topic, host, h, 262143), /BEEF bound/)
  assert.throws(() => new RecordsTopicManager(topic, host, h, 262144, 1023), /sweep input bound/)
  assert.throws(() => new BlobTopicManager(topic, host), /not a tm_bstoreblob_ topic/)
})
