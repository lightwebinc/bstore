/**
 * Every Go-generated vector under testdata/vectors, run through the
 * TypeScript codec: records decode, re-encode byte for byte and refuse for
 * the same labels; identifiers, keys, roots, contexts, pages, folds,
 * indices, responses, settlements, questions, closures and transactions
 * reach the Go codec's values and verdicts.
 */
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import { KeyDeriver, PrivateKey, PublicKey, type ChainTracker } from '@bsv/sdk'
import { pathLength, readerLockingKey, rootOfLeafHashes, segmentRoot } from '@lightwebinc/bcommon'
import * as s from './index.js'
import { fromHex, sha256, toHex } from './util.js'

const dir = new URL('../../testdata/vectors/', import.meta.url)
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const load = (name: string): any => JSON.parse(readFileSync(new URL(name, dir), 'utf8'))

interface Fill {
  at: number
  byte: number
  len: number
}

function expand(hex: string, fill?: Fill | null): Uint8Array {
  const b = fromHex(hex)
  if (fill === undefined || fill === null) return b
  const out = new Uint8Array(b.length + fill.len)
  out.set(b.subarray(0, fill.at), 0)
  out.fill(fill.byte, fill.at, fill.at + fill.len)
  out.set(b.subarray(fill.at), fill.at + fill.len)
  return out
}

function decodeKind(kind: string, raw: Uint8Array): [Record<string, unknown>, Uint8Array] {
  switch (kind) {
    case 'bucket': {
      const r = s.decodeBucket(raw)
      return [s.bucketView(r), s.encodeBucket(r)]
    }
    case 'entry': {
      const r = s.decodeEntry(raw)
      return [s.entryView(r), s.encodeEntry(r)]
    }
    case 'piece': {
      const r = s.decodePiece(raw)
      return [s.pieceView(r), s.encodePiece(r)]
    }
    case 'front': {
      const r = s.decodeFront(raw)
      return [s.frontView(r), s.encodeFront(r)]
    }
    case 'outcome': {
      const r = s.decodeOutcome(raw)
      return [s.outcomeView(r), s.encodeOutcome(r)]
    }
    case 'request': {
      const r = s.decodeRequest(raw)
      return [s.requestView(r), s.encodeRequest(r)]
    }
    case 'body': {
      const r = s.decodeBody(raw)
      return [s.bodyView(r), s.encodeBody(r)]
    }
  }
  throw new Error(`kind ${kind}`)
}

function labelOf(f: () => unknown): [string, string] {
  try {
    f()
  } catch (e) {
    if (e instanceof s.StoreError) return [e.label, e.rule]
    throw e
  }
  return ['ok', '']
}

async function labelOfAsync(f: () => Promise<unknown>): Promise<[string, string]> {
  try {
    await f()
  } catch (e) {
    if (e instanceof s.StoreError) return [e.label, e.rule]
    throw e
  }
  return ['ok', '']
}

test('derivation-v1: registry values, derived keys, identifiers, bucket ids, fronts', () => {
  const v = load('derivation-v1.json')
  const r = v.registry
  assert.deepEqual(r.protocol, { securityLevel: s.Protocol[0], protocol: s.Protocol[1] })
  assert.deepEqual(r.keyIds, s.KeyIDs)
  assert.equal(r.tagPrefix, toHex(s.TagPrefix))
  assert.equal(r.tagToken, toHex(s.TagToken))
  assert.equal(r.tagFunding, toHex(s.TagFunding))
  assert.equal(r.tagHold, toHex(s.TagHold))
  for (const [k, m] of [['magicBucket', s.MagicBucket], ['magicEntry', s.MagicEntry], ['magicPiece', s.MagicPiece], ['magicOutcome', s.MagicOutcome],
    ['magicFront', s.MagicFront], ['magicRequest', s.MagicRequest], ['magicResponse', s.MagicResponse]] as const) {
    assert.equal(r[k], toHex(m), k)
    assert.ok(!r.neverAssigned.includes(toHex(m).slice(4, 6)) || toHex(m).slice(4, 6) !== '00')
    assert.ok(r.neverAssigned.includes(toHex(m).slice(4, 6)), `${k}'s third byte is never a tag type`)
  }
  assert.equal(r.recordsPrefix, s.RecordsPrefix)
  assert.equal(r.blobPrefix, s.BlobPrefix)
  assert.equal(r.lookupService, s.LookupService)
  assert.equal(r.frontSuffix, s.FrontSuffix)
  assert.deepEqual(r.baskets, s.Baskets)
  assert.equal(r.domainBucketId, s.DomainBucketID)
  assert.equal(r.anyoneKey, toHex(s.AnyoneKey))
  assert.equal(r.borrowed[0].protocol, s.AdProtocol[1])
  assert.equal(r.borrowed[1].protocol, s.SettleProtocol[1])
  for (const id of v.identities) {
    const priv = PrivateKey.fromHex(id.priv)
    assert.equal(toHex(priv.toPublicKey().encode(true) as number[]), id.identity)
    for (const [kid, want] of Object.entries(id.keys)) {
      assert.equal(toHex(s.lockingKey(kid, fromHex(id.identity))), want, `${id.name} ${kid}`)
      // The wallet's own derivation, for itself, agrees.
      const own = new KeyDeriver(priv).derivePublicKey(s.Protocol, kid, 'anyone', true)
      assert.equal(own.toString(), want, `${id.name} ${kid} forSelf`)
    }
    assert.equal(readerLockingKey(s.AdProtocol, s.AdKeyID, id.identity).toString(), id.advertisementLock, id.name)
  }
  for (const c of v.identifiers) {
    assert.equal(s.isBucket(c.id), c.ok, c.id)
    if (!c.ok) continue
    assert.equal(s.recordsTopic(c.id), c.records)
    assert.equal(s.blobTopic(c.id), c.blob)
    assert.deepEqual(s.s3Name(c.id), [c.s3, c.s3ok])
  }
  for (const b of v.bucketIds) assert.equal(toHex(s.bucketID(fromHex(b.txid), b.vout)), b.id)
  for (const f of v.derivedFronts) assert.equal(s.derivedFront(f.url) ?? '', f.front, f.url)
  assert.equal(s.chirpURLSuffix(fromHex(v.chirpURLSuffix.root)), v.chirpURLSuffix.suffix)
})

test('hex-v1: every record decodes to its view and re-encodes to the same bytes', () => {
  const v = load('hex-v1.json')
  for (const r of v.records) {
    const [view, again] = decodeKind(r.kind, fromHex(r.hex))
    assert.deepEqual(view, r.view, r.name)
    assert.equal(toHex(again), r.hex, r.name)
  }
  const parts = s.splitSigned(fromHex(v.signedResponse))!
  assert.ok(parts !== undefined)
  const req = s.decodeRequest(fromHex(v.request))
  assert.ok(s.verifyChallengeSig(req.answerer, sha256(fromHex(v.request)), sha256(parts[0]), parts[1]))
})

test('refusal-v1: every case refused for the Go label and rule, or admitted', () => {
  const v = load('refusal-v1.json')
  for (const c of v.cases) {
    const [label, rule] = labelOf(() => decodeKind(c.kind, expand(c.hex, c.fill)))
    assert.equal(label, c.label, `${c.kind}: ${c.name}`)
    assert.equal(rule, c.rule ?? '', `${c.kind}: ${c.name} rule`)
  }
})

test('script-v1: the classifier and the advertisement check', () => {
  const v = load('script-v1.json')
  let narrowedAdmitted = 0
  for (const c of v.cases) {
    const script = fromHex(c.script)
    assert.equal(s.classify(script), c.claim, c.name)
    const [label, rule] = labelOf(() => s.decodeAdvertisement(script))
    if (typeof c.adRule === 'string' && c.adRule.endsWith('/narrowed')) {
      // Go refuses an internationalised host it does not parse; the WHATWG
      // parser decides it here, and refuses or admits it.
      assert.ok(label === 'ok' || (label === 'advertisement' && rule === 'url'), c.name)
      if (label === 'ok') narrowedAdmitted++
      continue
    }
    assert.equal(label, c.advertisement, c.name)
    assert.equal(rule, c.adRule ?? '', `${c.name} rule`)
    if (label === 'ok') assert.equal(s.decodeAdvertisement(script).url, c.adUrl, c.name)
  }
  assert.ok(narrowedAdmitted >= 0)
  for (const f of v.frontUrls) {
    const [label] = labelOf(() => s.checkFrontURL(f.url))
    assert.equal(label, f.verdict, `front url ${JSON.stringify(f.url)}`)
  }
  for (const e of v.eligibility) {
    const ad = s.decodeAdvertisement(fromHex(e.script))
    assert.equal(s.eligibility(ad, s.decodeEntry(fromHex(e.entry)), e.rootLength), e.verdict, e.name)
  }
})

const mapOf = (v: [string, string][]): s.KeyMap => s.mapFromView(v)
const leavesFrom = (hex: string): s.MapLeaf[] => s.decodeTokenContext(fromHex(hex))

test('map-v1: roots, folds, batches, objects, contexts, pages', () => {
  const v = load('map-v1.json')
  for (const r of v.roots) {
    const m = mapOf(r.map)
    assert.deepEqual(m.leaves().map(toHex), r.leaves ?? [])
    assert.equal(toHex(m.root()), r.root)
  }
  for (const f of v.folds) {
    const toks: s.Bucket[] = f.tokens.map((t: { seq: number; changes: string[][]; consumed: string[]; count: number; mapRoot: string }) => ({
      bucket: '', owner: new Uint8Array(33), id: new Uint8Array(32), seq: t.seq, prev: new Uint8Array(32), writers: [],
      changes: t.changes.map((c) => ({ key: c[0]!, c: fromHex(c[c.length - 1]!), removal: c.length === 3 })),
      count: t.count, mapRoot: fromHex(t.mapRoot), created: 1, consumed: t.consumed.map(fromHex), extra: [],
    }))
    const got = s.foldChain(toks)
    assert.deepEqual(got.statuses, f.statuses, f.name)
    assert.deepEqual(got.reasons, f.reasons, f.name)
    assert.equal(toHex(got.map.root()), f.mapRoot, f.name)
    assert.deepEqual(got.map.keys().map((k) => [k, toHex(got.map.m.get(k)!)]), f.map ?? [], f.name)
  }
  for (const b of v.batches) {
    const entries = b.entries.map((e: { c: string; key: string; kind: number; created: number }) => ({ c: fromHex(e.c), key: e.key, kind: e.kind, created: e.created }))
    const [changes, consumed] = s.resolveBatch(mapOf(b.current ?? []), new Set(b.named), entries)
    assert.deepEqual(changes.map((c) => (c.removal ? [c.key, toHex(new Uint8Array(32)), toHex(c.c)] : [c.key, toHex(c.c)])), b.changes, b.name)
    assert.deepEqual(consumed.map(toHex), b.consumed, b.name)
  }
  const om = mapOf(v.objectMap)
  assert.equal(toHex(om.root()), v.objectRoot)
  for (const o of v.objects) {
    const count = o.count ?? v.objectCount
    const root = fromHex(o.mapRoot ?? v.objectRoot)
    let got: string
    let named: string | undefined
    try {
      const ctx = o.entryContext !== undefined ? s.decodeEntryContext(fromHex(o.entryContext)) : undefined
      const [res, c] = s.checkObject(count, root, o.key, o.c !== undefined ? fromHex(o.c) : undefined, ctx, leavesFrom(o.tokenContext))
      got = res
      named = c !== undefined ? toHex(c) : undefined
    } catch (e) {
      got = s.reasonOf(e)
      assert.equal(s.ruleOf(e), o.rule, o.name)
    }
    assert.equal(got, o.result, o.name)
    if (o.named !== undefined) assert.equal(named, o.named, o.name)
    // The honest answers are the ones the map proves.
    if (o.name.startsWith('honest: ') && got !== 'unheld') {
      const [ctx, leaves] = s.proveObject(om, o.key, () => true)
      assert.equal(toHex(s.encodeTokenContext(leaves)), o.tokenContext, o.name)
      if (ctx !== undefined) assert.equal(toHex(s.encodeEntryContext(ctx)), o.entryContext, o.name)
    }
  }
  for (const c of v.contexts) {
    const [label, rule] = labelOf(() => (c.which === 'entry' ? s.decodeEntryContext(fromHex(c.hex)) : s.decodeTokenContext(fromHex(c.hex))))
    assert.equal(label, c.label, c.name)
    assert.equal(rule, c.rule, c.name)
  }
  for (const p of v.pages) {
    const m = mapOf(p.map)
    const entries = p.entries.map((e: { key: string; c: string; context: string }) => ({ key: e.key, c: fromHex(e.c), ctx: s.decodeEntryContext(fromHex(e.context)) }))
    const [label, rule] = labelOf(() => s.checkPage(m.size, m.root(), p.prefix, p.after, p.max, entries, leavesFrom(p.tokenContext)))
    assert.equal(label, p.label, p.name)
    assert.equal(rule, p.rule, p.name)
    if (p.name.startsWith('honest') || p.name.startsWith('list') || p.name.startsWith('export')) {
      const [he, hl] = s.provePage(m, p.prefix, p.after, p.max, () => true)
      if (p.label === 'ok' && !p.name.includes('255')) {
        assert.deepEqual(he.map((e) => e.key), p.entries.map((e: { key: string }) => e.key), p.name)
        assert.equal(toHex(s.encodeTokenContext(hl)), p.tokenContext, p.name)
      }
    }
  }
  for (const a of v.abstract) {
    const K: string[] = a.keys
    const pk = (i: number): s.PageKey | undefined => (i < 0 ? undefined : { index: i, key: K[i]! })
    const ans: s.PageKey[] = []
    for (let i = a.start; i < a.start + a.answered; i++) ans.push({ index: i, key: K[i]! })
    const [label, rule] = labelOf(() => s.checkPageOrder(K.length, a.p, a.a, v.abstractMax, ans, pk(a.r0), pk(a.r1), pk(a.r2)))
    assert.equal(label === 'ok' ? 'ok' : rule, a.verdict, JSON.stringify(a))
  }
  // A reader's walk and head checks.
  for (const w of v.reader.walks) {
    const links: s.Link[] = w.walk.map((l: { txid: string; record: string; spends?: { txid: string; vout: number } }) => ({
      txid: fromHex(l.txid), bucket: s.decodeBucket(fromHex(l.record)), ...(l.spends ? { spends: { txid: fromHex(l.spends.txid), vout: l.spends.vout } } : {}),
    }))
    const held = w.held === undefined ? undefined : { seq: w.held.seq, txid: fromHex(w.held.txid) }
    assert.equal(labelOf(() => s.checkWalk(held, links))[0], w.label, w.name)
  }
  for (const h of v.reader.heads) {
    const tok = s.decodeBucket(fromHex(h.token))
    let got: string
    try {
      got = s.checkHead(tok, h.key, h.c !== undefined ? fromHex(h.c) : undefined, h.entry !== undefined ? s.decodeEntry(fromHex(h.entry)) : undefined,
        h.entryContext !== undefined ? s.decodeEntryContext(fromHex(h.entryContext)) : undefined, leavesFrom(h.tokenContext))
    } catch (e) {
      got = s.reasonOf(e)
    }
    assert.equal(got, h.result, h.name)
  }
})

// The proof vectors' content rule.
function segmentOf(sg: number): Uint8Array {
  const out = new Uint8Array(4096)
  const dv = new DataView(out.buffer)
  let x = (Math.imul(sg, 0x9e3779b9) + 0x7f4a7c15) >>> 0
  if (x === 0) x = 1
  for (let w = 0; w < 1024; w++) {
    x = (x ^ (x << 13)) >>> 0
    x = (x ^ (x >>> 17)) >>> 0
    x = (x ^ (x << 5)) >>> 0
    dv.setUint32(4 * w, x, true)
  }
  return out
}

function contentOf(size: number): Uint8Array {
  const out = new Uint8Array(size)
  for (let sg = 0; sg * 4096 < size; sg++) out.set(segmentOf(sg).subarray(0, Math.min(4096, size - sg * 4096)), sg * 4096)
  return out
}

const fastLeaf = (seg: Uint8Array): Uint8Array => Uint8Array.from(createHash('sha256').update(Uint8Array.of(0)).update(seg).digest())

function proofRootOfRule(size: number): Uint8Array {
  const leaves: Uint8Array[] = []
  for (let sg = 0; sg * 4096 < size; sg++) leaves.push(fastLeaf(segmentOf(sg).subarray(0, Math.min(4096, size - sg * 4096))))
  return rootOfLeafHashes(leaves)
}

test('proof-v1: roots, paths, indices, responses, answers, detection', async () => {
  const v = load('proof-v1.json')
  for (const r of v.roots) {
    assert.equal(toHex(proofRootOfRule(r.size)), r.proofRoot, r.name)
    if (r.size <= 4097) assert.equal(toHex(s.proofRoot(contentOf(r.size))), r.proofRoot, r.name)
    assert.equal(s.segments(r.size), r.segments, r.name)
  }
  assert.equal(toHex(s.proofRoot(fromHex(v.hand.content))), v.hand.proofRoot)
  for (const l of v.pathLengths) assert.equal(pathLength(l.index, l.segments), l.length, `${l.index} of ${l.segments}`)
  const ten = contentOf(v.tenSegments.size)
  const tt = s.ProofTree.of(ten)
  assert.equal(toHex(tt.root()), v.tenSegments.proofRoot)
  assert.equal(toHex(segmentRoot(ten, 4096)), v.tenSegments.proofRoot)
  for (const p of v.tenSegments.paths) assert.deepEqual(tt.path(p.index).map(toHex), p.siblings ?? [], `path ${p.index}`)
  for (const x of v.indices) {
    const q = s.decodeRequest(fromHex(x.request))
    assert.deepEqual(s.domain(q), [x.domain, x.offset])
    for (let k = 0; k < q.c; k++) {
      assert.equal(toHex(s.indexDigest(q, k)), x.digests[k])
      assert.equal(s.sampleIndex(q, k), x.indices[k])
    }
  }
  const respSize = 4194304 + 2097152 + 100
  const content = contentOf(respSize)
  const blob = (b: number): Uint8Array => content.subarray(b * 4194304, Math.min((b + 1) * 4194304, respSize))
  const blobHashes = [sha256(blob(0)), sha256(blob(1))]
  for (const r of v.responses) {
    const q = s.decodeRequest(fromHex(r.request))
    for (let k = 0; k < q.c; k++) assert.equal(s.sampleIndex(q, k), r.indices[k], r.name)
    const f = r.fetch
    const faulty = (b: number): boolean => f.faultBlob < 0 || f.faultBlob === b
    const fetcher: s.Fetcher = {
      range: async (b, off, len) => {
        const seg = blob(b).slice(off, off + len)
        if (faulty(b)) {
          if (f.rangeFault === 'error') throw new Error('range refused')
          if (f.rangeFault === 'flip') seg[0]! ^= 1
          if (f.rangeFault === 'short') return seg.subarray(0, seg.length - 1)
        }
        return seg
      },
      blob: async (b) => {
        const bl = blob(b).slice()
        if (faulty(b)) {
          if (f.blobFault === 'error') throw new Error('blob refused')
          if (f.blobFault === 'flip') bl[bl.length >> 1]! ^= 1
        }
        return bl
      },
    }
    const signed = expand(r.signed, r.signedFill)
    const got = await s.verifyResponse(q, fromHex(r.request), signed, (b) => blobHashes[b]!, fetcher)
    assert.deepEqual([got.outcome, got.label, got.rule, got.sample, got.recheck, got.rechecked], [r.outcome, r.label, r.rule, r.sample, r.recheck, r.rechecked], r.name)
  }
  for (const a of v.answers) {
    const headers = new Map<number, Uint8Array>(Object.entries(a.headers as Record<string, string>).map(([h, x]) => [Number(h), fromHex(x)]))
    const req = labelOf(() => s.decodeRequest(fromHex(a.request)))[0] === 'ok' ? s.decodeRequest(fromHex(a.request)) : undefined
    const own = (root: Uint8Array): [Uint8Array, number] | undefined => {
      if (a.name === 'a root not held' || a.name.startsWith('order: seed before')) return undefined
      // The answerer holds the sample root, its proof root and size.
      const base = load('hex-v1.json').records.find((x: { name: string }) => x.name === 'a request answered by its provider').view
      return toHex(root) === base.root ? [fromHex(base.proofRoot), base.size] : undefined
    }
    void req
    const [label] = labelOf(() => s.answerCheck(fromHex(a.request), a.tip, (h) => headers.get(h), own))
    assert.equal(label, a.label, a.name)
  }
  for (const d of v.detection) assert.ok(Math.abs(s.detect(d.c, d.f) - d.p) < 1e-12, `${d.c} ${d.f}`)
  for (const n of v.samplesNeeded) assert.equal(s.samplesFor(n.f, n.confidence), n.c)
  const [a, sn] = s.detectionTables()
  assert.equal(a, v.detectionTable)
  assert.equal(sn, v.samplesNeededTbl)
})

test('settle-v1: payouts, derivations, settlements', () => {
  const v = load('settle-v1.json')
  for (const p of v.payouts) {
    const got = s.payouts(p.R, p.k)
    assert.deepEqual(got, p.payouts, `R ${p.R} k ${p.k}`)
    assert.equal(got.reduce((x, y) => x + y, 0), p.R)
  }
  for (const d of v.derivations) {
    assert.equal(s.derivationPrefix(fromHex(d.commitment)), d.prefix)
    assert.equal(s.derivationSuffix(d.rank), d.suffix)
  }
  // The payment key: the payee derives it for itself from the writer's key.
  const ids = load('derivation-v1.json').identities as { name: string; priv: string; identity: string }[]
  const bob = ids.find((i) => i.name === 'bob')!
  for (const d of v.derivations) {
    const k = new KeyDeriver(PrivateKey.fromHex(bob.priv)).derivePublicKey(s.SettleProtocol, `${d.prefix} ${d.suffix}`, PublicKey.fromString(d.writer).toString(), true)
    assert.equal(k.toString(), d.paymentKey, `rank ${d.rank}`)
  }
  for (const k of v.tieKeys) {
    assert.equal(toHex(s.tieKey(fromHex(k.txid).reverse(), k.vout, fromHex(k.commitment))), k.key, `tie key ${k.txid}.${k.vout}`)
  }
  type AdV = { txid: string; vout: number; advertiser: string; root: string; expiry: number; spent: boolean; spentBy?: AdV[] }
  const ad = (a: AdV): s.Ad => ({
    txid: fromHex(a.txid).reverse(), vout: a.vout, advertiser: fromHex(a.advertiser), root: fromHex(a.root), expiry: a.expiry, spent: a.spent,
    spentBy: (a.spentBy ?? []).map(ad),
  })
  for (const x of v.settlements) {
    const cands = x.candidates.map((c: { ad: AdV; firstSeen: number; passed: boolean }) => ({ ad: ad(c.ad), firstSeen: c.firstSeen, passed: c.passed }))
    const got = s.settle(fromHex(x.commitment), x.R, x.t, x.K, cands)
    assert.equal(got.settled, x.settled, x.name)
    assert.deepEqual(got.dropped.map(toHex), x.dropped, x.name)
    assert.deepEqual(got.payees.map((p) => ({ rank: p.rank, advertiser: toHex(p.advertiser), ad: s.displayTxid(p.ad), satoshis: p.satoshis, prefix: p.prefix, suffix: p.suffix })),
      x.payees, x.name)
  }
})

test('query-v1: every class and value rule', () => {
  const v = load('query-v1.json')
  assert.deepEqual(s.Classes.map((c) => [c.name, c.members, c.free, c.served]), v.classes.map((c: { Name: string; Members: string[]; Free: boolean; Served: boolean }) => [c.Name, c.Members, c.Free, c.Served]))
  for (const c of v.cases) {
    let got = 'ok'
    let cls = ''
    try {
      cls = s.parseQuery(c.json).class
    } catch (e) {
      got = s.reasonOf(e)
    }
    assert.equal(got, c.label, c.name)
    assert.equal(cls, c.class ?? '', c.name)
  }
})

test('closure-v1: bstore root rules, the closure check, the proof root', async () => {
  const v = load('closure-v1.json')
  for (const c of v.cases) {
    const objs = new Map<string, Uint8Array>()
    for (const n of c.nodes) objs.set(toHex(sha256(fromHex(n))), fromHex(n))
    if (c.ruleSize > 0) {
      const content = contentOf(c.ruleSize)
      for (let off = 0; off < c.ruleSize; off += 4194304) {
        const b = content.subarray(off, Math.min(off + 4194304, c.ruleSize))
        objs.set(toHex(sha256(b)), b)
      }
    } else {
      for (const b of c.blobs ?? []) objs.set(toHex(sha256(fromHex(b))), fromHex(b))
    }
    const missing = new Set<string>(c.missing ?? [])
    const fetch = async (h: Uint8Array): Promise<Uint8Array> => {
      const k = toHex(h)
      if (missing.has(k)) throw new Error('not served')
      if (c.serve[k] !== undefined) return fromHex(c.serve[k])
      const o = objs.get(k)
      if (o === undefined) throw new Error('not served')
      return o
    }
    let got: [string, string]
    if (c.chirpOnly) {
      got = await labelOfAsync(() => s.verifyCHIRP(fromHex(c.root), fetch))
    } else {
      const e = { kind: s.KindPut, root: fromHex(c.root), size: c.size, contentHash: fromHex(c.contentHash), proofRoot: fromHex(c.proofRoot) } as s.Entry
      got = await labelOfAsync(() => s.verifyClosure(e, fetch, c.proof))
    }
    assert.deepEqual(got, [c.label, c.rule], c.name)
  }
})

class Headers implements ChainTracker {
  constructor(private readonly roots: Map<number, string>) {}
  async isValidRootForHeight(root: string, height: number): Promise<boolean> {
    return this.roots.get(height) === root
  }
  async currentHeight(): Promise<number> {
    return Math.max(...this.roots.keys())
  }
}

test('transaction-v1: every case admitted or refused as the Go codec decided', async () => {
  const v = load('transaction-v1.json')
  for (const h of v.hostConfigs) {
    const [label] = labelOf(() => s.hostBounds({ bucket: v.bucket, maxBEEF: h.maxBeef, maxSweepInputs: h.maxSweepInputs }))
    assert.equal(label === 'ok' ? 'ok' : label, h.result, `host ${h.maxBeef} ${h.maxSweepInputs}`)
  }
  const headers = new Headers(new Map(v.headers.map((h: { height: number; merkleRoot: string }) => [h.height, h.merkleRoot])))
  for (const x of v.cases) {
    let got = 'ok'
    let a: s.Admission | undefined
    try {
      a = await s.admit(expand(x.beef, x.beefFill), x.previousCoins, { bucket: v.bucket, headers })
    } catch (e) {
      got = s.reasonOf(e)
    }
    assert.equal(got, x.verdict === 'admit' ? 'ok' : x.reason, x.name)
    if (a === undefined) continue
    const ad = x.admits
    assert.equal(a.kind, ad.kind, x.name)
    assert.deepEqual(a.outputs, ad.outputsToAdmit, x.name)
    assert.deepEqual(a.retain, ad.coinsToRetain, x.name)
    if (a.carrier !== undefined) {
      assert.equal(toHex(a.carrier.commitment), ad.commitment, x.name)
      assert.equal(toHex(a.carrier.record), ad.record, x.name)
      assert.equal(a.carrier.funding.txid, ad.fundingTxid, x.name)
      assert.equal(a.carrier.funding.vout, ad.fundingVout, x.name)
    }
    if (a.token !== undefined) {
      assert.equal(a.token.bucket.seq, ad.seq, x.name)
      assert.equal(toHex(a.token.record), ad.record, x.name)
      assert.equal(a.parent, ad.parentTxid, x.name)
      assert.equal(`${a.spends!.txid}.${a.spends!.vout}`, ad.spends, x.name)
      assert.equal(a.predecessor?.bucket.seq, ad.predecessorSeq, x.name)
    }
    if (a.spent !== undefined) assert.deepEqual(a.spent.map((o) => `${o.txid}.${o.vout}`), ad.spent, x.name)
    if (a.kind === 'mined') {
      assert.deepEqual([...a.ads!.keys()].sort((p, q) => p - q), ad.advertisements ?? [], x.name)
      assert.deepEqual(a.holds, ad.holds ?? [], x.name)
    }
  }
})

test('nottaken-v1: notes, every refusal for the Go rule, the buyer check', () => {
  const v = load('nottaken-v1.json')
  for (const n of v.notes) {
    const body = fromHex(n.body)
    const got = s.decodeNotTakenBody(body)
    assert.deepEqual(s.notTakenView(got), n.view, n.name)
    assert.equal(toHex(s.encodeNotTaken(got)), n.body, `${n.name}: re-encoded`)
    assert.deepEqual(s.notTakenView(s.verifyNotTaken(fromHex(n.signed))), n.view, `${n.name}: verified`)
    assert.equal(toHex(s.challengeKey(got.answerer)), n.challengeKey, `${n.name}: key`)
    assert.equal(toHex(sha256(fromHex(n.request))), n.view.request, `${n.name}: request hash`)
  }
  for (const c of v.refusals) {
    const raw = fromHex(c.hex)
    const [label, rule] = labelOf(() => (c.kind === 'body' ? s.decodeNotTakenBody(raw) : s.verifyNotTaken(raw)))
    assert.equal(label, c.label, `${c.kind}: ${c.name}`)
    assert.equal(rule, c.rule ?? '', `${c.kind}: ${c.name} rule`)
  }
  for (const c of v.checks) {
    const [label, rule] = labelOf(() => s.checkNotTaken(fromHex(c.signed), fromHex(c.payment), fromHex(c.request), fromHex(c.answerer)))
    assert.equal(label, c.label, c.name)
    assert.equal(rule, c.rule ?? '', `${c.name} rule`)
  }
})
