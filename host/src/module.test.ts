/**
 * The module's configuration and its mount: every BSTORE_ variable read
 * exactly or refused, the floors held, prices needing their route and
 * payee, OVERLAY_TOPICS and OVERLAY_SYNC_PEERS checked, and dist/module.js
 * loaded by absolute path as the reference host loads a module.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import type { Module, ModuleHost } from '@lightwebinc/bcommon'
import { checkNoSync, parseBuckets, parseConfig, parseServed, storeModule } from './module.js'
import { MemoryJournal } from './journal.js'
import { TokenStatuses } from './ls_bstore.js'
import { config } from './testhost.js'
import { FakeHost } from './testutil.js'
import { Reasons } from './tm_bstore.js'

const b1 = 'photos_qzvmkwerty'
const b2 = 'backups_abcdefghij'
const owner = '02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5'
const id = 'ab'.repeat(32)

test('BSTORE_BUCKETS: bucket identifiers, once each; anything else refused', () => {
  assert.deepEqual(parseBuckets(` ${b1} , ${b2}`), [b1, b2])
  for (const bad of [undefined, '', 'photos', 'Photos_qzvmkwerty', `${b1},${b1}`, 'photos__x_qzvmkwerty', 'photos_qzvmkwert']) assert.throws(() => parseBuckets(bad), String(bad))
})

test('BSTORE_SERVED: <bucket>:<owner>:<id> of a carried bucket, once each', () => {
  assert.deepEqual(parseServed(`${b1}:${owner}:${id}`, [b1]), [`${b1}|${owner}|${id}`])
  assert.deepEqual(parseServed('', [b1]), [])
  for (const bad of [`${b2}:${owner}:${id}`, `${b1}:${owner}`, `${b1}:${owner.toUpperCase()}:${id}`, `${b1}:0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798:${id}`, `${b1}:${owner}:${id.slice(2)}`, `${b1}:${owner}:${id},${b1}:${owner}:${id}`]) {
    assert.throws(() => parseServed(bad, [b1]), bad)
  }
})

test('the configuration: a state directory and a native header source are required, floors hold, and a price needs its route and payee', () => {
  const base = { BSTORE_BUCKETS: b1, BSTORE_STATE_DIR: '/var/lib/bstore', BSTORE_HEADERS_URL: 'https://headers.example' }
  const c = parseConfig(base)
  assert.deepEqual([c.keepDays, c.pendingDays, c.otherHours, c.maxBEEF, c.maxSweepInputs, c.sessions.max, c.sessions.ttlSeconds], [31, 7, 24, 262144, 1024, 10000, 600])
  assert.deepEqual([c.handshakes, c.responses], [{ perSec: 4, burst: 8, perAddressPerSec: 1, addressBurst: 4 }, { perSec: 5, burst: 20 }])
  assert.equal(parseConfig({ ...base, BSTORE_HEADERS_URL: undefined, OVERLAY_CHAIN_TRACKER_URL: 'http://127.0.0.1:8090' }).headersURL, 'http://127.0.0.1:8090')
  assert.equal(parseConfig({ ...base, BSTORE_KEEP_DAYS: '60', BSTORE_PENDING_DAYS: '14', BSTORE_OTHER_HOURS: '48' }).keepDays, 60)
  for (const [k, v] of [
    ['BSTORE_STATE_DIR', 'relative/dir'],
    ['BSTORE_HEADERS_URL', 'ftp://x'],
    ['BSTORE_KEEP_DAYS', '30'],
    ['BSTORE_PENDING_DAYS', '6'],
    ['BSTORE_OTHER_HOURS', '23'],
    ['BSTORE_MAX_BEEF', '262143'],
    ['BSTORE_MAX_SWEEP_INPUTS', '1023'],
    ['BSTORE_LISTEN', 'nope'],
    ['BSTORE_PAYEE_KEY', 'AB'.repeat(32)],
    ['BSTORE_PRICES', 'list=5'],
    ['BSTORE_PRICES', 'history=5'],
    ['BSTORE_HANDSHAKES_PER_SEC', '0'],
    ['BSTORE_SESSIONS', '0'],
  ] as const) {
    assert.throws(() => parseConfig({ ...base, [k]: v }), `${k}=${v}`)
  }
  assert.throws(() => parseConfig({ ...base, BSTORE_HEADERS_URL: undefined, OVERLAY_CHAIN_TRACKER_URL: 'whatsonchain' }), /BSTORE_HEADERS_URL is required/)
  assert.throws(() => parseConfig({ ...base, BSTORE_PRICES: 'history=5', BSTORE_LISTEN: '127.0.0.1:9000' }), /BSTORE_PAYEE_KEY are required/)
  assert.throws(() => parseConfig({ ...base, BSTORE_PRICES: 'history=0' }), /needs BSTORE_LISTEN/)
  const priced = parseConfig({ ...base, BSTORE_PRICES: 'history=5,chain=0', BSTORE_LISTEN: '[::1]:9000', BSTORE_PAYEE_KEY: '11'.repeat(32) })
  assert.deepEqual(priced.listen, { host: '::1', port: 9000 })
})

test('OVERLAY_TOPICS: every bstore topic must be a configured bucket; a blob topic is mounted only where it is named; no bstore topic is synchronised', () => {
  const c = { ...config, buckets: [b1] }
  const o = { journal: new MemoryJournal(), headers: { isValidRootForHeight: async () => false, currentHeight: async () => 0 } }
  assert.throws(() => storeModule(new FakeHost(), c, [`tm_bstore_${b1}`, `tm_bstore_${b2}`], o), new RegExp(`tm_bstore_${b2}`))
  assert.throws(() => storeModule(new FakeHost(), c, [`tm_bstore_${b1}`, `tm_bstoreblob_${b2}`], o), new RegExp(`tm_bstoreblob_${b2}`))
  assert.deepEqual(Object.keys(storeModule(new FakeHost(), c, [`tm_bstore_${b1}`], o).module.topics!), [`tm_bstore_${b1}`])
  const host = new FakeHost()
  assert.deepEqual(Object.keys(storeModule(host, c, ['tm_anytx', `tm_bstore_${b1}`, `tm_bstoreblob_${b1}`], o).module.topics!).sort(), [`tm_bstore_${b1}`, `tm_bstoreblob_${b1}`])
  assert.match(String(host.lines.find((l) => l.msg === 'bstore module mounted')?.extra?.['blobs']), /refuses everything/)
  assert.throws(() => checkNoSync(`tm_anytx=https://peer.example;tm_bstore_${b1}=https://peer.example`), /never synchronised/)
  assert.throws(() => checkNoSync(`tm_bstoreblob_${b1}=https://peer.example`), /never synchronised/)
  checkNoSync('tm_anytx=https://peer.example')
  checkNoSync(undefined)
})

function loaderShape(m: unknown): Module {
  const mod = m as Module
  for (const tm of Object.values(mod.topics ?? {})) assert.equal(typeof tm.identifyAdmissibleOutputs, 'function')
  for (const ls of Object.values(mod.lookups ?? {})) {
    assert.equal(typeof ls.lookup, 'function')
    assert.equal(typeof ls.outputAdmittedByTopic, 'function')
    assert.equal(typeof ls.admissionMode, 'string')
    assert.equal(typeof (ls as { restore?: unknown }).restore, 'function', 'the index is in memory, so the module must restore')
  }
  return mod
}

test('dist/module.js loads by absolute path, as OVERLAY_MODULES names it, and mounts each bucket and ls_bstore', async () => {
  const path = fileURLToPath(new URL('./module.js', import.meta.url))
  const imported = (await import(pathToFileURL(path).href)) as { default: (h: ModuleHost) => Promise<Module> }
  const keys = ['BSTORE_BUCKETS', 'BSTORE_STATE_DIR', 'BSTORE_HEADERS_URL', 'OVERLAY_TOPICS', 'OVERLAY_SYNC_PEERS'] as const
  const saved = Object.fromEntries(keys.map((k) => [k, process.env[k]]))
  const dir = mkdtempSync(join(tmpdir(), 'bstore-module-'))
  try {
    process.env['BSTORE_BUCKETS'] = `${b1},${b2}`
    process.env['BSTORE_STATE_DIR'] = dir
    process.env['BSTORE_HEADERS_URL'] = 'http://127.0.0.1:1'
    process.env['OVERLAY_TOPICS'] = `tm_anytx,tm_bstore_${b1},tm_bstore_${b2}`
    const host = new FakeHost()
    const mod = loaderShape(await imported.default(host))
    assert.deepEqual(Object.keys(mod.topics!).sort(), [`tm_bstore_${b2}`, `tm_bstore_${b1}`])
    assert.deepEqual(Object.keys(mod.lookups!), ['ls_bstore'])
    for (const reason of Reasons) assert.ok(host.counters.has(`bstore_refused_total{reason="${reason}"}`), `${reason} is preset`)
    for (const status of TokenStatuses) assert.ok(host.counters.has(`bstore_token_status_total{status="${status}"}`), `${status} is preset`)
    for (const g of ['bstore_buckets', 'bstore_tokens', 'bstore_entries', 'bstore_outcomes', 'bstore_fronts', 'bstore_advertisements', 'bstore_hold_outputs', 'bstore_sweeps', 'bstore_sweep_outpoints', 'bstore_retraction_marks', 'bstore_pending_drops', 'bstore_maps_incomplete']) assert.ok(host.gauges.has(g), g)
    assert.equal(host.gauges.get('bstore_buckets')!(), 2)
    const mounted = host.lines.find((l) => l.msg === 'bstore module mounted')
    assert.deepEqual([mounted?.extra?.['lookup'], mounted?.extra?.['build'], mounted?.extra?.['terms']], ['ls_bstore', 'unbundled', 'none'])
    process.env['OVERLAY_SYNC_PEERS'] = `tm_bstore_${b1}=https://peer.example`
    await assert.rejects(imported.default(new FakeHost()), new RegExp(`OVERLAY_SYNC_PEERS names tm_bstore_${b1}`))
    delete process.env['OVERLAY_SYNC_PEERS']
    delete process.env['BSTORE_BUCKETS']
    await assert.rejects(imported.default(new FakeHost()), /BSTORE_BUCKETS/)
  } finally {
    for (const k of keys) {
      if (saved[k] === undefined) delete process.env[k]
      else process.env[k] = saved[k]
    }
    rmSync(dir, { recursive: true, force: true })
  }
})
