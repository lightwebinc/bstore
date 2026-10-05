/**
 * The shipped file rather than the tsc tree: bundle/bstore-module.js as
 * the bundle step leaves it, mounted the way the host mounts a module,
 * admitting golden carriers, tokens, advertisements and a sweep against a
 * header source over HTTP, and answering ls_bstore. Every other test runs
 * on dist/, which resolves the library from node_modules, so only this one
 * proves the bundle works with the library inlined and nothing but
 * @bsv/sdk and Node's built-ins beside it.
 */
import { test, after } from 'node:test'
import assert from 'node:assert/strict'
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { Module, ModuleHost } from '@lightwebinc/bcommon'
import { FakeHost, beefOf, byName, heldOf, txOf, txVectors } from './testutil.js'

const v = txVectors()
// dist/bundle.test.js -> host/. Imported by URL, so the type checker does not
// go looking for a file that exists only after the bundle step.
const bundleUrl = new URL('../bundle/bstore-module.js', import.meta.url)

function version(pkg: string): string {
  const url = new URL(`../node_modules/${pkg}/package.json`, import.meta.url)
  return (JSON.parse(readFileSync(url, 'utf8')) as { version: string }).version
}

/** The vectors' headers in the native shape a module reads. */
async function headerSource(): Promise<string> {
  const roots = new Map(v.headers.map((h) => [h.height, h.merkleRoot]))
  const server: Server = createServer((req, res) => {
    const m = /^\/v1\/root\/(\d+)$/.exec(req.url ?? '')
    const body = req.url === '/v1/tip' ? { height: Math.max(...roots.keys()) } : m !== null && roots.has(Number(m[1])) ? { merkleRoot: roots.get(Number(m[1])) } : undefined
    res.writeHead(body === undefined ? 404 : 200, { 'content-type': 'application/json' })
    res.end(JSON.stringify(body ?? {}))
  })
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  after(() => server.close())
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

async function mount(host: FakeHost, dir: string, headers: string): Promise<Module> {
  const { default: create } = (await import(bundleUrl.href)) as { default: (host: ModuleHost) => Promise<Module> }
  const keys = ['BSTORE_BUCKETS', 'BSTORE_STATE_DIR', 'BSTORE_HEADERS_URL', 'OVERLAY_TOPICS'] as const
  const saved = Object.fromEntries(keys.map((k) => [k, process.env[k]]))
  try {
    process.env['BSTORE_BUCKETS'] = v.bucket
    process.env['BSTORE_STATE_DIR'] = dir
    process.env['BSTORE_HEADERS_URL'] = headers
    process.env['OVERLAY_TOPICS'] = `tm_anytx,${v.topic}`
    return await create(host)
  } finally {
    for (const k of keys) {
      if (saved[k] === undefined) delete process.env[k]
      else process.env[k] = saved[k]
    }
  }
}

test('the shipped bundle admits golden carriers, tokens, mined objects and a sweep, refuses by raising, and ls_bstore answers them', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bstore-bundle-'))
  try {
    const host = new FakeHost()
    const mod = await mount(host, dir, await headerSource())
    const tm = mod.topics?.[v.topic]
    const ls = mod.lookups?.['ls_bstore'] as (NonNullable<Module['lookups']>[string] & { restore: (o: unknown[], s: unknown) => Promise<number> }) | undefined
    if (tm === undefined || ls === undefined) throw new Error('the bundle did not mount the bucket and ls_bstore')
    await ls.restore([], { findOutput: async () => null })
    for (const name of ['entry-put', 'outcome-pass', 'front', 'token-first', 'token-put', 'advertisement', 'hold-output']) {
      const t = byName(v, name)
      assert.deepEqual(await tm.identifyAdmissibleOutputs(beefOf(t), heldOf(t)), { outputsToAdmit: t.admits!.outputsToAdmit, coinsToRetain: t.admits!.coinsToRetain }, name)
      for (const i of t.admits!.outputsToAdmit) await ls.outputAdmittedByTopic({ mode: 'whole-tx', atomicBEEF: txOf(t).toAtomicBEEF(), outputIndex: i, topic: v.topic })
    }
    assert.equal(host.count('bstore_admitted_total', { kind: 'entry' }), 1)
    assert.equal(host.count('bstore_admitted_total', { kind: 'bucket-token' }), 2)
    await assert.rejects(tm.identifyAdmissibleOutputs(beefOf(byName(v, 'carrier-final-input')), []), /mineable/)
    assert.equal(host.count('bstore_refused_total', { reason: 'mineable' }), 1)
    await assert.rejects(tm.identifyAdmissibleOutputs(beefOf(byName(v, 'piece')), []), /piece/)
    const entry = await ls.lookup({ service: 'ls_bstore', query: { bucket: v.bucket, entry: byName(v, 'entry-put').txid } })
    assert.deepEqual(entry.map((f) => f.txid), [byName(v, 'entry-put').txid])
    assert.equal(host.gauges.get('bstore_tokens')?.(), 2)
    assert.ok(existsSync(join(dir, 'bstore-rows.jsonl')), 'the rows are on disk')
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('the bundle names its versions in its banner and in the host log', async () => {
  const banner = readFileSync(bundleUrl, 'utf8').split('\n', 1)[0] ?? ''
  assert.ok(banner.startsWith('// @lightwebinc/bstore-host '), banner)
  assert.ok(banner.includes(`@lightwebinc/bcommon ${version('@lightwebinc/bcommon')} inlined`), banner)
  assert.ok(banner.includes(`imports @bsv/sdk (built against ${version('@bsv/sdk')})`), banner)
  const dir = mkdtempSync(join(tmpdir(), 'bstore-bundle-'))
  try {
    const host = new FakeHost()
    await mount(host, dir, 'http://127.0.0.1:1')
    const mounted = host.lines.filter((l) => l.msg === 'bstore module mounted')
    assert.equal(mounted.length, 1)
    assert.equal(`// ${String(mounted[0]?.extra?.['build'])}`, banner)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('the bundle imports nothing but @bsv/sdk and Node built-ins, and carries no source map', () => {
  const text = readFileSync(bundleUrl, 'utf8')
  const specifiers = new Set([...text.matchAll(/^\s*(?:import|export)\b[^'"]*?from\s*["']([^"']+)["']/gm)].map((m) => m[1]!))
  assert.ok(specifiers.has('@bsv/sdk'))
  assert.deepEqual([...specifiers].filter((s) => s !== '@bsv/sdk' && !s.startsWith('node:')), [])
  assert.equal(/\bimport\s*\(|\brequire\s*\(/.test(text), false, 'no dynamic import or require')
  assert.equal(text.includes('sourceMappingURL'), false)
  assert.equal(existsSync(new URL('../bundle/bstore-module.js.map', import.meta.url)), false)
})

test('the bundle holds no test helper and no provider route: a host fetches nothing a record names', () => {
  const text = readFileSync(bundleUrl, 'utf8')
  for (const word of ['PayingWallet', 'OwnerSim', 'fromNowhere', 'verifyResponse', 'verifyClosure']) assert.equal(text.includes(word), false, word)
  for (const route of ['challenge', 'renew', 'settle']) assert.equal(new RegExp(`["'\`]/${route}["'\`]`).test(text), false, route)
})
