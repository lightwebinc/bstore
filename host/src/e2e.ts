/**
 * The host module end to end on a local reference overlay host, loaded
 * from the bundle (bundle/bstore-module.js) through OVERLAY_MODULES.
 *
 *   node dist/e2e.js /path/to/reference-host
 *
 * The reference host directory holds its compiled dist/index.js and its
 * node_modules. The bundle is staged as a deployment places it: copied into
 * a modules/ directory whose only node_modules is the host's, so it loads
 * with nothing but what the host provides. The run needs Docker, for a
 * throwaway MySQL (the host's only store), and serves block headers itself
 * in the host's native shape for a local chain it mines on.
 *
 * It mounts both topics of a bucket (the blob topic refusing everything),
 * admits a served chain, entries, outcomes, front statements,
 * advertisements, a renewal, a hold output and sweeps, refuses a worse
 * BEEF of a token and a piece without a trace, asks every lookup class
 * (the five priceable ones paid through their 402 on the terms route, the
 * payments landing in the shared payee ledger), checks every map answer as
 * a reader does, retracts an entry by a sweep and shows its replay never
 * answered, restarts the host cleanly and after a kill with the same
 * answers, moves the host's clock forward to watch retention drop what its
 * floors allow and keep the rest, replays a dropped object and sees it
 * decided afresh, and floods and replays the terms route to see its
 * hardening hold. The clock is moved by a preload the harness hands the
 * host's process (it shifts Date.now); the module has no test hook.
 * Everything it starts, it stops.
 */
import { execFileSync, spawn, type ChildProcess } from 'node:child_process'
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { createServer, request, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { AuthFetch, PrivateKey, Random, Transaction, Utils, type CreateActionArgs, type CreateActionResult, type WalletInterface } from '@bsv/sdk'
import { encodePiece } from './records.js'
import { KeyEntry, KeyPiece } from './storerec.js'
import { Buyer, Chain, FrontSim, OwnerSim, PayingWallet, Provider, Writer, sweep } from './testmint.js'
import { readObject, readPage } from './testworld.js'
import type { Answered } from './testhost.js'
import { sha256, toHex, utf8 } from './util.js'

const bucket = 'example_photos_hmqzvtkwrx'
const topic = `tm_bstore_${bucket}`
const blobTopic = `tm_bstoreblob_${bucket}`
const password = 'bstore-e2e'
const payeeKey = '11'.repeat(32)
const prices = 'history=5,chain=10,export=20,outcome-history=3,outcome-history-of=4'
const bundle = fileURLToPath(new URL('../bundle/bstore-module.js', import.meta.url))
const day = 86400_000
const hour = 3600_000

function fail(msg: string): never {
  throw new Error(`e2e: ${msg}`)
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

/** Stages the bundle beside the reference host's node_modules, and the clock preload; returns the paths. */
function stage(dir: string): { file: string; root: string; clock: string } {
  const root = mkdtempSync(join(tmpdir(), 'bstore-e2e-'))
  symlinkSync(resolve(dir, 'node_modules'), join(root, 'node_modules'))
  mkdirSync(join(root, 'modules', 'bstore'), { recursive: true })
  mkdirSync(join(root, 'state'))
  const file = join(root, 'modules', 'bstore', 'bstore-module.js')
  copyFileSync(bundle, file)
  const clock = join(root, 'clock.mjs')
  writeFileSync(clock, "const off = Number(process.env.E2E_CLOCK_OFFSET_MS ?? '0')\nif (off !== 0) { const real = Date.now; Date.now = () => real() + off }\n")
  return { file, root, clock }
}

async function headers(chain: Chain): Promise<Server> {
  const server = createServer((req, res) => {
    const m = /^\/v1\/root\/(\d+)$/.exec(req.url ?? '')
    const body = req.url === '/v1/tip' ? { height: chain.tip } : m !== null && chain.roots.has(Number(m[1])) ? { merkleRoot: chain.roots.get(Number(m[1])) } : undefined
    res.writeHead(body === undefined ? 404 : 200, { 'content-type': 'application/json' })
    res.end(JSON.stringify(body ?? {}))
  })
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  return server
}

function docker(...args: string[]): string {
  return execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim()
}

async function mysql(): Promise<{ id: string; port: number }> {
  const id = docker('run', '-d', '--rm', '-e', `MYSQL_ROOT_PASSWORD=${password}`, '-e', 'MYSQL_DATABASE=overlay', '-p', '127.0.0.1::3306', '--tmpfs', '/var/lib/mysql', 'mysql:9')
  const port = Number(docker('port', id, '3306/tcp').split('\n')[0]!.split(':').pop())
  for (let i = 0; i < 120; i++) {
    try {
      docker('exec', id, 'mysql', '-h127.0.0.1', '-uroot', `-p${password}`, '-e', 'select 1')
      return { id, port }
    } catch {
      await sleep(1000)
    }
  }
  docker('rm', '-f', id)
  fail('MySQL did not come up')
}

async function freePort(): Promise<number> {
  return await new Promise<number>((r) => {
    const s = createServer().listen(0, '127.0.0.1', () => {
      const p = (s.address() as AddressInfo).port
      s.close(() => r(p))
    })
  })
}

interface Host {
  proc: ChildProcess
  url: string
  terms: string
  out: string[]
}

interface Ports {
  db: number
  headers: number
  host: number
  terms: number
}

async function startHost(dir: string, staged: ReturnType<typeof stage>, p: Ports, served: string, offsetMs = 0): Promise<Host> {
  const out: string[] = []
  const proc = spawn(process.execPath, ['--import', pathToFileURL(staged.clock).href, resolve(dir, 'dist/index.js')], {
    env: {
      PATH: process.env['PATH'] ?? '',
      E2E_CLOCK_OFFSET_MS: String(offsetMs),
      OVERLAY_TOPICS: `${topic},${blobTopic}`,
      OVERLAY_MODULES: staged.file,
      OVERLAY_KNEX_URL: `mysql://root:${password}@127.0.0.1:${p.db}/overlay`,
      OVERLAY_CHAIN_TRACKER_URL: `http://127.0.0.1:${p.headers}`,
      OVERLAY_ADMIN_TOKEN: 'bstore-e2e',
      OVERLAY_LISTEN: '127.0.0.1',
      OVERLAY_PORT: String(p.host),
      BSTORE_BUCKETS: bucket,
      BSTORE_STATE_DIR: join(staged.root, 'state'),
      BSTORE_SERVED: served,
      BSTORE_LISTEN: `127.0.0.1:${p.terms}`,
      BSTORE_PRICES: prices,
      BSTORE_PAYEE_KEY: payeeKey,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  proc.stdout!.on('data', (b: Buffer) => out.push(b.toString()))
  proc.stderr!.on('data', (b: Buffer) => out.push(b.toString()))
  const url = `http://127.0.0.1:${p.host}`
  for (let i = 0; i < 60; i++) {
    if (proc.exitCode !== null) fail(`the host exited ${proc.exitCode}:\n${out.join('')}`)
    try {
      if ((await fetch(`${url}/readyz`)).status === 200) return { proc, url, terms: `http://127.0.0.1:${p.terms}`, out }
    } catch {
      // not listening yet
    }
    await sleep(500)
  }
  proc.kill('SIGKILL')
  fail(`the host did not become ready:\n${out.join('')}`)
}

async function stopHost(h: Host, signal: NodeJS.Signals = 'SIGTERM'): Promise<void> {
  if (h.proc.exitCode !== null || h.proc.signalCode !== null) return
  const done = new Promise((r) => h.proc.once('exit', r))
  h.proc.kill(signal)
  await done
}

function mountLine(h: Host): string {
  return h.out.join('').split('\n').find((l) => l.includes('bstore module mounted')) ?? fail(`no mount line:\n${h.out.join('')}`)
}

async function submitTo(h: Host, t: string, beef: number[] | Uint8Array, what: string): Promise<number[]> {
  const res = await fetch(`${h.url}/submit`, { method: 'POST', headers: { 'content-type': 'application/octet-stream', 'x-topics': t }, body: Uint8Array.from(beef) })
  const body = (await res.json()) as Record<string, { outputsToAdmit?: number[] }>
  if (res.status !== 200) fail(`submit ${what}: ${res.status} ${JSON.stringify(body)}`)
  return body[t]?.outputsToAdmit ?? []
}

const submit = async (h: Host, beef: number[] | Uint8Array, what: string): Promise<number[]> => await submitTo(h, topic, beef, what)
const submitTx = async (h: Host, tx: Transaction, what: string): Promise<number[]> => await submit(h, tx.toAtomicBEEF(), what)

interface Answer {
  outputs?: Array<{ beef: number[]; outputIndex: number; context?: number[] }>
  error?: string
}

const asked = (body: Answer): Answered[] =>
  (body.outputs ?? []).map((o) => {
    const tx = Transaction.fromBEEF(o.beef)
    return { tx, txid: tx.id('hex'), vout: o.outputIndex, beef: o.beef, context: o.context }
  })

async function query(base: string, q: Record<string, string>): Promise<Answered[] | string> {
  const res = await fetch(`${base}/lookup`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ service: 'ls_bstore', query: q }) })
  const body = (await res.json()) as Answer
  if (res.status !== 200) return `${res.status} ${body.error ?? ''}`
  return asked(body)
}

async function ask(h: Host, q: Record<string, string>): Promise<Answered[]> {
  const a = await query(h.url, q)
  if (typeof a === 'string') fail(`${JSON.stringify(q)}: ${a}`)
  return a
}

const txids = async (h: Host, q: Record<string, string>): Promise<string[]> => (await ask(h, q)).map((o) => o.txid)

function expect(what: string, got: unknown, want: unknown): void {
  const g = JSON.stringify(got)
  const w = JSON.stringify(want)
  if (g !== w) fail(`${what}: got ${g}, want ${w}`)
  console.log(`ok  ${what}`)
}

function metric(text: string, name: string, ...labels: string[]): string | undefined {
  return text
    .split('\n')
    .find((l) => l.startsWith(name) && labels.every((x) => l.includes(x)))
    ?.trim()
    .split(' ')
    .pop()
}

/** Waits up to ten seconds for f to hold. */
async function eventually(what: string, f: () => Promise<boolean>): Promise<void> {
  for (let i = 0; i < 40; i++) {
    if (await f()) {
      console.log(`ok  ${what}`)
      return
    }
    await sleep(250)
  }
  fail(`${what}: did not happen`)
}

/** AuthFetch prints each payment attempt; the run reads the responses instead. */
async function quietly<T>(f: () => Promise<T>): Promise<T> {
  const saved = { warn: console.warn, info: console.info, error: console.error }
  console.warn = () => {}
  console.info = () => {}
  console.error = () => {}
  try {
    return await f()
  } finally {
    Object.assign(console, saved)
  }
}

/** One BRC-104 initial request, raw, from a fresh identity. */
async function shake(url: string): Promise<number> {
  const body = JSON.stringify({ version: '0.1', messageType: 'initialRequest', identityKey: PrivateKey.fromRandom().toPublicKey().toString(), initialNonce: Utils.toBase64(Random(32)), requestedCertificates: { certifiers: [], types: {} } })
  return await new Promise((resolveStatus, reject) => {
    const req = request(`${url}/.well-known/auth`, { method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      res.resume()
      res.on('end', () => resolveStatus(res.statusCode ?? 0))
    })
    req.on('error', reject)
    req.end(body)
  })
}

async function main(): Promise<void> {
  const dir = process.argv[2]
  if (dir === undefined) fail('usage: node dist/e2e.js /path/to/reference-host')
  const chain = new Chain(30000)
  const owner = new OwnerSim(bucket, 'the owner', chain)
  const alice = new Writer(bucket, 'alice', chain)
  const ownerAsWriter = new Writer(bucket, 'the owner', chain)
  const bob = new Writer(bucket, 'bob', chain)
  const prov = new Provider('provider', chain)
  const prov2 = new Provider('second provider', chain)
  const buyer = new Buyer(bucket, 'buyer', chain)
  const ownerAsBuyer = new Buyer(bucket, 'the owner', chain)
  const front = new FrontSim(bucket, 'front', chain)
  const served = `${bucket}:${owner.hex}:${owner.idHex}`
  const chainQ = { bucket, owner: owner.hex, id: owner.idHex }
  // Records are dated now, so the classes that never answer a future one do.
  const now = Date.now()
  const nowS = Math.floor(now / 1000)
  const hdr = await headers(chain)
  const db = await mysql()
  const ports: Ports = { db: db.port, headers: (hdr.address() as AddressInfo).port, host: await freePort(), terms: await freePort() }
  const staged = stage(dir)
  let host: Host | undefined
  try {
    host = await startHost(dir, staged, ports, served)
    const mounted = mountLine(host)
    if (!mounted.includes('with @lightwebinc/bcommon') || !mounted.includes('refuses everything')) fail(`the mount line: ${mounted}`)
    console.log("ok  the bundle mounts from beside the host's node_modules, both topics of the bucket")

    // The blob topic admits nothing; a piece on the records topic neither.
    const piece = await alice.post(KeyPiece, encodePiece({ bucket, writer: alice.id, root: sha256(utf8('root')), piece: utf8('some bytes'), extra: [] }))
    expect('a piece on the blob topic is refused', await submitTo(host, blobTopic, piece.tx.toAtomicBEEF(), 'piece blob'), [])
    expect('a piece on the records topic is refused', await submit(host, piece.tx.toAtomicBEEF(), 'piece'), [])

    // The chain: its first token in a worse BEEF first, refused by raising, nothing recorded.
    const t0 = await owner.token()
    expect('the first token as Atomic BEEF admits nothing', await submit(host, t0.tx.toAtomicBEEF(), 'worse token'), [])
    expect('the first token with its parent is admitted after it', await submit(host, t0.beef, 't0'), [0])

    // Entries: alice's anchored and replaced, the owner's pending, bob's a stranger's.
    const hold = alice.hold(50_000)
    const a1 = await alice.put(owner.id, 'photos/a.jpg', { created: now, hold: hold.terms })
    const b1 = await alice.put(owner.id, 'photos/b.jpg', { created: now })
    const c1 = await alice.put(owner.id, 'photos/c.jpg', { created: now })
    const a2 = await alice.put(owner.id, 'photos/a.jpg', { label: 'a again', created: now + 1 })
    const pend = await ownerAsWriter.put(owner.id, 'photos/p.jpg', { created: now + 2 })
    const strange = await bob.put(owner.id, 'photos/s.jpg', { created: now + 3 })
    const swept = await alice.put(owner.id, 'photos/x.jpg', { created: now + 4 })
    for (const [e, name] of [[a1, 'a1'], [b1, 'b1'], [a2, 'a2'], [pend, 'pending'], [strange, 'stranger'], [swept, 'to be swept']] as const) expect(`submit entry ${name}`, await submitTx(host, e.tx, name), [0])
    expect('the hold output', await submitTx(host, hold.tx, 'hold'), [0])
    const t1 = await owner.token({ writers: [alice.id], anchor: [a1, b1, c1] })
    expect('the token that anchors them', await submit(host, t1.beef, 't1'), [0])

    // Advertisements, a renewal, front statements and outcomes.
    const ad = await prov.advertise([a1.e.root], nowS + 30 * 86400)
    const ad2 = await prov2.advertise([a1.e.root], nowS + 30 * 86400)
    const renewed = await prov2.renew(ad2, 0, a1.e.root, nowS + 60 * 86400)
    for (const [tx, name] of [[ad, 'ad'], [ad2, 'ad2'], [renewed, 'renewal']] as const) expect(`submit ${name}`, await submitTx(host, tx, name), [0])
    const f1 = await front.statement(prov.party.id, { created: now })
    const o1 = await ownerAsBuyer.outcome({ root: b1.e.root, provider: prov.party, created: now })
    const o2 = await buyer.outcome({ root: a1.e.root, provider: prov.party, answerer: front.party, created: now + 1 })
    for (const [x, name] of [[f1, 'front'], [o1, 'outcome'], [o2, 'outcome 2']] as const) expect(`submit ${name}`, await submitTx(host, x.tx, name), [0])

    // Every free class, each map answer checked as a reader checks it.
    expect('bucket: the latest token', await txids(host, chainQ), [t1.txid])
    expect('bucket-seq', await txids(host, { ...chainQ, seq: '0' }), [t0.txid])
    expect('object: found, proven against t1', readObject(await ask(host, { ...chainQ, key: 'photos/a.jpg' }), 'photos/a.jpg'), { result: 'found', entry: a1.txid, token: t1.txid })
    expect('object: absent, proven', readObject(await ask(host, { ...chainQ, key: 'photos/z.jpg' }), 'photos/z.jpg'), { result: 'absent', token: t1.txid })
    expect('object: unheld, named by its leaf', readObject(await ask(host, { ...chainQ, key: 'photos/c.jpg' }), 'photos/c.jpg'), { result: 'unheld', token: t1.txid })
    const page = readPage(await ask(host, { ...chainQ, prefix: 'photos/', after: '' }), 'photos/', '')
    expect('list: complete up to the entry the host cannot produce', [page.keys, page.leaves], [['photos/a.jpg', 'photos/b.jpg'], [[2, 'photos/c.jpg']]])
    expect('pending: no token names it, the owner wrote it', await txids(host, { ...chainQ, pending: 'photos/p.jpg' }), [pend.txid])
    expect('pending: a stranger is not answered', await txids(host, { ...chainQ, pending: 'photos/s.jpg' }), [])
    expect('unanchored: alice, by txid', await txids(host, { ...chainQ, writer: alice.hex, after: '' }), [a2.txid, swept.txid].sort())
    expect('entry', await txids(host, { bucket, entry: strange.txid }), [strange.txid])
    expect('hold', await txids(host, { bucket, hold: hold.op }), [hold.tx.id('hex')])
    expect('ads: live ones, the renewed one in place of what it spent', await txids(host, { bucket, root: toHex(a1.e.root), after: '' }), [ad.id('hex'), renewed.id('hex')].sort())
    expect('ad', await txids(host, { bucket, root: toHex(a1.e.root), advertiser: prov2.hex }), [renewed.id('hex')])
    expect('fronts', await txids(host, { bucket, advertiser: prov.hex, after: '' }), [f1.txid])
    expect('front', await txids(host, { bucket, advertiser: prov.hex, front: front.hex }), [f1.txid])
    expect('outcomes', await txids(host, { bucket, answerer: prov.hex }), [o1.txid])
    expect('outcomes-of', await txids(host, { bucket, provider: prov.hex }), [o2.txid, o1.txid])
    expect('outcomes-by', await txids(host, { bucket, answerer: front.hex, buyer: buyer.hex }), [o2.txid])
    expect('outcomes-of-by', await txids(host, { bucket, provider: prov.hex, buyer: owner.hex }), [o1.txid])

    // Retraction: alice sweeps an entry; the sweep is answered by the outpoint; the replay is admitted and never answered.
    const sw = await sweep(swept.tree, [swept.vout], alice.party, KeyEntry, chain)
    expect('submit the sweep', await submitTx(host, sw, 'sweep'), [0])
    expect('sweep: by the outpoint it spent', await txids(host, { bucket, spent: swept.op }), [sw.id('hex')])
    expect('the swept entry is answered no more', await txids(host, { bucket, entry: swept.txid }), [])
    expect('the replay of the swept entry is admitted by the manager', await submitTx(host, swept.tx, 'replay'), [0])
    expect('and never answered', await txids(host, { bucket, entry: swept.txid }), [])
    await sleep(3000)
    expect('evicted: a second replay is decided afresh, and dropped again', await submitTx(host, swept.tx, 'replay 2'), [0])

    // Priced: refused on the host's own route; 402 and paid on the terms route.
    const refused = await query(host.url, { bucket, owner: owner.hex, chain: owner.idHex, after: '0' })
    if (typeof refused !== 'string' || !refused.includes('priced')) fail(`chain on the host route: ${JSON.stringify(refused)}`)
    console.log('ok  a priced class on the host route is refused')
    expect('the terms document', await (await fetch(`${host.terms}/ls_bstore/terms`)).json(), {
      service: 'ls_bstore',
      terms: 1,
      classes: [
        { class: 'history', satoshis: 5 },
        { class: 'chain', satoshis: 10 },
        { class: 'export', satoshis: 20 },
        { class: 'outcome-history', satoshis: 3 },
        { class: 'outcome-history-of', satoshis: 4 },
      ],
    })
    expect('a priced class without BRC-104', await query(host.terms, { bucket, owner: owner.hex, chain: owner.idHex, after: '0' }), '401 ')
    const wallet = new PayingWallet(PrivateKey.fromHex('22'.repeat(32)), chain)
    const client = new AuthFetch(wallet as unknown as WalletInterface)
    let paidTotal = 0
    const paid = async (q: Record<string, string>): Promise<Answered[]> =>
      await quietly(async () => {
        const res = await client.fetch(`${host!.terms}/lookup`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ service: 'ls_bstore', query: q }) })
        if (res.status !== 200) fail(`paid ${JSON.stringify(q)}: ${res.status} ${await res.text()}`)
        paidTotal += Number(res.headers.get('x-bsv-payment-satoshis-paid') ?? 0)
        return asked((await res.json()) as Answer)
      })
    expect('history, paid', (await paid({ ...chainQ, history: 'photos/a.jpg', before: '9' })).map((o) => o.txid), [t1.txid, a1.txid])
    expect('chain, paid', (await paid({ bucket, owner: owner.hex, chain: owner.idHex, after: '0' })).map((o) => o.txid), [t1.txid])
    const exported = readPage(await paid({ bucket, owner: owner.hex, export: owner.idHex, after: '' }), '', '', true)
    expect('export, paid: complete up to the entry the host cannot produce', exported.keys, ['photos/a.jpg', 'photos/b.jpg'])
    expect('outcome-history, paid', (await paid({ bucket, 'outcome-history': prov.hex, before: `${now + day}:${'f'.repeat(64)}` })).map((o) => o.txid), [o1.txid])
    expect('outcome-history-of, paid', (await paid({ bucket, 'outcome-history-of': prov.hex, before: `${now + day}:${'f'.repeat(64)}` })).map((o) => o.txid), [o2.txid, o1.txid])
    expect('the payee was paid for five questions', paidTotal, 5 + 10 + 20 + 3 + 4)
    const ledger = (): string[] => readFileSync(join(staged.root, 'state', 'payments.jsonl'), 'utf8').trim().split('\n')
    expect('each payment is a line of the shared payee ledger, version 2, keys in its order', ledger().map((l) => Object.keys(JSON.parse(l) as object).join(',')), Array<string>(5).fill('txid,beef,outputIndex,satoshis,derivationPrefix,derivationSuffix,senderIdentityKey,class,inputs,at'))

    // Hardening: a handshake flood is refused before any signature; a replayed request 401; a coin pays once.
    const flood = await Promise.all(Array.from({ length: 30 }, () => shake(host!.terms)))
    expect('a flood from one address: its burst answered, the rest 429', [flood.filter((s) => s === 200).length <= 8, flood.filter((s) => s === 429).length >= 22], [true, true])
    await sleep(8000)
    class OneCoin extends PayingWallet {
      readonly same = this.coin()
      override async createAction(args: CreateActionArgs): Promise<CreateActionResult> {
        const tx = await this.pay((args.outputs ?? []).map((o) => ({ satoshis: o.satoshis, lockingScript: o.lockingScript })), this.same)
        return { tx: tx.toAtomicBEEF(), txid: tx.id('hex') }
      }
    }
    const one = new AuthFetch(new OneCoin(PrivateKey.fromHex('33'.repeat(32)), chain) as unknown as WalletInterface)
    const statuses: number[] = []
    for (let i = 0; i < 2; i++) {
      statuses.push(await quietly(async () => (await one.fetch(`${host!.terms}/lookup`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ service: 'ls_bstore', query: { bucket, owner: owner.hex, chain: owner.idHex, after: '0' } }) })).status))
    }
    expect('two payments of one coin buy one answer: the second 409', statuses, [200, 409])

    let metrics = await (await fetch(`${host.url}/metrics`)).text()
    expect('refusals are counted by reason', [metric(metrics, 'bstore_refused_total', 'reason="beef"'), metric(metrics, 'bstore_refused_total', 'reason="piece"')], ['1', '2'])
    expect('the host counts them as failed topic admissions', metric(metrics, 'overlay_host_topic_failures_total', `topic="${topic}"`), '2')
    expect('every token consistent', metric(metrics, 'bstore_token_status_total', 'status="consistent"'), '2')
    expect('one retraction, however often its carrier is replayed', metric(metrics, 'bstore_retractions_total'), '1')
    expect('drops of the retracted carrier: at the sweep and at each replay', metric(metrics, 'bstore_dropped_total', 'why="retracted"'), '3')

    // A clean restart and a kill: the same answers.
    const snapshot = async (h: Host): Promise<unknown> => ({
      latest: await txids(h, chainQ),
      object: readObject(await ask(h, { ...chainQ, key: 'photos/a.jpg' }), 'photos/a.jpg'),
      list: readPage(await ask(h, { ...chainQ, prefix: '', after: '' }), '', ''),
      pending: await txids(h, { ...chainQ, pending: 'photos/p.jpg' }),
      ads: await txids(h, { bucket, root: toHex(a1.e.root), after: '' }),
      outcomes: await txids(h, { bucket, provider: prov.hex }),
      sweep: await txids(h, { bucket, spent: swept.op }),
      swept: await txids(h, { bucket, entry: swept.txid }),
    })
    const before = await snapshot(host)
    await stopHost(host)
    host = await startHost(dir, staged, ports, served)
    expect('restarted: the same answers', await snapshot(host), before)
    await stopHost(host, 'SIGKILL')
    host = await startHost(dir, staged, ports, served)
    expect('after a kill: the same answers', await snapshot(host), before)
    expect('the swept entry offered again stays retracted', [await submitTx(host, swept.tx, 'replay 3'), await txids(host, { bucket, entry: swept.txid })], [[0], []])

    // A second anchor replaces a1; the replaced entry keeps its 31 days.
    const t2 = await owner.token({ anchor: [a2] })
    expect('the token that replaces a1', await submit(host, t2.beef, 't2'), [0])
    expect('object: a2 now', readObject(await ask(host, { ...chainQ, key: 'photos/a.jpg' }), 'photos/a.jpg').entry, a2.txid)

    // Retention: eight days on, the stranger's entry (24 hours) and the owner's pending one (7 days) go; the anchored and the replaced stay.
    await stopHost(host)
    host = await startHost(dir, staged, ports, served, 8 * day)
    const h8 = host
    await eventually("eight days on: the stranger's and the pending entry are dropped", async () => (await txids(h8, { bucket, entry: strange.txid })).length === 0 && (await txids(h8, { bucket, entry: pend.txid })).length === 0)
    expect('the anchored and the replaced stay', [await txids(host, { bucket, entry: a2.txid }), await txids(host, { bucket, entry: a1.txid })], [[a2.txid], [a1.txid]])
    expect("outcomes: the owner's on a root the map names stays, a stranger's goes", await txids(host, { bucket, provider: prov.hex }), [o1.txid])
    expect('dropped means evicted: the pending entry offered again is admitted afresh', await submitTx(host, pend.tx, 'pending again'), [0])
    expect('and answered again', await txids(host, { ...chainQ, pending: 'photos/p.jpg' }), [pend.txid])
    metrics = await (await fetch(`${host.url}/metrics`)).text()
    if (Number(metric(metrics, 'bstore_dropped_total', 'why="retention"') ?? 0) < 2) fail('retention drops are not counted')
    console.log('ok  retention drops are counted')

    // Forty days on: the replaced entry and the advertisements go; the served chain's tokens and the anchored entry stay.
    await stopHost(host)
    host = await startHost(dir, staged, ports, served, 40 * day + hour)
    const h40 = host
    await eventually('forty days on: the replaced entry is dropped', async () => (await txids(h40, { bucket, entry: a1.txid })).length === 0)
    expect("a served chain's tokens are kept", await txids(host, { ...chainQ, seq: '0' }), [t0.txid])
    expect('the anchored entry is kept', readObject(await ask(host, { ...chainQ, key: 'photos/a.jpg' }), 'photos/a.jpg').entry, a2.txid)
    expect('the swept entry offered again is still dropped', [await submitTx(host, swept.tx, 'replay 4'), await txids(host, { bucket, entry: swept.txid })], [[0], []])
    console.log('e2e: PASS')
  } finally {
    if (host !== undefined) await stopHost(host)
    docker('rm', '-f', db.id)
    hdr.close()
    rmSync(staged.root, { recursive: true, force: true })
  }
}

main().catch((e: unknown) => {
  console.error(e instanceof Error ? e.message : e)
  process.exit(1)
})
