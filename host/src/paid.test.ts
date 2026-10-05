/**
 * The terms route over real HTTP: the terms document, free questions with
 * and without BRC-104, each priceable class answered 402 and then answered
 * once paid by the SDK's own AuthFetch client, every refusal of a payment,
 * the payee ledger in the shared format, the bound on sessions, the
 * handshake budget, the budget of signed responses, replayed requests and
 * conflicting payments.
 */
import { test, after } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import type { AddressInfo } from 'node:net'
import { createServer, request, type IncomingHttpHeaders, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  AuthFetch,
  LockingScript,
  P2PKH,
  PrivateKey,
  ProtoWallet,
  PublicKey,
  Random,
  Transaction,
  UnlockingScript,
  Utils,
  createNonce,
  type CreateActionArgs,
  type CreateActionResult,
  type PeerSession,
  type WalletInterface,
} from '@bsv/sdk'
import { DefaultBudget, type BudgetConfig, type ResponseBudgetConfig } from './budget.js'
import { MemoryJournal } from './journal.js'
import { storeModule } from './module.js'
import { BoundedSessions, LedgerReceiver, LookupFront, MemoryReceiver, PaymentProtocol, ledgerLine, parsePrices, spentOutpoints, termsDocument, type ReceivedPayment } from './paid.js'
import { Chain, OwnerSim, Party, PayingWallet, Provider, Buyer, Writer, T0, fromNowhere } from './testmint.js'
import { Clock, bucket, config, engineOf, topic } from './testhost.js'
import { FakeHost, MemoryStorage } from './testutil.js'
import { readPage } from './testworld.js'

const payee = PrivateKey.fromHex('11'.repeat(32))
const payeeWallet = new ProtoWallet(payee)
const payeeKey = payee.toPublicKey().toString()
const all = 'history=5,chain=10,export=20,outcome-history=3,outcome-history-of=4'

interface Rig {
  url: string
  server: Server
  receiver: MemoryReceiver
  wallet: PayingWallet
  client: Party
  chain: Chain
  owner: OwnerSim
  prov: Provider
  /** What the priced classes answer: the tokens, the entries in key order, the outcomes newest first. */
  tokens: string[]
  entries: string[]
  outcomes: string[]
  front: LookupFront
  host: FakeHost
  /** Signatures the payee's wallet made. */
  signatures: () => number
}

const servers: Server[] = []
after(() => {
  for (const s of servers) s.close()
})

/** A module serving one chain of three tokens, the second anchoring three entries, with outcomes, its engine, and its terms route on a free port. */
async function rig(prices: string, sessions?: { max: number; ttlSeconds: number }, budget?: BudgetConfig, responses?: ResponseBudgetConfig): Promise<Rig> {
  const host = new FakeHost()
  const chain = new Chain(95000)
  const owner = new OwnerSim(bucket, 'paid owner', chain)
  const clock = new Clock(T0 + 86_400_000)
  const m = storeModule(host, { ...config, prices: parsePrices(prices), served: [owner.served] }, [topic], { journal: new MemoryJournal(), headers: chain.tracker, now: clock.now })
  const storage = new MemoryStorage()
  await m.ls.restore([], storage)
  const engine = engineOf(m.module, storage, chain.tracker)
  const alice = new Writer(bucket, 'paid writer', chain)
  const es = [await alice.put(owner.id, 'k/1'), await alice.put(owner.id, 'k/2'), await alice.put(owner.id, 'k/3')]
  for (const x of es) await engine.submit({ beef: x.tx.toAtomicBEEF(), topics: [topic] })
  const ts = [await owner.token(), await owner.token({ writers: [alice.party.id], anchor: es }), await owner.token()]
  for (const t of ts) await engine.submit({ beef: Array.from(t.beef), topics: [topic] })
  const prov = new Provider('paid provider', chain)
  const buyer = new Buyer(bucket, 'paid buyer', chain)
  const os = [await buyer.outcome({ root: es[0]!.e.root, provider: prov.party, created: T0 }), await buyer.outcome({ root: es[0]!.e.root, provider: prov.party, created: T0 + 1 })]
  for (const o of os) await engine.submit({ beef: o.tx.toAtomicBEEF(), topics: [topic] })
  const client = new Party('client')
  const receiver = new MemoryReceiver()
  let signatures = 0
  const wallet = new ProtoWallet(payee)
  const sign = wallet.createSignature.bind(wallet)
  wallet.createSignature = async (args) => {
    signatures++
    return await sign(args)
  }
  const front = new LookupFront({ ls: m.ls, host, prices: parsePrices(prices), wallet, receiver, headers: chain.tracker, sessions, budget, responses })
  const server = await front.listen(0, '127.0.0.1')
  servers.push(server)
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    server,
    receiver,
    wallet: new PayingWallet(client.key, chain),
    client,
    chain,
    owner,
    prov,
    tokens: ts.map((t) => t.txid),
    entries: es.map((e) => e.txid),
    outcomes: os.map((o) => o.txid).reverse(),
    front,
    host,
    signatures: () => signatures,
  }
}

const free = (r: Rig): Record<string, string> => ({ bucket, owner: r.owner.hex, id: r.owner.idHex })
const chainQ = (r: Rig): Record<string, string> => ({ bucket, owner: r.owner.hex, chain: r.owner.idHex, after: '0' })
const historyQ = (r: Rig): Record<string, string> => ({ ...free(r), history: 'k/2', before: '9' })
const exportQ = (r: Rig): Record<string, string> => ({ bucket, owner: r.owner.hex, export: r.owner.idHex, after: '' })
const outcomeQ = (r: Rig): Record<string, string> => ({ bucket, 'outcome-history': r.prov.hex, before: `${T0 + 9}:${'f'.repeat(64)}` })
const outcomeOfQ = (r: Rig): Record<string, string> => ({ bucket, 'outcome-history-of': r.prov.hex, before: `${T0 + 9}:${'f'.repeat(64)}` })

const post = (body: unknown, headers: Record<string, string> = {}): { method: string; headers: Record<string, string>; body: string } => ({
  method: 'POST',
  headers: { 'content-type': 'application/json', ...headers },
  body: JSON.stringify(body),
})

const txids = (a: { outputs: Array<{ beef: number[] }> }): string[] => a.outputs.map((o) => Transaction.fromBEEF(o.beef).id('hex'))

/** AuthFetch prints each payment attempt; the tests read the responses instead. */
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

interface Seen402 {
  required: unknown
  version: unknown
  prefix: unknown
}

/** Runs f and returns, beside its result, the payment headers of every 402 the route answered while it ran. */
async function watching<T>(r: Rig, f: () => Promise<T>): Promise<{ result: T; seen: Seen402[] }> {
  const seen: Seen402[] = []
  const watch = (_req: IncomingMessage, res: ServerResponse): void => {
    const writeHead = res.writeHead.bind(res) as (status: number, headers: Record<string, string>) => ServerResponse
    res.writeHead = ((status: number, headers: Record<string, string>) => {
      if (status === 402) seen.push({ required: headers['x-bsv-payment-satoshis-required'], version: headers['x-bsv-payment-version'], prefix: headers['x-bsv-payment-derivation-prefix'] })
      return writeHead(status, headers)
    }) as typeof res.writeHead
  }
  r.server.on('request', watch)
  try {
    return { result: await f(), seen }
  } finally {
    r.server.off('request', watch)
  }
}

test('prices: priceable classes only, once each, 0 to 2^53 - 1; the terms document lists them in class order', () => {
  assert.deepEqual([...parsePrices('export=7, chain=20')], [['export', 7], ['chain', 20]])
  assert.deepEqual(parsePrices(undefined).size, 0)
  for (const bad of ['bucket=1', 'object=1', 'list=1', 'outcomes=1', 'sweep=1', 'history', 'history=-1', 'history=01', 'history=1.5', 'history=1,history=2', 'nosuch=1', 'chain=9007199254740992']) {
    assert.throws(() => parsePrices(bad), bad)
  }
  assert.deepEqual(termsDocument(parsePrices('outcome-history-of=7,chain=20,history=0')), {
    service: 'ls_bstore',
    terms: 1,
    classes: [
      { class: 'history', satoshis: 0 },
      { class: 'chain', satoshis: 20 },
      { class: 'outcome-history-of', satoshis: 7 },
    ],
  })
})

test('the terms document, free questions without authentication, and refusals as errors', async () => {
  const r = await rig(all)
  const terms = await fetch(`${r.url}/ls_bstore/terms`)
  assert.equal(terms.status, 200)
  assert.deepEqual(await terms.json(), termsDocument(parsePrices(all)))
  const answered = await fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: free(r) }))
  assert.equal(answered.status, 200)
  assert.deepEqual(txids((await answered.json()) as never), [r.tokens[2]])
  for (const q of [{ ...free(r), bucket: 'other_qzvmkwerty' }, { ...free(r), extra: 'x' }, { bucket, owner: r.owner.hex }]) {
    assert.equal((await fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: q }))).status, 400)
  }
  assert.equal((await fetch(`${r.url}/lookup`, { method: 'POST', body: 'nope' })).status, 400)
  assert.equal((await fetch(`${r.url}/nosuch`)).status, 404)
  for (const q of [chainQ(r), historyQ(r), exportQ(r), outcomeQ(r), outcomeOfQ(r)]) {
    assert.equal((await fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: q }))).status, 401, 'a priced class is asked over BRC-104')
  }
  // No spelling turns a free question into a priced one, or a priced one into a free one.
  assert.equal((await fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: { ...chainQ(r), id: r.owner.idHex } }))).status, 400)
  assert.equal((await fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: free(r) }, { 'x-bsv-auth-nonce': 'x' }))).status, 400)
  // The engine's own route refuses a priced class with an error that says where to ask.
  await assert.rejects((async () => await (r.front as unknown as { o: { ls: { lookup: (q: unknown) => Promise<unknown> } } }).o.ls.lookup({ service: 'ls_bstore', query: chainQ(r) }))(), /priced on this host/)

  const none = await rig('')
  assert.equal((await fetch(`${none.url}/ls_bstore/terms`)).status, 404, 'a host that prices nothing serves no terms')
  const unpriced = await fetch(`${none.url}/lookup`, post({ service: 'ls_bstore', query: chainQ(none) }))
  assert.equal(unpriced.status, 200)
  assert.deepEqual(txids((await unpriced.json()) as never), none.tokens.slice(1))
})

test('each of the five priceable classes is answered 402 over BRC-104 with the price and a prefix, and answered once paid', async () => {
  const r = await rig(all)
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  const cases: Array<[string, Record<string, string>, number, string[]]> = [
    ['history', historyQ(r), 5, [r.tokens[1]!, r.entries[1]!]],
    ['chain', chainQ(r), 10, r.tokens.slice(1)],
    ['export', exportQ(r), 20, [r.tokens[2]!, ...r.entries]],
    ['outcome-history', outcomeQ(r), 3, r.outcomes],
    ['outcome-history-of', outcomeOfQ(r), 4, r.outcomes],
  ]
  for (const [cls, query, price, want] of cases) {
    const { result: paid, seen } = await watching(r, async () => await quietly(async () => await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query }))))
    assert.equal(seen.length, 1, cls)
    assert.deepEqual([seen[0]!.required, seen[0]!.version], [String(price), '1.0'], cls)
    assert.ok(typeof seen[0]!.prefix === 'string' && seen[0]!.prefix.length > 0, `${cls}: a derivation prefix`)
    assert.equal(paid.status, 200, cls)
    assert.equal(paid.headers.get('x-bsv-payment-satoshis-paid'), String(price), cls)
    assert.deepEqual(txids((await paid.json()) as never), want, cls)
  }
  assert.deepEqual(r.receiver.payments.map((p) => [p.class, p.satoshis]), [['history', 5], ['chain', 10], ['export', 20], ['outcome-history', 3], ['outcome-history-of', 4]])
  assert.equal(r.host.count('bstore_payments_total', { result: 'requested' }), 5)
  assert.equal(r.host.count('bstore_payments_total', { result: 'accepted' }), 5)
})

test('the terms route answers as the engine does: an export page carries the token context and each entry context, and checks', async () => {
  const r = await rig('')
  const res = await fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: exportQ(r) }))
  assert.equal(res.status, 200)
  const body = (await res.json()) as { outputs: Array<{ beef: number[]; outputIndex: number; context?: number[] }> }
  const page = readPage(body.outputs.map((o) => {
    const tx = Transaction.fromBEEF(o.beef)
    return { tx, txid: tx.id('hex'), vout: o.outputIndex, beef: o.beef, context: o.context }
  }), '', '', true)
  assert.deepEqual(page.keys, ['k/1', 'k/2', 'k/3'])
})

test('over BRC-104: a free question is answered and signed; a priced one is answered 402, paid, and answered', async () => {
  const r = await rig('history=5')
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  const signed = await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: free(r) }))
  assert.equal(signed.status, 200)
  assert.equal(signed.headers.get('x-bsv-auth-identity-key'), payeeKey, 'signed by the payee')
  const paid = await quietly(async () => await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: historyQ(r) })))
  assert.equal(paid.status, 200)
  assert.equal(paid.headers.get('x-bsv-payment-satoshis-paid'), '5')
  assert.equal(r.receiver.payments.length, 1)
  const p = r.receiver.payments[0]!
  assert.equal(p.senderIdentityKey, r.client.hex)
  assert.equal(p.class, 'history')
  assert.equal(p.satoshis, 5)
  // What the payee's wallet internalizes: output 0 pays the key it derives for the remittance.
  const { publicKey } = await payeeWallet.getPublicKey({ protocolID: PaymentProtocol, keyID: `${p.derivationPrefix} ${p.derivationSuffix}`, counterparty: p.senderIdentityKey, forSelf: true })
  const tx = Transaction.fromAtomicBEEF(p.beef)
  assert.equal(tx.outputs[0]!.lockingScript.toHex(), new P2PKH().lock(PublicKey.fromString(publicKey).toAddress()).toHex())
  // A second question is a second payment.
  const again = await quietly(async () => await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: historyQ(r) })))
  assert.equal(again.status, 200)
  assert.equal(r.receiver.payments.length, 2)
})

test("a payment is refused unless the prefix is the server's, output 0 pays the price to the derived key, and it verifies; it buys one question", async () => {
  const r = await rig('history=5')
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  const ask = async (payment: string): Promise<{ status: number; code?: string }> => {
    const res = await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: historyQ(r) }, { 'x-bsv-payment': payment }))
    const body = (await res.json()) as { code?: string }
    return { status: res.status, code: body.code }
  }
  const prefix = await createNonce(payeeWallet as unknown as WalletInterface)
  const suffix = Utils.toBase64(Random(32))
  const derived = async (counterparty: string): Promise<string> => {
    const { publicKey } = await r.wallet.getPublicKey({ protocolID: PaymentProtocol, keyID: `${prefix} ${suffix}`, counterparty })
    return new P2PKH().lock(PublicKey.fromString(publicKey).toAddress()).toHex()
  }
  const header = (tx: Transaction, pre = prefix): string => JSON.stringify({ derivationPrefix: pre, derivationSuffix: suffix, transaction: Utils.toBase64(tx.toAtomicBEEF()) })
  const good = await r.wallet.pay([{ satoshis: 5, lockingScript: await derived(payeeKey) }])

  assert.deepEqual(await ask('not json'), { status: 400, code: 'ERR_MALFORMED_PAYMENT' })
  assert.deepEqual(await ask(JSON.stringify({ derivationPrefix: prefix, derivationSuffix: suffix })), { status: 400, code: 'ERR_MALFORMED_PAYMENT' })
  assert.deepEqual(await ask(header(good, Utils.toBase64(Random(32)))), { status: 400, code: 'ERR_INVALID_DERIVATION_PREFIX' })
  assert.deepEqual(await ask(header(await r.wallet.pay([{ satoshis: 4, lockingScript: await derived(payeeKey) }]))), { status: 400, code: 'ERR_INVALID_PAYMENT' })
  const other = PrivateKey.fromRandom().toPublicKey().toString()
  assert.deepEqual(await ask(header(await r.wallet.pay([{ satoshis: 5, lockingScript: await derived(other) }]))), { status: 400, code: 'ERR_INVALID_PAYMENT' })
  // Paid from a coin whose block the host's headers do not hold.
  const stray = fromNowhere([{ satoshis: 100000, lockingScript: new P2PKH().lock(r.client.key.toAddress()) }])
  new Chain(99000).mine(stray)
  assert.deepEqual(await ask(header(await r.wallet.pay([{ satoshis: 5, lockingScript: await derived(payeeKey) }], stray))), { status: 400, code: 'ERR_PAYMENT_SPV' })
  assert.equal(r.receiver.payments.length, 0, 'nothing refused was taken')
  assert.equal((await ask(header(good))).status, 200)
  assert.deepEqual(await ask(header(good)), { status: 409, code: 'ERR_PAYMENT_REPLAYED' })
  assert.equal(r.receiver.payments.length, 1)
})

test('one coin pays once: conflicting unbroadcast payments that spend it buy one answer, not one each', async () => {
  const r = await rig('chain=10')
  class OneCoin extends PayingWallet {
    readonly same = this.coin()
    override async createAction(args: CreateActionArgs): Promise<CreateActionResult> {
      const tx = await this.pay((args.outputs ?? []).map((o) => ({ satoshis: o.satoshis, lockingScript: o.lockingScript })), this.same)
      return { tx: tx.toAtomicBEEF(), txid: tx.id('hex') }
    }
  }
  const af = new AuthFetch(new OneCoin(r.client.key, r.chain) as unknown as WalletInterface)
  const statuses: number[] = []
  const codes: Array<string | undefined> = []
  for (let i = 0; i < 4; i++) {
    const res = await quietly(async () => await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: chainQ(r) })))
    statuses.push(res.status)
    codes.push(res.status === 200 ? undefined : ((await res.json()) as { code?: string }).code)
  }
  assert.deepEqual(statuses, [200, 409, 409, 409])
  assert.deepEqual(codes, [undefined, 'ERR_PAYMENT_CONFLICT', 'ERR_PAYMENT_CONFLICT', 'ERR_PAYMENT_CONFLICT'])
  assert.equal(r.receiver.payments.length, 1)
  assert.equal(r.host.count('bstore_payments_total', { result: 'replayed' }), 3)
})

test('the payment ledger keeps every accepted payment across a restart and refuses its txids, and coins it spent, again', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bstore-ledger-'))
  try {
    const p: ReceivedPayment = { txid: 'ab'.repeat(32), beef: [1, 1, 1, 1], outputIndex: 0, satoshis: 5, derivationPrefix: 'AA==', derivationSuffix: 'AQ==', senderIdentityKey: payeeKey, class: 'history', inputs: [`${'dd'.repeat(32)}.0`] }
    const a = new LedgerReceiver(dir)
    assert.equal(a.claim(p), 'accepted')
    assert.equal(a.claim(p), 'replayed')
    a.close()
    const b = new LedgerReceiver(dir)
    assert.equal(b.claim(p), 'replayed', 'claimed before the restart')
    assert.equal(b.claim({ ...p, txid: 'cd'.repeat(32) }), 'conflict', 'another transaction spending the coin the first one spent')
    assert.equal(b.claim({ ...p, txid: 'cd'.repeat(32), inputs: [`${'ee'.repeat(32)}.1`] }), 'accepted')
    assert.equal(b.claim({ ...p, txid: 'ef'.repeat(32), inputs: [] }), 'conflict', 'a payment that names no coin is not one')
    b.close()
    const c = new LedgerReceiver(dir)
    assert.equal(c.claim({ ...p, txid: '12'.repeat(32), inputs: [`${'ee'.repeat(32)}.1`, `${'ff'.repeat(32)}.0`] }), 'conflict', 'the coins are remembered across a restart')
    c.close()
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

/**
 * The golden ledger line: a payment built from fixed keys on a fixed chain,
 * as the route accepts one, written by the ledger exactly as
 * testdata/host/payments-v2.jsonl holds it. A Go test holds the same file
 * to bcommon's payee package: it is the shared format's own bytes, version
 * 2, so the payee's settle tooling reads it as it reads any host's.
 */
async function goldenPayment(): Promise<ReceivedPayment> {
  const chain = new Chain(123456)
  const key = PrivateKey.fromHex('22'.repeat(32))
  const coin = new Transaction(1, [{ sourceTXID: 'aa'.repeat(32), sourceOutputIndex: 3, unlockingScript: new UnlockingScript(), sequence: 0xffffffff }], [{ satoshis: 100000, lockingScript: new P2PKH().lock(key.toAddress()) }], 0)
  chain.mine(coin)
  const prefix = 'cHJlZml4LWZvci10aGUtZ29sZGVuLWxpbmU='
  const suffix = 'c3VmZml4'
  const { publicKey } = await new ProtoWallet(key).getPublicKey({ protocolID: PaymentProtocol, keyID: `${prefix} ${suffix}`, counterparty: payeeKey })
  const tx = new Transaction(
    1,
    [{ sourceTransaction: coin, sourceOutputIndex: 0, unlockingScriptTemplate: new P2PKH().unlock(key), sequence: 0xffffffff }],
    [
      { satoshis: 5, lockingScript: new P2PKH().lock(PublicKey.fromString(publicKey).toAddress()) },
      { satoshis: 99895, lockingScript: LockingScript.fromHex(new P2PKH().lock(key.toAddress()).toHex()) },
    ],
    0,
  )
  await tx.sign()
  return { txid: tx.id('hex'), beef: tx.toAtomicBEEF(), outputIndex: 0, satoshis: 5, derivationPrefix: prefix, derivationSuffix: suffix, senderIdentityKey: key.toPublicKey().toString(), class: 'history', inputs: spentOutpoints(tx) }
}

const golden = new URL('../../testdata/host/payments-v2.jsonl', import.meta.url)

test('the ledger writes the shared payee ledger line, byte for byte as the golden file holds it', async () => {
  const p = await goldenPayment()
  const at = 1_798_761_600
  const line = ledgerLine(p, at)
  if (process.env['BSTORE_WRITE_GOLDEN'] === '1') writeFileSync(golden, line)
  assert.equal(line, readFileSync(golden, 'utf8'))
  const dir = mkdtempSync(join(tmpdir(), 'bstore-ledger-'))
  try {
    const ledger = new LedgerReceiver(dir, () => at * 1000)
    assert.equal(ledger.claim(p), 'accepted')
    ledger.close()
    assert.equal(readFileSync(join(dir, 'payments.jsonl'), 'utf8'), line)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

const session = (nonce: string, key: string, lastUpdate = 0): PeerSession => ({ isAuthenticated: true, sessionNonce: nonce, peerIdentityKey: key, lastUpdate })

test('the BRC-104 session store is bounded: a cap that forgets the least recently used, and an idle life', () => {
  let now = 1_000
  const evicted: string[] = []
  const s = new BoundedSessions(2, 60_000, () => now, (why) => evicted.push(why))
  s.addSession(session('n1', 'k1'))
  s.addSession(session('n2', 'k2'))
  assert.equal(s.getSession('n1')?.sessionNonce, 'n1')
  s.addSession(session('n3', 'k3'))
  assert.equal(s.size, 2)
  assert.equal(s.hasSession('n2'), false, 'the least recently used went at the cap')
  assert.deepEqual(evicted, ['cap'])
  now += 60_000
  assert.equal(s.getSession('n1'), undefined, 'idle for the whole life: forgotten')
  assert.equal(s.size, 0)
  assert.deepEqual(evicted, ['cap', 'idle', 'idle'])
})

test('the terms route keeps at most its bound of sessions; a client whose session went shakes hands again', async () => {
  const r = await rig('history=5', { max: 2, ttlSeconds: 600 }, { perSec: 100, burst: 100, perAddressPerSec: 100, addressBurst: 100 })
  const ask = { service: 'ls_bstore', query: free(r) }
  const clients = [0, 1, 2].map(() => new AuthFetch(new ProtoWallet(PrivateKey.fromRandom()) as unknown as WalletInterface))
  for (const c of clients) assert.equal((await c.fetch(`${r.url}/lookup`, post(ask))).status, 200)
  assert.equal(r.front.sessions.size, 2, 'three handshakes, two sessions kept')
  assert.equal(r.host.count('bstore_sessions_evicted_total', { why: 'cap' }), 1)
  assert.equal(r.host.gauges.get('bstore_sessions')?.(), 2)
  const res = await quietly(async () => await clients[0]!.fetch(`${r.url}/lookup`, post(ask)))
  assert.equal(res.status, 200)
  assert.equal(r.front.sessions.size, 2)
})

/** One BRC-104 initial request, raw, from a fresh identity, sent from localAddress. */
async function shake(url: string, localAddress: string): Promise<{ status: number; retryAfter?: string }> {
  const body = JSON.stringify({
    version: '0.1',
    messageType: 'initialRequest',
    identityKey: PrivateKey.fromRandom().toPublicKey().toString(),
    initialNonce: Utils.toBase64(Random(32)),
    requestedCertificates: { certifiers: [], types: {} },
  })
  return await new Promise((resolve, reject) => {
    const req = request(`${url}/.well-known/auth`, { method: 'POST', localAddress, headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, (res) => {
      res.resume()
      res.on('end', () => resolve({ status: res.statusCode ?? 0, retryAfter: res.headers['retry-after'] as string | undefined }))
    })
    req.on('error', reject)
    req.end(body)
  })
}

test('a handshake flood is refused 429 before any signature, by address and then for the route; the defaults are 4/s burst 8 and 1/s burst 4 an address', async () => {
  assert.deepEqual(DefaultBudget, { perSec: 4, burst: 8, perAddressPerSec: 1, addressBurst: 4 })
  const r = await rig('history=5', undefined, { perSec: 0.01, burst: 8, perAddressPerSec: 0.01, addressBurst: 4 })
  const flood = await Promise.all(Array.from({ length: 50 }, () => shake(r.url, '127.0.0.2')))
  const ok = flood.filter((x) => x.status === 200).length
  const limited = flood.filter((x) => x.status === 429)
  assert.equal(ok, 4, 'the address burst')
  assert.equal(limited.length, 46)
  assert.ok(limited.every((x) => Number(x.retryAfter) >= 1), 'each refusal says when to come back')
  assert.equal(r.signatures(), 4, 'one signature per answered handshake, none for a refused one')
  assert.equal(r.host.count('bstore_handshakes_total', { result: 'limited_address' }), 46)
  const others = await Promise.all(['127.0.0.3', '127.0.0.4', '127.0.0.5', '127.0.0.6', '127.0.0.7'].map((a) => shake(r.url, a)))
  assert.deepEqual(others.map((x) => x.status).sort(), [200, 200, 200, 200, 429], 'the route burst of 8, four already spent')
  assert.equal(r.host.count('bstore_handshakes_total', { result: 'limited_global' }), 1)
})

test('a malformed handshake inside the budget is counted failed and costs no signature', async () => {
  const r = await rig('history=5')
  const bad = await fetch(`${r.url}/.well-known/auth`, post({ messageType: 'general' }))
  assert.equal(bad.status, 400)
  assert.equal(r.host.count('bstore_handshakes_total', { result: 'failed' }), 1)
  assert.equal(r.signatures(), 0)
})

/** A server in front of the route that keeps each request as it arrived, to send it again. */
async function recording(r: Rig): Promise<{ url: string; port: number; requests: Array<{ method: string; url: string; headers: IncomingHttpHeaders; body: Buffer[] }> }> {
  const requests: Array<{ method: string; url: string; headers: IncomingHttpHeaders; body: Buffer[] }> = []
  const server = createServer((req, res) => {
    const body: Buffer[] = []
    requests.push({ method: req.method ?? 'GET', url: req.url ?? '/', headers: { ...req.headers }, body })
    req.on('data', (c: Buffer) => body.push(c))
    r.front.handler(req, res)
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  servers.push(server)
  const port = (server.address() as AddressInfo).port
  return { url: `http://127.0.0.1:${port}`, port, requests }
}

test('a signed request sent again byte for byte is refused 401 before any signature', async () => {
  const r = await rig('history=5')
  const rec = await recording(r)
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  assert.equal((await af.fetch(`${rec.url}/lookup`, post({ service: 'ls_bstore', query: free(r) }))).status, 200)
  const sent = rec.requests.find((x) => x.url === '/lookup' && x.headers['x-bsv-auth-request-id'] !== undefined)!
  const before = r.signatures()
  const replay = async (): Promise<number> =>
    await new Promise((resolve, reject) => {
      const q = request({ host: '127.0.0.1', port: rec.port, path: sent.url, method: sent.method, headers: sent.headers }, (res) => {
        res.resume()
        res.on('end', () => resolve(res.statusCode ?? 0))
      })
      q.on('error', reject)
      q.end(Buffer.concat(sent.body))
    })
  const statuses = new Set<number>()
  for (let i = 0; i < 30; i++) statuses.add(await replay())
  for (const s of await Promise.all(Array.from({ length: 20 }, replay))) statuses.add(s)
  assert.deepEqual([...statuses], [401])
  assert.equal(r.signatures(), before, 'no signature for a replay')
  assert.equal(r.host.count('bstore_requests_total', { result: 'replayed' }), 50)
  assert.equal((await af.fetch(`${rec.url}/lookup`, post({ service: 'ls_bstore', query: free(r) }))).status, 200)
})

test('a session has a budget of signed responses: over it a request is refused 429 and nothing is signed', async () => {
  const r = await rig('history=5', undefined, undefined, { perSec: 0.5, burst: 3 })
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  const ask = async (): Promise<number> => {
    try {
      return (await quietly(async () => await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: free(r) })))).status
    } catch {
      return 429
    }
  }
  const statuses: number[] = []
  for (let i = 0; i < 6; i++) statuses.push(await ask())
  assert.deepEqual(statuses, [200, 200, 200, 429, 429, 429])
  assert.equal(r.signatures(), 4, 'the handshake and three responses')
  assert.equal(r.host.count('bstore_requests_total', { result: 'limited' }), 3)
})

test('a forged request on a live session is charged to the session before any verification: past its budget nothing is verified', async () => {
  const r = await rig('history=5', undefined, undefined, { perSec: 0.001, burst: 2 })
  const rec = await recording(r)
  let verifies = 0
  const w = (r.front as unknown as { peer: { wallet: ProtoWallet } }).peer.wallet
  const verify = w.verifySignature.bind(w)
  w.verifySignature = async (args) => {
    verifies++
    return await verify(args)
  }
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  assert.equal((await af.fetch(`${rec.url}/lookup`, post({ service: 'ls_bstore', query: free(r) }))).status, 200)
  const sent = rec.requests.find((x) => x.url === '/lookup' && x.headers['x-bsv-auth-request-id'] !== undefined)!
  const before = verifies
  const statuses: number[] = []
  for (let i = 0; i < 20; i++) {
    const headers = Object.fromEntries(Object.entries(sent.headers).filter(([k]) => k.startsWith('x-bsv') || k === 'content-type')) as Record<string, string>
    headers['x-bsv-auth-request-id'] = Utils.toBase64(Random(32))
    statuses.push((await fetch(`${rec.url}/lookup`, { method: 'POST', headers, body: JSON.stringify({ service: 'ls_bstore', query: free(r) }) })).status)
  }
  assert.deepEqual([statuses[0], statuses.slice(1).every((s) => s === 429)], [401, true], 'the one left in the budget is verified and refused; the rest are not verified')
  assert.equal(verifies - before, 1)
})
