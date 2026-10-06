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
import { ArcadeHttp, BroadcastRefused, PaymentGate, isFinal, overspends } from './accept.js'
import { TestNetwork } from './testnetwork.js'
import { defaultAcceptancePolicy, type AcceptancePolicy } from '@lightwebinc/bcommon'
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
  net: TestNetwork
  gate: PaymentGate
  /** Signatures the payee's wallet made. */
  signatures: () => number
}

const servers: Server[] = []
after(() => {
  for (const s of servers) s.close()
})

/** A module serving one chain of three tokens, the second anchoring three entries, with outcomes, its engine, and its terms route on a free port. */
async function rig(prices: string, sessions?: { max: number; ttlSeconds: number }, budget?: BudgetConfig, responses?: ResponseBudgetConfig, policy: Partial<AcceptancePolicy> = {}, networked = true): Promise<Rig> {
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
  const net = new TestNetwork(chain)
  const gate = new PaymentGate({ app: 'bstore', host, headers: chain.tracker, policy: { ...defaultAcceptancePolicy(), ...policy }, arcade: networked ? net : undefined, node: networked ? net : undefined, waitMs: 200, pollMs: 10 })
  const front = new LookupFront({ ls: m.ls, host, prices: parsePrices(prices), wallet, receiver, headers: chain.tracker, sessions, budget, responses, gate })
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
    net,
    gate,
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
  assert.equal(r.host.count('bstore_payments_total', { decision: 'requested', reason: 'no-payment' }), 5)
  assert.equal(r.host.count('bstore_payments_total', { decision: 'fast', reason: 'at-or-below-threshold' }), 5)
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
  assert.equal(r.host.count('bstore_payments_total', { decision: 'refuse', reason: 'conflict' }), 3)
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

/** The priced question the acceptance tests ask. */
const paidQuery = (r: Rig, _who: Party): Record<string, string> => historyQ(r)

/** Asks one priced question with wallet, paying as AuthFetch does; a held answer (402, no BRC-105 headers) throws in the SDK and is status 0. */
async function asker(r: Rig, who: Party = r.client, wallet: PayingWallet = r.wallet): Promise<() => Promise<number>> {
  const af = new AuthFetch(wallet as unknown as WalletInterface)
  return async () => {
    try {
      return (await quietly(async () => await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: paidQuery(r, who) })))).status
    } catch {
      return 0
    }
  }
}

const last = (r: Rig): ReceivedPayment => r.receiver.payments[r.receiver.payments.length - 1]!

test('acceptance: a small paid lookup is broadcast by the host, then answered; the watch releases it once mined', async () => {
  const r = await rig('history=5')
  const ask = await asker(r)
  assert.equal(await ask(), 200)
  const p = last(r)
  assert.deepEqual(r.net.sent, [p.txid], 'broadcast before the answer')
  assert.equal(p.decision, 'fast')
  assert.equal(r.host.count('bstore_payments_total', { decision: 'fast', reason: 'at-or-below-threshold' }), 1)
  assert.deepEqual(r.gate.watching, [p.txid])
  assert.equal(r.gate.exposure.unmined(r.client.hex).payer, 5)
  assert.deepEqual(await r.gate.sweep(), [], 'nothing to report while unmined')
  r.net.mine(p.txid)
  assert.deepEqual((await r.gate.sweep()).map((e) => e.kind), ['confirmed'])
  assert.deepEqual(r.gate.watching, [])
  assert.equal(r.gate.exposure.unmined(r.client.hex).total, 0)
  assert.equal(r.host.count('bstore_payment_events_total', { kind: 'confirmed' }), 1)
})

test('acceptance: a payment above the threshold is broadcast and held 402 until it mines, then the same payment is answered', async () => {
  const r = await rig('history=5', undefined, undefined, undefined, { thresholdSats: 4 })
  const ask = await asker(r)
  assert.equal(await ask(), 0, 'held: 402 without BRC-105 headers, so the client pays nothing more')
  const p = last(r)
  assert.equal(p.decision, 'hold')
  assert.equal(p.reason, 'above-threshold')
  // AuthFetch sends the request again after a 402 it cannot pay; a held payment offered again is held again, once in the ledger.
  assert.deepEqual([...new Set(r.net.sent)], [p.txid], 'a held payment is broadcast too')
  assert.equal(new Set(r.receiver.payments.map((x) => x.txid)).size, 1)
  assert.ok(r.host.count('bstore_payments_total', { decision: 'hold', reason: 'above-threshold' }) >= 1)
  const header = JSON.stringify({ derivationPrefix: p.derivationPrefix, derivationSuffix: p.derivationSuffix, transaction: Utils.toBase64(Uint8Array.from(p.beef)) })
  const af = new AuthFetch(r.wallet as unknown as WalletInterface)
  const again = async (): Promise<number> => {
    try {
      return (await af.fetch(`${r.url}/lookup`, post({ service: 'ls_bstore', query: paidQuery(r, r.client) }, { 'x-bsv-payment': header }))).status
    } catch {
      return 0
    }
  }
  assert.equal(await again(), 0, 'still held before it mines')
  r.net.mine(p.txid)
  assert.equal(await again(), 200)
  assert.equal(last(r).decision, 'mined')
  assert.equal(await again(), 409, 'and answered once')
})

test('acceptance: arcade answers DOUBLE_SPEND_ATTEMPTED: held, and nothing charged', async () => {
  const r = await rig('history=5')
  r.net.mode = 'double-spend'
  assert.equal(await (await asker(r))(), 0)
  assert.equal(last(r).reason, 'double-spend-attempted')
  assert.ok(r.host.count('bstore_payments_total', { decision: 'hold', reason: 'double-spend-attempted' }) >= 1)
  assert.equal(r.gate.exposure.unmined(r.client.hex).total, 0)
})

test('acceptance: a refusal, an unknown spend view, a silent network and a broadcast that fails', async () => {
  const r = await rig('history=5')
  const ask = await asker(r)
  r.net.mode = 'reject'
  assert.equal(await ask(), 400)
  assert.equal(r.host.count('bstore_payments_total', { decision: 'refuse', reason: 'network-refused' }), 1)
  r.net.mode = 'refuse-http'
  assert.equal(await ask(), 400)
  assert.equal(r.host.count('bstore_payments_total', { decision: 'refuse', reason: 'network-refused' }), 2)
  r.net.mode = 'silent'
  assert.equal(await ask(), 0)
  assert.equal(last(r).reason, 'no-network-verdict')
  r.net.mode = 'down'
  assert.equal(await ask(), 0)
  assert.equal(last(r).reason, 'broadcast-unconfirmed')
  r.net.mode = 'accept'
  r.net.spendUnknown = true
  assert.equal(await ask(), 0)
  assert.equal(last(r).reason, 'spend-view-unknown')
  assert.equal(r.gate.exposure.unmined(r.client.hex).total, 0, 'every demotion released its charge')
})

test('acceptance: a host with no arcade or no node holds every payment', async () => {
  const r = await rig('history=5', undefined, undefined, undefined, {}, false)
  assert.equal(await (await asker(r))(), 0)
  assert.equal(last(r).reason, 'no-broadcast-leg')
  assert.deepEqual(r.net.sent, [])
})

test('acceptance: a payer whose fast payment was double-spent is flagged; its later payments are held, others are not', async () => {
  const r = await rig('history=5')
  const ask = await asker(r)
  assert.equal(await ask(), 200)
  const p = last(r)
  r.net.spendElsewhere(p.inputs[0]!)
  const events = await r.gate.sweep()
  assert.deepEqual(events.map((e) => [e.kind, e.payer]), [['double-spent', r.client.hex]])
  assert.ok(r.host.lines.some((l) => l.msg.includes('fast payment lost')))
  assert.equal(r.host.count('bstore_payment_events_total', { kind: 'double-spent' }), 1)
  assert.equal(await ask(), 0)
  assert.equal(last(r).reason, 'payer-flagged')
  const bob = new Party('bob')
  assert.equal(await (await asker(r, bob, new PayingWallet(bob.key, r.chain)))(), 200, 'another payer is still fast')
})

test('acceptance: fast payments are bounded per payer and in total until they mine', async () => {
  const r = await rig('history=5', undefined, undefined, undefined, { payerLimit: 10, totalLimit: 15 })
  const ask = await asker(r)
  assert.equal(await ask(), 200)
  assert.equal(await ask(), 200)
  assert.equal(await ask(), 0)
  assert.equal(last(r).reason, 'payer-limit')
  const bob = new Party('bob')
  const bobAsk = await asker(r, bob, new PayingWallet(bob.key, r.chain))
  assert.equal(await bobAsk(), 200)
  assert.equal(await bobAsk(), 0)
  assert.equal(last(r).reason, 'total-limit')
  r.net.mine(r.receiver.payments[0]!.txid)
  await r.gate.sweep()
  assert.equal(await bobAsk(), 200, 'a mined payment leaves the sum')
})

test('acceptance: finality and conservation', async () => {
  const chain = new Chain(97000)
  const w = new PayingWallet(PrivateKey.fromRandom(), chain)
  const tx = await w.pay([{ satoshis: 5, lockingScript: new P2PKH().lock(payee.toAddress()).toHex() }])
  assert.equal(isFinal(tx), true)
  assert.equal(overspends(tx), undefined)
  tx.lockTime = 500
  assert.equal(isFinal(tx), true, 'every input final')
  tx.inputs[0]!.sequence = 1
  assert.equal(isFinal(tx), false)
  tx.outputs[0]!.satoshis = 200000
  assert.match(overspends(tx)!, /outputs .* inputs/)
  tx.inputs[0]!.sourceTransaction = undefined
  assert.match(overspends(tx)!, /no source/)
})

test('acceptance: held and fast lines survive a restart; a lost payment is written to unsettleable.jsonl', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bstore-accept-'))
  try {
    const chain = new Chain(98000)
    const w = new PayingWallet(PrivateKey.fromRandom(), chain)
    const tx = await w.pay([{ satoshis: 5, lockingScript: new P2PKH().lock(payee.toAddress()).toHex() }])
    const base: ReceivedPayment = { txid: tx.id('hex'), beef: tx.toAtomicBEEF(), outputIndex: 0, satoshis: 5, derivationPrefix: 'AA==', derivationSuffix: 'AQ==', senderIdentityKey: payeeKey, class: 'history', inputs: spentOutpoints(tx) }
    const a = new LedgerReceiver(dir)
    assert.equal(a.claim({ ...base, decision: 'hold' }), 'accepted')
    assert.equal(a.claim({ ...base, decision: 'hold' }), 'accepted', 'held again, not written twice')
    a.close()
    const b = new LedgerReceiver(dir)
    assert.equal(b.refusal(base), undefined, 'a held payment can still be answered')
    assert.equal(b.claim({ ...base, decision: 'mined' }), 'accepted')
    assert.equal(b.claim({ ...base, decision: 'mined' }), 'replayed')
    const fast = await w.pay([{ satoshis: 5, lockingScript: new P2PKH().lock(payee.toAddress()).toHex() }])
    assert.equal(b.claim({ ...base, txid: fast.id('hex'), beef: fast.toAtomicBEEF(), inputs: spentOutpoints(fast), decision: 'fast' }), 'accepted')
    b.close()
    const c = new LedgerReceiver(dir)
    assert.equal(c.fast.length, 1)
    c.close()
    const host = new FakeHost()
    const net = new TestNetwork(chain)
    const gate = new PaymentGate({ app: 'bstore', host, headers: chain.tracker, policy: defaultAcceptancePolicy(), arcade: net, node: net, stateDir: dir })
    const f = c.fast[0]!
    gate.restore(Transaction.fromAtomicBEEF(Utils.toArray(f.beef, 'base64')), f.senderIdentityKey, f.satoshis, f.at)
    assert.deepEqual(gate.watching, [fast.id('hex')])
    assert.equal(gate.exposure.unmined(payeeKey).payer, 5)
    net.spendElsewhere(spentOutpoints(fast)[0]!)
    await gate.sweep()
    const lines = readFileSync(join(dir, 'unsettleable.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l) as { kind: string; txid: string })
    assert.deepEqual(lines.map((l) => [l.kind, l.txid]), [['double-spent', fast.id('hex')]])
    assert.equal(gate.exposure.isFlagged(payeeKey), true)
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('acceptance: flags outlive a restart, restored lines are watched a day and charged an hour, a partial network still broadcasts, the watch is bounded', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bstore-accept-'))
  try {
    const chain = new Chain(99500)
    const w = new PayingWallet(PrivateKey.fromRandom(), chain)
    const lock = new P2PKH().lock(payee.toAddress()).toHex()
    writeFileSync(join(dir, 'unsettleable.jsonl'), JSON.stringify({ kind: 'double-spent', txid: 'ab'.repeat(32), payer: '02cd', sats: 5 }) + '\n')
    const host = new FakeHost()
    const net = new TestNetwork(chain)
    const gate = new PaymentGate({ app: 'bstore', host, headers: chain.tracker, policy: defaultAcceptancePolicy(), arcade: net, node: net, stateDir: dir, maxWatched: 2, waitMs: 200, pollMs: 10 })
    assert.equal(gate.exposure.isFlagged('02cd'), true, 'flagged again from unsettleable.jsonl')
    const old = await w.pay([{ satoshis: 5, lockingScript: lock }])
    gate.restore(old, '02ef', 5, Date.now() - 2 * 3_600_000)
    assert.deepEqual(gate.watching, [old.id('hex')], 'watched: under a day old')
    assert.equal(gate.exposure.unmined('02ef').payer, 0, 'not charged: past the window')
    const ancient = await w.pay([{ satoshis: 5, lockingScript: lock }])
    gate.restore(ancient, '02ef', 5, Date.now() - 2 * 86_400_000)
    assert.equal(gate.watching.length, 1, 'over a day old: not watched')
    assert.equal((await gate.accept(await w.pay([{ satoshis: 5, lockingScript: lock }]), '02aa', 5)).decision, 'fast')
    assert.deepEqual(await gate.accept(await w.pay([{ satoshis: 5, lockingScript: lock }]), '02aa', 5), { decision: 'hold', reason: 'watch-limit', detail: 'too many fast payments are waiting to mine' })
    // arcade alone: broadcast, and held, since no node can show it mined.
    const half = new PaymentGate({ app: 'bstore', host: new FakeHost(), headers: chain.tracker, policy: defaultAcceptancePolicy(), arcade: net })
    const p = await w.pay([{ satoshis: 5, lockingScript: lock }])
    assert.equal((await half.accept(p, '02aa', 5)).reason, 'no-broadcast-leg')
    assert.ok(net.sent.includes(p.id('hex')), 'broadcast even so')
    net.mode = 'reject'
    assert.equal((await half.accept(await w.pay([{ satoshis: 5, lockingScript: lock }]), '02aa', 5)).decision, 'refuse')
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('acceptance: arcade over HTTP: a refusal code refuses, "already known" is no verdict, "already spent" is never accepted', async () => {
  const replies: Array<[number, string]> = [
    [465, '{"title":"input already spent"}'],
    [409, '{"title":"txn-already-known"}'],
    [400, '{"title":"missing inputs or already spent"}'],
    [200, '{"txid":"x","txStatus":"SEEN_ON_NETWORK"}'],
  ]
  const server = createServer((req, res) => {
    req.resume()
    const [status, body] = replies.shift()!
    res.writeHead(status, { 'content-type': 'application/json' })
    res.end(body)
  })
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  servers.push(server)
  const a = new ArcadeHttp(`http://127.0.0.1:${(server.address() as AddressInfo).port}`)
  await assert.rejects(a.submit(Uint8Array.of(1)), BroadcastRefused)
  assert.deepEqual(await a.submit(Uint8Array.of(1)), { txStatus: 'RECEIVED', extraInfo: 'already known' })
  await assert.rejects(a.submit(Uint8Array.of(1)), (e: unknown) => !(e instanceof BroadcastRefused))
  assert.equal((await a.submit(Uint8Array.of(1))).txStatus, 'SEEN_ON_NETWORK')
})
