/**
 * Payment acceptance for the host's priced questions: the value
 * discriminator of bcommon's acceptance package (decidePayment,
 * arcadeVerdict, PaymentExposure) with the network evidence the host
 * gathers itself.
 *
 * A payment that passed the route's own checks (prefix, output 0 pays the
 * derived key, SPV against the host's headers) is then:
 *
 *   1. held to finality (lock time 0 or every input final) and conservation
 *      (every input carries its source, outputs do not exceed inputs);
 *   2. decided: at or below the threshold and within the payer and total
 *      limits is fast, above or over a limit is held, a flagged payer held;
 *   3. broadcast by the host through arcade, unmined ancestors first;
 *   4. on the fast path, answered only once arcade says the network took it
 *      (arcadeVerdict 'accepted'), with no conflict, and the node's spend
 *      view shows every input unspent or spent by this payment.
 *
 * A held payment is answered once the node serves a proof of it that
 * verifies against the host's headers. A host with no arcade or no node
 * holds every payment. Each fast payment is watched until it mines; one
 * that is double-spent, refused or never mines flags its payer, whose later
 * payments are then held, and is written to unsettleable.jsonl in the state
 * directory and logged.
 */
import { appendFileSync, mkdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { MerklePath, Transaction, type ChainTracker } from '@bsv/sdk'
import {
  PaymentExposure,
  arcadeVerdict,
  decidePayment,
  defaultAcceptancePolicy,
  type AcceptancePolicy,
  type ModuleHost,
} from '@lightwebinc/bcommon'

/** What the route does with a payment. */
export type Decision = 'fast' | 'hold' | 'refuse'

export interface GateVerdict {
  decision: Decision
  /** A fixed label, the same as the Go package's where one exists. */
  reason: string
  detail?: string
}

/** An ARC status, as arcade answers POST /tx and GET /tx/{txid}. */
export interface ArcadeStatus {
  txid?: string
  txStatus?: string
  extraInfo?: string
  competingTxs?: string[] | null
}

/** The host's broadcast leg. */
export interface Broadcaster {
  /** POST /tx; throws BroadcastRefused on a definitive refusal, any other error when the answer is unknown. */
  submit(ef: Uint8Array): Promise<ArcadeStatus>
  /** GET /tx/{txid}; undefined when arcade does not hold it. */
  status(txid: string): Promise<ArcadeStatus | undefined>
}

/** The node's view: who spent an output, and a transaction's proof. */
export interface NodeView {
  /** '' when the node holds the output unspent, the spender's txid when spent; throws on anything else. */
  spender(txid: string, vout: number): Promise<string>
  /** The proof of txid, or undefined while it is not mined. */
  proof(txid: string): Promise<MerklePath | undefined>
}

/** A refusal the network will not take back. */
export class BroadcastRefused extends Error {}

const timeout = 10_000

/** arcade over HTTP. */
export class ArcadeHttp implements Broadcaster {
  constructor(private readonly base: string) {}

  private url(p: string): string {
    return `${this.base.replace(/\/+$/, '')}${p}`
  }

  async submit(ef: Uint8Array): Promise<ArcadeStatus> {
    for (let attempt = 0; ; attempt++) {
      const res = await fetch(this.url('/tx'), { method: 'POST', headers: { 'content-type': 'application/octet-stream' }, body: Buffer.from(ef), signal: AbortSignal.timeout(timeout) })
      const body = await res.text()
      if (res.status === 200 || res.status === 202) return JSON.parse(body) as ArcadeStatus
      if (res.status === 503 && attempt < 3) {
        const s = Number(res.headers.get('retry-after'))
        await sleep(s > 0 && s <= 10 ? s * 1000 : 1000)
        continue
      }
      const why = `arcade answered ${res.status}: ${body.slice(0, 300)}`
      if (res.status === 422 || (res.status >= 460 && res.status <= 475)) throw new BroadcastRefused(why)
      // Known already is not a verdict: the status is read again (GET /tx), never assumed.
      if (/already (known|in (the )?mempool)|txn-already-known/i.test(body) && !/spent/i.test(body)) return { txStatus: 'RECEIVED', extraInfo: 'already known' }
      throw new Error(why)
    }
  }

  async status(txid: string): Promise<ArcadeStatus | undefined> {
    const res = await fetch(this.url(`/tx/${txid}`), { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(timeout) })
    if (res.status === 404) return undefined
    if (res.status !== 200) throw new Error(`arcade status ${txid}: ${res.status}`)
    return (await res.json()) as ArcadeStatus
  }
}

const isTxid = (s: unknown): s is string => typeof s === 'string' && /^[0-9a-fA-F]{64}$/.test(s)

/** The node's asset service over HTTP: /api/v1/utxos and /api/v1/merkle_proof. */
export class AssetHttp implements NodeView {
  constructor(private readonly base: string) {}

  private url(p: string): string {
    return `${this.base.replace(/\/+$/, '')}${p}`
  }

  async spender(txid: string, vout: number): Promise<string> {
    const res = await fetch(this.url(`/api/v1/utxos/${txid}/json`), { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(timeout) })
    if (res.status !== 200) throw new Error(`the node answers ${res.status} for ${txid}`)
    const outs = (await res.json()) as Array<{ txid?: string; vout?: number; status?: string; spendingData?: { txId?: string } } | null>
    if (!Array.isArray(outs)) throw new Error('the node answers no list')
    let found: string | undefined
    for (const o of outs) {
      if (o === null || typeof o.vout !== 'number') throw new Error('an entry names no output')
      if (o.vout !== vout) continue
      if (found !== undefined) throw new Error('the node names the output twice')
      if (o.txid !== undefined && o.txid !== '' && o.txid.toLowerCase() !== txid.toLowerCase()) throw new Error(`the answer is for ${o.txid}`)
      const st = (o.status ?? '').toUpperCase()
      if (st === 'OK') found = ''
      else if (st === 'SPENT' && isTxid(o.spendingData?.txId)) found = o.spendingData!.txId!.toLowerCase()
      else throw new Error(`the node answers status "${o.status}"`)
    }
    if (found === undefined) throw new Error(`the answer has no output ${vout}`)
    return found
  }

  async proof(txid: string): Promise<MerklePath | undefined> {
    const res = await fetch(this.url(`/api/v1/merkle_proof/${txid}`), { signal: AbortSignal.timeout(timeout) })
    if (res.status === 404) return undefined
    if (res.status !== 200) throw new Error(`merkle_proof ${txid}: ${res.status}`)
    const raw = new Uint8Array(await res.arrayBuffer())
    if (raw.length > 1 << 20) throw new Error('merkle_proof: too large')
    const mp = MerklePath.fromBinary(Array.from(raw))
    if (!(mp.path[0] ?? []).some((l) => l.hash === txid)) throw new Error('the proof does not contain the txid')
    return mp
  }
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms).unref?.())

export interface GateOptions {
  /** The metric prefix: `<app>`. */
  app: string
  host: ModuleHost
  headers: ChainTracker
  policy: AcceptancePolicy
  arcade?: Broadcaster
  node?: NodeView
  /** Milliseconds the fast path waits for the network's word; default 10 s. */
  waitMs?: number
  /** Milliseconds it keeps watching for a conflict before answering; default 0. */
  watchMs?: number
  pollMs?: number
  /** How long a fast payment may stay unmined before it is reported; default a day. */
  maxAgeMs?: number
  /** The most fast payments watched at once; past it a payment is held. Default 100000. */
  maxWatched?: number
  /** The most watched payments one sweep checks, oldest checked first; default 500, 8 at a time. */
  sweepBatch?: number
  /** Where unsettleable.jsonl is written; none in tests. */
  stateDir?: string
  now?: () => number
}

/** The labels the route counts payments under, preset so a dashboard sees zeros. */
export const PaymentLabels: Array<[Decision | 'requested', string]> = [
  ['requested', 'no-payment'],
  ['fast', 'at-or-below-threshold'],
  ['fast', 'mined'],
  ['hold', 'above-threshold'],
  ['hold', 'payer-limit'],
  ['hold', 'total-limit'],
  ['hold', 'payer-flagged'],
  ['hold', 'no-broadcast-leg'],
  ['hold', 'broadcast-unconfirmed'],
  ['hold', 'no-network-verdict'],
  ['hold', 'double-spend-attempted'],
  ['hold', 'spend-view-unknown'],
  ['hold', 'already-charged'],
  ['hold', 'payer-asked-hold'],
  ['hold', 'price-unknown'],
  ['hold', 'price-stale'],
  ['hold', 'no-txid'],
  ['hold', 'watch-limit'],
  ['refuse', 'pays-nothing'],
  ['refuse', 'malformed'],
  ['refuse', 'wrong-script'],
  ['refuse', 'spv-failed'],
  ['refuse', 'not-final'],
  ['refuse', 'outputs-exceed-inputs'],
  ['refuse', 'network-refused'],
  ['refuse', 'double-spent'],
  ['refuse', 'replayed'],
  ['refuse', 'conflict'],
]

export const EventKinds = ['confirmed', 'double-spent', 'refused', 'unmined'] as const
export type EventKind = (typeof EventKinds)[number]

export interface GateEvent {
  kind: EventKind
  txid: string
  payer: string
  sats: number
  why?: string
}

interface Watched {
  tx: Transaction
  payer: string
  sats: number
  since: number
}

/** True when tx cannot be replaced before it mines. */
export function isFinal(tx: Transaction): boolean {
  return tx.lockTime === 0 || tx.inputs.every((i) => (i.sequence ?? 0xffffffff) === 0xffffffff)
}

/** The outputs' total over the inputs', or why it cannot be read. */
export function overspends(tx: Transaction): string | undefined {
  let inSats = 0
  for (const [n, i] of tx.inputs.entries()) {
    const src = i.sourceTransaction?.outputs[i.sourceOutputIndex]
    if (src?.satoshis === undefined) return `input ${n} carries no source transaction`
    inSats += src.satoshis
  }
  const outSats = tx.outputs.reduce((a, o) => a + (o.satoshis ?? 0), 0)
  return outSats > inSats ? `outputs ${outSats} sat, inputs ${inSats} sat` : undefined
}

/** The unmined ancestors of tx, parents before children. */
function unminedAncestors(tx: Transaction): Transaction[] {
  const out: Transaction[] = []
  const seen = new Set<Transaction>()
  const walk = (t: Transaction): void => {
    for (const i of t.inputs) {
      const s = i.sourceTransaction
      if (s === undefined || s.merklePath !== undefined || seen.has(s)) continue
      seen.add(s)
      walk(s)
      out.push(s)
    }
  }
  walk(tx)
  return out
}

/** The fast path, the hold, and the watch over what the fast path took. */
export class PaymentGate {
  readonly exposure = new PaymentExposure()
  private readonly watched = new Map<string, Watched>()
  private readonly now: () => number

  constructor(private readonly o: GateOptions) {
    this.now = o.now ?? Date.now
    for (const [decision, reason] of PaymentLabels) o.host.metrics.preset(`${o.app}_payments_total`, { decision, reason })
    for (const kind of EventKinds) o.host.metrics.preset(`${o.app}_payment_events_total`, { kind })
    o.host.metrics.gauge(`${o.app}_payments_unmined`, () => this.watched.size)
    this.reflag()
  }

  /** Flags again every payer unsettleable.jsonl names: a flag outlives a restart. */
  private reflag(): void {
    if (this.o.stateDir === undefined) return
    let text = ''
    try {
      text = readFileSync(join(this.o.stateDir, 'unsettleable.jsonl'), 'utf8')
    } catch {
      return
    }
    for (const l of text.split('\n')) {
      try {
        const v = JSON.parse(l) as { kind?: unknown; txid?: unknown; payer?: unknown }
        if (typeof v.payer === 'string' && typeof v.kind === 'string' && typeof v.txid === 'string') this.exposure.flag(v.payer, `${v.kind}: ${v.txid}`)
      } catch {
        // a line cut short
      }
    }
  }

  /** Releases and stops watching a payment the route did not take after all. */
  forget(txid: string): void {
    this.exposure.release(txid)
    this.watched.delete(txid)
  }

  /** True when the host can take a payment on the fast path at all. */
  get networked(): boolean {
    return this.o.arcade !== undefined && this.o.node !== undefined
  }

  count(decision: Decision | 'requested', reason: string): void {
    this.o.host.metrics.inc(`${this.o.app}_payments_total`, { decision, reason })
  }

  /** Decides tx, which pays sats and passed the route's own checks, for payer. */
  async accept(tx: Transaction, payer: string, sats: number): Promise<GateVerdict> {
    const v = await this.decide(tx, payer, sats)
    this.count(v.decision, v.reason)
    return v
  }

  private async decide(tx: Transaction, payer: string, sats: number): Promise<GateVerdict> {
    const txid = tx.id('hex')
    if (tx.merklePath !== undefined) return { decision: 'fast', reason: 'mined' }
    if (!isFinal(tx)) return { decision: 'refuse', reason: 'not-final', detail: 'the payment can be replaced before it mines' }
    const over = overspends(tx)
    if (over !== undefined) return { decision: 'refuse', reason: 'outputs-exceed-inputs', detail: over }
    const { arcade, node } = this.o
    if (arcade === undefined || node === undefined) {
      // Not fast without both; still broadcast and still answer a mined one where it can.
      if (arcade !== undefined) {
        try {
          for (const a of unminedAncestors(tx)) await arcade.submit(Uint8Array.from(a.toEF())).catch(() => undefined)
          const st = await arcade.submit(Uint8Array.from(tx.toEF()))
          if (arcadeVerdict(st) === 'refused') return { decision: 'refuse', reason: 'network-refused', detail: `${st.txStatus} ${st.extraInfo ?? ''}`.trim() }
        } catch (e) {
          if (e instanceof BroadcastRefused) return { decision: 'refuse', reason: 'network-refused', detail: e.message }
        }
      }
      if (await this.mined(tx)) return { decision: 'fast', reason: 'mined' }
      return { decision: 'hold', reason: 'no-broadcast-leg', detail: 'this host has no arcade or no node to take a payment fast' }
    }
    if (this.watched.size >= (this.o.maxWatched ?? 100_000) && !(await this.mined(tx))) return { decision: 'hold', reason: 'watch-limit', detail: 'too many fast payments are waiting to mine' }
    const d = await decidePayment(this.o.policy, this.exposure, { payer, txid, sats }, this.now())
    if (d.decision === 'refuse') return { decision: 'refuse', reason: d.reason }
    const fast = d.decision === 'fast'
    const settle = async (v: GateVerdict): Promise<GateVerdict> => {
      if (fast && v.decision !== 'fast') this.exposure.release(txid)
      if (v.decision === 'hold' && (await this.mined(tx))) return { decision: 'fast', reason: 'mined' }
      return v
    }
    // The host broadcasts, held or fast: this is how it collects.
    let st: ArcadeStatus
    try {
      for (const a of unminedAncestors(tx)) await arcade.submit(Uint8Array.from(a.toEF())).catch(() => undefined)
      st = await arcade.submit(Uint8Array.from(tx.toEF()))
    } catch (e) {
      if (e instanceof BroadcastRefused) return await settle({ decision: 'refuse', reason: 'network-refused', detail: e.message })
      return await settle({ decision: 'hold', reason: 'broadcast-unconfirmed', detail: String(e) })
    }
    if (st.txid !== undefined && st.txid !== '' && st.txid !== txid) return await settle({ decision: 'hold', reason: 'broadcast-unconfirmed', detail: `arcade acknowledged ${st.txid}` })
    const first = arcadeVerdict(st)
    if (first === 'refused') return await settle({ decision: 'refuse', reason: 'network-refused', detail: `${st.txStatus} ${st.extraInfo ?? ''}`.trim() })
    if (first === 'conflict') return await settle({ decision: 'hold', reason: 'double-spend-attempted', detail: (st.competingTxs ?? []).join(',') })
    if (!fast) return await settle({ decision: 'hold', reason: d.reason })
    const v = await this.evidence(tx, txid, first)
    if (v.decision !== 'fast') return await settle(v)
    this.watched.set(txid, { tx, payer, sats, since: this.now() })
    return { decision: 'fast', reason: 'at-or-below-threshold' }
  }

  /** arcade's verdict and the node's spend view, until both agree or the wait ends. */
  private async evidence(tx: Transaction, txid: string, first: ReturnType<typeof arcadeVerdict>): Promise<GateVerdict> {
    const wait = this.o.waitMs ?? 10_000
    const watch = this.o.watchMs ?? 0
    const poll = this.o.pollMs ?? 500
    const start = this.now()
    let verdict = first
    for (;;) {
      const elapsed = this.now() - start
      if (verdict === 'refused') return { decision: 'refuse', reason: 'network-refused' }
      if (verdict === 'conflict') return { decision: 'hold', reason: 'double-spend-attempted' }
      if (verdict === 'accepted') {
        const spends = await this.spends(tx, txid)
        if (spends !== undefined) return spends
        if (elapsed >= watch) return { decision: 'fast', reason: 'at-or-below-threshold' }
      }
      if (elapsed >= Math.max(wait, watch)) return { decision: 'hold', reason: 'no-network-verdict', detail: `arcade had not answered it accepted after ${wait} ms` }
      await sleep(poll)
      try {
        const st = await this.o.arcade!.status(txid)
        verdict = st === undefined ? 'pending' : arcadeVerdict(st)
      } catch {
        verdict = 'pending'
      }
    }
  }

  /** undefined when every input is unspent or spent by txid; a hold or a refusal otherwise. */
  private async spends(tx: Transaction, txid: string): Promise<GateVerdict | undefined> {
    for (const [n, i] of tx.inputs.entries()) {
      const src = i.sourceTXID ?? i.sourceTransaction?.id('hex')
      if (src === undefined) return { decision: 'hold', reason: 'spend-view-unknown', detail: `input ${n} names no source` }
      let by: string
      try {
        by = await this.o.node!.spender(src, i.sourceOutputIndex)
      } catch (e) {
        return { decision: 'hold', reason: 'spend-view-unknown', detail: String(e) }
      }
      if (by !== '' && by !== txid) return { decision: 'refuse', reason: 'double-spent', detail: `input ${n} (${src}.${i.sourceOutputIndex}) is spent by ${by}` }
    }
    return undefined
  }

  /** True when the node serves a proof of tx that verifies against the host's headers. */
  async mined(tx: Transaction): Promise<boolean> {
    if (this.o.node === undefined) return false
    const txid = tx.id('hex')
    try {
      const mp = await this.o.node.proof(txid)
      return mp !== undefined && (await mp.verify(txid, this.o.headers))
    } catch (e) {
      this.o.host.log(`${this.o.app} proof read failed`, { txid, err: String(e) })
      return false
    }
  }

  /** Watches a fast payment taken before a restart, charged as it was. */
  restore(tx: Transaction, payer: string, sats: number, at: number): void {
    const txid = tx.id('hex')
    const age = this.now() - at
    if (age > (this.o.maxAgeMs ?? 86_400_000)) return
    const window = this.o.policy.window
    if (window <= 0 || age <= window) this.exposure.charge(payer, txid, sats, at)
    this.watched.set(txid, { tx, payer, sats, since: at })
  }

  /** The txids being watched. */
  get watching(): string[] {
    return [...this.watched.keys()].sort()
  }

  /** One pass over the fast payments not yet mined. */
  async sweep(): Promise<GateEvent[]> {
    const events: GateEvent[] = []
    const maxAge = this.o.maxAgeMs ?? 86_400_000
    // The oldest checked first; each checked one goes to the back.
    const batch = [...this.watched].slice(0, this.o.sweepBatch ?? 500)
    for (const [txid, w] of batch) {
      this.watched.delete(txid)
      this.watched.set(txid, w)
    }
    let next = 0
    const worker = async (): Promise<void> => {
      while (next < batch.length) {
        const [txid, w] = batch[next++]!
        const ev = await this.check(txid, w, maxAge).catch(() => undefined)
        if (ev === undefined || !this.watched.has(txid)) continue
        this.watched.delete(txid)
        if (ev.kind === 'confirmed') this.exposure.release(txid)
        else this.exposure.flag(w.payer, `${ev.kind}: ${txid}`)
        events.push(ev)
      }
    }
    await Promise.all(Array.from({ length: Math.min(8, batch.length) }, worker))
    events.sort((a, b) => (a.txid < b.txid ? -1 : 1))
    for (const ev of events) this.record(ev)
    return events
  }

  private async check(txid: string, w: Watched, maxAge: number): Promise<GateEvent | undefined> {
    const ev = { txid, payer: w.payer, sats: w.sats }
    if (await this.mined(w.tx)) return { ...ev, kind: 'confirmed' }
    if (this.o.node !== undefined) {
      for (const [n, i] of w.tx.inputs.entries()) {
        const src = i.sourceTXID ?? i.sourceTransaction?.id('hex')
        if (src === undefined) continue
        const by = await this.o.node.spender(src, i.sourceOutputIndex).catch(() => '')
        if (by !== '' && by !== txid) return { ...ev, kind: 'double-spent', why: `input ${n} (${src}.${i.sourceOutputIndex}) is spent by ${by}` }
      }
    }
    const st = await this.o.arcade?.status(txid).catch(() => undefined)
    if (st?.txStatus === 'REJECTED') return { ...ev, kind: 'refused', why: `${st.txStatus} ${st.extraInfo ?? ''}`.trim() }
    if (maxAge > 0 && this.now() - w.since > maxAge) return { ...ev, kind: 'unmined', why: `not mined within ${Math.round(maxAge / 1000)} s` }
    return undefined
  }

  private record(ev: GateEvent): void {
    this.o.host.metrics.inc(`${this.o.app}_payment_events_total`, { kind: ev.kind })
    if (ev.kind === 'confirmed') return
    this.o.host.log(`${this.o.app} fast payment lost; its payer is held for confirmation`, { ...ev })
    if (this.o.stateDir === undefined) return
    try {
      mkdirSync(this.o.stateDir, { recursive: true })
      appendFileSync(join(this.o.stateDir, 'unsettleable.jsonl'), JSON.stringify({ ...ev, at: Math.floor(this.now() / 1000) }) + '\n')
    } catch (e) {
      this.o.host.log(`${this.o.app} unsettleable record failed`, { err: String(e) })
    }
  }
}

/** The acceptance settings a host reads from its environment. */
export interface AcceptConfig {
  arcadeURL?: string
  assetURL?: string
  policy: AcceptancePolicy
  watchMs: number
}

/** A duration: <n>ms, <n>s, <n>m or <n>h. */
export function parseDuration(name: string, raw: string): number {
  const m = /^([0-9]+)(ms|s|m|h)$/.exec(raw.trim())
  if (m === null || !Number.isSafeInteger(Number(m[1]))) throw new Error(`${name}: "${raw}" is not a duration like 2s, 10m or 1h`)
  return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as 'ms' | 's' | 'm' | 'h']
}

/**
 * Reads <P>_ARCADE_URL, <P>_ASSET_URL and <P>_ACCEPT_THRESHOLD_SATS,
 * _PAYER_LIMIT, _TOTAL_LIMIT, _WINDOW, _WATCH. Defaults are bcommon's
 * DefaultPolicy, watch 0.
 */
export function parseAcceptConfig(prefix: string, env: NodeJS.ProcessEnv): AcceptConfig {
  const policy = defaultAcceptancePolicy()
  const sats = (key: string, def: number): number => {
    const raw = env[`${prefix}_${key}`]?.trim()
    if (raw === undefined || raw === '') return def
    if (!/^(0|[1-9][0-9]*)$/.test(raw) || !Number.isSafeInteger(Number(raw))) throw new Error(`${prefix}_${key}: "${raw}" is not a whole number of satoshis`)
    return Number(raw)
  }
  const dur = (key: string, def: number): number => {
    const raw = env[`${prefix}_${key}`]?.trim()
    return raw === undefined || raw === '' ? def : parseDuration(`${prefix}_${key}`, raw)
  }
  policy.thresholdSats = sats('ACCEPT_THRESHOLD_SATS', policy.thresholdSats)
  policy.payerLimit = sats('ACCEPT_PAYER_LIMIT', policy.thresholdSats)
  policy.totalLimit = sats('ACCEPT_TOTAL_LIMIT', 10 * policy.thresholdSats)
  policy.window = dur('ACCEPT_WINDOW', policy.window)
  const url = (key: string): string | undefined => {
    const raw = env[`${prefix}_${key}`]?.trim()
    if (raw === undefined || raw === '') return undefined
    if (!/^https?:\/\/[^\s]+$/.test(raw)) throw new Error(`${prefix}_${key}: "${raw}" is not an http(s) URL`)
    return raw
  }
  return { arcadeURL: url('ARCADE_URL'), assetURL: url('ASSET_URL'), policy, watchMs: dur('ACCEPT_WATCH', 0) }
}

/** The gate for a host's configuration. */
export function gateFor(app: string, host: ModuleHost, headers: ChainTracker, c: AcceptConfig, stateDir?: string): PaymentGate {
  return new PaymentGate({
    app,
    host,
    headers,
    policy: c.policy,
    arcade: c.arcadeURL === undefined ? undefined : new ArcadeHttp(c.arcadeURL),
    node: c.assetURL === undefined ? undefined : new AssetHttp(c.assetURL),
    watchMs: c.watchMs,
    stateDir,
  })
}
