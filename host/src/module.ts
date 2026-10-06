/**
 * The bstore host module: `tm_bstore_<bucket>` for each configured bucket,
 * `tm_bstoreblob_<bucket>` where the host's topics name one, and
 * `ls_bstore` for all of them, loaded into a reference overlay host by path
 * (`OVERLAY_MODULES=/abs/path/to/bstore-module.js`). The host carries no
 * bstore code; this file's default export is the factory it calls.
 *
 * Configuration is read from the environment when the host calls the
 * factory (docs/host.md):
 *
 *   BSTORE_BUCKETS            required: the bucket identifiers, a comma list
 *   BSTORE_STATE_DIR          required: an absolute directory kept for the
 *                             host's life (the rows journal, the payment ledger)
 *   BSTORE_HEADERS_URL        the native header source (/v1/root/<height>,
 *                             /v1/tip) admission checks proofs against;
 *                             OVERLAY_CHAIN_TRACKER_URL when that is one
 *   BSTORE_SERVED             the chains this host serves, a comma list of
 *                             <bucket>:<owner>:<id>
 *   BSTORE_KEEP_DAYS          the long floor (replaced entries, advertisements,
 *                             hold outputs, outcomes, front statements, sweeps'
 *                             outpoints, retraction marks); at least and by default 31
 *   BSTORE_PENDING_DAYS       of an entry no token names, of a served chain's
 *                             owner or writers; at least and by default 7
 *   BSTORE_OTHER_HOURS        of everything else; at least and by default 24
 *   BSTORE_MAX_BEEF           the host's BEEF bound; at least and by default 262144
 *   BSTORE_MAX_SWEEP_INPUTS   the inputs of a sweep the host accepts; at
 *                             least and by default 1024
 *   BSTORE_LISTEN             host:port of the terms route (paid.ts)
 *   BSTORE_PRICES             the classes priced, e.g.
 *                             history=5,chain=10,export=20,outcome-history=5,outcome-history-of=5
 *   BSTORE_PAYEE_KEY          the payee's private key, 64 hex characters
 *   BSTORE_SESSIONS, BSTORE_SESSION_TTL
 *                             the most BRC-104 sessions kept, and seconds
 *                             one idle is kept; default 10000 and 600
 *   BSTORE_HANDSHAKES_PER_SEC, BSTORE_HANDSHAKE_BURST
 *                             the BRC-104 handshakes a second the terms
 *                             route answers, and at once; default 4 and 8
 *   BSTORE_HANDSHAKES_PER_ADDR_PER_SEC, BSTORE_HANDSHAKE_ADDR_BURST
 *                             the same for one remote address; default 1 and 4
 *   BSTORE_RESPONSES_PER_SEC, BSTORE_RESPONSE_BURST
 *                             the signed responses a second one session is
 *                             given, and at once; default 5 and 20
 *   BSTORE_ARCADE_URL, BSTORE_ASSET_URL
 *                            arcade, which the host broadcasts payments
 *                            through, and the node's asset service (spend
 *                            view, proofs); without both every payment is
 *                            held until mined (accept.ts)
 *   BSTORE_ACCEPT_THRESHOLD_SATS, BSTORE_ACCEPT_PAYER_LIMIT,
 *   BSTORE_ACCEPT_TOTAL_LIMIT, BSTORE_ACCEPT_WINDOW, BSTORE_ACCEPT_WATCH
 *                            the acceptance rule; default 25000000, one
 *                            and ten thresholds, 1h, 0s
 *
 * Every `tm_bstore_` and `tm_bstoreblob_` topic in OVERLAY_TOPICS must be a
 * configured bucket's, because a bstore topic left to the host's default
 * manager would admit anything, and none may be named in
 * OVERLAY_SYNC_PEERS. Every mistake throws, and the host refuses to start.
 */
import { isAbsolute } from 'node:path'
import type { Server } from 'node:http'
import { PrivateKey, ProtoWallet, type ChainTracker } from '@bsv/sdk'
import { Transaction, Utils } from '@bsv/sdk'
import type { Module, ModuleHost, TopicManager } from '@lightwebinc/bcommon'
import { gateFor, parseAcceptConfig, type AcceptConfig } from './accept.js'
import { DefaultBudget, DefaultResponseBudget, type BudgetConfig, type ResponseBudgetConfig } from './budget.js'
import { HeaderTracker, isNativeSource } from './headers.js'
import { FileJournal, type Journal } from './journal.js'
import { DropReasons, FloorKeepDays, FloorOtherHours, FloorPendingDays, StoreLookupService, TokenStatuses } from './ls_bstore.js'
import { isBucket, isKey } from './names.js'
import { DefaultMaxSessions, DefaultSessionTTL, HandshakeResults, LedgerReceiver, LookupFront, RequestResults, parsePrices } from './paid.js'
import { BlobPrefix, DefaultMaxBEEF, DefaultMaxSweepInputs, LookupService, RecordsPrefix } from './storerec.js'
import { AdmitKinds, BlobTopicManager, Reasons, RecordsTopicManager } from './tm_bstore.js'
import { fromHex } from './util.js'

/** The bundle's banner, which the bundle step defines (scripts/bundle.js); the tsc tree leaves it undefined. */
declare const BSTORE_BUILD: string | undefined

/** How often retention runs, seconds. */
const PruneEvery = 3600
/** How often drops still waiting are tried again, seconds. */
const FlushEvery = 2

export interface Config {
  buckets: string[]
  stateDir: string
  headersURL: string
  /** `<bucket>|<owner hex>|<id hex>`. */
  served: string[]
  keepDays: number
  pendingDays: number
  otherHours: number
  maxBEEF: number
  maxSweepInputs: number
  listen?: { host: string; port: number }
  prices: Map<string, number>
  payeeKey?: PrivateKey
  sessions: { max: number; ttlSeconds: number }
  handshakes: BudgetConfig
  responses: ResponseBudgetConfig
  /** Payment acceptance; bcommon's DefaultPolicy with no arcade or node (every payment held) when unset. */
  accept?: AcceptConfig
}

/** Parses BSTORE_BUCKETS. Throws on anything it cannot take exactly. */
export function parseBuckets(raw: string | undefined): string[] {
  if (raw === undefined || raw.trim() === '') throw new Error('BSTORE_BUCKETS is required: name each bucket this host carries')
  const out: string[] = []
  for (const entry of raw.split(',')) {
    const b = entry.trim()
    if (!isBucket(b)) throw new Error(`BSTORE_BUCKETS: "${b}" is not a bucket identifier <name>_<suffix>`)
    if (out.includes(b)) throw new Error(`BSTORE_BUCKETS names ${b} twice`)
    out.push(b)
  }
  return out
}

/** Parses BSTORE_SERVED: `<bucket>:<owner>:<id>`, each of a carried bucket, once. */
export function parseServed(raw: string | undefined, buckets: readonly string[]): string[] {
  const out: string[] = []
  if (raw === undefined || raw.trim() === '') return out
  for (const entry of raw.split(',')) {
    const parts = entry.trim().split(':')
    const [b, owner, id] = parts
    if (parts.length !== 3 || b === undefined || owner === undefined || id === undefined) throw new Error(`BSTORE_SERVED: "${entry.trim()}" is not <bucket>:<owner>:<id>`)
    if (!buckets.includes(b)) throw new Error(`BSTORE_SERVED: ${b} is not a bucket BSTORE_BUCKETS names`)
    if (!/^[0-9a-f]{66}$/.test(owner) || !isKey(fromHex(owner))) throw new Error(`BSTORE_SERVED: ${owner} is not an owner's key, 66 lowercase hex digits`)
    if (!/^[0-9a-f]{64}$/.test(id)) throw new Error(`BSTORE_SERVED: ${id} is not a bucket id, 64 lowercase hex digits`)
    const key = `${b}|${owner}|${id}`
    if (out.includes(key)) throw new Error(`BSTORE_SERVED names ${entry.trim()} twice`)
    out.push(key)
  }
  return out
}

function integer(name: string, raw: string | undefined, def: number, min: number): number {
  if (raw === undefined || raw.trim() === '') return def
  const v = raw.trim()
  if (!/^[0-9]+$/.test(v) || !Number.isSafeInteger(Number(v)) || Number(v) < min) throw new Error(`${name}: "${raw}" is not an integer of at least ${min}`)
  return Number(v)
}

/** A positive rate, decimals allowed, at most 1e6. */
function rate(name: string, raw: string | undefined, def: number): number {
  if (raw === undefined || raw.trim() === '') return def
  const v = raw.trim()
  if (!/^[0-9]+(\.[0-9]+)?$/.test(v) || !(Number(v) > 0) || Number(v) > 1e6) throw new Error(`${name}: "${raw}" is not a positive number of at most 1000000`)
  return Number(v)
}

/** Reads the whole configuration from env. */
export function parseConfig(env: NodeJS.ProcessEnv): Config {
  const buckets = parseBuckets(env['BSTORE_BUCKETS'])
  const stateDir = env['BSTORE_STATE_DIR']
  if (stateDir === undefined || !isAbsolute(stateDir)) {
    throw new Error("BSTORE_STATE_DIR is required: an absolute directory kept for the host's life, where the rows journal and the payment ledger live")
  }
  const own = env['BSTORE_HEADERS_URL']?.trim()
  const host = env['OVERLAY_CHAIN_TRACKER_URL']?.trim()
  let headersURL: string
  if (own !== undefined && own !== '') {
    if (!isNativeSource(own)) throw new Error(`BSTORE_HEADERS_URL: "${own}" is not an http or https URL`)
    headersURL = own
  } else if (host !== undefined && isNativeSource(host)) {
    headersURL = host
  } else {
    throw new Error('BSTORE_HEADERS_URL is required: a native header source (/v1/root/<height>, /v1/tip) that token and mined-object proofs are checked against; OVERLAY_CHAIN_TRACKER_URL is not one')
  }
  const c: Config = {
    buckets,
    stateDir,
    headersURL,
    served: parseServed(env['BSTORE_SERVED'], buckets),
    keepDays: integer('BSTORE_KEEP_DAYS', env['BSTORE_KEEP_DAYS'], FloorKeepDays, FloorKeepDays),
    pendingDays: integer('BSTORE_PENDING_DAYS', env['BSTORE_PENDING_DAYS'], FloorPendingDays, FloorPendingDays),
    otherHours: integer('BSTORE_OTHER_HOURS', env['BSTORE_OTHER_HOURS'], FloorOtherHours, FloorOtherHours),
    maxBEEF: integer('BSTORE_MAX_BEEF', env['BSTORE_MAX_BEEF'], DefaultMaxBEEF, DefaultMaxBEEF),
    maxSweepInputs: integer('BSTORE_MAX_SWEEP_INPUTS', env['BSTORE_MAX_SWEEP_INPUTS'], DefaultMaxSweepInputs, DefaultMaxSweepInputs),
    prices: parsePrices(env['BSTORE_PRICES']),
    sessions: {
      max: integer('BSTORE_SESSIONS', env['BSTORE_SESSIONS'], DefaultMaxSessions, 1),
      ttlSeconds: integer('BSTORE_SESSION_TTL', env['BSTORE_SESSION_TTL'], DefaultSessionTTL, 1),
    },
    handshakes: {
      perSec: rate('BSTORE_HANDSHAKES_PER_SEC', env['BSTORE_HANDSHAKES_PER_SEC'], DefaultBudget.perSec),
      burst: integer('BSTORE_HANDSHAKE_BURST', env['BSTORE_HANDSHAKE_BURST'], DefaultBudget.burst, 1),
      perAddressPerSec: rate('BSTORE_HANDSHAKES_PER_ADDR_PER_SEC', env['BSTORE_HANDSHAKES_PER_ADDR_PER_SEC'], DefaultBudget.perAddressPerSec),
      addressBurst: integer('BSTORE_HANDSHAKE_ADDR_BURST', env['BSTORE_HANDSHAKE_ADDR_BURST'], DefaultBudget.addressBurst, 1),
    },
    responses: {
      perSec: rate('BSTORE_RESPONSES_PER_SEC', env['BSTORE_RESPONSES_PER_SEC'], DefaultResponseBudget.perSec),
      burst: integer('BSTORE_RESPONSE_BURST', env['BSTORE_RESPONSE_BURST'], DefaultResponseBudget.burst, 1),
    },
    accept: parseAcceptConfig('BSTORE', env),
  }
  const listen = env['BSTORE_LISTEN']?.trim()
  if (listen !== undefined && listen !== '') {
    const m = /^(.+):([0-9]{1,5})$/.exec(listen)
    const port = m === null ? NaN : Number(m[2])
    if (m === null || port < 1 || port > 65535) throw new Error(`BSTORE_LISTEN: "${listen}" is not host:port`)
    c.listen = { host: m[1]!.replace(/^\[(.*)\]$/, '$1'), port }
  }
  const key = env['BSTORE_PAYEE_KEY']?.trim()
  if (key !== undefined && key !== '') {
    if (!/^[0-9a-f]{64}$/.test(key)) throw new Error('BSTORE_PAYEE_KEY is not 64 lowercase hex characters')
    c.payeeKey = PrivateKey.fromHex(key)
  }
  const charged = [...c.prices.values()].some((p) => p > 0)
  if (charged && (c.listen === undefined || c.payeeKey === undefined)) {
    throw new Error('BSTORE_PRICES prices a class: BSTORE_LISTEN and BSTORE_PAYEE_KEY are required')
  }
  if (c.prices.size > 0 && c.listen === undefined) throw new Error('BSTORE_PRICES needs BSTORE_LISTEN, where the terms document is served')
  return c
}

export interface Mounted {
  module: Module
  ls: StoreLookupService
  front?: LookupFront
  /** Starts the terms route (when configured), retention and the drop timer; resolves when the route listens. */
  start: () => Promise<Server | undefined>
}

export interface Overrides {
  journal?: Journal
  headers?: ChainTracker
  /** The host's clock, Unix milliseconds. */
  now?: () => number
}

/**
 * The module for this configuration. `overlayTopics` is OVERLAY_TOPICS,
 * when known: a blob topic is mounted only where it names one. The
 * overrides replace the state directory's journal, the header source and
 * the clock, for tests.
 */
export function storeModule(host: ModuleHost, c: Config, overlayTopics?: readonly string[], o: Overrides = {}): Mounted {
  const records = new Set(c.buckets.map((b) => RecordsPrefix + b))
  const blobs = new Set(c.buckets.map((b) => BlobPrefix + b))
  const named = overlayTopics ?? []
  for (const t of named) {
    if (t.startsWith(BlobPrefix) && !blobs.has(t)) throw new Error(`OVERLAY_TOPICS names ${t} but BSTORE_BUCKETS does not configure its bucket; it would be admitted by the default manager`)
    if (t.startsWith(RecordsPrefix) && !t.startsWith(BlobPrefix) && !records.has(t)) {
      throw new Error(`OVERLAY_TOPICS names ${t} but BSTORE_BUCKETS does not configure it; it would be admitted by the default manager`)
    }
  }
  for (const kind of AdmitKinds) host.metrics.preset('bstore_admitted_total', { kind })
  for (const kind of AdmitKinds) host.metrics.preset('bstore_admitted_repeats_total', { kind })
  for (const reason of Reasons) host.metrics.preset('bstore_refused_total', { reason })
  for (const status of TokenStatuses) host.metrics.preset('bstore_token_status_total', { status })
  for (const why of DropReasons) host.metrics.preset('bstore_dropped_total', { why })
  for (const why of ['cap', 'idle']) host.metrics.preset('bstore_sessions_evicted_total', { why })
  for (const result of RequestResults) host.metrics.preset('bstore_requests_total', { result })
  for (const result of HandshakeResults) host.metrics.preset('bstore_handshakes_total', { result })
  host.metrics.preset('bstore_retractions_total')
  const priced = new Set([...c.prices].filter(([, p]) => p > 0).map(([name]) => name))
  const tracker = o.headers ?? new HeaderTracker(c.headersURL)
  const ls = new StoreLookupService({
    buckets: new Set(c.buckets),
    served: new Set(c.served),
    host,
    journal: o.journal ?? new FileJournal(c.stateDir),
    priced,
    keepDays: c.keepDays,
    pendingDays: c.pendingDays,
    otherHours: c.otherHours,
    headers: tracker,
    now: o.now,
  })
  host.metrics.gauge('bstore_buckets', () => c.buckets.length)
  host.metrics.gauge('bstore_tokens', () => ls.tokenCount)
  host.metrics.gauge('bstore_tokens_pending', () => ls.pendingTokens)
  host.metrics.gauge('bstore_entries', () => ls.count('entry'))
  host.metrics.gauge('bstore_outcomes', () => ls.count('outcome'))
  host.metrics.gauge('bstore_fronts', () => ls.count('front'))
  host.metrics.gauge('bstore_advertisements', () => ls.adCount)
  host.metrics.gauge('bstore_hold_outputs', () => ls.holdCount)
  host.metrics.gauge('bstore_sweeps', () => ls.sweepCount)
  host.metrics.gauge('bstore_sweep_outpoints', () => ls.sweepOutpoints)
  host.metrics.gauge('bstore_retraction_marks', () => ls.markCount)
  host.metrics.gauge('bstore_outpoint_rows', () => ls.rowCount)
  host.metrics.gauge('bstore_pending_drops', () => ls.pendingDrops)
  // As the last retention pass counted them.
  host.metrics.gauge('bstore_maps_complete', () => ls.tally.mapsComplete)
  host.metrics.gauge('bstore_maps_incomplete', () => ls.tally.mapsIncomplete)
  host.metrics.gauge('bstore_chains_broken', () => ls.tally.chainsBroken)
  host.metrics.gauge('bstore_anchored_entries', () => ls.tally.anchored)
  const topics: Record<string, TopicManager> = {}
  for (const b of c.buckets) topics[RecordsPrefix + b] = new RecordsTopicManager(RecordsPrefix + b, host, tracker, c.maxBEEF, c.maxSweepInputs)
  const mountedBlobs = named.filter((t) => blobs.has(t))
  for (const t of mountedBlobs) topics[t] = new BlobTopicManager(t, host)
  let front: LookupFront | undefined
  const accept = c.accept ?? parseAcceptConfig('BSTORE', {})
  const receiver = c.listen !== undefined && priced.size > 0 ? new LedgerReceiver(c.stateDir) : undefined
  const gate = receiver !== undefined && tracker !== undefined ? gateFor('bstore', host, tracker, accept, c.stateDir) : undefined
  if (gate !== undefined && receiver !== undefined) {
    for (const f of receiver.fast) {
      try {
        gate.restore(Transaction.fromAtomicBEEF(Utils.toArray(f.beef, 'base64')), f.senderIdentityKey, f.satoshis, f.at)
      } catch {
        // a line whose BEEF does not parse was never answered fast
      }
    }
  }
  if (c.listen !== undefined) {
    front = new LookupFront({
      ls,
      host,
      prices: c.prices,
      wallet: c.payeeKey === undefined ? undefined : new ProtoWallet(c.payeeKey),
      receiver,
      headers: tracker,
      gate,
      sessions: c.sessions,
      budget: c.handshakes,
      responses: c.responses,
    })
  }
  const start = async (): Promise<Server | undefined> => {
    // Retention runs once the host has restored the index, and hourly after.
    const first = setInterval(() => {
      if (!ls.restored) return
      clearInterval(first)
      ls.prune().catch((e: unknown) => host.log('ls_bstore retention failed', { err: String(e) }))
    }, 1000)
    first.unref()
    const prune = setInterval(() => {
      if (!ls.restored) return
      ls.prune().catch((e: unknown) => host.log('ls_bstore retention failed', { err: String(e) }))
    }, PruneEvery * 1000)
    prune.unref()
    let sweeping = false
    let swept = 0
    const flush = setInterval(() => {
      // The payment watch rides this tick, at most every 30 s.
      if (gate !== undefined && !sweeping && Date.now() - swept >= 30_000) {
        sweeping = true
        swept = Date.now()
        gate
          .sweep()
          .catch((e: unknown) => host.log('bstore payment watch failed', { err: String(e) }))
          .finally(() => (sweeping = false))
      }
      if (ls.restored && ls.pendingDrops > 0) ls.flush().catch((e: unknown) => host.log('ls_bstore drop failed', { err: String(e) }))
    }, FlushEvery * 1000)
    flush.unref()
    if (front === undefined || c.listen === undefined) return undefined
    const server = await front.listen(c.listen.port, c.listen.host)
    server.unref()
    return server
  }
  const build = typeof BSTORE_BUILD === 'string' ? BSTORE_BUILD : 'unbundled'
  host.log('bstore module mounted', {
    topics: [...records].join(','),
    blobs: mountedBlobs.length === 0 ? 'none' : `${mountedBlobs.join(',')} (refuses everything: a host holds no pieces)`,
    lookup: LookupService,
    served: c.served.length,
    headers: o.headers === undefined ? c.headersURL : 'supplied',
    floors: `keep ${c.keepDays}d, pending ${c.pendingDays}d, other ${c.otherHours}h`,
    terms: c.listen === undefined ? 'none' : `${c.listen.host}:${c.listen.port}`,
    priced: [...priced].join(',') || 'none',
    sessions: c.listen === undefined ? 'none' : `${c.sessions.max}, idle ${c.sessions.ttlSeconds}s`,
    handshakes: c.listen === undefined ? 'none' : `${c.handshakes.perSec}/s burst ${c.handshakes.burst}, per address ${c.handshakes.perAddressPerSec}/s burst ${c.handshakes.addressBurst}`,
    responses: c.listen === undefined ? 'none' : `${c.responses.perSec}/s burst ${c.responses.burst} a session`,
    payments: gate === undefined ? 'none' : gate.networked ? `fast to ${accept.policy.thresholdSats} sat, payer ${accept.policy.payerLimit}, total ${accept.policy.totalLimit}` : 'held until mined (no arcade or asset URL)',
    build,
  })
  return { module: { topics, lookups: { [LookupService]: ls } }, ls, front, start }
}

/**
 * Refuses a host configured to synchronise a `tm_bstore_` topic with a peer
 * (OVERLAY_SYNC_PEERS, `topic=url,...;topic=...`): that synchronisation
 * carries only unspent outputs, so it never carries a chain's spent tokens,
 * and it drops what a topic manager refuses without trying it again. A host
 * joins a bucket by replay (spec section 12.6).
 */
export function checkNoSync(raw: string | undefined): void {
  for (const entry of (raw ?? '').split(';')) {
    const t = entry.split('=')[0]!.trim()
    if (t.startsWith(RecordsPrefix) || t.startsWith(BlobPrefix)) {
      throw new Error(`OVERLAY_SYNC_PEERS names ${t}: a bstore topic is never synchronised as unspent outputs; a host joins a bucket by replay`)
    }
  }
}

/** The factory the host calls: the configuration from the environment, checked against OVERLAY_TOPICS. */
export default async function create(host: ModuleHost): Promise<Module> {
  const overlay = process.env['OVERLAY_TOPICS']
  const topics = overlay === undefined ? undefined : overlay.split(',').map((t) => t.trim()).filter((t) => t !== '')
  checkNoSync(process.env['OVERLAY_SYNC_PEERS'])
  const m = storeModule(host, parseConfig(process.env), topics)
  await m.start()
  return m.module
}

export { Refused, RecordsTopicManager, BlobTopicManager } from './tm_bstore.js'
export { StoreLookupService } from './ls_bstore.js'
export { LookupFront, termsDocument } from './paid.js'
