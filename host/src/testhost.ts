/**
 * The module and the real overlay engine together, for tests: an engine
 * over an in-memory store with the module's managers and lookup service
 * mounted, the configuration the tests use, and submit and answer helpers.
 */
import { Engine } from '@bsv/overlay'
import { Transaction, type ChainTracker } from '@bsv/sdk'
import type { Module, TopicManager } from '@lightwebinc/bcommon'
import { DefaultBudget, DefaultResponseBudget } from '@lightwebinc/bcommon/host'
import { MemoryJournal } from './journal.js'
import { storeModule, type Config } from './module.js'
import { LookupService } from './storerec.js'
import { Chain } from './testmint.js'
import { FakeHost, MemoryStorage, quietConsole } from './testutil.js'
import { Refused } from './tm_bstore.js'

export const bucket = 'photos_qzvmkwerty'
export const topic = `tm_bstore_${bucket}`

export function engineOf(mod: Module, storage: MemoryStorage, tracker: ChainTracker, topics: string[] = [topic]): Engine {
  return new Engine(
    mod.topics as never,
    mod.lookups as never,
    storage as never,
    tracker,
    undefined,
    [],
    [],
    undefined,
    undefined,
    Object.fromEntries(topics.map((t) => [t, false])),
    false,
    undefined,
    undefined,
    undefined,
    quietConsole(),
  )
}

export const config: Config = {
  buckets: [bucket],
  stateDir: '/nonexistent',
  headersURL: 'http://127.0.0.1:1',
  served: [],
  keepDays: 31,
  pendingDays: 7,
  otherHours: 24,
  maxBEEF: 262144,
  maxSweepInputs: 1024,
  prices: new Map(),
  sessions: { max: 100, ttlSeconds: 600 },
  handshakes: DefaultBudget,
  responses: DefaultResponseBudget,
}

/** A clock the tests move: Unix milliseconds. */
export class Clock {
  constructor(public ms = Date.now()) {}
  readonly now = (): number => this.ms
  advance(ms: number): void {
    this.ms += ms
  }
}

export interface SetupOptions {
  naive?: boolean
  journal?: MemoryJournal
  chain?: Chain
  storage?: MemoryStorage
  config?: Partial<Config>
  clock?: Clock
  /** OVERLAY_TOPICS, when a test names blob topics. */
  overlayTopics?: string[]
}

/** A module and its engine, with storage handed over as the host does. */
export async function setup(opts: SetupOptions = {}) {
  const host = new FakeHost()
  const chain = opts.chain ?? new Chain(70000)
  const journal = opts.journal ?? new MemoryJournal()
  const clock = opts.clock ?? new Clock()
  const c: Config = { ...config, ...opts.config }
  const records = c.buckets.map((b) => `tm_bstore_${b}`)
  const m = storeModule(host, c, opts.overlayTopics ?? records, { journal, headers: chain.tracker, now: clock.now })
  const topics = Object.keys(m.module.topics ?? {})
  const storage = opts.storage ?? new MemoryStorage()
  const rows = []
  for (const t of topics) rows.push(...(await storage.findUTXOsForTopic(t, undefined, undefined, true)))
  await m.ls.restore(rows, storage)
  let calls = 0
  for (const t of topics) {
    const tm = m.module.topics![t]!
    const counting: TopicManager = {
      identifyAdmissibleOutputs: async (beef, previousCoins) => {
        calls++
        try {
          return await tm.identifyAdmissibleOutputs(beef, previousCoins)
        } catch (e) {
          // A manager that returned every refusal as empty instructions, as a manager that does not raise would.
          if (opts.naive === true && e instanceof Refused) return { outputsToAdmit: [], coinsToRetain: [...previousCoins] }
          throw e
        }
      },
      getDocumentation: tm.getDocumentation.bind(tm),
      getMetaData: tm.getMetaData.bind(tm),
    }
    m.module.topics![t] = counting
  }
  const engine = engineOf(m.module, storage, chain.tracker, topics)
  return { host, ls: m.ls, storage, journal, chain, config: c, engine, module: m.module, clock, overlayTopics: opts.overlayTopics, calls: () => calls }
}

export type Rig = Awaited<ReturnType<typeof setup>>

/** Submits to topic t; the outputs admitted, or undefined when the engine itself refused. Drops the index decided are then carried out. */
export async function submit(r: Rig, beef: number[] | Uint8Array, t = topic): Promise<number[] | undefined> {
  let out: number[] | undefined
  try {
    const steak = await r.engine.submit({ beef: Array.from(beef), topics: [t] })
    out = steak[t]?.outputsToAdmit ?? []
  } catch {
    out = undefined
  }
  await r.ls.flush()
  return out
}

/** Submits a transaction alone (Atomic BEEF), as a carrier or a mined object is published. */
export const submitTx = async (r: Rig, tx: Transaction, t = topic): Promise<number[] | undefined> => await submit(r, tx.toAtomicBEEF(), t)

export interface Answered {
  tx: Transaction
  txid: string
  vout: number
  beef: number[]
  context?: number[]
}

/** An engine answer, hydrated: each output's transaction, and its context when it has one. */
export async function outputs(r: Rig, query: Record<string, unknown>): Promise<Answered[]> {
  const a = await r.engine.lookup({ service: LookupService, query })
  if (a.type !== 'output-list') throw new Error('not an output list')
  return a.outputs.map((o) => {
    const tx = Transaction.fromBEEF(o.beef)
    return { tx, txid: tx.id('hex'), vout: o.outputIndex, beef: o.beef, context: (o as { context?: number[] }).context }
  })
}

/** An engine answer: each output's txid. */
export const answer = async (r: Rig, query: Record<string, unknown>): Promise<string[]> => (await outputs(r, query)).map((o) => o.txid)

/** A question the service refuses: its error message. */
export async function refused(r: Rig, query: Record<string, unknown>): Promise<string> {
  try {
    await r.ls.lookup({ service: LookupService, query })
  } catch (e) {
    return (e as Error).message
  }
  throw new Error(`answered: ${JSON.stringify(query)}`)
}

/** A new module and lookup service over the same storage and journal: a restart. */
export async function restart(r: Rig, o: { journal?: MemoryJournal; config?: Partial<Config> } = {}): Promise<Rig> {
  return await setup({ chain: r.chain, storage: r.storage, journal: o.journal ?? r.journal, config: { ...r.config, ...o.config }, clock: r.clock, overlayTopics: r.overlayTopics })
}
