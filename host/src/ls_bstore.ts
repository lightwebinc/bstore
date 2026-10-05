/**
 * `ls_bstore`: one lookup service for every bucket a host carries (spec
 * sections 11 and 12.2 to 12.6).
 *
 * Two stores back it. The engine's storage holds the outputs the host
 * keeps: bucket tokens, entry, outcome and front carriers, advertisements,
 * hold outputs and sweeps' output 0, which this service indexes in memory
 * and rebuilds from storage on start. The journal (journal.ts) holds what
 * storage cannot: the first carrier of each funding outpoint, the input
 * outpoints of every admitted sweep, the retraction marks, first sights,
 * and which held outputs a later transaction spent.
 *
 * Every status is a function of what the host holds, never of arrival
 * order, except where the contract makes it one (the first carrier per
 * funding outpoint, the first 16 front statements, first sights):
 *
 * - a token is **rooted** when the host holds every token from it back to
 *   its chain's first by spends, `pending` until then; a rooted token of a
 *   served chain takes the status its branch's fold gives it
 *   (`consistent`, or `map-mismatch`, sticky). Only rooted, consistent
 *   tokens are answered as latest;
 * - an entry is `anchored` while a served chain's current map holds it,
 *   `replaced` once a consistent token named it and the map no longer
 *   does, `pending` while no token of the chain names it, and `retracted`
 *   when an admitted sweep spends its funding outpoint or the outpoint
 *   carries a retraction mark. A retracted carrier is dropped at once and
 *   never answered;
 * - an advertisement is live until a renewal (or any admitted transaction)
 *   spends it or its expiry passes the host's clock.
 *
 * To drop an object is to delete its output from the topic and evict the
 * engine's record that its transaction was applied, so the same object
 * offered again is decided afresh. The reference host hands a module no
 * engine, so the module does both through the storage object a restore is
 * handed (deleteOutput, deleteAppliedTransaction), after the engine has
 * recorded the admission it is dropping.
 */
import { Transaction, type ChainTracker } from '@bsv/sdk'
import type { LookupFormula, LookupQuestion, LookupService, LookupServiceMetaData, ModuleHost, Output, OutputAdmittedByTopic, RestoreStorage } from '@lightwebinc/bcommon'
import { firstPush, storedToken } from '@lightwebinc/bcommon'
import { readBucketOutput } from './admit.js'
import { decodeAdvertisement } from './advert.js'
import { outKey, afterTxid, keyOfCursor, outpoint, timeKey, txidOfKey } from './hostutil.js'
import type { Journal, Line } from './journal.js'
import { encodeEntryContext, encodeTokenContext, type MapLeaf } from './keymap.js'
import { MapState, proveObjectFast, provePageFast } from './mapstate.js'
import { AnyoneKey, bucketID } from './names.js'
import { Classes, parseQuery, type Question } from './query.js'
import { decodeEntry, decodeFront, decodeOutcome, type Bucket } from './records.js'
import { classify } from './script.js'
import { Sorted, SortedNumbers, addTo, removeFrom, sortedIn, sortedOut } from './sorted.js'
import { LookupService as ServiceName, PageExport, PageList, RecordsPrefix, StoreError } from './storerec.js'
import { displayTxid, equal, hashOrder, toHex } from './util.js'

/** Outputs a page of ads, unanchored, outcomes, outcome-history, history and chain holds (tokens, for history and chain). */
export const PageLimit = 64
/** Tokens answered at one seq, and the classes answered eight at a time (pending, ad, fronts, front, sweep). */
export const PerSeq = 8
export const PerSmall = 8
/** How far back `unanchored` answers by admission, and `outcomes` and its kin. */
export const UnanchoredDays = 7
export const OutcomeWindowDays = 31
/** Front statements kept the long floor per (root, advertiser), by first sight. */
export const FrontsPerAdvertiser = 16
/** Retention floors (spec section 12.3): the long floor, the floor of unnamed entries, and everything else. */
export const FloorKeepDays = 31
export const FloorPendingDays = 7
export const FloorOtherHours = 24
/** How long a choice between two rooted tokens at one seq is reused, milliseconds. */
const ChoiceTTL = 60_000
/** How long a drop waits for the engine to record the admission it drops, milliseconds. */
export const SettleMs = 60_000
const Day = 86_400_000
const Hour = 3_600_000

export type TokenStatus = 'pending' | 'rooted' | 'consistent' | 'map-mismatch'
export const TokenStatuses: readonly TokenStatus[] = ['pending', 'rooted', 'consistent', 'map-mismatch']
export const DropReasons = ['retracted', 'retention', 'second-carrier', 'unmatched-sweep'] as const

/** A bucket token the host holds. */
interface TokenEntry {
  txid: string
  vout: number
  b: string
  chain: string
  seq: number
  /** The predecessor's txid in display order; empty at seq 0. */
  prev: string
  rec: Bucket
  /** Every outpoint the transaction's inputs spend. */
  inputs: string[]
  /** At seq 0: an input's outpoint gives the bucket id. */
  first: boolean
  height: number
  root: string
  rooted: boolean
  status: TokenStatus
  reason: string
  /** First sight, Unix milliseconds. */
  at: number
}

/** One chain: a (bucket, owner, id) triple's tokens. */
interface ChainIdx {
  key: string
  b: string
  owner: string
  id: string
  served: boolean
  bySeq: Map<number, TokenEntry[]>
  seqs: SortedNumbers
  byTxid: Map<string, TokenEntry>
  /** prev txid -> the txids of tokens naming it. */
  children: Map<string, Set<string>>
  /** A served chain's maps, by the txid of the last consistent token of each branch. */
  branches: Map<string, MapState>
  /** The highest seq of a rooted token. */
  top: number
  /** Tokens a full seq turned away while pending, by the txid they name as predecessor. */
  turned: Map<string, Map<string, number>>
  choice: Map<number, { among: string; txid: string; at: number }>
  /** A served chain: every commitment a held token names (changes and consumed), with how many name it. */
  named: Map<string, number>
  /** A served chain: by key, the rooted tokens whose changes set or remove it; built when first asked. */
  hist?: Map<string, Array<{ t: TokenEntry; c: string }>>
  /** latest(), until a token is rooted or forgotten. */
  latest?: TokenEntry[]
}

/** A served chain's own indexes of entries no token of it names (pending, unanchored). */
interface ServedIdx {
  chain: string
  b: string
  owner: string
  id: string
  /** key -> writer -> time keys. */
  pendingByKey: Map<string, Map<string, Sorted>>
  /** writer -> txids, admitted within UnanchoredDays as of the last retention pass. */
  unanchored: Map<string, Sorted>
}

type CarrierKind = 'entry' | 'outcome' | 'front'

/** An entry, outcome or front carrier the host holds. */
interface CarrierEntry {
  c: string
  txid: string
  b: string
  kind: CarrierKind
  op: string
  at: number
  /** The record's owner: its writer, buyer or front. */
  owner: string
  created: number
  tkey: string
  // entry
  id?: string
  key?: string
  ekind?: number
  root?: string
  hold?: string
  // outcome
  answerer?: string
  provider?: string
  oroot?: string
  // front
  advertiser?: string
}

interface AdEntry {
  b: string
  txid: string
  vout: number
  root: string
  advertiser: string
  expiry: number
  at: number
  spent: boolean
  anyone: boolean
}

interface HoldEntry {
  b: string
  txid: string
  vout: number
  at: number
  spent: boolean
}

interface SweepEntry {
  b: string
  txid: string
  spent: string[]
  height: number
  offset: number
  at: number
}

/** What restore and retention need from the engine's storage. */
export interface Storage extends RestoreStorage {
  findOutput: (txid: string, outputIndex: number, topic?: string, spent?: boolean, includeBEEF?: boolean) => Promise<Output | null>
  deleteOutput?: (txid: string, outputIndex: number, topic: string) => Promise<void>
  deleteAppliedTransaction?: (txid: string, topic: string) => Promise<void>
  doesAppliedTransactionExist?: (tx: { txid: string; topic: string }) => Promise<boolean>
}

/** The engine's lookup answer. */
export interface OutputList {
  type: 'output-list'
  outputs: Array<{ beef: number[]; outputIndex: number; context?: number[] }>
}

export interface Options {
  buckets: ReadonlySet<string>
  /** The chains this host serves: `<bucket>|<owner hex>|<id hex>`. */
  served?: ReadonlySet<string>
  host: ModuleHost
  journal: Journal
  /** Priceable classes this host prices: asked through the engine, they are refused. */
  priced?: ReadonlySet<string>
  keepDays?: number
  pendingDays?: number
  otherHours?: number
  headers?: ChainTracker
  /** The host's clock, Unix milliseconds. */
  now?: () => number
}

const cmp = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0)
const srcTxid = (i: Transaction['inputs'][number]): string | undefined => i.sourceTXID ?? i.sourceTransaction?.id('hex')
const bin = (s: { toBinary(): number[] }): Uint8Array => Uint8Array.from(s.toBinary())
/** The commitment of a carrier: its txid in hash byte order, as hex. */
const cOf = (txid: string): string => toHex(hashOrder(txid))

/** A proof's block height, the txid's position in its block, and the root the path gives. */
function place(tx: Transaction): { height: number; offset: number; root: string } {
  const mp = tx.merklePath
  if (mp === undefined) return { height: 0, offset: 0, root: '' }
  const txid = tx.id('hex')
  const at = mp.path[0]?.find((l) => l.txid === true && l.hash === txid)
  let root = ''
  try {
    root = mp.computeRoot(txid)
  } catch {
    // A path that does not hold the txid proves nothing.
  }
  return { height: mp.blockHeight, offset: at?.offset ?? 0, root }
}

/** What retention reads of the served chains of one bucket, computed once a pass. */
interface ServedView {
  /** The owners and the latest consistent tokens' writers. */
  parties: Set<string>
  /** The commitments the current maps hold. */
  anchored: Set<string>
  /** Commitments a consistent token named that the current map no longer holds, with when they left it. */
  left: Map<string, number>
  /** The roots of the anchored entries the host holds. */
  roots: Set<string>
  /** The hold outpoints anchored or pending entries name. */
  holds: Set<string>
  /** The front statements kept the long floor. */
  fronts: Set<string>
}

export class StoreLookupService implements LookupService {
  readonly admissionMode = 'whole-tx' as const
  readonly spendNotificationMode = 'none' as const

  readonly buckets: ReadonlySet<string>
  private readonly served: ReadonlySet<string>
  private readonly servedBuckets = new Set<string>()
  private readonly host: ModuleHost
  private readonly journal: Journal
  private readonly priced: ReadonlySet<string>
  private readonly keep: number
  private readonly pendingFloor: number
  private readonly other: number
  private readonly headers: ChainTracker | undefined
  private readonly now: () => number

  // What the journal holds.
  private readonly rows = new Map<string, { c: string; at: number }>()
  private readonly marks = new Map<string, { key: string; at: number; life: boolean }>()
  private readonly table = new Map<string, { s: string; at: number }>()
  private readonly seen = new Map<string, { b: string; at: number }>()
  private readonly tokenSeen = new Map<string, { b: string; vout: number; at: number }>()
  private readonly minedSeen = new Map<string, { b: string; txid: string; vout: number; at: number }>()
  private readonly spentHeld = new Set<string>()

  // What the host holds, from storage.
  private readonly chains = new Map<string, ChainIdx>()
  private readonly tokenChain = new Map<string, string>()
  private readonly servedIdx = new Map<string, ServedIdx>()
  private readonly carriers = new Map<string, CarrierEntry>()
  private readonly byOp = new Map<string, Set<string>>()
  private readonly outcomesBy = new Map<string, Sorted>()
  private readonly outcomeHistory = new Map<string, Sorted>()
  private readonly frontsByAdvertiser = new Map<string, Sorted>()
  private readonly frontsByPair = new Map<string, Sorted>()
  private readonly ads = new Map<string, AdEntry>()
  private readonly adsLive = new Map<string, Sorted>()
  private readonly holds = new Map<string, HoldEntry>()
  private readonly sweeps = new Map<string, SweepEntry>()
  private readonly sweepsByOp = new Map<string, Set<string>>()
  /** By `<bucket>|<txid>`: the output indexes the index holds, so a transaction is evicted only once none is left. */
  private readonly heldByTx = new Map<string, Set<number>>()

  private readonly reread: Array<{ b: string; txid: string; vout: number }> = []
  /** Outputs to delete from storage, with the engine's record of their admission. */
  private readonly drops = new Map<string, { b: string; txid: string; vout: number; topic: string; since: number }>()
  /** Later carriers on an outpoint a restore read: decided once every chain is folded. */
  private recheck: string[] = []
  private storage: Storage | undefined
  private restoring = false
  private ready = false
  private inflight: Promise<number> | undefined
  private flushing: Promise<void> | undefined
  private readonly warned = new Set<string>()

  /** The last retention pass's counts, for the gauges. */
  tally = { mapsComplete: 0, mapsIncomplete: 0, chainsBroken: 0, anchored: 0 }

  constructor(o: Options) {
    this.buckets = o.buckets
    this.served = o.served ?? new Set()
    for (const s of this.served) this.servedBuckets.add(s.split('|')[0]!)
    this.host = o.host
    this.journal = o.journal
    this.priced = o.priced ?? new Set()
    const floor = (name: string, v: number | undefined, def: number): number => {
      const x = v ?? def
      if (!Number.isSafeInteger(x) || x < def) throw new Error(`ls_bstore: ${name} of ${x} is under its floor of ${def}`)
      return x
    }
    this.keep = floor('keep days', o.keepDays, FloorKeepDays) * Day
    this.pendingFloor = floor('pending days', o.pendingDays, FloorPendingDays) * Day
    this.other = floor('other hours', o.otherHours, FloorOtherHours) * Hour
    this.headers = o.headers
    this.now = o.now ?? Date.now
    this.servedIndexes()
    for (const l of this.journal.load()) this.apply(l)
  }

  /** An empty index of unnamed entries for each served chain. */
  private servedIndexes(): void {
    this.servedIdx.clear()
    for (const s of this.served) {
      const [b, owner, id] = s.split('|') as [string, string, string]
      this.servedIdx.set(s, { chain: s, b, owner, id, pendingByKey: new Map(), unanchored: new Map() })
    }
  }

  // ---- gauges

  get tokenCount(): number {
    return this.tokenChain.size
  }
  get pendingTokens(): number {
    let n = 0
    for (const v of this.chains.values()) for (const t of v.byTxid.values()) if (!t.rooted) n++
    return n
  }
  count(kind: CarrierKind): number {
    let n = 0
    for (const e of this.carriers.values()) if (e.kind === kind) n++
    return n
  }
  get adCount(): number {
    return this.ads.size
  }
  get holdCount(): number {
    return this.holds.size
  }
  get sweepCount(): number {
    return this.sweeps.size
  }
  get sweepOutpoints(): number {
    return this.table.size
  }
  get markCount(): number {
    return this.marks.size
  }
  get rowCount(): number {
    return this.rows.size
  }
  get pendingDrops(): number {
    return this.drops.size
  }
  /** Whether the index is restored and ready to answer. */
  get restored(): boolean {
    return this.ready
  }

  // ---- the journal

  private apply(l: Line): void {
    switch (l.k) {
      case 'c':
        if (!this.rows.has(l.op)) this.rows.set(l.op, { c: l.c, at: l.at })
        break
      case 'cf':
        this.rows.delete(l.op)
        break
      case 'a':
        if (!this.seen.has(l.c)) this.seen.set(l.c, { b: l.b, at: l.at })
        break
      case 'af':
        this.seen.delete(l.c)
        break
      case 'x':
        if (!this.marks.has(l.op)) this.marks.set(l.op, { key: l.key, at: l.at, life: l.life })
        break
      case 'xl': {
        const m = this.marks.get(l.op)
        if (m !== undefined) m.life = true
        break
      }
      case 'xf':
        this.marks.delete(l.op)
        break
      case 'p':
        if (!this.table.has(l.op)) this.table.set(l.op, { s: l.s, at: l.at })
        break
      case 'pf':
        this.table.delete(l.op)
        break
      case 't':
        if (!this.tokenSeen.has(l.txid)) this.tokenSeen.set(l.txid, { b: l.b, vout: l.vout, at: l.at })
        break
      case 'tf':
        this.tokenSeen.delete(l.txid)
        break
      case 'o': {
        const k = `${l.b}|${l.txid}.${l.vout}`
        if (!this.minedSeen.has(k)) this.minedSeen.set(k, { b: l.b, txid: l.txid, vout: l.vout, at: l.at })
        break
      }
      case 'w':
        this.spentHeld.add(`${l.b}|${l.txid}.${l.vout}`)
        break
      case 'of': {
        const k = `${l.b}|${l.txid}.${l.vout}`
        this.minedSeen.delete(k)
        this.spentHeld.delete(k)
        break
      }
    }
  }

  private write(l: Line): void {
    this.journal.append(l)
    this.apply(l)
  }

  /** The journal rewritten as the state it holds: what the host keeps, and nothing it dropped. */
  compact(): void {
    const out: Line[] = []
    for (const [op, r] of this.rows) out.push({ k: 'c', op, c: r.c, at: r.at })
    for (const [op, m] of this.marks) out.push({ k: 'x', op, key: m.key, at: m.at, life: m.life })
    for (const [op, p] of this.table) out.push({ k: 'p', op, s: p.s, at: p.at })
    for (const [c, s] of this.seen) if (this.carriers.has(c)) out.push({ k: 'a', b: s.b, c, at: s.at })
    for (const [txid, t] of this.tokenSeen) out.push({ k: 't', b: t.b, txid, vout: t.vout, at: t.at })
    for (const [k, o] of this.minedSeen) {
      out.push({ k: 'o', b: o.b, txid: o.txid, vout: o.vout, at: o.at })
      if (this.spentHeld.has(k)) out.push({ k: 'w', b: o.b, txid: o.txid, vout: o.vout })
    }
    this.journal.rewrite(out)
    for (const c of [...this.seen.keys()]) if (!this.carriers.has(c)) this.seen.delete(c)
    for (const k of [...this.spentHeld]) if (!this.minedSeen.has(k)) this.spentHeld.delete(k)
  }

  private warnOnce(key: string, msg: string): void {
    if (this.warned.has(key)) return
    this.warned.add(key)
    this.host.log(msg, {})
  }

  // ---- held outputs and drops

  private holdOutput(b: string, txid: string, vout: number): void {
    addTo(this.heldByTx, `${b}|${txid}`, vout)
  }

  private releaseOutput(b: string, txid: string, vout: number): void {
    removeFrom(this.heldByTx, `${b}|${txid}`, vout)
  }

  /** Queues output vout of txid in bucket b to be deleted, with the engine's record of its transaction once no output of it is held. */
  private queueDrop(b: string, txid: string, vout: number): void {
    this.releaseOutput(b, txid, vout)
    const topic = RecordsPrefix + b
    const k = `${topic}|${txid}.${vout}`
    if (!this.drops.has(k)) this.drops.set(k, { b, txid, vout, topic, since: Date.now() })
  }

  /**
   * Deletes dropped outputs from storage and evicts the engine's record that
   * their transactions were applied once the index holds no output of them,
   * so the same object offered again is decided afresh. A drop decided
   * while the engine admits the object waits until the engine has recorded
   * the admission (it records it after the lookup services are told), for
   * at most SettleMs. A store that cannot delete keeps what was dropped,
   * unanswered; a store that cannot evict leaves a dropped object a
   * duplicate when it is offered again. Each is said once in the log.
   */
  async flush(): Promise<void> {
    while (this.flushing !== undefined) await this.flushing
    this.flushing = this.flushOnce()
    try {
      await this.flushing
    } finally {
      this.flushing = undefined
    }
  }

  private async flushOnce(): Promise<void> {
    const s = this.storage
    if (s === undefined || this.drops.size === 0) return
    if (s.deleteOutput === undefined) {
      this.warnOnce('delete', 'ls_bstore cannot delete dropped outputs: the storage has no deleteOutput')
      this.drops.clear()
      return
    }
    if (s.deleteAppliedTransaction === undefined) this.warnOnce('evict', 'ls_bstore cannot evict a dropped transaction: the storage has no deleteAppliedTransaction')
    for (const [k, d] of [...this.drops]) {
      try {
        // Offered again and held again while the drop waited: nothing to drop.
        if (this.heldByTx.get(`${d.b}|${d.txid}`)?.has(d.vout) === true) {
          this.drops.delete(k)
          continue
        }
        if (s.doesAppliedTransactionExist !== undefined && Date.now() - d.since < SettleMs && !(await s.doesAppliedTransactionExist({ txid: d.txid, topic: d.topic }))) continue
        await s.deleteOutput(d.txid, d.vout, d.topic)
        // Another output of the transaction is still held: its record stays.
        if (!this.heldByTx.has(`${d.b}|${d.txid}`)) await s.deleteAppliedTransaction?.(d.txid, d.topic)
        this.drops.delete(k)
      } catch (e) {
        this.host.log('ls_bstore could not drop an output', { txid: d.txid, err: String(e) })
      }
    }
  }

  // ---- chains (spec section 12.2)

  private chainKey(b: string, owner: string, id: string): string {
    return `${b}|${owner}|${id}`
  }

  private descends(prev: TokenEntry, next: TokenEntry): boolean {
    const spent = next.inputs.filter((op) => op === outpoint(prev.txid, prev.vout))
    return spent.length === 1 && next.prev === prev.txid && prev.seq + 1 === next.seq
  }

  /** The map of a consistent token, folded again from seq 0 along its predecessors. */
  private rebuild(v: ChainIdx, t: TokenEntry): MapState {
    const path: TokenEntry[] = []
    for (let cur: TokenEntry | undefined = t; cur !== undefined; cur = cur.seq === 0 ? undefined : v.byTxid.get(cur.prev)) path.push(cur)
    const m = new MapState()
    for (const x of path.reverse()) {
      const r = m.apply(x.rec)
      if (r !== undefined) throw new Error(`ls_bstore: token ${x.txid} folded consistent once and ${r} now`)
    }
    return m
  }

  private rootFrom(v: ChainIdx, t: TokenEntry, count: boolean): void {
    const work = [t]
    while (work.length > 0) {
      const x = work.pop()!
      if (x.rooted) continue
      let p: TokenEntry | undefined
      if (x.seq === 0) {
        if (!x.first) continue
      } else {
        p = v.byTxid.get(x.prev)
        if (p === undefined || !p.rooted || !this.descends(p, x)) continue
      }
      x.rooted = true
      if (!v.served) {
        x.status = 'rooted'
      } else if (p !== undefined && p.status === 'map-mismatch') {
        // Sticky: everything after a break is broken.
        x.status = 'map-mismatch'
        x.reason = 'after-break'
      } else {
        let m: MapState
        if (p === undefined) {
          m = new MapState()
        } else {
          const held = v.branches.get(p.txid)
          v.branches.delete(p.txid)
          m = held ?? this.rebuild(v, p)
        }
        const r = m.apply(x.rec)
        if (r === undefined) {
          x.status = 'consistent'
          v.branches.set(x.txid, m)
        } else {
          x.status = 'map-mismatch'
          x.reason = r
          // The map stays its predecessor's: it is the map of the last consistent token.
          if (p !== undefined) v.branches.set(p.txid, m)
        }
        if (v.hist !== undefined) this.histAdd(v, x)
      }
      v.top = Math.max(v.top, x.seq)
      v.choice.clear()
      delete v.latest
      if (count) this.counted(v, x)
      for (const [txid, vout] of v.turned.get(x.txid) ?? []) this.reread.push({ b: v.b, txid, vout })
      v.turned.delete(x.txid)
      for (const c of v.children.get(x.txid) ?? []) {
        const child = v.byTxid.get(c)
        if (child !== undefined) work.push(child)
      }
    }
  }

  private counted(v: ChainIdx, t: TokenEntry): void {
    this.host.metrics.inc('bstore_token_status_total', { status: t.status })
    if (t.status === 'map-mismatch') {
      this.host.log('ls_bstore token status', { bucket: v.b, owner: v.owner, id: v.id, seq: t.seq, txid: t.txid, status: t.status, reason: t.reason })
    }
  }

  private reroot(v: ChainIdx): void {
    v.branches.clear()
    v.top = -1
    v.choice.clear()
    delete v.hist
    delete v.latest
    for (const t of v.byTxid.values()) {
      t.rooted = false
      t.status = 'pending'
      t.reason = ''
    }
    for (const t of v.bySeq.get(0) ?? []) this.rootFrom(v, t, false)
  }

  /** At most PerSeq tokens of a chain at one seq: rooted first, then the lowest txids. */
  private bound(v: ChainIdx, seq: number): void {
    const at = v.bySeq.get(seq)
    if (at === undefined || at.length <= PerSeq) return
    const order = [...at].sort((a, b) => Number(b.rooted) - Number(a.rooted) || cmp(a.txid, b.txid))
    let again = false
    for (const t of order.slice(PerSeq)) {
      this.host.log('ls_bstore kept no more tokens at this seq', { bucket: v.b, owner: v.owner, id: v.id, txid: t.txid, seq })
      again = again || t.rooted
      if (!t.rooted && t.prev !== '') {
        let m = v.turned.get(t.prev)
        if (m === undefined) v.turned.set(t.prev, (m = new Map()))
        m.set(t.txid, t.vout)
      }
      this.forgetToken(v, t, false)
    }
    if (again) this.reroot(v)
  }

  /** The rooted, consistent tokens at the highest seq that has one (served), or the rooted ones at the highest seq (any other chain). */
  private latest(v: ChainIdx): TokenEntry[] {
    if (v.latest !== undefined) return v.latest
    v.latest = []
    for (let i = v.seqs.size - 1; i >= 0; i--) {
      const at = (v.bySeq.get(v.seqs.a[i]!) ?? []).filter((t) => t.rooted && (v.served ? t.status === 'consistent' : true))
      if (at.length > 0) {
        v.latest = at
        break
      }
    }
    return v.latest
  }

  /**
   * Of several tokens at one seq, the one an answer uses: the one whose
   * proof verifies against the host's headers, else the lowest txid. The
   * choice is kept for a minute.
   */
  private async choose(v: ChainIdx, seq: number, ts: TokenEntry[]): Promise<TokenEntry | undefined> {
    if (ts.length <= 1) return ts[0]
    const sorted = [...ts].sort((a, b) => cmp(a.txid, b.txid))
    const among = sorted.map((t) => t.txid).join(',')
    const now = Date.now()
    const kept = v.choice.get(seq)
    if (kept !== undefined && kept.among === among && now - kept.at < ChoiceTTL) return sorted.find((t) => t.txid === kept.txid)
    let pick = sorted[0]!
    if (this.headers !== undefined) {
      for (const t of sorted) {
        try {
          if (t.root !== '' && (await this.headers.isValidRootForHeight(t.root, t.height))) {
            pick = t
            break
          }
        } catch {
          // A header source that cannot answer leaves the lowest txid.
        }
      }
    }
    v.choice.set(seq, { among, txid: pick.txid, at: now })
    return pick
  }

  /** The token a served chain's answers are proven against, and its map. */
  private async view(v: ChainIdx): Promise<{ t: TokenEntry; m: MapState } | undefined> {
    const cands = this.latest(v)
    if (cands.length === 0) return undefined
    const t = await this.choose(v, cands[0]!.seq, cands)
    if (t === undefined) return undefined
    let m = v.branches.get(t.txid)
    if (m === undefined) v.branches.set(t.txid, (m = this.rebuild(v, t)))
    return { t, m }
  }

  /** The keys of a served chain's owner, known from configuration, and the writers of its latest consistent tokens. */
  private parties(s: ServedIdx): Set<string> {
    const out = new Set([s.owner])
    const v = this.chains.get(s.chain)
    if (v !== undefined) for (const t of this.latest(v)) for (const w of t.rec.writers) out.add(toHex(w))
    return out
  }

  /** Whether key is a served chain's owner or a writer of its latest consistent token, in bucket b. */
  private isParty(b: string, key: string): boolean {
    for (const s of this.servedOf(b)) if (this.parties(s).has(key)) return true
    return false
  }

  /** The chains bucket b's configuration serves. */
  private servedOf(b: string): ServedIdx[] {
    if (!this.servedBuckets.has(b)) return []
    return [...this.servedIdx.values()].filter((s) => s.b === b)
  }

  /** The served chains of bucket b that the host holds a token of. */
  private *servedChains(b: string): Generator<ChainIdx> {
    for (const s of this.servedOf(b)) {
      const v = this.chains.get(s.chain)
      if (v !== undefined) yield v
    }
  }

  /** Whether a token of a served chain of bucket b names commitment c. */
  private namedByServed(b: string, c: string): boolean {
    for (const v of this.servedChains(b)) if ((v.named.get(c) ?? 0) > 0) return true
    return false
  }

  /** The status of the token with txid (display order), when the host holds it. */
  tokenStatus(txid: string): TokenStatus | undefined {
    const key = this.tokenChain.get(txid)
    return key === undefined ? undefined : this.chains.get(key)?.byTxid.get(txid)?.status
  }

  /** Whether the host holds the carrier with commitment c (hash byte order hex). */
  holdsCarrier(c: string): boolean {
    return this.carriers.has(c)
  }

  /** The retraction mark of outpoint op, when the host keeps one. */
  mark(op: string): { key: string; at: number; life: boolean } | undefined {
    const m = this.marks.get(op)
    return m === undefined ? undefined : { ...m }
  }

  /** Whether the host keeps an admitted sweep's spend of outpoint op. */
  swept(op: string): boolean {
    return this.table.has(op)
  }

  /** The first carrier the host admitted on funding outpoint op. */
  firstOn(op: string): string | undefined {
    return this.rows.get(op)?.c
  }

  // ---- the served chains' indexes of unnamed entries

  private servedFor(b: string, id: string): ServedIdx[] {
    const out: ServedIdx[] = []
    for (const s of this.servedIdx.values()) if (s.b === b && s.id === id) out.push(s)
    return out
  }

  private unnamedAdd(s: ServedIdx, e: CarrierEntry): void {
    let byWriter = s.pendingByKey.get(e.key!)
    if (byWriter === undefined) s.pendingByKey.set(e.key!, (byWriter = new Map()))
    sortedIn(byWriter, e.owner).insert(e.tkey)
    if (this.now() - e.at < UnanchoredDays * Day) sortedIn(s.unanchored, e.owner).insert(e.txid)
  }

  private unnamedRemove(s: ServedIdx, e: CarrierEntry): void {
    const byWriter = s.pendingByKey.get(e.key!)
    if (byWriter !== undefined) {
      sortedOut(byWriter, e.owner, e.tkey)
      if (byWriter.size === 0) s.pendingByKey.delete(e.key!)
    }
    sortedOut(s.unanchored, e.owner, e.txid)
  }

  /** A commitment a served chain's held tokens name one more time, or one fewer. */
  private name(v: ChainIdx, c: string, by: 1 | -1): void {
    const n = (v.named.get(c) ?? 0) + by
    if (n > 0) v.named.set(c, n)
    else v.named.delete(c)
    const e = this.carriers.get(c)
    const s = this.servedIdx.get(v.key)
    if (e === undefined || e.kind !== 'entry' || s === undefined || e.b !== v.b || e.id !== v.id) return
    if (by === 1 && n === 1) this.unnamedRemove(s, e)
    if (by === -1 && n === 0) this.unnamedAdd(s, e)
  }

  private namesOf(rec: Bucket): string[] {
    return [...rec.changes.map((ch) => toHex(ch.c)), ...rec.consumed.map(toHex)]
  }

  // ---- admission

  async outputAdmittedByTopic(payload: OutputAdmittedByTopic): Promise<void> {
    if (payload.mode !== 'whole-tx') return
    const b = this.bucketOf(payload.topic)
    if (b === undefined) return
    let tx: Transaction
    try {
      tx = Transaction.fromAtomicBEEF(payload.atomicBEEF)
    } catch {
      this.host.log('ls_bstore ignored an admission whose BEEF did not parse', { topic: payload.topic })
      return
    }
    while (this.inflight !== undefined) await this.inflight.catch(() => {})
    this.index(b, tx, payload.outputIndex, this.now())
    await this.drain()
    // The engine records the admission after this returns; a drop waits for it.
    setTimeout(() => void this.flush().catch(() => {}), 10).unref()
  }

  private async drain(): Promise<void> {
    const storage = this.storage
    if (storage === undefined) return
    while (this.reread.length > 0) {
      const r = this.reread.shift()!
      if (this.tokenChain.has(r.txid)) continue
      const o = await storage.findOutput(r.txid, r.vout, RecordsPrefix + r.b, undefined, true)
      if (o?.beef === undefined) continue
      try {
        this.index(r.b, storedToken(Uint8Array.from(o.beef)), r.vout, this.tokenSeen.get(r.txid)?.at ?? this.now())
      } catch (e) {
        this.host.log('ls_bstore could not read a token again', { txid: r.txid, err: String(e) })
      }
    }
  }

  outputEvicted(txid: string, outputIndex: number): void {
    this.forget(txid, outputIndex)
  }

  outputNoLongerRetainedInHistory(txid: string, outputIndex: number, t: string): void {
    const b = this.bucketOf(t)
    if (b !== undefined) this.forget(txid, outputIndex, b)
  }

  private bucketOf(t: string): string | undefined {
    if (!t.startsWith(RecordsPrefix)) return undefined
    const b = t.slice(RecordsPrefix.length)
    return this.buckets.has(b) ? b : undefined
  }

  /** Indexes output vout of an admitted transaction, first seen at `at`; returns whether it holds it now. */
  private index(b: string, tx: Transaction, vout: number, at: number): boolean {
    const out = tx.outputs[vout]
    if (out === undefined) return false
    this.spends(b, tx)
    const script = bin(out.lockingScript)
    const claim = classify(script)
    if (claim === 'entry' || claim === 'outcome' || claim === 'front') return this.indexCarrier(b, tx, claim, script, at)
    if (claim === 'bucket-token') return this.indexToken(b, tx, vout, at)
    if (claim === 'hold') return this.indexHold(b, tx, vout, at)
    if (claim === 'funding' && vout === 0) return this.indexSweep(b, tx, at)
    try {
      return this.indexAd(b, tx, vout, decodeAdvertisement(script), at)
    } catch {
      this.host.log('ls_bstore indexed an output of no known kind', { bucket: b, txid: tx.id('hex'), vout })
      return false
    }
  }

  /** Records what an admitted transaction spends among the outputs the index holds: an advertisement or hold output spent, a sweep's output spent. */
  private spends(b: string, tx: Transaction): void {
    for (const i of tx.inputs) {
      const src = srcTxid(i)
      if (src === undefined) continue
      const k = `${b}|${src}.${i.sourceOutputIndex}`
      if (this.spentHeld.has(k)) continue
      const ad = this.ads.get(k)
      if (ad !== undefined) {
        ad.spent = true
        this.adLive(ad, false)
      }
      const h = this.holds.get(k)
      if (h !== undefined) h.spent = true
      if (ad !== undefined || h !== undefined || (i.sourceOutputIndex === 0 && this.sweeps.has(`${b}|${src}`))) this.write({ k: 'w', b, txid: src, vout: i.sourceOutputIndex })
    }
  }

  private indexCarrier(b: string, tx: Transaction, kind: CarrierKind, script: Uint8Array, at: number): boolean {
    const input = tx.inputs[0]
    const src = input === undefined ? undefined : srcTxid(input)
    if (input === undefined || src === undefined) return false
    const txid = tx.id('hex')
    const c = cOf(txid)
    if (this.carriers.has(c)) return true
    const raw = firstPush(script)!
    const op = outpoint(src, input.sourceOutputIndex)
    let e: CarrierEntry
    try {
      e = this.carrierOf(b, kind, raw, txid, c, op, this.seen.get(c)?.at ?? at)
    } catch (err) {
      this.host.log('ls_bstore could not read a carrier', { bucket: b, txid, err: String(err) })
      return false
    }
    // A sweep that arrived before the carrier: the outpoint is marked now.
    const early = this.table.get(op)
    if (early !== undefined && !this.marks.has(op)) this.write({ k: 'x', op, key: e.owner, at: early.at, life: this.isParty(b, e.owner) })
    // Retraction wins over every floor: a carrier on a marked outpoint is retracted whenever it is offered.
    const mark = this.marks.get(op)
    if (mark !== undefined) {
      if (!mark.life && this.isParty(b, e.owner)) this.write({ k: 'xl', op })
      this.queueDrop(b, txid, 0)
      this.host.metrics.inc('bstore_dropped_total', { why: 'retracted' })
      return false
    }
    const row = this.rows.get(op)
    if (row === undefined) {
      this.write({ k: 'c', op, c, at: e.at })
    } else if (row.c !== c) {
      // A later carrier on the outpoint has no floor unless a served chain names it.
      if (this.restoring) {
        this.recheck.push(c)
      } else if (!this.namedByServed(b, c)) {
        this.queueDrop(b, txid, 0)
        this.host.metrics.inc('bstore_dropped_total', { why: 'second-carrier' })
        return false
      }
    }
    if (!this.seen.has(c)) this.write({ k: 'a', b, c, at: e.at })
    this.holdCarrier(e)
    return true
  }

  /** A carrier's index entry, read from its record. */
  private carrierOf(b: string, kind: CarrierKind, raw: Uint8Array, txid: string, c: string, op: string, at: number): CarrierEntry {
    if (kind === 'entry') {
      const r = decodeEntry(raw)
      const e: CarrierEntry = { c, txid, b, kind, op, at, owner: toHex(r.writer), created: r.created, tkey: timeKey(r.created, txid), id: toHex(r.id), key: r.key, ekind: r.kind }
      if (r.kind === 1) e.root = toHex(r.root)
      if (r.hold !== undefined) e.hold = outpoint(displayTxid(r.hold.txid), r.hold.vout)
      return e
    }
    if (kind === 'outcome') {
      const r = decodeOutcome(raw)
      return { c, txid, b, kind, op, at, owner: toHex(r.buyer), created: r.created, tkey: timeKey(r.created, txid), answerer: toHex(r.req.answerer), provider: toHex(r.req.provider), oroot: toHex(r.req.root) }
    }
    const r = decodeFront(raw)
    return { c, txid, b, kind, op, at, owner: toHex(r.front), created: r.created, tkey: timeKey(r.created, txid), advertiser: toHex(r.advertiser) }
  }

  private holdCarrier(e: CarrierEntry): void {
    this.carriers.set(e.c, e)
    addTo(this.byOp, e.op, e.c)
    this.holdOutput(e.b, e.txid, 0)
    if (e.kind === 'entry') {
      for (const s of this.servedFor(e.b, e.id!)) {
        const v = this.chains.get(s.chain)
        if ((v?.named.get(e.c) ?? 0) === 0) this.unnamedAdd(s, e)
      }
    } else if (e.kind === 'outcome') {
      const free = this.now() - e.at < OutcomeWindowDays * Day
      for (const k of this.outcomeKeys(e)) {
        sortedIn(this.outcomeHistory, k).insert(e.tkey)
        if (free) sortedIn(this.outcomesBy, k).insert(e.tkey)
      }
      for (const k of this.outcomeBuyerKeys(e)) if (free) sortedIn(this.outcomesBy, k).insert(e.tkey)
    } else {
      sortedIn(this.frontsByAdvertiser, `${e.b}|${e.advertiser}`).insert(e.txid)
      sortedIn(this.frontsByPair, `${e.b}|${e.advertiser}|${e.owner}`).insert(e.tkey)
    }
  }

  private outcomeKeys(e: CarrierEntry): string[] {
    return [`${e.b}|a|${e.answerer}`, `${e.b}|p|${e.provider}`]
  }

  private outcomeBuyerKeys(e: CarrierEntry): string[] {
    return [`${e.b}|a|${e.answerer}|${e.owner}`, `${e.b}|p|${e.provider}|${e.owner}`]
  }

  private forgetCarrier(e: CarrierEntry): void {
    this.carriers.delete(e.c)
    removeFrom(this.byOp, e.op, e.c)
    this.releaseOutput(e.b, e.txid, 0)
    if (e.kind === 'entry') {
      for (const s of this.servedFor(e.b, e.id!)) this.unnamedRemove(s, e)
    } else if (e.kind === 'outcome') {
      for (const k of this.outcomeKeys(e)) {
        sortedOut(this.outcomeHistory, k, e.tkey)
        sortedOut(this.outcomesBy, k, e.tkey)
      }
      for (const k of this.outcomeBuyerKeys(e)) sortedOut(this.outcomesBy, k, e.tkey)
    } else {
      sortedOut(this.frontsByAdvertiser, `${e.b}|${e.advertiser}`, e.txid)
      sortedOut(this.frontsByPair, `${e.b}|${e.advertiser}|${e.owner}`, e.tkey)
    }
  }

  /** Drops a held carrier: out of every answer, its output deleted and its admission evicted. */
  private drop(e: CarrierEntry, why: (typeof DropReasons)[number]): void {
    this.forgetCarrier(e)
    if (this.seen.has(e.c)) this.write({ k: 'af', c: e.c })
    this.queueDrop(e.b, e.txid, 0)
    this.host.metrics.inc('bstore_dropped_total', { why })
    this.host.log('ls_bstore dropped a carrier', { txid: e.txid, outpoint: e.op, kind: e.kind, why })
  }

  /** Retracts a held carrier: its outpoint is marked, and the carrier goes. */
  private retract(e: CarrierEntry, sweepAt: number): void {
    if (!this.marks.has(e.op)) this.write({ k: 'x', op: e.op, key: e.owner, at: sweepAt, life: this.isParty(e.b, e.owner) })
    this.host.metrics.inc('bstore_retractions_total')
    this.drop(e, 'retracted')
  }

  private indexToken(b: string, tx: Transaction, vout: number, at: number): boolean {
    const out = tx.outputs[vout]!
    const to = readBucketOutput(bin(out.lockingScript), out.satoshis, vout)
    const txid = tx.id('hex')
    if (to === undefined || to.bucket.bucket !== b) {
      this.host.log('ls_bstore could not read a token', { bucket: b, txid, vout })
      return false
    }
    const rec = to.bucket
    const owner = toHex(rec.owner)
    const id = toHex(rec.id)
    const key = this.chainKey(b, owner, id)
    let v = this.chains.get(key)
    if (v === undefined) {
      v = { key, b, owner, id, served: this.served.has(key), bySeq: new Map(), seqs: new SortedNumbers(), byTxid: new Map(), children: new Map(), branches: new Map(), top: -1, turned: new Map(), choice: new Map(), named: new Map() }
      this.chains.set(key, v)
    }
    if (!this.tokenSeen.has(txid)) this.write({ k: 't', b, txid, vout, at })
    if (v.byTxid.has(txid)) return true
    const inputs: string[] = []
    let first = false
    for (const i of tx.inputs) {
      const src = srcTxid(i)
      if (src === undefined) continue
      inputs.push(outpoint(src, i.sourceOutputIndex))
      if (rec.seq === 0 && equal(bucketID(hashOrder(src), i.sourceOutputIndex), rec.id)) first = true
    }
    const { height, root } = place(tx)
    const e: TokenEntry = { txid, vout, b, chain: key, seq: rec.seq, prev: rec.seq === 0 ? '' : displayTxid(rec.prev), rec, inputs, first, height, root, rooted: false, status: 'pending', reason: '', at: this.tokenSeen.get(txid)?.at ?? at }
    v.bySeq.set(rec.seq, [...(v.bySeq.get(rec.seq) ?? []), e].sort((a, b) => cmp(a.txid, b.txid)))
    v.seqs.insert(rec.seq)
    v.byTxid.set(txid, e)
    this.tokenChain.set(txid, key)
    this.holdOutput(b, txid, vout)
    if (e.prev !== '') addTo(v.children, e.prev, txid)
    if (v.served) for (const c of this.namesOf(rec)) this.name(v, c, 1)
    if (this.restoring) return true
    this.rootFrom(v, e, true)
    if (!e.rooted) this.counted(v, e)
    this.bound(v, rec.seq)
    return v.byTxid.has(txid)
  }

  private forgetToken(v: ChainIdx, t: TokenEntry, reroot = true): void {
    delete v.latest
    v.byTxid.delete(t.txid)
    this.tokenChain.delete(t.txid)
    this.releaseOutput(v.b, t.txid, t.vout)
    const list = (v.bySeq.get(t.seq) ?? []).filter((x) => x.txid !== t.txid)
    if (list.length === 0) {
      v.bySeq.delete(t.seq)
      v.seqs.remove(t.seq)
    } else {
      v.bySeq.set(t.seq, list)
    }
    if (t.prev !== '') removeFrom(v.children, t.prev, t.txid)
    if (v.served) for (const c of this.namesOf(t.rec)) this.name(v, c, -1)
    if (reroot && t.rooted) this.reroot(v)
  }

  /** Registers a mined output of bucket b (an advertisement, a hold output, a sweep's output 0), first seen at `at`. */
  private minedOutput(b: string, txid: string, vout: number, at: number): number {
    const k = `${b}|${txid}.${vout}`
    const seen = this.minedSeen.get(k)
    if (seen !== undefined) return seen.at
    this.write({ k: 'o', b, txid, vout, at })
    return at
  }

  private indexHold(b: string, tx: Transaction, vout: number, at: number): boolean {
    const txid = tx.id('hex')
    const k = `${b}|${txid}.${vout}`
    if (this.holds.has(k)) return true
    this.holds.set(k, { b, txid, vout, at: this.minedOutput(b, txid, vout, at), spent: this.spentHeld.has(k) })
    this.holdOutput(b, txid, vout)
    return true
  }

  private indexAd(b: string, tx: Transaction, vout: number, ad: ReturnType<typeof decodeAdvertisement>, at: number): boolean {
    const txid = tx.id('hex')
    const k = `${b}|${txid}.${vout}`
    if (this.ads.has(k)) return true
    const e: AdEntry = { b, txid, vout, root: toHex(ad.hash), advertiser: toHex(ad.host), expiry: ad.expiry, at: this.minedOutput(b, txid, vout, at), spent: this.spentHeld.has(k), anyone: equal(ad.host, AnyoneKey) }
    this.ads.set(k, e)
    this.holdOutput(b, txid, vout)
    this.adLive(e, true)
    return true
  }

  /** Puts an advertisement in the indexes `ads` and `ad` answer from, or takes it out: only one unspent, not of the anyone key, unexpired at the last look. */
  private adLive(e: AdEntry, live: boolean): void {
    const k = outKey(e.txid, e.vout)
    if (live && !e.spent && !e.anyone && e.expiry * 1000 > this.now()) {
      sortedIn(this.adsLive, `${e.b}|${e.root}`).insert(k)
      sortedIn(this.adsLive, `${e.b}|${e.root}|${e.advertiser}`).insert(k)
      return
    }
    sortedOut(this.adsLive, `${e.b}|${e.root}`, k)
    sortedOut(this.adsLive, `${e.b}|${e.root}|${e.advertiser}`, k)
  }

  /**
   * A sweep (spec sections 12.1 and 12.4): its input outpoints are kept,
   * once per host whatever bucket admitted it, and every carrier the host
   * holds on one of them is retracted. The sweep's own transaction is kept
   * only where an input matched a carrier the host held; otherwise it is
   * dropped at once, and its outpoints still retract a carrier that comes
   * later.
   */
  private indexSweep(b: string, tx: Transaction, at: number): boolean {
    const txid = tx.id('hex')
    const k = `${b}|${txid}`
    if (this.sweeps.has(k)) return true
    const spent: string[] = []
    let matched = false
    for (const input of tx.inputs) {
      const src = srcTxid(input)
      if (src === undefined) continue
      const op = outpoint(src, input.sourceOutputIndex)
      spent.push(op)
      if (!this.table.has(op)) this.write({ k: 'p', op, s: txid, at })
      const on = this.byOp.get(op)
      if (on === undefined) continue
      matched = true
      for (const c of [...on]) this.retract(this.carriers.get(c)!, this.table.get(op)!.at)
    }
    // A restore keeps what storage kept: a sweep is in storage only because it matched.
    if (!matched && !this.restoring) {
      this.queueDrop(b, txid, 0)
      this.host.metrics.inc('bstore_dropped_total', { why: 'unmatched-sweep' })
      return false
    }
    const { height, offset } = place(tx)
    this.sweeps.set(k, { b, txid, spent, height, offset, at: this.minedOutput(b, txid, 0, at) })
    for (const op of spent) addTo(this.sweepsByOp, `${b}|${op}`, txid)
    this.holdOutput(b, txid, 0)
    return true
  }

  private forgetSweep(s: SweepEntry): void {
    this.sweeps.delete(`${s.b}|${s.txid}`)
    for (const op of s.spent) removeFrom(this.sweepsByOp, `${s.b}|${op}`, s.txid)
    this.releaseOutput(s.b, s.txid, 0)
  }

  /** Removes a held output from the index, in one bucket or in all; the journal's rows, marks and outpoints stay. */
  private forget(txid: string, vout: number, b?: string): void {
    for (const bk of b === undefined ? [...this.buckets] : [b]) {
      const k = `${bk}|${txid}.${vout}`
      const ad = this.ads.get(k)
      if (ad !== undefined) {
        this.adLive(ad, false)
        this.ads.delete(k)
        this.releaseOutput(bk, txid, vout)
      }
      if (this.holds.delete(k)) this.releaseOutput(bk, txid, vout)
      if (vout === 0) {
        const s = this.sweeps.get(`${bk}|${txid}`)
        if (s !== undefined) this.forgetSweep(s)
      }
    }
    if (vout === 0) {
      const e = this.carriers.get(cOf(txid))
      if (e !== undefined && (b === undefined || e.b === b)) this.forgetCarrier(e)
    }
    const key = this.tokenChain.get(txid)
    const v = key === undefined ? undefined : this.chains.get(key)
    const t = v?.byTxid.get(txid)
    if (v !== undefined && t !== undefined && t.vout === vout && (b === undefined || v.b === b)) this.forgetToken(v, t)
  }

  // ---- retention (spec section 12.3)

  /** The paths of a served chain's latest consistent tokens, replayed for when each named commitment left the map. */
  private leftTimes(v: ChainIdx, into: Map<string, number>): void {
    for (const tip of this.latest(v)) {
      const path: TokenEntry[] = []
      for (let cur: TokenEntry | undefined = tip; cur !== undefined; cur = cur.seq === 0 ? undefined : v.byTxid.get(cur.prev)) path.push(cur)
      const map = new Map<string, string>()
      const leave = (c: string, at: number): void => {
        into.set(c, Math.max(into.get(c) ?? 0, at))
      }
      for (const t of path.reverse()) {
        for (const ch of t.rec.changes) {
          const old = map.get(ch.key)
          if (old !== undefined) leave(old, t.at)
          if (ch.removal) {
            map.delete(ch.key)
            leave(toHex(ch.c), t.at)
          } else {
            map.set(ch.key, toHex(ch.c))
          }
        }
        for (const c of t.rec.consumed) leave(toHex(c), t.at)
      }
    }
  }

  /** What retention reads of bucket b's served chains. */
  private servedView(b: string): ServedView {
    const sv: ServedView = { parties: new Set(), anchored: new Set(), left: new Map(), roots: new Set(), holds: new Set(), fronts: new Set() }
    for (const s of this.servedOf(b)) for (const p of this.parties(s)) sv.parties.add(p)
    for (const v of this.servedChains(b)) {
      for (const t of this.latest(v)) {
        let m = v.branches.get(t.txid)
        if (m === undefined) v.branches.set(t.txid, (m = this.rebuild(v, t)))
        for (const c of m.commitments()) sv.anchored.add(toHex(c))
      }
      this.leftTimes(v, sv.left)
    }
    for (const c of sv.anchored) sv.left.delete(c)
    for (const e of this.carriers.values()) {
      if (e.b !== b || e.kind !== 'entry') continue
      if (sv.anchored.has(e.c)) {
        if (e.root !== undefined) sv.roots.add(e.root)
        if (e.hold !== undefined) sv.holds.add(e.hold)
      } else if (e.hold !== undefined && sv.parties.has(e.owner) && !this.namedByServed(b, e.c)) {
        sv.holds.add(e.hold)
      }
    }
    // The first 16 front statements, by first sight, of each advertiser of a root the maps name.
    const advertisers = new Set<string>()
    for (const ad of this.ads.values()) if (ad.b === b && !ad.anyone && sv.roots.has(ad.root)) advertisers.add(ad.advertiser)
    for (const a of advertisers) {
      const fs = [...(this.frontsByAdvertiser.get(`${b}|${a}`)?.a ?? [])].map((txid) => this.carriers.get(cOf(txid))!).filter((e) => this.rows.get(e.op)?.c === e.c)
      fs.sort((x, y) => x.at - y.at || cmp(x.txid, y.txid))
      for (const e of fs.slice(0, FrontsPerAdvertiser)) sv.fronts.add(e.c)
    }
    return sv
  }

  /** When a carrier's floor ends, Unix milliseconds; it is dropped at or after it. */
  private floorOf(e: CarrierEntry, sv: ServedView): number {
    const first = this.rows.get(e.op)?.c === e.c
    const named = this.namedByServed(e.b, e.c)
    // A later carrier on an outpoint has no floor unless a served chain names it.
    if (!first && !named) return e.at
    if (e.kind === 'entry') {
      if (sv.anchored.has(e.c)) return Infinity
      const left = sv.left.get(e.c)
      if (left !== undefined) return Math.max(left + this.keep, e.at + this.other)
      // Named only by tokens off the latest branch, or not rooted: kept as an unnamed one is.
      if (named || sv.parties.has(e.owner)) return e.at + this.pendingFloor
      return e.at + this.other
    }
    if (e.kind === 'outcome') return sv.parties.has(e.owner) && sv.roots.has(e.oroot!) ? e.at + this.keep : e.at + this.other
    return sv.fronts.has(e.c) ? e.at + this.keep : e.at + this.other
  }

  /**
   * Applies retention at the host's time (spec section 12.3), never against
   * a restore in flight: drops every carrier, token, advertisement, hold
   * output and sweep whose floor has passed, forgets the outpoints, marks
   * and rows whose floor has passed, ages the windows `unanchored`,
   * `outcomes` and `ads` answer from, and rewrites the journal. Returns how
   * many objects were dropped.
   */
  async prune(): Promise<number> {
    while (this.inflight !== undefined) await this.inflight.catch(() => {})
    if (!this.ready) return 0
    const now = this.now()
    const views = new Map<string, ServedView>()
    const sv = (b: string): ServedView => {
      let x = views.get(b)
      if (x === undefined) views.set(b, (x = this.servedView(b)))
      return x
    }
    let dropped = 0
    for (const e of [...this.carriers.values()]) {
      const mark = this.marks.get(e.op)
      const swept = this.table.get(e.op)
      if (mark !== undefined || swept !== undefined) {
        this.retract(e, mark?.at ?? swept!.at)
        dropped++
        continue
      }
      if (now >= this.floorOf(e, sv(e.b))) {
        this.drop(e, 'retention')
        dropped++
      }
    }
    for (const v of [...this.chains.values()]) {
      // A served chain's tokens are kept for the host's life, those a full seq turned away included.
      if (v.served) continue
      let rooted = false
      for (const t of [...v.byTxid.values()]) {
        if (now - t.at < this.other) continue
        rooted ||= t.rooted
        this.forgetToken(v, t, false)
        this.queueDrop(v.b, t.txid, t.vout)
        this.write({ k: 'tf', txid: t.txid })
        this.host.metrics.inc('bstore_dropped_total', { why: 'retention' })
        dropped++
      }
      if (rooted) this.reroot(v)
      // Tokens a full seq turned away are still in storage: they go by the same rule.
      for (const [prev, m] of [...v.turned]) {
        for (const [txid, vout] of [...m]) {
          const seen = this.tokenSeen.get(txid)?.at ?? now
          if (now - seen < this.other) continue
          this.queueDrop(v.b, txid, vout)
          m.delete(txid)
          if (this.tokenSeen.has(txid)) this.write({ k: 'tf', txid })
          this.host.metrics.inc('bstore_dropped_total', { why: 'retention' })
        }
        if (m.size === 0) v.turned.delete(prev)
      }
      if (v.byTxid.size === 0 && v.turned.size === 0) this.chains.delete(v.key)
    }
    for (const ad of [...this.ads.values()]) {
      const until = !ad.anyone && sv(ad.b).roots.has(ad.root) ? Math.min(ad.expiry * 1000, ad.at + this.keep) + this.other : ad.at + this.other
      if (now >= until) {
        this.dropMined(ad.b, ad.txid, ad.vout)
        dropped++
      } else if (ad.expiry * 1000 <= now) {
        this.adLive(ad, false)
      }
    }
    for (const h of [...this.holds.values()]) {
      const until = sv(h.b).holds.has(outpoint(h.txid, h.vout)) ? h.at + this.keep : h.at + this.other
      if (now >= until) {
        this.dropMined(h.b, h.txid, h.vout)
        dropped++
      }
    }
    for (const s of [...this.sweeps.values()]) {
      if (now - s.at < this.keep) continue
      this.forgetSweep(s)
      this.queueDrop(s.b, s.txid, 0)
      this.write({ k: 'of', b: s.b, txid: s.txid, vout: 0 })
      this.host.metrics.inc('bstore_dropped_total', { why: 'retention' })
      dropped++
    }
    for (const [op, p] of [...this.table]) if (now - p.at >= this.keep) this.write({ k: 'pf', op })
    for (const [op, m] of [...this.marks]) {
      if (m.life) continue
      // A mark is kept for the host's life once its carrier's owner is a served chain's party.
      let party = false
      for (const b of this.servedBuckets) party ||= sv(b).parties.has(m.key)
      if (party) this.write({ k: 'xl', op })
      else if (now - m.at >= this.keep) this.write({ k: 'xf', op })
    }
    for (const [op, r] of [...this.rows]) if ((this.byOp.get(op)?.size ?? 0) === 0 && now - r.at >= this.other) this.write({ k: 'cf', op })
    // The windows the free classes answer from.
    for (const s of this.servedIdx.values()) {
      for (const [w, list] of [...s.unanchored]) {
        for (const txid of [...list.a]) {
          const e = this.carriers.get(cOf(txid))
          if (e === undefined || now - e.at >= UnanchoredDays * Day) sortedOut(s.unanchored, w, txid)
        }
      }
    }
    for (const e of this.carriers.values()) {
      if (e.kind !== 'outcome' || now - e.at < OutcomeWindowDays * Day) continue
      for (const k of [...this.outcomeKeys(e), ...this.outcomeBuyerKeys(e)]) sortedOut(this.outcomesBy, k, e.tkey)
    }
    this.countMaps(views)
    this.compact()
    await this.flush()
    return dropped
  }

  private dropMined(b: string, txid: string, vout: number): void {
    this.forget(txid, vout, b)
    this.queueDrop(b, txid, vout)
    this.write({ k: 'of', b, txid, vout })
    this.host.metrics.inc('bstore_dropped_total', { why: 'retention' })
  }

  /** Counts the served maps the host holds whole, and the broken chains, for the gauges; logs each served map that is incomplete or broken. */
  private countMaps(views: Map<string, ServedView>): void {
    const t = { mapsComplete: 0, mapsIncomplete: 0, chainsBroken: 0, anchored: 0 }
    for (const b of this.servedBuckets) {
      for (const v of this.servedChains(b)) {
        if ([...v.byTxid.values()].some((x) => x.status === 'map-mismatch')) {
          t.chainsBroken++
          this.host.log('ls_bstore served chain is broken', { bucket: v.b, owner: v.owner, id: v.id })
        }
        const sv = views.get(b) ?? this.servedView(b)
        let missing = 0
        for (const c of sv.anchored) {
          t.anchored++
          if (!this.carriers.has(c)) missing++
        }
        if (missing === 0) t.mapsComplete++
        else {
          t.mapsIncomplete++
          this.host.log('ls_bstore served map is incomplete', { bucket: v.b, owner: v.owner, id: v.id, missing })
        }
      }
    }
    this.tally = t
  }

  // ---- questions (spec section 11)

  async lookup(question: LookupQuestion): Promise<LookupFormula> {
    return await this.answer(question, false)
  }

  /** The class of a question, validated, and whether this host prices it. */
  classify(question: LookupQuestion): { q: Question; priced: boolean } {
    if (question.service !== ServiceName) throw new Error(`ls_bstore: this service is ${ServiceName}, not ${String(question.service)}`)
    let raw: string | undefined
    try {
      raw = JSON.stringify(question.query)
    } catch {
      raw = undefined
    }
    let q: Question
    try {
      q = parseQuery(raw ?? '', (b) => this.buckets.has(b), (b, o, i) => this.served.has(this.chainKey(b, o, i)))
    } catch (e) {
      if (e instanceof StoreError) throw new Error(`ls_bstore: refused: ${e.message}`)
      throw e
    }
    const free = Classes.find((c) => c.name === q.class)!.free
    return { q, priced: !free && this.priced.has(q.class) }
  }

  /** Whether the host holds the entry with commitment c in bucket b, so a map answer can carry it. */
  private heldEntry(b: string, c: Uint8Array): boolean {
    const e = this.carriers.get(toHex(c))
    return e !== undefined && e.kind === 'entry' && e.b === b
  }

  /**
   * Answers a question. paid says the question was paid for (or asked where
   * no payment is due); a class this host prices is refused otherwise.
   */
  async answer(question: LookupQuestion, paid: boolean): Promise<LookupFormula> {
    const { q, priced } = this.classify(question)
    if (priced && !paid) throw new Error(`ls_bstore: ${q.class} is priced on this host; ask its terms route's /lookup over BRC-104`)
    if (!this.ready) throw new Error('ls_bstore: the index is not restored yet')
    this.host.metrics.inc('bstore_lookups_total', { class: q.class })
    const m = q.values
    const b = m['bucket']!
    const nowMs = this.now()
    const notAfter = timeKey(nowMs + 1, '')
    const car = (e: CarrierEntry): LookupFormula[number] => ({ txid: e.txid, outputIndex: 0 })
    const tok = (t: TokenEntry): LookupFormula[number] => ({ txid: t.txid, outputIndex: t.vout })
    const chainOf = (owner: string, id: string): ChainIdx | undefined => this.chains.get(this.chainKey(b, owner, id))
    switch (q.class) {
      case 'bucket': {
        const v = chainOf(m['owner']!, m['id']!)
        return v === undefined ? [] : this.latest(v).slice(0, PerSeq).map(tok)
      }
      case 'bucket-seq': {
        const v = chainOf(m['owner']!, m['id']!)
        const at = [...(v?.bySeq.get(Number(m['seq'])) ?? [])].sort((x, y) => Number(y.rooted) - Number(x.rooted) || cmp(x.txid, y.txid))
        return at.slice(0, PerSeq).map(tok)
      }
      case 'object': {
        const v = chainOf(m['owner']!, m['id']!)
        const w = v === undefined ? undefined : await this.view(v)
        if (w === undefined) return []
        const [ctx, leaves] = proveObjectFast(w.m, m['key']!, (c) => this.heldEntry(b, c))
        const out: LookupFormula = [{ ...tok(w.t), context: Array.from(encodeTokenContext(leaves)) }]
        if (ctx !== undefined) {
          const e = this.carriers.get(toHex(w.m.get(m['key']!)!))!
          out.push({ ...car(e), context: Array.from(encodeEntryContext(ctx)) })
        }
        return out
      }
      case 'list':
      case 'export': {
        const v = chainOf(m['owner']!, q.class === 'list' ? m['id']! : m['export']!)
        const w = v === undefined ? undefined : await this.view(v)
        if (w === undefined) return []
        const [entries, leaves] = provePageFast(w.m, q.class === 'list' ? m['prefix']! : '', m['after']!, q.class === 'list' ? PageList : PageExport, (c) => this.heldEntry(b, c))
        return [{ ...tok(w.t), context: Array.from(encodeTokenContext(leaves as MapLeaf[])) }, ...entries.map((x) => ({ ...car(this.carriers.get(toHex(x.c))!), context: Array.from(encodeEntryContext(x.ctx)) }))]
      }
      case 'pending': {
        const v = chainOf(m['owner']!, m['id']!)
        const s = this.servedIdx.get(this.chainKey(b, m['owner']!, m['id']!))
        if (v === undefined || s === undefined) return []
        const w = await this.view(v)
        const who = new Set([v.owner, ...(w?.t.rec.writers ?? []).map(toHex)])
        const found: string[] = []
        const byWriter = s.pendingByKey.get(m['pending']!)
        for (const writer of who) {
          const list = byWriter?.get(writer)
          if (list === undefined) continue
          for (let i = list.lower(notAfter) - 1, n = 0; i >= 0 && n < PerSmall; i--, n++) found.push(list.a[i]!)
        }
        return found.sort().reverse().slice(0, PerSmall).map((k) => car(this.carriers.get(cOf(txidOfKey(k)))!))
      }
      case 'unanchored': {
        const s = this.servedIdx.get(this.chainKey(b, m['owner']!, m['id']!))
        const list = s?.unanchored.get(m['writer']!)
        if (list === undefined) return []
        const out: LookupFormula = []
        const since = nowMs - UnanchoredDays * Day
        for (let i = m['after'] === '' ? 0 : list.upper(m['after']!); i < list.size && out.length < PageLimit; i++) {
          const e = this.carriers.get(cOf(list.a[i]!))
          if (e !== undefined && e.at >= since) out.push(car(e))
        }
        return out
      }
      case 'entry': {
        const e = this.carriers.get(cOf(m['entry']!))
        return e === undefined || e.kind !== 'entry' || e.b !== b ? [] : [car(e)]
      }
      case 'ads':
      case 'ad': {
        const list = this.adsLive.get(q.class === 'ads' ? `${b}|${m['root']}` : `${b}|${m['root']}|${m['advertiser']}`)
        if (list === undefined) return []
        const cap = q.class === 'ads' ? PageLimit : PerSmall
        const out: LookupFormula = []
        let i = list.lower(afterTxid(q.class === 'ads' ? m['after']! : ''))
        while (i < list.size) {
          // A page never splits a transaction: the next page asks strictly after its txid.
          const txid = list.a[i]!.slice(0, 64)
          const group: LookupFormula = []
          for (; i < list.size && list.a[i]!.startsWith(`${txid}:`); i++) {
            const vout = Number(list.a[i]!.slice(65))
            const ad = this.ads.get(`${b}|${txid}.${vout}`)
            if (ad !== undefined && !ad.spent && ad.expiry * 1000 > nowMs) group.push({ txid, outputIndex: vout })
          }
          // `ad` has no cursor and no next page: it answers the first eight, a transaction split or not.
          if (out.length + group.length > cap) {
            if (q.class === 'ad') out.push(...group.slice(0, cap - out.length))
            break
          }
          out.push(...group)
        }
        return out
      }
      case 'fronts': {
        const list = this.frontsByAdvertiser.get(`${b}|${m['advertiser']}`)
        if (list === undefined) return []
        const out: LookupFormula = []
        for (let i = m['after'] === '' ? 0 : list.upper(m['after']!); i < list.size && out.length < PerSmall; i++) out.push({ txid: list.a[i]!, outputIndex: 0 })
        return out
      }
      case 'front':
        return this.newest(this.frontsByPair.get(`${b}|${m['advertiser']}|${m['front']}`), notAfter, PerSmall).map((txid) => ({ txid, outputIndex: 0 }))
      case 'hold': {
        const h = this.holds.get(`${b}|${m['hold']}`)
        return h === undefined ? [] : [{ txid: h.txid, outputIndex: h.vout }]
      }
      case 'outcomes':
        return this.newest(this.outcomesBy.get(`${b}|a|${m['answerer']}`), notAfter, PageLimit, nowMs).map((txid) => ({ txid, outputIndex: 0 }))
      case 'outcomes-of':
        return this.newest(this.outcomesBy.get(`${b}|p|${m['provider']}`), notAfter, PageLimit, nowMs).map((txid) => ({ txid, outputIndex: 0 }))
      case 'outcomes-by':
        return this.newest(this.outcomesBy.get(`${b}|a|${m['answerer']}|${m['buyer']}`), notAfter, PageLimit, nowMs).map((txid) => ({ txid, outputIndex: 0 }))
      case 'outcomes-of-by':
        return this.newest(this.outcomesBy.get(`${b}|p|${m['provider']}|${m['buyer']}`), notAfter, PageLimit, nowMs).map((txid) => ({ txid, outputIndex: 0 }))
      case 'outcome-history':
      case 'outcome-history-of': {
        const cursor = keyOfCursor(m['before']!)
        const list = this.outcomeHistory.get(q.class === 'outcome-history' ? `${b}|a|${m['outcome-history']}` : `${b}|p|${m['outcome-history-of']}`)
        return this.newest(list, cursor < notAfter ? cursor : notAfter, PageLimit).map((txid) => ({ txid, outputIndex: 0 }))
      }
      case 'sweep':
        return [...(this.sweepsByOp.get(`${b}|${m['spent']}`) ?? [])]
          .map((txid) => this.sweeps.get(`${b}|${txid}`)!)
          .sort((x, y) => x.height - y.height || x.offset - y.offset || cmp(x.txid, y.txid))
          .slice(0, PerSmall)
          .map((s) => ({ txid: s.txid, outputIndex: 0 }))
      case 'history': {
        const v = chainOf(m['owner']!, m['id']!)
        if (v === undefined) return []
        const hist = this.hist(v).get(m['history']!) ?? []
        const before = Number(m['before'])
        const out: LookupFormula = []
        let tokens = 0
        // Newest first: by seq, then txid.
        for (let i = hist.length - 1; i >= 0 && tokens < PageLimit; i--) {
          const h = hist[i]!
          if (h.t.seq >= before) continue
          tokens++
          out.push(tok(h.t))
          const e = this.carriers.get(h.c)
          if (e !== undefined && e.kind === 'entry' && e.b === b) out.push(car(e))
        }
        return out
      }
      case 'chain': {
        const v = chainOf(m['owner']!, m['chain']!)
        if (v === undefined) return []
        const out: LookupFormula = []
        for (let i = v.seqs.upper(Number(m['after'])); i < v.seqs.size; i++) {
          const at = (v.bySeq.get(v.seqs.a[i]!) ?? []).filter((t) => t.rooted)
          // A page never splits a seq.
          if (out.length > 0 && out.length + at.length > PageLimit) break
          out.push(...at.map(tok))
        }
        return out
      }
    }
    throw new Error(`ls_bstore: no answer for class ${q.class}`)
  }

  /** Up to n txids from a list of time keys, newest first below `below`; with `since`, only those admitted since the window began. */
  private newest(list: Sorted | undefined, below: string, n: number, nowMs?: number): string[] {
    const out: string[] = []
    if (list === undefined) return out
    for (let i = list.lower(below) - 1; i >= 0 && out.length < n; i--) {
      const txid = txidOfKey(list.a[i]!)
      if (nowMs !== undefined) {
        const e = this.carriers.get(cOf(txid))
        if (e === undefined || nowMs - e.at >= OutcomeWindowDays * Day) continue
      }
      out.push(txid)
    }
    return out
  }

  /** A served chain's history index: by key, the rooted tokens whose changes set or remove it, ascending by seq then txid. */
  private hist(v: ChainIdx): Map<string, Array<{ t: TokenEntry; c: string }>> {
    if (v.hist === undefined) {
      v.hist = new Map()
      const ts = [...v.byTxid.values()].filter((t) => t.rooted).sort((a, b) => a.seq - b.seq || cmp(a.txid, b.txid))
      for (const t of ts) this.histAdd(v, t)
    }
    return v.hist
  }

  private histAdd(v: ChainIdx, t: TokenEntry): void {
    for (const ch of t.rec.changes) {
      let list = v.hist!.get(ch.key)
      if (list === undefined) v.hist!.set(ch.key, (list = []))
      const at = list.findIndex((h) => h.t.seq > t.seq || (h.t.seq === t.seq && h.t.txid > t.txid))
      list.splice(at < 0 ? list.length : at, 0, { t, c: toHex(ch.c) })
    }
  }

  /** An answer with each output's BEEF, as the engine hydrates one, read from the host's storage, for the terms route. */
  async hydrate(formula: LookupFormula): Promise<OutputList> {
    const storage = this.storage
    if (storage === undefined) throw new Error('ls_bstore: storage is not restored yet')
    const outputs: OutputList['outputs'] = []
    for (const { txid, outputIndex, context } of formula) {
      const o = await storage.findOutput(txid, outputIndex, undefined, undefined, true)
      if (o?.beef === undefined) continue
      outputs.push(context === undefined ? { beef: o.beef, outputIndex } : { beef: o.beef, outputIndex, context })
    }
    return { type: 'output-list', outputs }
  }

  // ---- restore (spec section 12.5)

  /**
   * Rebuilds the index: the journal's state, then every output of this
   * module's topics the host hands over (a row handed without its BEEF is
   * read again with it), then every token and spent held output the journal
   * names that the unspent rows did not carry, and then each held token's
   * predecessors through the input that spends them. A token is read as the
   * engine stores it, alone with its proof. Chains are rooted and folded
   * from seq 0 whatever order storage returns rows in. A storage failure
   * propagates: a restore that skipped a sweep would answer retracted
   * carriers.
   */
  async restore(outputs: Output[], storage: RestoreStorage): Promise<number> {
    while (this.inflight !== undefined) await this.inflight.catch(() => {})
    this.inflight = this.restoreAll(outputs, storage)
    try {
      return await this.inflight
    } finally {
      this.inflight = undefined
    }
  }

  private async restoreAll(outputs: Output[], storage: RestoreStorage): Promise<number> {
    for (const m of [this.rows, this.marks, this.table, this.seen, this.tokenSeen, this.minedSeen, this.chains, this.tokenChain, this.servedIdx, this.carriers, this.byOp,
      this.outcomesBy, this.outcomeHistory, this.frontsByAdvertiser, this.frontsByPair, this.ads, this.adsLive, this.holds, this.sweeps, this.sweepsByOp, this.heldByTx, this.drops]) {
      m.clear()
    }
    this.spentHeld.clear()
    this.reread.length = 0
    this.recheck = []
    for (const l of this.journal.load()) this.apply(l)
    this.servedIndexes()
    const full = storage as Storage
    this.storage = full
    this.ready = false
    const seen = new Set<string>()
    const queue: Output[] = []
    for (const o of outputs) {
      const k = `${o.topic}|${o.txid}.${o.outputIndex}`
      if (this.bucketOf(o.topic) === undefined || seen.has(k)) continue
      seen.add(k)
      queue.push(o)
    }
    const named: Array<{ topic: string; txid: string; vout: number }> = []
    for (const [txid, t] of this.tokenSeen) if (this.buckets.has(t.b)) named.push({ topic: RecordsPrefix + t.b, txid, vout: t.vout })
    for (const k of this.spentHeld) {
      const o = this.minedSeen.get(k)
      if (o !== undefined && this.buckets.has(o.b)) named.push({ topic: RecordsPrefix + o.b, txid: o.txid, vout: o.vout })
    }
    for (const n of named) {
      const k = `${n.topic}|${n.txid}.${n.vout}`
      if (seen.has(k)) continue
      seen.add(k)
      const o = await full.findOutput(n.txid, n.vout, n.topic, undefined, true)
      if (o !== null) queue.push(o)
    }
    const read = async (o: Output): Promise<Transaction | undefined> => {
      let row: Output | null = o
      if (row.beef === undefined) row = (await full.findOutput(o.txid, o.outputIndex, o.topic, undefined, true)) ?? row
      if (row.beef === undefined) return undefined
      try {
        const tx = Transaction.fromBEEF(row.beef, row.txid)
        const out = tx.outputs[o.outputIndex]
        if (out === undefined || classify(bin(out.lockingScript)) !== 'bucket-token') return tx
        return storedToken(Uint8Array.from(row.beef))
      } catch {
        return undefined
      }
    }
    const txs: Array<{ o: Output; tx: Transaction; sweep: boolean }> = []
    for (const o of queue) {
      const tx = await read(o)
      if (tx === undefined) {
        this.host.log('ls_bstore could not restore an output without its transaction', { topic: o.topic, txid: o.txid })
        continue
      }
      const out = tx.outputs[o.outputIndex]
      txs.push({ o, tx, sweep: out !== undefined && o.outputIndex === 0 && classify(bin(out.lockingScript)) === 'funding' })
    }
    // Everything else first, in storage's order; sweeps after them.
    txs.sort((a, b) => Number(a.sweep) - Number(b.sweep) || (a.o.score ?? 0) - (b.o.score ?? 0) || cmp(a.o.txid, b.o.txid))
    let indexed = 0
    this.restoring = true
    const take = (o: Output, tx: Transaction): void => {
      try {
        const at = o.score !== undefined && o.score > 0 ? o.score : this.now()
        if (this.index(this.bucketOf(o.topic)!, tx, o.outputIndex, at)) indexed++
      } catch (e) {
        this.host.log('ls_bstore could not restore an output', { topic: o.topic, txid: o.txid, err: String(e) })
      }
    }
    try {
      for (const { o, tx } of txs) take(o, tx)
      const walk: Array<{ topic: string; tx: Transaction; prev: string }> = []
      const next = (o: Output, tx: Transaction): void => {
        const key = this.tokenChain.get(o.txid)
        const t = key === undefined ? undefined : this.chains.get(key)?.byTxid.get(o.txid)
        if (t !== undefined && t.vout === o.outputIndex && t.prev !== '') walk.push({ topic: o.topic, tx, prev: t.prev })
      }
      for (const { o, tx } of txs) next(o, tx)
      while (walk.length > 0) {
        const w = walk.pop()!
        if (this.tokenChain.has(w.prev)) continue
        const input = w.tx.inputs.find((i) => srcTxid(i) === w.prev)
        if (input === undefined) continue
        const o = await full.findOutput(w.prev, input.sourceOutputIndex, w.topic, undefined, true)
        if (o === null) continue
        const tx = await read(o)
        if (tx === undefined) continue
        take(o, tx)
        next(o, tx)
      }
    } finally {
      this.restoring = false
    }
    for (const v of this.chains.values()) {
      for (const t of v.bySeq.get(0) ?? []) this.rootFrom(v, t, true)
      for (const t of v.byTxid.values()) if (!t.rooted) this.counted(v, t)
      for (const seq of [...v.seqs.a]) this.bound(v, seq)
    }
    // Later carriers on an outpoint stay only where a served chain names them.
    for (const c of this.recheck) {
      const e = this.carriers.get(c)
      if (e === undefined || this.rows.get(e.op)?.c === c || this.namedByServed(e.b, c)) continue
      this.drop(e, 'second-carrier')
    }
    this.recheck = []
    await this.drain()
    await this.flush()
    this.ready = true
    return indexed
  }

  async getDocumentation(): Promise<string> {
    return [
      '# ls_bstore',
      '',
      'Answers every bstore bucket this host carries (spec section 11). Free: `bucket`,',
      '`bucket-seq`, `object`, `list`, `pending`, `unanchored`, `entry`, `ads`, `ad`, `fronts`,',
      '`front`, `hold`, `outcomes`, `outcomes-of`, `outcomes-by`, `outcomes-of-by` and `sweep`.',
      'Priceable: `history`, `chain`, `export`, `outcome-history` and `outcome-history-of`. The',
      'map classes answer for the chains this host serves, each proven against its latest',
      'rooted, consistent token. A question must carry exactly the members of one class;',
      'anything else is refused with an error. An empty answer means only that this host',
      'holds nothing that answers.',
    ].join('\n')
  }

  async getMetaData(): Promise<LookupServiceMetaData> {
    return { name: ServiceName, shortDescription: 'Answers bstore buckets: tokens, key maps, entries, advertisements, outcomes and fronts.' }
  }
}

