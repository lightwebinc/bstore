/**
 * Test helpers that make fresh bstore objects the golden vectors do not
 * hold: funding trees, bucket tokens, advertisements, hold outputs and
 * sweeps mined on a local chain; entry, outcome and front carriers; and a
 * paying client wallet. OwnerSim keeps a bucket chain and its map and
 * anchors entries the way spec section 3.4 says the owner's tooling does,
 * or, on request, the way it says not to. Every key is drawn from a fixed
 * label, so a party is the same on every run. Nothing here is part of the
 * module the host loads.
 */
import {
  CompletedProtoWallet,
  LockingScript,
  MerklePath,
  P2PKH,
  PrivateKey,
  ProtoWallet,
  PushDrop,
  Transaction,
  UnlockingScript,
  type ChainTracker,
  type CreateActionArgs,
  type CreateActionResult,
} from '@bsv/sdk'
import { LockTime, compareBytes, pushDropScript, tokenBEEF } from '@lightwebinc/bcommon'
import { encodeRequest, type Request } from './challenge.js'
import { KeyMap, resolveBatch, type Applicable } from './keymap.js'
import { bucketID } from './names.js'
import { encodeBucket, encodeEntry, encodeFront, encodeOutcome, type Bucket, type Change, type Entry, type Hold } from './records.js'
import { fundingScript, holdScript, lockingKey } from './script.js'
import {
  AdKeyID, AdProtocol, EmptyHash, EmptyRoot, KeyBucket, KeyChain, KeyChallenge, KeyEntry, KeyFront, KeyOutcome, KindDelete, KindPut, OutcomePass,
  Protocol, TagToken,
} from './storerec.js'
import { compareText, concat, sha256, toHex, utf8 } from './util.js'

export const sha = (s: string): Uint8Array => sha256(utf8(s))
const ls = (b: Uint8Array): LockingScript => LockingScript.fromBinary(Array.from(b))

/** Unix milliseconds the tests write as `created` by default. */
export const T0 = 1_798_761_600_000

/** A local chain: blocks of two, each a minted transaction beside a fixed sibling. */
export class Chain {
  readonly roots = new Map<number, string>()
  private height: number

  constructor(first = 50000, extra: Array<{ height: number; merkleRoot: string }> = []) {
    this.height = first
    for (const h of extra) this.roots.set(h.height, h.merkleRoot)
  }

  mine(tx: Transaction): Transaction {
    const height = this.height++
    tx.merklePath = new MerklePath(height, [
      [
        { offset: 0, hash: '55'.repeat(32) },
        { offset: 1, hash: tx.id('hex'), txid: true },
      ],
    ])
    this.roots.set(height, tx.merklePath.computeRoot(tx.id('hex')))
    return tx
  }

  /** Takes the block at height out of the best chain. */
  orphan(height: number): void {
    this.roots.delete(height)
  }

  get tip(): number {
    return Math.max(...this.roots.keys())
  }

  get tracker(): ChainTracker {
    return {
      isValidRootForHeight: async (root, height) => this.roots.get(height) === root,
      currentHeight: async () => this.tip,
    }
  }
}

/** One identity: its key, its wallet and its compressed public key. */
export class Party {
  readonly key: PrivateKey
  readonly id: Uint8Array
  readonly hex: string
  readonly wallet: CompletedProtoWallet

  constructor(readonly label: string) {
    this.key = PrivateKey.fromHex(toHex(sha(`bstore/test/party/${label}`)))
    this.id = Uint8Array.from(this.key.toPublicKey().encode(true) as number[])
    this.hex = toHex(this.id)
    this.wallet = new CompletedProtoWallet(this.key)
  }

  derived(keyID: string): Uint8Array {
    return lockingKey(keyID, this.id)
  }

  async sign(keyID: string, data: Uint8Array): Promise<Uint8Array> {
    const r = await this.wallet.createSignature({ data: Array.from(data), protocolID: Protocol, keyID, counterparty: 'anyone' })
    return Uint8Array.from(r.signature)
  }

  unlock(keyID: string): ReturnType<PushDrop['unlock']> {
    return new PushDrop(this.wallet).unlock(Protocol, keyID, 'anyone', 'all', false)
  }
}

let nowhere = 0

/** A transaction from nowhere: one unsigned input of an outpoint no one holds. */
export function fromNowhere(outs: Array<{ satoshis: number; lockingScript: LockingScript }>): Transaction {
  return new Transaction(
    1,
    [{ sourceTXID: toHex(sha(`bstore/test/nowhere/${nowhere++}/${Date.now()}`)), sourceOutputIndex: 0, unlockingScript: new UnlockingScript(), sequence: 0xffffffff }],
    outs,
    0,
  )
}

/** A mined funding tree of n outputs of 1000 satoshis, locked to derive(owner, keyID). */
export function fundingTree(owner: Party, keyID: string, n: number, chain: Chain): Transaction {
  const lock = ls(fundingScript(owner.derived(keyID)))
  return chain.mine(fromNowhere(Array.from({ length: n }, () => ({ satoshis: 1000, lockingScript: lock }))))
}

/** A carrier of record spending output vout of owner's tree, locked and signed under keyID. */
export async function carrier(tree: Transaction, vout: number, owner: Party, keyID: string, record: Uint8Array): Promise<Transaction> {
  const lock = pushDropScript(owner.derived(keyID), [record], await owner.sign(keyID, record))
  const tx = new Transaction(
    1,
    [{ sourceTransaction: tree, sourceOutputIndex: vout, unlockingScriptTemplate: owner.unlock(keyID), sequence: 0 }],
    [{ satoshis: tree.outputs[vout]!.satoshis!, lockingScript: ls(lock) }],
    LockTime,
  )
  await tx.sign()
  return tx
}

/** A mined sweep of outputs vouts of owner's tree: its output 0 funding-shaped under keyID. */
export async function sweep(tree: Transaction, vouts: number[], owner: Party, keyID: string, chain: Chain): Promise<Transaction> {
  const value = vouts.reduce((n, v) => n + tree.outputs[v]!.satoshis!, 0)
  const tx = new Transaction(
    1,
    vouts.map((v) => ({ sourceTransaction: tree, sourceOutputIndex: v, unlockingScriptTemplate: owner.unlock(keyID), sequence: 0xffffffff })),
    [{ satoshis: value, lockingScript: ls(fundingScript(owner.derived(keyID))) }],
    0,
  )
  await tx.sign()
  return chain.mine(tx)
}

/** C = txid(K), hash byte order. */
export const commitment = (tx: Transaction): Uint8Array => Uint8Array.from(tx.hash() as number[])
export const cHex = (tx: Transaction): string => toHex(commitment(tx))

/** A carrier a party made: the record, its carrier and its funding outpoint. */
export interface Posted {
  tx: Transaction
  txid: string
  c: string
  record: Uint8Array
  vout: number
  tree: Transaction
  /** Where the funding output is: `<tree txid>.<vout>`. */
  op: string
}

export interface PostedEntry extends Posted {
  e: Entry
}

/** A party with funding trees of its own, one per key id it writes under. */
export class Author {
  readonly party: Party
  private readonly trees = new Map<string, { tx: Transaction; next: number }>()

  constructor(
    readonly bucket: string,
    label: string,
    readonly chain: Chain,
    private readonly size = 512,
  ) {
    this.party = new Party(label)
  }

  get hex(): string {
    return this.party.hex
  }

  get id(): Uint8Array {
    return this.party.id
  }

  tree(keyID: string): Transaction {
    let t = this.trees.get(keyID)
    if (t === undefined) this.trees.set(keyID, (t = { tx: fundingTree(this.party, keyID, this.size, this.chain), next: 0 }))
    return t.tx
  }

  /** A carrier of record under keyID, on the next unused output of the tree, or on output vout. */
  async post(keyID: string, record: Uint8Array, vout?: number): Promise<Posted> {
    const tree = this.tree(keyID)
    const t = this.trees.get(keyID)!
    const at = vout ?? t.next++
    const tx = await carrier(tree, at, this.party, keyID, record)
    return { tx, txid: tx.id('hex'), c: cHex(tx), record, vout: at, tree, op: `${tree.id('hex')}.${at}` }
  }
}

/** A deterministic object's identity: a root, a content hash and a proof root. */
export function object(label: string, size = 1000): { root: Uint8Array; contentHash: Uint8Array; proofRoot: Uint8Array; size: number } {
  return { root: sha(`root/${label}`), contentHash: sha(`content/${label}`), proofRoot: sha(`proof/${label}`), size }
}

/** A writer of entries in one bucket. */
export class Writer extends Author {
  /** A put of key, an object of the label's, created at `created`. */
  async put(id: Uint8Array, key: string, o: { label?: string; created?: number; hold?: Hold; vout?: number; empty?: boolean } = {}): Promise<PostedEntry> {
    const obj = o.empty === true ? { root: EmptyRoot, contentHash: EmptyHash, proofRoot: EmptyHash, size: 0 } : object(o.label ?? key)
    const e: Entry = {
      bucket: this.bucket, id, writer: this.id, key, kind: KindPut, created: o.created ?? T0, size: obj.size, root: obj.root, contentHash: obj.contentHash,
      proofRoot: obj.proofRoot, proofProfile: 1, contentType: 'application/octet-stream', unknown: [], extra: [],
    }
    if (o.hold !== undefined) e.hold = o.hold
    const record = encodeEntry(e)
    return { ...(await this.post(KeyEntry, record, o.vout)), e }
  }

  /** A delete of key. */
  async del(id: Uint8Array, key: string, o: { created?: number; vout?: number } = {}): Promise<PostedEntry> {
    const e: Entry = {
      bucket: this.bucket, id, writer: this.id, key, kind: KindDelete, created: o.created ?? T0, size: 0, root: new Uint8Array(32), contentHash: new Uint8Array(32),
      proofRoot: new Uint8Array(32), proofProfile: 0, contentType: '', unknown: [], extra: [],
    }
    const record = encodeEntry(e)
    return { ...(await this.post(KeyEntry, record, o.vout)), e }
  }

  /** A mined hold output of sats, and the terms an entry names it by. */
  hold(sats: number, terms: Partial<Hold> = {}): { tx: Transaction; terms: Hold; op: string } {
    const tx = this.chain.mine(fromNowhere([{ satoshis: sats, lockingScript: ls(holdScript(this.party.derived('hold'))) }, { satoshis: 1000, lockingScript: new P2PKH().lock(this.party.key.toAddress()) }]))
    const t: Hold = { txid: commitment(tx), vout: 0, satoshis: sats, t: 1, K: 2, retention: 86400, deadline: 1_000_000, checkC: 30, ...terms }
    return { tx, terms: t, op: `${tx.id('hex')}.0` }
  }
}

/** A buyer's outcome record of a challenge of provider, answered by answerer. */
export class Buyer extends Author {
  async outcome(o: { root: Uint8Array; provider: Party; answerer?: Party; outcome?: number; created?: number; vout?: number }): Promise<Posted> {
    const answerer = o.answerer ?? o.provider
    const q: Request = {
      root: o.root, proofRoot: sha('proof'), size: 1000, provider: o.provider.id, answerer: answerer.id, seedHeight: 100, seedHash: sha('seed'),
      nonce: sha('nonce').subarray(0, 16), c: 30,
    }
    const request = encodeRequest(q)
    const outcome = o.outcome ?? 3
    const rec: Parameters<typeof encodeOutcome>[0] = { bucket: this.bucket, buyer: this.id, created: o.created ?? T0, request, req: q, outcome, extra: [] }
    if (outcome === OutcomePass || outcome === 2) {
      rec.response = sha('response')
      rec.signature = await answerer.sign(KeyChallenge, concat(sha256(request), rec.response))
    }
    return await this.post(KeyOutcome, encodeOutcome(rec), o.vout)
  }
}

/** A front: statements that it answers for an advertiser. */
export class FrontSim extends Author {
  async statement(advertiser: Uint8Array, o: { url?: string; created?: number; vout?: number } = {}): Promise<Posted> {
    const record = encodeFront({ bucket: this.bucket, front: this.id, advertiser, url: o.url ?? 'https://front.example/bstore/v1', created: o.created ?? T0, extra: [] })
    return await this.post(KeyFront, record, o.vout)
  }
}

/** CompactSize, as BRC-26 writes an advertisement's integers. */
function compact(v: number): Uint8Array {
  if (v < 0xfd) return Uint8Array.of(v)
  if (v <= 0xffff) return Uint8Array.of(0xfd, v & 0xff, v >> 8)
  const b = new Uint8Array(5)
  b[0] = 0xfe
  new DataView(b.buffer).setUint32(1, v, true)
  return b
}

/** A provider: BRC-26 advertisements of roots, mined, and their renewals. */
export class Provider {
  readonly party: Party

  constructor(
    label: string,
    readonly chain: Chain,
  ) {
    this.party = new Party(label)
  }

  get hex(): string {
    return this.party.hex
  }

  async adScript(root: Uint8Array, expiry: number, url?: string, fileSize = 100): Promise<LockingScript> {
    const id = toHex(root)
    const fields = [this.party.id, root, utf8(url ?? `https://${this.party.label.replace(/[^a-z]/g, '')}.example/chirp/v1/${id}/objects/${id}`), compact(expiry), compact(fileSize)]
    const { signature } = await this.party.wallet.createSignature({ data: Array.from(concat(...fields)), protocolID: AdProtocol, keyID: AdKeyID, counterparty: 'anyone' })
    const { publicKey } = await this.party.wallet.getPublicKey({ protocolID: AdProtocol, keyID: AdKeyID, counterparty: 'anyone', forSelf: true })
    return ls(pushDropScript(Uint8Array.from(Buffer.from(publicKey, 'hex')), fields, Uint8Array.from(signature)))
  }

  /** A mined transaction of advertisements of roots, expiring at expiry (Unix seconds). */
  async advertise(roots: Uint8Array[], expiry: number): Promise<Transaction> {
    const outs = []
    for (const r of roots) outs.push({ satoshis: 1, lockingScript: await this.adScript(r, expiry) })
    outs.push({ satoshis: 1000, lockingScript: new P2PKH().lock(this.party.key.toAddress()) })
    return this.chain.mine(fromNowhere(outs))
  }

  /** A renewal: spends advertisement vout of ad into a new one of the same root with a later expiry. */
  async renew(ad: Transaction, vout: number, root: Uint8Array, expiry: number): Promise<Transaction> {
    const tx = new Transaction(
      1,
      [{ sourceTransaction: ad, sourceOutputIndex: vout, unlockingScriptTemplate: new PushDrop(this.party.wallet).unlock(AdProtocol, AdKeyID, 'anyone', 'all', false), sequence: 0xffffffff }],
      [{ satoshis: 1, lockingScript: await this.adScript(root, expiry) }],
      0,
    )
    await tx.sign()
    return this.chain.mine(tx)
  }
}

/** A mined token and the BEEF it is published in. */
export interface Minted {
  tx: Transaction
  txid: string
  beef: Uint8Array
  rec: Bucket
  parent: Transaction
}

/** A bucket's owner tooling over a local chain: its chain tree, one chain, and the map it anchors. */
export class OwnerSim {
  readonly party: Party
  readonly chainTree: Transaction
  readonly id: Uint8Array
  tip: Minted | undefined
  history: Minted[] = []
  map = new KeyMap()
  named = new Set<string>()
  writers: Uint8Array[] = []

  constructor(
    readonly bucket: string,
    label: string,
    readonly chain: Chain,
    private readonly chainVout = 0,
  ) {
    this.party = new Party(label)
    this.chainTree = fundingTree(this.party, KeyChain, 8, chain)
    this.id = bucketID(commitment(this.chainTree), chainVout)
  }

  get hex(): string {
    return this.party.hex
  }

  get idHex(): string {
    return toHex(this.id)
  }

  /** The served triple of this chain, as BSTORE_SERVED holds it after parsing. */
  get served(): string {
    return `${this.bucket}|${this.hex}|${this.idHex}`
  }

  /** The same chain at the same tip, to build a competing token on. */
  fork(): OwnerSim {
    const f = Object.create(OwnerSim.prototype) as OwnerSim
    Object.assign(f, this)
    f.history = [...this.history]
    f.map = this.map.clone()
    f.named = new Set(this.named)
    return f
  }

  /**
   * The next token, mined unless mine is false: it anchors entries as the
   * owner's tooling does (each named once, a batch resolved by created then
   * commitment, no-ops consumed), names writers, and mutate may change the
   * record before it is encoded.
   */
  async token(o: { anchor?: PostedEntry[]; writers?: Uint8Array[]; mutate?: (b: Bucket) => void; mine?: boolean; created?: number } = {}): Promise<Minted> {
    const seq = this.tip === undefined ? 0 : this.tip.rec.seq + 1
    if (o.writers !== undefined) this.writers = [...o.writers].sort(compareBytes)
    let changes: Change[] = []
    let consumed: Uint8Array[] = []
    const next = this.map.clone()
    const named = new Set(this.named)
    if (seq > 0) {
      const batch: Applicable[] = (o.anchor ?? []).map((p) => ({ c: commitment(p.tx), key: p.e.key, kind: p.e.kind, created: p.e.created }))
      ;[changes, consumed] = resolveBatch(this.map, this.named, batch)
      for (const ch of changes) {
        named.add(toHex(ch.c))
        if (ch.removal) next.m.delete(ch.key)
        else next.m.set(ch.key, ch.c)
      }
      for (const c of consumed) named.add(toHex(c))
    }
    const rec: Bucket = {
      bucket: this.bucket,
      owner: this.party.id,
      id: this.id,
      seq,
      prev: this.tip === undefined ? new Uint8Array(32) : commitment(this.tip.tx),
      writers: seq === 0 ? [] : this.writers,
      changes: changes.sort((a, b) => compareText(a.key, b.key)),
      count: next.size,
      mapRoot: next.root(),
      created: o.created ?? T0 + seq * 600_000,
      consumed,
      extra: [],
    }
    o.mutate?.(rec)
    const record = encodeBucket(rec)
    const input = this.tip === undefined ? { src: this.chainTree, vout: this.chainVout, keyID: KeyChain } : { src: this.tip.tx, vout: 0, keyID: KeyBucket }
    const fields = [TagToken, record]
    const lock = pushDropScript(this.party.derived(KeyBucket), fields, await this.party.sign(KeyBucket, concat(...fields)))
    const total = input.src.outputs[input.vout]!.satoshis!
    const outs = [{ satoshis: 1, lockingScript: ls(lock) }]
    if (total > 1) outs.push({ satoshis: total - 1, lockingScript: new P2PKH().lock(this.party.key.toAddress()) })
    const tx = new Transaction(1, [{ sourceTransaction: input.src, sourceOutputIndex: input.vout, unlockingScriptTemplate: this.party.unlock(input.keyID), sequence: 0xffffffff }], outs, 0)
    await tx.sign()
    if (o.mine !== false) this.chain.mine(tx)
    const m: Minted = { tx, txid: tx.id('hex'), beef: o.mine === false ? new Uint8Array(0) : tokenBEEF(tx, input.src), rec, parent: input.src }
    this.tip = m
    this.history.push(m)
    if (o.mutate === undefined) {
      this.map = next
      this.named = named
    }
    return m
  }
}

/**
 * A client wallet that can pay, as BRC-105's client asks of one: each
 * createAction spends a fresh coin mined on chain to the outputs asked for,
 * with change back to itself.
 */
export class PayingWallet extends ProtoWallet {
  constructor(
    private readonly key: PrivateKey,
    private readonly chain: Chain,
  ) {
    super(key)
  }

  coin(): Transaction {
    return this.chain.mine(fromNowhere([{ satoshis: 100000, lockingScript: new P2PKH().lock(this.key.toAddress()) }]))
  }

  async pay(outputs: Array<{ satoshis: number; lockingScript: string }>, coin = this.coin()): Promise<Transaction> {
    const tx = new Transaction(
      1,
      [{ sourceTransaction: coin, sourceOutputIndex: 0, unlockingScriptTemplate: new P2PKH().unlock(this.key), sequence: 0xffffffff }],
      [
        ...outputs.map((o) => ({ satoshis: o.satoshis, lockingScript: LockingScript.fromHex(o.lockingScript) })),
        { satoshis: 100000 - outputs.reduce((n, o) => n + o.satoshis, 0) - 100, lockingScript: new P2PKH().lock(this.key.toAddress()) },
      ],
      0,
    )
    await tx.sign()
    return tx
  }

  async createAction(args: CreateActionArgs): Promise<CreateActionResult> {
    const tx = await this.pay((args.outputs ?? []).map((o) => ({ satoshis: o.satoshis, lockingScript: o.lockingScript })))
    return { tx: tx.toAtomicBEEF(), txid: tx.id('hex') }
  }
}
