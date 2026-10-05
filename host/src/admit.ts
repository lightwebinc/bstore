/**
 * The topic manager's transaction rules of spec section 12.1, the twin of
 * the Go admit.go: a carrier's rules 1 to 9, a bucket token's 1 to 9,
 * advertisements and hold outputs, a sweep, and every refusal thrown with
 * the Go codec's label. No rule reads anything the topic holds.
 */
import { type ChainTracker, type Transaction } from '@bsv/sdk'
import {
  BeefRefusal, LockTime, MaxSequence, aloneShape, bytesEqual, carrierShape, firstPush, mined, pushDropScript, pushFields, readTokenOutput,
  readWire, subjectTx, tokenLockedTo, tokenShape, tokenSignedBy, tokenSpends, unlockingRefusal, verifyField, type Wire,
} from '@lightwebinc/bcommon'
import { decodeAdvertisement, type Advertisement } from './advert.js'
import { bucketID, isBucket } from './names.js'
import { decodeBucket, decodeEntry, decodeFront, decodeOutcome, type Bucket, type Entry, type Front, type Outcome } from './records.js'
import { claims, classify, fundingScript, lockingKey, type Claim } from './script.js'
import {
  DefaultMaxBEEF, DefaultMaxSweepInputs, KeyBucket, KeyChain, KeyEntry, KeyFront, KeyOutcome, MaxAdsPerTx, MaxCarrierBEEF, MaxMinedBEEF,
  MaxSweepBEEF, MaxTokenBEEF, refuse,
} from './storerec.js'
import { equal, hashOrder } from './util.js'

export interface Host {
  bucket: string
  headers?: ChainTracker
  maxBEEF?: number
  maxSweepInputs?: number
}

export interface Outpoint {
  /** Display order. */
  txid: string
  vout: number
}

export interface TokenOut {
  bucket: Bucket
  record: Uint8Array
  vout: number
}

export interface Admission {
  kind: 'entry' | 'outcome' | 'front' | 'bucket-token' | 'mined' | 'sweep'
  txid: Uint8Array
  outputs: number[]
  retain: number[]
  carrier?: { record: Uint8Array; entry?: Entry; outcome?: Outcome; front?: Front; commitment: Uint8Array; funding: Outpoint }
  token?: TokenOut
  predecessor?: TokenOut
  parent?: string
  spends?: Outpoint
  ads?: Map<number, Advertisement>
  holds?: number[]
  spent?: Outpoint[]
}

const bin = (s: { toBinary(): number[] }): Uint8Array => Uint8Array.from(s.toBinary())
const srcTxid = (i: Transaction['inputs'][number]): string => i.sourceTXID ?? i.sourceTransaction!.id('hex')

function beef<T>(f: () => T): T {
  try {
    return f()
  } catch (e) {
    if (e instanceof BeefRefusal) throw refuse('beef', '', e.message)
    throw e
  }
}

function retained(tx: Transaction, held: number[]): number[] {
  for (const i of held) {
    if (!Number.isInteger(i) || i < 0 || i >= tx.inputs.length) throw refuse('host-config', '', `previous coin names input ${i} of ${tx.inputs.length}`)
  }
  return [...new Set(held)].sort((a, b) => a - b)
}

/** A host's bounds, zero or absent meaning the floor; a bound below its floor is host-config. */
export function hostBounds(h: Host): { maxBEEF: number; maxSweepInputs: number } {
  const maxBEEF = h.maxBEEF || DefaultMaxBEEF
  const maxSweepInputs = h.maxSweepInputs || DefaultMaxSweepInputs
  if (maxBEEF < DefaultMaxBEEF || maxSweepInputs < DefaultMaxSweepInputs) throw refuse('host-config')
  return { maxBEEF, maxSweepInputs }
}

export async function admit(raw: Uint8Array, held: number[], h: Host): Promise<Admission> {
  if (!isBucket(h.bucket)) throw refuse('host-config', '', 'bucket identifier breaks the grammar')
  const bounds = hostBounds(h)
  const w = beef(() => readWire(raw, bounds.maxBEEF))
  const tx = subjectTx(w)
  const a: Admission = { kind: 'sweep', txid: Uint8Array.from(tx.hash() as number[]), outputs: [], retain: retained(tx, held) }
  let claim = '' as Claim
  let n = 0
  let at = 0
  for (const [i, o] of tx.outputs.entries()) {
    const c = classify(bin(o.lockingScript))
    if (claims(c)) {
      claim = c
      at = i
      n++
    }
  }
  if (n > 1) throw refuse('shape', '', `${n} outputs claim a token or a record`)
  if (claim === 'piece') throw refuse('piece')
  if (claim === 'entry' || claim === 'outcome' || claim === 'front') {
    a.carrier = checkCarrier(w, tx, claim, raw.length, h)
    a.kind = claim
    a.outputs = [0]
    return a
  }
  if (claim === 'bucket-token') {
    await checkToken(a, w, tx, at, raw.length, h)
    a.kind = 'bucket-token'
    a.outputs = [at]
    return a
  }
  // Mined objects: advertisements and hold-shaped outputs.
  const ads = new Map<number, Advertisement>()
  const holds: number[] = []
  const admitted: number[] = []
  tx.outputs.forEach((o, i) => {
    const s = bin(o.lockingScript)
    if (classify(s) === 'hold') {
      holds.push(i)
      admitted.push(i)
      return
    }
    try {
      ads.set(i, decodeAdvertisement(s))
      admitted.push(i)
    } catch {
      // not an advertisement
    }
  })
  if (admitted.length > 0) {
    beef(() => aloneShape(w, tx))
    if (raw.length > MaxMinedBEEF) throw refuse('beef', '', `a mined object's BEEF of ${raw.length} bytes`)
    if (!(await mined(tx, h.headers))) throw refuse('unmined')
    if (admitted.length > MaxAdsPerTx) throw refuse('too-many')
    Object.assign(a, { kind: 'mined', outputs: admitted, ads, holds })
    return a
  }
  const first = tx.outputs[0]
  if (tx.outputs.length >= 1 && tx.outputs.length <= 2 && first !== undefined && classify(bin(first.lockingScript)) === 'funding') {
    beef(() => aloneShape(w, tx))
    if (raw.length > MaxSweepBEEF) throw refuse('beef', '', `a sweep's BEEF of ${raw.length} bytes`)
    if (!(await mined(tx, h.headers))) throw refuse('unmined')
    if (tx.inputs.length > bounds.maxSweepInputs) throw refuse('too-large', '', `a sweep of ${tx.inputs.length} inputs`)
    a.outputs = [0]
    a.spent = tx.inputs.map((i) => ({ txid: srcTxid(i), vout: i.sourceOutputIndex }))
    return a
  }
  throw refuse('not-bstore')
}

function checkCarrier(w: Wire, tx: Transaction, claim: 'entry' | 'outcome' | 'front', size: number, h: Host): NonNullable<Admission['carrier']> {
  if (tx.inputs.length !== 1 || tx.outputs.length !== 1) throw refuse('shape', '', `${tx.inputs.length} inputs, ${tx.outputs.length} outputs`)
  const input = tx.inputs[0]!
  const out = tx.outputs[0]!
  const tree = beef(() => carrierShape(w, tx))
  if (size > MaxCarrierBEEF) throw refuse('beef', '', `a carrier's BEEF of ${size} bytes`)
  const lock = bin(out.lockingScript)
  const record = firstPush(lock)!
  const c: NonNullable<Admission['carrier']> = { record, commitment: Uint8Array.from(tx.hash() as number[]), funding: { txid: tree.txid, vout: input.sourceOutputIndex } }
  let owner: Uint8Array
  let keyID: string
  let bucket: string
  if (claim === 'entry') {
    c.entry = decodeEntry(record)
    ;[owner, keyID, bucket] = [c.entry.writer, KeyEntry, c.entry.bucket]
  } else if (claim === 'outcome') {
    c.outcome = decodeOutcome(record)
    ;[owner, keyID, bucket] = [c.outcome.buyer, KeyOutcome, c.outcome.bucket]
  } else {
    c.front = decodeFront(record)
    ;[owner, keyID, bucket] = [c.front.front, KeyFront, c.front.bucket]
  }
  if (bucket !== h.bucket) throw refuse('bucket', '', "not this topic's bucket")
  if (tx.lockTime < LockTime || (input.sequence ?? MaxSequence) === MaxSequence) throw refuse('mineable')
  if (unlockingRefusal(tx) !== undefined) throw refuse('unlock')
  const key = lockingKey(keyID, owner)
  const fields = pushFields(lock)
  if (fields?.length !== 2) throw refuse('lock', '', 'want the record and a signature')
  const sig = fields[1]!
  if (!bytesEqual(lock, pushDropScript(key, [record], sig))) throw refuse('lock')
  if (!verifyField(key, record, sig)) throw refuse('signature')
  const src = tree.tx!.outputs[input.sourceOutputIndex]
  if (src === undefined || !bytesEqual(bin(src.lockingScript), fundingScript(key))) throw refuse('funding')
  if ((src.satoshis ?? 0) !== (out.satoshis ?? 0)) throw refuse('funding', '', 'the value is not carried whole')
  return c
}

/** One token output read well formed, or undefined. */
export function readBucketOutput(script: Uint8Array, satoshis: number | undefined, vout: number): TokenOut | undefined {
  if (classify(script) !== 'bucket-token') return undefined
  const to = readTokenOutput(script, satoshis, vout, 2)
  if (to === undefined) return undefined
  try {
    const bucket = decodeBucket(to.fields[1]!)
    const key = lockingKey(KeyBucket, bucket.owner)
    if (!tokenLockedTo(to, script, key) || !tokenSignedBy(to, key)) return undefined
    return { bucket, record: to.fields[1]!, vout }
  } catch {
    return undefined
  }
}

const samePredecessor = (t: Bucket, p: Bucket): boolean => p.bucket === t.bucket && equal(p.owner, t.owner) && equal(p.id, t.id) && p.seq + 1 === t.seq

async function checkToken(a: Admission, w: Wire, tx: Transaction, at: number, size: number, h: Host): Promise<void> {
  const out = tx.outputs[at]!
  const lock = bin(out.lockingScript)
  const to = readTokenOutput(lock, out.satoshis, at, 2)
  if (to === undefined) throw refuse('shape', '', 'a token is two fields, a signature and 1 satoshi')
  const parent = beef(() => tokenShape(w, tx))
  if (size > MaxTokenBEEF) throw refuse('beef', '', `a token's BEEF of ${size} bytes`)
  const bucket = decodeBucket(to.fields[1]!)
  if (bucket.bucket !== h.bucket) throw refuse('bucket', '', "not this topic's bucket")
  const key = lockingKey(KeyBucket, bucket.owner)
  if (!tokenLockedTo(to, lock, key)) throw refuse('lock')
  if (!tokenSignedBy(to, key)) throw refuse('signature')
  if (!(await mined(tx, h.headers)) || !(await mined(parent.tx, h.headers))) throw refuse('unmined')
  const ptxid = hashOrder(parent.txid)
  const spends = tokenSpends(tx, parent)
  a.token = { bucket, record: to.fields[1]!, vout: at }
  a.parent = parent.txid
  if (bucket.seq === 0) {
    if (spends.length !== 1) throw refuse('token-input', '', `seq 0 has ${spends.length} inputs spending its parent`)
    const sp = spends[0]!
    if (!bytesEqual(sp.script, fundingScript(lockingKey(KeyChain, bucket.owner)))) throw refuse('token-input', '', 'not a chain-tree output of the owner')
    if (!equal(bucket.id, bucketID(ptxid, sp.vout))) throw refuse('bucket-id')
    a.spends = { txid: parent.txid, vout: sp.vout }
    return
  }
  let pred: TokenOut | undefined
  let spendsToken = 0
  for (const sp of spends) {
    if (classify(sp.script) !== 'bucket-token') continue
    spendsToken++
    pred = readBucketOutput(sp.script, sp.satoshis, sp.vout) ?? pred
  }
  if (!equal(ptxid, bucket.prev) || spendsToken !== 1 || pred === undefined || !samePredecessor(bucket, pred.bucket)) {
    throw refuse('token-input', '', "the parent's token at seq - 1 is not spent exactly once")
  }
  a.predecessor = pred
  a.spends = { txid: parent.txid, vout: pred.vout }
}
