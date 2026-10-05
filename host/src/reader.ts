/** A reader's checks that need no network (spec section 15), the twin of the Go reader.go. */
import { checkObject, type EntryContext, type MapLeaf, type ObjectResult } from './keymap.js'
import type { Bucket, Entry } from './records.js'
import { bucketID } from './names.js'
import { KindPut, refuse } from './storerec.js'
import { equal } from './util.js'

export interface Link {
  txid: Uint8Array
  bucket: Bucket
  /** For the token at seq 0, the outpoint its chain-tree input spends (txid in hash byte order). */
  spends?: { txid: Uint8Array; vout: number }
}

export interface Held {
  seq: number
  txid: Uint8Array
}

/** Step 1 on verified tokens, latest first: rollback, or bucket when the walk does not link, end on the token held, or prove its id. */
export function checkWalk(held: Held | undefined, walk: Link[]): void {
  if (walk.length === 0) throw refuse('bucket', '', 'no token')
  const top = walk[0]!.bucket
  if (held !== undefined && top.seq < held.seq) throw refuse('rollback')
  for (let i = 0; i + 1 < walk.length; i++) {
    const a = walk[i]!.bucket
    const b = walk[i + 1]!.bucket
    if (!equal(a.prev, walk[i + 1]!.txid) || a.seq !== b.seq + 1 || a.bucket !== b.bucket || !equal(a.owner, b.owner) || !equal(a.id, b.id)) {
      throw refuse('bucket', '', `token at seq ${a.seq} does not link`)
    }
  }
  const last = walk[walk.length - 1]!
  if (held !== undefined && last.bucket.seq === held.seq) {
    if (!equal(last.txid, held.txid)) throw refuse('bucket', '', 'the walk ends on another token at the seq held')
    return
  }
  if (last.bucket.seq !== 0) throw refuse('bucket', '', `the walk stops at seq ${last.bucket.seq}`)
  if (last.spends === undefined || !equal(bucketID(last.spends.txid, last.spends.vout), last.bucket.id)) throw refuse('bucket', '', "the first token's id is not its outpoint's")
}

export function checkEntry(e: Entry, bucket: string, id: Uint8Array, key: string): void {
  if (e.bucket !== bucket || !equal(e.id, id) || e.key !== key) throw refuse('entry')
}

export function checkWriter(e: Entry, tok: Bucket): void {
  if (equal(e.writer, tok.owner) || tok.writers.some((w) => equal(w, e.writer))) return
  throw refuse('writer')
}

export function checkKind(e: Entry): void {
  if (e.kind !== KindPut) throw refuse('kind')
}

/** Steps 2 to 5 for an object answer; a head stops here. */
export function checkHead(tok: Bucket, key: string, c: Uint8Array | undefined, e: Entry | undefined, ctx: EntryContext | undefined, leaves: MapLeaf[]): ObjectResult {
  const [res] = checkObject(tok.count, tok.mapRoot, key, c, ctx, leaves)
  if (res !== 'found') return res
  checkEntry(e!, tok.bucket, tok.id, key)
  checkWriter(e!, tok)
  checkKind(e!)
  return 'found'
}
