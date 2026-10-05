/** A reader's checks that need no network (spec section 15), the twin of the Go reader.go. */
import { checkObject, type EntryContext, type MapLeaf, type ObjectResult } from './keymap.js'
import type { Bucket, Entry } from './records.js'
import { KindPut, refuse } from './storerec.js'
import { equal } from './util.js'

export interface Link {
  txid: Uint8Array
  bucket: Bucket
}

/** Step 1 on verified tokens, latest first: rollback, or bucket when the walk does not link. */
export function checkWalk(held: number, walk: Link[]): void {
  if (walk.length === 0) throw refuse('bucket', '', 'no token')
  const top = walk[0]!.bucket
  if (held >= 0 && top.seq < held) throw refuse('rollback')
  for (let i = 0; i + 1 < walk.length; i++) {
    const a = walk[i]!.bucket
    const b = walk[i + 1]!.bucket
    if (!equal(a.prev, walk[i + 1]!.txid) || a.seq !== b.seq + 1 || a.bucket !== b.bucket || !equal(a.owner, b.owner) || !equal(a.id, b.id)) {
      throw refuse('bucket', '', `token at seq ${a.seq} does not link`)
    }
  }
  const last = walk[walk.length - 1]!.bucket
  if (last.seq !== 0 && (held < 0 || last.seq > held)) throw refuse('bucket', '', `the walk stops at seq ${last.seq}`)
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
