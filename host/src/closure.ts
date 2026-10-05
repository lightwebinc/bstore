/**
 * bstore's checks of a CHIRP Profile 1 closure and the proof root, the
 * twin of the Go closure.go and proof.go: the root's rules, bcommon's
 * closure check with a 2^43-byte bound behind a fetch that takes only bytes
 * hashing to the object asked for, and the proof root recomputed.
 */
import { ChirpError, Profile1, SegmentWriter, decodeRoot, segmentRoot, verifyClosure as chirpVerify, type ChirpClosure, type ChirpFetch } from '@lightwebinc/bcommon'
import { MaxSize, SegmentSize, refuse } from './storerec.js'
import type { Entry } from './records.js'
import { equal, sha256 } from './util.js'

export const ClosureMaxLength = BigInt(MaxSize)

export const proofRoot = (content: Uint8Array): Uint8Array => segmentRoot(content, SegmentSize)

/** The root's bytes held to bstore's rules (label root, the rule the step). */
export function checkRoot(rootBytes: Uint8Array, root: Uint8Array, size: number, contentHash: Uint8Array): void {
  if (!equal(sha256(rootBytes), root)) throw refuse('root', 'hash')
  let r
  try {
    r = decodeRoot(rootBytes)
  } catch (e) {
    if (e instanceof ChirpError) throw refuse('root', e.code)
    throw e
  }
  if (r.profile !== Profile1) throw refuse('root', 'profile')
  if (r.extensions.length !== 0) throw refuse('root', 'extension')
  if (r.length !== BigInt(size)) throw refuse('root', 'length')
  if (!equal(r.contentHash, contentHash)) throw refuse('root', 'content-hash')
}

/** bcommon's closure check with the 2^43-byte bound, its refusals as labels. */
export async function verifyCHIRP(rootHash: Uint8Array, fetch: ChirpFetch): Promise<ChirpClosure> {
  let wrong = false
  const checked = async (h: Uint8Array): Promise<Uint8Array> => {
    const o = await fetch(h)
    if (!equal(sha256(o), h)) {
      wrong = true
      throw new Error('served other bytes')
    }
    return o
  }
  try {
    return await chirpVerify(rootHash, checked, { maxLength: ClosureMaxLength })
  } catch (e) {
    if (!(e instanceof ChirpError)) throw e
    if (e.code === 'missing' && wrong) throw refuse('closure', 'hash')
    if (e.code === 'missing' || e.code === 'hash') throw refuse('closure', e.code)
    if (e.code === 'content-hash') throw refuse('content', e.code)
    throw refuse('root', e.code)
  }
}

/** A provider's check before it holds, and a reader's steps 6 to 9 on a whole read. */
export async function verifyClosure(e: Entry, fetch: ChirpFetch, proof: boolean): Promise<ChirpClosure> {
  let rb: Uint8Array
  try {
    rb = await fetch(e.root)
  } catch {
    throw refuse('closure', 'missing')
  }
  if (!equal(sha256(rb), e.root)) throw refuse('closure', 'hash')
  checkRoot(rb, e.root, e.size, e.contentHash)
  const c = await verifyCHIRP(e.root, fetch)
  if (proof) {
    const w = new SegmentWriter(SegmentSize)
    for (const bl of c.blobs) {
      let o: Uint8Array
      try {
        o = await fetch(bl.hash)
      } catch {
        throw refuse('closure', 'missing')
      }
      if (!equal(sha256(o), bl.hash)) throw refuse('closure', 'missing')
      w.write(o)
    }
    if (!equal(w.root(), e.proofRoot)) throw refuse('proof-root')
  }
  return c
}
