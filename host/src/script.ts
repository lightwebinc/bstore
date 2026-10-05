/** The classifier and the tagged scripts, the twin of the Go script.go. */
import { bytesEqual, claimsRecord, firstPush, pushDropScript, readerLockingKey } from '@lightwebinc/bcommon'
import { MagicEntry, MagicFront, MagicOutcome, MagicPiece, Protocol, TagFunding, TagHold, TagToken } from './storerec.js'
import { toHex } from './util.js'

export type Claim = '' | 'bucket-token' | 'entry' | 'outcome' | 'front' | 'piece' | 'funding' | 'hold'

export const claims = (c: Claim): boolean => c === 'bucket-token' || c === 'entry' || c === 'outcome' || c === 'front' || c === 'piece'

export function classify(s: Uint8Array): Claim {
  const p = firstPush(s)
  if (p === undefined) return ''
  if (bytesEqual(p, TagToken)) return 'bucket-token'
  if (claimsRecord(p, MagicEntry)) return 'entry'
  if (claimsRecord(p, MagicOutcome)) return 'outcome'
  if (claimsRecord(p, MagicFront)) return 'front'
  if (claimsRecord(p, MagicPiece)) return 'piece'
  if (s.length === 40 && s[35] === 0x03 && s[39] === 0x75 && bytesEqual(p, TagFunding)) return 'funding'
  if (s.length === 40 && s[35] === 0x03 && s[39] === 0x75 && bytesEqual(p, TagHold)) return 'hold'
  return ''
}

export const fundingScript = (key: Uint8Array): Uint8Array => pushDropScript(key, [TagFunding])
export const holdScript = (key: Uint8Array): Uint8Array => pushDropScript(key, [TagHold])

/** The key an identity's output is locked to under a key id of [1, "bstore"], counterparty anyone. */
export const lockingKey = (keyID: string, identity: Uint8Array): Uint8Array =>
  Uint8Array.from(readerLockingKey(Protocol, keyID, toHex(identity)).encode(true) as number[])
