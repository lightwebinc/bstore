/**
 * The registry values, bounds and labels of the bstore contract, the twin
 * of the Go storerec.go. Every refusal is a StoreError carrying the label
 * its spec section names and, where a label covers several rules, the
 * codec's finer rule, both pinned by the vectors.
 */
import type { WalletProtocol } from '@bsv/sdk'

export const ProtocolName = 'bstore'
export const Protocol: WalletProtocol = [1, ProtocolName]
export const KeyBucket = 'bucket'
export const KeyChain = 'chain'
export const KeyEntry = 'entry'
export const KeyPiece = 'piece'
export const KeyOutcome = 'outcome'
export const KeyFront = 'front'
export const KeyHold = 'hold'
export const KeyChallenge = 'challenge'
export const KeyFund = 'fund'
export const KeyIDs = [KeyBucket, KeyChain, KeyEntry, KeyPiece, KeyOutcome, KeyFront, KeyHold, KeyChallenge, KeyFund]

export const RecordsPrefix = 'tm_bstore_'
export const BlobPrefix = 'tm_bstoreblob_'
export const LookupService = 'ls_bstore'
export const HostTerms = '/ls_bstore/terms'
export const FrontSuffix = '/bstore/v1'
export const DomainBucketID = 'bstore bucket id v1'

export const AdProtocol: WalletProtocol = [2, 'uhrp advertisement']
export const AdKeyID = '1'
export const SettleProtocol: WalletProtocol = [2, '3241645161d8']

export const Baskets = ['bstore fund', 'bstore entry funding', 'bstore piece funding', 'bstore outcome funding',
  'bstore front funding', 'bstore chain funding', 'bstore chain', 'bstore hold', 'bstore kill tombstone']

const b = (...x: number[]): Uint8Array => Uint8Array.from(x)
export const TagPrefix = b(0x73, 0x74)
export const TagToken = b(0x73, 0x74, 0x01)
export const TagFunding = b(0x73, 0x74, 0x02)
export const TagHold = b(0x73, 0x74, 0x03)
export const MagicBucket = b(0x73, 0x74, 0x62, 0x01)
export const MagicEntry = b(0x73, 0x74, 0x65, 0x01)
export const MagicPiece = b(0x73, 0x74, 0x70, 0x01)
export const MagicOutcome = b(0x73, 0x74, 0x6f, 0x01)
export const MagicFront = b(0x73, 0x74, 0x66, 0x01)
export const MagicRequest = b(0x73, 0x74, 0x71, 0x01)
export const MagicResponse = b(0x73, 0x74, 0x72, 0x01)
export const MagicNotTaken = b(0x73, 0x74, 0x6e, 0x01)

export const MaxSafe = Number.MAX_SAFE_INTEGER
export const MinTime = 1
export const MaxTime = 253402300799999
export const MaxBucketRecord = 65536
export const MaxWriters = 16
export const MaxChanges = 1024
export const MaxConsumed = 1024
export const MaxKeysInMap = 1048576
export const MaxEntry = 8192
export const MaxObjectKey = 1024
export const MaxSize = 2 ** 43
export const MinContentType = 3
export const MaxContentType = 127
export const MaxETag = 64
export const MaxMetaPairs = 32
export const MaxMetaBytes = 2048
export const MaxMetaKey = 128
export const MaxMetaValue = 1024
export const MaxKind = 255
export const MaxPiece = 4194304
export const MaxPieceRecord = 4194560
export const MaxOutcome = 4096
export const MaxFront = 2048
export const MinURL = 12
export const MaxURL = 512
export const MaxRequest = 512
export const MaxC = 1024
export const NonceLen = 16
export const MaxSignedResponse = 8388608
export const MaxNotTaken = 512
export const MaxNoteReason = 64
export const MaxSatoshis = 2100000000000000
export const MaxT = 16
export const MaxU32 = 2 ** 32 - 1
export const MaxCheckC = 1024
export const ProofProfile = 1
export const MaxEntryContext = 1024
export const MaxTokenContext = 4096
export const MaxMapPath = 20
export const PageList = 256
export const PageExport = 512
export const SegmentSize = 4096
export const BlobSize = 4194304
export const SegmentsPerBlob = BlobSize / SegmentSize
export const SeedWindow = 6
export const MaxAdsPerTx = 16
export const MaxAdURL = 2048
export const SuffixLen = 10
export const MaxName = 25
export const MinBucket = 1 + 1 + SuffixLen
export const MaxBucket = MaxName + 1 + SuffixLen
export const MaxCarrierBEEF = 81920
export const MaxTokenBEEF = 196608
export const MaxSweepBEEF = 196608
export const MaxMinedBEEF = 65536
export const DefaultMaxBEEF = 262144
export const DefaultMaxSweepInputs = 1024

// Record keys.
export const BK = { magic: 0, bucket: 1, owner: 2, id: 3, seq: 4, prev: 5, writers: 6, changes: 7, count: 8, mapRoot: 9, created: 10, consumed: 11, last: 11 }
export const EK = {
  magic: 0, bucket: 1, id: 2, writer: 3, key: 4, kind: 5, created: 6, size: 7, root: 8, contentHash: 9, proofRoot: 10,
  proofProfile: 11, contentType: 12, etag: 13, meta: 14, hold: 15, last: 15,
}
export const PK = { magic: 0, bucket: 1, writer: 2, root: 3, piece: 4, last: 4 }
export const OK = { magic: 0, bucket: 1, buyer: 2, created: 3, request: 4, outcome: 5, response: 6, signature: 7, last: 7 }
export const FK = { magic: 0, bucket: 1, front: 2, advertiser: 3, url: 4, created: 5, last: 5 }
export const QK = { magic: 0, root: 1, proofRoot: 2, size: 3, provider: 4, answerer: 5, seedHeight: 6, seedHash: 7, nonce: 8, c: 9, range: 10, last: 10 }
export const RK = { magic: 0, request: 1, samples: 2, answerer: 3, last: 3 }
export const NK = { magic: 0, payment: 1, request: 2, reason: 3, time: 4, answerer: 5, last: 5 }

export const KindPut = 1
export const KindDelete = 2
export const OutcomePass = 1
export const OutcomeInvalid = 2
export const OutcomeNoAnswer = 3
export const OutcomeUnretrievable = 4

/** SHA-256 of no bytes. */
export const EmptyHash = Uint8Array.from([0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
  0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55])
/** The CHIRP Profile 1 root of empty content. */
export const EmptyRoot = Uint8Array.from([0x04, 0x03, 0x64, 0x0d, 0x63, 0x5f, 0xd2, 0x7b, 0x6c, 0x71, 0x9d, 0x2b, 0x81, 0xdb, 0x85, 0x34,
  0x83, 0xff, 0x8d, 0x0f, 0xb4, 0x6f, 0x7b, 0x90, 0xce, 0x9f, 0x9d, 0x7e, 0x9a, 0x27, 0x29, 0xee])

/** A refusal: its label and, where one label covers several rules, the rule. */
export class StoreError extends Error {
  constructor(
    readonly label: string,
    readonly rule = '',
    detail?: string,
  ) {
    super(`storerec: ${label}${rule !== '' ? ` (${rule})` : ''}${detail !== undefined ? `: ${detail}` : ''}`)
  }
}

export const refuse = (label: string, rule = '', detail?: string): StoreError => new StoreError(label, rule, detail)

/** The label of a refusal, "ok" for none, "other" for any other error. */
export function reasonOf(e: unknown): string {
  return e instanceof StoreError ? e.label : 'other'
}

export function ruleOf(e: unknown): string {
  return e instanceof StoreError ? e.rule : ''
}

/** Runs f and returns its label, "ok" when it returns. */
export function reasonFor(f: () => unknown): string {
  try {
    f()
  } catch (e) {
    return reasonOf(e)
  }
  return 'ok'
}
