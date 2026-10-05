/**
 * The topic managers of one bucket (spec section 12.1): `tm_bstore_<bucket>`,
 * the records topic, and `tm_bstoreblob_<bucket>`, the blob topic. The rules
 * are admit.ts's, the codec held to the golden vectors; this file is the
 * engine seam: it runs them on the submission and turns the verdict into
 * admittance instructions.
 *
 * The overlay engine records every transaction a topic manager returns
 * instructions for as applied, empty instructions included, and a later
 * submission of the same txid is a duplicate no topic manager is shown
 * again. A refusal returned as empty instructions would therefore be
 * permanent for that txid: anyone could suppress a genuine object by
 * submitting it first in a different BEEF, and a token refused because the
 * host's headers lagged would stay refused. So every refusal is raised, and
 * the engine records nothing and marks nothing spent.
 *
 * No rule reads another topic, another record or anything this topic holds
 * (the admission seam is not stable on this engine). The previous coins
 * decide only which held outputs an accepted transaction retains: every
 * one, so no admitted output is deleted when something spends it.
 */
import type { ChainTracker } from '@bsv/sdk'
import type { AdmittanceInstructions, ModuleHost, TopicManager } from '@lightwebinc/bcommon'
import { admit, type Admission } from './admit.js'
import { isBucket } from './names.js'
import { BlobPrefix, DefaultMaxBEEF, DefaultMaxSweepInputs, RecordsPrefix, StoreError } from './storerec.js'
import { toHex } from './util.js'

/**
 * Every label a host counts a refused submission by: the record rules of
 * spec sections 3.2, 4.2, 8.1 and 8.2 and the transaction rules of section
 * 12.1. A reader's labels and `query` are never an admission verdict.
 */
export const Reasons: readonly string[] = [
  'too-large',
  'cbor',
  'key-type',
  'magic',
  'missing',
  'type',
  'bucket',
  'identity',
  'seq',
  'writers',
  'changes',
  'count',
  'created',
  'consumed',
  'first',
  'objkey',
  'kind',
  'size',
  'profile',
  'content-type',
  'etag',
  'meta',
  'hold',
  'empty',
  'request',
  'outcome',
  'response-signature',
  'url',
  'shape',
  'beef',
  'mineable',
  'unlock',
  'lock',
  'signature',
  'funding',
  'unmined',
  'token-input',
  'bucket-id',
  'too-many',
  'piece',
  'not-bstore',
  'host-config',
  'range',
  'list',
]

/** What an admitted transaction was. */
export const AdmitKinds: ReadonlyArray<Admission['kind']> = ['entry', 'outcome', 'front', 'bucket-token', 'mined', 'sweep']

/** How many admitted txids a manager remembers to count each once. */
export const RecentAdmissions = 4096

/** Raised for every refusal: the engine counts it as a failed topic for this submission and stores nothing. */
export class Refused extends Error {
  constructor(
    readonly reason: string,
    detail: string,
  ) {
    super(`bstore: refused, ${reason}: ${detail}`)
    this.name = 'Refused'
  }
}

export class RecordsTopicManager implements TopicManager {
  readonly bucket: string
  private readonly recent = new Set<string>()

  constructor(
    readonly topic: string,
    private readonly host: ModuleHost,
    private readonly headers: ChainTracker,
    private readonly maxBEEF: number = DefaultMaxBEEF,
    private readonly maxSweepInputs: number = DefaultMaxSweepInputs,
  ) {
    if (!topic.startsWith(RecordsPrefix) || topic.startsWith(BlobPrefix)) throw new Error(`bstore: ${topic} is not a tm_bstore_ records topic`)
    this.bucket = topic.slice(RecordsPrefix.length)
    if (!isBucket(this.bucket)) throw new Error(`bstore: ${this.bucket} is not a bucket identifier <name>_<suffix>`)
    if (!Number.isSafeInteger(maxBEEF) || maxBEEF < DefaultMaxBEEF) throw new Error(`bstore: a BEEF bound of ${maxBEEF} is below ${DefaultMaxBEEF}`)
    if (!Number.isSafeInteger(maxSweepInputs) || maxSweepInputs < DefaultMaxSweepInputs) throw new Error(`bstore: a sweep input bound of ${maxSweepInputs} is below ${DefaultMaxSweepInputs}`)
  }

  async identifyAdmissibleOutputs(beef: number[], previousCoins: number[]): Promise<AdmittanceInstructions> {
    try {
      const a = await admit(Uint8Array.from(beef), previousCoins, { bucket: this.bucket, headers: this.headers, maxBEEF: this.maxBEEF, maxSweepInputs: this.maxSweepInputs })
      this.count(a.kind, toHex(a.txid))
      return { outputsToAdmit: a.outputs, coinsToRetain: a.retain }
    } catch (e) {
      if (!(e instanceof StoreError)) throw e
      this.host.metrics.inc('bstore_refused_total', { reason: e.label })
      this.host.log('tm_bstore refused', { topic: this.topic, reason: e.label, detail: e.message })
      throw new Refused(e.label, e.message)
    }
  }

  /**
   * Counts an admission once per transaction. The engine shows a manager a
   * txid it already applied only when two submissions of it race past its
   * duplicate check, or when the index dropped it and evicted the engine's
   * record, so `bstore_admitted_total` counts transactions not seen lately
   * and `bstore_admitted_repeats_total` the rest.
   */
  private count(kind: Admission['kind'], txid: string): void {
    if (this.recent.has(txid)) {
      this.host.metrics.inc('bstore_admitted_repeats_total', { kind })
      return
    }
    this.recent.add(txid)
    if (this.recent.size > RecentAdmissions) this.recent.delete(this.recent.values().next().value as string)
    this.host.metrics.inc('bstore_admitted_total', { kind })
  }

  /** Nothing is needed: a carrier carries its funding tree, a token its parent, and a mined object its own proof. */
  async identifyNeededInputs(): Promise<Array<{ txid: string; outputIndex: number }>> {
    return []
  }

  async getDocumentation(): Promise<string> {
    return [
      `# ${this.topic}`,
      '',
      `The records topic of the bstore bucket ${this.bucket} (spec section 12.1): object`,
      "entries, outcome records and front statements, each on a carrier that spends one output of",
      "its owner's mined funding tree; bucket tokens, each mined and published with its parent;",
      'advertisements and hold outputs, mined; and output 0 of mined sweeps. Each is admitted on',
      "its own bytes and the host's headers; no other record and nothing the topic holds is read.",
      'Pieces belong on the blob topic and are never admitted. Every refusal is raised.',
    ].join('\n')
  }

  async getMetaData(): Promise<{ name: string; shortDescription: string }> {
    return { name: this.topic, shortDescription: `Admits the records of the bstore bucket ${this.bucket}.` }
  }
}

/**
 * A bucket's blob topic on a host: it carries pieces only, and no host
 * admits a piece or holds bytes (spec sections 5 and 12.1), so every
 * submission is refused, raised and counted as `piece`. Mounted only where
 * the operator names the topic, so that the host's default manager never
 * admits anything on it.
 */
export class BlobTopicManager implements TopicManager {
  readonly bucket: string

  constructor(
    readonly topic: string,
    private readonly host: ModuleHost,
  ) {
    if (!topic.startsWith(BlobPrefix)) throw new Error(`bstore: ${topic} is not a tm_bstoreblob_ topic`)
    this.bucket = topic.slice(BlobPrefix.length)
    if (!isBucket(this.bucket)) throw new Error(`bstore: ${this.bucket} is not a bucket identifier <name>_<suffix>`)
  }

  async identifyAdmissibleOutputs(): Promise<AdmittanceInstructions> {
    this.host.metrics.inc('bstore_refused_total', { reason: 'piece' })
    throw new Refused('piece', 'a host admits nothing on a blob topic: pieces go to providers')
  }

  async identifyNeededInputs(): Promise<Array<{ txid: string; outputIndex: number }>> {
    return []
  }

  async getDocumentation(): Promise<string> {
    return [`# ${this.topic}`, '', `The blob topic of the bstore bucket ${this.bucket}: pieces, which providers hold. A host admits nothing here.`].join('\n')
  }

  async getMetaData(): Promise<{ name: string; shortDescription: string }> {
    return { name: this.topic, shortDescription: `Refuses everything: the blob topic of ${this.bucket} is for providers.` }
  }
}
