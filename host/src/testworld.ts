/**
 * A bucket world for the module's tests: a served chain's owner, a writer
 * the owner names, a stranger, a provider, a buyer and a front, over one
 * local chain, and readers of the answers the way a reader checks them.
 */
import { firstPush } from '@lightwebinc/bcommon'
import { readBucketOutput } from './admit.js'
import { checkHead } from './reader.js'
import { checkPage, decodeEntryContext, decodeTokenContext, type ObjectResult } from './keymap.js'
import { decodeEntry, type Bucket } from './records.js'
import { PageExport, PageList } from './storerec.js'
import { Buyer, Chain, FrontSim, OwnerSim, Provider, T0, Writer, commitment } from './testmint.js'
import { Clock, bucket, outputs, setup, type Answered, type Rig, type SetupOptions } from './testhost.js'

export const Day = 86_400_000
export const Hour = 3_600_000

export interface World {
  r: Rig
  chain: Chain
  clock: Clock
  owner: OwnerSim
  alice: Writer
  owned: Writer
  bob: Writer
  prov: Provider
  prov2: Provider
  buyer: Buyer
  front: FrontSim
  /** The chain's question members. */
  chainQ: { bucket: string; owner: string; id: string }
}

/** A world whose owner's chain the host serves (unless served is false), its clock a day after T0. */
export async function world(o: { served?: boolean; seed?: string; setup?: SetupOptions } = {}): Promise<World> {
  const seed = o.seed ?? 'w'
  const chain = o.setup?.chain ?? new Chain(80000)
  const clock = o.setup?.clock ?? new Clock(T0 + Day)
  const owner = new OwnerSim(bucket, `${seed} owner`, chain)
  const r = await setup({ ...o.setup, chain, clock, config: { ...o.setup?.config, served: o.served === false ? [] : [owner.served] } })
  // The owner writes entries under its own key too.
  const owned = new Writer(bucket, `${seed} owner`, chain)
  return {
    r,
    chain,
    clock,
    owner,
    alice: new Writer(bucket, `${seed} alice`, chain),
    owned,
    bob: new Writer(bucket, `${seed} bob`, chain),
    prov: new Provider(`${seed} provider`, chain),
    prov2: new Provider(`${seed} other provider`, chain),
    buyer: new Buyer(bucket, `${seed} buyer`, chain),
    front: new FrontSim(bucket, `${seed} front`, chain),
    chainQ: { bucket, owner: owner.hex, id: owner.idHex },
  }
}

/** The bucket record of a token output. */
export function tokenOf(a: Answered): Bucket {
  const out = a.tx.outputs[a.vout]!
  const t = readBucketOutput(Uint8Array.from(out.lockingScript.toBinary()), out.satoshis, a.vout)
  if (t === undefined) throw new Error(`${a.txid}.${a.vout} is not a token`)
  return t.bucket
}

/** An object answer, checked as a reader checks it (spec section 15.2, steps 2 to 5): what it proves, and the entry's txid when found. */
export function readObject(outs: Answered[], key: string): { result: ObjectResult; entry?: string; token: string } {
  const tok = tokenOf(outs[0]!)
  const leaves = decodeTokenContext(Uint8Array.from(outs[0]!.context ?? []))
  const e = outs[1]
  if (e === undefined) return { result: checkHead(tok, key, undefined, undefined, undefined, leaves), token: outs[0]!.txid }
  const entry = decodeEntry(firstPush(Uint8Array.from(e.tx.outputs[0]!.lockingScript.toBinary()))!)
  const res = checkHead(tok, key, commitment(e.tx), entry, decodeEntryContext(Uint8Array.from(e.context ?? [])), leaves)
  return { result: res, entry: e.txid, token: outs[0]!.txid }
}

/** A list or export page, checked as a reader checks it (spec section 11.3): its keys, and the leaves' roles and keys. */
export function readPage(outs: Answered[], p: string, a: string, exported = false): { keys: string[]; leaves: Array<[number, string]>; txids: string[] } {
  const tok = tokenOf(outs[0]!)
  const leaves = decodeTokenContext(Uint8Array.from(outs[0]!.context ?? []))
  const entries = outs.slice(1).map((o) => {
    const e = decodeEntry(firstPush(Uint8Array.from(o.tx.outputs[0]!.lockingScript.toBinary()))!)
    return { key: e.key, c: commitment(o.tx), ctx: decodeEntryContext(Uint8Array.from(o.context ?? [])) }
  })
  checkPage(tok.count, tok.mapRoot, p, a, exported ? PageExport : PageList, entries, leaves)
  return { keys: entries.map((e) => e.key), leaves: leaves.map((l) => [l.role, l.key]), txids: outs.slice(1).map((o) => o.txid) }
}

export const object = async (w: World, key: string): Promise<{ result: ObjectResult; entry?: string; token: string } | undefined> => {
  const outs = await outputs(w.r, { ...w.chainQ, key })
  return outs.length === 0 ? undefined : readObject(outs, key)
}

export const list = async (w: World, prefix: string, after: string): Promise<ReturnType<typeof readPage>> => readPage(await outputs(w.r, { ...w.chainQ, prefix, after }), prefix, after)
