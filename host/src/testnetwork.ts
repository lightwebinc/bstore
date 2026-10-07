/**
 * A network for tests of payment acceptance: arcade and the node's asset
 * service in memory, over a test chain that can mine what it was sent. Not
 * part of the module the host loads.
 */
import type { IncomingMessage, ServerResponse } from 'node:http'
import { MerklePath, Transaction } from '@bsv/sdk'
import { BroadcastRefused, type ArcadeStatus, type Broadcaster, type NodeView } from '@lightwebinc/bcommon/host'

/** How arcade answers what it is sent. */
export type ArcadeMode = 'accept' | 'double-spend' | 'reject' | 'refuse-http' | 'silent' | 'down'

interface Mineable {
  mine(tx: Transaction): Transaction
}

export class TestNetwork implements Broadcaster, NodeView {
  mode: ArcadeMode = 'accept'
  /** The node cannot say whether an output is spent. */
  spendUnknown = false
  readonly sent: string[] = []
  private readonly txs = new Map<string, Transaction>()
  private readonly statuses = new Map<string, ArcadeStatus>()
  private readonly spent = new Map<string, string>()
  private readonly proofs = new Map<string, MerklePath>()

  constructor(private readonly chain: Mineable) {}

  async submit(ef: Uint8Array): Promise<ArcadeStatus> {
    if (this.mode === 'down') throw new Error('arcade unreachable')
    const tx = Transaction.fromEF(Array.from(ef))
    const txid = tx.id('hex')
    this.sent.push(txid)
    if (this.mode === 'refuse-http') throw new BroadcastRefused('arcade answered 465: fee too low')
    if (this.statuses.has(txid)) return this.statuses.get(txid)!
    let st: ArcadeStatus
    const competing = tx.inputs.map((i) => this.spent.get(`${i.sourceTXID}.${i.sourceOutputIndex}`)).filter((s): s is string => s !== undefined && s !== txid)
    if (this.mode === 'reject') st = { txid, txStatus: 'REJECTED', extraInfo: 'missing inputs' }
    else if (this.mode === 'double-spend' || competing.length > 0) st = { txid, txStatus: 'DOUBLE_SPEND_ATTEMPTED', competingTxs: competing.length > 0 ? competing : ['00'.repeat(32)] }
    else if (this.mode === 'silent') st = { txid, txStatus: 'RECEIVED' }
    else st = { txid, txStatus: 'SEEN_ON_NETWORK' }
    this.statuses.set(txid, st)
    this.txs.set(txid, tx)
    if (st.txStatus === 'SEEN_ON_NETWORK') for (const i of tx.inputs) this.spent.set(`${i.sourceTXID}.${i.sourceOutputIndex}`, txid)
    return st
  }

  async status(txid: string): Promise<ArcadeStatus | undefined> {
    return this.statuses.get(txid)
  }

  async spender(txid: string, vout: number): Promise<string> {
    if (this.spendUnknown) throw new Error('the node answers status "NOT_FOUND"')
    return this.spent.get(`${txid}.${vout}`) ?? ''
  }

  async proof(txid: string): Promise<MerklePath | undefined> {
    return this.proofs.get(txid)
  }

  /** Mines txid, which was sent: the node then serves its proof. */
  mine(txid: string): void {
    const tx = this.txs.get(txid)
    if (tx === undefined) throw new Error(`${txid} was never sent`)
    const copy = Transaction.fromEF(tx.toEF())
    this.chain.mine(copy)
    this.proofs.set(txid, copy.merklePath!)
    this.statuses.set(txid, { txid, txStatus: 'MINED' })
  }

  /** Another transaction spends outpoint `<txid>.<vout>` (a double spend that won). */
  spendElsewhere(outpoint: string, by = 'ab'.repeat(32)): void {
    this.spent.set(outpoint, by)
  }
}

/**
 * The network over HTTP, as the host reads it: arcade's POST /tx and
 * GET /tx/{txid} under /arcade, and the asset service's
 * /api/v1/utxos/{txid}/json and /api/v1/merkle_proof/{txid}. Answers false
 * for any other path.
 */
export async function serveNetwork(net: TestNetwork, req: IncomingMessage, res: ServerResponse): Promise<boolean> {
  const url = req.url ?? ''
  const send = (status: number, body: unknown, type = 'application/json'): true => {
    res.writeHead(status, { 'content-type': type })
    res.end(type === 'application/json' ? JSON.stringify(body) : (body as Buffer))
    return true
  }
  if (req.method === 'POST' && url === '/arcade/tx') {
    const chunks: Buffer[] = []
    for await (const c of req) chunks.push(c as Buffer)
    try {
      return send(200, await net.submit(new Uint8Array(Buffer.concat(chunks))))
    } catch (e) {
      return send(e instanceof BroadcastRefused ? 465 : 503, { title: String(e) })
    }
  }
  let m = /^\/arcade\/tx\/([0-9a-f]{64})$/.exec(url)
  if (m !== null) {
    const st = await net.status(m[1]!)
    return st === undefined ? send(404, {}) : send(200, st)
  }
  m = /^\/api\/v1\/utxos\/([0-9a-f]{64})\/json$/.exec(url)
  if (m !== null) {
    const out: unknown[] = []
    for (let vout = 0; vout < 16; vout++) {
      try {
        const by = await net.spender(m[1]!, vout)
        out.push(by === '' ? { txid: m[1], vout, status: 'OK' } : { txid: m[1], vout, status: 'SPENT', spendingData: { txId: by } })
      } catch {
        out.push({ txid: m[1], vout, status: 'NOT_FOUND' })
      }
    }
    return send(200, out)
  }
  m = /^\/api\/v1\/merkle_proof\/([0-9a-f]{64})$/.exec(url)
  if (m !== null) {
    const mp = await net.proof(m[1]!)
    return mp === undefined ? send(404, {}) : send(200, Buffer.from(mp.toBinary()), 'application/octet-stream')
  }
  return false
}
