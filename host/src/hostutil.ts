/** Helpers the host module needs beside the codec's (util.ts). */
import { Utils } from '@bsv/sdk'
import { concat } from './util.js'

export const toBase64 = (b: Uint8Array): string => Utils.toBase64(Array.from(b))

const b64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/

/**
 * Decodes s when it is non-empty standard base64 with padding and zero pad
 * bits, the one spelling of its bytes; undefined otherwise.
 */
export function strictBase64(s: string): Uint8Array | undefined {
  if (s.length === 0 || !b64.test(s)) return undefined
  const b = Uint8Array.from(Utils.toArray(s, 'base64'))
  return Utils.toBase64(Array.from(b)) === s ? b : undefined
}

/** Reads a response body to at most max bytes, or throws: the bound every answer this code reads is held to. */
export async function readCapped(res: Response, max: number): Promise<string> {
  if (res.body === null) return ''
  const reader = res.body.getReader()
  const parts: Uint8Array[] = []
  let n = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    n += value.length
    if (n > max) {
      await reader.cancel()
      throw new Error(`the answer is over ${max} bytes`)
    }
    parts.push(value)
  }
  return new TextDecoder().decode(concat(...parts))
}

/** `<txid>.<vout>`, the txid in display order: an outpoint as the index and BRC-100 name it. */
export const outpoint = (txid: string, vout: number): string => `${txid}.${vout}`

/** The sort key of a time cursor and of a dated record: created, then txid in display order. */
export const timeKey = (created: number, txid: string): string => `${created.toString().padStart(16, '0')}:${txid}`

/** The sort key of a time cursor `<created>:<txid>` as a question gives it. */
export function keyOfCursor(q: string): string {
  const i = q.indexOf(':')
  return timeKey(Number(q.slice(0, i)), q.slice(i + 1))
}

/** The txid of a time key. */
export const txidOfKey = (k: string): string => k.slice(17)

/** The sort key of an output of a transaction among others: txid, then output index. */
export const outKey = (txid: string, vout: number): string => `${txid}:${vout.toString().padStart(10, '0')}`

/** The first outKey of a transaction whose txid sorts after txid ('' for every one). */
export const afterTxid = (txid: string): string => (txid === '' ? '' : `${txid};`)
