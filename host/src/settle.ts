/** Settlement and the detection tables, twins of the Go settle.go and detect.go. */
import { Utils } from '@bsv/sdk'
import { AnyoneKey } from './names.js'
import { bigEndian, equal, toHex } from './util.js'

/** BRC-178's rule: sqrt weights in binary64, floors, the remainder to rank 1. */
export function payouts(r: number, k: number): number[] {
  if (k < 1) return []
  const w: number[] = []
  let s = 0
  for (let i = 1; i <= k; i++) {
    w.push(Math.sqrt(k - i + 1))
    s += w[i - 1]!
  }
  const out = w.map((x) => Math.floor((r * x) / s))
  out[0]! += r - out.reduce((a, b) => a + b, 0)
  return out
}

export const derivationPrefix = (c: Uint8Array): string => Utils.toBase64(Array.from(c))
export const derivationSuffix = (rank: number): string => Utils.toBase64(Array.from(bigEndian(rank, 2)))

export interface Ad {
  txid: Uint8Array
  advertiser: Uint8Array
  root: Uint8Array
  expiry: number
  spent: boolean
  spentBy: Ad[]
}

/** Follows renewal successors; true when the last is unspent. */
export function live(a: Ad): boolean {
  let cur = a
  for (let hops = 0; hops < 1 << 16; hops++) {
    if (!cur.spent) return true
    const next = cur.spentBy.find((s) => equal(s.advertiser, cur.advertiser) && equal(s.root, cur.root) && s.expiry > cur.expiry)
    if (next === undefined) return false
    cur = next
  }
  return false
}

export interface Candidate {
  ad: Ad
  firstSeen: number
  passed: boolean
}

function displayLess(a: Uint8Array, b: Uint8Array): number {
  for (let i = 31; i >= 0; i--) if (a[i] !== b[i]) return a[i]! - b[i]!
  return 0
}

export function rank(cands: Candidate[]): Candidate[] {
  const out = [...cands].sort((x, y) => (x.firstSeen !== y.firstSeen ? x.firstSeen - y.firstSeen : displayLess(x.ad.txid, y.ad.txid)))
  const seen = new Set<string>()
  return out.filter((c) => {
    const k = toHex(c.ad.advertiser)
    if (seen.has(k) || equal(c.ad.advertiser, AnyoneKey)) return false
    seen.add(k)
    return true
  })
}

export interface Payee {
  rank: number
  advertiser: Uint8Array
  ad: Uint8Array
  satoshis: number
  prefix: string
  suffix: string
}

export interface Settlement {
  settled: boolean
  dropped: Uint8Array[]
  payees: Payee[]
}

export function settle(c: Uint8Array, r: number, t: number, K: number, cands: Candidate[]): Settlement {
  const passed = rank(cands).filter((x) => x.passed)
  const s: Settlement = { settled: false, dropped: [], payees: [] }
  const keep: Candidate[] = []
  for (const x of passed) {
    if (keep.length === K) break
    if (!live(x.ad)) {
      s.dropped.push(x.ad.advertiser)
      continue
    }
    keep.push(x)
  }
  if (keep.length < t || keep.length === 0) return s
  s.settled = true
  const pay = payouts(r, keep.length)
  keep.forEach((x, i) => {
    if (pay[i] === 0) return
    s.payees.push({ rank: i + 1, advertiser: x.ad.advertiser, ad: x.ad.txid, satoshis: pay[i]!, prefix: derivationPrefix(c), suffix: derivationSuffix(i + 1) })
  })
  return s
}

// ---- detection ----

export const DetectionCs = [1, 8, 16, 30, 60, 100, 230, 459, 1000]
export const DetectionLosses = [0.5, 0.25, 0.1, 0.05, 0.01, 0.001]
export const ConfidenceLevels = [0.9, 0.99, 0.999]

export const detect = (c: number, f: number): number => 1 - Math.pow(1 - f, c)

export function samplesFor(f: number, conf: number): number {
  let c = Math.ceil(Math.log(1 - conf) / Math.log(1 - f))
  while (c > 1 && detect(c - 1, f) >= conf) c--
  while (detect(c, f) < conf) c++
  return c
}

function pct(p: number): string {
  const v = Math.round(p * 1000) / 10
  return v > 99.9 ? '>99.9' : v.toFixed(1)
}

const lossLabel = (f: number): string => `${parseFloat((f * 100).toPrecision(12))} %`

/** Spec section 7.6's two tables, as the spec prints them. */
export function detectionTables(): [string, string] {
  let a = '| `c` |' + DetectionLosses.map((f, i) => ` ${lossLabel(f)}${i === 0 ? ' lost' : ''} |`).join('') + '\n'
  a += '|' + ' --- |'.repeat(DetectionLosses.length + 1) + '\n'
  for (const c of DetectionCs) a += `| ${c} |` + DetectionLosses.map((f) => ` ${pct(detect(c, f))} |`).join('') + '\n'
  let s = '| Lost |' + ConfidenceLevels.map((c) => ` ${parseFloat((c * 100).toPrecision(12))} % |`).join('') + '\n'
  s += '|' + ' --- |'.repeat(ConfidenceLevels.length + 1) + '\n'
  for (const f of DetectionLosses) s += `| ${lossLabel(f)} |` + ConfidenceLevels.map((c) => ` ${samplesFor(f, c).toLocaleString('en-US')} |`).join('') + '\n'
  return [a, s]
}
