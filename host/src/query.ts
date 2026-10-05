/** Lookup questions (spec sections 11.1 and 11.2), the twin of the Go query.go. */
import { fromHex } from './util.js'
import { isBucket, isKey, isObjectKey, oneLine } from './names.js'
import { MaxObjectKey, MaxSafe, MaxU32, refuse } from './storerec.js'
import { isLowerHex, utf8 } from './util.js'

export interface Class {
  name: string
  members: string[]
  free: boolean
  served: boolean
}

const c = (name: string, members: string[], free: boolean, served: boolean): Class => ({ name, members, free, served })

export const Classes: Class[] = [
  c('bucket', ['bucket', 'owner', 'id'], true, false),
  c('bucket-seq', ['bucket', 'owner', 'id', 'seq'], true, false),
  c('object', ['bucket', 'owner', 'id', 'key'], true, true),
  c('list', ['bucket', 'owner', 'id', 'prefix', 'after'], true, true),
  c('pending', ['bucket', 'owner', 'id', 'pending'], true, true),
  c('unanchored', ['bucket', 'owner', 'id', 'writer', 'after'], true, true),
  c('entry', ['bucket', 'entry'], true, false),
  c('ads', ['bucket', 'root', 'after'], true, false),
  c('ad', ['bucket', 'root', 'advertiser'], true, false),
  c('fronts', ['bucket', 'advertiser', 'after'], true, false),
  c('front', ['bucket', 'advertiser', 'front'], true, false),
  c('hold', ['bucket', 'hold'], true, false),
  c('outcomes', ['bucket', 'answerer'], true, false),
  c('outcomes-of', ['bucket', 'provider'], true, false),
  c('outcomes-by', ['bucket', 'answerer', 'buyer'], true, false),
  c('outcomes-of-by', ['bucket', 'provider', 'buyer'], true, false),
  c('sweep', ['bucket', 'spent'], true, false),
  c('history', ['bucket', 'owner', 'id', 'history', 'before'], false, true),
  c('chain', ['bucket', 'owner', 'chain', 'after'], false, false),
  c('export', ['bucket', 'owner', 'export', 'after'], false, true),
  c('outcome-history', ['bucket', 'outcome-history', 'before'], false, false),
  c('outcome-history-of', ['bucket', 'outcome-history-of', 'before'], false, false),
]

function decimal(s: string): boolean {
  if (s.length === 0 || s.length > 16 || (s.length > 1 && s[0] === '0') || !/^[0-9]+$/.test(s)) return false
  return Number(s) <= MaxSafe
}

const keyHex = (s: string): boolean => isLowerHex(s, 66) && isKey(fromHex(s))
const timeCursor = (s: string): boolean => {
  const i = s.indexOf(':')
  return i >= 0 && decimal(s.slice(0, i)) && isLowerHex(s.slice(i + 1), 64)
}
function outpoint(s: string): boolean {
  const i = s.indexOf('.')
  if (i < 0) return false
  const t = s.slice(0, i)
  const v = s.slice(i + 1)
  if (!isLowerHex(t, 64) || v.length === 0 || v.length > 10 || (v.length > 1 && v[0] === '0') || !/^[0-9]+$/.test(v)) return false
  return Number(v) <= MaxU32
}
const cursorText = (v: string): boolean => utf8(v).length <= MaxObjectKey && oneLine(v)

function checkValue(cls: string, name: string, v: string): boolean {
  switch (name) {
    case 'bucket':
      return isBucket(v)
    case 'id':
    case 'export':
    case 'chain':
    case 'root':
    case 'entry':
      return isLowerHex(v, 64)
    case 'owner':
    case 'writer':
    case 'advertiser':
    case 'front':
    case 'answerer':
    case 'provider':
    case 'buyer':
    case 'outcome-history':
    case 'outcome-history-of':
      return keyHex(v)
    case 'key':
    case 'pending':
    case 'history':
      return isObjectKey(v)
    case 'prefix':
      return cursorText(v)
    case 'hold':
    case 'spent':
      return outpoint(v)
    case 'seq':
      return decimal(v)
    case 'before':
      return cls === 'history' ? decimal(v) : timeCursor(v)
    case 'after':
      if (cls === 'list' || cls === 'export') return cursorText(v)
      if (cls === 'unanchored' || cls === 'ads' || cls === 'fronts') return v === '' || isLowerHex(v, 64)
      if (cls === 'chain') return decimal(v)
  }
  return false
}

export interface Question {
  class: string
  values: Record<string, string>
}

/** Validates a question given as JSON text, as parsed: a member given twice takes its last value. */
export function parseQuery(raw: string, carries?: (bucket: string) => boolean, serves?: (bucket: string, owner: string, id: string) => boolean): Question {
  let q: unknown
  try {
    q = JSON.parse(raw)
  } catch {
    throw refuse('query', '', 'not JSON')
  }
  if (q === null || typeof q !== 'object' || Array.isArray(q)) throw refuse('query', '', 'not an object')
  const names = Object.keys(q).sort()
  const cls = Classes.find((k) => [...k.members].sort().join(',') === names.join(','))
  if (cls === undefined) throw refuse('query', '', 'the members of no class')
  const values: Record<string, string> = {}
  for (const n of names) {
    const v = (q as Record<string, unknown>)[n]
    if (typeof v !== 'string') throw refuse('query', '', `${n} is not a string`)
    // A lone surrogate reads as U+FFFD, as a parser that decodes to UTF-8 holds it.
    const u = new TextDecoder().decode(utf8(v))
    if (!checkValue(cls.name, n, u)) throw refuse('query', '', `${n} breaks its rule`)
    values[n] = u
  }
  if (carries !== undefined && !carries(values.bucket!)) throw refuse('query', '', 'a bucket this host does not carry')
  if (cls.served && serves !== undefined && !serves(values.bucket!, values.owner!, cls.name === 'export' ? values.export! : values.id!)) {
    throw refuse('query', '', 'a chain this host does not serve')
  }
  return { class: cls.name, values }
}
