/**
 * What ls_bstore keeps beside the engine's storage (spec sections 12.2 to
 * 12.5), as an append-only journal that retention compacts. The engine's
 * storage holds the outputs a host still keeps; it cannot hold what must
 * outlive them, or what it has no column for: which carrier a funding
 * outpoint admitted first, the input outpoints of every admitted sweep, the
 * retraction marks, when the host first saw each object, and which held
 * outputs a later transaction spent (storage hands a restore its unspent
 * outputs only).
 *
 * One JSON object a line:
 *
 *   {"k":"c","op":"<txid>.<vout>","c":"<C>","at":<ms>}               the first carrier the host admitted on a funding outpoint
 *   {"k":"cf","op":"..."}                                           that row was dropped by retention
 *   {"k":"a","b":"<bucket>","c":"<C>","at":<ms>}                    a carrier, first seen
 *   {"k":"af","c":"<C>"}                                            the carrier was dropped
 *   {"k":"x","op":"...","key":"<owner>","at":<ms>,"life":<bool>,"b":"..."}
 *                                                                   a retraction mark: the swept outpoint of a carrier, its owner, the sweep's first sight, the carrier's bucket
 *   {"k":"xl","op":"..."}                                           the mark is kept for the host's life
 *   {"k":"xf","op":"..."}                                           the mark was dropped by retention
 *   {"k":"p","op":"...","s":"<sweep txid>","at":<ms>}               an input outpoint of an admitted sweep, first seen
 *   {"k":"pf","op":"..."}                                           that outpoint was dropped by retention
 *   {"k":"t","b":"...","txid":"<txid>","vout":<n>,"at":<ms>}        a bucket token, first seen
 *   {"k":"tf","txid":"<txid>"}                                      the token was dropped
 *   {"k":"o","b":"...","txid":"...","vout":<n>,"at":<ms>}           an advertisement, hold output or sweep output, first seen
 *   {"k":"w","b":"...","txid":"...","vout":<n>}                     a held output a later admitted transaction spent
 *   {"k":"of","b":"...","txid":"...","vout":<n>}                    the output was dropped
 *
 * Commitments are in hash byte order and txids in display order, both
 * lowercase hex; keys are 66 hex digits; `at` is the host's time in Unix
 * milliseconds. Replayed in order the lines rebuild the state; the first
 * line about a thing sets it and a later one only forgets or upgrades it.
 * Retention rewrites the file as the state it holds.
 */
import { closeSync, fdatasyncSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, truncateSync, writeSync } from 'node:fs'
import { join } from 'node:path'

export type Line =
  | { k: 'c'; op: string; c: string; at: number }
  | { k: 'cf'; op: string }
  | { k: 'a'; b: string; c: string; at: number }
  | { k: 'af'; c: string }
  | { k: 'x'; op: string; key: string; at: number; life: boolean; b?: string }
  | { k: 'xl'; op: string }
  | { k: 'xf'; op: string }
  | { k: 'p'; op: string; s: string; at: number }
  | { k: 'pf'; op: string }
  | { k: 't'; b: string; txid: string; vout: number; at: number }
  | { k: 'tf'; txid: string }
  | { k: 'o'; b: string; txid: string; vout: number; at: number }
  | { k: 'w'; b: string; txid: string; vout: number }
  | { k: 'of'; b: string; txid: string; vout: number }

/** Where the state lives. `append` and `rewrite` are durable when they return. */
export interface Journal {
  load(): Line[]
  append(l: Line): void
  /** Replaces everything with lines, atomically. */
  rewrite(lines: Line[]): void
}

/** A journal in memory, for tests and for a host that accepts losing it. */
export class MemoryJournal implements Journal {
  lines: Line[] = []
  load(): Line[] {
    return this.lines.map((l) => ({ ...l }))
  }
  append(l: Line): void {
    this.lines.push({ ...l })
  }
  rewrite(lines: Line[]): void {
    this.lines = lines.map((l) => ({ ...l }))
  }
}

const hex = /^[0-9a-f]*$/
const outpointRe = /^[0-9a-f]{64}\.(0|[1-9][0-9]{0,9})$/
const bucketRe = /^[a-z_]{12,36}$/

/** Whether v is a journal line, each field of its type and shape. */
export function isLine(v: unknown): v is Line {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) return false
  const o = v as Record<string, unknown>
  const s = (k: string): boolean => typeof o[k] === 'string'
  const h = (k: string, n: number): boolean => s(k) && hex.test(o[k] as string) && (o[k] as string).length === n
  const n = (k: string): boolean => typeof o[k] === 'number' && Number.isSafeInteger(o[k]) && (o[k] as number) >= 0
  const op = (): boolean => s('op') && outpointRe.test(o['op'] as string)
  const b = (): boolean => s('b') && bucketRe.test(o['b'] as string)
  switch (o['k']) {
    case 'c':
      return op() && h('c', 64) && n('at')
    case 'cf':
    case 'xl':
    case 'xf':
    case 'pf':
      return op()
    case 'a':
      return b() && h('c', 64) && n('at')
    case 'af':
      return h('c', 64)
    case 'x':
      return op() && h('key', 66) && n('at') && typeof o['life'] === 'boolean' && (o['b'] === undefined || b())
    case 'p':
      return op() && h('s', 64) && n('at')
    case 't':
    case 'o':
      return b() && h('txid', 64) && n('vout') && n('at')
    case 'tf':
      return h('txid', 64)
    case 'w':
    case 'of':
      return b() && h('txid', 64) && n('vout')
  }
  return false
}

/** The journal's file name in the state directory. */
export const JournalFile = 'bstore-rows.jsonl'

/**
 * The journal as a file, `bstore-rows.jsonl` in dir. Each line is written
 * and flushed to disk before append returns. A last line cut short by a
 * crash is cut off on load; any other line that does not parse stops the
 * load, because a mark or a sweep's outpoint silently skipped could let a
 * retracted carrier be answered again. A rewrite goes to a new file that is
 * flushed and then renamed over the old one, so a crash leaves one or the
 * other whole.
 */
export class FileJournal implements Journal {
  readonly path: string
  private fd: number | undefined

  constructor(readonly dir: string) {
    mkdirSync(dir, { recursive: true })
    this.path = join(dir, JournalFile)
  }

  load(): Line[] {
    let text: string
    try {
      text = readFileSync(this.path, 'utf8')
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return []
      throw e
    }
    if (text.length > 0 && !text.endsWith('\n')) {
      const keep = text.lastIndexOf('\n') + 1
      truncateSync(this.path, Buffer.byteLength(text.slice(0, keep), 'utf8'))
      text = text.slice(0, keep)
    }
    const out: Line[] = []
    for (const [i, l] of text.split('\n').entries()) {
      if (l === '') continue
      let v: unknown
      try {
        v = JSON.parse(l)
      } catch {
        throw new Error(`${this.path}: line ${i + 1} is not JSON`)
      }
      if (!isLine(v)) throw new Error(`${this.path}: line ${i + 1} is not a bstore row`)
      out.push(v)
    }
    return out
  }

  append(l: Line): void {
    this.fd ??= openSync(this.path, 'a')
    writeSync(this.fd, JSON.stringify(l) + '\n')
    fdatasyncSync(this.fd)
  }

  rewrite(lines: Line[]): void {
    const tmp = `${this.path}.new`
    const fd = openSync(tmp, 'w')
    try {
      // In chunks, so a large state is not one string.
      let chunk = ''
      for (const l of lines) {
        chunk += JSON.stringify(l) + '\n'
        if (chunk.length > 1 << 20) {
          writeSync(fd, chunk)
          chunk = ''
        }
      }
      if (chunk !== '') writeSync(fd, chunk)
      fdatasyncSync(fd)
    } finally {
      closeSync(fd)
    }
    this.close()
    renameSync(tmp, this.path)
    syncDir(this.dir)
  }

  close(): void {
    if (this.fd !== undefined) closeSync(this.fd)
    this.fd = undefined
  }
}

/** Flushes a directory's entries, so a rename survives a crash. */
export function syncDir(dir: string): void {
  let fd: number | undefined
  try {
    fd = openSync(dir, 'r')
    fsyncSync(fd)
  } catch {
    // Some file systems cannot sync a directory; the rename stands.
  } finally {
    if (fd !== undefined) closeSync(fd)
  }
}
