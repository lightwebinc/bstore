/**
 * Sorted lists the index pages from: each answer starts with a binary
 * search and walks at most a page, so no question costs more than its page
 * whatever the index holds.
 */

/** Strings in ascending order (UTF-16 code units, which is byte order for the ASCII keys used here), each once. */
export class Sorted {
  readonly a: string[] = []

  get size(): number {
    return this.a.length
  }

  /** The index of the first element at or above k. */
  lower(k: string): number {
    let lo = 0
    let hi = this.a.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.a[mid]! < k) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  /** The index of the first element above k. */
  upper(k: string): number {
    let lo = 0
    let hi = this.a.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.a[mid]! <= k) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  has(k: string): boolean {
    return this.a[this.lower(k)] === k
  }

  insert(k: string): void {
    const i = this.lower(k)
    if (this.a[i] !== k) this.a.splice(i, 0, k)
  }

  remove(k: string): void {
    const i = this.lower(k)
    if (this.a[i] === k) this.a.splice(i, 1)
  }
}

/** Numbers in ascending order, each once. */
export class SortedNumbers {
  readonly a: number[] = []

  get size(): number {
    return this.a.length
  }

  /** The index of the first element above n. */
  upper(n: number): number {
    let lo = 0
    let hi = this.a.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.a[mid]! <= n) lo = mid + 1
      else hi = mid
    }
    return lo
  }

  insert(n: number): void {
    const i = this.upper(n)
    if (this.a[i - 1] !== n) this.a.splice(i, 0, n)
  }

  remove(n: number): void {
    const i = this.upper(n) - 1
    if (i >= 0 && this.a[i] === n) this.a.splice(i, 1)
  }
}

/** Adds v to the set under k, making the set when it is new. */
export function addTo<K, V>(m: Map<K, Set<V>>, k: K, v: V): void {
  let s = m.get(k)
  if (s === undefined) m.set(k, (s = new Set()))
  s.add(v)
}

/** Takes v from the set under k, and the set when it is empty. */
export function removeFrom<K, V>(m: Map<K, Set<V>>, k: K, v: V): void {
  const s = m.get(k)
  s?.delete(v)
  if (s?.size === 0) m.delete(k)
}

/** The sorted list under k, made when it is new. */
export function sortedIn<K>(m: Map<K, Sorted>, k: K): Sorted {
  let s = m.get(k)
  if (s === undefined) m.set(k, (s = new Sorted()))
  return s
}

/** Takes x from the sorted list under k, and the list when it is empty. */
export function sortedOut<K>(m: Map<K, Sorted>, k: K, x: string): void {
  const s = m.get(k)
  s?.remove(x)
  if (s?.size === 0) m.delete(k)
}
