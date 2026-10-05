/** A question's values reach the codec as UTF-8 holds them, as the Go codec reads them. */
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { parseQuery } from './query.js'

test('a lone surrogate in a key value, a valid question', () => {
  const owner = '02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5'
  const q = parseQuery(`{"bucket":"photos_qzvmkwerty","owner":"${owner}","id":"${'0'.repeat(64)}","key":"a\\ud800b"}`)
  assert.equal(q.values.key, 'a�b')
})
