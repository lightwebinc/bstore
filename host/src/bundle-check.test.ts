/**
 * The bundle step's refusals, fed metafiles no clean build produces. The
 * smoke test cannot hold them: a bundle with a second SDK or another
 * package inlined still mounts and admits the golden transactions, so only
 * these say that what the file contains is enforced.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { type CheckedMetafile, refusals } from '../scripts/bundle-check.js'

const library = '@lightwebinc/bcommon'
const sdk = '@bsv/sdk'
const outfile = 'bundle/bstore-module.js'
const lib = `node_modules/${library}/`

function metafile(inputs: string[], imports: string[] = [sdk]): CheckedMetafile {
  return {
    inputs: Object.fromEntries(inputs.map((path) => [path, { bytes: 1, imports: [] }])),
    outputs: { [outfile]: { imports: imports.map((path) => ({ path, kind: 'import-statement', external: true })) } },
  }
}

const clean = ['src/module.ts', 'src/tm_bstore.ts', `${lib}dist/index.js`, `${lib}dist/cbor.js`]
const check = (m: CheckedMetafile): string[] => refusals(m, outfile, library, sdk)

test('a bundle of src/ and the library, importing the SDK and built-ins, is kept', () => {
  assert.deepEqual(check(metafile(clean)), [])
  assert.deepEqual(check(metafile(clean, [sdk, sdk, 'node:zlib', 'node:crypto'])), [])
})

test('an input outside src/ and the library is refused by name', () => {
  for (const stray of ['stray.js', 'node_modules/other/index.js', 'node_modules/@bsv/sdk/dist/esm/mod.js']) {
    assert.deepEqual(check(metafile([...clean, stray])), [`input outside src/ and ${lib}: ${stray}`], stray)
  }
})

test('a package nested inside the library or src/ is refused, though its path starts with theirs', () => {
  const nested = `${lib}node_modules/x/index.js`
  assert.deepEqual(check(metafile([...clean, nested])), [`input from a package nested under ${lib}: ${nested}`])
  assert.deepEqual(check(metafile(['src/module.ts', nested])), [
    `input from a package nested under ${lib}: ${nested}`,
    `nothing from ${library} was inlined`,
  ])
})

test('a bundle with nothing from the library is refused', () => {
  assert.deepEqual(check(metafile(['src/module.ts'])), [`nothing from ${library} was inlined`])
})

test('an import other than the SDK or a built-in is refused, once per specifier', () => {
  assert.deepEqual(check(metafile(clean, [sdk, 'knex', 'knex', 'fs'])), [
    `import other than ${sdk} or a node: built-in: knex`,
    `import other than ${sdk} or a node: built-in: fs`,
  ])
})

test('a metafile without the bundle is refused', () => {
  const m = metafile(clean)
  m.outputs = {}
  assert.deepEqual(check(m), [`esbuild reported no ${outfile}`])
})
