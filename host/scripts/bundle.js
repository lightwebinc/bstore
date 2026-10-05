/**
 * Builds what a reference overlay host loads: ONE ESM file,
 * bundle/bstore-module.js, from src/module.ts, with @lightwebinc/bcommon
 * inlined. It imports only @bsv/sdk, which the host's own node_modules
 * provides, and Node's built-in modules, so the file needs nothing of this
 * package beside it and the SDK that runs is the host's own copy.
 *
 * The result is refused, and deleted, unless every input esbuild read is a
 * file of src/ or of the library's own package (not of a package nested
 * inside either), at least one of them is the library's, and the file
 * imports nothing but @bsv/sdk and node: built-ins (bundle-check.js).
 * Without this a second SDK, or another package's runtime code, would reach
 * the shipped file through a dependency change no line of this repository
 * shows.
 */
import { readFileSync, rmSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'
import { refusals } from './bundle-check.js'

const library = '@lightwebinc/bcommon'
const sdk = '@bsv/sdk'
const outfile = 'bundle/bstore-module.js'
const root = fileURLToPath(new URL('..', import.meta.url))
const readJSON = (path) => JSON.parse(readFileSync(new URL(`../${path}`, import.meta.url), 'utf8'))

// The versions actually installed, which are what get inlined and what the
// host is expected to provide, rather than the ranges package.json asks for.
const version = readJSON(`node_modules/${library}/package.json`).version
const sdkVersion = readJSON(`node_modules/${sdk}/package.json`).version
const self = readJSON('package.json')
// The bundle targets what tsc compiles to, so the tests on dist/ and the
// shipped file are built for the same language level.
const target = readJSON('tsconfig.json').compilerOptions.target.toLowerCase()
const banner = `${self.name} ${self.version}: tm_bstore_<bucket>, tm_bstoreblob_<bucket> and ls_bstore, with ${library} ${version} inlined; imports ${sdk} (built against ${sdkVersion})`

rmSync(new URL('../bundle/', import.meta.url), { recursive: true, force: true })

const result = await build({
  absWorkingDir: root,
  entryPoints: ['src/module.ts'],
  outfile,
  bundle: true,
  format: 'esm',
  platform: 'node',
  target,
  external: [sdk],
  sourcemap: false,
  define: { BSTORE_BUILD: JSON.stringify(banner) },
  banner: { js: `// ${banner}` },
  metafile: true,
  logLevel: 'warning',
})

const problems = refusals(result.metafile, outfile, library, sdk)
if (problems.length > 0) {
  rmSync(new URL(`../${outfile}`, import.meta.url), { force: true })
  for (const p of problems) console.error(`bundle: ${p}`)
  console.error(`bundle: refused; ${outfile} removed`)
  process.exit(1)
}

const out = result.metafile.outputs[outfile]
const imports = [...new Set(out.imports.map((i) => i.path))].sort()
console.log(`${outfile}: ${out.bytes} bytes, ${library} ${version} inlined, target ${target}; imports ${imports.join(', ')}`)
