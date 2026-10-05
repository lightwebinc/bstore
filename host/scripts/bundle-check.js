/**
 * What bundle.js checks in esbuild's metafile before it keeps the bundle,
 * apart from the build so that a test can feed it metafiles no clean build
 * produces.
 */

/**
 * Whether path is one of root's own files. A node_modules/ below root holds
 * another package: npm nests a dependency there when its version conflicts
 * with the hoisted copy, so the prefix alone would let that package's code
 * through under the library's name.
 */
function ownFile(path, root) {
  return path.startsWith(root) && !path.slice(root.length).includes('node_modules/')
}

/**
 * The reasons to refuse the bundle, empty when it may be kept: an input that
 * is not a file of src/ or of the library's own package, no input of the
 * library's at all, or an import in outfile other than sdk or a node:
 * built-in. Each reason is one line naming what failed.
 */
export function refusals(metafile, outfile, library, sdk) {
  const roots = ['src/', `node_modules/${library}/`]
  const inputs = Object.keys(metafile.inputs)
  const problems = []
  for (const path of inputs) {
    if (roots.some((root) => ownFile(path, root))) continue
    const nestedIn = roots.find((root) => path.startsWith(root))
    problems.push(
      nestedIn === undefined
        ? `input outside ${roots.join(' and ')}: ${path}`
        : `input from a package nested under ${nestedIn}: ${path}`,
    )
  }
  // An empty answer is not a clean one: a library marked external, or
  // resolved from somewhere else, would pass the check above and leave the
  // host to find a package it does not have.
  if (!inputs.some((path) => ownFile(path, roots[1]))) problems.push(`nothing from ${library} was inlined`)
  const output = metafile.outputs[outfile]
  if (output === undefined) {
    problems.push(`esbuild reported no ${outfile}`)
  } else {
    for (const path of new Set(output.imports.map((imp) => imp.path))) {
      if (path !== sdk && !path.startsWith('node:')) problems.push(`import other than ${sdk} or a node: built-in: ${path}`)
    }
  }
  return problems
}
