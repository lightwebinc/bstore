/**
 * The part of esbuild's metafile the check reads. esbuild's own Metafile
 * satisfies it, and a test's synthetic one need carry nothing more.
 */
export interface CheckedMetafile {
  inputs: Record<string, unknown>
  outputs: Record<string, { imports: Array<{ path: string }> }>
}

export declare function refusals(metafile: CheckedMetafile, outfile: string, library: string, sdk: string): string[]
