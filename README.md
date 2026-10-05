# bstore

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Object storage as a market on Bitcoin. A publisher puts an object on the
object plane once; every storage provider subscribed to the bucket receives
the same bytes and competes to hold them. Readers speak S3.

## How it works

- **Publish once, many providers.** An object is a CHIRP (BRC-167) closure
  delivered to every provider on its bucket's blob topic, with a signed
  entry on the bucket's records topic. Providers advertise what they hold
  with ordinary UHRP (BRC-26) advertisements.
- **A bounty races the write.** A publisher may commit a hold bounty that it
  pays, by rank, to the first providers to advertise and pass a challenge,
  with BRC-178's square-root split.
- **Anyone can buy time or proof.** A renewal extends a provider's holding;
  a challenge samples 4 KiB segments from a seed no single party controls
  and proves each into the object's proof root, about 5 KB per sample.
- **The bucket proves its own listing.** The owner's chain of mined tokens
  commits to the whole key map, so a reader proves what a key names, that a
  key is absent and that a listing page is complete.
- **Incumbent UHRP hosts join unchanged** through a bridge that speaks
  their HTTP routes.
- **Honest about limits.** Availability is a market outcome, keys are not
  copies, and enforcement is reputation in this version.

## Documentation

| If you want to | Read |
| --- | --- |
| Implement a compatible publisher, provider, host or reader | [docs/spec.md](docs/spec.md) (the specification) |
| Know what cannot change once published | [docs/frozen.md](docs/frozen.md) |

## Status

Contract draft. Nothing is frozen until every row of
[docs/frozen.md](docs/frozen.md) is signed.

## License

Apache License 2.0. See [LICENSE](LICENSE).
