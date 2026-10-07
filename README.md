# bstore

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Object storage as a market on Bitcoin. A writer publishes an object once;
storage providers compete to hold it for a bounty; a reader proves every
byte it gets. Readers and writers may speak S3.

## How it works

- **Publish once, many providers.** An object is a CHIRP (BRC-167) closure
  sent to every provider of its bucket, with a signed entry on the
  bucket's records topic. Providers advertise what they hold with ordinary
  UHRP (BRC-26) advertisements.
- **A bounty races the write.** A writer may commit a hold bounty that its
  own client pays, by rank, to the first providers to advertise and pass a
  challenge, with BRC-178's square-root split.
- **Anyone can buy time or proof.** A renewal extends a provider's holding;
  a challenge samples 4 KiB segments and proves each into the object's
  proof root.
- **The bucket proves its own listing.** The owner's chain of mined tokens
  commits to the whole key map, so a reader proves what a key names, that a
  key is absent and that a listing page is complete.
- **Incumbent UHRP hosts join unchanged** through a bridge
  (`bstore-bridge`) that speaks their HTTP routes.
- **Honest about limits.** Availability is a market outcome, keys are not
  copies, and enforcement is reputation in this version.

## Documentation

| If you want to | Read |
| --- | --- |
| Try it on one machine, in Docker, with no coin | [QUICKSTART.md](QUICKSTART.md) |
| Learn the ideas and the day-to-day use, by role | [docs/user-guide.md](docs/user-guide.md) |
| Know the bounds, what each operation costs, and the known limits | [docs/limits.md](docs/limits.md) |
| Look up a command, a flag or a setting | [docs/usage.md](docs/usage.md) |
| Run a storage provider | [docs/provider.md](docs/provider.md) |
| Run the S3 gateway, and what S3 clients can do through it | [docs/s3.md](docs/s3.md) |
| Run an overlay host that carries buckets | [docs/host.md](docs/host.md) |
| Implement a compatible writer, provider, host or reader | [docs/spec.md](docs/spec.md) (the specification) |
| Know what cannot change once published | [docs/frozen.md](docs/frozen.md) |
| Use or check the codec and its golden vectors | [docs/codec.md](docs/codec.md) |

## Building

```sh
cd host && npm ci && cd ..
make verify NODE=/path/to/node24
make bstore bstore-provider       # bin/: the command line (and bstore s3d), the provider
make module NODE=/path/to/node24  # host/bundle/bstore-module.js
make docker-build                 # the images
```

Go 1.27 and Node 24. bstore pins go-sdk v1.5.2 and bcommon v0.11.0.

## License

Apache License 2.0. See [LICENSE](LICENSE), and [NOTICE](NOTICE) for the
third-party software and standards.
