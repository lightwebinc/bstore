# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Three images from one build, each static binaries on a distroless nonroot
# base:
#
#   docker build -t ghcr.io/lightwebinc/bstore .                             # the command and the S3 gateway (default)
#   docker build --target provider -t ghcr.io/lightwebinc/bstore-provider .  # a storage provider
#   docker build --target devchain -t ghcr.io/lightwebinc/bstore-devchain .  # the local chain
#
# The builder is pinned by digest to the builder toolchain, Go 1.27.3, so
# scanning sees the standard library that ships.
#
# No ENV defaults are baked in. The hosts, the header source, the node, the
# settlement leg and the providers are addresses of somebody's deployment
# and have no default anywhere: a default would send a writer's records, or
# a provider's advertisements, to a server nobody configured. Pass them in
# the config files (docs/usage.md, docs/provider.md, docs/s3.md).
#
# Each image carries its state directories, empty, owned by nonroot, so a
# new named volume mounted there starts with the same owner and mode:
#
#   bstore           /home/nonroot/.bstore (a home, $BSTORE_HOME), /var/lib/bstore-s3d
#   bstore-provider  /var/lib/bstore-provider
#   bstore-devchain  /var/lib/devchain, /certs
#
# A volume mounted on a path the image lacks is created root-owned, and
# every write to it is then refused. A bind mount keeps the host
# directory's owner instead: make it writable by uid 65532, or run with
# --user.

# The builder runs on the build machine's own platform and cross-compiles
# to the target's, so an arm64 image needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
ENV GOWORK=off

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    mkdir -p /out /state/.bstore /state/bstore-s3d /state/bstore-provider /state/devchain /state/certs; \
    for cmd in bstore bstore-provider devchain; do \
      CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -buildvcs=false \
          -ldflags "-s -w -X main.version=${VERSION}" \
          -o /out/$cmd ./cmd/$cmd; \
    done

# The local chain for trying bstore without a node or coin: a node's RPC
# and asset API and a header source on port 8080. It has no proof of work
# and no peers, and nothing it mines is money. Its journal, which replays
# the chain after a restart, is kept in /var/lib/devchain; mount a volume
# there. `devchain certs` writes a throwaway CA and server certificates
# into /certs: a provider's advertised URL must be https.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7 AS devchain
USER nonroot:nonroot
COPY --from=builder /out/devchain /usr/local/bin/devchain
COPY --from=builder --chown=65532:65532 --chmod=0700 /state/devchain /var/lib/devchain
COPY --from=builder --chown=65532:65532 --chmod=0755 /state/certs /certs
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/bstore/
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/devchain", "-journal", "/var/lib/devchain/journal"]

# A storage provider (docs/provider.md): `run -config FILE` serves the
# front, the object routes and /submit on the configuration's listen
# address.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7 AS provider
USER nonroot:nonroot
COPY --from=builder /out/bstore-provider /usr/local/bin/bstore-provider
COPY --from=builder --chown=65532:65532 --chmod=0700 /state/bstore-provider /var/lib/bstore-provider
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/bstore/
EXPOSE 8095
ENTRYPOINT ["/usr/local/bin/bstore-provider"]

# The command line and the S3 gateway (`bstore s3d run -config FILE`).
# Last, so a plain build produces it.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7 AS bstore
USER nonroot:nonroot
COPY --from=builder /out/bstore /usr/local/bin/bstore
COPY --from=builder --chown=65532:65532 --chmod=0700 /state/.bstore /home/nonroot/.bstore
COPY --from=builder --chown=65532:65532 --chmod=0700 /state/bstore-s3d /var/lib/bstore-s3d

# The licences travel with the image. The binaries statically contain
# go-sdk and the other modules LICENSE-THIRD-PARTY lists, whose licences
# require their text in any copy, and static linking leaves those files
# behind.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/bstore/
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/bstore"]
