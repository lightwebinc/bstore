# GOWORK=off everywhere on purpose: a workspace would resolve bcommon to a
# local checkout, so a go.mod skew would stay invisible here. What is tested
# is what go.mod pins.
#
# The TypeScript package under host/ needs Node 24 and its dependencies
# installed once (cd host && npm ci). The targets run tsc and node directly,
# so NODE may name a Node 24 binary that is not on PATH:
#
#	make verify NODE=/path/to/node24

.PHONY: verify test go-test go-oracle ts-test ts-check ts-build module module-test e2e provider-e2e s3-e2e publisher-e2e node-check vet fmt-check deps-check vectors vectors-check fuzz bstore bstore-provider devchain licences licences-update docker-build docker-build-host docker-smoke docker-smoke-host quickstart-check

NODE ?= node
TSC := $(NODE) node_modules/typescript/bin/tsc

IMAGE          ?= ghcr.io/lightwebinc/bstore
PROVIDER_IMAGE ?= ghcr.io/lightwebinc/bstore-provider
DEVCHAIN_IMAGE ?= ghcr.io/lightwebinc/bstore-devchain
HOST_IMAGE     ?= ghcr.io/lightwebinc/bstore-host

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

verify: fmt-check vet deps-check licences ts-check vectors-check test

# Go and TypeScript. Both read the Go-generated vectors under
# testdata/vectors: the TypeScript tests decode, re-encode, refuse, fold,
# check, verify and admit every case and reach the bytes and verdicts the
# Go codec wrote.
test: go-test go-oracle ts-test

go-test:
	GOWORK=off go test -race -short -count=1 ./...

# The page checks of spec section 11.3 against the contract review's brute
# force, exhaustively (about 38 million pages); without the race detector,
# which would make it take minutes.
go-oracle:
	GOWORK=off go test -count=1 -run 'TestPageChecksAgainstOracle$$' ./storerec/

vet:
	GOWORK=off go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "gofmt:"; gofmt -l .; exit 1; }

# The commands, into bin/, stamped with the version.
bstore:
	GOWORK=off go build -ldflags "-X main.version=$(VERSION)" -o bin/bstore ./cmd/bstore

bstore-provider:
	GOWORK=off go build -ldflags "-X main.version=$(VERSION)" -o bin/bstore-provider ./cmd/bstore-provider

# The local stand-in chain the quickstart runs on.
devchain:
	GOWORK=off go build -ldflags "-X main.version=$(VERSION)" -o bin/devchain ./cmd/devchain

# LICENSE-THIRD-PARTY is generated from what the binaries link: every
# module's licence text travels with a binary or an image built here.
# licences fails when the file is stale.
licences:
	GOWORK=off python3 scripts/gen-third-party-licenses.py . --check

licences-update:
	GOWORK=off python3 scripts/gen-third-party-licenses.py .

# The direct dependencies, asserted rather than intended:
#  - go.mod and go.sum are tidy, and no module is replaced;
#  - the direct set is exactly go-sdk, bcommon, bcommon's devkit
#    (imported only by tests and the devchain and vectors commands) and
#    the AWS SDK for Go v2 modules the S3 client (bstore cp) speaks
#    through, with smithy-go for its error types, and minio's crc64nvme
#    (Apache-2.0): the gateway checks the AWS CLI's default checksum,
#    CRC-64/NVME, over nearly every body, and hash/crc64's table runs at a
#    seventh of its speed (internal/s3d/checksum.go);
#  - go-sdk resolves to exactly v1.7.1, the version bcommon pins and the
#    vectors were made with. Minimal version selection takes the higher of
#    two pins, so a library asking for a later go-sdk would change what
#    bstore ships without a line changing here.
DEPS_DIRECT := github.com/aws/aws-sdk-go-v2 github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/feature/s3/manager \
               github.com/aws/aws-sdk-go-v2/service/s3 github.com/aws/smithy-go \
               github.com/bsv-blockchain/go-sdk github.com/lightwebinc/bcommon github.com/lightwebinc/bcommon/devkit \
               github.com/minio/crc64nvme
DEPS_SDK    := github.com/bsv-blockchain/go-sdk v1.7.1

deps-check:
	@set -eu; \
	if ! GOWORK=off go mod tidy -diff >/dev/null; then \
		echo "deps-check: go.mod or go.sum is not tidy; run go mod tidy"; exit 1; \
	fi; \
	replaced=$$(GOWORK=off go list -m -f '{{if .Replace}}{{.Path}}{{end}}' all); \
	if [ -n "$$replaced" ]; then echo "deps-check: no module may be replaced, found: $$replaced"; exit 1; fi; \
	direct=$$(GOWORK=off go list -m -f '{{if not (or .Main .Indirect)}}{{.Path}}{{end}}' all | sed '/^$$/d' | sort); \
	want=$$(printf '%s\n' $(DEPS_DIRECT) | sort); \
	if [ "$$direct" != "$$want" ]; then \
		echo "deps-check: the direct dependencies must be exactly:"; echo "$$want" | sed 's/^/  /'; \
		echo "found:"; echo "$$direct" | sed 's/^/  /'; exit 1; \
	fi; \
	sdk=$$(GOWORK=off go list -m $(firstword $(DEPS_SDK))); \
	if [ "$$sdk" != "$(DEPS_SDK)" ]; then echo "deps-check: go-sdk must resolve to exactly $(lastword $(DEPS_SDK)), found: $$sdk"; exit 1; fi

node-check:
	@$(NODE) -e 'process.exit(process.versions.node.split(".")[0] === "24" ? 0 : 1)' || \
		{ echo "host/ needs Node 24; set NODE=/path/to/node"; exit 1; }

ts-check: node-check
	cd host && $(TSC) --noEmit

# tsc into host/dist, then the deployable bundle into host/bundle.
ts-build: node-check
	cd host && $(NODE) -e "require('node:fs').rmSync('dist', { recursive: true, force: true })" && $(TSC) && $(NODE) scripts/bundle.js

# The host module a reference overlay host loads: one file,
# host/bundle/bstore-module.js, with bcommon inlined and nothing imported
# but @bsv/sdk (the host's own copy) and Node built-ins. It is copied into
# the host's tree and named by absolute path in OVERLAY_MODULES
# (docs/host.md). Its tests (make test) mount that file and run golden
# vectors through it.
module: ts-build

# The module's own tests: the topic managers on every golden transaction,
# every lookup class, retention, restore, the terms route and its
# hardening, on the compiled tree and on the bundle, with the real overlay
# engine over an in-memory store.
module-test: ts-build
	cd host && $(NODE) --test "dist/**/*.test.js"

ts-test: module-test

# The module end to end on a local reference overlay host loading the
# bundle, staged beside the host's node_modules as a deployment places it,
# over MySQL: both topics, every lookup class (the priced ones paid through
# their 402 on the terms route), a restart, a restore and a kill, retention
# pruning with the host's clock moved, the replay of a dropped and of a
# retracted object, and the route's hardening. Not part of verify: it needs
# Docker (for MySQL) and a reference host directory holding its compiled
# dist/index.js and its node_modules.
#
#	make e2e REFERENCE_HOST=/path/to/reference-host
e2e: ts-build
	@test -n "$(REFERENCE_HOST)" || { echo "set REFERENCE_HOST=/path/to/reference-host"; exit 1; }
	cd host && $(NODE) dist/e2e.js $(abspath $(REFERENCE_HOST))

# Write the vectors under testdata/vectors from the Go codec. A change to
# them is a change to the contract's goldens.
vectors:
	GOWORK=off go run ./cmd/vectors

# Regenerate them in memory and compare byte for byte; write nothing.
vectors-check:
	GOWORK=off go run ./cmd/vectors -check

# Each decoder and the advertisement check under the Go fuzzer, briefly.
FUZZTIME ?= 30s
fuzz:
	@for f in FuzzBucket FuzzEntry FuzzOutcome FuzzFront FuzzRequest FuzzContexts FuzzAdvertisement; do \
		GOWORK=off go test ./storerec/ -run '^$$' -fuzz "^$$f\$$" -fuzztime $(FUZZTIME) || exit 1; \
	done

# The provider end to end (docs/provider.md): two bstore-provider processes
# and a test-only writer on a local reference overlay host loading the
# bundle, over MySQL, on a local test chain. Not part of verify: it needs
# Docker and a reference host directory, and NODE must name a Node 24
# binary by absolute path.
#
#	make provider-e2e REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
provider-e2e: module
	@test -n "$(REFERENCE_HOST)" || { echo "set REFERENCE_HOST=/path/to/reference-host"; exit 1; }
	BSTORE_E2E_REFHOST=$(abspath $(REFERENCE_HOST)) BSTORE_E2E_NODE=$(abspath $(shell command -v $(NODE))) \
		GOWORK=off go test -count=1 -timeout 30m -run '^TestProviderE2E$$' -v ./cmd/bstore-provider/

# The S3 gateway end to end (docs/s3.md): bstore s3d, two bstore-provider
# processes and a test-only bucket owner on a local reference overlay host
# loading the bundle, over MySQL, on a local test chain, driven by the AWS
# CLI and by Terraform's S3 backend when terraform or tofu is installed.
# Not part of verify: it needs Docker, the aws command and a reference host
# directory, and NODE must name a Node 24 binary by absolute path.
#
#	make s3-e2e REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
s3-e2e: module
	@test -n "$(REFERENCE_HOST)" || { echo "set REFERENCE_HOST=/path/to/reference-host"; exit 1; }
	BSTORE_E2E_REFHOST=$(abspath $(REFERENCE_HOST)) BSTORE_E2E_NODE=$(abspath $(shell command -v $(NODE))) \
		GOWORK=off go test -count=1 -timeout 45m -run '^TestS3E2E$$' -v ./cmd/bstore/

# The publisher client end to end (docs/usage.md): the bstore command line
# alone, as a bucket owner and a writer, with two bstore-provider processes
# on a local reference overlay host loading the bundle, over MySQL, on a
# local test chain: a 100 MiB put by upload session, the race and its
# settlement challenges, settlement by rank killed and resumed, anchoring,
# a verified read, a dropped blob caught by a paid challenge, rm and
# NoSuchKey, a reclaim after the deadline and the kill switch. Not part of
# verify: it needs Docker and a reference host directory, and NODE must
# name a Node 24 binary by absolute path.
#
#	make publisher-e2e REFERENCE_HOST=/path/to/reference-host NODE=/path/to/node24
publisher-e2e: module
	@test -n "$(REFERENCE_HOST)" || { echo "set REFERENCE_HOST=/path/to/reference-host"; exit 1; }
	BSTORE_E2E_REFHOST=$(abspath $(REFERENCE_HOST)) BSTORE_E2E_NODE=$(abspath $(shell command -v $(NODE))) \
		GOWORK=off go test -count=1 -timeout 45m -run '^TestPublisherE2E$$' -v ./cmd/bstore/

# The images, built and tagged locally, each as $(VERSION) and latest.
# Nothing here pushes. docker-build makes the command and gateway image, the
# provider image and the local chain; the host image adds the bstore module
# to a reference overlay host image:
#
#	make docker-build-host REFERENCE_HOST_IMAGE=<reference overlay host image>
docker-build:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .
	docker build --build-arg VERSION=$(VERSION) --target provider -t $(PROVIDER_IMAGE):$(VERSION) -t $(PROVIDER_IMAGE):latest .
	docker build --build-arg VERSION=$(VERSION) --target devchain -t $(DEVCHAIN_IMAGE):$(VERSION) -t $(DEVCHAIN_IMAGE):latest .
	@docker image ls --format '{{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E '^($(IMAGE)|$(PROVIDER_IMAGE)|$(DEVCHAIN_IMAGE)):$(VERSION) '

docker-build-host:
	@test -n "$(REFERENCE_HOST_IMAGE)" || { echo "set REFERENCE_HOST_IMAGE to the reference overlay host image the module is added to"; exit 1; }
	docker build -f host/Dockerfile --build-arg REFERENCE_HOST_IMAGE=$(REFERENCE_HOST_IMAGE) -t $(HOST_IMAGE):$(VERSION) -t $(HOST_IMAGE):latest .
	@docker image ls --format '{{.Repository}}:{{.Tag}}  {{.Size}}' | grep -E '^$(HOST_IMAGE):$(VERSION) '

# The images with no network at all: each command's stamped version and
# help, a first run of each in a fresh named volume mounted where the
# documentation says, and a provider that refuses a configuration with an
# unknown key.
docker-smoke: docker-build
	@set -eu; id=bstore-smoke-$$$$; \
	docker volume create $$id-home >/dev/null; docker volume create $$id-prov >/dev/null; \
	trap 'docker volume rm -f $$id-home $$id-prov >/dev/null' EXIT; \
	echo "== versions"; \
	out=$$(docker run --rm --network none $(IMAGE):$(VERSION) -version); echo "$$out"; [ "$$out" = "bstore $(VERSION)" ]; \
	out=$$(docker run --rm --network none $(PROVIDER_IMAGE):$(VERSION) version); echo "$$out"; [ "$$out" = "bstore-provider $(VERSION)" ]; \
	out=$$(docker run --rm --network none $(DEVCHAIN_IMAGE):$(VERSION) -version); echo "$$out"; [ "$$out" = "devchain $(VERSION)" ]; \
	echo "== help"; docker run --rm --network none $(IMAGE):$(VERSION) help | head -1; \
	docker run --rm --network none $(IMAGE):$(VERSION) s3d -h 2>&1 | head -1; \
	docker run --rm --network none $(PROVIDER_IMAGE):$(VERSION) -h | head -1; \
	echo "== init in a named volume"; \
	docker run --rm --network none -v $$id-home:/home/nonroot/.bstore $(IMAGE):$(VERSION) init -network regtest \
		-node http://192.0.2.1 -settle rpc:http://192.0.2.1/rpc -headers http://192.0.2.1 -hosts http://192.0.2.1 | grep -E '^(identity|config) '; \
	docker run --rm --network none -v $$id-prov:/var/lib/bstore-provider $(PROVIDER_IMAGE):$(VERSION) init -data /var/lib/bstore-provider | grep '^identity '; \
	echo "== a provider configuration with an unknown key is refused"; \
	rc=0; out=$$(printf '{"data":"/var/lib/bstore-provider","nope":1}' | docker run --rm -i --network none --entrypoint /usr/local/bin/bstore-provider \
		$(PROVIDER_IMAGE):$(VERSION) run -config /dev/stdin 2>&1) || rc=$$?; echo "$$out"; \
	[ "$$rc" = 2 ] || { echo "docker-smoke: want exit 2, got $$rc"; exit 1; }; \
	echo "docker-smoke: ok"

# The host image loads the module with the host's own @bsv/sdk, and refuses
# to start with no bucket configured.
docker-smoke-host:
	@set -eu; img=$(HOST_IMAGE):$(VERSION); \
	echo "== the module imports from the host's node_modules"; \
	docker run --rm --network none --entrypoint node $$img -e \
		"import('/app/modules/bstore/bstore-module.js').then(m => { if (typeof m.default !== 'function') process.exit(1); console.log('bstore module: default export is the factory') })"; \
	echo "== no BSTORE_BUCKETS: exit 64 with the reason"; \
	rc=0; out=$$(docker run --rm --network none $$img 2>&1) || rc=$$?; echo "$$out"; \
	[ "$$rc" = 64 ] && echo "$$out" | grep -q 'BSTORE_BUCKETS is empty' || { echo "docker-smoke-host: want exit 64, got $$rc"; exit 1; }; \
	echo "docker-smoke-host: ok ($$img)"

# QUICKSTART.md, run as written, in a throwaway copy of the quickstart
# compose project. Needs this repository's four images (docker-build,
# docker-build-host) and the AWS CLI v2 (aws) on PATH.
quickstart-check:
	scripts/quickstart-check.sh
