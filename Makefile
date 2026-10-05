# GOWORK=off everywhere on purpose: a workspace would resolve bcommon to a
# local checkout, so a go.mod skew would stay invisible here. What is tested
# is what go.mod pins.
#
# The TypeScript package under host/ needs Node 24 and its dependencies
# installed once (cd host && npm ci). The targets run tsc and node directly,
# so NODE may name a Node 24 binary that is not on PATH:
#
#	make verify NODE=/path/to/node24

.PHONY: verify test go-test go-oracle ts-test ts-check ts-build module module-test e2e node-check vet fmt-check deps-check vectors vectors-check fuzz

NODE ?= node
TSC := $(NODE) node_modules/typescript/bin/tsc

verify: fmt-check vet deps-check ts-check vectors-check test

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
	GOWORK=off go test -count=1 -run 'TestPageChecksAgainstOracle$$' ./internal/storerec/

vet:
	GOWORK=off go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "gofmt:"; gofmt -l .; exit 1; }

# The direct dependencies, asserted rather than intended:
#  - go.mod and go.sum are tidy, and no module is replaced;
#  - the direct set is exactly go-sdk and bcommon;
#  - go-sdk resolves to exactly v1.5.2, the version bcommon pins and the
#    vectors were made with. Minimal version selection takes the higher of
#    two pins, so a library asking for a later go-sdk would change what
#    bstore ships without a line changing here.
DEPS_DIRECT := github.com/bsv-blockchain/go-sdk github.com/lightwebinc/bcommon
DEPS_SDK    := github.com/bsv-blockchain/go-sdk v1.5.2

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
		GOWORK=off go test ./internal/storerec/ -run '^$$' -fuzz "^$$f\$$" -fuzztime $(FUZZTIME) || exit 1; \
	done
