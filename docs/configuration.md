# Configuration

Every flag, environment variable and configuration key of the bstore
commands and images, from the code. The servers' longer tables live with
their guides and are linked here: the provider's in
[provider.md](provider.md#configure), the S3 gateway's in
[s3.md](s3.md#configure) and the host module's in
[host.md](host.md#configuration). [usage.md](usage.md) says what each
command does.

## Networks and endpoints

Three settings recur in every configuration: the network, where proofs and
spends come from, and where headers come from.

| Setting | Values |
| --- | --- |
| network | `main` (mainnet), `test` (testnet), `regtest` (a chain you run). For the provider and the gateway, `mainnet: true` or `false` selects the fund address form; `false` gives the testnet and regtest form |
| node | a Teranode asset API base URL (`/api/v1/...`): proofs, spends, block hashes and the tip. Yours, or one you are given access to |
| settle | the settlement leg transactions are broadcast through: `arcade:<url>` (an arcade broadcaster; key in `arcade_key`), `tcp:<host:port>` (a fabric ingress), or `rpc:<url>` (a node's JSON-RPC, with `rpc_user`, `rpc_pass`). Payment acceptance before a block (`accept_threshold_sats`) needs `arcade:` |
| headers | the header source proofs are checked against: `woc:main` or `woc:test` (the public WhatsOnChain API, proof of work checked here), `chaintracks:<url>` (a chaintracks v2 service), or a bridge URL (an overlay bridge's native `/v1/root/<height>` and `/v1/tip`, which carries no proof of work to check) |

On mainnet use `woc:main` or a chaintracks service you trust; on testnet
`woc:test`. A regtest chain you run serves its own headers in the bridge
shape (the [quickstart](../QUICKSTART.md)'s `devchain` does).

## bstore

### Global flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-home DIR` | `$BSTORE_HOME`, else `~/.bstore` | the identity's home |
| `-bucket B` | `config.json`'s `bucket`, else the only bucket listed | the bucket: its identifier, or its S3 name (`-` for `_`) |
| `-hosts URLS` | `config.json`'s `hosts` | overlay hosts, comma separated, for this run |
| `-v` | off | verbose: the log on standard error |
| `-version` | | print the version (also `bstore version`) |

`-v` and `-bucket` are accepted after a command's name too.

### Environment

| Variable | Meaning |
| --- | --- |
| `BSTORE_HOME` | the home when `-home` is not given |

### Command flags

| Command | Flags |
| --- | --- |
| `init` | `-network` (`main`, `test`, `regtest`; default `main`), `-node URL`, `-settle LEG`, `-headers SPEC`, `-hosts URLS`, `-facade URL`, `-providers URLS`, `-uploads URLS`, `-ca FILE`, `-fetch-private` |
| `fund` | none: print the fund address and pool. `-txid TXID`: import a mined payment you sent to the fund address (mainnet, testnet). `-blocks N`; `-from H` `[-to H]`: **coinbase: only on a regtest chain you run (development and tests)**. `-from` finds coinbase only, never an ordinary payment |
| `doctor`, `sync`, `fill`, `due` | none |
| `terms URL` | none |
| `bucket new NAME` | `-writers K1,K2` |
| `bucket add BUCKET` | `-owner KEY`, `-id HEX` |
| `bucket list` | none |
| `anchor` | `-add-writer KEY` (repeatable), `-remove-writer KEY` (repeatable), `-dry-run` |
| `put KEY [FILE\|-]` | `-type MIME`, `-meta k=v` (repeatable), `-max BYTES` (default 2^43), `-bounty SATS`, `-t N`, `-k N`, `-retention SECONDS`, `-deadline BLOCKS`, `-check-c N`, `-no-bounty` |
| `cp SRC DST` | `-to BUCKET`, and the bounty flags of `put` |
| `rm KEY` | none |
| `get KEY [OUT\|-]` | `-proof-root` |
| `ls [PREFIX]` | `-after KEY`, `-max N` |
| `holds` | `-entry TXID`, `-watch DURATION`, `-budget SATS` |
| `settle` | `-entry TXID`, `-close`, `-wait DURATION`, `-budget SATS` |
| `reclaim` | `-entry TXID`, `-unused` |
| `challenge KEY` | `-provider KEY`, `-c N` (default 30), `-budget SATS`, `-nonce HEX`, `-front URL`, `-answerer KEY`, `-no-publish` |
| `renew KEY` | `-provider KEY`, `-until UNIX` or `-days N`, `-budget SATS`, `-front URL` |
| `kill` | `-confirm KILL`, `-yes` |

### config.json

`bstore init` writes it in the home; an unknown key is refused.

| Key | Default | Meaning |
| --- | --- | --- |
| `network` | required | `main`, `test` or `regtest` |
| `node`, `settle` (`rpc_user`, `rpc_pass`, `arcade_key`), `headers` | required | the node's asset API (proofs, spends, the tip), the settlement leg, the header source (above) |
| `hosts` | required | the overlay hosts asked and written to |
| `facade` | none | the plane: pieces and records are submitted to it once |
| `providers` | none | providers' submit routes: without a facade, entries (and piece carriers) go to each |
| `uploads` | none | providers' public URLs: without a facade, a closure goes to each by BRC-167 upload session instead of as piece carriers |
| `quorum` | 1 | hosts that must take an entry when there is no facade |
| `ca`, `fetch_private` | none, false | certificates trusted besides the system's; whether advertised URLs at loopback, private or link-local addresses are reached |
| `bounty` | none | what a put asks for unless told otherwise: `satoshis`, `t`, `k`, `retention_seconds`, `deadline_blocks`, `check_c` |
| `hold_ahead` | 8 | mined hold outputs of a bounty kept ready |
| `race_window_seconds` | 3600 | the writer's race window |
| `publish_outcomes` | true | publish failed settlement challenges and every challenge's outcome as outcome records |
| `piece_tree`, `entry_tree` | 256, 16 | outputs of a new funding tree (outcome trees take `entry_tree`); the next tree of each kind is minted ahead, in the background, so a put of up to `piece_tree` pieces never waits for a block |
| `fee_sat_per_byte`, `fee_floor` | 1, 250 | the fee policy: satoshis a byte (a whole number; 1 is the least) and the least fee of a transaction |
| `accept_threshold_sats` | 25000000 | a settlement whose largest payout is at or below it is remitted once arcade says the network took it and the node shows its inputs spent by it alone, before it mines (race state `remitted`); above it, remitted once mined. Needs `settle = arcade:<url>` |
| `accept_payer_limit`, `accept_total_limit`, `accept_window_seconds`, `accept_watch_seconds` | one and ten thresholds, 3600, 0 | the rest of the acceptance rule, as for a provider ([provider.md](provider.md)) |
| `buckets`, `bucket` | none | the chains this home reads and writes (`{"bucket", "owner", "id"}`), and the one used unless `-bucket` names another |

A mainnet home:

```json
{
  "network": "main",
  "node": "https://node.example.com",
  "settle": "arcade:https://arcade.example.com",
  "headers": "woc:main",
  "hosts": ["https://overlay-a.example.com", "https://overlay-b.example.com"],
  "providers": ["https://provider.example.com"],
  "buckets": [{"bucket": "photos_hmqzvtkwrx", "owner": "02...", "id": "5f..."}]
}
```

### The home's files

| Path | Holds |
| --- | --- |
| `config.json` | the configuration |
| `wallet/` | the identity key and the pool; `wallet/paycoins.json` the small coins paid calls take |
| `store/` | the closures this home built |
| `outcomes.jsonl` | every challenge's outcome |
| the rest | funding trees, the outbox, hold races and the chains it owns |

Back the home up: the wallet is the identity. One process uses a home at a
time.

## bstore s3d

`bstore s3d <command> -config FILE`; the configuration file is in
[s3.md](s3.md#configure).

| Command | Flags |
| --- | --- |
| `init` | `-config FILE`: create every identity's wallet |
| `address` | `-config FILE -identity NAME` |
| `fund` | `-config FILE -identity NAME -txid TXID`: import a mined payment you sent to the identity's fund address (mainnet, testnet). `-blocks N`: **coinbase: only on a regtest chain you run (development and tests)** |
| `rescan` | `-config FILE -identity NAME -from H [-to H]`: **coinbase: only on a regtest chain you run (development and tests)** |
| `run` | `-config FILE` |

The configuration's keys: `data`, `listen`, `tls_cert`, `tls_key`,
`region`, `domain`, `node`, `settle`, `rpc_user`, `rpc_pass`,
`arcade_key`, `headers`, `hosts`, `facade`, `providers`, `quorum`, `ca`,
`fetch_private`, `identities`, `access_keys`, `buckets`, `bounty`,
`hold_ahead`, `limits.*`, `piece_tree`, `entry_tree`, `piece_ahead`,
`piece_workers`, `fee_sat_per_byte`, `fee_floor`, `mainnet`, the `accept_`
keys, `race.*`, `store_bytes`, `outbox_bytes` and `metrics_listen`.

## bstore-provider

`bstore-provider <command> [flags]`; the configuration file is in
[provider.md](provider.md#configure).

| Command | Flags |
| --- | --- |
| `init` | `-data DIR` |
| `address` | `-config FILE` |
| `fund` | `-config FILE -txid TXID`: import a mined payment you sent to the fund address (mainnet, testnet). `-blocks N`: **coinbase: only on a regtest chain you run (development and tests)** |
| `rescan` | `-config FILE -from H [-to H]`: **coinbase: only on a regtest chain you run (development and tests)** |
| `run` | `-config FILE` |
| `doctor` | `-config FILE`, `-ca FILE` (a PEM file trusted for the front's certificate) |
| `version` | |

The configuration's keys: `data`, `listen`, `tls_cert`, `tls_key`,
`public_url`, `node`, `settle`, `rpc_user`, `rpc_pass`, `arcade_key`,
`headers`, `hosts`, `facade`, `uhrp_topic`, `buckets`, `backend`,
`export`, `quota_bytes`, `unclaimed_bytes`, `unclaimed_seconds`,
`policy.*`, `terms.*`, `limits.*`, `tree_count`, `tree_sats`,
`fee_sat_per_byte`, `fee_floor`, `mainnet`, `trusted_proxies`,
`metrics_listen` and the `accept_` keys.

## The host module

Environment variables of an overlay host loading the module, all
`BSTORE_`: `BUCKETS`, `STATE_DIR`, `HEADERS_URL`, `SERVED`, `KEEP_DAYS`,
`PENDING_DAYS`, `OTHER_HOURS`, `MAX_BEEF`, `MAX_SWEEP_INPUTS`, `LISTEN`,
`PRICES`, `PAYEE_KEY`, `SESSIONS`, `SESSION_TTL`, `HANDSHAKES_PER_SEC`,
`HANDSHAKE_BURST`, `HANDSHAKES_PER_ADDR_PER_SEC`, `HANDSHAKE_ADDR_BURST`,
`RESPONSES_PER_SEC`, `RESPONSE_BURST`, `ARCADE_URL`, `ASSET_URL`,
`ACCEPT_THRESHOLD_SATS`, `ACCEPT_PAYER_LIMIT`, `ACCEPT_TOTAL_LIMIT`,
`ACCEPT_WINDOW` and `ACCEPT_WATCH`, with the host's own `OVERLAY_*`
settings. Defaults and meanings: [host.md](host.md#configuration).

## devchain

The local stand-in chain of the development sandbox: **coinbase: only on a
regtest chain you run (development and tests)**. Nothing it mines is money.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen ADDR` | `:8080` | where the RPC (`/rpc`), the asset API (`/api/v1/`), arcade (`/arcade/`) and the header source (`/v1/`) are served |
| `-height N` | 700 | the tip before anything is mined |
| `-journal FILE` | none | append every accepted RPC call and replay it at start |
| `-version` | | print the version |

`devchain certs -dir DIR -names a,b` writes a throwaway CA and a server
certificate for each name.

## Container images

Built from the [Dockerfile](../Dockerfile); static binaries on a distroless
nonroot base (uid 65532). No `ENV` defaults are baked in: every endpoint is
an address of somebody's deployment.

| Image | Build | Entrypoint | Port | State to mount |
| --- | --- | --- | --- | --- |
| `ghcr.io/lightwebinc/bstore` | `docker build .` | `bstore` | 9000 (`s3d run`) | `/home/nonroot/.bstore` (a home), `/var/lib/bstore-s3d` |
| `ghcr.io/lightwebinc/bstore-provider` | `--target provider` | `bstore-provider` | 8095 | `/var/lib/bstore-provider` |
| `ghcr.io/lightwebinc/bstore-devchain` | `--target devchain` | `devchain -journal /var/lib/devchain/journal` | 8080 | `/var/lib/devchain`, `/certs` |
| `ghcr.io/lightwebinc/bstore-host` | `-f host/Dockerfile --build-arg REFERENCE_HOST_IMAGE=...` | the reference host with the module | the host's | `BSTORE_STATE_DIR` |

Each image carries the licenses under `/usr/share/doc/bstore/` (`LICENSE`,
`NOTICE`, `LICENSE-THIRD-PARTY`). A named volume mounted on a state
directory starts owned by nonroot; a bind mount keeps the host directory's
owner, so make it writable by uid 65532 or run with `--user`.

```console
$ docker build -t bstore .
$ docker run --rm -v bstore-home:/home/nonroot/.bstore bstore init -network main \
    -node https://node.example.com -settle arcade:https://arcade.example.com \
    -headers woc:main -hosts https://overlay.example.com
$ docker run --rm -v bstore-home:/home/nonroot/.bstore bstore fund
$ docker run --rm -v bstore-home:/home/nonroot/.bstore bstore fund -txid <txid>
```

`ghcr.io/lightwebinc/bstore` is published by a manual workflow
(`.github/workflows/image-publish.yml`) once a release is tagged; build the
provider, devchain and host images from a checkout (`make docker-build`,
`make docker-build-host REFERENCE_HOST_IMAGE=...`).
