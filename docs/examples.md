# Examples

Each task below on mainnet, with the testnet variant where it differs. The
commands are real; `example.com` addresses stand for the services you use.
[usage.md](usage.md) says what each command does step by step,
[configuration.md](configuration.md) lists every flag and key, and
[limits.md](limits.md) what each step costs. To try everything with no coin
on one machine, use the development sandbox in
[../QUICKSTART.md](../QUICKSTART.md).

## What you need

| What | Mainnet | Testnet |
| --- | --- | --- |
| `-network` | `main` | `test` |
| Overlay hosts (`-hosts`) | hosts that carry your bucket ([host.md](host.md)) | the same |
| Coin | 100,000 satoshis or more in your own wallet | testnet coin from your own testnet wallet |

Nothing else. No node is needed: unless you name others, the chain view
and the header source are WhatsOnChain (`woc:main`, `woc:test`; every
header held to its proof of work, every proof to those headers) and
transactions are broadcast through GorillaPool's public arcade
(`arcade:main`, `arcade:test`). Each is a setting:

| Optional | Mainnet | Testnet |
| --- | --- | --- |
| A chain view (`-chain`) | your own node, `asset:<url>`: the better choice, since WhatsOnChain's "unspent" is taken on its word | the same, on testnet |
| A settlement leg (`-settle`) | your own arcade (`arcade:<url>`), ARC (`arc:<url>`), or a node (`rpc:<url>`) | the same, on testnet |
| A header source (`-headers`) | your own block-headers-service (`bhs:<url>`), an arcade's (`arcade:<url>`), or `chaintracks:<url>` | the same, on testnet |
| A WhatsOnChain API key (`-woc-key`) | for a busy home, gateway or provider: the free tier allows 3 requests a second | the same |

## What it costs

bstore pays the network's rate, 100 satoshis per 1000 bytes, with a floor
of 250 satoshis a transaction (the `fee` setting; `-fee-source arc`
follows the broadcaster's published rate instead). The fees below were
measured at the older 1 satoshi a byte and are upper figures: at the
network's rate every transaction under 2,500 bytes pays the floor. From
[limits.md](limits.md):

| Step | Mined bytes | Fee |
| --- | --- | --- |
| A bucket token (`bucket new`, each `anchor`) | 616 to 727 | about 620 to 730 satoshis |
| A piece funding tree, 256 outputs (minted ahead, one a put of up to 256 pieces) | about 13 KB | about 13,300 satoshis |
| A 100 MiB put's funding, from those trees | about 1.4 KB | about 1,400 satoshis |
| A hold output's transaction (with a bounty) | 240 | about 495 satoshis, plus the bounty |
| A settlement | about 406 | about 446 satoshis |
| A challenge, a renewal | not mined by you | the provider's price: 40 at `c = 30` on terms of 10 + 1; a renewal's minimum, often 500 |

100,000 satoshis (0.001 BSV) is ample to make a bucket and put objects
without a bounty. A bounty locks `hold_ahead` (8) hold outputs of its size
ahead of time; set `hold_ahead` lower or fund more.

## Set up a home and fund it

```console
$ bstore init -network main \
    -hosts https://overlay-a.example.com,https://overlay-b.example.com \
    -providers https://provider.example.com
identity 02...
config /home/you/.bstore/config.json
$ bstore fund
identity     02...
fund address 1...
pool         0 output(s), 0 satoshis
```

Send 100,000 satoshis from your own wallet to the fund address, then
import the payment. If your wallet hands you the payment as BEEF, import
that at once; it need not have mined:

```console
$ bstore fund -beef payment.beef
4f3c...e9a1 not mined yet (held until it mines): 1 output(s) to the fund address, 1 imported (100000 satoshis), 0 already held, 0 spent
pool 1 output(s), 100000 satoshis
```

Or, once the payment has a block, import it by its txid:

```console
$ bstore fund -txid 4f3c...e9a1
4f3c...e9a1 mined at height <h>: 1 output(s) to the fund address, 1 imported (100000 satoshis), 0 already held, 0 spent
pool 1 output(s), 100000 satoshis
$ bstore doctor
```

`fund -beef` reads the payment from the BEEF (a file, or `-` for standard
input) with no lookup: a mined payment's proof is checked against your
header source, and one not mined yet is taken when every transaction it
spends is proven, sent to the settlement leg in case your wallet did not
broadcast it, and held in the pool until it mines (`-refuse-unmined`, or
`refuse_unmined` in `config.json`, takes mined payments only). `fund -txid`
fetches the payment and its proof from the chain view, checks the proof
against your header source and asks the chain view that each output is
unspent; before the payment is mined it exits 3 (`not mined yet`). A
payment that pays the fund address nothing is refused (exit 1). Importing
it twice adds nothing. `doctor` shows the pool, the fee policy and whether
the chain view, the header source, the settlement leg, the hosts and the
providers answer; it spends nothing.

Given a mined mainnet transaction that pays someone else, a fresh home
answers:

```console
$ bstore fund -txid 52854619b1c2e78d6c1e9a91fdb14c4bef1b8d1897de3253a538e152bd055be6
bstore: bwallet: the payment pays nothing to this wallet's fund address: 52854619b1c2e78d6c1e9a91fdb14c4bef1b8d1897de3253a538e152bd055be6
```

The transaction was fetched from WhatsOnChain and its proof checked
against the WhatsOnChain headers before its outputs were read.

On testnet: `-network test`; the fund address starts with `m` or `n`.

Keep `~/.bstore` backed up: the wallet in it is the identity and its coin.
Several identities are several homes (`-home DIR` or `BSTORE_HOME`).

## Own a bucket

```console
$ bstore bucket new photos -writers 03...
bucket photos_hmqzvtkwrx
owner  02...
id     5f...
served photos_hmqzvtkwrx:02...:5f...
topics tm_bstore_photos_hmqzvtkwrx tm_bstoreblob_photos_hmqzvtkwrx
```

Give each host's operator the `served` line and the providers' operators
the bucket identifier. Until a host carries the bucket, `bucket new` exits
3 with its first token kept; `bstore sync` delivers it once they do. Then
fold writers' entries into the bucket whenever you like:

```console
$ bstore anchor -dry-run           # what the next token would hold
$ bstore anchor                    # mint it
$ bstore anchor -add-writer 02...  # name another writer
```

## Write

```console
$ bstore put photos/cat.jpg ./cat.jpg
$ bstore put photos/cat.jpg ./cat.jpg -bounty 20000 -t 2 -k 3 -deadline 144
$ tar c ./album | bstore put backups/album.tar -
$ bstore cp photos/cat.jpg photos/cat-copy.jpg
$ bstore rm photos/old.jpg
$ bstore due                       # what is still owed; exit 3 while anything is
$ bstore sync                      # send it
```

A put is pending until the owner's next `anchor`. With a bounty, your own
client judges the race:

```console
$ bstore holds -watch 10m          # rank advertisers, challenge each
$ bstore settle                    # pay the winners by rank
$ bstore reclaim                   # after the deadline, take back a bounty nobody won
```

## Read

A read needs no coin, only a home that names the bucket:

```console
$ bstore bucket add photos_hmqzvtkwrx -owner 02... -id 5f...
$ bstore -bucket photos_hmqzvtkwrx ls photos/
$ bstore -bucket photos_hmqzvtkwrx get photos/cat.jpg ./cat.jpg -proof-root
$ bstore -bucket photos_hmqzvtkwrx get photos/cat.jpg - | sha256sum
```

Exit 0 is a verified file; 1 a key proven absent (`NoSuchKey`); 3 a key no
host or advertiser produced.

## Check a provider, buy more time

```console
$ bstore terms https://provider.example.com/store/bstore/v1
$ bstore challenge photos/cat.jpg -provider 03... -c 113 -budget 2000
$ bstore renew photos/cat.jpg -provider 03... -days 30 -budget 5000
```

A challenge's outcome is kept in the home and published on the bucket's
records topic (`-no-publish` keeps it local). Both pay the provider's price
from the pool, never more than `-budget`.

## Use S3

Against a gateway someone runs for you:

```console
$ export AWS_ACCESS_KEY_ID=EXAMPLEACCESSKEY AWS_SECRET_ACCESS_KEY=...
$ aws --endpoint-url https://s3.example.com s3 cp ./cat.jpg s3://photos-hmqzvtkwrx/photos/cat.jpg
$ aws --endpoint-url https://s3.example.com s3 ls s3://photos-hmqzvtkwrx/photos/
$ aws --endpoint-url https://s3.example.com s3 cp s3://photos-hmqzvtkwrx/photos/cat.jpg ./cat.jpg
```

Running one ([s3.md](s3.md)):

```console
$ bstore s3d init -config s3d.json
identity main 03...
$ bstore s3d address -config s3d.json -identity main
$ bstore s3d fund -config s3d.json -identity main -beef payment.beef   # or -txid <txid>
$ bstore s3d run -config s3d.json
```

## Run a provider

```console
$ bstore-provider init -data /var/lib/bstore-provider
$ bstore-provider address -config provider.json
$ bstore-provider fund -config provider.json -beef payment.beef        # or -txid <txid>
$ bstore-provider doctor -config provider.json
$ bstore-provider run -config provider.json
```

`provider.json` needs `"mainnet": true` on mainnet, a public `https`
`public_url`, and the buckets you serve ([provider.md](provider.md)); with
no `chain`, `settle` or `headers` it uses WhatsOnChain and GorillaPool's
arcade. A provider answers every writer's entries and challenges, so it
reads the chain far more than a home: past WhatsOnChain's free 3 requests
a second it needs an API key (`woc_key`, with `woc_rate` raised to the
plan's) or a node of its own (`chain: asset:<url>`). The S3 gateway is the
same.

## In a container

```console
$ docker build -t bstore .
$ docker run --rm -v bstore-home:/home/nonroot/.bstore bstore init -network main \
    -hosts https://overlay.example.com
$ docker run --rm -v bstore-home:/home/nonroot/.bstore bstore fund
$ docker run --rm -v bstore-home:/home/nonroot/.bstore bstore \
    get photos/cat.jpg - -proof-root > cat.jpg
```

The home lives in the named volume; standard output carries the file, so
no bind mount needs to be writable by the image's user (uid 65532).

## Stop everything

```console
$ bstore kill -confirm KILL        # what would be swept
$ bstore kill -confirm KILL -yes   # sweep every funding tree: irreversible
```

Every entry this identity has not had anchored is retracted, and it
publishes nothing after.
