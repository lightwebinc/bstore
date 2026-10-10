#!/usr/bin/env bash
# QUICKSTART.md, run as written, in a throwaway copy of the quickstart
# compose project: the local chain and its throwaway CA; the owner's
# bucket, made before the hosts carry it; two hosts over MySQL, two
# providers and the S3 gateway; the gateway's identity named a writer; a
# 20 MiB put through S3 with a bounty; the hold race judged on the
# gateway's identity, both providers passing a free settlement challenge;
# settlement by rank, both providers paid by remittance; the owner
# anchors; the object read back through S3 and by a reader's verified get;
# a paid challenge and a renewal bought by the reader; a delete, the
# owner's anchor, and NoSuchKey through S3 and from the reader; nothing
# owed.
#
#   scripts/quickstart-check.sh
#
# Needs the images the compose file names: this repository's four (make
# docker-build, make docker-build-host), and the AWS CLI v2 on PATH.
# COMPOSE names the compose command (default "docker compose", else
# Compose from the docker:cli image with the work directory mounted where
# it is). Everything
# it creates, volumes included, is removed at the end, pass or fail.
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
project=bstore-quickstart-check-$$
cp "$here/deploy/quickstart/compose.yaml" "$work/"
cd "$work"
if [ -n "${COMPOSE:-}" ]; then
	compose=$COMPOSE
elif docker compose version >/dev/null 2>&1; then
	compose="docker compose"
else
	compose="docker run --rm -i -v /var/run/docker.sock:/var/run/docker.sock -v $work:$work -w $work docker:cli compose"
fi
command -v aws >/dev/null || { echo "quickstart-check: the AWS CLI v2 (aws) is needed" >&2; exit 1; }

dc() { $compose -p "$project" "$@"; }
bs() { local who=$1; shift; dc run --rm -T -e "BSTORE_HOME=/home/nonroot/.bstore/$who" bstore "$@"; }
gw() { dc run --rm -T gw "$@"; }
prov() { local p=$1; shift; dc run --rm -T --no-deps "$p" "$@"; }
s3() { AWS_ACCESS_KEY_ID=QUICKSTARTACCESSKEY AWS_SECRET_ACCESS_KEY=quickstart-secret-not-secret AWS_DEFAULT_REGION=us-east-1 \
	AWS_EC2_METADATA_DISABLED=true aws --endpoint-url "http://127.0.0.1:${BSTORE_S3_PORT:-9000}" "$@"; }
cleanup() {
	dc --profile cli down -v --remove-orphans >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT
step() { printf '\n== %s\n' "$*"; }
fail() {
	echo "quickstart-check: $*" >&2
	dc logs --no-color --tail 40 >&2 || true
	exit 1
}
INIT="-network regtest -node http://chain:8080 -settle rpc:http://chain:8080/rpc -headers http://chain:8080
 -hosts http://host-a:8080,http://host-b:8080 -providers https://provider-a:8095,https://provider-b:8095
 -ca /certs/ca.pem -fetch-private"

step "1. the local chain and its throwaway CA"
dc up -d chain
dc run --rm -T certs

step "2. the owner and a reader, each a home"
# shellcheck disable=SC2086
bs owner init $INIT | tee owner.txt
# shellcheck disable=SC2086
bs reader init $INIT
bs owner fund -blocks 101

step "3. the bucket, before any host carries it"
rc=0; bs owner bucket new photos | tee bucket.txt || rc=$?
[ "$rc" = 3 ] || fail "bucket new: want exit 3 (no host yet), got $rc"
B=$(sed -n 's/^bucket *//p' bucket.txt) O=$(sed -n 's/^owner *//p' bucket.txt) I=$(sed -n 's/^id *//p' bucket.txt)
[ -n "$B" ] && [ -n "$O" ] && [ -n "$I" ] || fail "bucket new printed no bucket"
{
	echo "BSTORE_BUCKETS=$B"
	echo "BSTORE_SERVED=$B:$O:$I"
	echo "BSTORE_OWNER=$O"
	echo "BSTORE_ID=$I"
	echo "BSTORE_PAYEE_KEY=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
} >.env
S3B=$(printf '%s' "$B" | tr _ -)

step "4. two providers and the gateway's identity, funded"
for p in provider-a provider-b; do prov $p init -data /var/lib/bstore-provider; prov $p fund -config /etc/bstore/provider.json -blocks 1; done
dc run --rm -T --no-deps gateway s3d init -config /etc/bstore/gateway.json | tee gw.txt
GW=$(sed -n 's/^identity gw \([0-9a-f]\{66\}\).*/\1/p' gw.txt)
[ -n "$GW" ] || fail "s3d init printed no identity"
dc run --rm -T --no-deps gateway s3d fund -config /etc/bstore/gateway.json -identity gw -blocks 2
bs reader fund -blocks 3
bs owner fund -blocks 100

step "5. start everything; the first token reaches both hosts; the gateway a writer"
dc up -d
for i in $(seq 1 90); do
	out=$(bs owner doctor 2>/dev/null || true)
	[ "$(printf '%s\n' "$out" | grep -c ' answers (')" = 4 ] && break
	sleep 2
done
printf '%s\n' "$out"
[ "$(printf '%s\n' "$out" | grep -c ' answers (')" = 4 ] || fail "the hosts and providers did not come up"
bs owner sync
bs owner anchor -add-writer "$GW" | tee anchor0.txt

step "6. a 20 MiB put through S3, with a bounty"
head -c 20971520 /dev/urandom >obj.bin
s3 s3api put-object --bucket "$S3B" --key photos/cat.bin --body obj.bin | tee put.json
grep -q ETag put.json || fail "put-object"

step "7. the hold race, on the gateway's identity"
dc stop gateway
for i in $(seq 1 30); do
	gw holds -watch 20s | tee holds.txt || true
	[ "$(grep -c ' passed  last: pass c=30' holds.txt)" -ge 2 ] && break
	sleep 5
done
[ "$(grep -c ' passed  last: pass c=30' holds.txt)" -ge 2 ] || fail "two providers did not pass"
gw settle | tee settle.txt
[ "$(grep -c 'satoshis  output [0-9]  remitted' settle.txt)" = 2 ] || fail "settle: both remitted"
dc start gateway

step "8. the owner anchors"
bs owner anchor | tee anchor1.txt
grep -q '^minted 1 token(s): 1 change(s)' anchor1.txt || fail "anchor"

step "9. read back through S3 and by a reader"
for i in $(seq 1 30); do s3 s3api head-object --bucket "$S3B" --key photos/cat.bin >/dev/null 2>&1 && break; sleep 2; done
s3 s3api get-object --bucket "$S3B" --key photos/cat.bin got-s3.bin >/dev/null
cmp obj.bin got-s3.bin || fail "the S3 read differs"
bs reader -bucket "$B" bucket add "$B" -owner "$O" -id "$I"
bs reader -bucket "$B" get photos/cat.bin - -proof-root >got-reader.bin
cmp obj.bin got-reader.bin || fail "the reader's read differs"

step "10. a paid challenge and a renewal, bought by the reader"
PA=$(prov provider-a address -config /etc/bstore/provider.json | sed -n 's/^identity //p')
bs reader -bucket "$B" challenge photos/cat.bin -provider "$PA" -c 30 -budget 1000 | tee challenge.txt
grep -q ': pass (c = 30, paid 40)' challenge.txt && grep -q '^  outcome record [0-9a-f]\{64\}' challenge.txt || fail "challenge, or its outcome not published"
bs reader sync >/dev/null || true
rc=0; bs reader -bucket "$B" renew photos/cat.bin -provider "$PA" -days 7 -budget 2000 >renew.txt || rc=$?; cat renew.txt
# The local chain has no arcade, so the provider takes the payment and holds
# it for confirmation (202), and makes the renewal itself once it mines.
if ! { [ "$rc" = 0 ] && grep -q ': 200 (paid ' renew.txt; }; then
	[ "$rc" = 3 ] && grep -q ': 202 (paid ' renew.txt || fail "renew: exit $rc"
	made=""
	for _ in $(seq 1 60); do
		dc logs --no-color provider-a 2>/dev/null | grep -q 'mined; the renewal through [0-9]* is made now' && { made=yes; break; }
		sleep 2
	done
	[ -n "$made" ] || fail "the held renewal was never made"
fi

step "11. delete, anchor, NoSuchKey"
s3 s3api delete-object --bucket "$S3B" --key photos/cat.bin
bs owner anchor | tee anchor2.txt
rc=0; s3 s3api get-object --bucket "$S3B" --key photos/cat.bin gone.bin >gone.txt 2>&1 || rc=$?
grep -q NoSuchKey gone.txt || fail "S3 NoSuchKey, got exit $rc: $(cat gone.txt)"
rc=0; bs reader -bucket "$B" get photos/cat.bin - >/dev/null 2>reader-gone.txt || rc=$?
[ "$rc" = 1 ] || fail "reader get after delete: want exit 1, got $rc: $(cat reader-gone.txt)"

step "12. nothing owed"
dc stop gateway
gw sync || true
gw due | tee due.txt || fail "the gateway's identity owes something"
bs owner due

echo
echo "quickstart-check: ok"
