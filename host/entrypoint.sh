#!/bin/sh
# Starts the overlay host with the bstore module. Three things are added to
# the stock start:
#
#  - BSTORE_BUCKETS is required. When OVERLAY_TOPICS is unset it is
#    derived: tm_bstore_<bucket> for each bucket, the records topic. A
#    directory host does not carry the blob topics (docs/host.md): its blob
#    manager refuses every piece, so set OVERLAY_TOPICS yourself to name
#    one.
#  - when OVERLAY_MODULES is unset the bstore module is loaded.
#  - an unset OVERLAY_ADMIN_TOKEN is replaced by a random one for this run,
#    because the host requires one. Nothing can use the admin surface until
#    the operator sets their own.
set -eu

if [ -z "${BSTORE_BUCKETS:-}" ]; then
  echo "bstore-host: BSTORE_BUCKETS is empty. Make a bucket with 'bstore bucket new <name>' and set BSTORE_BUCKETS to the bucket it prints, and BSTORE_SERVED to its served line (several: comma separated)." >&2
  exit 64
fi

if [ -z "${OVERLAY_TOPICS:-}" ]; then
  topics=""
  for b in $(printf '%s' "$BSTORE_BUCKETS" | tr ',' ' '); do
    topics="${topics:+$topics,}tm_bstore_$b"
  done
  OVERLAY_TOPICS=$topics
  export OVERLAY_TOPICS
fi

if [ -z "${OVERLAY_MODULES:-}" ]; then
  OVERLAY_MODULES=/app/modules/bstore/bstore-module.js
  export OVERLAY_MODULES
fi

if [ -z "${OVERLAY_ADMIN_TOKEN:-}" ]; then
  OVERLAY_ADMIN_TOKEN=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
  export OVERLAY_ADMIN_TOKEN
  echo "bstore-host: OVERLAY_ADMIN_TOKEN is unset; using a random one for this run (set it to use /admin)" >&2
fi

echo "bstore-host: topics $OVERLAY_TOPICS" >&2
exec node /app/dist/index.js "$@"
