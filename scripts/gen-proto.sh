#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

OUT_DIR="$REPO_ROOT/messages/pb"

if [ "${PROTO_REPO_DIR:-}" ]; then
  CONTRACTS_DIR=$PROTO_REPO_DIR
elif [ -d "$REPO_ROOT/proto-repo-temp" ]; then
  CONTRACTS_DIR="$REPO_ROOT/proto-repo-temp"
elif [ -d "$REPO_ROOT/contracts" ]; then
  CONTRACTS_DIR="$REPO_ROOT/contracts"
else
  CONTRACTS_DIR="$REPO_ROOT/../proto-repo-temp"
fi

if [ ! -d "$CONTRACTS_DIR" ]; then
  echo "Proto contracts repo not found: $CONTRACTS_DIR" >&2
  echo "Set PROTO_REPO_DIR=/path/to/proto-repo-temp and retry." >&2
  exit 1
fi

if [ ! -f "$CONTRACTS_DIR/Makefile" ]; then
  echo "Proto contracts Makefile not found in: $CONTRACTS_DIR" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

echo "Generating Go proto contracts..."
echo "  contracts: $CONTRACTS_DIR"
echo "  out:       $OUT_DIR"

make -C "$CONTRACTS_DIR" golang OUT_DIR="$OUT_DIR"
