#!/usr/bin/env bash

set -euo pipefail

environment_file="${1:-environment.yml}"

if [ ! -f "$environment_file" ]; then
  echo "Python environment file not found: $environment_file" >&2
  exit 1
fi

versions="$(
  sed -nE 's/^[[:space:]]*-[[:space:]]*python=([^[:space:]#]+)[[:space:]]*(#.*)?$/\1/p' \
    "$environment_file"
)"
version_count="$(printf '%s\n' "$versions" | grep -c . || true)"

if [ "$version_count" -ne 1 ] ||
  ! printf '%s\n' "$versions" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "Expected exactly one pinned python=<version> dependency in $environment_file" >&2
  exit 1
fi

printf '%s\n' "$versions"
