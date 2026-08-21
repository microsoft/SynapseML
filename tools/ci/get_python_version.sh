#!/usr/bin/env bash

set -euo pipefail

environment_file="${1:-environment.yml}"

if [ ! -f "$environment_file" ]; then
  echo "Python environment file not found: $environment_file" >&2
  exit 1
fi

mapfile -t versions < <(
  sed -nE 's/^[[:space:]]*-[[:space:]]*python=([^[:space:]#]+)[[:space:]]*(#.*)?$/\1/p' \
    "$environment_file"
)

if [ "${#versions[@]}" -ne 1 ] ||
  [[ ! "${versions[0]:-}" =~ ^[0-9]+(\.[0-9]+){1,2}$ ]]; then
  echo "Expected exactly one pinned python=<version> dependency in $environment_file" >&2
  exit 1
fi

printf '%s\n' "${versions[0]}"
