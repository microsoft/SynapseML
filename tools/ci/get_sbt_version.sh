#!/usr/bin/env bash

set -euo pipefail

SBT_CMD="${SBT_VERSION_SBT_CMD:-sbt}"

version="$(
  "$SBT_CMD" "core/version" |
    sed 's/\x1b\[[0-9;]*m//g' |
    tail -1 |
    cut -d' ' -f2
)"

if [ -z "$version" ] ||
  [[ "$version" =~ [[:space:]] ]] ||
  [[ ! "$version" =~ ^([0-9]|HEAD-) ]]; then
  echo "Unable to resolve a valid SynapseML package version from sbt" >&2
  exit 1
fi

printf '%s\n' "$version"
