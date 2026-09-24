#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# Validates the "previous primary release tag" algorithm used by
# .github/workflows/release-notes.yml against the repository's real tag list.
set -euo pipefail

if python3 -c 'import sys; raise SystemExit(sys.version_info.major != 3)' >/dev/null 2>&1; then
  PYTHON_BIN=python3
elif python -c 'import sys; raise SystemExit(sys.version_info.major != 3)' >/dev/null 2>&1; then
  PYTHON_BIN=python
else
  echo "FAIL  Python 3 is required to sort semantic release tags" >&2
  exit 1
fi

primary_tags() {
  # macOS/BSD sort has no GNU -V mode, so use the release tooling's Python 3.
  git tag --list 'v[0-9]*.[0-9]*.[0-9]*' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | "$PYTHON_BIN" -c 'import sys
tags = [line.strip() for line in sys.stdin if line.strip()]
tags.sort(key=lambda tag: tuple(map(int, tag[1:].split("."))))
sys.stdout.writelines(f"{tag}\n" for tag in tags)'
}

prev_tag() {
  local cur="$1"
  primary_tags \
    | awk -v cur="$cur" '$0 == cur {found=1} !found {last=$0} END {print last}'
}

fail=0
check() {
  local tag="$1" want="$2" got
  got="$(prev_tag "$tag")"
  if [ "$got" = "$want" ]; then
    printf 'PASS  %-12s prev=%s\n' "$tag" "${got:-<none>}"
  else
    printf 'FAIL  %-12s want=%s got=%s\n' "$tag" "${want:-<none>}" "${got:-<none>}"
    fail=1
  fi
}

# Expectations transcribed from the live tag list.
check v1.1.3  v1.1.1     # v1.1.2 was abandoned; must skip the gap
check v1.1.1  v1.1.0
check v1.1.0  v1.0.15
check v1.0.15 v1.0.14
check v1.0.14 v1.0.13
check v1.0.10 v1.0.9     # numeric, not lexical: v1.0.10 must follow v1.0.9

# The repository predates the current naming examples. Whatever the oldest
# primary semantic-version tag is, it must have no predecessor.
OLDEST=$(primary_tags | sed -n '1p')
check "$OLDEST" ""

# Suffixed tags must never be selected as a predecessor.
if prev_tag v1.1.3 | grep -q -- '-'; then
  echo "FAIL  suffixed tag leaked into predecessor selection"
  fail=1
else
  echo "PASS  suffixed tags excluded"
fi

exit "$fail"
