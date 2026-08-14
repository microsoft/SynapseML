#!/usr/bin/env bash
# Validates the "previous primary release tag" algorithm used by
# .github/workflows/release-notes.yml against the repository's real tag list.
set -euo pipefail

prev_tag() {
  local cur="$1"
  git tag --list 'v[0-9]*.[0-9]*.[0-9]*' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -V \
    | awk -v cur="$cur" '$0 == cur {exit} {last=$0} END {print last}'
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
check v0.9.0  ""         # oldest tag has no predecessor

# Suffixed tags must never be selected as a predecessor.
if prev_tag v1.1.3 | grep -q -- '-'; then
  echo "FAIL  suffixed tag leaked into predecessor selection"
  fail=1
else
  echo "PASS  suffixed tags excluded"
fi

exit "$fail"
