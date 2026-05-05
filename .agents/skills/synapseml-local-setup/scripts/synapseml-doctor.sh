#!/usr/bin/env bash
# SYNOPSIS
#   Diagnose whether a SynapseML repo is ready for local SBT validation.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: synapseml-doctor.sh --repo <path> [--jdk <java-home>]

Checks repo shape, git state, default Java, JDK 11 availability, sbt, and
SynapseML Scala/Spark versions. Does not compile or run tests.
EOF
}

repo=""
jdk="/usr/lib/jvm/java-11-openjdk-amd64"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --jdk)
      jdk="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$repo" ]]; then
  echo "ERROR repo path is required. Pass --repo <synapseml-repo>." >&2
  exit 2
fi

if [[ ! -d "$repo" ]]; then
  echo "ERROR repo path does not exist: $repo" >&2
  exit 2
fi

if [[ ! -f "$repo/build.sbt" || ! -f "$repo/project/build.properties" ]]; then
  echo "ERROR path does not look like a SynapseML sbt repo: $repo" >&2
  exit 2
fi

echo "repo=$repo"
echo "repo_status=ok"

if git -C "$repo" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "git_head=$(git -C "$repo" rev-parse --short HEAD)"
  echo "git_branch=$(git -C "$repo" branch --show-current || true)"
  dirty_count="$(git -C "$repo" status --short | wc -l | tr -d ' ')"
  echo "git_dirty_count=$dirty_count"
else
  echo "git_status=not-a-git-worktree"
fi

echo "default_java=$(command -v java || true)"
if command -v java >/dev/null 2>&1; then
  java -version 2>&1 | sed 's/^/default_java_version: /'
fi

if [[ -x "$jdk/bin/java" ]]; then
  echo "jdk11=$jdk"
  "$jdk/bin/java" -version 2>&1 | sed 's/^/jdk11_version: /'
else
  echo "ERROR jdk11_missing=$jdk/bin/java" >&2
  echo "Install JDK 11 or pass --jdk <java-home>." >&2
  exit 3
fi

if command -v sbt >/dev/null 2>&1; then
  echo "sbt=$(command -v sbt)"
  sbt --script-version 2>/dev/null | sed 's/^/sbt_runner_version: /' || true
else
  echo "ERROR sbt_missing=true" >&2
  exit 3
fi

sed -n '1,20p' "$repo/project/build.properties" | sed 's/^/build_properties: /'
rg -n 'scalaVersion|sparkVersion' "$repo/build.sbt" | sed 's/^/build_sbt: /' || true

echo "doctor_status=ok"
