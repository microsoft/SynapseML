#!/usr/bin/env bash
# SYNOPSIS
#   Run SynapseML sbt commands with JDK 11 by default.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: synapseml-sbt.sh --repo <path> [--jdk <java-home>] [--dry-run] -- <sbt-arg> [<sbt-arg> ...]

Runs sbt in a SynapseML checkout with JAVA_HOME set to JDK 11 by default.
EOF
}

repo=""
jdk="/usr/lib/jvm/java-11-openjdk-amd64"
dry_run=0

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
    --dry-run)
      dry_run=1
      shift
      ;;
    --)
      shift
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument before --: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$repo" ]]; then
  echo "Missing --repo <path>." >&2
  usage >&2
  exit 2
fi

if [[ $# -eq 0 ]]; then
  echo "Missing sbt command after --." >&2
  usage >&2
  exit 2
fi

if [[ ! -f "$repo/build.sbt" || ! -f "$repo/project/build.properties" ]]; then
  echo "Path does not look like a SynapseML sbt repo: $repo" >&2
  exit 2
fi

if [[ ! -x "$jdk/bin/java" ]]; then
  echo "JDK java executable not found: $jdk/bin/java" >&2
  exit 2
fi

export JAVA_HOME="$jdk"
export PATH="$JAVA_HOME/bin:$PATH"

echo "repo=$repo"
echo "JAVA_HOME=$JAVA_HOME"
java -version 2>&1 | sed 's/^/java: /'
echo "sbt_args=$*"

if [[ "$dry_run" -eq 1 ]]; then
  exit 0
fi

cd "$repo"
exec sbt "$@"
