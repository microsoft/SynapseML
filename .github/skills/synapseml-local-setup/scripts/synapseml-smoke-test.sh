#!/usr/bin/env bash
# SYNOPSIS
#   Run a safe local SynapseML Spark smoke test with JDK 11.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: synapseml-smoke-test.sh --repo <path> [--jdk <java-home>]

Runs a filtered core UDFTransformerSuite test that does not touch external services.
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
  echo "Missing --repo <path>." >&2
  usage >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$script_dir/synapseml-sbt.sh" --repo "$repo" --jdk "$jdk" -- \
  'core/testOnly com.microsoft.azure.synapse.ml.stages.UDFTransformerSuite -- -z "Apply inputCol after inputCols error"'
