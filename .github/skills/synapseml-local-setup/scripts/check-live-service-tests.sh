#!/usr/bin/env bash
# SYNOPSIS
#   Detect SynapseML test files that appear to call live external services.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: check-live-service-tests.sh --path <test-file-or-directory>

Searches for common live-service hooks in SynapseML tests. If matches are found,
ask the user before running the suite.
EOF
}

target=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --path)
      target="${2:-}"
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

if [[ -z "$target" ]]; then
  echo "Missing --path <test-file-or-directory>." >&2
  usage >&2
  exit 2
fi

if [[ ! -e "$target" ]]; then
  echo "Path does not exist: $target" >&2
  exit 2
fi

pattern='beforeAll\(|afterAll\(|afterEach\(|SearchIndex\.createIfNoneExists|AzureSearchWriter\.write\(|AzureSearchWriter\.stream\(|getExisting\(|deleteIndex|OpenAIEmbedding\(|CognitiveServices|Secrets\.|sys\.env\.getOrElse'

if rg -n "$pattern" "$target"; then
  echo "live_service_status=review_required"
  echo "Do not run this suite without explicit user approval if it creates or mutates external resources."
  exit 1
else
  echo "live_service_status=no_common_hooks_found"
fi
