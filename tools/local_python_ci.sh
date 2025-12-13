#!/usr/bin/env bash

set -euo pipefail

# Local CI proxy for Python-related checks.
# Intended to be run inside the synapseml-spark4 Docker image
# (see Dockerfile.verification), which provides the synapseml
# conda environment and Java/sbt setup matching CI.

SBT_J_OPTS="-J--add-opens=java.prefs/java.util.prefs=ALL-UNNAMED"

# Ensure git operations (used by sbt-dynver) can run inside
# the container even though the repo is bind-mounted and owned
# by a different UID than the container's user.
git config --global --add safe.directory "$(pwd)" || true

# Some local development setups may have extra Scala files in the
# repo root (e.g., master_verify.scala) that CI agents do not have.
# These can cause root Scaladoc / publishM2 to fail. To better
# approximate CI, temporarily move such files out of the way while
# this script runs.
declare -a LOCAL_CI_STASHED_FILES=()

stash_local_only_files() {
  for f in master_verify.scala master_tap.scala; do
    if [[ -f "$f" ]]; then
      local backup="${f}.local-ci-backup.$(date +%s)"
      echo "Stashing local-only file '$f' -> '$backup'"
      mv "$f" "$backup"
      LOCAL_CI_STASHED_FILES+=("$f:$backup")
    fi
  done
}

restore_local_only_files() {
  for entry in "${LOCAL_CI_STASHED_FILES[@]}"; do
    local orig="${entry%%:*}"
    local backup="${entry#*:}"
    if [[ -f "$backup" ]]; then
      echo "Restoring local-only file '$backup' -> '$orig'"
      mv "$backup" "$orig"
    fi
  done
}

stash_local_only_files
trap restore_local_only_files EXIT

# Resolve a single, fixed dynver version for this run so that
# publishM2 and all subsequent sbt invocations agree on the
# synapseml_2.13 artifact version.
echo "=== Step 0: resolve dynver version ==="
FIXED_DYNVER_VERSION="$(sbt -no-colors --error 'print version' | tail -1 | tr -d '[:space:]')"
echo "Using dynver.v=${FIXED_DYNVER_VERSION} for local Python CI"
SBT_VERSION_OPTS="-Ddynver.v=${FIXED_DYNVER_VERSION}"

echo "=== Step 1: localPythonCi ==="
sbt ${SBT_J_OPTS} ${SBT_VERSION_OPTS} localPythonCi

echo "=== Local Python CI proxy completed ==="
