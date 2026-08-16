#!/usr/bin/env bash
#
# sbt_retry.sh - resilient sbt bootstrap wrapper for CI.
#
# Problem
# -------
# SynapseML's Azure Pipelines fans out ~30 hosted-agent matrix jobs that each
# cold-bootstrap the sbt launcher (org.scala-sbt:sbt:<version>, pinned in
# project/build.properties) and the project's Ivy dependencies from public
# Maven Central. When many fresh agents (and several overlapping PR builds) do
# this at the same instant, Maven Central replies with HTTP 429 (rate limit)
# and "Setup repo" fails before any test runs.
#
# Durable fix (primary)
# ---------------------
# The Azure Cache@2 sbt-boot / Ivy caches in templates/sbt_cache.yml, warmed by
# the BuildAndCacheSbt prewarm job, mean steady-state builds restore the sbt
# launcher and resolved dependencies from Azure's cache service and never touch
# Maven Central. That eliminates the herd for the vast majority of builds.
#
# Role of this script (supplement)
# --------------------------------
# This wrapper only smooths the *cold-cache* path (the first build for a new sbt
# version or a changed dependency set, when the cache key legitimately misses).
# It adds a bounded random start stagger so concurrent cold jobs do not hit
# Maven Central at the same instant, followed by bounded jittered exponential
# backoff retries. On exhaustion it fails visibly with a non-zero exit code; it
# never masks a failure with a success fallback.
#
# Usage
# -----
#   bash tools/ci/sbt_retry.sh <sbt-args...>
#     e.g. bash tools/ci/sbt_retry.sh setup
#          bash tools/ci/sbt_retry.sh -J--add-opens=java.prefs/... setup
#
# Tunables (environment; defaults are production values, overrides enable
# deterministic tests):
#   SBT_SETUP_MAX_ATTEMPTS          total attempts            (default 5)
#   SBT_SETUP_TIMEOUT               per-attempt timeout       (default 5m;
#                                   empty string disables the timeout wrapper)
#   SBT_SETUP_MAX_STAGGER_SECONDS   max random start stagger  (default 60; 0 off)
#   SBT_SETUP_BASE_BACKOFF_SECONDS  backoff base + jitter span (default 20; 0 off)
#   SBT_SETUP_MAX_BACKOFF_SECONDS   backoff cap               (default 120)
#   SBT_SETUP_SBT_CMD               sbt executable            (default sbt)
#   SBT_SETUP_SLEEP_CMD             sleep command             (default sleep)
#   SBT_SETUP_RANDOM                fixed RNG value for tests (default $RANDOM)
#
set -uo pipefail

MAX_ATTEMPTS="${SBT_SETUP_MAX_ATTEMPTS:-5}"
TIMEOUT_DURATION="${SBT_SETUP_TIMEOUT-5m}"
MAX_STAGGER="${SBT_SETUP_MAX_STAGGER_SECONDS:-60}"
BASE_BACKOFF="${SBT_SETUP_BASE_BACKOFF_SECONDS:-20}"
MAX_BACKOFF="${SBT_SETUP_MAX_BACKOFF_SECONDS:-120}"
SBT_CMD="${SBT_SETUP_SBT_CMD:-sbt}"
SLEEP_CMD="${SBT_SETUP_SLEEP_CMD:-sleep}"

if [ "$#" -eq 0 ]; then
  echo "sbt_retry.sh: no sbt arguments provided" >&2
  exit 2
fi

# Return a non-negative integer < $1. Deterministic when SBT_SETUP_RANDOM is set.
rand_below() {
  local bound="$1"
  if [ "$bound" -le 0 ]; then
    echo 0
    return 0
  fi
  local r
  if [ -n "${SBT_SETUP_RANDOM:-}" ]; then
    r="$SBT_SETUP_RANDOM"
  else
    r="$RANDOM"
  fi
  echo $(( r % bound ))
}

run_sbt() {
  if [ -n "$TIMEOUT_DURATION" ] && command -v timeout >/dev/null 2>&1; then
    timeout "$TIMEOUT_DURATION" "$SBT_CMD" "$@"
  else
    "$SBT_CMD" "$@"
  fi
}

# Bounded random start stagger to desynchronise concurrent cold bootstraps.
if [ "$MAX_STAGGER" -gt 0 ]; then
  stagger="$(rand_below "$((MAX_STAGGER + 1))")"
  if [ "$stagger" -gt 0 ]; then
    echo "sbt_retry: staggering start by ${stagger}s to avoid Maven Central thundering herd"
    "$SLEEP_CMD" "$stagger"
  fi
fi

attempt=1
while : ; do
  echo "sbt_retry: attempt ${attempt}/${MAX_ATTEMPTS}: ${SBT_CMD} $*"
  # Capture sbt's exit status directly: an `if run_sbt; then ...; fi` compound
  # returns 0 when the condition fails and no else branch runs, which would mask
  # the real failure code.
  run_sbt "$@"
  status=$?
  if [ "$status" -eq 0 ]; then
    echo "sbt_retry: succeeded on attempt ${attempt}"
    exit 0
  fi
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "sbt_retry: exhausted ${MAX_ATTEMPTS} attempts; failing (last exit ${status})" >&2
    exit "$status"
  fi
  # Exponential backoff (base * 2^(attempt-1)) capped, plus bounded jitter.
  backoff=$(( BASE_BACKOFF * (1 << (attempt - 1)) ))
  if [ "$backoff" -gt "$MAX_BACKOFF" ]; then
    backoff="$MAX_BACKOFF"
  fi
  jitter="$(rand_below "$((BASE_BACKOFF + 1))")"
  delay=$(( backoff + jitter ))
  echo "sbt_retry: attempt ${attempt} failed (exit ${status}); retrying in ${delay}s"
  "$SLEEP_CMD" "$delay"
  attempt=$(( attempt + 1 ))
done
