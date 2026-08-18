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
# Second failure mode: partially restored caches
# ----------------------------------------------
# A restored Azure cache occasionally lands incomplete on a single agent: a
# module directory under ~/.ivy2/cache exists but its metadata/artifacts do not.
# Ivy treats that as an authoritative "not found" and fails *without* attempting
# any download, so plain retries of the same command all fail identically within
# seconds. Observed on one shard as ten consecutive 12-17s failures on
# com.globalmentor#hadoop-bare-naked-local-fs while the other 39 shards of the
# same build resolved that module from the byte-identical cache key offline.
# Retrying cannot change on-disk state, so before each retry this script evicts
# the modules named in "unresolved dependency" errors, forcing a clean re-fetch.
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
#   SBT_SETUP_CURL_CMD              curl used by the Maven    (default curl)
#                                   Central diagnostic probe
#   SBT_SETUP_IVY_HOME              ivy home scanned for      (default ~/.ivy2)
#                                   incomplete modules
#   SBT_SETUP_COURSIER_CACHE        coursier cache scanned    (default
#                                   for incomplete modules    ~/.cache/coursier)
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
CURL_CMD="${SBT_SETUP_CURL_CMD:-curl}"
IVY_HOME="${SBT_SETUP_IVY_HOME:-${HOME:-}/.ivy2}"
COURSIER_CACHE="${SBT_SETUP_COURSIER_CACHE:-${HOME:-}/.cache/coursier}"

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

# Report what Maven Central actually returns for a module sbt could not resolve.
#
# Ivy collapses every remote outcome - 404, 429, TLS failure, DNS failure - into
# the same "not found" message, so a failed resolution alone cannot distinguish
# an absent artifact from a throttled or unreachable agent. A resolution failure
# is also typically the job's only network request, leaving nothing to compare
# against. This probe records the HTTP status so the next occurrence is
# diagnosable from the log instead of inferred. It never affects the exit status.
probe_central() {
  local org_path="$1" name="$2" rev="$3"
  local url="https://repo1.maven.org/maven2/${org_path}/${name}/${rev}/${name}-${rev}.pom"
  command -v "$CURL_CMD" >/dev/null 2>&1 || return 0
  local code
  code="$("$CURL_CMD" -sS -o /dev/null -w '%{http_code}' --max-time 30 "$url" 2>&1)" ||
    code="request-failed(${code})"
  echo "sbt_retry: Maven Central probe: HTTP ${code} for ${url}"
}

# Evict every module named in an "unresolved dependency" error from the local
# Ivy and Coursier caches so the next attempt re-fetches it from scratch.
#
# This targets the partially-restored-cache failure mode described above: when a
# module directory exists but is incomplete, Ivy short-circuits to "not found"
# with no network request, making every identical retry fail identically. Only
# the named coordinates are removed, so an unrelated failure evicts nothing and
# the retry behaviour is unchanged. Returns 0 when something was evicted.
evict_unresolved_modules() {
  local log_file="$1"
  local evicted=0
  local coords org name rev org_path target

  [ -s "$log_file" ] || return 1
  coords="$(
    sed -n 's/.*unresolved dependency: \([^ #]*\)#\([^ ;]*\);\([^ :]*\).*/\1 \2 \3/p' \
      "$log_file" | sort -u
  )"
  [ -n "$coords" ] || return 1

  while read -r org name rev; do
    [ -n "$org" ] && [ -n "$name" ] || continue
    # Defensive: coordinates are path components, never traversals.
    case "$org$name$rev" in
      */*|*\\*|*..*) continue ;;
    esac
    org_path="$(printf '%s' "$org" | tr '.' '/')"
    for target in \
      "$IVY_HOME/cache/$org/$name" \
      "$IVY_HOME/local/$org/$name" \
      "$COURSIER_CACHE/v1/https/repo1.maven.org/maven2/$org_path/$name/$rev"; do
      if [ -e "$target" ]; then
        # What the damaged entry actually contained decides whether the next
        # occurrence is a poisoned marker or a genuinely absent artifact.
        echo "sbt_retry: evicting incomplete cache entry: $target"
        find "$target" -maxdepth 2 -printf '  %10s  %p\n' 2>/dev/null | head -n 20 ||
          ls -la "$target" 2>/dev/null | head -n 20
        rm -rf "$target"
        evicted=1
      fi
    done
    probe_central "$org_path" "$name" "$rev"
  done <<< "$coords"

  [ "$evicted" -eq 1 ]
}

# Bounded random start stagger to desynchronise concurrent cold bootstraps.
if [ "$MAX_STAGGER" -gt 0 ]; then
  stagger="$(rand_below "$((MAX_STAGGER + 1))")"
  if [ "$stagger" -gt 0 ]; then
    echo "sbt_retry: staggering start by ${stagger}s to avoid Maven Central thundering herd"
    "$SLEEP_CMD" "$stagger"
  fi
fi

attempt_log="$(mktemp)"
trap 'rm -f "$attempt_log"' EXIT

attempt=1
while : ; do
  echo "sbt_retry: attempt ${attempt}/${MAX_ATTEMPTS}: ${SBT_CMD} $*"
  # Capture sbt's exit status directly: an `if run_sbt; then ...; fi` compound
  # returns 0 when the condition fails and no else branch runs, which would mask
  # the real failure code. tee keeps the output streaming to the CI log while
  # retaining a copy to scan for unresolved-dependency coordinates, so
  # PIPESTATUS[0] - not $? - carries sbt's status.
  run_sbt "$@" 2>&1 | tee "$attempt_log"
  status="${PIPESTATUS[0]}"
  if [ "$status" -eq 0 ]; then
    echo "sbt_retry: succeeded on attempt ${attempt}"
    exit 0
  fi
  if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
    echo "sbt_retry: exhausted ${MAX_ATTEMPTS} attempts; failing (last exit ${status})" >&2
    exit "$status"
  fi
  # A resolution failure is usually an incomplete cache entry rather than a
  # transient network fault; evicting it is what makes the next attempt able to
  # succeed at all.
  if evict_unresolved_modules "$attempt_log"; then
    echo "sbt_retry: evicted unresolved modules; attempt $((attempt + 1)) will re-fetch them"
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
