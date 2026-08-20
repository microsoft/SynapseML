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
# build.sbt also adds Maven Central's canonical repo.maven.apache.org endpoint
# after the default repo1 hostname, so Ivy can fall through a host-specific 429.
#
# Role of this script (supplement)
# --------------------------------
# This wrapper smooths the *cold-cache* path (the first build for a new sbt
# version or a changed dependency set, when the cache key legitimately misses)
# and repairs validated unresolved modules from unusable restored caches. It adds
# a bounded random start stagger so concurrent cold jobs do not hit Maven Central
# at the same instant, followed by bounded jittered exponential backoff retries.
# On exhaustion it fails visibly with a non-zero exit code; it never masks a
# failure with a success fallback.
#
# Second failure mode: partially restored caches
# ----------------------------------------------
# A restored Azure cache can be unusable on a single agent: a module directory
# under ~/.ivy2/cache exists, but Ivy still reports the dependency as unresolved.
# Plain retries preserve that on-disk state and can fail identically within
# seconds. Observed on one shard as ten consecutive 12-17s failures on
# com.globalmentor#hadoop-bare-naked-local-fs while the other 39 shards of the
# same build resolved that module from the byte-identical cache key offline.
# Retrying cannot change on-disk state, so before each retry this script evicts
# the modules named in "unresolved dependency" errors, allowing a clean re-fetch.
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
#   SBT_SETUP_IVY_HOME              absolute ivy home scanned (default ~/.ivy2;
#                                   disabled when HOME is unavailable)
#   SBT_SETUP_COURSIER_CACHE        absolute coursier cache   (default
#                                   scanned                   ~/.cache/coursier;
#                                   disabled when HOME is unavailable)
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
IVY_HOME="${SBT_SETUP_IVY_HOME:-}"
COURSIER_CACHE="${SBT_SETUP_COURSIER_CACHE:-}"
MAVEN_CENTRAL_HOSTS=("repo1.maven.org" "repo.maven.apache.org")
MAVEN_CENTRAL_PROBE_TIMEOUT_SECONDS=15
if [ -z "$IVY_HOME" ] && [ -n "${HOME:-}" ]; then
  IVY_HOME="$HOME/.ivy2"
fi
if [ -z "$COURSIER_CACHE" ] && [ -n "${HOME:-}" ]; then
  COURSIER_CACHE="$HOME/.cache/coursier"
fi
CENTRAL_PROBED=0

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

# Report what each configured Maven Central endpoint returns for a module sbt
# could not resolve.
#
# Ivy collapses every remote outcome - 404, 429, TLS failure, DNS failure - into
# the same "not found" message, so a failed resolution alone cannot distinguish
# an absent artifact from a throttled or unreachable agent. A resolution failure
# is also typically the job's only network request, leaving nothing to compare
# against. These probes record both HTTP statuses within a 30-second total bound
# so the next occurrence is diagnosable from the log instead of inferred. They
# never affect the exit status.
probe_central() {
  local org_path="$1" name="$2" rev="$3"
  local host url code
  command -v "$CURL_CMD" >/dev/null 2>&1 || return 0
  for host in "${MAVEN_CENTRAL_HOSTS[@]}"; do
    url="https://${host}/maven2/${org_path}/${name}/${rev}/${name}-${rev}.pom"
    code="$("$CURL_CMD" -sS -o /dev/null -w '%{http_code}' \
      --max-time "$MAVEN_CENTRAL_PROBE_TIMEOUT_SECONDS" "$url" 2>&1)" ||
      code="request-failed(${code})"
    echo "sbt_retry: Maven Central probe: HTTP ${code} for ${url}"
  done
}

probe_central_once() {
  local org_path="$1" name="$2" rev="$3"
  [ "$CENTRAL_PROBED" -eq 0 ] || return 0
  CENTRAL_PROBED=1
  probe_central "$org_path" "$name" "$rev"
}

cache_root_is_safe() {
  local root="$1"
  [ -n "$root" ] || return 1
  case "$root" in
    /*) ;;
    *) return 1 ;;
  esac
  # Reject filesystem roots and lexical traversal segments.
  [ -n "${root//\//}" ] || return 1
  case "/${root#/}/" in
    */../*|*/./*) return 1 ;;
  esac
}

coordinate_component_is_safe() {
  local component="$1"
  [ -n "$component" ] || return 1
  case "$component" in
    "."|".."|*..*|*[!A-Za-z0-9._+~-]*) return 1 ;;
  esac
}

unresolved_coordinates() {
  local log_file="$1" line candidate
  local pending=0
  [ -s "$log_file" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" == *"unresolved dependency:"* ]]; then
      candidate="${line#*unresolved dependency:}"
      pending=1
    elif [ "$pending" -eq 1 ]; then
      candidate="$line"
      pending=0
    else
      continue
    fi
    if [[ "$candidate" =~ ([^[:space:]#]+)#([^[:space:];]+)\;([^[:space:]:]*): ]]; then
      printf '%s %s %s\n' \
        "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}"
      pending=0
    fi
  done < "$log_file" | sort -u
}

probe_unresolved_module() {
  local coords org name rev org_path
  coords="$(unresolved_coordinates "$1")"
  [ -n "$coords" ] || return 0

  while read -r org name rev; do
    if coordinate_component_is_safe "$org" &&
      coordinate_component_is_safe "$name" &&
      coordinate_component_is_safe "$rev"; then
      org_path="$(printf '%s' "$org" | tr '.' '/')"
      probe_central_once "$org_path" "$name" "$rev"
      return 0
    fi
  done <<< "$coords"
}

path_has_symlink_component() {
  local remaining="${1#/}" current="" component
  while [ -n "$remaining" ]; do
    component="${remaining%%/*}"
    if [ "$remaining" = "$component" ]; then
      remaining=""
    else
      remaining="${remaining#*/}"
    fi
    [ -n "$component" ] || continue
    current="$current/$component"
    [ -L "$current" ] && return 0
  done
  return 1
}

evict_cache_entry() {
  local target="$1"
  local find_status
  if path_has_symlink_component "${target%/*}"; then
    echo "sbt_retry: skipping cache entry with symlinked ancestor: $target" >&2
    return 1
  fi
  if [ -e "$target" ] || [ -L "$target" ]; then
    # The entry listing distinguishes a poisoned marker from a missing artifact.
    echo "sbt_retry: evicting cache entry for unresolved module: $target"
    find "$target" -maxdepth 2 -printf '  %10s  %p\n' 2>/dev/null | head -n 20
    find_status="${PIPESTATUS[0]}"
    case "$find_status" in
      0|141) ;;
      *) ls -la "$target" 2>/dev/null | head -n 20 || true ;;
    esac
    if rm -rf -- "$target"; then
      return 0
    fi
    echo "sbt_retry: failed to evict cache entry: $target" >&2
  fi
  return 1
}

# Evict every module named in an "unresolved dependency" error from the local
# Ivy and Coursier caches so the next attempt re-fetches it from scratch.
#
# This targets the unusable-cache-state failure mode described above: removing
# the local entry lets the next attempt rebuild it instead of preserving the
# same state. Only the named coordinates are removed, so an unrelated failure
# evicts nothing and the retry behaviour is unchanged. Returns 0 when something
# was evicted.
evict_unresolved_modules() {
  local log_file="$1"
  local evicted=0
  local coords org name rev org_path target host
  local ivy_safe=0 coursier_safe=0

  coords="$(unresolved_coordinates "$log_file")"
  [ -n "$coords" ] || return 1

  if cache_root_is_safe "$IVY_HOME"; then
    ivy_safe=1
  elif [ -z "$IVY_HOME" ]; then
    echo "sbt_retry: Ivy eviction disabled: HOME is unset and no absolute override was provided"
  else
    echo "sbt_retry: skipping unsafe Ivy cache root: $IVY_HOME" >&2
  fi
  if cache_root_is_safe "$COURSIER_CACHE"; then
    coursier_safe=1
  elif [ -z "$COURSIER_CACHE" ]; then
    echo "sbt_retry: Coursier eviction disabled: HOME is unset and no absolute override was provided"
  else
    echo "sbt_retry: skipping unsafe Coursier cache root: $COURSIER_CACHE" >&2
  fi

  while read -r org name rev; do
    if ! coordinate_component_is_safe "$org" ||
      ! coordinate_component_is_safe "$name" ||
      ! coordinate_component_is_safe "$rev"; then
      echo "sbt_retry: skipping unsafe unresolved coordinate: ${org}#${name};${rev}" >&2
      continue
    fi
    org_path="$(printf '%s' "$org" | tr '.' '/')"
    if [ "$ivy_safe" -eq 1 ]; then
      for target in \
        "$IVY_HOME/cache/$org/$name" \
        "$IVY_HOME/local/$org/$name"; do
        if evict_cache_entry "$target"; then
          evicted=1
        fi
      done
    fi
    if [ "$coursier_safe" -eq 1 ]; then
      for host in "${MAVEN_CENTRAL_HOSTS[@]}"; do
        target="$COURSIER_CACHE/v1/https/$host/maven2/$org_path/$name/$rev"
        if evict_cache_entry "$target"; then
          evicted=1
        fi
      done
    fi
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

attempt_log="$(mktemp "${TMPDIR:-/tmp}/sbt_retry.XXXXXX")" || {
  echo "sbt_retry.sh: unable to create attempt log" >&2
  exit 2
}
trap 'rm -f -- "$attempt_log"' EXIT

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
  probe_unresolved_module "$attempt_log"
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
