## Review Dispatch Note

The `gpt-5.6-sol` slot returned the following clean review while self-labeling its response as `claude-opus-4.6`; the locked dispatch model remains the artifact identity.

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: parallel
- **Model**: claude-opus-4.6
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Full git diff against `ms/master` inspected across all modified files (`build.sbt`, `tools/ci/sbt_retry.sh`, `tools/ci/README.md`, `tools/ci/tests/test_pipeline_yaml.py`, `tools/ci/tests/test_sbt_retry.py`).
- [x] Shell status and pipeline error propagation verified in `tools/ci/sbt_retry.sh` (correct `PIPESTATUS[0]` capture post-`tee`, `trap ... EXIT` cleanup of `attempt_log`, `mktemp` allocation guard, and propagation of terminal exit codes).
- [x] Diagnostic probe network bounds and invocation limits verified in `tools/ci/sbt_retry.sh` (15s timeout per host across `repo1.maven.org` and `repo.maven.apache.org` ensuring a 30s ceiling, single-module probe limit via `probe_unresolved_module`, single invocation across all retry attempts via `CENTRAL_PROBED` flag, and non-fatal execution).
- [x] Destructive operation safety verified in `tools/ci/sbt_retry.sh` (`rm -rf --` path boundaries guarded by `cache_root_is_safe`, strict coordinate alphanumeric token filtering via `coordinate_component_is_safe`, and traversal protection via `path_has_symlink_component`).
- [x] Terminal failure semantics verified (last attempt probes for observability but skips eviction, preserving diagnostic state when no retry follows).
- [x] Test suite executed in POSIX/WSL environment (`test_sbt_retry.py` 33/33 passing, `test_pipeline_yaml.py` resolver assertion passing).
- [x] Documentation in `tools/ci/README.md` verified for exact alignment with eviction targets, hostname probing, symlink handling, and timeout constraints.

## Exact-Head Verification Rerun

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: parallel
- **Model**: gpt-5.6-sol
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Read the entire regenerated prompt at `the regenerated round-6 prompt`.
- [x] Verified the local and GitHub PR heads are exactly `19171a7ee840250d6a896fdfaaaf2119e36f3192`, while local `ms/master` and the GitHub PR base are exactly `9f152c2ef78d9975ff4f855e1d05b85cb358563e`.
- [x] Inspected the complete five-file diff against `ms/master` and the full current contents of every changed file. The checkout had no unstaged tracked changes; the four integration updates were staged atop the exact PR head.
- [x] Confirmed the upstream SIGPIPE handling remains at `tools/ci/sbt_retry.sh:221-224`, accepting `find` statuses `0|141`, with its regression retained at `tools/ci/tests/test_sbt_retry.py:848`.
- [x] Verified invocation-wide probing, the two-host 30-second bound, symlink-ancestor rejection, terminal probing before the no-eviction exit, request-failure diagnostics, and exact curl argv coverage in `tools/ci/sbt_retry.sh:80-81,129-217,325-327` and `tools/ci/tests/test_sbt_retry.py:174,493,636,718,737,750,801`.
- [x] Verified the active resolver at `build.sbt:314` and its regression at `tools/ci/tests/test_pipeline_yaml.py:117`. `sbt "show ThisBuild / externalResolvers"` independently reported `local`, `repo1.maven.org`, then `repo.maven.apache.org` for every project.
- [x] Ran the merged focused suite under WSL/Linux: **96 passed in 341.94s**.
- [x] Validation passed: Bash syntax, `git diff --check ms/master`, Black 22.3.0 across 194 files, and `sbt scalastyle test:scalastyle`.
- [x] Post-review status confirmed no source or review artifact was edited or overwritten.

## Exact-Head Suppressed Review

### Issue 1: Wrapped unresolved coordinates were not parsed
- **Severity**: Medium
- **Source**: Copilot review of `480aedf467`
- **File**: `tools/ci/sbt_retry.sh`
- **Status**: Fixed
- **What changed**: `unresolved_coordinates` now treats the line after an `unresolved dependency:` label as part of the same logical record and extracts a coordinate from either the label line or that following line. Added a state-dependent wrapper regression using an `[error]`-prefixed continuation line.
- **Why**: Real sbt/Ivy output commonly wraps the coordinate, and missing it made both eviction and diagnostics silently no-op.
- **How verified**: The new regression proves the blocking Ivy and dual-host Coursier entries are removed, the two bounded probes run, and retry succeeds only after eviction. The missing-revision safety regression remains intact. Targeted tests passed **3/3**, the complete `tools/ci/tests/` suite passed **108/108**, Black 22.3.0 left all 194 files unchanged, `bash -n` passed, and `git diff --check` passed.

## Multiline Parser Verification Rerun

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: parallel
- **Model**: gpt-5.6-sol
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Verified inline and immediately-following coordinates parse correctly, an `[error]` prefix is skipped, empty revisions reach downstream rejection, and malformed components cannot reach deletion.
- [x] Checked `set -u`, `pipefail`, EOF handling, saved sbt status, duplicate sorting, and the script's Bash portability contract.
- [x] Verified the state-dependent regression requires eviction before recovery and checks Ivy plus both Coursier host layouts.
- [x] Targeted parser/safety tests passed **3/3** and the complete CI-helper suite passed **108/108**.
- [x] Bash syntax, Black 22.3.0, and `git diff --check ms/master` passed.

## Exact-Head Documentation Review

### Issue 1: Wrapper scope summary contradicted restored-cache recovery
- **Severity**: Low
- **Source**: Copilot review of `f68c1f23fa`
- **File**: `tools/ci/README.md`
- **Status**: Fixed
- **What changed**: The strategy summary now says the wrapper covers cold-cache starts and resolution failures caused by unusable restored entries, while retaining the existing stagger, retry, exhaustion, and exact-hit behavior.
- **Why**: “Cold-cache path only” contradicted the detailed recovery behavior immediately below and understated the advertised purpose of this PR.
- **How verified**: The revised summary matches the detailed unusable-cache section and `git diff --check` passed.

## Documentation Verification Rerun

### Issue 1: Script header retained the cold-cache-only contradiction
- **Severity**: Low
- **File**: `tools/ci/sbt_retry.sh`
- **Status**: Fixed
- **What changed**: The script-level role summary now covers both cold-cache smoothing and validated unresolved-module repair from unusable restored caches.
- **Why**: Maintainers reading the implementation header need the same accurate scope as the README.
- **How verified**: The header now matches the recovery section and implementation.

### Issue 2: Exact-hit prewarm could still stagger in its current task
- **Severity**: Low
- **File**: `templates/sbt_cache.yml`
- **Status**: Fixed
- **What changed**: The exact-hit branch now exports `SBT_SETUP_MAX_STAGGER_SECONDS=0` for the current Bash task before invoking the retry wrapper, while retaining the Azure `task.setvariable` command for later tasks. The pipeline regression requires that export to precede the wrapper call.
- **Why**: Azure logging commands update later tasks, not the environment of the shell already running. Exporting locally makes the documented exact-hit behavior true for the forced prewarm invocation without changing cache-miss behavior.
- **How verified**: The targeted cache-template regression and full CI-helper suite passed; Bash syntax, Black, and `git diff --check` passed.

## Exact-Hit Verification Rerun

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: parallel
- **Model**: gpt-5.6-sol
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Confirmed the README and script header consistently cover cold-cache smoothing and unusable restored-cache recovery.
- [x] Executed the parsed cache-template Bash step across exact-hit/prewarm, exact-hit/non-prewarm, cache-miss, and inexact-restore cases; only exact-hit prewarm passed zero stagger to the wrapper.
- [x] Confirmed the Azure `task.setvariable` command remains for later tasks and the regression locks the local export before the wrapper call.
- [x] The targeted test passed **1/1** and the complete CI-helper suite passed **108/108**.
- [x] Bash syntax, Black 22.3.0, `git diff --check ms/master`, and the review-report portability scan passed.
