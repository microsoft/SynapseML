## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: parallel
- **Model**: gpt-5.6-sol
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the complete regenerated 864-line round prompt and independently inspected the current wrapper and all 29 focused wrapper tests.
- [x] Traced malformed coordinates, absent/unsafe roots, Ivy and Coursier symlink ancestors, multiple coordinates, missing curl, failed `mktemp`, retry exhaustion, `PIPESTATUS`, and EXIT-trap cleanup through `tools/ci/sbt_retry.sh`.
- [x] Reran `tools/ci/tests/test_sbt_retry.py` and `tools/ci/tests/test_pipeline_yaml.py` under WSL: **91 passed in 317.85s**.
- [x] Exercised an unresolved failure with `SBT_SETUP_MAX_ATTEMPTS=1`, safe empty cache roots, a successful fake curl command, and an sbt exit status of 7. The wrapper preserved exit 7 but emitted no Maven Central probe because it exited at exhaustion first.
- [x] Confirmed statically that the exhaustion branch at `tools/ci/sbt_retry.sh:300-302` precedes the only path to `probe_central_once`, through `evict_unresolved_modules` at lines 307 and 265.
- [x] Ran `bash -n tools/ci/sbt_retry.sh`, Black checks on both changed Python test files, and `git diff a6fd536ad76eb1b60ac82f31a362ae624886c6ff --check`; all passed.

## Issues

### Issue 1: Terminal-only unresolved failures bypass diagnostics
- **Severity**: Low
- **File**: tools/ci/sbt_retry.sh
- **Line(s)**: 265, 300-307
- **Description**: The loop exits immediately when the current attempt exhausts `MAX_ATTEMPTS`, before calling `evict_unresolved_modules`. Because `probe_central_once` is coupled to that eviction function, an unresolved dependency first seen on the terminal attempt is never probed. This occurs deterministically when the supported `SBT_SETUP_MAX_ATTEMPTS=1` setting is used and can also occur when earlier attempts fail for unrelated reasons. A direct run with a terminal unresolved error preserved the intended exit status 7 but produced neither Central endpoint diagnostic.
- **Risk**: The failure remains visible, but the PR’s attribution feature is absent exactly when a terminal-only resolution failure needs diagnosis. Operators are left unable to distinguish HTTP 404/429, TLS, or DNS failure despite the documented promise that one representative unresolved coordinate is probed per wrapper invocation.
- **Suggested Fix**: Separate coordinate probing from cache mutation. Process the failed attempt log before the exhaustion check so a terminal unresolved failure can consume the one global diagnostic budget, while performing eviction only when another retry remains. Add a regression using `MAX_ATTEMPTS=1` that asserts the original non-zero exit status and exactly one probe for each Central endpoint.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: `tools/ci/sbt_retry.sh` now extracts unresolved coordinates through one helper and performs read-only probing immediately after every failed attempt, before exhaustion is evaluated. Cache eviction remains after the exhaustion branch and therefore runs only when another wrapper attempt will follow. `tools/ci/tests/test_sbt_retry.py` adds a terminal-only exit-7 regression that asserts both endpoint probes occur, the exact exit code survives, and all Ivy/Coursier entries remain untouched.
- **Why**: Separating attribution from mutation makes the terminal path observable without adding destructive work that cannot benefit the exhausted invocation.
- **How verified**: The new terminal regression and all **92** focused tests passed under WSL; the exact failed exit status and no-eviction assertions are exercised through the wrapper. One unchanged release-replay test transiently failed in the first aggregate run, then passed alone and in the complete clean rerun. Black and `bash -n tools/ci/sbt_retry.sh` passed.

## Verification Rerun 1

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: parallel
- **Model**: gpt-5.6-sol
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Read the complete regenerated 907-line round prompt and independently inspected the current wrapper, extraction helpers, and all focused tests.
- [x] Verified extraction and validation boundaries are shared consistently: `unresolved_coordinates` at `tools/ci/sbt_retry.sh:169-174` performs parsing only, while both probing and eviction validate every coordinate component before URL or path construction.
- [x] Verified control-flow ordering at `tools/ci/sbt_retry.sh:318-326`: diagnostics run after every failed attempt, the original status is retained, exhaustion exits before mutation, and eviction runs only when another retry remains.
- [x] Ran `test_terminal_unresolved_failure_is_probed_without_eviction` independently under WSL: **1 passed**. The regression preserves exit status 7, probes both endpoints, and confirms Ivy, Coursier, and neighboring cache entries remain untouched.
- [x] Directly exercised a terminal-only unresolved failure with `MAX_ATTEMPTS=1`: both Central probe lines were emitted, no eviction line appeared, and the wrapper returned the original exit status **7**.
- [x] Reran `tools/ci/tests/test_sbt_retry.py` and `tools/ci/tests/test_pipeline_yaml.py` together under WSL: **92 passed in 397.16s**.
- [x] Rechecked prior failure modes covered by the focused suite: empty/malformed coordinates, missing or unsafe roots, absent caches, intermediate symlinks, multiple coordinates, missing curl, `mktemp` failure, visible output through `tee`, `PIPESTATUS` preservation, retry exhaustion, state-dependent recovery, and the invocation-wide probe cap.
- [x] Ran `bash -n tools/ci/sbt_retry.sh`, Black checks on both changed Python test files, and `git diff a6fd536ad76eb1b60ac82f31a362ae624886c6ff --check`; all passed.
