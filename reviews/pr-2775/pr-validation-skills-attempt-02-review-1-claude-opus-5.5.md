## Review Summary
- Round: 1
- Theme: Broad sweep
- Mode: sequential direct-task fallback
- Requested Model: complementary claude-opus-5.5 (max); primary gpt-6-astra
- Issues Found: 3
- Verdict: ISSUES_FOUND

## Evidence Checklist
- [x] Checked the review diff's SHA-256 (`70c7bd4c…2bf`) and read all 2,580 lines. `git status` shows 17 staged files, no unstaged changes, and HEAD at the base commit.
- [x] Read `Get-PrReadiness.ps1` and `scripts/watch_azure_pipeline.py` in full. Read `summarize_timeline`, `collect_build` and `main` in `scripts/collect_ci_evidence.py` (lines 182–297 and 460–482).
- [x] Checked job conditions in `pipeline.yaml`:
  - FabricE2E and InternalCompat run only when `ne(variables['System.PullRequest.IsFork'],'True')` holds.
  - The Databricks CPU/GPU and Fabric E2E jobs are skipped through `detectTestImpact` outputs.
- [x] Compared how each script is invoked in the docs:
  - The watcher (`references/ci-triage.md:24`) is run without `-B`.
  - The collector (`references/ci-evidence.md:52`) and the verifier (`references/trusted-guidance.md:18`) both use `python -B`.
  - `scripts/verify_trusted_guidance.py:84-85` rejects untracked or ignored files.
- [x] Checked how often the target branch moves with `git log`: master gets several commits a day.
- Not run: no tests or repository code were executed.

## Issues

### Issue 1: Watcher's new sibling import writes `__pycache__` into the trusted snapshot, so the next trust check fails
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`. Also involved: `references/ci-triage.md` and `scripts/verify_trusted_guidance.py`.
- **Lines:** `watch_azure_pipeline.py` 14–20; `ci-triage.md` 24; `verify_trusted_guidance.py` 84–85
- **Description:**
  - The watcher now runs `from collect_ci_evidence import (...)` when the module loads.
  - The documented watcher command is `python <watcher-script> ...` without `-B`. So Python writes `scripts/__pycache__/collect_ci_evidence.cpython-*.pyc` inside the trusted guidance snapshot.
  - `verify_trusted_guidance.py` runs `git ls-files --others` over the guidance paths without `--exclude-standard`. It then raises "Untracked or ignored files exist in the guidance paths."
- **Risk:** After the first watcher run, the trusted-guidance recheck that is required on resume fails. The docs treat that failure as a blocker, so the loop is stopped by its own tooling until someone edits or rebuilds the trusted snapshot.
- **SuggestedFix:** Use `python -B` in the `ci-triage.md` command. Also set `sys.dont_write_bytecode = True` in the watcher before the sibling import, which covers every way the script might be run.

### Issue 2: `validationPassed` can never be true when the pipeline skips a job by condition, which always happens on fork PRs
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Lines:** 295–297 and 473–482. Related: 191–198 and 259–260, which keep every latest-attempt `Job` record with its raw `state` and `result`.
- **Description:**
  - `allJobsSucceeded` requires `state == "completed" and result == "succeeded"` for every job, and `validationPassed` requires `allJobsSucceeded`.
  - Azure DevOps records a job skipped by its condition as completed with result `skipped`, and `summarize_timeline` does not filter those out.
  - In `pipeline.yaml`, FabricE2E and InternalCompat are always skipped on fork PRs. The Databricks and Fabric E2E jobs are also skipped by `detectTestImpact`.
  - So a fully green build (result `succeeded`, expected tests passed) still gives `validationPassed: false` and exit code 1.
  - This contradicts the unchanged "Reading job results correctly" guidance in `ci-triage.md`: a filter that accepts only `succeeded` reports phantom failures.
- **Risk:** `ci-delegation.md` now sends agents to this collector, but it can never certify an external-contributor fork PR, which is the main use case. Agents will either retry forever or learn to ignore the exit code, and that defeats the gate.
- **SuggestedFix:**
  - Treat jobs that are completed with result `skipped` as non-blocking, but list them (for example in a `skippedJobs` field).
  - Keep `failed`, `canceled` and `abandoned` as blocking, and optionally send `succeededWithIssues` to manual review.
  - Update `ci-evidence.md:81` to match.
  - Add a test with a skipped job record.

### Issue 3: In `--azure-timeline` mode the watcher stops with `superseded` whenever the target branch moves, and the same build cannot be watched again
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`
- **Lines:** 149–163. The exit-code mapping is at 386.
- **Description:**
  - On every poll (every 600 s, for up to 120 minutes), the watcher fetches the live target-branch tip. If it differs from `--base-sha`, it returns `superseded` (exit 2).
  - The build's merge commit is fixed when the build is queued, and `verify_merge` has already checked it, so a moving tip does not invalidate it.
  - Restarting does not help:
    - With the original `--base-sha`, the first poll returns `superseded` again.
    - With the new tip, `verify_merge` fails because the build's merge parents are still the old base and the head.
  - This also breaks the documented resume after a failed-job retry on the same build with the original kickoff.
  - It contradicts `ci-evidence.md`: "Being behind is an integration gate, not invalid evidence."
- **Risk:** master commonly gets several commits a day. For many PRs, any of them during the 2-hour window ends timeline monitoring permanently. Agents then fall back to manual polling or wrongly report the build as superseded.
- **SuggestedFix:** Check merge provenance once and stop comparing against the live tip on each poll. Report movement only as information (for example `targetMoved: true`), and leave the "behind target" decision to `Get-PrReadiness`.

## Driver provenance, decision, and resolutions

- Effective effort: **Medium** by primary estimate. The patch spans 17 tooling,
  test, and guidance files with moderate provenance, pagination, timeout, and
  attempt-state logic; it does not change production APIs or CI definitions.
- Primary: `gpt-6-astra`. Reviewer selection: complementary `claude-opus-5.5` at
  maximum reasoning from the available harness catalog, per the installed
  Medium mapping. Six independent lenses run sequentially under repository
  policy rather than the installed default concurrency.
- Transport: repository pre-PR direct-prompt/session-draft fallback. The
  installed runner rejects the repository-required pre-PR artifact location;
  the driving user directed autonomous resolution. No six-round requirement
  was waived and no installed tooling was modified. Script-owned Execute was
  not run. Requested model identity is recorded, not presented as an observed
  CLI transport receipt.
- Source base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
- Reviewed diff SHA-256: `70c7bd4cf64eb2525a3260b7295bf03b32ef3ece1d97d365d30896f7ec1fc2bf`.
- This pass was stopped after round 1; a new complete clean pass is required.
- Issue 1 fixed: the documented command uses `python -B`, and the watcher
  disables bytecode writes before importing its sibling helper. A subprocess
  test runs it without `-B` and confirms the copied guidance stays clean.
- Issue 2 fixed with an explicit-policy boundary: `--allow-skipped-job` accepts
  exact timeline identifiers for documented conditional skips. Raw skips,
  exemptions, and unresolved jobs remain visible; unexplained skips and all
  failed jobs remain blocking, and missing expected tests are never waived.
  The regression covers a fork-conditional skip, its explicit exemption, and a
  failure with the same exemption.
- Issue 3 fixed: the watcher validates the queued immutable merge parents and
  continues observing that build after target movement. Current integration is
  assessed separately by the readiness helper. The regression confirms only
  the immutable commit is queried, not a moving branch tip.
- Validation after these fixes:
  `python -m pytest tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_trusted_guidance.py -q --tb=short`
  passed: 64 tests and 76 subtests. Black 22.3.0 formatting and `git diff --check`
  also passed.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `c96921717ffb059ab0ea36c98b1bfe4b26ca56c57a122f64f2aa1b8d5a9f883b`
- Reviewed diff SHA-256: `70c7bd4cf64eb2525a3260b7295bf03b32ef3ece1d97d365d30896f7ec1fc2bf`
- Original artifact SHA-256: `14406d47f61cac81f43275e45324f837984ad86c79798310f7b121b8664df97f`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
