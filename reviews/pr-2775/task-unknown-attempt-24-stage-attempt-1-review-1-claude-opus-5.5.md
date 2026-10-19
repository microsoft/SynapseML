---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 24
stage_attempt: 1
round: 1
timestamp: "2026-10-17T14:59:35Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "07595de6c9e8a908848c1234da2ebfae3886e0b6a319b83b60d5865f1fd589ae"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-24-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the whole supplied merge-base-to-index diff across 17 source, test and documentation paths. No product, API, dependency, release-tool or pipeline/workflow definition changes are present.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`:
  - Azure endpoint allowlist in `AzureClient.get`.
  - Redirect refusal: the `EvidenceError` raised by `NoRedirect` is not caught by the `HTTPError`/`URLError` handlers.
  - The access token stays in memory, and CLI and HTTP error bodies are sanitized in `command_json`.
  - Pagination with `$skip`/`$top=100` detects duplicate IDs.
  - `verify_merge` checks the ordered `[base, head]` parents.
  - `verify_build` checks provenance.
  - `summarize_timeline` selects the right attempt, inherits ancestor issues and detects parent cycles.
  - `collect_build` applies the job gates.
  - `main` rechecks `collectionStable` before `validationPassed`.
  - `parse_args` requires full SHAs and at least one expected suite or test.
- [x] Checked `monitor()` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - The deadline is kickoff time plus timeout, tracked on a monotonic clock.
  - A head or state change returns `superseded`.
  - The merge is verified once.
  - `agent_loss` events are de-duplicated per (identifier, attempt).
  - The `replaced`, pending, `azure_completed` and `attention_required` paths behave as written.
  - I cross-checked all of this against `references/ci-triage.md` lines 30–87.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - Tool lookup skips PATH entries under the current directory.
  - `GIT_*` environment variables are removed, and replace objects and fsmonitor are disabled.
  - It compares HEAD, tree, index, working files, and untracked and ignored files.
  - It rejects symlinks and submodules.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - `#Requires -Version 7.2` matches the docs.
  - `-RunPipeline` requires one PR and both expected SHAs, refuses `-WaitForReview`, and needs complete evidence and an OPEN PR. It only acts when `microsoft.SynapseML` is absent.
  - Unevaluated rules and classic-protection gates block the clear signal.
  - Workflow association includes the fork fallback.
  - The `humanApproval` states and the wait loop behave correctly.
- [x] Inspected the tests in `tools/ci/tests/`: `test_collect_ci_evidence.py`, `test_watch_azure_pipeline.py`, `test_get_pr_readiness.py` and `test_trusted_guidance.py`.
  - The readiness harness fakes `gh` with a PowerShell function and asserts trigger and review-request side effects.
  - I ran no tests. Your Windows, WSL and Black results are taken as reported, not reproduced.
- [x] Checked the docs:
  - Every new relative link and anchor resolves, including `references/trusted-guidance.md`, `references/ci-evidence.md`, `references/review-gauntlet.md`, `loop-control.md#evidence-invalidation` and `ci-triage.md#waiting-for-azure-pipelines`.
  - Doc claims match the code: exit semantics, `azure_completed`/`attention_required`, Stage/Phase issue inheritance, manual-reason disclosure and timeline-pending handling.
- [x] Checked the CIHelpers job in `pipeline.yaml` (around lines 111–126). It runs `tools/ci/tests`, and the top-level `jobs:` layout fits the `Phase`/`Job` identifier handling.
- [x] Checked the no-bloat gate against this PR. "Validated without maintainer changes" is restated in:
  - the external-contributor `SKILL.md`
  - `ci-delegation.md`, in step 5 and in an acceptance row
  - the pr-loop `SKILL.md`
  - a `loop-control.md` acceptance row

  The wording is consistent. I accepted this because each skill is loaded on its own and the tables are acceptance scenarios, so I did not raise it.
- [ ] Not checked live: whether Azure always returns integer `errorCount`/`warningCount` for in-progress timeline records. Only fixtures and completed-build evidence were supplied, and I cannot verify this read-only.
- [ ] Not checked: the PowerShell readiness tests on Linux pwsh in CIHelpers. Current-candidate evidence covers Windows only.

## Issues

### Issue 1: Timeline mode can return a triage exit for a healthy build because GitHub is checked before Azure and not re-checked
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py
- **Line(s)**: 116, 146, 189–198, 232–239, 244–250 (doc: .github/skills/synapseml-pr-loop/references/ci-triage.md 46–49)
- **Description**: Each poll reads GitHub first (`query_pr`, line 116), then Azure (`collect_build`, line 146).
  - The Azure read is slow: each poll creates a new `AzureClient`, so it runs `az account get-access-token` again.
  - The first poll that sees Azure `completed` compares it with the older GitHub reading and returns straight away. It gives `azure_completed` when the GitHub check is pending or not yet registered. It gives `attention_required` when GitHub still shows the earlier attempt's FAILURE after a same-build retry succeeded.
  - GitHub is never re-read, within the deadline, before returning.
  - Azure updates the GitHub check asynchronously. So when the build finishes just before a 600-second poll, inside the gap between reads or the update delay, a successful run ends with exit 4 instead of 0.
- **Risk**: Nothing passes falsely, but healthy builds sometimes end with a nonzero triage exit in the recommended monitoring mode. Agents then have to rerun readiness and triage by hand. A stale-FAILURE retry can be reported as a GitHub/Azure disagreement. Three existing tests lock in the immediate return, and no test covers the update delay: `test_agent_loss_surfaces_before_job_or_build_is_terminal`, `test_absent_aggregate_can_be_observed_but_never_passes` and `test_stale_github_failure_does_not_stop_an_active_same_build_retry`.
- **Suggested Fix**:
  - When Azure is completed but the GitHub check is pending, missing, stale or disagreeing, re-read GitHub before returning. Use a short delay (about 60 seconds or less) and stay within the original deadline.
  - Also consider reading Azure before GitHub in each poll, so the GitHub reading is never older than the Azure one.
  - Add a regression test: GitHub shows IN_PROGRESS at the first read after completion and SUCCESS at the next, and the expected result is `success`. A check that stays pending or missing must still return `azure_completed`.

### Issue 2: The CI evidence collector does not record or check which Azure pipeline produced the build
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 180–207 (`verify_build`), 403–429 (`collect_build` output); .github/skills/synapseml-pr-loop/references/ci-evidence.md 89–106
- **Description**: `verify_build` accepts any build in the project that matches four things:
  - repository `microsoft/SynapseML`
  - source branch `refs/pull/<N>/merge`
  - source version equal to the expected merge SHA
  - reason `pullRequest` or `manual`

  It never checks `build.definition`, and `collect_build` does not include it in the output. A build from a different pipeline, queued on the same PR merge ref and publishing the named test run, passes every check. `jobGatesPassed` and `validationPassed` then describe that pipeline's jobs, not the required `microsoft.SynapseML` pipeline. The docs ask for "an explicit build ID" but don't say it must come from the head's `microsoft.SynapseML` check. The watcher ties the build to that check, but the standalone collector does not.
- **Risk**: This weakens the PR's exact CI/test provenance goal. The evidence JSON cannot show which pipeline, and therefore which set of jobs, was validated. A partial or different pipeline's success could be recorded as full CI evidence. The separate required-GitHub-check gate limits the impact.
- **Suggested Fix**:
  - Add `definition.id` and `definition.name` to the evidence JSON.
  - Check the definition against an expected value, such as a constant next to `AZURE_PROJECT_ID` or a required `--definition-id` flag. Fail closed when it is missing or different.
  - Document that the build ID must come from the details URL of the current head's `microsoft.SynapseML` check.
  - Add a test fixture with a mismatched definition.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not accepted — intentional observation contract
- **What changed**: No watcher behavior or polling interval changed.
- **Why**: `ci-triage.md` explicitly defines `azure_completed` for a missing or
  lagging GitHub gate and `attention_required` for conflicting observations.
  Neither outcome calls a successful Azure build failed or permits readiness.
  It also requires fresh head/build checks after every monitor exit. A second
  read cannot establish that asynchronous publication has settled; adding
  sub-minute polling would change the documented 600-second cadence for a
  convenience rather than fix a false-green result.
- **How verified**: Checked the terminal branches and the documented handoff
  against the existing missing-check, stale-failure retry, and agent-loss
  regressions. The full collector/watcher command below passes with those
  outcomes intact. The suggested faster-convergence behavior remains outside
  this PR's scope.

### Issue 2
- **Status**: Fixed
- **What changed**: Required the repository's Azure pipeline definition ID
  (`17563`), rejected absent or malformed definition evidence, and included its
  ID and name in `pipelineDefinition`. Updated the collector instructions to
  obtain current-head build IDs from the required check's details URL.
- **Why**: A different pipeline must not supply this pipeline's CI evidence
  merely because it ran the same merge SHA. The collector is already scoped
  to one repository and Azure project, so no extra configuration was needed.
- **How verified**: Six invalid/foreign definition cases failed before the fix.
  `python -B -m pytest -q tools/ci/tests/test_collect_ci_evidence.py
  tools/ci/tests/test_watch_azure_pipeline.py --tb=short` passed with 73 tests
  and 165 subtests. The real-collector watcher fixture now includes the required
  definition metadata. Black 22.3.0 and whitespace checks passed.
  A read-only query of historical build `241234907` confirmed definition
  `17563`, name `microsoft.SynapseML`; collection accepted that metadata and
  reported 67 jobs with 27 unresolved gates. This checks API compatibility,
  not merge-parent identity or current-head CI for this PR.

The original artifact was retained before this resolution update; SHA-256:
`2d3531d6e20f554f7371dab1c529aeb1f5790f38172ce4367557d010605039ed`.
The original `issues_found` verdict remains unchanged and does not qualify
the revised patch.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4097 added and 151 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 24,
  "completedPriorPasses": 23,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
