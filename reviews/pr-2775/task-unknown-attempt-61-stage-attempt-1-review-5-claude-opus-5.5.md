---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 61
stage_attempt: 1
round: 5
timestamp: "2026-10-19T06:41:11Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1e47d9ab3ce1749ca6dc2be04745983ff9aab40d0056d07f6bbfb31bcb54ff45"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-61-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`. This covered the workflow identity guard (line 384), PR-association scoping (411–426), supersession by workflow and event (427–431), the workflow gate lists (432–440) and `remoteGatesClear` (561–568).
- [x] Read all of `tools/ci/tests/test_get_pr_readiness.py` (42 test methods), focusing on these parts:
  - The pwsh harness (21–151) and the `snapshot()` side-effect assertions (253–306). A trigger file or review-request file must exist only when it is allowed and no error is expected. Each file must contain exactly one `gh api ... -X POST` line.
  - The status matrix (463–485).
  - Same-event rerun supersession (1076–1092).
  - The identity and fork test (1095–1127). Under `-RunPipeline` it asserts the exact error, incomplete evidence and no trigger side effect.
- [x] Grepped `test_get_pr_readiness.py` for `event`. The only value is `"pull_request"` in `workflow_run()` (line 228). No fixture overrides or removes it.
- [x] Checked `scripts/collect_ci_evidence.py` against `tools/ci/tests/test_collect_ci_evidence.py` (42 methods):
  - Public `main()` runs the real `verify_merge` with mocked transport and the exact gh commit query, and rejects reversed parents before any Azure call.
  - `run_windows_batch` cleanup covers denied kill and timed-out wait.
  - Timeline tests cover malformed fields, cycles and agent loss.
  - Test-run tests cover pagination, retries, shards, named versus suite results, and skips.
- [x] Checked `scripts/watch_azure_pipeline.py` against `tools/ci/tests/test_watch_azure_pipeline.py` (41 methods):
  - Azure-mode merge verification runs once.
  - `EvidenceError` produces an error before the deadline and a timeout at it.
  - The post-collection deadline boundary is tested at 7199, 7200 and 7201.
  - Agent-loss reports are deduplicated across attempts.
  - An absent aggregate exits with code 4.
  - The agreement/disagreement matrix and the real `collect_build` path are covered.
- [x] Checked `scripts/verify_trusted_guidance.py` against `tools/ci/tests/test_trusted_guidance.py` (17 methods). The tests cover:
  - `GIT_*` isolation
  - HEAD, index and worktree mismatch
  - replace refs
  - untracked and ignored files
  - symlinks
  - shadow imports
  - PATH, cwd and PATHEXT exclusion
  - fsmonitor not being executed
- [x] Cross-checked the claimed test counts against the source. The current files define 42 + 41 = 83 readiness and watcher tests, matching the latest claimed run. The earlier claims (40 watcher tests, 83 readiness plus collector tests) match the stated states before one test was added to each suite.
- [x] Checked `pipeline.yaml` lines 112–125: the `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q`. The readiness class uses `skipUnless(pwsh or TF_BUILD)` (line 249), so Azure CI cannot silently skip it. Line 24 of `references/ci-evidence.md` documents "newest run per workflow/event on the head".
- [ ] Tests not executed: this is a read-only review. The validation results are the caller's claims, cross-checked against the source only.

## Issues

### Issue 1: Non-`pull_request` workflow runs and per-event supersession have no readiness test
- **Severity**: Low
- **File**: `tools/ci/tests/test_get_pr_readiness.py` (missing test). The untested code is in `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`.
- **Line(s)**:
  - `Get-PrReadiness.ps1` 411–431: non-PR runs are admitted at 425, and `Group-Object workflow_id,event` is at 429. The gate is consumed at 566–567.
  - Test helper `workflow_run()` 223–233.
  - Tests 463–485 and 1076–1092.
- **Description**: The script admits every workflow run on the head SHA whose event is not `pull_request*` (line 425). It then keeps the newest run per workflow *and event* (line 429), as `references/ci-evidence.md` line 24 documents. Every test fixture comes from `workflow_run()`, whose event is always `"pull_request"`, so:
  - line 425 never runs under test;
  - the `event` part of the grouping key is never distinguished;
  - the rerun test (1076–1092) proves only same-event supersession.
- **Risk**: Either of two one-line regressions passes the whole suite and fails open:
  - Grouping by `workflow_id` alone would let a newer successful `pull_request` run hide an older failed, pending or approval-waiting `push` run of the same workflow on the same head.
  - Changing line 425 to `return $false` would drop all non-PR runs.

  Either change removes the run from `failedWorkflows`, `pendingWorkflows` or `workflowApprovalsRequired`. `remoteEngineeringGatesClear` and `completeness.complete` could then read true while a workflow on the head is failing. Same-repository branches can have both `push` and `pull_request` runs on one SHA. The current code appears correct; the gap is missing regression protection for a documented engineering gate.
- **Suggested Fix**: Add one public-path test using the existing `snapshot()` harness. Give it these workflow runs, which share the same `workflow_id`:
  - `[workflow_run(id=41, event="push", pull_requests=[], conclusion="failure"), workflow_run(id=42)]`

  Assert:
  - the `failedWorkflows` ids equal `[41]`;
  - `evidenceComplete` is true;
  - `collectionErrors` is empty;
  - `remoteEngineeringGatesClear` is false.

  This one test catches both regressions. Optionally, add an `event="push"` subtest to the status matrix at lines 463–485. No production change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5294 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 61,
  "completedPriorPasses": 60,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver Disposition

The low-severity finding is resolved by extending the existing rerun test;
the original feedback above is unchanged. After proving that a newer run
replaces a failure in the same workflow/event, the test now proves that it
cannot hide a failed `push` run or a failed run of a different workflow.
Both cases assert the retained failure ID, complete collection without errors,
and blocked engineering gates. No production changes, new test methods, or
new source files were needed.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py -q --tb=short -rs`
passed on Windows Python 3.14.6 / PowerShell 7 (42 tests, 135 subtests, no skips,
142.46 s) and Linux Python 3.10.20 / PowerShell 7.6.7 (42 tests, 135 subtests,
no skips, 197.78 s).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py`
passed. Three isolated mutations each produced one targeted assertion failure:
dropping the event grouping key, dropping the workflow grouping key, and
excluding non-PR events. There were no errors, skips, or production-file edits.

This pass stopped after lens 5; the other five lenses did not run. The changed
test invalidates attempt 61 as a qualifying pass. Its reviewed source diff was
`3f10fee88939d7334aff31f0384ac24cd76e3a1a61ba62b175ea3a4015dd2711` against
base `d34f762a785ee8a7c7c624806f8c112256168833` and local predecessor
`38685fad3d2e3ec93e6757adf36aa270cc3a24d1`.
The transport audit confirmed the requested model, exact embedded diff,
independent read-only source access, and publication-safe output.
