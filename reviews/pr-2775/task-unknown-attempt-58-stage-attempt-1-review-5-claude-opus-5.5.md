---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 58
stage_attempt: 1
round: 5
timestamp: "2026-10-19T05:09:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9d38546becb0cb272e8b32007a8dd081f0cb2f4d454cd3dd32e404888fd06033"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-58-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied 17-path merge-base-to-index diff. Traced each new guard in `.github/skills/synapseml-pr-loop/scripts/` (`collect_ci_evidence.py`, `watch_azure_pipeline.py`, `verify_trusted_guidance.py`, `Get-PrReadiness.ps1`) to its tests in `tools/ci/tests/`. The checked-out sources match the diff.
- [x] `collect_ci_evidence.py`: `main()` (723–752) calls `verify_merge(args)` at line 727. The function is at 206–218 and is the only check that ties `--head-sha` and `--base-sha` to the build. Lines 461–463 publish those values as `headSha`, `baseSha` and `mergeSha`. All four `main()` tests replace `verify_merge` with a `Mock` and never check that it was called: `test_collect_ci_evidence.py` lines 481, 525, 1178 and 1212. The direct unit test (152–181) only checks parent order.
- [x] The watcher's equivalent wiring is tested. `test_azure_provenance_failure_does_not_pass` (710) and `test_merge_verification_uses_pinned_revisions_once` (720–742) patch `github_json`, which `watch_azure_pipeline.py` looks up when it runs (139–141). They assert the `git/commits/<merge>` call with `timeout=60`.
- [x] `Get-PrReadiness.ps1` `$pendingChecks` (447–450) and `$remoteGatesClear` (561–568): every readiness CheckRun fixture uses `status: COMPLETED` (lines 171, 504, 522, 549, 713, 799, 967). `PENDING` appears only on required checks (583, 882), which `$unprovenRequiredChecks` (470–474) already blocks. `pendingChecks` is asserted only at line 603.
- [x] The supplied evidence claims hold in source, and side effects are asserted:
  - `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (889–959) checks that the retained complete-collection CI-trigger gate (612–623) refuses to post `/azp run`.
  - Trigger receipts are covered at 975–1003.
  - CLOSED/MERGED review-request refusal is covered at 660–684.
  - Collector InProgress/Aborted runs are covered at 402–408, and allowed-skip outcomes at 603–605.
- [x] No defects found in:
  - watcher timeline coverage: agent-loss deduplication, absent aggregate, original deadline, GitHub/Azure disagreement and `IncompleteRead`;
  - collector coverage: pagination, attempts, shards, timeline, redirects, deadlines and batch cleanup;
  - the trusted-guidance verifier tests;
  - the human-approval matrix (614–658) and unevaluated-rule matrix (809–839).
- [x] The `pipeline.yaml` CIHelpers job (112–126) picks up these suites through `python3 -m pytest tools/ci/tests/ -q`. This PR doesn't change it.
- [ ] No tests were run (read-only review). Supplied results and guard-removal claims were checked against the source only. Whether the suites finish within the job's 15-minute limit on a hosted agent is unverified; the supplied local full-directory baseline was 486.94 s.

## Issues

### Issue 1: Collector entry point never proves merge-parent provenance is checked
- **Severity**: Medium
- **File**: tools/ci/tests/test_collect_ci_evidence.py
- **Line(s)**: 481, 525, 1178 (in 1151–1196), 1212 (in 1198–1219). Production code: `collect_ci_evidence.py` 206–218, 461–463, 727.
- **Description**: Every test that runs `evidence.main()` patches `verify_merge` with a `Mock`, and none asserts it was called. No `main()`-level case makes merge verification fail. The direct unit test proves the function works, not that the public entry point calls it. If `verify_merge(args)` were deleted from `main()`, moved after the output or had its error swallowed, the collector suite would still pass. The watcher has exactly these wiring and failure tests; the collector, which `ci-evidence.md` (128–153) names as the merge verifier, does not.
- **Risk**: Nothing else checks `head_sha` or `base_sha`; they are copied straight into the published evidence. `ci-evidence.md` (132–134) lets callers take `--merge-sha` from the run's own `sourceVersion`. On that route, the `verify_build` check `sourceVersion == merge_sha` (line 242) is true by construction. An unnoticed regression would therefore output `validationPassed: true` with `revisionScope: explicit_expected_pair` for a head/base pair the build never merged. That is a false provenance claim on this PR's core deliverable.
- **Suggested Fix**:
  - In the existing entry-point test, stop mocking `verify_merge`. Instead, patch `evidence.command_json` to return `{"sha": MERGE, "parents": [{"sha": BASE}, {"sha": HEAD}]}`, and assert a single `gh api …/git/commits/<MERGE>` request. Patching `evidence.github_json` would not work, because `verify_merge` binds its default `query=github_json` when the function is defined; `github_json` itself looks up `command_json` at call time.
  - Add one reversed-parents subcase. Assert exit code 1, `outcome == "error"`, the parent-mismatch message, and that the `AzureClient` factory was never called.
  - Smaller alternative: keep the mock, assert `assert_called_once()` with arguments carrying HEAD, BASE and MERGE, and add a `side_effect=EvidenceError` subcase.

### Issue 2: In-progress or non-required pending checks never decide the readiness pending gate
- **Severity**: Low
- **File**: tools/ci/tests/test_get_pr_readiness.py
- **Line(s)**: 580–612. Production code: `Get-PrReadiness.ps1` 447–450 and 565.
- **Description**: No readiness fixture has a CheckRun with a non-`COMPLETED` status such as QUEUED, IN_PROGRESS or WAITING, so the `($_.status -and $_.status -ne "COMPLETED")` branch of `$pendingChecks` never runs. The only pending fixtures are required checks: `license/cla` at 583, and the always-required Azure aggregate at 882. In both, `$unprovenRequiredChecks` already makes `remoteEngineeringGatesClear` false. As a result, either the status branch or the `$pendingChecks.Count -eq 0` condition could be removed without any test failing. The failing equivalents are covered (non-required legacy FAILURE at 586; CheckRun FAILURE at 961–973), but the pending equivalent is not.
- **Risk**: A regression would report `remoteEngineeringGatesClear: true` and `engineering: requires_manual_evidence` while a non-required check that isn't a GitHub Actions run is still running. That could be a third-party app's CheckRun or a legacy status. This contradicts `ci-evidence.md` line 76. Exposure is limited because `$pendingWorkflows` separately catches GitHub Actions runs.
- **Suggested Fix**: Add `("PENDING", False)` to the existing legacy matrix; its current assertions already expect `pendingChecks == ["license/cla"]`, no unproven checks, and remote clear false. Also add one non-required `{"name": "external", "status": "IN_PROGRESS", "conclusion": None}` subcase with the same assertions. No production change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver resolution

Both findings are resolved without production or documentation changes:

- Issue 1: the existing successful entry-point test now runs the real merge
  verifier, mocking only `command_json`, and asserts the exact GitHub merge
  query. A focused reversed-parent case asserts error JSON and verifies that
  no Azure client is created.
- Issue 2: the legacy matrix includes a non-required pending status. A focused
  non-required `IN_PROGRESS` CheckRun case asserts the pending set and blocked
  remote readiness while required-check evidence remains clear.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_collect_ci_evidence.py -q --tb=short -rs`
passed on Windows (83 tests, 298 subtests, no skips, 144.93 seconds) and Linux
(81 tests, 294 subtests, two Windows-only skips, 188.00 seconds).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_collect_ci_evidence.py`
passed.

Isolated guard-removal checks produced six assertion failures when the
entry-point merge verification was removed, one when CheckRun pending
extraction was removed, and two when pending checks were removed from the
readiness gate. None produced test errors or skips. The first mutation-script
attempt used a mismatching whitespace pattern for the last guard; correcting
that test harness expression completed the check without modifying production
files or rerunning settled checks.
These two test-file changes require a new complete pre-commit pass.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5157 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 58,
  "completedPriorPasses": 57,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
