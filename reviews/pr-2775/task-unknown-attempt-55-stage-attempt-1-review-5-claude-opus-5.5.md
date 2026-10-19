---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 55
stage_attempt: 1
round: 5
timestamp: "2026-10-18T23:28:15Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9e9c0bbd420a7e17fa07e2c0bd696e3c07b05d9fd67afedaaf7a166e356d0ff6"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-55-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff and method:** I reviewed the supplied merge-base-to-index diff for the four helpers and the four suites in `tools/ci/tests/`.
  - Helpers: `Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`.
  - For each documented gate, I traced whether some test would fail if that gate were removed.
- [x] **Latest test additions confirmed in the test files:**
  - `test_github_conclusion_cannot_override_active_retry_or_azure_result`: four GitHub/Azure disagreement pairs, each exiting 4.
  - Head-branch, base-branch and state movement subcases in `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (`test_get_pr_readiness.py` 829-899).
  - `test_agent_loss_surfaces_before_job_or_build_is_terminal`: reports attempts `[1, 2]` once each and sleeps three times for 600 seconds.
  - The three-subcase `command_json` cleanup test that keeps the process ID (`test_collect_ci_evidence.py` 929-967).
- [x] **Test-result collector:** tests cover these `collect_tests` checks (`collect_ci_evidence.py` 490-668):
  - published result count, job-attempt provenance and retry currency;
  - per-job suite and named-test publication;
  - skip exemptions and malformed values;
  - `verify_merge` parent order (152-181).
  - The test-run lifecycle-state check has no test (Issue 2).
- [x] **Readiness write guards:** I checked `Get-PrReadiness.ps1` 589-648 against the harness side-effect files and the `snapshot()` assertion (`test_get_pr_readiness.py` 77-80 and 279-283).
  - `-RequestReview` has tests for the one-PR rule, stale revisions, unrelated policy errors and read-only later polls.
  - The CI-trigger route has a closed-PR test (878-880); the review-request route does not (Issue 1).
- [x] **CI wiring:** the `pipeline.yaml` CIHelpers job (111-125) runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04 with a 15-minute timeout.
  - The new scripts and suites import only the standard library.
  - The PowerShell suite skips only when `pwsh` is missing and `TF_BUILD` is unset, so it cannot silently skip in Azure.
  - The 486.94-second full-directory time is a local run from before the latest edits, not a hosted-agent timing. Final CI has to confirm the 15-minute budget; I have not raised it as an issue.
- [x] **Deliberately not raised:**
  - Untested defensive checks against malformed GitHub/Azure responses: workflow identity mismatch, incomplete head-lookup identities, the merge-commit SHA echo and the duplicate classic `.checks` array.
  - The `githubCheck: "pending"` value.
  - The empty-results clause, which the "expected suite/test not published" errors already back up.
  - The plan excludes blanket branch coverage and duplicate guards.
- [ ] **Tests not executed:** this lens is read-only. I took the supplied Windows/Linux pytest, Black and PowerShell results as reported; I did not reproduce them.

## Issues

### Issue 1: The review-request "must be open" guard has no regression test
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1; tools/ci/tests/test_get_pr_readiness.py
- **Line(s)**: Get-PrReadiness.ps1 589-602; test_get_pr_readiness.py 613-630. The only closed-PR fixtures are for `-RunPipeline` (859-862, 878-880).
- **Description**: `-RequestReview` posts to `pulls/<n>/requested_reviewers` unless `$revisionErrors.Count -gt 0 -or $view.state -ne "OPEN"`.
  - When a PR is closed or merged and the initial and fresh views agree, there are no revision errors. The state clause is then the only thing blocking the write.
  - `references/ci-evidence.md` (84-90) documents that this route accepts exactly one PR, "which must be open".
  - The other documented clauses have tests: one PR (632-636), stale revision and unrelated policy errors (613-630), and read-only later polls (638-652).
  - Every review-request test uses the default `"state": "OPEN"` fixture.
- **Risk**: If the clause is deleted, every test still passes, and the helper would post a review request to a closed or merged PR. That is an unrequested external write that contradicts the documented contract. The CI-trigger route has exactly this test, so the gap is easy to miss in a later edit.
- **Suggested Fix**: Add a closed subcase to `test_review_request_uses_revision_gate_not_unrelated_policy_evidence`. No new test, helper or production change is needed.
  - Set `data["view"]["state"] = data["freshView"]["state"] = "CLOSED"`.
  - Keep `reviews.nodes = []` and `requestReview=True`.
  - Set `allowReviewRequest=False` and expect `"Review request blocked"`.
  - The existing `snapshot()` side-effect assertion then proves nothing was posted.

### Issue 2: No test exercises the collector's test-run lifecycle-state check
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py; tools/ci/tests/test_collect_ci_evidence.py
- **Line(s)**: collect_ci_evidence.py 564-565 and 595-599; test_collect_ci_evidence.py 77-90 (`run()` fixture) and 394-401
- **Description**: `run.get("state") != "Completed"` is the collector's only check of Azure's test-run lifecycle.
  - It fails a current, selected run that is `InProgress` or `Aborted`, even when `totalTests == len(results)` and every published result passed.
  - The `run()` fixture hardcodes `"state": "Completed"`, and no test overrides it.
  - `test_run_provenance_and_incomplete_publication_are_not_accepted` covers only a result-count mismatch.
  - The malformed-value test covers only non-string states, which raise before this check.
  - `references/ci-evidence.md` line 181 says incomplete selected results block success. The run-state form of that rule is untested.
- **Risk**: If the clause is deleted, every test still passes. `tests.passed` and `errors` would then accept a run that Azure still reports as publishing or aborted.
  - This matters when the collector runs before a build completes, which ci-evidence.md 209-211 documents, and for `--expected-suite` completeness.
  - It is Low because `validationPassed` also requires a completed, succeeded build with clear job gates.
- **Suggested Fix**: In `test_run_provenance_and_incomplete_publication_are_not_accepted`, add a `subTest` loop over `("InProgress", "Aborted")`. No production change or new helper is needed.
  - Call `evidence.collect_tests(client_fixture([{**run(), "state": state}]), self.args, [job()])`. This uses the default single passing result with `totalTests: 1`.
  - Assert that `proof["passed"]` is False and that `"Run 1 is incomplete or empty."` is in `proof["errors"]`.

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

Both findings are resolved in the next pending patch without production,
documentation or new-file changes.

- Issue 1: the existing review-request test now covers both `CLOSED` and
  `MERGED` PRs with matching initial/fresh views. Its side-effect assertion
  proves neither case posts a review request.
- Issue 2: the existing incomplete-publication test now covers `InProgress`
  and `Aborted` runs with complete passing results, checking both failure and
  the lifecycle-specific error.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_collect_ci_evidence.py -q --tb=short -rs`
passed on Windows (80 tests, 284 subtests, no skips, 143.23 seconds) and Linux
(78 tests, 280 subtests, two Windows-only skips, 194.61 seconds).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_collect_ci_evidence.py`
passed.

Isolated guard-removal checks produced exactly two assertion failures for
each guard, with no errors or skips. The production files were not modified.
These are documented-contract regressions, not blanket branch coverage.
The test changes invalidate pass 55; they require a new complete pass.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5074 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 55,
  "completedPriorPasses": 54,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
