---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 47
stage_attempt: 1
round: 5
timestamp: "2026-10-18T12:05:46Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3deecf4ca046d3f93f52b06f267e28e3023761492aa67433a3fa15ab941ba807"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-47-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `collect_tests` in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 451–635).
  - For an expected suite, a run with a known job attempt adds a job/suite pair to `required_publications` (lines 512–513).
  - The all-jobs suite check (lines 574–577) passes when any job has a current row for the suite.
  - Only the per-job loop (lines 588–597) catches this case: a retried job never republished, but another shard published a run with the same name.
- [x] Searched `tools/ci/tests/` for `expected-suite|expected_suite|current job attempt|expectedPublications`.
  - Lines 123–130 only test argument rejection.
  - `collect_tests` is called with expected suites only at lines 299, 324 and 553.
  - No test asserts the per-job error text, `expectedPublications` or `expectedTestPublications`.
- [x] Static mutation check: I traced what happens if lines 588–597 are removed.
  - The tests at lines 292–315 and 317–334 still fail when an expected suite is set, but for other reasons: `NotExecuted` rows and incomplete publication.
  - The suite case at lines 552–559 has one current job, so it still passes.
  - Every test therefore keeps its result.
  - The named-test version of this guard is covered by `test_other_job_cannot_mask_missing_retry_publication_or_named_test` (lines 249–264).
- [x] `.github/skills/synapseml-pr-loop/references/ci-evidence.md` (lines 164–169) promises this per-job suite guarantee ("Another job's success cannot mask a missing retry publication") and the `expectedPublications` audit list.
- [x] Checked the latest receipt change in `Get-PrReadiness.ps1` (trigger block, lines 609–647) against `test_pipeline_trigger_preserves_receipt_without_retrying` (`tools/ci/tests/test_get_pr_readiness.py`, lines 901–929). Coverage is adequate:
  - It covers five responses: success, failure, invalid, empty and missing.
  - It asserts that `pipelineRunRequested` follows the POST exit status.
  - It asserts the receipt is exactly `{id, html_url, created_at}`.
  - It asserts no warning on success and an "inspect before retrying" warning otherwise.
  - The fake-`gh` side-effect log enforces a single POST with no retry.
- [x] Checked the other new paths against their tests and found no further user-facing path without a test:
  - unsafe `-RunPipeline` combinations;
  - wait-timeout warnings (`Get-PrReadiness.ps1` lines 716–755; test at line 931);
  - watcher `monitor()` timeline branches: agent-loss deduplication, `azure_completed`, `attention_required`, one-time merge verification, and the original deadline on a same-build retry (`test_watch_azure_pipeline.py` line 686);
  - collector provenance, pagination and job-reference checks (line 682);
  - verifier dirty-state and lookup cases.
- [x] Considered and excluded under the plan's rule that blanket branch coverage and duplicate internal guards are not acceptance criteria:
  - the closed-PR clause of the `-RequestReview` guard (`Get-PrReadiness.ps1` line 590);
  - the workflow identity mismatch check;
  - run states other than Completed.
- [x] Considered and excluded `pipeline.yaml` lines 112–125 (CIHelpers job, 15-minute timeout). The supplied 8.1–8.4-minute timings are local, not hosted-agent evidence, and pipeline changes are out of scope.
- [x] I executed no commands or tests; all pass counts are the caller's supplied evidence. I read no review artifacts, checkpoints or session state.

## Issues

### Issue 1: No test catches removal of the per-job `--expected-suite` retry-publication check
- **Severity**: Medium
- **File**: tools/ci/tests/test_collect_ci_evidence.py
- **Line(s)**: 249–264 (closest existing test, named tests only). The untested check is in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` at lines 512–513 and 588–597.
- **Description**:
  - The PR adds a documented guarantee: every job that published an expected suite must publish current results for it.
  - Lines 588–597 are the only check that fails when a retried job (attempt 2) never republishes while another shard publishes the same run name. The all-jobs check at lines 574–577 passes in that case.
  - No test sets up that scenario. The three tests that set an expected suite fail or pass for other reasons, so deleting lines 588–597 leaves every test green.
  - The sibling test at lines 249–264 uses only ARGV's `--expected-test Suite::regression`. It exercises `required_test_publications`, not `required_publications`.
  - `expectedPublications` and `expectedTestPublications` are never asserted.
- **Risk**: A refactor that drops or weakens this loop would go unnoticed. The collector would then report `passed: true` for whole-suite evidence even though a retried shard's tests never ran on its current attempt. That is false-green CI evidence from the tool meant to provide reliable validation evidence.
- **Suggested Fix**: Extend the existing test at line 249 with a suite mode instead of adding new fixtures.
  - In suite mode, set `self.args.expected_test = []` and `self.args.expected_suite = ["Suite"]`, then reuse the same three `retry_results` cases.
  - Expect `passed` to be False when `retry_results` is `None`, and True for both retry publications.
  - For the `None` case, assert that `proof["errors"]` contains `"Expected suite did not run in the current job attempt: UnitTests.scala::Suite"`.
  - Also assert `proof["expectedPublications"] == [{"job": "UnitTests.python", "suite": "Suite"}, {"job": "UnitTests.scala", "suite": "Suite"}]`.
  - Confirm the new case fails when lines 588–597 are removed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4827 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 47,
  "completedPriorPasses": 46,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`2f9fc76614f340012f3a015c599c78993cfaaa007d231033d94b76481faa08af`.

Issue 1 is fixed by extending the existing three-case retry test with suite
selection, the per-job error, and both suite/test publication audit lists.
No production code, fixture, or new file was added.
An in-memory mutation removing the per-job suite guard passed before this
extension and now fails exactly the missing-retry suite subcase, without errors.
`python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py -q --tb=short -rs`
passed on Windows/Python 3.14.6 and Linux/Python 3.10.20:
38 tests and 145 subtests on each platform. Pinned Black 22.3.0 passed.
The test change invalidates this pass; round 6 did not run.
