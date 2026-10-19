---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 48
stage_attempt: 1
round: 5
timestamp: "2026-10-18T13:31:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5a34170e979e2a03faa0a43ccb47b92deaf60c7d1b02404cdecd5b1048b95f99"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-48-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared `collect_tests` in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 452–635) and the `main()` verdict (`validationPassed` at lines 697–703) with all 18 `collect_tests` calls in `tools/ci/tests/test_collect_ci_evidence.py`. No test checks for the `lacks matching job-attempt provenance` error. No test puts an unknown-provenance run beside a valid current run.
- [x] Traced the extended `test_other_job_cannot_mask_missing_retry_publication_or_named_test` (lines 249–290) through both scopes (suite and test) and all three retry variants (`None`, unrelated, regression). Its `passed` assertions fail if either per-job publication check (errors at source lines 596 and 604) is removed. This matches the supplied mutation evidence.
- [x] Readiness `-RunPipeline` guards are covered. In `tools/ci/tests/test_get_pr_readiness.py`:
  - `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (lines 823–885) covers 12 refusals. They map to `Get-PrReadiness.ps1` lines 102–109, 555–556 and 612–621: expected SHAs missing or mismatched, head/base movement, `-WaitForReview` or `-RequestReview` combined with `-RunPipeline`, multiple PRs, closed PR, truncated threads, and rules errors.
  - `test_pipeline_trigger_preserves_receipt_without_retrying` (lines 901–929) covers five receipt responses. It asserts one POST and the no-retry warnings.
- [x] Watcher: `tools/ci/tests/test_watch_azure_pipeline.py` tests the kickoff deadline and restarts, replacement builds, legacy `EXPECTED`/`PENDING` statuses, untrusted URLs, build-ID bounds, CLI failures, and `--azure-timeline` argument rules. I checked the timeline-mode branches against `watch_azure_pipeline.py`: absent aggregate, disagreement, agent loss and deadline.
- [x] Verifier: `tools/ci/tests/test_trusted_guidance.py` covers:
  - HEAD mismatch, working-tree and staged drift;
  - untracked and ignored files, symlinks;
  - a missing safety resource, CRLF, `GIT_*` redirection and fsmonitor hooks.

  The root-checkout check (`verify_trusted_guidance.py` lines 100–101) and index-conflict check (lines 125–127) have no tests. Later checks still reject those cases, so I did not raise them; they are duplicate guards under the stated scope.
- [x] Considered but not raised:
  - No fixture uses a non-`Completed` run state with a complete result count (collector line 525). In `main()` such a publication also leaves job/build gates unresolved, so this is a duplicate guard.
  - The `pipeline.yaml` CIHelpers job (lines 112–126, 15-minute timeout) runs all of `tools/ci/tests/`. The reported timings fit, so a timeout risk is speculative.
- [ ] Tests not executed: this is a read-only review. Pass counts, timings and mutation outcomes come from the supplied validation evidence and were not re-run.

## Issues

### Issue 1: No test fails if the unknown-provenance check is removed
- **Severity**: Medium
- **File**: `tools/ci/tests/test_collect_ci_evidence.py` (check is in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`)
- **Line(s)**: Tests 708–724 and 643–662. Source 556–558, with context at 510 and 520.
- **Description**: `collect_tests` fails closed when a requested run's `pipelineReference` does not match a timeline Job attempt (lines 556–558, `Run N lacks matching job-attempt provenance.`).
  - **This is the only check that blocks such a run.** `current = False` (line 520) only keeps the run's results out of current evidence.
  - **Existing tests can't detect its removal.** `test_unknown_or_missing_job_reference_never_supplies_current_results` and `test_leaf_containers_are_gated_but_cannot_supply_test_attempts` each use a single run. `passed` is already False there through `Expected test was not published in a current attempt`, and neither test asserts the provenance error.
  - **Result: deleting lines 557–558 leaves every test green.** This is the same gap the latest change closed for the per-job expected-suite check.
  - **Concrete case the tests miss:**
    - Setup: timeline `[job()]` and runs `[run(), {**run(2), "pipelineReference": {}}]`.
    - Results: `{1: [result()], 2: [result(2, "Failed")]}`, with `--expected-test Suite::regression`.
    - With the check, `passed` is False. Without it, `passed` is True and `currentOutcomes` is `{"Passed": 1}`, so the failed named result is hidden.
    - The same happens with the `"Unknown"` phase reference and with `--expected-suite Suite`.
- **Risk**: `references/ci-evidence.md` (lines 160–174) says job-attempt metadata must match a real timeline Job. It also says failed selected results block success. `tests.passed` is reported as its own scoped verdict (`expected_suites_and_tests_only`). When it is cited as named-test evidence, build and job gates do not make up for a missing check. Because the check looks redundant next to `current = False` and the `main()` build gates, a refactor or the new no-bloat gate could remove it with no failing test. An unattributed failing publication would then pass silently.
- **Suggested Fix**: Extend the loop in `test_unknown_or_missing_job_reference_never_supplies_current_results`. For each `reference`, add a variant that places a known current `run()` beside `{**run(2), "pipelineReference": reference}` with results `{1: [result()], 2: [result(2, "Failed")]}`. Assert that:
  - `proof["passed"]` is False;
  - `proof["currentResultCount"] == 1`;
  - `"Run 2 lacks matching job-attempt provenance."` is in `proof["errors"]`.

  Optionally repeat it under `--expected-suite Suite`, as the latest masking test does. No production code, fixtures or new files are needed. A smaller alternative is to assert the provenance error in the existing single-run subcases. That catches removal of the message, but not the masking outcome.

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
  "changeSize": "17 source, test, and documentation paths; 4853 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 48,
  "completedPriorPasses": 47,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`0c88845cb68201d5bfd121dfaafcee5192e83a9071eb69aedf817a1f25fed50c`.

Issue 1 is fixed by strengthening the existing two unknown-reference subcases:
a valid passing current run now sits beside an unattributed failed named result.
The test asserts the scoped failure, the provenance error, and that only the
known current run contributes results. No production code, test case, or fixture
was added. Both subcases previously passed with the provenance guard removed
in memory; after this change both catch the false `passed=True`, with no errors.
`python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py -q --tb=short -rs`
passed on Windows/Python 3.14.6 and Linux/Python 3.10.20:
38 tests and 145 subtests on each platform. Pinned Black 22.3.0 passed.
The test change invalidates this pass; round 6 did not run.
