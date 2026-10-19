---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 51
stage_attempt: 1
round: 5
timestamp: "2026-10-18T18:12:05Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "fdd544c76a1d524f7442c090a194c21b9ef88e59ef2846faae0df114e0ec1f05"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-51-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I traced `main()` in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 720–749) against every `evidence.main(ARGV)` call in `tools/ci/tests/test_collect_ci_evidence.py` (lines 469, 521, 1118 and 1145). Each call either sets `jobGatesPassed: True` and uses the default passing fixture, or raises `EvidenceError` before `validationPassed` is computed.
- [x] I traced `summarize_timeline`, `collect_build` and `collect_tests` (lines 259–672) against the component tests (lines 200–835). Those tests check specific values for attempt provenance, per-job publication, skip exemptions, ancestor issues, failure-stage hints, pagination and the endpoint allowlist.
- [x] I searched `tools/ci/tests/test_collect_ci_evidence.py` for fixtures with a malformed Job `attempt` or `identifier` and found none. The malformed-timeline test (lines 428–456) changes only Task records.
- [x] I compared `watch_azure_pipeline.py` `monitor()` and `main()` (lines 98–370) with the timeline tests in `tools/ci/tests/test_watch_azure_pipeline.py` (lines 113–151 and 557–778). They cover a missing aggregate, agent-loss deduplication, keeping the original deadline on a same-build retry, verifying the merge only once, GitHub/Azure disagreement and the exit-code mapping.
- [x] I compared the argument guards in `Get-PrReadiness.ps1` (lines 103–111) and its evidence, trigger and readiness logic (lines 541–720) with `tools/ci/tests/test_get_pr_readiness.py` (harness at lines 21–150; cases at 308–660 and 720–935). The tests cover `-RunPipeline` refusals, the exact-case aggregate name, trigger receipts without retry, and the separate evidence, engineering, human and merge states.
- [x] I compared `verify_trusted_guidance.py` with `tools/ci/tests/test_trusted_guidance.py`. The tests cover HEAD, index, working-tree, untracked, ignored, symlink, fsmonitor and PATH-lookup cases. The documented-flag tests read command lines that exist in `trusted-guidance.md` (line 18), `ci-evidence.md` (line 133) and `ci-triage.md` (line 31).
- [x] I checked that the mocks match the real APIs:
  - The Azure run, result and timeline fixtures use string `build.id` and `testRun.id` values and the `pipelineReference` phase/job/attempt shape.
  - The fake `gh` requires `--paginate --jq tojson` and rejects `--slurp`.
  - grep confirms the source builds the Authorization header from the bearer token.
- [x] `pipeline.yaml` CIHelpers (lines 112–126) runs `python3 -m pytest tools/ci/tests/ -q`. Under `TF_BUILD`, the readiness suite fails rather than skips when `pwsh` is missing (`test_get_pr_readiness.py` lines 249–252).
- [x] Independence: I did not open or search `reviews/` or any other reviewer's artifacts. Searches were limited to `.github/skills/`, `tools/ci/` and the named `pipeline.yaml`.
- [ ] Tests were not executed because this is a read-only review. Pass/fail status relies on the validation evidence supplied with the request.

## Issues

### Issue 1: No test checks that the collector fails when job gates or named tests fail
- **Severity**: Medium
- **File**: tools/ci/tests/test_collect_ci_evidence.py (missing tests); .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py (untested combination logic)
- **Line(s)**: collect_ci_evidence.py 733–742; test_collect_ci_evidence.py 458–524, 1099–1148
- **Description**: `main()` sets `validationPassed` only when all of these are true: status `completed`, result `succeeded`, `jobGatesPassed`, `tests.passed` and `collectionStable`. It exits 0 only when `validationPassed` is true.
  - Every test that reaches this calculation sets `jobGatesPassed: True` and uses the default `client_fixture()`, where the named test passes.
  - The entry-point test changes only `result` (`succeeded` vs `partiallySucceeded`).
  - The retry test changes only `collectionStable`.
  - The malformed-input tests fail with `EvidenceError` before the calculation runs.
  - `jobGatesPassed` and `tests.passed` are checked only in the helper tests for `collect_build` and `collect_tests`.

  Deleting `and evidence["jobGatesPassed"]` or `and evidence["tests"]["passed"]` (lines 736–737) would leave every test in `tools/ci/tests/` passing.
- **Risk**: A later change could make the CLI print `validationPassed: true` and exit 0 when CI did not pass. Examples include a named regression test that failed, was skipped or was never published, and jobs with unexplained skips, timeline errors or agent loss. That is the false-green result this PR exists to prevent. It also contradicts `ci-evidence.md`: "Jobs must succeed … for `jobGatesPassed` and `validationPassed`" and "Missing, empty, incomplete, failed, or unexplained skipped selected results block success." The PR's own test-quality gate in `readiness-gates.md` ("Public end-to-end behavior is tested, not only helpers") is not met for this path.
- **Suggested Fix**: Add entry-point subcases where status, result and `collectionStable` all pass and exactly one gate fails:
  - (a) Job gates fail: a timeline job with `result: "failed"` or an unexplained `skipped`.
  - (b) The named test fails or is missing: `client_fixture(results={1: [result(outcome="Failed")]})` or `[result(name="unrelated")]`.

  Assert exit code 1, `validationPassed` False and `mergeReady` False, and check the reported `jobGatesPassed`, `tests.passed` and `tests.errors` values. Run at least one subcase through the real `collect_build` with a mocked `AzureClient` that returns the build and timeline, as the watcher's timeline test does. That covers the path from timeline to job gate to exit code end to end.

### Issue 2: No test covers a malformed Job `identifier` or `attempt` in the timeline
- **Severity**: Low
- **File**: tools/ci/tests/test_collect_ci_evidence.py (missing tests); .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py (untested check)
- **Line(s)**: collect_ci_evidence.py 297–307, 309, 551; test_collect_ci_evidence.py 428–456
- **Description**: `summarize_timeline` rejects Job, Phase and Stage records that lack an `id` or `identifier`, or whose `attempt` is not an integer of at least 1. It raises "Timeline job is missing its identifier or attempt." No test reaches this check:
  - `test_malformed_timeline_fields_report_sanitized_evidence_errors` changes only Task fields (issues, name, order, parentId, warning and error counts).
  - Every Job fixture, including `job()` and the watcher's timeline fixture, has a valid identifier and an integer attempt.
- **Risk**: The attempt number decides which test results count as current, and this check alone keeps bad values out of the attempt comparisons. If it is weakened, a null or string `attempt` reaches those comparisons (line 309 for repeated identifiers, line 551 in `collect_tests`) and raises `TypeError`. The collector's `main()` catches only `EvidenceError`, and the watcher's `main()` catches only `MonitorError` and `KeyboardInterrupt`. The user would get a Python traceback instead of the error JSON that `ci-evidence.md` promises ("Malformed nested Azure fields produce sanitized error evidence rather than an uncaught traceback"), and no test would catch it.
- **Suggested Fix**: Add Job-level subcases to the existing malformed-timeline test: `{**job(), "attempt": None}`, `{**job(), "attempt": "1"}`, `{**job(), "attempt": 0}` and `{**job(), "identifier": None}`, each asserting `EvidenceError` from `summarize_timeline`. Optionally, use the end-to-end harness from Issue 1 to assert that `main()` prints `outcome: "error"` JSON and exits 1 for one of these cases.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
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
  "changeSize": "17 source, test, and documentation paths; 4986 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 51,
  "completedPriorPasses": 50,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Maintainer disposition

Both gaps are addressed by extending existing tests; no production code or
new test file was added.

- The entry-point test now uses the real build/timeline collector and test-run
  pagination, mocking only Azure responses and merge verification. Its cases
  isolate an unexplained skipped job, a failed named test, and a missing named
  test beside passing build status and stable collection. They assert the
  individual gates, diagnostic errors, exit status, `validationPassed`, and
  `mergeReady`.
- The existing malformed-timeline test now checks null, string, zero and boolean
  Job attempts, plus a missing identifier.
- In-memory mutations removed each corresponding production guard. The old
  targeted tests passed all three mutations. The strengthened tests produced
  one failure for the job gate, two for the named-test gate, and five for the
  malformed Job guard, with no test errors and no source-file mutation.
- `python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py -q --tb=short -rs`
  passed on Windows: 41 tests, 157 subtests, no skips, 12.41 seconds.
  It passed on Linux: 39 tests, 153 subtests, 9.96 seconds; the two intentional
  Windows-only cases passed on Windows. Pinned Black 22.3.0 and diff checks passed.

The changed tests invalidate this review pass. Publication, a new clean review,
and authorized current-head Azure validation remain outstanding.
