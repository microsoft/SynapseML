---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 56
stage_attempt: 1
round: 5
timestamp: "2026-10-19T01:12:53Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "25cfd010edf3140005660269fb2a05b50014ed72b8b2f03ed00e3dfe11115e0d"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-56-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read-only review of the supplied diff (17 paths), mapping each new helper path to its tests. I read `Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py` under `.github/skills/synapseml-pr-loop/scripts/`, plus all four suites under `tools/ci/tests/`. I ran no commands or tests; the quoted pass counts come from the caller.
- [x] CI wiring: the existing, unchanged CIHelpers job in `pipeline.yaml` (about lines 111–126) runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04, so it picks up the new suites automatically. The readiness suite skips for missing `pwsh` only outside `TF_BUILD`, so a missing PowerShell fails loudly in Azure.
- [x] The readiness harness (`tools/ci/tests/test_get_pr_readiness.py` lines 21–145) uses a fake `gh` with these checks:
  - It rejects `--slurp`.
  - It requires `--paginate --jq tojson`, immutable compare SHAs and `state=all`.
  - It records trigger and review-request side effects. Tests assert exactly one POST for allowed actions, and none for CLOSED or MERGED PRs, with stderr checks.
  - A real native `sys.exit(1)` fixture proves the native-error fix.
- [x] Collector tests in `tools/ci/tests/test_collect_ci_evidence.py`:
  - InProgress and Aborted test runs are covered (lines 394–408).
  - Cleanup failures assert the PID and `__cause__` (about lines 936–970).
  - `main` runs end to end through the real `collect_build` and `collect_tests` with mocked Azure responses (about lines 1135–1180).
  - `collectionStable` invalidation is tested.
  - `--allow-skipped-job` has negative subtests for `failed` and `succeededWithIssues` jobs (about lines 642–663).
- [x] Watcher tests in `tools/ci/tests/test_watch_azure_pipeline.py` cover:
  - waiting on the timeline from an empty queue to a completed build
  - agent-loss deduplication by identifier and attempt
  - `azure_completed` with a missing or stale GitHub check
  - four GitHub/Azure disagreement cases leading to `attention_required`
  - a single merge verification with a 60-second timeout
  - truncated responses
  - retries that keep the original deadline
- [x] Trusted-guidance tests in `tools/ci/tests/test_trusted_guidance.py` cover:
  - a hook-mode fsmonitor case that would fail without `-c core.fsmonitor=false`
  - flags parsed from the documentation, tying the docs to the tests
  - PATH, cwd and PATHEXT lookup
  - preserved launcher names
  - replace refs, symlinks, and ignored or untracked files
- [x] I confirmed the finding by grep and reading:
  - `allow_skipped` appears in the tests only at line 125 (a CLI parse case) and line 590.
  - `ARGV` passes no `--allow-skipped`.
  - The guard is at `collect_ci_evidence.py` lines 617–626.
  - `validationPassed` includes `tests.passed` (lines 736–745).
- [x] Not reported, because the scope excludes blanket branch coverage and duplicate fail-closed guards, or the field is undocumented:
  - readiness guards for typeless branch rules, workflow identity and incomplete head identities
  - the watcher's `githubCheck: "pending"` field
  - the collector's KeyboardInterrupt cleanup branch and classic `checks[].context`
  - the verifier's root≠checkout, conflict-stage and invalid `--expected-sha` paths
- [ ] Not verified: hosted-agent runtime of the whole `tools/ci/tests/` directory against the CIHelpers 15-minute timeout. The 486.94-second figure is a local Linux baseline from before the latest edits, and no Azure run exists for this candidate.
- [ ] Not reproducible here: the Windows-only real-process batch tests skip in the Linux CIHelpers job. Only the caller's local Windows run exercises them.

## Issues

### Issue 1: `--allow-skipped` has no negative test proving it cannot waive failed or incomplete results
- **Severity**: Low
- **File**: tools/ci/tests/test_collect_ci_evidence.py (guard: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py)
- **Line(s)**: test 589–604; guard 617–626; contract .github/skills/synapseml-pr-loop/references/ci-evidence.md 181–185
- **Description**: In `collect_tests`, an `--allow-skipped` entry suppresses the "Required result is not passed" error only when the result state is `Completed` and the outcome is `NotExecuted` or `NotApplicable`. That restriction is the whole safety limit of the exemption, but no test exercises it.
  - The only behavioral test, `test_documented_skip_does_not_waive_required_named_test`, uses `NotExecuted` in both halves. It proves that a named `--expected-test` cannot be waived, and that a NotExecuted result is waived under `--expected-suite`.
  - Every other test runs with no exemptions, because `ARGV` passes no `--allow-skipped`.
  - As a result, two mutations keep the whole suite green: replacing `not (allowed and row["outcome"] in ("NotExecuted", "NotApplicable"))` with `not allowed`, or checking `allowed` before the `state != "Completed"` test.
  - The job-level test, `test_only_documented_conditional_job_skips_clear_job_gates`, already has the missing pattern: it asserts that an allowed job with a `failed` or `succeededWithIssues` result still blocks.
- **Risk**: A future refactor could let a skip exemption accept Failed, Aborted, Timeout or non-Completed results, and `tests.passed` would then report true.
  - `validationPassed` (lines 736–742) and the exit code (line 745) depend on `tests.passed`.
  - ci-evidence.md lines 181–185 promise an independent gate: "Missing, empty, incomplete, failed, or unexplained skipped selected results block success" and `--allow-skipped` is "only for an exact, documented intentional skip".
  - Without that gate, success would depend only on Azure's build and job results. That is a false-green path in the evidence this PR is meant to make reliable, and the current tests would not catch it.
- **Suggested Fix**: Extend the existing test by about 8–12 lines; no production change is needed. Set `expected_test = []`, `expected_suite = ["Suite"]` and `allow_skipped = ["Suite::regression"]`, then:
  1. For each of `Failed`, `Aborted` and `Timeout`, add a subtest asserting that `passed` is False and `errors` contains `Required result is not passed: Suite::regression (<outcome>)`.
  2. Add one subtest using `{**result(outcome="NotExecuted"), "state": "InProgress"}` and assert that `passed` is False.
  3. Optionally assert that `NotApplicable` is waived, to pin both permitted outcomes.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver resolution

Resolved by extending the existing skip-exemption test. Suite exemptions
still accept completed `NotExecuted` and `NotApplicable` results, but cannot
waive `Failed`, `Aborted`, `Timeout`, or an `InProgress` skipped result.
The original named-test refusal remains covered. Negative cases assert both
failure and the exact required-result error.

`python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py -q --tb=short -rs`
passed on Windows (41 tests, 168 subtests, no skips, 12.81 seconds) and Linux
(39 tests, 164 subtests, two Windows-only skips, 10.94 seconds).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_collect_ci_evidence.py`
passed.

Isolated mutations that broadened exemptions produced three assertion
failures for failed outcomes and one for an incomplete result, with no errors
or skips. No production file was modified. The change is confined to one
existing test, with no helper, documentation or new source file.
Pass 56 is invalidated and cannot serve as the clean pre-commit pass.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5088 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 56,
  "completedPriorPasses": 55,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
