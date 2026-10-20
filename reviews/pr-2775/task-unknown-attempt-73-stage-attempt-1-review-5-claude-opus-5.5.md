---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 73
stage_attempt: 1
round: 5
timestamp: "2026-10-20T00:45:04Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "c8350db670c007f2dd6250c525bd1cc2b9d42e7763a0b3e4d66811e9dae3b594"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\singhrana\Documents\SynapseML\.worktrees\pr-validation-skills-20261016\reviews\pr-2775\task-unknown-attempt-73-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **CIHelpers has no publisher by design.** `pipeline.yaml` lines 112–126 define `CIHelpers`: a pip install step, then `python3 -m pytest tools/ci/tests/ -q`, with no `PublishTestResults` task. This matches `ci-evidence.md` 243–268, `ci-triage.md` 105–107, `SKILL.md` 263–268 and `loop-control.md` 254–255.
- [x] **The local command matches its claims.** `ci-triage.md` 111 runs `python -B -m pytest` on all four changed suites with `-v -rs`, and all four files exist.
  - Its skip reasons cover the POSIX-only tests (`test_trusted_guidance.py` 108, 349) and the Windows-only tests (`test_collect_ci_evidence.py` 906, 934).
  - It also covers the `pwsh`/`TF_BUILD` class gate (`test_get_pr_readiness.py` 249–252) that `ci-triage.md` 114–116 describes.
- [x] **Every new anchor resolves.**
  - `#jobs-without-published-test-results` resolves to `ci-evidence.md` 243 from `SKILL.md` 266, `ci-evidence.md` 158, `ci-triage.md` 107 and `loop-control.md` 255.
  - In `ci-evidence.md`, `#comment-trigger-gate` resolves to line 94 and `#github-snapshot` to line 7.
  - In `readiness-gates.md`, `#user-value-and-scope` resolves to line 20 and `#review-and-validation` to line 78.
- [x] **Doc-parsing tests still match the intended lines.**
  - `test_trusted_guidance.py` 221–261 regex-match `ci-evidence.md` 144 and `ci-triage.md` 31. The new line 111 has no placeholder, so it cannot match.
  - Lines 128–172 match `trusted-guidance.md` 18.
- [x] **Collector gates map to assertions** in `test_collect_ci_evidence.py`:
  - selector and SHA/ID validation: 120
  - build and merge provenance: 154, 185
  - timeline validation, ancestors and cycles: 437, 680, 704
  - attempt-scoped publication, retries, unknown jobs and skip waivers: 211–436, 591, 642, 745
  - sanitized deadline errors: 1057–1288, 1364, 1421
  - Windows batch cleanup: 907–1003
- [x] **`Get-PrReadiness.ps1` states map to assertions** in `test_get_pr_readiness.py`:
  - engineering, human and merge separation: 361, 563, 626
  - exact and case-sensitive required checks, plus skipped/neutral checks: 487–560, 721, 787, 803
  - unevaluated rules and classic protection settings: 756, 822
  - revision rechecks: 406, 438, 854
  - `snapshot()` (253) checks trigger and review-request side effects for 673–720 and 902–1017.
- [x] **Streaming test traced against the code.** `test_collect_ci_evidence.py` 1087–1168 was traced against `DeadlineReader`/`DeadlineHTTPSHandler` (`collect_ci_evidence.py` 126–169).
  - It proves per-read deadline checks, raw socket closure and sender shutdown.
  - It never observes the per-receive `settimeout` (Issue 1).
  - `NoRedirect` is asserted at line 1057.
- [x] **Verifier gaps fail safe.** `verify_trusted_guidance.py` was checked against `test_trusted_guidance.py` 67–387. Its untested branches all fail closed through later checks, so I did not report them:
  - index stage ≠ 0
  - `--checkout` not at the worktree root
  - malformed `--expected-sha`
  - the NUL-byte CRLF exception
- [ ] **Not executed.** I did not run tests, Black, link checks or `git diff --check`; this lens is read-only. The supplied Windows and Linux runs and red/green baselines were not reproduced.
- [ ] **Hosted result not verified.** I could not check the predecessor's hosted CIHelpers aggregate, because aggregate-only output publishes no skip identities. The hosted image's `pwsh` version also cannot be confirmed from source.

## Issues

### Issue 1: Streaming deadline test does not check the per-receive socket timeout
- **Severity**: Low
- **File**: `tools/ci/tests/test_collect_ci_evidence.py`; `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: test 1087–1168; code 136–138; `references/ci-evidence.md` 195–197
- **Description**: `DeadlineReader.readinto` does two things before each raw receive:
  - It checks the deadline through `remaining_timeout()`.
  - It calls `socket.settimeout(remaining)`, so a stalled receive cannot outlast the deadline.

  The streaming test only exercises the deadline check. Its fake clock jumps past the deadline between bytes that arrive every 20 ms, and the test sets the socket timeout itself (`reader.settimeout(2)`), so no receive blocks long enough to observe the second step. The other HTTP tests replace `opener.open` with Mock responses and never reach `DeadlineReader`. By inspection, replacing line 137 with a bare `self.remaining_timeout()` would keep both subtests green.
- **Risk**: Nothing protects the documented guarantee "Each blocking HTTP response read uses the remaining budget". If it regressed, a stalled header or body receive could block for the open-time socket timeout (up to `AzureClient.timeout`, 60 s by default). That would run past the collector's 600 s deadline, or the watcher's original deadline, before the after-the-fact recheck reports expiry, and the suite would stay green.
- **Suggested Fix**: Add a stall subtest to the socket-pair harness.
  - Fix the injected clock so little budget remains, e.g. `now=99.8` against deadline 100.
  - Have the sender pause about 1.5 s before the first body byte.
  - Assert that `client.get` raises "Azure evidence query timed out." well before 1.5 s, and that `reader.fileno() == -1`.
  - Alternatively, record values through a class-level wrapper around `socket.socket.settimeout` and assert each equals the injected remaining budget.

### Issue 2: No readiness test covers the exempt-diff policy fields or classic `checks[]` required checks
- **Severity**: Low
- **File**: `tools/ci/tests/test_get_pr_readiness.py`; `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: tests 487–492, 721–740 (733); script 356–363 (362), 667–668; `references/readiness-gates.md` 127–133
- **Description**: `readiness-gates.md` 129–133 and `SKILL.md` 282–290 classify a missing `microsoft.SynapseML` on an exempt diff as a GitHub-enforced mismatch only when it appears in `ruleRequiredChecks[].context` or `classicProtection.required_status_checks`. No test reads either output field.
  - The classic `required_status_checks.checks[].context` extraction at line 362 has no fixture. The only classic required-check fixture (733) uses `contexts`.
  - By inspection, emitting `ruleRequiredChecks = @()` or deleting line 362 would leave every readiness test passing.
- **Risk**: The documented procedure could report a GitHub-enforced Azure check as only the helper's built-in requirement, hiding a real merge-policy mismatch. A regression at line 362 would drop classic required checks listed only under `checks` from `missingRequiredChecks` and `unprovenRequiredChecks`. A required check that never started, or that finished SKIPPED/NEUTRAL, could then leave `remoteEngineeringGatesClear` true. Current GitHub responses include both `contexts` and `checks`, which masks this today.
- **Suggested Fix**: Add one table-driven test with the Azure aggregate absent:
  - **Ruleset requires it:** assert `ruleRequiredChecks[].context` contains `microsoft.SynapseML`.
  - **Classic protection lists it only under `checks`:** assert `classicProtection.required_status_checks.checks` contains it.
  - **No rule:** assert neither field contains it.
  - **All three cases:** assert `missingRequiredChecks` contains it and `completeness.complete` is false.
  - **Extra case:** add a non-Azure required check listed only under classic `checks` that is SKIPPED or absent from the rollup. Assert it lands in `unprovenRequiredChecks` or `missingRequiredChecks`.

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
    "stageBehavior": "pinned target sequential six-lens policy",
    "bounds": "explicit user autonomous continuation after the reported blocker; complete the documentation root cause and its copyable command, retaining cumulative counters without a review waiver"
  },
  "changeSize": "17 source/test/documentation paths; 5890 added and 209 removed lines; follow-up confined to four existing docs",
  "cyclomaticComplexity": "Moderate evidence-provenance, pagination, retry, readiness, deadline and filesystem interactions; follow-up changes no executable behavior",
  "rationale": "Review the complete corrected evidence contract, with an explicit full manifest and a copyable verbose local-validation command",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/SKILL.md at d34f762a785ee8a7c7c624806f8c112256168833; explicit user continuation on 2026-10-19",
  "reviewAttempt": 73,
  "completedPriorPrecommitPasses": 71,
  "consumedFinalCiQualifiedPasses": 1,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver triage after round 5

- The original findings and verdict above are unchanged. Only round 5 ran;
  rounds 6, 1, 2, 3 and 4 did not run. This is not a completed or clean pass.
- Receipt hashes, the complete supplied patch, read-only source access and
  unchanged staged source were verified.
- Issue 1 is a confirmed regression-coverage gap: the existing streaming case
  checks expiry between receives, but would not catch removal of the remaining
  budget passed to `settimeout`. The current implementation does set that
  timeout; no current runtime defect was demonstrated.
- Issue 2 is a confirmed regression-coverage gap: the classic protection
  fixture exercises `contexts`, not a `checks`-only response, and no assertion
  checks the policy fields used by the documented manual assessment. The current
  implementation includes those fields and the checks-only extraction; no
  current runtime defect was demonstrated.
- Both remain open pending a scope and review-budget decision. The authorized
  bounded documentation follow-up has not been silently expanded into another
  test-change cycle. No source changes, new commit, push or CI trigger followed
  these findings.
- Cumulative usage is 72 pre-commit passes and one incomplete final
  CI-qualified pass; context resets do not renew these counters.

### Follow-up implementation (pending publication)

The user subsequently directed autonomous completion. One bounded test-fix
cycle was added; no review-policy waiver was inferred.

- Issue 1: extended the existing socket-pair HTTP regression with a stalled
  body. With 0.2 seconds remaining and a 2-second connection timeout, it requires
  the sanitized query-timeout result before 1.5 seconds, raw descriptor closure
  and bounded sender shutdown. Removing only the per-receive socket timeout
  causes this case to fail at 2.015 seconds; the complete and between-read-expiry
  cases still pass.
- Issue 2: added table-driven checks for the policy source of a missing Azure
  aggregate (ruleset, classic checks-only, no policy) and a non-Azure classic
  checks-only requirement (missing, successful, skipped, neutral). Isolated
  mutations removing checks-only extraction, ruleset policy output or classic
  policy output each fail the relevant assertions, with no setup errors.
- `python -B -m pytest tools/ci/tests/test_collect_ci_evidence.py::CiEvidenceTests::test_streaming_response_stops_at_the_original_deadline tools/ci/tests/test_get_pr_readiness.py::PrReadinessTests::test_missing_azure_aggregate_exposes_enforced_policy_source tools/ci/tests/test_get_pr_readiness.py::PrReadinessTests::test_classic_checks_only_require_present_successful_checks -v -rs --tb=short`
  passed on Windows Python 3.14.6: 3 tests, 10 subtests, no skips.
  On Linux Python 3.10.20, the streaming test and its 3 subtests passed;
  the two readiness tests skipped because that local environment lacks
  PowerShell. Their Windows results are not represented as Linux passes.

Both fixes are implemented locally, not yet resolved by a published commit.
Attempt 73 remains incomplete and superseded; its original feedback is retained.
