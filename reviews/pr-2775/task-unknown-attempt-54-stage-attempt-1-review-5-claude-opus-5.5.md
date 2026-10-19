---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 54
stage_attempt: 1
round: 5
timestamp: "2026-10-18T21:38:12Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bcc4665d5e8c09871b0d34542cfccc78e1e58bd1e3158f87e73f0d451f49548e"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-54-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared watcher timeline logic in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` (agent-loss reporting at lines 155-173, GitHub/Azure disagreement at lines 244-256) with all of `tools/ci/tests/test_watch_azure_pipeline.py`. `snapshot()` defaults to `IN_PROGRESS` (line 60). The only `attention_required` assertion (line 776) pairs a GitHub `FAILURE` with an Azure `succeeded` result.
- [x] Compared `collect_tests` in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 503-675) with `tools/ci/tests/test_collect_ci_evidence.py`. Spot-checked the supplied claim about the cleanup regression: lines 929-936 cover a timed-out wait, a denied kill, and the normal fallback.
- [x] Compared `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (rules and workflows at lines 322-440, revision recheck at lines 520-549, `-RunPipeline` at line 621) with `tools/ci/tests/test_get_pr_readiness.py`, including `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (lines 829-891) and the legacy-owner recheck (lines 1125-1134).
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`; its Git hardening flags are at lines 72-90. Compared it with `tools/ci/tests/test_trusted_guidance.py`, which covers `GIT_*` stripping (lines 66-76) and the fsmonitor hook (line 330) but not replace refs.
- [x] Checked the documented contracts the tests should pin: `references/ci-triage.md` lines 41-42 (one diagnostic per job attempt), 55-58 (disagreement returns `attention_required`) and 63 (exit zero means success); `references/ci-evidence.md` lines 10-12 (re-read the PR to detect movement).
- [x] Read `pipeline.yaml`. The CIHelpers job (lines 112-126) runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu. The InternalCompat job is advisory, with job-level `continueOnError: true` (lines 1019-1025).
- [x] Considered but did not report:
  - The readiness untyped-rule guard (lines 333-335) and workflow-identity guard (lines 383-387) have no fixtures, but they only fail closed on malformed GitHub responses.
  - The collector's `Completed` run and result gates (lines 564 and 620) are only tested with `Completed` fixtures, but a non-`Completed` current run would also fail the build and job gates in `validationPassed`.
- [ ] Tests were not executed; this was a read-only review. I cite the supplied results without reproducing them.
- [ ] Windows-only batch-launcher tests are skipped in the Ubuntu CIHelpers job. I accepted this because pipeline changes are out of scope.

## Issues

### Issue 1: No test for the false-pass direction of a GitHub/Azure disagreement
- **Severity**: Medium
- **File**: tools/ci/tests/test_watch_azure_pipeline.py (production: .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py)
- **Line(s)**: test 741-777; production 244-256
- **Description**: The only disagreement test, `test_stale_github_failure_does_not_stop_an_active_same_build_retry`, pairs a GitHub `FAILURE` with an Azure `succeeded` result. The other completed-build tests either have a pending or absent check (`azure_completed`) or agree on success. No test pairs a passing aggregate (`COMPLETED`/`SUCCESS`) with a completed Azure result of `failed`, `partiallySucceeded`, or `canceled`. Line 244 could regress to check only the tested direction, or start treating `partiallySucceeded` as success. Either way the watcher would return `success` with exit 0, and every current test would still pass.
- **Risk**: This is the false-pass direction of a documented contract (ci-triage.md lines 55-58 and 63), and it is realistic in this pipeline. InternalCompat uses job-level `continueOnError: true`, and its comment (pipeline.yaml lines 1019-1025) says a failure there does not affect the required aggregate. So the build can be non-`succeeded` while the aggregate check passes.
- **Suggested Fix**: Add subtests that use `snapshot("COMPLETED", "SUCCESS")` with `collect_build` returning a completed build whose result is `failed`, `partiallySucceeded`, or `canceled`. Assert that `watcher.main(...)` returns 4, the final outcome is `attention_required`, and the Azure result is preserved.

### Issue 2: No test for a retarget, head-branch rename, or close during readiness collection
- **Severity**: Low
- **File**: tools/ci/tests/test_get_pr_readiness.py (production: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1)
- **Line(s)**: test 829-891 (`head_move` at 853-855) and 1125-1134; production 540, 545-549, 621
- **Description**: The tests only change two fields in `freshView`: `headRefOid` and the head owner. `headRefOid` is the first operand of the `-or` chain, so the `headRefName`, `baseRefName`, and `state` comparisons never decide the result in any test. These comparisons are the only guard for those cases:
  - `$freshBranch` (line 540) re-reads the original `$baseRef`, so it cannot detect a retarget.
  - `-RunPipeline` (line 621) checks only the initial `$view.state`, so it cannot detect a close.
  
  Removing these comparisons passes every test.
- **Risk**: ci-evidence.md lines 10-12 promise movement detection, and the retained trigger gate depends on it. If the comparisons regressed, `/azp run` could be posted for a PR that was retargeted or closed mid-collection. `ExpectedBaseSha` would still match, because `$baseSha` was read from the original target.
- **Suggested Fix**: Add three subcases to `test_unsafe_pipeline_requests_fail_without_posting_a_comment`:
  - set `freshView.baseRefName` to another branch;
  - change `freshView.headRefName`;
  - set `freshView.state` to `CLOSED` while `view` stays `OPEN`.
  
  Each should expect "PR head, base, or state changed during collection."

### Issue 3: The verifier's `--no-replace-objects` hardening has no regression test
- **Severity**: Low
- **File**: tools/ci/tests/test_trusted_guidance.py (production: .github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py)
- **Line(s)**: production 72-90 (flag at 79) and 93-152; tests 66-76 and 330
- **Description**: The verifier's other Git hardening (inherited `GIT_*` variables and the fsmonitor hook) is tested, but replace refs are not. Without the flag, a `refs/replace` entry that maps the trusted commit to a tampered commit lets `verify()` return `verified: true` for unreviewed guidance:
  - `rev-parse HEAD` still returns the trusted SHA.
  - `ls-tree <sha>`, the index, and the working files all reflect the replacement tree.
  
  The environment filter also strips `GIT_NO_REPLACE_OBJECTS`, so the command-line flag is the only control. Dropping it passes every current test.
- **Risk**: A trust-boundary protection could be lost without notice, letting a tampered copy be accepted as pinned policy.
- **Suggested Fix**: Use the existing fixture helpers:
  1. Write tampered `AGENTS.md` content, `git add` it, and run `write-tree` and `commit-tree` without updating `HEAD`.
  2. Run `git replace <self.sha> <tampered>`.
  3. Assert that `verify(self.root, self.sha)` raises `GuidanceError`.

### Issue 4: Agent-loss reporting per job attempt is untested; only deduplication is
- **Severity**: Low
- **File**: tools/ci/tests/test_watch_azure_pipeline.py (production: .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py)
- **Line(s)**: test 617-652; production 155-173
- **Description**: The test repeats the same attempt-1 record and asserts one event. It never shows agent loss on a later attempt of the same job. If `reported_agent_losses` were keyed by job identifier alone, the test would still pass, even though ci-triage.md lines 41-42 promise one diagnostic per affected job attempt.
- **Risk**: Agent loss on a same-build retry would not be reported while the job runs. The operator would only see it in the final JSON, which delays triage. The impact is limited to diagnostics.
- **Suggested Fix**: Make the second poll return attempt 2 with `agentLossDetected`, then repeat that poll. Assert exactly two `agent_loss` events, for attempts 1 and 2.

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

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
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
  "changeSize": "17 source, test, and documentation paths; 5046 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 54,
  "completedPriorPasses": 53,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

All four findings accepted as targeted regression gaps. No production code,
documentation, dependency or new file was needed:

- Issue 1: extended the existing watcher retry test with a successful GitHub
  check against failed, partially succeeded and canceled Azure results. Each
  asserts exit 4, `attention_required`, and the preserved Azure result.
- Issue 2: added fresh-view retarget, head-branch rename and close cases to
  the existing unsafe-trigger test. All require the movement error and no POST.
- Issue 3: added a short replace-ref test using the existing Git fixture helpers.
  An unreviewed replacement commit cannot satisfy verification of the pinned SHA.
- Issue 4: extended the existing agent-loss test with attempt 2 and a repeated
  poll, requiring exactly one diagnostic for each of attempts 1 and 2.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_trusted_guidance.py -q --tb=short -rs`
passed on Windows (94 tests, 208 subtests, two POSIX-only skips) and Linux
(96 tests, 208 subtests, no skips). Black 22.3.0 formatted the readiness test;
the other two files were already formatted.

Isolated guard-removal checks produced only the intended assertion failures:
three disagreement subcases, three movement subcases, one replacement-ref case
and one attempt-aware diagnostic case. There were no test errors or skips.
Python guards were changed in memory and the PowerShell guard in a temporary
copy; no production source file was changed.

The executed `claude-opus-5.5` identity, complete recorded source-patch input
and independent read-only tool calls were audited.
Reviewed predecessor: `a96e0370dab63a22a73f56c24ec7441cd36d3622`;
base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
source diff SHA-256:
`627ac5e78d8f9d1df88ac6b967a3a3b6ec0bf8b8c3a526be59967e24c1881cd7`.
The source manifest is `pr-validation-skills-attempt-54-manifest.json`.
The original review is preserved above. Round 6 did not run; these test changes
invalidate this pass. It is not current-patch or Azure CI evidence.
