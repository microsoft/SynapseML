---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 65
stage_attempt: 1
round: 3
timestamp: "2026-10-19T09:41:48Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b7599f7ea3f6104bcf756a853d0b309d5ee37ddf46fde7495d6f8fd78f296478"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-65-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced `monitor()` in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` (lines 98–258). Each poll reads GitHub at line 117, then reads Azure at lines 136–153 after a one-time merge check. I also traced deadline clipping (lines 113–123 and 148–153), the agent-loss deduplication (lines 155–174), the outcome branches (lines 189–250) and the exit-code mapping in `main()`.
- [x] Checked `tools/ci/tests/test_watch_azure_pipeline.py` lines 113–156, 693–735, 737–766 and 835–886. In the tests where Azure completes but GitHub still shows the check as pending or missing, or after a retry, the GitHub snapshot never changes. No test covers GitHub catching up on the next poll.
- [x] Reviewed the failure paths in `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` and found no case that passes when it should fail:
  - `run_windows_batch`: the taskkill and forced-kill fallback keeps the PID in its error.
  - `command_json`: errors use fixed operation labels and do not echo arguments or stderr.
  - `AzureClient.get`: enforces the endpoint allowlist, refuses redirects, clips timeouts to the deadline, and returns `None` only for a missing timeline (404 or empty).
  - `list`: detects repeated IDs.
  - `verify_merge`: requires the parents in `[base, head]` order.
  - `verify_build`: checks pipeline, repository, ref, `sourceVersion` and reason.
  - `summarize_timeline`: detects parent cycles and keeps only the latest attempt.
  - `collect_tests`: rejects test runs from an attempt newer than the timeline, and checks result totals and run state.
  - `main`: calls `collect_build` a second time to set `collectionStable`.
- [x] Reviewed `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - `trusted_executable` skips relative PATH entries and entries inside the current directory.
  - `git()` removes `GIT_*` variables and disables fsmonitor.
  - `verify()` compares HEAD, tree, index and working-file bytes, allowing CRLF differences only for text files.
  - It rejects symlinks, submodules, index conflicts, and untracked or ignored files.
- [x] Reviewed `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`:
  - The parameter guards after `param()` stop `-RunPipeline` from being combined with `-WaitForReview` or `-RequestReview`, so polling cannot repost `/azp run`.
  - `Get-RestEvidence` handles empty and `null` pages.
  - Workflow runs are checked against `total_count` and for valid identities. Fork-PR association is scoped (lines 326–440).
  - PR revisions are re-checked (lines 520–560).
  - The complete-evidence CI-trigger gate and receipt parsing (lines 611–648) are kept as intended.
- [x] `pipeline.yaml` lines 111–126 run `CIHelpers` on `$(UBUNTU_VERSION)` (ubuntu-22.04, line 102) with `python3 -m pytest tools/ci/tests/ -q`. Windows-only timing tests are therefore skipped in hosted CI, which is not a defect.
- [x] `.github/skills/synapseml-pr-loop/references/ci-triage.md` lines 45–58 and `ci-evidence.md` lines 193–202 document `azure_completed` and the rule that timeline issue counts block the job gate. That gating is intentional.
- [ ] Tests were not executed. This is a read-only review: I inspected the supplied validation evidence but did not rerun it.

## Issues

### Issue 1: Timeline mode can flag a normal completion because it judges a GitHub snapshot taken before the Azure read
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py
- **Line(s)**: 117, 136–153, 189–198, 232–239, 244–250
- **Description**: In `--azure-timeline` mode, each poll reads GitHub first (`query_pr`, line 117) and only then reads Azure (line 147). Line 147 creates a new `AzureClient` every poll, so `az account get-access-token` also runs before the build request, which widens the gap between the two reads. On the first poll where Azure reports `completed`, the watcher treats the earlier GitHub snapshot as final:
  - If the GitHub check is still `QUEUED` or `IN_PROGRESS`, it returns `azure_completed` with `githubCheck: "pending"` (lines 232–239).
  - If the GitHub check is not registered yet, it returns `azure_completed` with `githubCheck: "missing_or_stale"` (lines 189–198).
  - After a retry of the same build, if GitHub still shows the earlier attempt's conclusion, it returns `attention_required` (lines 244–250).

  Azure Pipelines updates the GitHub check asynchronously after the build finishes, so this happens whenever completion falls inside the read gap plus normal update delay. Nothing requires the mismatch to last beyond one poll. Two tests fix this single-poll behavior in place: `test_agent_loss_surfaces_before_job_or_build_is_terminal` and `test_timeline_results_wait_for_retry_and_distinguish_agreement` both use a GitHub snapshot that never changes.
- **Risk**: This never produces a false pass. However, the recommended timeline mode turns some ordinary successful or failed builds into exit 4 ("requires triage") instead of 0 or 1. The chance per watched build grows with the read gap plus GitHub's update delay, divided by the 600-second poll interval. The agent then has to triage again or restart the watcher, which only gets the time left before the original deadline. That undermines the "launch one watcher and read its completion" contract and the goal of reliable evidence.
- **Suggested Fix**: Do not classify on the first poll where Azure has completed but GitHub's check is pending, missing or disagrees. Record that state and check again at the next normal poll, clipped to the original deadline.
  - Return `azure_completed` or `attention_required` only if the mismatch persists, or if no further poll fits before the deadline, so the information is not lost to a bare timeout.
  - Reading Azure before GitHub in each poll also removes the gap the watcher creates itself.
  - Add regression tests in which GitHub is pending, or still shows a stale FAILURE after a retry, when Azure first completes, then shows `COMPLETED`/`SUCCESS` on the next poll. Expect exit 0.
  - Keep the cases where the mismatch persists returning exit 4.
  - Update `references/ci-triage.md` lines 55–58 to match.

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
  "changeSize": "17 source, test, and documentation paths; 5379 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 65,
  "completedPriorPasses": 64,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver disposition

Issue 1 is resolved in `watch_azure_pipeline.py`. When Azure has completed but
GitHub is pending, missing, or disagrees, the watcher permits one confirmation
at its normal 600-second cadence, only if that poll falls strictly before the
original deadline. A persistent mismatch or insufficient remaining budget
returns the existing nonzero outcome and Azure diagnostics. Observing a retry
in progress resets the confirmation allowance without resetting the deadline.
No new monitor, pipeline run, configuration option, or source file was added.
`references/ci-triage.md` describes this bounded wait.

The focused public-CLI regression covers pending, missing and stale-failure
checks catching up; a normal and a near-deadline start; insufficient remaining
time; and a retry starting during confirmation. Existing tests retain the
persistent missing, pending, disagreement and agent-loss cases. Before the
production fix, the selected tests produced 12 assertion failures without
errors or skips.

`python -B -m pytest tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed with 42 tests, 96 subtests and no skips on Windows Python 3.14.6
(3.25 seconds) and Linux Python 3.10.20 (5.94 seconds).
Black 22.3.0 passed both changed Python files, and source whitespace checks
passed.

The runner receipts and input hashes for lenses 5, 6, 1, 2 and 3 were verified.
Transport inspection confirmed read-only source inspection without prior or
sibling feedback. Root-relative globs were confined to named source locations;
they did not search review artifacts. All five original responses are preserved.
The reviewed source diff SHA-256 was
`786a45df0757208d5c8f269ebf567d55911f0646dc86752f9bea638486fae1c7`,
with manifest SHA-256
`1e50a3a97956633d3f488b0af6a230765ac0539c4a98a331c336414e920e9075`.

The fix invalidates attempt 65, including its four clean lenses. Lens 4 was not
run. This is not a qualifying pre-commit pass or current-head CI evidence.
