---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 62
stage_attempt: 1
round: 5
timestamp: "2026-10-19T07:10:54Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "8c8b06cea9d3de80d21e64623e4cb7ac62155ebe9bca4a493691421e76080dd5"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-62-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **CI collection.** `pipeline.yaml:111-126` (job `CIHelpers`, ubuntu-22.04, 15-minute timeout) runs `python3 -m pytest tools/ci/tests/ -q`, so all four changed suites are collected. `tools/ci/tests/test_get_pr_readiness.py:249-252` skips only when `pwsh` is missing outside Azure (`TF_BUILD` unset). On a hosted agent without PowerShell the suite fails rather than skipping silently.
- [x] **Readiness test harness.**
  - The `gh` mock in `test_get_pr_readiness.py:21-150` returns only the fields requested with `--json`. It requires `--paginate --jq tojson`, throws on `--slurp`, injects recheck failures on the second poll, and uses a real native exit for permission errors.
  - `snapshot()` (253-306) checks that the side-effect files for the `/azp run` trigger and the reviewer request exist only when the call is allowed and no error is expected. It also checks the exact single-line POST arguments, so a repeated POST across polls would fail.
- [x] **Workflow gates** (`Get-PrReadiness.ps1:376-440`).
  - Tests cover pagination completeness (376-382), the identity guard (383-387), the unique-fork-branch fallback (388-410), association scoping (411-426), superseding older runs within `(workflow_id, event)` (428-431), and the approval, failed and pending classes (432-440).
  - The rerun test at `test_get_pr_readiness.py:1077-1104` fails one subcase if `event` or `workflow_id` is removed from the grouping, or if non-PR events are excluded. Reading only one page trips the `total_count` check. This matches the mutation results supplied with the PR.
  - In `-RunPipeline` mode with an ambiguous PR association (1305-1312), the test checks the exact error and that no trigger was posted.
- [x] **Check gates.** Failed, pending, missing and unproven checks (`Get-PrReadiness.ps1:442-474`) are covered by:
  - `test_legacy_status_contexts_gate_readiness` (580-613)
  - `test_in_progress_non_required_check_blocks_readiness` (615-624)
  - the exact-name test that rejects prefix matches (720-739)
  - `test_hidden_workflow_approval_blocks_green_check_rollup` (387-404)
- [x] **Human approval vs. merge.** `Get-PrReadiness.ps1:561-579` and `686-691` were compared with the tests at 361-385, 563-578, 626-670, 741-753, 879-885 and 1314-1323. One untested core condition is reported as Issue 1.
- [x] **Evidence collector** (`collect_ci_evidence.py`). The 48 tests in `test_collect_ci_evidence.py` cover every function from `run_windows_batch` through `main`, including:
  - `main` running the real `verify_merge` with mocked transport, the exact `gh` commit query, and rejection of reversed merge parents before any Azure call;
  - real pagination, retries and sharding, named tests, skip exemptions, and run identity;
  - sanitized errors, redirect refusal and the endpoint allowlist;
  - batch timeout and cleanup, keeping the PID and chaining the original exception.

  The mocked cleanup test patches `sys.platform`, so it also runs on Linux.
- [x] **Azure pipeline watcher.** Timeline-mode tests in `test_watch_azure_pipeline.py` cover:
  - empty queue through completion (113)
  - truncated responses (573)
  - deadline boundaries at 7199, 7200 and 7201 seconds (622)
  - agent-loss deduplication per job attempt (672)
  - an absent GitHub check, exiting with code 4 (716)
  - a same-build retry keeping the original deadline (747)
  - a provenance failure (768)
  - a single merge verification (778)
  - the outcome-to-exit-code matrix (802)

  The non-timeline path keeps its existing coverage (158-520).
- [x] **Guidance verifier.** Each guard in `verify_trusted_guidance.py` maps to a test in `test_trusted_guidance.py`:
  - `GIT_*` environment isolation;
  - wrong HEAD; dirty, staged, untracked or ignored files; replace refs;
  - symlinks; missing required files;
  - edits to `CODEOWNERS`, `pipeline.yaml` or `templates/`;
  - the fsmonitor hook not being executed;
  - `PATH` lookup that skips the current directory and relative entries, plus `PATHEXT`;
  - shadow imports, tested with the flags parsed from `references/trusted-guidance.md`. The claims in that document match the tests.
- [x] **Documentation.** All of `references/ci-evidence.md` and lines 20-112 of `references/ci-triage.md` were checked against the tests. `ci-evidence.md:69-71` states the rule behind Issue 1.
- [x] **Candidates I considered and excluded.**
  - `failedChecks` (`Get-PrReadiness.ps1:443`) does not list the CheckRun conclusion `STARTUP_FAILURE`. Only GitHub Actions produces it, and Actions runs are already blocked by the `startup_failure` entry in `failedWorkflows` (436). Required checks fail closed through `unprovenRequiredChecks` (470-474).
  - The rest are defensive or duplicate guards against hypothetical inputs:
    - the malformed-rule guard (333-335)
    - classic `checks[].context` (362)
    - the `verify_merge` shape checks
    - the collector's missing-test-name guard
    - stopping pagination on a short page
    - the batch `KeyboardInterrupt` path
    - the non-integer type check (573-574)
- [ ] **Tests were not run** because this round is read-only. The supplied Windows and Linux results are treated as claims and were checked only against the assertions read.

## Issues

### Issue 1: No test covers "a nonzero required-approval count means `unknown`"; removing that check leaves every test green
- **Severity**: Low
- **File**: tools/ci/tests/test_get_pr_readiness.py (production code: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1)
- **Line(s)**:
  - Test: 626-670, with the `requirements` tuple at 629-636.
  - Production: 572-579, with the check at 575.
  - Documented rule: `references/ci-evidence.md:69-71`.
- **Description**:
  - When GitHub's `reviewDecision` is null, the script derives `humanApproval` from the collected review rules (rulesets and classic branch protection).
  - `ci-evidence.md:69-71` says the result is `not_required` only if every collected rule requires zero approvals; otherwise it is `unknown`. For integer counts, line 575 (`$_.required_approving_review_count -ne 0`) is the only code that enforces this.
  - `test_zero_approval_rules_without_other_review_requirements_need_no_approval` sets `reviewDecision=None` and tests:
    - a zero count;
    - four other review requirements (the two code-owner flags, last-push approval and named reviewers);
    - a `None` count.

    It never tests a nonzero count.
  - The `None` row does not cover line 575 on its own. In PowerShell `$null -ne 0` is `$true`, so both the type check (573-574) and line 575 match a null count. Removing either one alone still passes.
  - The two fixtures with a count of 1 never reach this code:
    - Line 570 sets `REVIEW_REQUIRED`, which returns `pending` earlier.
    - Line 733 keeps the default `APPROVED` and does not assert `humanApproval`.
  - Deleting line 575, or changing it to `-gt 1`, therefore leaves all 42 readiness tests passing.
- **Risk**: Such a regression would go unnoticed. When a ruleset or classic protection requires at least one approval and `reviewDecision` is null, the snapshot would report `humanApproval=not_required` and `merge=requires_engineering_evidence` instead of `unknown` and `blocked`. An agent following the PR loop would then tell the requester that no human approval is needed, which undermines the separate human gate this PR adds. Severity is Low because GitHub still blocks the actual merge on its side.
- **Suggested Fix**: Add one row to the existing `requirements` tuple: `({"required_approving_review_count": 1}, "unknown"),`. The existing `for classic in (True, False)` loop and its `merge == "blocked"` assertion then cover both rulesets and classic protection. No new test methods, files or mock changes are needed, and the type check needs no extra case.

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
  "changeSize": "17 source, test, and documentation paths; 5305 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 62,
  "completedPriorPasses": 61,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver Disposition

The low-severity finding is resolved with one additional row in the existing
approval-requirements matrix. A required approval count of one with no GitHub
review decision now explicitly asserts `humanApproval=unknown` and a blocked
merge for both classic protection and rulesets. Original feedback is unchanged;
no production code, new test methods, or new source files were needed.

`python -B -m pytest tools/ci/tests/test_get_pr_readiness.py -k zero_approval -q --tb=short -rs`
passed on Windows Python 3.14.6 / PowerShell 7 (one test, 14 subtests, no skips,
41 deselected, 14.07 s) and Linux Python 3.10.20 / PowerShell 7.6.7 (one test,
14 subtests, no skips, 41 deselected, 17.80 s).
The full readiness suite passed on both platforms immediately before this
one-row change: 42 tests and 135 subtests, no skips.
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py`
passed. Removing the nonzero-count guard in an isolated script copy caused
two targeted assertion failures, one per policy source, without errors or skips.

This pass stopped after lens 5; the other five lenses did not run. The changed
test invalidates attempt 62 as a qualifying pass. Its reviewed source diff was
`32dd0b0be7a364b198b02e06e80f01b5f30ee5330b7dda642c27796d72b84b3c` against
base `d34f762a785ee8a7c7c624806f8c112256168833` and local predecessor
`38685fad3d2e3ec93e6757adf36aa270cc3a24d1`.
Transport audit confirmed the requested model, exact embedded diff,
independent read-only source access, and publication-safe output.
