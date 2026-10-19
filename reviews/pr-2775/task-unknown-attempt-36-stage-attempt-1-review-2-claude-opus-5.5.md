---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 36
stage_attempt: 1
round: 2
timestamp: "2026-10-17T22:49:43Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "af77312a0fd1817d0f3e86b04cd5ad7834160734c30ed903d3b7b453819ff875"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-36-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Import layering and trust boundary.** The imports run in one direction with no cycle:
  - `watch_azure_pipeline.py` (lines 14-27) imports `collect_ci_evidence`.
  - `collect_ci_evidence` imports only `GuidanceError` and `trusted_executable` from `verify_trusted_guidance.py` (line 21).
  - `sys.dont_write_bytecode` is set only under `__main__`, before the sibling imports.
  - All three helpers live under `.github/skills`, which the guidance check covers (`GUIDANCE_PATHS`, `verify_trusted_guidance.py` lines 15-23).
- [x] **Shared collector contract matches the docs.**
  - `collect_build` reads `getattr(args, "allow_skipped_job", [])` (`collect_ci_evidence.py` line 404). The watcher defines no such flag, which matches `references/ci-triage.md` lines 56-59 ("no skipped-job exemptions in the watcher").
  - The watcher's outcome-to-exit-code map (`watch_azure_pipeline.py` lines 352-362) matches ci-triage.md lines 46-49: `azure_completed` and `attention_required` both exit with code 4.
- [x] **Readiness guards match the docs.**
  - The guards in `Get-PrReadiness.ps1` lines 101-109 agree with `references/ci-evidence.md` (GitHub snapshot section) and SKILL.md step 9:
    - Expected SHAs and `-RequestReview` require exactly one PR.
    - `-RunPipeline` requires both SHAs and cannot be combined with `-WaitForReview` or `-RequestReview`.
  - The request and trigger blocks (lines 578-630) agree with the same docs.
  - The separate `readiness` states and the `completeness.complete` alias match the output table in ci-evidence.md and `references/readiness-gates.md`.
- [x] **Cross-reference anchors resolve.**
  - `ci-evidence.md#github-snapshot`
  - `#azure-merge-and-named-tests`
  - `ci-triage.md#waiting-for-azure-pipelines`
  - `loop-control.md#evidence-invalidation`
  - `SKILL.md#validation-only-handoff`
  - `ci-delegation.md#evidence-required-before-triggering`

  SKILL.md step 8 now links to `references/review-gauntlet.md` instead of repeating the gauntlet text.
- [x] **AGENTS.md conventions are followed.**
  - No edits to `pipeline.yaml`, workflows, release tools, or dependency pins.
  - The new suites are in `tools/ci/tests/`, which the CIHelpers job already runs.
  - They load modules the same way as the existing watcher suite (`unittest` plus `spec_from_file_location`) and carry the license header.
  - The pwsh behavior (skip locally, fail when `TF_BUILD` is set) in `test_get_pr_readiness.py` lines 199-201 matches ci-triage.md lines 95-97.
- [x] **Test fixture patterns compared.**
  - `test_collect_ci_evidence.py` lines 50-111 build fixtures with small helpers (`build()`, `job()`, `run()`, `result()`, `client_fixture()`).
  - `test_get_pr_readiness.py` has similar helpers, `fixture()` and `association()` (lines 129-196).
  - Two other new suites repeat setup inline instead; see Issue 1.
- [x] **Checked and not reported: the hardcoded Azure check name.**
  - The `"microsoft.SynapseML"` literal and its exact-match special case appear in `Get-PrReadiness.ps1` at lines 86, 461, 469, 620 and 728.
  - The default behavior matches the documented exact-name rule and is tested at `test_get_pr_readiness.py` lines 357-362, 562-576 and 751-788.
  - Moving it to one constant would only help a hypothetical rename, which is out of scope.
- [ ] **Tests, Black and pwsh were not run.** This review is read-only, so I inspected the supplied validation evidence but did not reproduce it.
  - For this freeze, that evidence reports Black only for `test_get_pr_readiness.py` and `test_watch_azure_pipeline.py`.
  - The repository-wide Black workflow still has to check the other changed Python files.

## Issues

### Issue 1: New readiness and trusted-guidance tests repeat setup code instead of using the helper pattern the PR uses elsewhere
- **Severity**: Low
- **File**: tools/ci/tests/test_get_pr_readiness.py; tools/ci/tests/test_trusted_guidance.py
- **Line(s)**:
  - `test_get_pr_readiness.py`, inline workflow-run records: 245-262, 334-349, 822-830, 848-856, 874-889, 903-911, 944-963, 1021-1040.
  - `test_get_pr_readiness.py`, inline `required_status_checks` rule pages: 378-389, 502-511, 583-594, 657-667, 687-696, 757-766.
  - `test_trusted_guidance.py`, repeated commit sequence: 37-48, 119-130, 164-175, 224-235, 245-256.
- **Description**: The PR adds a no-bloat rule to `.github/skills/synapseml-pr-loop/SKILL.md` (lines 150-155). It says to remove duplicated content and verbose filler from every changed section. `test_collect_ci_evidence.py`, added in the same diff, follows that rule by building fixtures with small helper functions. Two other new suites do not:
  - **`test_get_pr_readiness.py`**:
    - The file already has the `fixture()` and `association()` helpers.
    - It still writes out the same workflow-run record eight times. Each copy repeats `workflow_id: 4`, `head_sha: HEAD`, `event: "pull_request"`, and the `id`/`pull_requests`/`status`/`conclusion` keys.
    - It also writes out the same four-level `[[{"type": "required_status_checks", "parameters": {"required_status_checks": [...]}}]]` rule page six times.
  - **`test_trusted_guidance.py`**:
    - The same 12-line sequence appears in `setUp` and in four tests: `write-tree`, then `commit-tree` with the fixture identity, then `update-ref HEAD`.
    - The copies differ only in the commit message, which no test checks.
    - The class already has a `git()` helper (line 50) but no helper for this sequence.
- **Risk**:
  - There is no functional defect, and coverage is fine.
  - The repetition adds well over 100 lines of setup that asserts nothing, which the PR's own no-bloat rule counts as bloat.
  - It hides the one or two fields each security-relevant case actually changes, such as the `pull_requests` candidate, `head_repository`/`head_branch`, or `conclusion`. That makes the workflow-association and approval tests harder to review.
- **Suggested Fix**:
  - **`test_trusted_guidance.py`**: Add a `commit(self, message)` method next to `git()`. It should run `write-tree`, `commit-tree` with the fixture identity, and `update-ref HEAD`, then return the SHA. Call it from `setUp` (`self.sha = self.commit("Test snapshot")`) and from the four tests, after each test's own index edit (`add`, `rm`, or `update-index`).
  - **`test_get_pr_readiness.py`**: Add two helpers next to `association()`.
    - `workflow_run(**overrides)` returns the common record (`id` 42, `workflow_id` 4, `head_sha` HEAD, `event` "pull_request", `pull_requests` [association()], `status` "completed", `conclusion` "success") with the overrides applied.
    - `required_checks(*contexts)` returns a one-page rules list.

    Keep `total_count` written out where it is used, because the pagination tests deliberately use totals that do not match the page contents.
  - Each test then lists only the fields it changes. No production code changes. Rerun both suites and Black 22.3.0 on both files.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Pending.
- **How verified**: Pending.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4392 added and 163 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 36,
  "completedPriorPasses": 35,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`34211a9c6aecdb205f2d66f0cf355e2eb0bfd7774a22541e79f63e05bb000869`.
The reviewed diff SHA-256 was
`c101afbe73b9a9836fafca6b132ef5b7d2ac7fae0d3a0a2d41c918be84f0c2e2`;
the attempt-36 manifest records its complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
Both transports used the observed `claude-opus-5.5` model and only read-only
source, policy, and test tools. Neither accessed prior or sibling feedback.

**Issue 1: Fixed.** The two test files now share their repeated setup through
small local `workflow_run`, `required_checks`, and `commit` helpers. This removes
134 net lines while preserving assertions, case-specific inputs, explicit
pagination totals and page boundaries, index edits, and fixture commit messages.
The production helpers are unchanged. This addresses actual duplicated setup,
not the previously rejected hypothetical runtime-interface refactors.

- Windows: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_trusted_guidance.py -rs --tb=short`
  passed: 46 tests and 102 subtests; one POSIX-only launcher test skipped.
- Linux, from `tools/ci/tests` with PowerShell 7.6.7 on PATH:
  `python3 -B -m pytest -q test_get_pr_readiness.py test_trusted_guidance.py -rs --tb=short`
  passed: 47 tests and 102 subtests, without skips.
- From the same directory,
  `python3 -m black --check --required-version 22.3.0 test_get_pr_readiness.py test_trusted_guidance.py`
  passed after formatting the readiness test file.
- `git diff --check` passed.

Round 1 was clean; the fixture cleanup ends this pass. Both rounds remain
historical evidence, not signoff for the revised patch. No current-head Azure
validation or final CI-qualified pass is claimed.
