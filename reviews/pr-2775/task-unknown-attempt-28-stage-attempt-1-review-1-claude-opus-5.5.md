---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 28
stage_attempt: 1
round: 1
timestamp: "2026-10-17T17:36:31Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d39d9e456496f8ea9760680938a914c382a339d4dcbc7911b3ff5b52f68a6a4a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-28-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Collector provenance and transport** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`):
  - Lines 64–165: `NoRedirect` refuses redirects, Azure endpoints are restricted to an allowlist, transport and protocol errors are sanitized, and `$skip`/`$top` pagination rejects repeated pages.
  - `verify_merge` (167–180) requires merge parents `[base, head]` and the exact merge SHA.
  - `verify_build` (182–221) requires definition `17563`, the GitHub repository `microsoft/SynapseML`, source branch `refs/pull/<n>/merge`, `sourceVersion` equal to the merge SHA, an allowed reason, a valid status, and a queue time.
- [x] **Collector gating** (same file):
  - `summarize_timeline` (223–395) keeps the newest attempt for each (type, identifier) and passes Phase/Stage issues down to their jobs. Leaf containers are gated but cannot supply test attempts.
  - `collect_tests` (458–643) counts only current-attempt results, requires republication after a retry, scopes named tests to their job, and does not treat skips as passing.
  - `main` (691–720) sets the 600 s deadline before `verify_merge`, so the GitHub read uses the same budget. `validationPassed` requires a completed, succeeded build plus `jobGatesPassed`, `tests.passed`, and `collectionStable`. `mergeReady` is always false.
- [x] **Watcher** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`):
  - `monitor` (98–257): timeline mode verifies the merge once and deduplicates agent-loss events by (identifier, attempt). It never extends the kickoff deadline. A GitHub/Azure disagreement returns `attention_required`.
  - The exit map (352–362) matches the watcher section of `references/ci-triage.md`.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`):
  - Lines 40–152 strip `GIT_*` variables and use `--no-replace-objects` and `core.fsmonitor=false`. They require the toplevel and HEAD to match the expected checkout and SHA.
  - `ls-tree -rz` accepts only regular blobs (100644/100755). The stage-0 index must match the tree. `ls-files --others` runs without `--exclude-standard`, so ignored files are also caught. Symlinked parent directories are rejected.
  - `trusted_executable` skips relative PATH entries and entries under the current directory, and expands PATHEXT on Windows. This is covered by `tools/ci/tests/test_trusted_guidance.py`.
- [x] **Readiness helper** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - `#Requires -Version 7.2` is on line 1.
  - `-RunPipeline` guards (103–104, 585–601) require expected head and base SHAs, forbid `-WaitForReview`, and require complete evidence, an OPEN PR, and no existing `microsoft.SynapseML` check (exact, case-sensitive name).
  - `Get-RestEvidence` (237–271) fails closed on a CLI error, an empty body, or invalid JSON.
  - `remoteGatesClear` requires `behind_by -eq 0` (531–537). `humanApproval` depends on `$rulesComplete` and is unaffected by comparison or workflow read errors (538–547).
- [x] **Tests** under `tools/ci/tests/`:
  - `test_get_pr_readiness.py`: pwsh harness (20–118) and revision and safety tests (259–285, 427–441, 596–609, 657–699).
  - `test_collect_ci_evidence.py` (117–870): provenance, retries, skips, pagination, redirects, sanitization, and bytecode settings.
  - `test_watch_azure_pipeline.py`: timeline mode and deadline tests (113–340).
  - Gap: the branch fixture (lines 72–76) only models a successful re-read with a different SHA. No test covers a failed re-read of the target branch.
- [x] **Docs**:
  - All cross-document anchors resolve: `SKILL.md#validation-only-handoff`, `loop-control.md#evidence-invalidation`, `ci-triage.md#waiting-for-azure-pipelines`, and the targeted-retry and trigger-evidence anchors.
  - The documented `python -E -s -B` and `python -I -B` commands match the regex assertions in the tests.
  - `references/ci-evidence.md` (8–46) matches the readiness behavior.
- [x] **Scope**: all 17 paths are under `.github/skills/` or `tools/ci/tests/`. There are no changes to `pipeline.yaml`, workflows, dependency pins, release tools, or product APIs. The existing CIHelpers job in `pipeline.yaml` (112–126) already runs `tools/ci/tests/`.
- [ ] **Test execution**: not done. This review was read-only (view, grep, and glob). The caller's pytest, Black, and manifest SHA-256 results and the external API facts were noted but not re-run.

## Issues

### Issue 1: A failed re-read of the target branch is reported as "Target branch moved during collection."
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 311–312, 514, 525. Test gap: `tools/ci/tests/test_get_pr_readiness.py` 72–76.
- **Description**:
  - Line 514 re-reads the target branch with `$freshBranch = Get-RestEvidence "repos/$Repo/branches/$baseRef"`. When gh exits non-zero, returns an empty body, or returns invalid JSON (lines 243–270), `Get-RestEvidence` returns `$null` and only adds a collection error.
  - Line 525 then runs `if ($freshBranch.commit.sha -ne $baseSha)`. `$null -ne $baseSha` is true, so `revisionErrors` gets "Target branch moved during collection." even though the branch did not move.
  - The reverse case is mislabeled the same way. If the first branch read fails (line 312 leaves `$baseSha` null) and the re-read succeeds, the "moved" message appears next to "Missing head/base SHA."
  - The PR re-read just above (lines 517–523) handles this correctly: it reports "Could not recheck PR revisions." separately from a real head, base, or state change. The branch re-read does not.
  - The gate still blocks, because the read failure also lands in `collectionErrors` and `evidenceComplete` becomes false. The defect is the wrong diagnosis, not a gate bypass.
- **Risk**:
  - The skill docs treat target movement as a state that requires action:
    - `references/loop-control.md` lines 81–83 send target movement back to `fast` and consume a cycle. Lines 116–119 charge every target-driven integration or requeue to the five-cycle budget.
    - `synapseml-external-contributor-review/references/ci-delegation.md` lines 62–65 and the table row at line 92 say a target change revokes clearance and returns to safety review.
  - So a temporary GitHub read failure can lead an agent to do unnecessary integration, requeue, or safety re-review. That uses limited budget and points triage at the wrong cause.
  - `references/ci-evidence.md` line 12 says the re-read exists to detect movement; a failed read is not evidence of movement.
- **Suggested Fix**:
  - If the re-read is null or its SHA is malformed (`-not $freshBranch -or $freshBranch.commit.sha -notmatch '^[0-9a-fA-F]{40}$'`), add "Could not recheck target branch." to `revisionErrors`.
  - Emit "Target branch moved during collection." only when both SHAs are valid and different. Skip the comparison when `$baseSha` is already missing, since "Missing head/base SHA." covers that case.
  - Add a regression test with a fixture flag that makes the second `/branches/master` read fail. Assert that the "moved" message is absent, the recheck-failure message is present, and `evidenceComplete` is false.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver triage
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
  "changeSize": "17 source, test, and documentation paths; 4270 added and 157 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 28,
  "completedPriorPasses": 27,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition after round 1

The original response and verdict above are preserved. Its SHA-256 before this
append was `4a81e055ba026440f92411994c8d7bbc9335bec026e6b58a7bafb243ee130977`.
This finding ended the pre-commit pass; rounds 2-6 did not run. A new full
review is required on the corrected patch.

### Issue 1: Fixed; unreadable revisions are not evidence of movement

The target recheck now requires a valid SHA before comparing revisions.
It reports `Could not recheck target branch.` for unreadable or malformed
recheck evidence. Movement is reported only when both target SHAs are valid
and differ; a missing initial read no longer produces a false movement report.

The closely related PR-recheck path now validates its required identity fields
and clears an unusable parsed object before recording the read error. Valid
JSON with a missing or malformed head SHA no longer looks like an actual
head/base/state change. No incomplete evidence can clear the existing gates.
The evidence reference documents this distinction.

The new branch regression covers first-read and recheck failures and malformed
SHAs. The existing polling regression also covers two malformed PR identity
responses and asserts that neither is reported as movement. All six new cases
failed before the fix. Existing actual-movement and side-effect guards remain
covered.

### Validation

- Windows: `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py -rs --tb=short`
  — **31 passed, 85 subtests passed**.
- Linux, from `tools/ci/tests`, with PowerShell 7.6.7 on PATH:
  `python3 -B -m pytest -q test_get_pr_readiness.py -k 'revision or movement' -rs --tb=short`
  — **4 passed, 27 deselected, 16 subtests passed**.
- Black 22.3.0 and whitespace checks passed. Only the readiness helper, its
  existing tests, and its evidence documentation changed.
- The reviewer transport contains 41 file reads, 18 searches, and 5 globs,
  limited to source, tests, and repository policy. No prior/sibling feedback
  was read; the actual reviewer identity was `claude-opus-5.5`.
- Current-candidate Azure validation remains blocked on authorization.
