---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 74
stage_attempt: 1
round: 1
timestamp: "2026-10-20T01:47:37Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4dd5e8f678a40aa58bfb1335ffbb4b3a7e25b673425bdb49f8914a15de74456d"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-74-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **`Get-PrReadiness.ps1`: CI-trigger safety.**
  - `-RunPipeline` requires both expected SHAs and exactly one PR, and it refuses to combine with `-WaitForReview` or `-RequestReview`.
  - `$owner` and `$name` are defined at script scope, before the POST to the issue-comments endpoint.
  - Before anything is posted, the trigger checks that collection is complete, then that the PR is `OPEN`, then that the exact `microsoft.SynapseML` aggregate is absent (`-cnotin`).
  - `pipelineRunRequested` comes from the POST's exit status alone. Parsing the receipt (`catch [System.ArgumentException]`) cannot change it.
- [x] **`Get-PrReadiness.ps1`: policy and gate logic.**
  - Required checks: rule-derived and classic names are matched case-sensitively, `-RequiredCheck` names by case-insensitive prefix. SKIPPED/NEUTRAL results are reported as unproven, not missing.
  - The script exposes `ruleRequiredChecks` and `classicProtection` and computes `humanApproval` and the revision recheck.
  - Fork-run association uses the verified head-branch fallback, and workflow runs are grouped by workflow and event.
  - All of this matches the GitHub snapshot and comment-trigger gate text in `ci-evidence.md`, and the exempt-diff rules in `readiness-gates.md`.
- [x] **`collect_ci_evidence.py`: transport and deadline.**
  - `DeadlineReader` sets the remaining timeout before every raw receive and closes the original stream.
  - `https_open` keeps the default certificate-verifying context, and `NoRedirect` stays in place.
  - Error handling rechecks the original deadline first. A `TimeoutError`, raw or wrapped in `URLError`, becomes a sanitized query-timeout message; other failures stay generic.
- [x] **`collect_ci_evidence.py`: inputs and provenance.**
  - `parse_args` rejects a run with no `--expected-suite` or `--expected-test` selector.
  - `main` enforces a 600-second deadline and rechecks it after the final collection.
  - `verify_merge` and `verify_build` check the merge parent order, the pipeline definition (17563), repository, source branch and version, and queue reason.
  - `collect_tests` matches test runs to job attempts.
  - Direct runs set `dont_write_bytecode` before the sibling import, so they don't write bytecode into the verified snapshot.
- [x] **`watch_azure_pipeline.py`.**
  - It allows one extra confirmation poll only when more than 600 seconds remain, and resets that allowance while Azure is still running.
  - Every possible outcome maps to an exit code (success 0 through interrupted 130), and Azure evidence errors become monitor errors.
  - Build URLs are restricted to trusted hosts and paths, and build IDs to the int32 range.
  - It grants no skipped-job exemptions. All of this matches `ci-triage.md`.
- [x] **`verify_trusted_guidance.py`.** `GUIDANCE_PATHS` (lines 15–23) covers `CODEOWNERS`, `pipeline.yaml` and `templates`, as `trusted-guidance.md` states. The bootstrap command uses `-I -B`, and the helpers use `-E -s -B`.
- [x] **`pipeline.yaml`.**
  - Lines 112–126: the CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q` with no `PublishTestResults@2`, so it lacks a test-results publisher by design, as the docs say.
  - The pipeline has no top-level `stages:`; line 918 is a matrix leg name. The collector's `phase.job` keys therefore match the timeline Job identifiers.
- [x] **Skip reconciliation (static only).**
  - On hosted Linux with `pwsh` installed, the only skip markers that apply are the two Windows-only tests at `tools/ci/tests/test_collect_ci_evidence.py:906` and `:934`. That matches the reported aggregate of "2 skipped".
  - This is inference, not proof of which tests the hosted run skipped, and the docs say as much.
  - `test_get_pr_readiness.py:249–251` fails rather than skips under `TF_BUILD` when PowerShell is missing.
- [x] **Tests.**
  - The new cases at `test_get_pr_readiness.py:513` and `:536` assert the policy-source fields, missing versus unproven checks, and side-effect files through the harness.
  - The `invalid` case of the test at `:1044` exercises the receipt-parsing catch. The streaming-deadline test is at `test_collect_ci_evidence.py:1087`.
- [x] **Docs.**
  - All 15 distinct cross-file anchors resolve to existing headings.
  - The nonpublishing-job contract is consistent across `SKILL.md`, `ci-evidence.md`, `ci-triage.md` and `loop-control.md`. Jobs that publish results still need the named results.
  - The Azure scope exception is disqualified by any `tools/` path, including both sides of a rename, so this PR is not exempt.
  - The 17-path manifest contains no product API, dependency, release, pipeline or workflow-definition change.
- [ ] **Tests not executed.** This review used read-only tools only. The driver's reported Windows and Linux results and mutation checks were not reproduced.
- [ ] **Hosted evidence not independently queried.** Build 241706733 belongs to predecessor head `0487209b…`, not the pending patch, so new-head CI is still required.
- [ ] **Frozen identity not recomputed.** The index tree and the diff and manifest SHA-256 values were not checked. The inline diff display was truncated, so I read the 17 manifest files directly, relying on the stated absence of unstaged source changes.
- [ ] **Live service behavior not verified.** How GitHub and Azure actually behave was assessed from code paths only, for example `protection.enabled` on branches protected only by rulesets, Azure page-size caps, and null timeline counts.