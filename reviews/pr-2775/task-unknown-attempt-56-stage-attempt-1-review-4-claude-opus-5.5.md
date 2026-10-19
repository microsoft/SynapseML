---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 56
stage_attempt: 1
round: 4
timestamp: "2026-10-19T00:54:37Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "148d1da559644e2cc2e44a53d867e88c06a82f52f07ffa7af0fe9fcd5dc8bacf"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-56-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector Azure client and process handling** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`):
  - `AzureClient.get` enforces the endpoint allowlist, fetches its token within the deadline, maps a timeline 404 or empty body to `None`, catches `HTTPError` before the broader errors, and refuses redirects.
  - `list()` advances `$skip` by `len(values)` with `$top=100`, stops on a short page and rejects repeated IDs. Missing rows are caught by the `totalTests` check.
  - `command_json` wraps every expected error type.
  - `run_windows_batch` cleanup re-raises the original timeout, or raises an error that keeps the PID and chains the cause.
- [x] **Collector validation logic** (same file):
  - `verify_merge` requires the merge parents in order `[base, head]`.
  - `verify_build` checks the definition, repository ID, `refs/pull/N/merge`, that the build's source commit is the merge SHA, the build reason and status.
  - `summarize_timeline` keeps the highest attempt per type and identifier, and its parent-record handling and agent-loss detection behave as documented.
  - `collect_tests`:
    - takes attempts from Job records only;
    - builds job keys as `phaseName.jobName`, matching `pipeline.yaml`'s implicit `__default` stage;
    - raises when a test run is newer than the timeline;
    - keeps InProgress or Aborted runs incomplete;
    - does not let allowed skips exempt an expected test.
  - `main` sets its deadline before `verify_merge`, and `validationPassed` requires every gate.
- [x] **Readiness snapshot gates** (`Get-PrReadiness.ps1`):
  - The argument guards (lines 103–111) require both expected SHAs for `-RunPipeline` and exclude `-WaitForReview` and `-RequestReview`.
  - Collection errors reset for each PR (line 305).
  - The required-check list starts with the exact `microsoft.SynapseML` check and drops null entries. Missing and unproven checks are matched case-sensitively (lines 359–474).
  - Startup failures are caught through `$failedWorkflows`.
  - Unevaluated classic-protection settings stay empty when there is no classic protection.
- [x] **Approval and merge states** (`Get-PrReadiness.ps1`, lines 364–368, 569–579 and 686–691):
  - `$approvalRules` combines ruleset and classic review rules.
  - `not_required` needs an integer zero approval count with no code-owner, last-push or required-reviewer rule. A null count gives `unknown`.
  - `merge` is blocked unless remote gates are clear, approval is `approved` or `not_required`, and the PR is open, not a draft and cleanly mergeable.
  - The two approval tests at lines 563–612 of `test_get_pr_readiness.py` expect exactly these results.
- [x] **Watcher** (`watch_azure_pipeline.py`):
  - The deadline runs from kickoff, so timeline mode does not extend it.
  - Each poll uses a fresh `AzureClient`, and the merge is verified once.
  - Agent-loss events are de-duplicated by identifier and attempt.
  - A GitHub SUCCESS alongside a failed, partial or canceled Azure result yields `attention_required`.
  - The replaced, superseded and timeout outcomes and their exit codes are consistent.
  - `parse_kickoff` truncates fractional seconds to six digits.
- [x] **Trusted-guidance verifier** (`verify_trusted_guidance.py`):
  - The executable lookup handles `PATHEXT` and skips relative or working-directory `PATH` entries.
  - The `git` wrapper strips `GIT_*` variables, disables object replacement and fsmonitor, and times out.
  - It checks for regular files only, required files and an exact index match.
  - Its untracked-file check also catches ignored files.
  - It rejects symlinked parents and compares bytes while tolerating CRLF.
- [x] **Docs match the code; link targets resolve**:
  - `ci-evidence.md` (comment-trigger gate at line 94; warnings block job gates at lines 198–202), `ci-triage.md`, `trusted-guidance.md`, `review-gauntlet.md`, `loop-control.md` and `readiness-gates.md` all match helper behavior.
  - Every link anchor in the diff matches a real heading, and the relative paths from `synapseml-external-contributor-review/` resolve correctly.
- [x] **Test structure and CI wiring**:
  - The readiness suite skips only when neither `pwsh` nor `TF_BUILD` is present (`test_get_pr_readiness.py` line 249), so it cannot silently skip in Azure Pipelines.
  - The suites contain two Windows-only tests (`test_collect_ci_evidence.py` lines 875 and 903) and two POSIX-only tests (`test_trusted_guidance.py` lines 107 and 308), matching the reported skip counts.
  - The existing `pipeline.yaml` CIHelpers job (lines 112–126) runs all of `tools/ci/tests/`.
  - The diff makes no product API, dependency pin, release tool, pipeline or workflow changes.
- [ ] **Tests not run here:** this review was read-only, so no commands were executed. I checked the supplied Windows and Linux pytest results only for internal consistency, such as skip counts; I did not reproduce them.

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
