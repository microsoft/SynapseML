---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 51
stage_attempt: 1
round: 3
timestamp: "2026-10-18T17:29:37Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "b60376b5e42cc225577bd0c3dbdda24a48fc9db73fe07064e4f7a8b31ec9ffbb"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-51-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector failure modes** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, read in full):
  - **Batch-launcher timeout (lines 39–66).** It kills the whole process tree with `taskkill /T /F`, then re-raises the timeout or `KeyboardInterrupt`. If it can't confirm the tree is gone, it raises `EvidenceError` instead.
  - **CLI errors (lines 69–90).** Messages never include CLI stderr or response bodies.
  - **Azure requests (lines 97–200).** Redirects are refused, so credentials aren't forwarded. Requests have a deadline, and the ****** is never printed (line 150).
  - **Bad responses fail closed.** An HTTP 203 HTML page or other non-JSON body becomes an error.
  - **Pagination.** A repeated ID or an ignored `$skip` raises an error. A `totalTests` count that doesn't match the returned results is reported as incomplete.
  - **Other cases that fail closed:**
    - a test-run attempt newer than the timeline
    - a completed build with no timeline
    - non-integer or negative issue counts
    - a cycle in timeline parent links
    - a missing or zero attempt
  - **`main` (lines 720–749).** Validation only passes if two reads of the build give the same status, result and jobs (`collectionStable`).
- [x] **Watcher edge cases** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`):
  - **Build URLs (lines 47–75).** It requires:
    - HTTPS
    - a trusted host and path
    - no user info or fragment
    - exactly one `buildId` that fits in int32
  - **`--kickoff-at` (lines 259–274).** A time zone is required. Fractional seconds are normalized for Python 3.10, and `OverflowError` is handled.
  - **Deadline and polling.** The deadline is measured from kickoff. Each poll creates a new `AzureClient` (around line 147), so a token can't expire mid-watch. `agent_loss` is reported once per job attempt.
  - **Exits.** Ctrl+C exits 130, and the exit-code map covers every outcome.
  - **Read ordering (not a defect).** GitHub is read before Azure, so the watcher can return `azure_completed` while GitHub lags. `references/ci-triage.md` lines 53–56 document this as a nonzero result that needs triage.
- [x] **Guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, read in full):
  - **Git isolation.** It strips inherited `GIT_*` variables and disables fsmonitor and replace objects.
  - **Rejected states:**
    - symlink or submodule entries
    - unresolved merge conflicts in the index
    - staged changes that differ from the pinned commit
    - untracked or ignored files, because `--others` runs without `--exclude-standard`
    - symlinked path components
  - **Line endings.** CRLF is tolerated only for files without NUL bytes.
  - **Executable lookup.** On Windows the lookup always appends a PATHEXT extension, so the extensionless `az` script is never chosen. Relative PATH entries and entries inside the current directory are skipped.
  - **Errors.** All errors produce `verified: false` with exit 1.
- [x] **Readiness trigger guards** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, read in full):
  - **Prerequisites.** The script requires PowerShell 7.2 (line 1). Both expected SHAs are validated as full 40-character SHAs (lines 92–96).
  - **Refusals (lines 103–111).** `-RunPipeline` is refused with `-WaitForReview` or `-RequestReview`, or without both expected SHAs.
  - **Base SHA.** The expected base is compared with the live target-branch tip. `references/ci-evidence.md` lines 9–17 document that this is the base to pass.
  - **Trigger (about lines 608–643).** It throws on incomplete or stale evidence and requires an open PR. It posts `/azp run` once, only when the exact `microsoft.SynapseML` check is absent (case-sensitive). A failed post or incomplete receipt produces a warning and is never retried.
  - **Other blocking inputs:**
    - workflow pagination that falls short of `total_count`
    - ambiguous PR associations for workflow runs
    - a failed compare request
    - workflow runs that are waiting for approval
- [x] **Tests and CI:**
  - **Readiness tests** (`tools/ci/tests/test_get_pr_readiness.py`):
    - They fake `Get-Date` and `Start-Sleep` (lines 127–141), so nothing waits on the real clock.
    - They skip only when `pwsh` is missing and `TF_BUILD` is unset (lines 249–251), so they can't skip silently in Azure Pipelines.
    - They assert that the CI trigger and review request each happen exactly as expected.
  - **Trusted-guidance tests** (`tools/ci/tests/test_trusted_guidance.py`). They use temporary Git repositories, not the live `scripts/` folder. The CIHelpers job runs pytest without `-B` (`pipeline.yaml` lines 111–126), and that can't cause a false failure.
  - **Documented commands.** The command lines in `ci-evidence.md` (line 133), `ci-triage.md` (line 31) and `trusted-guidance.md` (line 18) match the patterns the tests read from them.
- [x] **Pipeline structure and links.** `pipeline.yaml` has top-level `jobs:` and no stages, so the collector's `phase.job` keys match the timeline's job identifiers. Every link anchor added by the diff resolves, including `#comment-trigger-gate`, `#validation-only-handoff`, `#user-value-and-scope`, `#evidence-invalidation` and `#artifact-retention`.
- [ ] **Test execution.** Not performed. This was a read-only review using only view, grep and glob. The supplied pytest, Black and PowerShell results were not rerun, and CIHelpers runtime on hosted agents was not verified.

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

## Subsequent disposition

Round 5 identified two public-path coverage gaps. Extending the existing tests
invalidated this pass. This original clean round is retained as historical
evidence, not counted toward the changed patch's review.
