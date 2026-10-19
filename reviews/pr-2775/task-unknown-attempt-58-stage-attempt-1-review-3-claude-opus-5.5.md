---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 58
stage_attempt: 1
round: 3
timestamp: "2026-10-19T04:30:23Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "636d290faaae37313e481e23cd5e5202658bdbae34cfc1a9960ca451b409be9f"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-58-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope:** I reviewed the supplied merge-base-to-index diff (17 paths) against the current sources:
  - `Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`
  - `ci-evidence.md`, `ci-triage.md` and `trusted-guidance.md`
  - `tools/ci/tests/test_get_pr_readiness.py`

  This was a read-only review; I ran no commands or tests. The supplied validation results were treated as claims.
- [x] **Side-effect guards (`Get-PrReadiness.ps1` 103-111 and 590-650):**
  - Argument guards throw before any network side effect.
  - `-RequestReview` is attempted at most once per PR (tracked in `reviewRequestOutcome`); later `-WaitForReview` polls stay read-only.
  - `-RunPipeline` throws on incomplete or stale evidence, or on a non-OPEN PR, before posting. It only posts when the exact, case-sensitive `microsoft.SynapseML` aggregate is absent.
  - The POST exit status is recorded separately from receipt parsing, so an empty, invalid or partial receipt cannot cause a retry.
- [x] **Read failures (`Get-PrReadiness.ps1` 240-274 and 331-387):**
  - A nonzero `gh` exit, an empty or `null` body or page, or invalid JSON adds a `collectionErrors` entry; it never becomes an empty policy.
  - `$rulesComplete` (line 369) is captured before the compare and workflow reads, so it reflects only the policy reads.
  - Workflow pagination is checked against `total_count`, and each run's identity against the head SHA.
- [x] **Fork runs and check sets (`Get-PrReadiness.ps1` 388-474):**
  - A fork run with an empty or foreign `pull_requests` list is attributed only when a `state=all` lookup proves one PR for that head repo and branch. Otherwise collection stays blocked, as `ci-evidence.md` documents.
  - Legacy StatusContext `state` and CheckRun `conclusion` both feed the failed, pending and unproven sets.
  - Duplicate names and names differing only by case fail closed.
- [x] **Revision recheck and approval (`Get-PrReadiness.ps1` 540-580):**
  - An unreadable recheck is reported separately from real head or target movement.
  - `humanApproval=not_required` requires an integer zero approval count with no code-owner, last-push or named-reviewer requirement. A null count stays `unknown`.
- [x] **CLI process handling (`collect_ci_evidence.py` 39-96):**
  - `run_windows_batch` re-raises the original `TimeoutExpired` or `KeyboardInterrupt` after a successful `taskkill /T`. Otherwise it raises `EvidenceError` with the PID, chained from the last cleanup error.
  - Discarding stderr avoids pipe deadlocks.
  - `command_json` turns OS, timeout, decode and JSON failures into sanitized errors without echoing CLI output.
- [x] **Azure client (`collect_ci_evidence.py` 110-203):**
  - Each request is bounded by the smaller of 60 seconds and the remaining deadline.
  - Redirects are refused before credentials are forwarded, and endpoints are regex-allowlisted.
  - `$skip`/`$top` pagination validates row shapes and IDs and rejects repeated IDs, which also catches a server that ignores `$skip`. It ends on a short or empty page.
- [x] **Timeline handling (`collect_ci_evidence.py` 262-478):**
  - Parsing rejects non-string identities, invalid order or issue-count values, and parent cycles.
  - It keeps the highest attempt per type and identifier, and skips only Phase/Stage records that have descendants.
  - An empty timeline is treated as pending only before the build completes.
  - `collectionStable` re-reads the build after collecting tests, so a retry that starts mid-collection cannot pass.
- [x] **Test collection (`collect_ci_evidence.py` 490-675)** reports an error for:
  - a requested run without matching job-attempt provenance;
  - a test-run attempt newer than the timeline;
  - a `totalTests` value that differs from the result-row count;
  - a run that is not `Completed` or has no results;
  - an expected publication missing from a retried job.

  `--allow-skipped` cannot waive a result that is not `Completed`, a failed result, or a named expected test.
- [x] **Watcher (`watch_azure_pipeline.py` 98-256):**
  - The deadline is anchored to the kickoff and never extended; an already-expired budget returns `timeout` before any query.
  - Expiry during a GitHub or Azure read returns `timeout`, not `error`.
  - Merge parents are verified once, and agent-loss events are de-duplicated per job identifier and attempt.
  - In timeline mode, an absent or stale aggregate keeps polling until Azure completes (`azure_completed`).
  - A GitHub/Azure disagreement returns `attention_required`.
- [x] **Trusted-guidance verifier (`verify_trusted_guidance.py` 40-156):**
  - Tool lookup skips relative PATH entries and anything resolving under the current directory, as `trusted-guidance.md` documents.
  - Verification fails closed on HEAD, index or working-tree mismatch, unresolved index conflicts, untracked or ignored files, symlinks and submodules, and any difference other than CRLF line endings.
- [x] **Readiness tests (`tools/ci/tests/test_get_pr_readiness.py`):**
  - The `gh` mock throws on unexpected endpoints and on `--slurp`, and logs any review-request or CI-trigger POST.
  - `snapshot()` asserts that no POST is logged unless the fixture allows it and no error is expected.
  - These three tests match the production logic above:
    - `test_legacy_status_contexts_gate_readiness`
    - the legacy-PENDING branch of `test_unrelated_required_check_does_not_trigger_azure_again`
    - the classic-metadata payload in `test_zero_approval_rules_without_other_review_requirements_need_no_approval`
- [x] **`pipeline.yaml` 111-127:** Top-level `jobs:` (one implicit stage) supports the collector's `phaseName.jobName` timeline identifiers. The `CIHelpers` job runs all of `tools/ci/tests/` with `timeoutInMinutes: 15`.
- [x] **Candidates I rejected as not concrete, or conservative by design:**
  - Pagination snapshot races: duplicates are detected or grouped away.
  - A stale `AzureClient.token` after a failure: the client is discarded.
  - A slow-drip `read()`: the socket timeout applies per operation.
  - Possible null `warningCount`/`errorCount`: not shown in Azure output, and it would fail closed.
  - Duplicate rollup entries and ambiguous fork associations: these fail closed, and the complete-collection trigger gate is intentional.
  - An orphaned `az` Python child after a POSIX timeout: outside the documented Windows-only cleanup scope.
  - Windows batch argument injection: all arguments are constants or validated hex.
- [ ] **Test execution:** not performed. As a read-only reviewer I inspected the supplied Windows and Linux pytest results but did not reproduce them.
- [ ] **Hosted-agent `CIHelpers` runtime against the 15-minute timeout:** not verifiable. No current-candidate Azure run exists, and the 486.94 s full-directory baseline was a local run. The final CI-qualified pass must establish it.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5157 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 58,
  "completedPriorPasses": 57,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
