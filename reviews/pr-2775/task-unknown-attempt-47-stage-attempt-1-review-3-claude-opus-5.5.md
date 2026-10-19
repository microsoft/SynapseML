---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 47
stage_attempt: 1
round: 3
timestamp: "2026-10-18T11:23:19Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "2a18ff537a73d72ac7ad329d62b75f7dbe53e27b585a697c1762057500db1b22"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-47-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Trigger guards and the latest receipt change.** Checked `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` L102–110, L555–559 and L609–647.
  - `-RunPipeline` is refused before any network call when:
    - more than one PR is given;
    - either expected SHA is missing;
    - it is combined with `-WaitForReview` or `-RequestReview`.
  - The script throws before posting if evidence is incomplete or the PR is not OPEN. It posts only when the exact `microsoft.SynapseML` check is absent, and the name match is case-sensitive.
  - `pipelineRunRequested` comes only from the POST's exit code.
  - Empty output parses to `$null`. Invalid JSON raises the `ArgumentException` that PowerShell 7 throws, and the script catches it. Both cases give a warning and no retry, so a POST that succeeded is never reported as failed.
  - Mixed-case expected SHAs pass `ValidatePattern` and compare case-insensitively at L555–556.
- [x] **Snapshot failure modes.** Checked the same file at about L179–233, L240–296, L298–575, L580–607 and L717–757.
  - `Invoke-PagedQuery` throws in each of these cases:
    - gh exits nonzero;
    - GraphQL returns errors;
    - the connection is missing;
    - the cursor does not advance.
  - `Get-RestEvidence` records nonzero, empty, `null` or invalid pages in `collectionErrors`. An empty array `[]` is still treated as a valid empty list.
  - Workflow pages are checked against `total_count`.
  - Each of these leaves `evidenceComplete=false`, which keeps the trigger blocked:
    - an ambiguous or foreign run-to-PR link;
    - a deleted head repository;
    - a failed or malformed recheck.
  - The review request is attempted at most once across wait polls.
  - Output depth 12 is enough for the deepest objects (workflow `pull_requests[].base.repo` and rule `parameters`).
- [x] **Azure collector.** Checked `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`.
  - **Errors (about L39–164):** command stderr is never echoed, only allowlisted endpoints are called, and redirects are refused. HTTP, protocol and JSON errors are reported without raw content. Only a timeline 404 or an empty timeline body counts as absent.
  - **Pagination (about L139–164):** a short page ends collection. Truncation can only make results fail: a `totalTests` mismatch or a missing expected suite or test in the current attempt is reported as an error. Repeated IDs also fail.
  - **Timeline (L223–400):** `summarize_timeline` checks field types, detects parent cycles and keeps the highest attempt.
  - **Build and tests:** `collect_build` (about L401–439) accepts an empty timeline only while the build is unfinished. `collect_tests` (L451–638) requires selected results to come from the current job attempt.
  - **Stability (L684–713):** `main` reads the build again so `collectionStable` gates `validationPassed`, within a 600-second deadline.
- [x] **Watcher.** Checked `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.
  - **Deadline (`monitor()`, about L103–260):** the deadline is checked around every query, and a timeout result still carries the build link.
  - **Retries:** Azure's `inProgress` status is checked before the GitHub check state. A stale GitHub `COMPLETED` during a same-build retry therefore keeps waiting.
  - **Outcomes:** a missing or stale GitHub check waits until Azure completes. A newer build returns `replaced`, and disagreeing results return `attention_required` with exit code 4.
  - **Errors and restarts:** query errors exit nonzero, as `references/ci-triage.md` L39 and L61–62 document. A restarted watcher keeps the original deadline.
  - **Input validation:** `parse_build_id` (about L47–79) rejects untrusted hosts, paths and ports, and requires an int32 build ID. `parse_kickoff` and `parse_args` (about L263–323) truncate sub-microsecond precision, require a time zone and bound the timeout.
- [x] **Guidance verifier.** Checked `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` L40–157.
  - **Tool lookup:** it skips relative PATH entries and entries under the current directory. It expands PATHEXT, which Python before 3.12 needs for `which()`.
  - **Git calls:** it removes `GIT_*` variables, disables fsmonitor and replace objects, and parses `-z` output.
  - **Rejections:** symlinks, submodules, conflicts, an index that differs from the pinned tree, and untracked or ignored files.
  - **Content check:** byte comparison catches edits hidden by assume-unchanged. CRLF differences are tolerated only for files without NUL bytes.
  - **Subprocess launches:** every Python subprocess launch goes through `trusted_executable` (collector L41–42, verifier L76–78).
- [x] **Docs match the code's failure paths.**
  - `SKILL.md` L274–281 includes both SHAs and the switch exclusions.
  - `ci-delegation.md` step 3 and `readiness-gates.md` send every trigger through `ci-evidence.md#comment-trigger-gate`.
  - The receipt and no-retry text in `ci-evidence.md` matches L630–645.
  - The `pipeline.yaml` CIHelpers job (L112–126) runs `pytest tools/ci/tests/`.
- [x] **Regression tests inspected (not executed).**
  - In `tools/ci/tests/test_get_pr_readiness.py`, the fake `gh` comment route (L86–100) covers failed, invalid, empty and incomplete responses. Side-effect assertions are at L268–297 and the receipt subtests at L901–930.
  - `tools/ci/tests/test_collect_ci_evidence.py` covers:
    - a missing timeline (L499);
    - pagination (L785);
    - redirects (L874);
    - the deadline (L896);
    - a retry starting during collection (L1013).
- [x] **Candidates considered and rejected as non-defects:**
  - The short-page stop in `AzureClient.list` can only cause a false failure, never a false pass.
  - A run summary with no name is fatal; this is intentional and tested.
  - If Azure rolls issue counts up to the job record, only the `issueCount` diagnostic is inflated; the gate result is the same.
  - The `NoRedirect` response stays open only until garbage collection.
  - Local clock skew against the watcher's future-kickoff check gives an explicit argument error.
  - An ambiguous same-branch, multi-PR association keeps the trigger blocked. That conservative gate is intentionally kept, so I am not suggesting it be relaxed.
- [ ] **Test execution**: not performed, because this review role is read-only. I relied on the supplied Windows and Linux pytest, Black and PowerShell parse results and did not rerun them.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4827 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 47,
  "completedPriorPasses": 46,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`2f9fc76614f340012f3a015c599c78993cfaaa007d231033d94b76481faa08af`.
This round was clean. A test change after round 5 invalidated the pass;
round 6 did not run. This is not qualifying final review or CI evidence.
