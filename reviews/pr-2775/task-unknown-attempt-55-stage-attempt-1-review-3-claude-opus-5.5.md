---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 55
stage_attempt: 1
round: 3
timestamp: "2026-10-18T22:55:01Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a41fac55f441107eb3bc4329c2b4eeb98307c9cdbe47b26ad5c6af4fec97e8f7"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-55-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I reviewed the merge-base-to-index state of all 17 paths. The supplied diff text was cut off after the `.github/skills/synapseml-pr-loop/SKILL.md` section 8 hunk, so I read the rest of the scripts, tests and references directly. Searches stayed within `.github/skills/`, `tools/ci/` and named root files such as `pipeline.yaml`. I did not open `reviews/` or any other reviewer's files.
- [x] **Subprocess and HTTP failures** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` L39–203):
  - A Windows batch timeout first runs `taskkill /T /F`. If that fails, it falls back to `kill` and `wait`, then raises `EvidenceError` with the PID and the original error chained. Cleanup takes at most 5+5+5 seconds, which matches the "fifteen additional seconds" in `ci-evidence.md`.
  - The cleanup error passes through `command_json` unchanged, so the PID is not lost. `OSError`, `TimeoutExpired`, `UnicodeError` and `ValueError` become error messages with sensitive output removed.
  - Redirects are refused, and only endpoints matching the allowlist pattern are requested.
  - Each request's timeout, including the token fetch, is limited by the remaining deadline. An expired deadline raises an error before any credential is fetched.
  - Only the timeline endpoint treats a 404, empty or null response as missing.
  - Pagination uses `$skip`/`$top=100`, rejects invalid or repeated IDs, and stops on a short page. A final page of exactly 100 rows costs one extra empty request.
- [x] **Evidence gates** (same file, L206–753):
  - **Merge:** the merge commit's parents must be `[base, head]` in that order.
  - **Build:** definition 17563, repository, source branch, `sourceVersion`, reason, `queueTime`, and the types of status and result are all checked.
  - **Timeline:**
    - Types, attempts, `order` and issue values are validated, and parent cycles are rejected.
    - Only the highest attempt of each job is used.
    - Phase or Stage records with no Job below them are gated like jobs.
    - Issues on parent records count against every job below them.
    - A missing `issues` list is accepted.
  - **Test runs:**
    - Build, run, name and attempt are cross-checked.
    - A run attempt newer than the timeline is rejected, and older attempts never count.
    - `totalTests` must equal the number of results, and the run must be `Completed`.
    - Allowed skips cover only `NotExecuted`/`NotApplicable` and never cover an `--expected-test`.
  - **Final result:** a second `collect_build` sets `collectionStable`, and `validationPassed` requires a completed, succeeded build with passing job gates, passing tests and stability. `mergeReady` is always false.
- [x] **Watcher limits** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` L47–367):
  - **Input checks:**
    - Build URLs are checked for host, scheme, port, user info, path and fragment.
    - The int32 limit is checked on the digit string before conversion.
    - A kickoff time in the future or without a time zone is rejected.
    - Long fractional seconds are truncated, not rounded up.
  - **Deadline:**
    - The kickoff time is converted once into a fixed deadline.
    - Every query timeout is clipped to the remaining time.
    - The deadline is rechecked after each query.
  - **Outcomes:**
    - A changed head or PR state returns `superseded`.
    - Duplicate build IDs are an error.
    - A newer build returns `replaced`.
    - While Azure is still running, a stale GitHub conclusion is ignored and the watcher keeps waiting.
    - A GitHub/Azure disagreement returns `attention_required`.
    - A missing or lagging GitHub check returns `azure_completed`, never success.
  - **Repeat work:** agent-loss events are reported once per job and attempt. The merge is verified once against the pinned revisions.
- [x] **Readiness helper** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` L100–117, L182–298, L370–425, L515–650, L713–756):
  - **`-RunPipeline` guards:**
    - It refuses multiple PRs, missing expected SHAs, and use with `-WaitForReview` or `-RequestReview`.
    - It throws before posting anything if evidence is incomplete or stale, or the PR is not open.
    - It posts only when the exact, case-sensitive `microsoft.SynapseML` check is absent.
    - The POST's exit status is recorded separately from reading its response.
  - **Collection:**
    - A review is requested at most once per invocation.
    - A GraphQL cursor that stops advancing is an error.
    - REST failures and empty responses become `collectionErrors`.
    - Workflow-run pagination is checked against `total_count`.
    - The fork fallback requires exactly one PR from that branch, including closed PRs.
    - Head, base, state, repository and target tip are rechecked after collection.
  - **Known limits:** the comment-trigger race is stated openly (script comment at L624–625, `ci-evidence.md` L122–124). The complete-collection gate is kept, as instructed.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` L15–175):
  - **Git safety:** it strips `GIT_*` variables, uses `--no-replace-objects` and `core.fsmonitor=false`, and gives Git a 30-second timeout.
  - **Snapshot checks:**
    - The checkout must be the worktree root, and HEAD must equal the expected SHA.
    - Files must be regular, and every index entry must be at stage 0.
    - The index must equal the pinned tree, and untracked or ignored files are rejected.
    - Symlinked paths are rejected, and file bytes must match. Only CRLF line endings are tolerated, and only in text files.
  - **Error reporting:** missing files, decode errors and timeouts produce `verified:false`.
  - **Executable lookup:** relative PATH entries and the current directory tree are ignored.
  - **Required files:** all eight `REQUIRED` paths exist. `.github/copilot-instructions.md` is absent, but it is not in `REQUIRED`.
- [x] **Docs match the code.** The edited references match the implemented behavior:
  - `ci-evidence.md`: GitHub snapshot, comment-trigger gate and collector contract.
  - `ci-triage.md` L6–96: watcher outcomes, deadline, timeline mode and exit codes.
  - `trusted-guidance.md` and `review-gauntlet.md`.
  - `SKILL.md` L22–31 and §7 (CI steps).
  - `ci-delegation.md` L66–97.

  All new anchors resolve: `#comment-trigger-gate`, `#azure-merge-and-named-tests`, `#validation-only-handoff`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#waiting-for-azure-pipelines` and `#evidence-required-before-triggering`.
- [x] **Regression coverage.** The tests exercise the edge paths above:
  - `tools/ci/tests/test_collect_ci_evidence.py`: repeated pages, malformed values, refused redirects, expired deadline before credentials, the batch timeout and cleanup PID, and a retry during collection.
  - `tools/ci/tests/test_watch_azure_pipeline.py`: deadline clipping, already-expired runs, replacement, oversized IDs, a missing GitHub check, and the retry/disagreement matrix.

  Fixture identifiers (`UnitTests.scala`, `FabricE2E`) fit the stage-less job layout in `pipeline.yaml`.
- [x] **Supplied validation evidence**, cited as reported by the driver and not run by this reviewer:
  - Three-suite run: Windows 94 tests/208 subtests; Linux 96/208.
  - Collector and watcher run: Windows 81/232; Linux 79/228.
  - Earlier full-directory Linux run: 370/338 in 486.94 s.
  - Pinned Black 22.3.0 passed.
- [ ] **Not verifiable without running CI; not a finding.** I could not check how long the `CIHelpers` job takes on a hosted agent. It has a 15-minute limit (`pipeline.yaml` L112–126, unchanged by this PR), and the earlier local full-directory run took 486.94 s. Check the job's elapsed time in the first authorized build of the current head.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5074 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 55,
  "completedPriorPasses": 54,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
