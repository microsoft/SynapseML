---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 57
stage_attempt: 1
round: 3
timestamp: "2026-10-19T02:33:06Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "34497f42d712768ce087ab35d61cfcd14eed76f67c357de6fb511952d46788d7"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-57-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector process and network failures** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, lines 38–203):
  - On Windows, a CLI batch launcher that times out or is interrupted is stopped with its process tree. Total cleanup time is bounded at 5 s for the kill, 5 s for collecting output and 5 s for the final wait.
  - If the forced kill or wait fails, the error keeps the process ID and chains the original exception.
  - `command_json` turns missing or untrusted tools, OS errors, timeouts, encoding errors and invalid JSON into sanitized `EvidenceError`s.
  - `AzureClient` only accepts allowlisted endpoints and refuses redirects. Each request's timeout is capped by the remaining deadline.
  - A timeline 404 or empty body returns nothing. Pagination fails on a repeated or invalid ID, and stops on a short page.
  - If pagination drops results, the collector fails closed: the result count no longer matches `totalTests`, or the current-attempt publication is missing.
- [x] **Malformed Azure data and evidence gates** (same file):
  - `summarize_timeline` (lines 265–434) checks the types of IDs, names, order, issues, issue counts and attempts, and detects parent cycles. It treats unstarted Phase or Stage records as jobs. A non-completed build with no timeline is reported as pending, never as passing.
  - `collect_tests` (lines 488–668) rejects:
    - a run from a different build or pipeline;
    - an inconsistent run name or ID;
    - a job attempt newer than the collected timeline;
    - a malformed `jobReference`.
  - A selected current run that is not `Completed`, is empty, or has the wrong result count blocks the test gate.
  - Skip waivers only accept `Completed` results with outcome `NotExecuted` or `NotApplicable`, and never a named expected test.
  - `main` (lines 714–756) re-reads the build and timeline after reading tests to set `collectionStable`.
- [x] **Watcher boundaries** (`.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`):
  - `monitor` (lines 99–268) rechecks the deadline after every query and timeline read, and only re-raises a query error before the deadline.
  - A head or state change returns `superseded`; a newer build returns `replaced`; duplicate rows for one build or an unknown check state are errors.
  - When GitHub is missing or lagging, it returns `azure_completed`; when GitHub and Azure disagree, it returns `attention_required`. Both exit with code 4.
  - Agent-loss events are reported once per job and attempt.
  - `parse_build_id` (lines 47–74) enforces the trusted host, project and path, and the int32 range.
  - Argument parsing (lines 271–320) requires a time zone on the kickoff, truncates rather than rounds sub-microsecond precision, rejects a future kickoff, and bounds the timeout to 1–120 minutes.
- [x] **Readiness helper guards** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`):
  - Parameters are validated: SHAs, timeout range and poll range (lines 49–97). Invalid mode combinations are refused (lines 103–111).
  - Paged GraphQL queries fail if the cursor does not advance (lines 178–232). Empty or invalid REST pages are recorded as collection errors, not treated as an empty policy (lines 234–270).
  - After collection, the PR and target branch are re-read to detect movement (lines 518–557).
  - `-RequestReview` (lines 582–606) is attempted at most once per invocation and is blocked by revision errors or a PR that is not open.
  - `-RunPipeline` (lines 608–645) requires complete evidence and an open PR, and posts only when the exact, case-sensitive Azure aggregate is absent. Failing to parse the posting receipt does not change the recorded posting status.
  - The conservative complete-collection gate is intact.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`):
  - `trusted_executable` skips relative PATH entries and anything inside the current directory, including symlink targets.
  - `git()` removes `GIT_*` environment variables, uses `--no-replace-objects`, disables fsmonitor and has a 30 s timeout.
  - `verify()` checks:
    - the worktree root and HEAD;
    - file modes in the pinned tree (no symlinks or submodules);
    - index equality with no unresolved conflicts;
    - untracked and ignored files;
    - symlinked parent directories;
    - file contents, tolerating CRLF line endings.
  - `main` reports GuidanceError, OSError, UnicodeError and timeouts as `verified: false`.
- [x] **Documentation matches code; links resolve**:
  - `references/ci-evidence.md` (comment-trigger gate at lines 94–124, collector semantics from line 126) matches the code.
  - Also checked: `references/ci-triage.md` lines 6–106 and the targeted-retry section, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md`, `trusted-guidance.md`, the external-contributor `SKILL.md`, `ci-delegation.md` and the PR-loop `SKILL.md` steps 7–9.
  - All relative links and anchors resolve: `comment-trigger-gate`, `validation-only-handoff`, `user-value-and-scope`, `artifact-retention`, `evidence-invalidation`, `waiting-for-azure-pipelines`, `targeted-infrastructure-retry`, `github-snapshot`, `azure-merge-and-named-tests` and `evidence-required-before-triggering`.
- [x] **Tests for the claimed behavior exist in the tree**:
  - `tools/ci/tests/test_get_pr_readiness.py`: fake `gh` harness (lines 21–150), skip unless `pwsh` or `TF_BUILD` (lines 249–251), 30 s per-snapshot timeout, and review requests on CLOSED/MERGED PRs (lines 613–636).
  - `tools/ci/tests/test_collect_ci_evidence.py`: InProgress and Aborted runs (lines 394–408), the skip-waiver outcome/state matrix (lines 589–618), and Windows cleanup with the PID (lines 891–987).
  - `tools/ci/tests/test_watch_azure_pipeline.py`: fake clock (lines 75–91) and a CLI run that writes no bytecode (lines 791–816).
- [x] **CI wiring**: the CIHelpers job in `pipeline.yaml` (lines 112–126) runs all of `tools/ci/tests/` with a 15-minute timeout. The diff does not change `pipeline.yaml` or any workflow.
- [x] **Candidates examined and not reported** (documented, fail-closed, pre-existing or speculative):
  - Timeline warnings block `jobGatesPassed` (documented).
  - No extra poll exactly at the deadline; recheck after exit is documented.
  - Clock skew can trigger the future-kickoff rejection.
  - One transient query error ends the watcher (documented as a nonzero exit).
  - HTTP timeouts are per request, not a strict total.
  - An invalid token stays cached; latent only, because no client is reused after an error.
  - Timeline record order could affect `collectionStable`; this fails closed.
  - Reused fork branches block the trigger (documented and intentional).
- [ ] **Tests not executed**: this is a read-only reviewer role, so the supplied Windows/Linux results and the 486.94 s full-directory baseline are taken as reported. Whether CIHelpers finishes within 15 minutes on hosted agents stays unverified until an authorized current-candidate Azure run exists.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5104 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 57,
  "completedPriorPasses": 56,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
