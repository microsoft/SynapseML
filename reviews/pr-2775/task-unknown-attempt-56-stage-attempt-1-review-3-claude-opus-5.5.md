---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 56
stage_attempt: 1
round: 3
timestamp: "2026-10-19T00:32:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e6cbd54b5cd68862f11153851ae7eb23a45e982b5118e4ad61d5dbc630059a05"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-56-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I read the full current text of the four changed helpers and checked it against the supplied merge-base-to-index diff: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`. I also read the changed tests under `tools/ci/tests/` and the changed references and skills under `.github/skills/`. Searches stayed within `.github/skills/` and `tools/ci/`, plus `pipeline.yaml` and `CODEOWNERS`, which I opened directly.
- [x] **Collector failure modes (`collect_ci_evidence.py`).**
  - `run_windows_batch` (≈39–69) re-raises the original timeout or interrupt only after `taskkill` confirms the process tree was killed.
  - If cleanup can't be confirmed, it raises `EvidenceError` naming the PID, chained to the taskkill, kill or wait error. Cleanup is bounded at 15 s, as `references/ci-evidence.md` documents.
  - `command_json` (≈72–94) turns tool-lookup, OS, timeout, Unicode and JSON failures into sanitized errors and never echoes stderr.
  - `AzureClient.get` refuses redirects and treats only a timeline 404 or empty body as pending. It rejects non-object JSON, such as a 203 HTML login page.
  - `main` emits sanitized JSON for every `EvidenceError` and exits nonzero when evidence is unresolved.
- [x] **Pagination and malformed responses.**
  - `AzureClient.list` (≈176–203) rejects non-list pages, non-object rows, and invalid or repeated IDs.
  - A service that ignores `$skip` trips the duplicate-ID guard. A final page that is exactly full ends on the next, empty page. `$skip`/`$top` paging is the standard contract, and the Results API documents a maximum of 1000.
  - `Get-RestEvidence` (`Get-PrReadiness.ps1` 240–274) records a nonzero exit, empty or `null` output, or an invalid page in `collectionErrors`. An empty rules page (`[]`) is valid; no output at all is an error.
  - A workflow `total_count` shortfall is flagged at 380–382.
- [x] **Timeline and test provenance.**
  - `summarize_timeline` (≈262–438) type-checks identities, attempts, order, issues and counts, and rejects parent cycles. It keeps the highest attempt per (type, identifier) and applies the same gates to Phase or Stage records that have no Job child.
  - `collect_tests` (≈490–680) rejects runs that belong to another build or run, and attempts newer than the timeline.
  - Selected runs that are not `Completed` (including InProgress and Aborted) cannot pass, nor can result-count mismatches or runs without attempt provenance. Stale attempts never satisfy current gates.
  - `main` collects the build a second time to set `collectionStable`.
  - `pipeline.yaml` line 111 uses top-level `jobs:` with no explicit stages, so job identifiers match the `phaseName.jobName` key the collector builds.
- [x] **Watcher (`watch_azure_pipeline.py` `monitor`, ≈96–258).**
  - The deadline is checked before and after the GitHub and Azure queries. Azure data collected after the deadline is never used, and the timeout result still carries the build URL.
  - A changed head or PR state returns `superseded`; a newer build returns `replaced`.
  - In timeline mode, a missing or older GitHub aggregate waits until Azure completes and then returns `azure_completed`. A GitHub/Azure disagreement returns `attention_required`.
  - `agent_loss` events are reported once per (identifier, attempt). A new `AzureClient` each poll avoids token expiry.
  - Every outcome maps to an exit code, and an interrupt exits 130.
- [x] **Readiness gates (`Get-PrReadiness.ps1`).**
  - Argument checks (103–111) require exactly one PR for expected SHAs, `-RequestReview` and `-RunPipeline`. `-RunPipeline` also needs both SHAs and can't be combined with waiting or a review request.
  - The review request throws when the PR isn't OPEN or revision errors exist, and is attempted at most once per invocation (≈589–608).
  - The CI trigger requires complete evidence (≈613–619), an OPEN PR (621–623) and an absent exact `microsoft.SynapseML` check (626).
  - `pipelineRunRequested` reflects only the POST's exit status, so a receipt-parsing failure can't flip it (631–646).
  - The second read of the PR and target (≈519–557) detects movement in head SHA, branch, repository, base, state and target tip. A null `$compare` fails the `behind_by -eq 0` check.
- [x] **Trusted guidance (`verify_trusted_guidance.py`).**
  - The resolved checkout must equal `git rev-parse --show-toplevel`.
  - It checks HEAD and `ls-tree` file modes, rejecting symlinks and gitlinks. It also requires stage-0 index entries to match the tree, rejects untracked or ignored files and symlinked parent directories, and tolerates only CRLF differences (binary files must match exactly).
  - `GIT_*` variables are stripped, and Git runs with `--no-replace-objects` and `core.fsmonitor=false`.
  - Timeouts, decode errors and OS errors produce `verified:false`.
  - `trusted_executable` skips relative PATH entries and anything at or under the working directory.
- [x] **Tests (inspected, not run).**
  - The readiness harness replaces `gh` with a global PowerShell function, runs its failing native-command stub through `sys.executable`, and mocks `Get-Date` and `Start-Sleep`. Wait-mode cases therefore never sleep against the 30 s per-snapshot timeout (`tools/ci/tests/test_get_pr_readiness.py` 21–150, 253–303).
  - The suite skips only when neither `pwsh` nor `TF_BUILD` is present (249–251). The hosted `ubuntu-22.04` CIHelpers pool (`pipeline.yaml` 102, 112–126) includes `pwsh`.
  - The CLOSED and MERGED review-request cases are at 613–636. The InProgress and Aborted run cases are in `test_collect_ci_evidence.py` 399–405.
  - In the cleanup-diagnostic cases (≈933–966), `__cause__` matches the exception chaining in production code.
  - Helpers load with bytecode writing disabled, so the tests don't leave files in the verified scripts directory.
- [x] **Documentation.** The documented contracts match the code, and every linked anchor resolves to a real heading:
  - `ci-evidence.md`: #github-snapshot (7), #comment-trigger-gate (94), #azure-merge-and-named-tests (126)
  - `ci-triage.md`: #waiting-for-azure-pipelines (6), #targeted-infrastructure-retry (193)
  - `loop-control.md`: #artifact-retention (53), #state-and-cost-discipline (71), #evidence-invalidation (136)
  - `readiness-gates.md`: #user-value-and-scope (20)
  - `ci-delegation.md`: #evidence-required-before-triggering (25)
  - The PR loop's #validation-only-handoff

  Every documented comment route defers to the comment-trigger gate: PR-loop `SKILL.md` steps 7 and 9, `ci-triage.md`, `readiness-gates.md`, and `ci-delegation.md` step 3.
- [x] **Rejected candidates.** These are documented, fail-closed behavior, not defects:
  - Timeline warnings block `jobGatesPassed` (`ci-evidence.md` ≈198–203).
  - A ruleset `creation` rule counts as unevaluated, while classic `block_creations` is ignored (`ci-evidence.md` 58–68).
  - Reused fork branches, review threads with more than 100 comments, and the comment-trigger race stay blocked under the intentionally retained complete-collection gate.
  - On Windows, a Ctrl+C whose taskkill cleanup fails is reported as `error` with the PID rather than `interrupted`. It never reports a false success.
- [ ] **Hosted-agent runtime (not verifiable from source).** Whether `python3 -m pytest tools/ci/tests/ -q` fits the CIHelpers job's existing `timeoutInMinutes: 15` (`pipeline.yaml` 114) can't be determined here. The supplied local baseline for the whole directory is 486.94 s, and no Azure run exists for this candidate. Confirm it in the final CI-qualified pass.
- [ ] **Execution (not done; read-only reviewer role).** I ran no commands, tests or live GitHub/Azure queries. The pass results quoted in the review plan come from the caller and were not re-run here.

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
