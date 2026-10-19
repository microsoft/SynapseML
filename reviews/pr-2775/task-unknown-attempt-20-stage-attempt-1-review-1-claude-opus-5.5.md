---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 1
timestamp: "2026-10-17T12:58:56Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2021645855a1834398cc3c5113324c783be0e54e288d16f7f73683a3ef1348e8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-20-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the four changed helpers in the worktree and confirmed they match the supplied diff. Files: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (lines 1–698), `watch_azure_pipeline.py`, `verify_trusted_guidance.py` (lines 1–173) and `Get-PrReadiness.ps1` (lines 1–720).
- [x] Collector provenance (`collect_ci_evidence.py` lines 163–207, 374–420, 663–698):
  - `verify_merge` requires the merge parents to be exactly `[base, head]`.
  - `verify_build` checks the build ID, the GitHub repository, `refs/pull/N/merge`, a `sourceVersion` equal to the merge SHA, a reason of `pullRequest` or `manual`, the queue time and the status.
  - After reading tests, the collector rechecks the build and timeline and sets `collectionStable`; `validationPassed` requires it.
- [x] Attempt-aware test selection (`collect_ci_evidence.py` lines 210–620):
  - Only `Job` timeline records supply attempts.
  - `phaseName.jobName` keys match `pipeline.yaml`'s top-level matrix jobs (no `stages:`; lines 111–1047).
  - A test-run attempt newer than the timeline raises an error.
  - Publications from earlier attempts create job/suite and job/test republication requirements.
  - `--allow-skipped` cannot exempt a named `--expected-test`.
- [x] Pagination and sanitization (`collect_ci_evidence.py` lines 37–160):
  - `$top=100` is below the documented Test Results page maximum.
  - `totalTests` equality catches truncated result lists, and repeated IDs fail closed.
  - CLI stderr and HTTP bodies are never echoed.
  - `NoRedirect` refuses redirects before credentials are forwarded.
  - The token stays in memory and goes only to the fixed API root.
- [x] Readiness gates (`Get-PrReadiness.ps1` lines 100–105, 237–271, 281, 296–450, 495–580):
  - `-RunPipeline` needs one PR, both expected SHAs, no `-WaitForReview`, `evidenceComplete`, an OPEN PR, and `microsoft.SynapseML` absent under a case-sensitive match.
  - Ruleset and classic required checks match exactly and case-sensitively; skipped or neutral required checks stay unproven.
  - Unevaluated rule types and classic settings block `remoteGatesClear`.
  - `collectionErrors` is a `List[string]`, so `.Add()` does not leak output into `Where-Object` predicates.
- [x] Watcher timeline mode (`watch_azure_pipeline.py` lines 100–262):
  - The merge is verified once, and each poll creates a new `AzureClient(deadline=deadline)`, so the collector's 600-second deadline is not inherited.
  - It waits until Azure completes, and `azure_completed` and `attention_required` exit nonzero.
  - `agent_loss` is printed once per (job, attempt).
- [x] Trusted-guidance verifier (`verify_trusted_guidance.py` lines 15–148):
  - Strips `GIT_*` variables and disables replace objects and fsmonitor.
  - Checks HEAD, compares the stage-0 index with `ls-tree`, and rejects untracked or ignored files, symlinks and submodules.
  - Compares working-file bytes with CRLF tolerance.
  - Every `REQUIRED` path exists in this checkout.
- [x] Documentation cross-check:
  - New anchors resolve: in `loop-control.md`, "Checkpoint before yielding", "State and cost discipline" and "Evidence invalidation"; in `ci-triage.md`, "Waiting for Azure Pipelines"; in `ci-delegation.md`, "Evidence required before triggering".
  - `ci-evidence.md` and `ci-triage.md` claims match the code, except the guard scope in Issue 1 and the association identity in Issue 2.
- [x] Scope: no change to `pipeline.yaml`, workflows, dependency pins, release tooling or product APIs.
- [x] Test gaps (`tools/ci/tests/test_get_pr_readiness.py`):
  - The harness's fake `Get-Date` and `Start-Sleep` (lines 89–98) never allow a second `-WaitForReview` poll.
  - Workflow fixtures model `pull_requests` entries by `number` only (lines 205–860).
- [ ] Tests not executed (read-only reviewer). The supplied Windows result (98 passed, 217 subtests) and the older WSL result were not reproduced.
- [ ] Two external behaviors could not be checked live from a read-only reviewer: the Azure Test Runs List `$top` cap, and how GitHub fills `pull_requests` for PRs opened inside a fork. Issue 2 relies on GitHub's documented schema semantics.

## Issues

### Issue 1: The review-request revision guard aborts `-WaitForReview` polling after the one allowed request
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 542–545 (guard), 546–562 (request block), 665–674 (wait loop; re-snapshot at 673), 508–510 (revision errors), 100–105 (parameter guards)
- **Description**:
  - The throw at 542–545 fires when `$RequestReview`, the head is not reviewed, and there are revision errors or the PR is not open.
  - Unlike the request block at 546–547, it does not check `-not $script:reviewRequestOutcome.ContainsKey($number)`.
  - Under `-WaitForReview`, `Get-PrSnapshot` reruns every poll until review coverage arrives.
  - After poll 1 sends the request, any later poll with revision errors throws even though no request would be sent. Causes include:
    - the target branch moving during that poll (line 508);
    - the target moving past `-ExpectedBaseSha` during a wait of up to 240 minutes (line 510; the guards at 100–105 restrict only `-RunPipeline`);
    - the PR being closed.
  - The same poll without `-RequestReview` returns JSON with `revisionErrors`.
- **Risk**:
  - The whole run terminates without JSON for any PR. In multi-PR runs, snapshots already collected are discarded because `$results` never completes.
  - Rerunning starts a new invocation, so the review request is posted again. This is the repeat notification that the comment at 538–541 is meant to prevent.
  - `ci-evidence.md` says invalid revision evidence "stops the request", not later observation.
- **Suggested Fix**:
  - Add `-not $script:reviewRequestOutcome.ContainsKey($number)` to the guard at 542, or move the throw inside the request block before the POST.
  - Add a two-poll regression: allow one fake sleep and use a fixture whose branch tip changes only on the second poll. Assert exactly one request, no throw, and JSON with `revisionErrors`.
  - Note that the current `freshBranch` fixture switches after the first branch call, which is still within poll 1.

### Issue 2: Non-empty workflow-run PR associations are trusted by number alone
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 366–404 (fallback trigger 368–371; association decision 392–401)
- **Description**:
  - For `pull_request*` runs on the head, a single `pull_requests` entry decides ownership by `.number` only.
  - A different number makes the run return `$false` silently, with no collection error.
  - GitHub's REST schema says these entries are open PRs whose head SHA *or* head branch matches the run, and that they "do not necessarily indicate pull requests that triggered the run".
  - Each entry carries `base.repo` (`id`, `url`, `name`), `head.sha` and `head.repo`, but none are checked.
  - The unique-fork-branch lookup runs only when some association list is empty, so a non-empty foreign association bypasses it.
- **Risk**:
  - GitHub may list one foreign association for this PR's run, such as the contributor's own PR inside their fork from the same branch, or a PR matched only by branch name.
  - This PR's `action_required`, failed or pending run is then dropped from `workflowApprovalsRequired`, `failedWorkflows` and `pendingWorkflows`.
  - `remoteEngineeringGatesClear` and `completeness.complete` can then report clear. This is the hidden-approval case that `ci-evidence.md` and the new acceptance scenario say must surface.
  - It fails open only in uncommon configurations, and merge is never automated.
- **Suggested Fix**:
  - Accept an association only when `base.repo` identifies `$Repo` (by `url` or ID) and `head.sha` equals the run's `head_sha`.
  - Treat lists with no verified entries as unassociated: run the unique-fork-branch lookup or record a collection error. Treat multiple verified entries as ambiguous.
  - Add `base.repo` and `head` fields to the fixtures, plus a regression where a foreign-base association on an `action_required` run does not clear the gate.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: The open/revision guard now runs inside the not-yet-attempted request block. Subsequent observations remain read-only and report revision errors instead of throwing.
- **Why**: Once the invocation has attempted its single request, later target movement must not abort evidence collection or invite another notification-producing invocation.
- **How verified**: The two-poll regression fails at the original revision guard before the fix and passes afterward, preserving JSON with the target-movement error and exactly one request. The harness counts requests rather than overwriting their marker.

### Issue 2
- **Status**: Fixed
- **What changed**: Associations must identify the target repository through `base.repo.url` and match the run's `head_sha` before their PR number is used. With no verified entries, the collector uses its conservative fork-branch lookup or marks the evidence incomplete. Fixtures now include those schema fields.
- **Why**: A foreign or branch-only association must not silently hide an approval-blocked run. Matching PR metadata is not proof of the triggering event; the docs and unassessed-gate list explicitly retain trigger-provenance assessment for shared branches or revisions.
- **How verified**: Foreign-base, wrong-head, and incomplete associations failed before the fix. With verified fork identity they now retain the approval-blocked run; without it they leave collection incomplete. The official `github/rest-api-description` schema confirms both the association semantics and `pull-request-minimal` identity fields.

Current Windows gate:
`python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py --tb=short`
passed with 29 tests and 75 subtests. Black 22.3.0 and Git whitespace checks passed.
No remote review or CI was triggered; request tests use offline fixtures.
The original feedback above is unchanged. Original artifact SHA-256:
`c325183dc99a42ffe7ffed9d820bb9cc406ab52b540db7523d4517174f0e778c`.
Reviewed diff SHA-256:
`4bc38a940facfbad58a353a1a18b19698295744031bd1b4f8dfd6a1534628971`.
Target/merge base: `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.
This pass stopped on findings and does not qualify the revised patch.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; approximately 3700 added lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 20,
  "completedPriorPasses": 19,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
