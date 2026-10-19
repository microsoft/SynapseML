---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 1
timestamp: "2026-10-17T12:34:00Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "4d39163eabcf6b657b8545cc1022a34bf52e6682cc51816b655d6c6e58ed4575"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-19-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the supplied diff for all 17 paths. The four helper scripts in the worktree match it: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `watch_azure_pipeline.py`, `verify_trusted_guidance.py` and `Get-PrReadiness.ps1`.
- [x] Azure client hardening in `collect_ci_evidence.py`:
  - The endpoint allowlist in `get` (lines 88-135) blocks path injection.
  - `NoRedirect` (62-66) raises `EvidenceError`, and nothing catches it, so the bearer token is never forwarded.
  - CLI and HTTP errors are sanitized (37-55).
  - The `az` arguments are constants, and the token stays in memory.
- [x] Provenance and pagination in `collect_ci_evidence.py`:
  - `verify_merge` checks the ordered target/head parents (165-177).
  - `verify_build` pins the repository, `refs/pull/<n>/merge`, the source version and the queue reason (180-207).
  - `$skip`/`$top` paging rejects repeated IDs (137-162), and results are cross-checked against `totalTests` (504).
  - Job attempts are matched as `phaseName.jobName` (487-499). This key is valid because `pipeline.yaml` has only top-level `jobs:` (line 111); the `stages:` at line 918 is a UnitTests matrix leg.
- [x] Traced the test gates:
  - Retried jobs must republish (`required_publications` / `required_test_publications`).
  - `--allow-skipped` cannot exempt a named `--expected-test`.
  - The build and timeline are re-read to set `collectionStable`.
  - `validationPassed` requires all conditions together (`main`, 664-697).
- [x] Watcher timeline mode (`watch_azure_pipeline.py` 97-257, 271-312, 350-362):
  - The merge is verified once.
  - It keeps sleeping while Azure is incomplete, even if GitHub shows a stale state.
  - `azure_completed` and `attention_required` both exit 4.
  - It emits one `agent_loss` event per (identifier, attempt).
  - `--base-sha` and `--merge-sha` are accepted only with `--azure-timeline`.
  - `dont_write_bytecode` is set before the sibling import (14-23).
- [x] Guidance verifier (`verify_trusted_guidance.py` 40-145):
  - Tools resolve only from absolute PATH entries outside the current directory tree.
  - `GIT_*` variables are stripped, and fsmonitor and replace objects are disabled.
  - It checks the tree, the index, untracked and ignored files, and working-file bytes.
  - Symlinks and submodules are rejected.
  - Every `REQUIRED` path exists: root `CODEOWNERS`, `pipeline.yaml`, `templates/`, `contributor-safety.md` and `ci-delegation.md`.
- [x] `Get-PrReadiness.ps1`:
  - The `-RunPipeline` guards (99-102, 559-577) require one PR, both expected SHAs, complete evidence, an OPEN PR, and an absent case-sensitive aggregate check.
  - Also checked the revision recheck (498-509), the required-check sets (434-447), the fork association lookup (370-385) and the separate readiness states (510-524, 621).
- [x] All links and anchors resolve:
  - `loop-control.md` headings at lines 7, 68 and 135.
  - `ci-triage.md` headings at lines 6 and 174.
  - `ci-delegation.md` heading at line 25.
  - The cross-skill relative links.
  - `.gitignore:19` ignores `*.pyc`.
- [x] Tests read: `tools/ci/tests/test_get_pr_readiness.py` (pwsh `skipUnless` at line 160; humanApproval assertions at 302, 348, 501 and 798; no `-RequestReview` case), plus `test_collect_ci_evidence.py`, `test_watch_azure_pipeline.py` and `test_trusted_guidance.py`. The CIHelpers job in `pipeline.yaml` (112-126) runs `tools/ci/tests/`.
- [ ] Not executed: this is a read-only static review under the plan. I did not reproduce the pytest, Black or live-collector results quoted in the request.

## Issues

### Issue 1: `-RequestReview` is silently skipped when any unrelated evidence is incomplete
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 510-512, 534-551, 664-667
- **Description**: The diff adds `$evidenceComplete` to the `-RequestReview` condition. That flag is false for any collection error, for example:
  - missing Administration read for classic protection;
  - an ambiguous fork workflow association;
  - a workflow pagination shortfall;
  - any review thread with more than 100 comments.

  In each case the requested write is skipped with no warning. `automatedReviewRequested=false` looks the same as "no request needed". Neither the script help nor `ci-evidence.md` mentions this gate; the docs say only `-RunPipeline` is blocked by missing classic permission. No test covers `-RequestReview`.
- **Risk**: With `-WaitForReview -RequestReview`, the loop polls until the full timeout (up to 240 minutes) without ever requesting the review. The final warning only says that no review arrived. The fallback cannot be used by identities without Administration read, or on PRs with long threads.
- **Suggested Fix**: Gate the write on what actually protects it: `$revisionErrors.Count -eq 0`, meaning the head and base are unchanged and match any expected SHAs. Alternatively, keep the broad gate but emit `Write-Warning` and a reason field when it suppresses the request. Document the gate next to the `-RunPipeline` note, and add a harness test with a classic-protection permission failure.

### Issue 2: `humanApproval` reports `unknown`, and merge stays `blocked`, when PR rules require no approvals
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`
- **Line(s)**: 345-350, 520-524
- **Description**: `$approvalRules` collects every `pull_request` rule's parameters and any classic `required_pull_request_reviews`, whatever their content. If GitHub returns a null `reviewDecision`, as it normally does when zero approvals are required, the logic falls through to `unknown` because `$approvalRules.Count` is nonzero. `not_required` is reachable only when no PR rule exists at all. The tests cover `pending` and the incomplete-rules `unknown` case, but not this one.
- **Risk**: On branches whose PR rule requires zero approvals (common for conversation-resolution or Copilot-only rulesets), `readiness.humanApproval` never becomes `not_required` and `readiness.merge` is permanently `blocked`. The new human-gate signal stops discriminating between states. This fails closed, so it never produces a false approval.
- **Suggested Fix**: Return `not_required` when `reviewDecision` is null and every collected rule has `required_approving_review_count` 0, no code-owner requirement and no required reviewers. Keep `unknown` for every other case, and add a fixture test.

### Issue 3: A wrong test-run name looks the same as a test that never ran
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` (docs: `references/ci-evidence.md` 81-86; `synapseml-external-contributor-review/references/ci-delegation.md` 78-80)
- **Line(s)**: 437-454, 556-593, 599
- **Description**: `collect_tests` pages through every run in the build, then skips any run whose name was not requested (line 453). `runs` and `queriedRunNames` only echo the caller's input. The docs say run names are not class names and must be found in the published results. Meanwhile `ci-delegation.md` discourages reconstructing test discovery in temporary scripts. A misspelled or wrong-level name produces exactly the same "Expected suite/test was not published in a current attempt" error as a regression that really did not run.
- **Risk**: An agent may treat a naming error as missing coverage and trigger unnecessary reruns, or report a false blocker. It may also fall back to ad-hoc queries, which the guidance discourages.
- **Suggested Fix**: Add the run names the collector already fetched to the `tests` output, for example `availableRunNames` with counts, and mention it in "Azure merge and named tests". Add a test where a non-matching name still fails but lists the available names.

### Issue 4: Moving the gauntlet text removes the pre-PR fallback while the acceptance table still expects it
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/review-gauntlet.md` (also `SKILL.md`, `references/loop-control.md`)
- **Line(s)**: review-gauntlet.md 30-42; SKILL.md 262-266; loop-control.md 205-206, 219
- **Description**: `SKILL.md` presents step 8 as a pure move ("Extraction into a reference does not waive policy"), but the reference also changes the policy:
  - The removed text said the installed prompt generator rejects output directories outside the repository. It allowed assembling the six prompts from `REVIEW-PROMPTS.md` before a PR exists or when there is no Task ID.
  - The new text requires the `-Execute` runner and says to stop if it rejects the session destination.
  - `loop-control.md` still says "session storage before the PR exists" (205-206), and its "New issue, no PR" row (219) still expects pre-commit review with session drafts to succeed.
  - No acceptance row covers a runner that rejects session output.
- **Risk**: If the runner keeps the generator's in-repository restriction, which the removed text asserted and the repository cannot verify, every new PR is blocked at its mandatory pre-commit gauntlet. The contradictory acceptance row invites forbidden workarounds, such as opening the PR before review or creating a placeholder `reviews/` folder.
- **Suggested Fix**:
  1. State in `SKILL.md` that this move also tightens the policy.
  2. Record which toolkit capability or version supports session output under `-Execute`.
  3. Add an acceptance row: "Runner rejects pre-PR session destination → stop blocked; no PR creation or placeholder directory."
  4. Alternatively, keep a fallback that preserves provenance.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: The explicit review-request fallback requires an open PR and complete, unchanged revision evidence rather than unrelated policy/workflow completeness. Invalid revision evidence produces an explicit error instead of silently waiting.
- **Why**: Missing classic-protection permission must not suppress an independently authorized automated review request or imply CI authorization.
- **How verified**: Offline entry-point tests verify that missing policy permission permits the review request, while a stale expected SHA prevents any request. Both failed before the change and pass afterward.

### Issue 2
- **Status**: Fixed
- **What changed**: With no review decision, zero-approval rules produce `not_required` only when all counts are explicit numeric zero and no code-owner, last-push, or named-reviewer requirement exists.
- **Why**: A zero-approval PR rule does not itself require human approval. Missing or additional requirements remain unknown.
- **How verified**: Classic and ruleset zero-count cases failed before the fix; they and the missing-count/additional-requirement cases now pass.

### Issue 3
- **Status**: Fixed
- **What changed**: `tests.availableRunNames` lists distinct names already fetched by the collector. No additional API queries or result selection changes were added.
- **Why**: The existing evidence can distinguish a misspelled run selector without unnecessary CI reruns or replacement discovery scripts.
- **How verified**: An unknown selector still fails and fetches no detailed results, but lists available names. The regression failed before the field was added.

### Issue 4
- **Status**: Fixed
- **What changed**: The main skill explicitly identifies the switch from manual prompt assembly to `-Execute`. The reference distinguishes session review artifacts from transport-only `-RunDir`, and the acceptance table separates compatible pre-PR runners from the blocked case.
- **Why**: An in-repository-only runner cannot satisfy session-only pre-PR drafts. That incompatibility must not invite a placeholder directory, premature PR, or manual provenance fallback.
- **How verified**: Static inspection of the installed direct-contract output guard confirmed it rejects out-of-repository output before dispatch. No compatible toolkit version or successful pre-PR execution is claimed; the blocked path is explicit.

Combined current Windows gate:
`python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_watch_azure_pipeline.py --tb=short`
passed with 98 tests and 217 subtests. Black 22.3.0 and Git whitespace checks passed.
All review requests in these regressions were offline fixtures; no remote review or CI was triggered.
The original feedback above is unchanged. Original artifact SHA-256:
`7e75e16f9074ad8e2d84ad9e7c50158a45d56d6e8a53c0b3aa9e82d9b36d3fce`.
Reviewed diff SHA-256:
`311656e958a1bd1143a5071c77231749c3de60c4c3ab38d94e3f01f42934de5f`.
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
  "reviewAttempt": 19,
  "completedPriorPasses": 18,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
