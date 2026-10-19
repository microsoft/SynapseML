---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 1
timestamp: "2026-10-17T12:07:21Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9fa9afb2b933b462e1b83f9c92f3de82b97914b9784e6846b1297c6fa20eea9e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-18-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` guards:
  - `#Requires -Version 7.2` is on line 1.
  - Lines 85–103: expected SHAs require exactly one PR. `-RunPipeline` requires both expected SHAs and rejects `-WaitForReview`.
  - Lines 552–570: the trigger throws on incomplete or stale evidence, or a PR that is not OPEN. It comments `/azp run` at most once per invocation, and only when the case-sensitive `microsoft.SynapseML` check is absent.
- [x] Same script, lines 300–401 and 487–518. I traced:
  - the target-tip compare;
  - paginated reads of rulesets, classic protection and workflow runs;
  - fork-association scoping and the newest-run selection;
  - the re-read after collection that detects movement;
  - the `evidenceComplete`, `remoteEngineeringGatesClear`, `humanApproval` and `merge` logic (503–517, 605–618).

  I checked these against `.github/skills/synapseml-pr-loop/references/ci-evidence.md`.
- [x] `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, read in full:
  - endpoint allowlist, refused redirects and sanitized HTTP errors;
  - the ****** on line 112, confirmed by grep;
  - `$skip`/`$top` pagination;
  - merge parents checked in `[base, head]` order;
  - build identity checks: repository, `refs/pull/<n>/merge`, `sourceVersion` and reason;
  - test selection that respects attempts, plus a second collection to check stability;
  - malformed payloads raise `EvidenceError`.
- [x] `pipeline.yaml`, read only as a dependency:
  - There is no explicit `stages:`.
  - The `UnitTests` matrix includes `image`.
  - `PublishTestResults@2` collects `**/test-reports/TEST-*.xml`.
  - `CIHelpers` runs `tools/ci/tests`.

  This matches the collector's `<job>.<leg>` keys and the author's runtime-format evidence.
- [x] `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py` and `references/trusted-guidance.md`:
  - The verifier strips `GIT_*` environment variables and uses `--no-replace-objects` and `core.fsmonitor=false`.
  - It compares tree, index and working files, and lists ignored files.
  - The required guidance paths exist, and the bootstrap link from the external-contributor skill resolves.
- [x] `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - Timeline mode waits while Azure is incomplete and verifies the merge once.
  - It keeps the original deadline.
  - Exit codes 0, 1, 2, 3, 4, 124 and 130 match `references/ci-triage.md`.
- [x] Documentation. I read both `SKILL.md` files, `review-gauntlet.md`, `readiness-gates.md`, `ci-triage.md`, `loop-control.md`, `ci-delegation.md` and `ci-evidence.md`.
  - New relative links and anchors resolve.
  - The validated-without-maintainer-changes outcome is consistently kept out of the commit and gauntlet stages.
  - The extracted gauntlet keeps both pre-commit passes and the final CI-qualified pass.
- [x] Tests in `tools/ci/tests/`:
  - `test_get_pr_readiness.py` defines `gh` as a PowerShell function, so it is skipped only when pwsh is absent.
  - Fork cases are at lines 676–755, and the unknown rule type at 396.
  - Classic protection is tested only for checks and reviews (306) and a permission failure (757).
  - `test_trusted_guidance.py` sets the Git identity inline.
- [x] `.github/workflows/` triggers, scanned to bound the exposure in Issue 2: `pull_request` workflows use the default event types, except `release-tag-spark.yml` (`types: [closed]`). The other matches are `pull_request_target`.
- [ ] No commands or tests were run; this was a read-only review. The quoted pytest counts, Black run, live fork collection and completed-build evidence come from the author and were not reproduced.
- [ ] Working-tree files were not byte-compared with the index. I reviewed working-tree content, which includes pending changes.

## Issues

### Issue 1: Classic branch-protection settings other than checks and reviews never block the remote-clear signal
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (contract: `.github/skills/synapseml-pr-loop/references/ci-evidence.md`)
- **Line(s)**: Get-PrReadiness.ps1 318–342, 505–512, 614–617; ci-evidence.md 30–31, 43–47
- **Description**:
  - `$unevaluatedRules` is built only from the ruleset rules returned by `rules/branches/<base>` (lines 320–324).
  - The classic `branches/<base>/protection` object is fetched (328–330), but only `required_status_checks` (334–337) and `required_pull_request_reviews` (340–342) are read from it.
  - Other enabled classic settings never reach `unevaluatedRules` or `readiness.unassessedGates`, and never block `$remoteGatesClear`: `required_signatures`, `lock_branch`, `required_linear_history` and push `restrictions`.
  - The equivalent ruleset rule types do block remote-clear; the test at line 396 asserts that an unknown rule type yields `False`.
  - `ci-evidence.md` states a fail-closed rule: "Only deletion and non-fast-forward restrictions are ignored … unknown future rule types require assessment too."

  The same gate therefore gives a different result depending on whether it is set as a ruleset or as classic protection. Two settings are already handled: conversation resolution is covered by the unresolved-thread gate, and `allow_force_pushes`/`allow_deletions` mirror the rule types that are deliberately ignored.
- **Risk**: On a target with classic protection, the script can report `remoteEngineeringGatesClear=true` and `engineering=requires_manual_evidence` while unsigned commits, a locked branch or a push restriction still blocks the merge. `unassessedGates` gives no prompt to check it.
  - Mitigations: `readiness.merge` still requires `mergeStateStatus == CLEAN` (610–613), and raw `classicProtection` is output (589). Neither state authorizes a merge.
- **Suggested Fix**:
  1. After reading `$classic`, list each enabled setting not already handled as `Classic protection requires assessment: <setting>`. Add that list to `$remoteGatesClear` and `readiness.unassessedGates`.
  2. Add a regression test with `required_signatures.enabled=true` and `lock_branch.enabled=true` that asserts `remoteEngineeringGatesClear=false`.

  If you prefer to keep the current behavior, state in `ci-evidence.md` that classic protection is evaluated only for checks and reviews.

### Issue 2: The fork-head uniqueness check ignores closed PRs from the same branch whose head has moved, so ci-evidence.md overstates the guarantee
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` (contract: `.github/skills/synapseml-pr-loop/references/ci-evidence.md`)
- **Line(s)**: Get-PrReadiness.ps1 371–378, 384–387, 397–400; ci-evidence.md 21–25
- **Description**:
  - When a fork run has an empty `pull_requests` list, the script lists all PRs from `<owner>:<branch>` with `state=all`.
  - It keeps only those whose recorded `head.sha` equals the current head (371–373). It sets `$uniqueForkHead` when that subset is exactly this PR (374–378).
  - It then attributes every empty-association `pull_request*` run from that repository and branch at this SHA to this PR (384–387). This happens before it keeps the newest run per `workflow_id,event` (397–400).

  A closed PR's `head.sha` stays at its last head. Suppose a PR from the same branch ran a workflow on this SHA, then moved to another commit, then closed. The filter drops that PR, and its run is credited to this PR.

  `ci-evidence.md` (21–25) claims the lookup proves "this is the only PR on that revision, including closed PRs that could own an earlier workflow run" and that "another PR's success cannot clear this PR's approval requirement". The code only proves that no other PR currently records this SHA as its head. The negative test (719–755) gives its closed PR the same `head.sha`, so this case is untested.
- **Risk**: Masking requires all of the following:
  - a workflow that does not re-run on `synchronize`, such as one triggered only by label, edit or open events;
  - another PR from the same branch that ran on this SHA after this PR's run;
  - that PR then moving off the SHA and closing.

  In that case, its newer `success` replaces this PR's `action_required` run, and `workflowApprovalsRequired` misses a pending fork approval. Current workflows use default event types or `closed`, so the practical exposure is low. Still, the documented guarantee for this security-relevant approval gate is stronger than what the code does.
- **Suggested Fix**:
  1. Treat an empty-association run as ambiguous (a collection error) when another PR from the same head repository and branch was open at the run's `created_at`. Use the existing `state=all` results whatever their current `head.sha`, and compare each PR's `created_at` and `closed_at`. Treat a PR as open when those fields are missing. This fails closed without permanently blocking contributors who reuse a fork branch such as `master`.
  2. Add a fixture: a closed PR from the same branch with a different `head.sha`, whose open period overlaps the run. Assert `evidenceComplete=false`.

  If you accept the remaining risk, change `ci-evidence.md` to "no other PR currently records this revision as its head" and document the moved-then-closed limitation.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Unassessed enabled classic-protection settings block the automatic engineering-clear signal and appear in `readiness.unassessedGates`. Disabled settings, metadata, already-assessed rules, and settings unrelated to ordinary PR merges do not add separate gates.
- **Why**: Classic protection must not silently omit restrictions that equivalent rulesets would flag for assessment.
- **How verified**: Enabled signature, lock, linear-history, push-restriction, and future-setting regressions failed before the fix. Their enabled and disabled cases pass afterward.

### Issue 2
- **Status**: Fixed
- **What changed**: Empty-association fork runs require a unique PR from the head repository/branch across the paginated open and closed PR lookup, without dropping other PRs whose recorded head moved. Reused branches remain ambiguous and require manual association.
- **Why**: Current PR heads cannot prove historical run ownership. The deliberately conservative branch-uniqueness check avoids adding an incomplete historical timestamp inference.
- **How verified**: A closed PR from the same branch with a different head now leaves evidence incomplete. That case failed before the change; existing unique-branch and shared-head cases still pass.

Both fixes passed the full Windows readiness suite:
`python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py --tb=short`
(25 tests and 55 subtests), pinned Black 22.3.0, and Git whitespace checks.
The original feedback is unchanged. Original artifact SHA-256:
`e3b98954f4771291b2744e9827f43b14e7dbd6cc49750e67cbc0a52308220b37`.
Reviewed diff SHA-256:
`2107eb30168b1c8e102fc6313f055314d66b9b497e94ce9b6bb5d196a070b799`.
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
  "reviewAttempt": 18,
  "completedPriorPasses": 17,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
