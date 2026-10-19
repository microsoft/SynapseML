---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 32
stage_attempt: 1
round: 1
timestamp: "2026-10-17T20:14:31Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "cc07999deade583caa5fee0978f7710e8569945b14aed040579802f7b475e33a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-32-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`. Line 1 is `#Requires -Version 7.2`.
  - Lines 100–105: expected SHAs apply to exactly one PR. `-RunPipeline` requires both SHAs and is refused with `-WaitForReview` or `-RequestReview`.
  - Lines 513–551: revision and target recheck.
  - Lines 552–572: `evidenceComplete`, `remoteEngineeringGatesClear`, and `humanApproval`. `humanApproval` reports `unknown` when rule collection is incomplete.
  - Lines 582–601: one review request, blocked when revision errors exist or the PR is not open.
  - Lines 603–626: the trigger requires complete evidence, an OPEN PR, and no check named exactly `microsoft.SynapseML`.
  - This matches `references/ci-evidence.md` and `references/readiness-gates.md`.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`.
  - Security handling: endpoint allowlist, redirects refused, Azure CLI token kept in memory, errors sanitized.
  - Pagination uses `$skip`/`$top` and detects duplicate IDs.
  - Build provenance checks definition, repository, branch, sourceVersion, and reason. Merge parents must be exactly `[base, head]`.
  - Test-run attempts must match timeline job attempts. Each job must publish its suite and named tests, and expected tests cannot be skip-exempted.
  - `collectionStable` is rechecked. `validationPassed` requires every gate, and `mergeReady` is always false.
  - This matches `ci-evidence.md#azure-merge-and-named-tests`.
- [x] Checked timeline mode in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`.
  - The deadline runs from kickoff and restarting does not extend it. The merge is verified once, and a fresh client is created each poll.
  - Agent-loss events are de-duplicated per (identifier, attempt).
  - Exit codes 0/1/2/3/4/124/130 match `ci-triage.md#waiting-for-azure-pipelines`.
- [x] Checked `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`.
  - It requires the repository top level and HEAD to match the expected checkout and SHA.
  - The tree may contain only regular blobs, so symlinks and submodules are rejected. The index must equal the tree.
  - Untracked and ignored files are both detected. Guidance paths may not pass through symlinks, and file bytes must match the blob.
  - `GIT_*` variables are stripped, `core.fsmonitor=false` is set, and only plumbing commands run.
  - `trusted_executable` skips the working directory, its subtree, and relative `PATH` entries.
  - The `REQUIRED` paths (`AGENTS.md`, `CODEOWNERS`, `pipeline.yaml`, `templates/`) exist at the repository root, matching `trusted-guidance.md`.
- [x] Every new cross-reference anchor resolves to a real heading:
  - `loop-control.md`: `#checkpoint-before-yielding`, `#state-and-cost-discipline`, `#evidence-invalidation`
  - `ci-triage.md`: `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `ci-evidence.md`: `#github-snapshot`, `#azure-merge-and-named-tests`
  - `ci-delegation.md`: `#evidence-required-before-triggering`
  - `SKILL.md`: `#validation-only-handoff`
- [x] `pipeline.yaml` lines 111–126 use top-level `jobs:` with no `stages:`, so the collector's `phaseName.jobName` keys fit the timeline. The `CIHelpers` job (`ubuntu-22.04`) runs `python3 -m pytest tools/ci/tests/ -q`, which covers all four new suites. The diff does not change `pipeline.yaml` or `.github/workflows/`.
- [x] No-bloat and scope audit of all 17 paths:
  - `references/review-gauntlet.md` replaces the step-8 text removed from `SKILL.md`, so it is not a duplicate. It agrees with the AGENTS.md session-draft and `reviews/pr-<number>/` rules.
  - The `loop-control.md` retention rule keeps original artifacts reachable through verified archival commits.
  - No product API, dependency pin, release tool, or workflow definition changes.
- [x] Cross-checked declared prerequisites. `synapseml-pr-loop/SKILL.md` lines 9–12 declare PowerShell 7.2+. The external-contributor `SKILL.md` lines 8–10 do not, although its handoff runs the PowerShell readiness helper (Issue 1).
- [x] Reviewed the supplied validation evidence but did not run any of it:
  - Affected-path pytest on Windows and Linux: 42 passed, 78 subtests.
  - Earlier full suites: 117 passed with 1 POSIX-only skip, and 118 passed.
  - Isolated missing-PowerShell skip, Black 22.3.0, and `git diff --check`.
  - Diff SHA-256 `3e5069564f20f427cdceb0ad3f63586b6e295c69cda84bad7f40713e7160fdeb` against merge base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`.

## Issues

### Issue 1: External-contributor compatibility omits the PowerShell 7.2+ prerequisite of its validation handoff
- **Severity**: Low
- **File**: .github/skills/synapseml-external-contributor-review/SKILL.md
- **Line(s)**: 8–10
- **Description**: This PR rewrote this skill's `compatibility` field to add the new helper prerequisites (`Python`, `Azure CLI (logged in)`) but left out PowerShell, which the skill's delegated path requires:
  - `references/ci-delegation.md` lines 78–80 point to the loop's shared evidence collectors.
  - The first of those collectors is `Get-PrReadiness.ps1` (`synapseml-pr-loop/references/ci-evidence.md` line 9: "Use PowerShell 7.2 or newer").
  - The loop's validation-only handoff (`synapseml-pr-loop/SKILL.md` lines 97–100) runs the readiness checks and the step 7 trigger.
  - `Get-PrReadiness.ps1` line 1 is `#Requires -Version 7.2`.

  The same diff changed the PR loop's `compatibility` to "PowerShell 7.2+". The two skills now declare different prerequisites for the same shared helpers.
- **Risk**: An agent or host that checks environments against `compatibility` could start an external-contributor review on a host that has only Windows PowerShell 5.1 or no `pwsh`. It would fail at the first readiness snapshot or delegated trigger, after the safety review is already finished. The failure is closed (no security or false-green impact), but it delays delegated CI and undercuts the PR's goal of reliable prerequisite declarations.
- **Suggested Fix**: Add PowerShell 7.2+ to the compatibility list, for example: "SynapseML checkout with git, GitHub CLI, PowerShell 7.2+, Python, Azure CLI (logged in), and GitHub/Azure Pipelines access." Nothing else needs to change.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4386 added and 162 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 32,
  "completedPriorPasses": 31,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`9fd0f6e6b844cbf993e07777e0b11b17b694578e5efb4354e7baf0e7d15aa27c`.
This finding ended the pass; the artifact does not qualify the revised patch.
The reviewed diff SHA-256 was
`3e5069564f20f427cdceb0ad3f63586b6e295c69cda84bad7f40713e7160fdeb`;
the accompanying attempt-32 manifest records the complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
The transport used the observed `claude-opus-5.5` model and only read-only
source, policy, and test tools, without accessing previous review feedback.

**Issue 1: Fixed.** The external-contributor skill's compatibility metadata now
declares PowerShell 7.2+, matching the shared readiness helper and PR-loop skill.
No helper or test behavior changed. `git diff --check` passed; behavior tests
were not repeated for this metadata-only change.

No current-head Azure validation or final CI-qualified pass is claimed.
