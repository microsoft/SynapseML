---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 49
stage_attempt: 1
round: 2
timestamp: "2026-10-18T14:15:55Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1632cb731b52691e08416f9209220bf52c3f286f268ad6fb48a2d2c991806b09"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-49-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Files read:**
  - PR-loop skill:
    - `.github/skills/synapseml-pr-loop/SKILL.md`: steps 1, 4, 7, 8 and 9
    - `.github/skills/synapseml-pr-loop/references/`: `ci-evidence.md`, `trusted-guidance.md`, `review-gauntlet.md`, `readiness-gates.md`, `loop-control.md`, `ci-triage.md`
    - All four scripts in `.github/skills/synapseml-pr-loop/scripts/`
  - Contributor skill: `.github/skills/synapseml-external-contributor-review/SKILL.md` and `references/ci-delegation.md`
  - Tests in `tools/ci/tests/`: the opening of each new test file, plus the end of `test_collect_ci_evidence.py` (bytecode, redirect, allowlist and entry-point tests)
  - Context only: `contributor-safety.md` (unchanged) and the `pipeline.yaml` CIHelpers job
- [x] **Module layering has no cycles, and each constant is defined once.**
  - `watch_azure_pipeline.py` imports `collect_build`, `verify_merge`, `command_json`, `AzureClient`, `REPOSITORY`, `MAX_BUILD_ID` and `AZURE_PROJECT_ID` from `collect_ci_evidence.py`.
  - `collect_ci_evidence.py` imports `trusted_executable` and `GuidanceError` from `verify_trusted_guidance.py`.
  - `REPOSITORY`, `AZURE_PROJECT_ID`, `AZURE_PIPELINE_ID = 17563`, `API_ROOT` and `MAX_BUILD_ID` are defined only at `collect_ci_evidence.py` lines 23-27.
- [x] **The comment-trigger rule is defined in one place.** Every comment route defers to `ci-evidence.md#comment-trigger-gate`:
  - pr-loop `SKILL.md` lines 215-219
  - `ci-triage.md` line 16
  - `ci-delegation.md` step 3
  - `readiness-gates.md` line 94
  - contributor `SKILL.md` lines 39-43 and 64-67, through the validation-only handoff

  `Get-PrReadiness.ps1` matches the gate document:
  - Lines 102-110: expected SHAs require exactly one PR, both SHAs are required, and `-RunPipeline` cannot be combined with `-WaitForReview` or `-RequestReview`.
  - Lines 612-650: one POST, with the receipt recorded and no automatic retry.

  The conservative complete-collection requirement is retained. Nothing here proposes relaxing it.
- [x] **Docs match code.** The documented values match the code:
  - Collector flags and fields: `--expected-suite`, `--expected-test`, `tests.availableRunNames`, `collectionStable`, `validationPassed`, `mergeReady`, `pipelineDefinition`
  - Readiness helper fields: `pipelineRunRequested`, `pipelineRunComment`
  - Watcher exit codes, and the verifier's `{verified, ...}` output
  - Line 1 of `Get-PrReadiness.ps1` is `#Requires -Version 7.2`, matching the new "PowerShell 7.2+" frontmatter.
  - The collector's `az account get-access-token` call matches "Azure CLI (logged in)".
- [x] **Every changed or new link anchor resolves:** `comment-trigger-gate`, `github-snapshot`, `azure-merge-and-named-tests`, `artifact-retention`, `evidence-invalidation`, `state-and-cost-discipline`, `waiting-for-azure-pipelines`, `targeted-infrastructure-retry`, `validation-only-handoff`, `evidence-required-before-triggering`.
- [x] **Repository conventions are followed:**
  - All seven Python files carry the MIT header.
  - The tests load scripts with `importlib.util.spec_from_file_location`, as the existing `test_website_doctest.py` does.
  - The unchanged CIHelpers job (`python3 -m pytest tools/ci/tests/`) picks up the new suites.
  - There are no edits to `pipeline.yaml`, workflows, release tools or dependency pins.
  - `.agents/skills/README.md` is correctly left alone.
- [x] **Trusted-tool lookup and bytecode handling are consistent:**
  - Python helpers find `git`, `gh` and `az` through `trusted_executable` (`verify_trusted_guidance.py` lines 41-71).
  - `sys.dont_write_bytecode` is set only under `__main__`. Two tests enforce this: `test_normal_cli_invocation_does_not_dirty_the_guidance_snapshot` and `test_importing_helpers_preserves_callers_bytecode_setting`.
  - The verifier's `GUIDANCE_PATHS` includes `.github/skills`, so these scripts fall inside the verified snapshot.
  - The root `CODEOWNERS` listed in `REQUIRED` exists.
- [x] **Evidence for Issue 1.** A search for `pull_requests|actions/runs|workflow_runs` in `.github/skills/synapseml-pr-loop/scripts` matched only `Get-PrReadiness.ps1`: `Get-VerifiedRunAssociations` at lines 275-282 and the workflow query at lines 376-430. The collector and the watcher never read GitHub workflow runs.
- [x] **Considered and not reported** (not a concrete defect, or not introduced by this PR):
  - The verifier's `REQUIRED` list does not include the newly added references.
  - The `GuidanceError` docstring is narrower than how the class is used.
  - The collector, verifier and watcher use different JSON error shapes.
  - Artifact-retention pruning against the AGENTS.md preservation rule. This is consistent, because pruning requires the originals to stay reachable from `refs/pull/<n>/head`.
  - CIHelpers timeout headroom. Only local timings exist; there is no hosted-agent evidence.
  - `Get-PrReadiness.ps1` already lacked a license header before this PR.
  - The stale "for every PR" qualifier at pr-loop `SKILL.md` line 280.
- [ ] **No commands, tests or builds were run.** I only read and searched files. The validation figures in the prompt were not reproduced.
- [x] **Independence disclosure.** One early search started at the checkout root and unintentionally returned matching lines from files under `reviews/`. I did not open those files, I disregarded the lines, and no finding relies on them. Every later search was limited to `.github/skills`, `tools/ci`, `.agents` or named source files.

## Issues

### Issue 1: "The collector" refers to two different tools in the CI evidence document
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/ci-evidence.md`
- **Line(s)**: 25-26. Compare line 18 ("the helper"), lines 133, 146, 152, 156 and 193, and the links at `references/ci-triage.md` line 65 and `SKILL.md` line 254.
- **Description**: The "GitHub snapshot" section describes `Get-PrReadiness.ps1` and calls it "the helper" at line 18. Lines 25-26 then say: "Fork runs often have an empty `pull_requests` array; the collector verifies an entry's base repository URL and head SHA before using its PR number." That check is `Get-VerifiedRunAssociations` in `Get-PrReadiness.ps1` (lines 275-282).

  Elsewhere, "the collector" means `collect_ci_evidence.py`, which never reads workflow runs:
  - In this document: the `<collector>` placeholder at line 133, and the uses at lines 146, 152, 156 and 193.
  - `ci-triage.md` line 65: "[collector](ci-evidence.md#azure-merge-and-named-tests)".
  - `SKILL.md` line 254: "CI evidence collector".

  So one term names two tools with unrelated responsibilities.
- **Risk**: Suppose an agent is diagnosing a missing or ambiguous fork workflow-run association. That gap blocks collection and the guarded `-RunPipeline` trigger. The sentence sends the agent to `collect_ci_evidence.py`, which has no such logic. The agent may also wrongly conclude that the Azure collector checks workflow-run association. Severity is low because the section context partly makes the meaning clear, and no gate is weakened.
- **Suggested Fix**: On lines 25-26, change "the collector verifies" to "the readiness helper verifies", or name `Get-PrReadiness.ps1` directly. Keep "collector" for `collect_ci_evidence.py`. This is a one-phrase wording change with no change in behaviour.

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
  "changeSize": "17 source, test, and documentation paths; 4861 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 49,
  "completedPriorPasses": 48,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. The full patch and observed model were verified,
but this round is **not qualifying**: its root-level Markdown search returned
prior review text. Disregarding those results afterwards does not restore
independence. Round 2 must be rerun with source-only searches on the same patch.

Issue 1 is an optional terminology suggestion, not a correctness defect.
The document's opening explicitly calls all the helpers "These collectors";
the GitHub snapshot section names `Get-PrReadiness.ps1` and its invocation
before describing its workflow-run collection. The Azure section separately
names `collect_ci_evidence.py`. No wrong command, contract, or behavior was
identified. The existing section context disambiguates the generic noun,
so no cosmetic source edit was made under the requested no-bloat scope.

Reviewed patch SHA-256 remains
`3b4f2d7e7cd4e2247e4c5755a54584a5e53db2168ea387bb10af26511b04969c`.
This disposition does not turn the excluded round into a clean result.
