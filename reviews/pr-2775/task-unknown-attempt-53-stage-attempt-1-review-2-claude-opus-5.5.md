---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 53
stage_attempt: 1
round: 2
timestamp: "2026-10-18T19:39:32Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e7f27856c06fc91c29bcd9beedd3c1eab0622fc5e43fd2efb9d8fa79097c543d"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-53-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Readiness helper parameters and readiness states.** I read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` lines 1–770.
  - The guards at lines 99–111 match `ci-evidence.md#comment-trigger-gate`: one PR, both expected SHAs, and no combining with `-WaitForReview` or `-RequestReview`.
  - The conservative complete-collection trigger gate (lines 611–623) is kept.
  - The trigger posts only when the exact `microsoft.SynapseML` aggregate is absent, compared case-sensitively (line 626).
  - `-RequestReview` is cached once per PR (lines 580–606).
  - The `readiness` object keeps `evidenceComplete`, `engineering`, `humanApproval` and `merge` separate (lines 684–699), matching the `ci-evidence.md` output table.
  - `completeness.complete` is only an alias for `remoteGatesClear`.
- [x] **Shared code instead of copies.** `watch_azure_pipeline.py` lines 17–27 import `AzureClient`, `command_json`, `collect_build`, `verify_merge`, `REPOSITORY`, `AZURE_PROJECT_ID` and `MAX_BUILD_ID` from `collect_ci_evidence.py`.
  - The collector takes `trusted_executable` from `verify_trusted_guidance.py`, so all three helpers resolve `git`, `gh`, `az` and `taskkill` the same way.
  - The collector's flags (lines 675–718) match the command in `ci-evidence.md` and its documented gates.
  - Its job gates (lines 433–475) and test-publication gates (lines 487–672) match `ci-evidence.md`.
- [x] **Watcher outcomes match `ci-triage.md`.** Timeline mode (lines 136–175) reports `agent_loss` once per job attempt.
  - The exit-code map (lines 352–362) matches the docs: `azure_completed` and `attention_required` both exit 4, and the watcher has no skipped-job exemptions.
  - The deadline is based on kickoff time and runs on a monotonic clock, consistent with `AzureClient.remaining_timeout`.
- [x] **Trusted-guidance verifier paths are real.** Every path in `REQUIRED` exists. I opened root `CODEOWNERS` directly, and `ci-delegation.md` and `loop-control.md` are present.
  - The `verifiedPaths` output and the `-I -B` (bootstrap) / `-E -s -B` (helpers) split match `references/trusted-guidance.md`.
- [x] **Cross-document anchors resolve.** These all point to existing headings:
  - `ci-evidence.md#comment-trigger-gate`, `#github-snapshot`, `#azure-merge-and-named-tests`
  - `readiness-gates.md#user-value-and-scope`
  - `loop-control.md#artifact-retention`, `#evidence-invalidation`, `#state-and-cost-discipline`
  - `ci-triage.md#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`
  - `ci-delegation.md#evidence-required-before-triggering`
  - `../synapseml-pr-loop/SKILL.md#validation-only-handoff`
- [x] **Every comment-trigger route uses the gate.** I searched `.github/skills/` for `/azp run`, `RunPipeline`, `requeue` and `azp`.
  - These routes all go through the gate: pr-loop `SKILL.md` steps 7 and 9, `ci-triage.md`, `readiness-gates.md`, `ci-delegation.md` step 3, and the requeue paragraph in `ci-evidence.md`.
  - `contributor-safety.md` and `branch-spark4-common.md` only forbid triggering or confirm that a build ran.
- [x] **No leftover manual-prompt instructions.** A search for `assembl`, `REVIEW-PROMPTS`, `-Execute` and `TaskName` found only the new contract.
  - The step-8 text that moved to `references/review-gauntlet.md` no longer describes assembling prompts by hand.
  - It agrees with the "Runner rejects pre-PR session artifacts" row in `loop-control.md`.
- [x] **Tests follow the existing pattern.** The new suites `test_collect_ci_evidence.py` and `test_trusted_guidance.py` follow the pattern of the existing `test_watch_azure_pipeline.py` and `test_get_pr_readiness.py`.
  - All four use the license header, load modules with `spec_from_file_location`, use `unittest` classes and patch `dont_write_bytecode`.
  - The `pwsh`/`TF_BUILD` skip rule in `test_get_pr_readiness.py` (lines 249–251) matches `ci-triage.md`.
- [x] **Repository conventions (AGENTS.md).** `pipeline.yaml` is unchanged. Its CIHelpers job (lines 111–126) picks up everything in `tools/ci/tests/`.
  - No workflow, dependency-pin or release-tool changes.
  - `AGENTS.md` and `CONTRIBUTING.md` are untouched and contain no stale `-RunPipeline` or `-WaitForReview` usage.
- [x] **Considered and rejected as non-defects:**
  - The collector reads the optional `allow_skipped_job` argument with `getattr`. This is documented, and the watcher has no skip exemptions on purpose.
  - The collector imports `trusted_executable` from the verifier module. This keeps it to one file.
  - Exit code 4 covers two outcomes. This is documented.
  - CIHelpers has `timeoutInMinutes: 15`, and the supplied local Linux timing is about 8.2 minutes for three suites. Local timings are not hosted-agent results, and the required current-candidate CI will settle this.
- [ ] **Test execution not performed.** This was a read-only review with no commands run. The pytest, Black and PowerShell results above are the caller's supplied evidence and were not reproduced.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5025 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 53,
  "completedPriorPasses": 52,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Historical clean round, superseded by the cleanup-diagnostic fix from round 3.
The executed model was `claude-opus-5.5`; its complete frozen-diff input and
read-only tool calls were audited. No sibling feedback was read.
The reviewed predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`,
base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`, and source diff SHA-256
`ccb6c3ad89d9d290719e0e5bd22f199849937cc5a24d451a7cfe9ca75ae9ba65`.
The matching source manifest is `pr-validation-skills-attempt-53-manifest.json`.
The original review is preserved above; it is not current-patch or Azure evidence.
