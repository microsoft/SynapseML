---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 67
stage_attempt: 1
round: 2
timestamp: "2026-10-19T11:32:58Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "430d48d8d5b954f717df785e651c37429f39209370e5128f48591381270ebf3a"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-67-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Layering and reuse.** `scripts/watch_azure_pipeline.py` (imports, lines 17–27) reuses the collector's `AzureClient`, `command_json`, `github_json`, `verify_merge`, `collect_build` and constants rather than re-implementing the GitHub and Azure transport.
  - `scripts/collect_ci_evidence.py` (line 21) reuses `trusted_executable` and `GuidanceError` from `scripts/verify_trusted_guidance.py` (lines 40–70).
  - Dependencies run one way (watcher → collector → verifier), with no cycles and no modules beyond the three documented helpers.
  - The watcher and collector set `sys.dont_write_bytecode` before their sibling imports. This matches `references/trusted-guidance.md` step 4: `-I -B` for the bootstrap, and `-E -s -B` for helpers that import siblings.
- [x] **Helper conventions are uniform.** All three helpers have an MIT header, use the module docstring as the argparse description, return an exit code from `main(argv=None)`, and print only JSON with sanitized errors.
  - Each defines one module-specific exception: `EvidenceError`, `MonitorError` or `GuidanceError`.
  - The watcher's outcome-to-exit map (lines ~374–385) matches `references/ci-triage.md` lines 55–64: `azure_completed` and `attention_required` exit 4, and `timeout` exits 124.
- [x] **Readiness state model.** In `scripts/Get-PrReadiness.ps1`, lines 559–577 (`evidenceComplete`, `remoteGatesClear`, `humanApproval`) and 651–717 (the output object, where `complete` aliases `remoteGatesClear`) match the `references/ci-evidence.md` output table field for field.
  - `engineering` is either `blocked` or `requires_manual_evidence`; `merge` is either `blocked` or `requires_engineering_evidence`.
  - The help text (lines 6–46) and the `-RequiredCheck` comment (lines 84–86) describe the same semantics.
  - No reference to the removed per-invocation trigger guard remains in the script.
- [x] **The comment-trigger gate has one source of truth.** The `-RunPipeline` argument checks (lines 103–111) and the complete-collection, open-PR and absent-aggregate checks (lines 610–649) implement `references/ci-evidence.md#comment-trigger-gate`.
  - A grep of `.github/skills/` for `azp run|RunPipeline` shows every trigger route links to that gate: `SKILL.md` steps 7 and 9, `references/readiness-gates.md` lines 96–101, `references/ci-triage.md` lines 16–17, and `ci-delegation.md` step 3.
  - The external `SKILL.md` (lines 41 and 66) routes CI through the PR loop rather than posting directly.
  - The mentions in `contributor-safety.md` and `synapseml-branches` only prohibit triggering or ask to verify a build; neither is a trigger route.
  - Per the plan, I didn't evaluate relaxing this conservative gate.
- [x] **Cross-document anchors resolve to existing headings:**
  - `ci-evidence.md#comment-trigger-gate`, `#github-snapshot` and `#azure-merge-and-named-tests`
  - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`
  - `loop-control.md#artifact-retention` (line 53), `#state-and-cost-discipline` (line 71) and `#evidence-invalidation` (line 136)
  - `ci-delegation.md#evidence-required-before-triggering` (line 25)
  - `readiness-gates.md#user-value-and-scope` and `SKILL.md#validation-only-handoff`
  - The "unsafe-draft procedure" cited in `review-gauntlet.md` exists as prose in `loop-control.md` (lines 104–109), not as a heading.
- [x] **Tests enforce the documented commands, not just the prose.** `tools/ci/tests/test_trusted_guidance.py` (lines ~127–220) reads the interpreter flags from `trusted-guidance.md`, `ci-evidence.md` and `ci-triage.md`, then runs the helpers with those flags against a shadowing `PYTHONPATH`.
- [x] **Test structure follows the existing `tools/ci/tests` pattern for skill scripts.** Each suite loads its script with a temporary `sys.path` insert and patched `dont_write_bytecode` (`test_collect_ci_evidence.py` 22–33, `test_watch_azure_pipeline.py` 19–35, `test_trusted_guidance.py` 15–22).
  - The suites don't depend on run order.
  - The subprocess import check (`test_collect_ci_evidence.py` ~1022–1052) runs against temporary copies, so nothing is written into the guidance tree.
- [x] **CI wiring and repository rules.**
  - The `pipeline.yaml` CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`, so it picks up all four suites without a pipeline edit.
  - `test_get_pr_readiness.py` lines 249–250 skip only when neither `pwsh` nor `TF_BUILD` is present, matching `ci-triage.md` lines ~106–113.
  - The 17 paths include no pipeline, workflow, dependency-pin or release-tool change, so AGENTS.md's "Ask before changing" rule isn't triggered.
  - Azure tokens stay in memory, and redirects and non-allowlisted endpoints are refused (`NoRedirect`, `AzureClient.get`).
- [x] **Considered but not reported** (excluded by the plan as speculative or cosmetic, or explicitly documented):
  - `collect_build` reads the watcher's argparse Namespace via `getattr(args, "allow_skipped_job", [])`. ci-triage.md documents that the watcher has no skipped-job exemptions, and replacing this with a parameter object would be a speculative abstraction.
  - The Azure build URL format is duplicated between the watcher's timeout result and `collect_build`. This is cosmetic.
  - The PowerShell helper uses normal command discovery rather than `trusted_executable`. `trusted-guidance.md` documents and justifies this.
- [ ] Not executed: this is a read-only review, so I ran no tests, linters or commands. The pass counts in the plan are caller-supplied evidence that I did not verify.
- [ ] Not assessed: the literal `Authorization` header template at `collect_ci_evidence.py` line 168 is masked in tool output, as is its paired assertion at `test_collect_ci_evidence.py` line 1129. I treated this as display redaction and did not assess it beyond that assertion.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5511 added and 199 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 67,
  "completedPriorPasses": 66,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [3, 5, 6, 1, 2, 4],
  "verifiedTaskId": null
}

## Driver completion evidence

The finalized runner receipt reports exit 0 and CLEAN; runtime events confirm
`claude-opus-5.5`. Input, response and decision hashes match the receipt.
The 40 read-only tool calls stayed within named source files and source subtrees;
no prior or sibling feedback was accessed. The original artifact is preserved
privately with SHA-256 `c556c71f12b8a9442dc24f57514b9e0c16ace26641d6dbd7bd70f142140b7b28`.
This is one lens of the six-clean pre-commit pass, not current-head CI clearance.
