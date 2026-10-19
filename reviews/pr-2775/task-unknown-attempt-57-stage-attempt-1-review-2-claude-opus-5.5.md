---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 57
stage_attempt: 1
round: 2
timestamp: "2026-10-19T02:03:42Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "81a0a74b49df850987eee6397e423fdc5c4b7eef8f5eee28ea93b792dda347e8"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-57-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Helper layering is acyclic and has one owner per policy.**
  - `watch_azure_pipeline.py` lines 17–27 import the client, provenance and job-gate logic from `collect_ci_evidence.py`. Line 21 of the collector imports `trusted_executable` from `verify_trusted_guidance.py` (lines 40–71).
  - The Azure client, merge/build provenance (`verify_merge`/`verify_build`, lines ~205–258) and executable lookup are each implemented once. No parallel copies or temporary scripts were introduced.
- [x] **Every documented comment route goes through one canonical gate:** `references/ci-evidence.md` lines 94–124.
  - The gate is linked from:
    - pr-loop `SKILL.md` lines 212–215 and 227–228, and step 9 at lines 266–277;
    - `ci-triage.md` lines 16–17;
    - `readiness-gates.md` lines 96–101;
    - `ci-delegation.md` lines 69–72.
  - External-contributor `SKILL.md` lines 39–43 and 64–68 route through the loop's validation-only stages. `contributor-safety.md` only prohibits triggering.
  - `Get-PrReadiness.ps1` enforces the same contract:
    - lines 103–111: one PR, expected SHAs, and no `-WaitForReview`/`-RequestReview`;
    - lines ~610–650: complete evidence, an OPEN PR, and an absent case-sensitive `microsoft.SynapseML` aggregate.
  - The conservative complete-collection gate is intact, as required.
- [x] **Evidence, engineering, human and merge states are separated consistently in code and docs.**
  - `Get-PrReadiness.ps1` computes them at lines ~559–580 and outputs them at lines ~683–711. `completeness.complete` remains an alias for `remoteGatesClear`.
  - The same states are described in `ci-evidence.md` lines 73–81, `readiness-gates.md` lines 127–131 and `SKILL.md` lines 92–93.
  - The collector emits a constant `mergeReady=False`, and `readiness.engineering` can never be clear. Neither helper can stand in for human or merge approval.
- [x] **Helper interfaces match their documentation.**
  - **Collector:** `parse_args`/`main` (lines 678–752) match `ci-evidence.md` lines 140–199. Specifically:
    - definition 17563;
    - the ten-minute deadline and stability recheck;
    - `--allow-skipped` cannot exempt `--expected-test`;
    - `--allow-skipped-job`;
    - exit codes 0 and 1.
  - **Watcher:** `parse_args` (lines 277–319) and the exit map (lines 356–366) match `ci-triage.md` lines 31 and 50–68. Specifically:
    - `--azure-timeline --base-sha --merge-sha`;
    - `azure_completed` and `attention_required` outcomes;
    - no skip exemptions, through the `getattr` default in `collect_build`.
- [x] **The trust bootstrap is coherent.**
  - `verify_trusted_guidance.py` contents:
    - `GUIDANCE_PATHS` and `REQUIRED` at lines 15–33;
    - git calls with `GIT_*` stripped, `--no-replace-objects` and `core.fsmonitor=false`;
    - CRLF tolerance;
    - `verifiedPaths` in its output.
  - These match `trusted-guidance.md` lines 8–57.
  - The interpreter flags are consistent with the sibling-import design. The stdlib-only verifier runs with `-I -B`. Helpers that import siblings run with `-E -s -B`, as `ci-evidence.md` line 140 and `ci-triage.md` line 31 document.
  - The PowerShell helper's ordinary command discovery is a documented exception, not a hidden inconsistency.
- [x] **Each policy has one source of truth, with no duplicate documents.**
  - The no-bloat gate lives in `readiness-gates.md` lines 20–27 and is linked from `SKILL.md` lines 150–151.
  - Artifact retention lives in `loop-control.md` lines 53–69 and is linked from `review-gauntlet.md` lines 26–29.
  - `SKILL.md` step 8 (lines 259–262) delegates to `review-gauntlet.md`.
  - The pre-PR stop rule agrees between `review-gauntlet.md` lines 39–42 and `loop-control.md` lines 220–221.
  - The validation-only no-change outcome (`SKILL.md` lines 108–110) is linked from external `SKILL.md` lines 90–91.
- [x] **Every new cross-reference anchor resolves:** `#comment-trigger-gate`, `#azure-merge-and-named-tests`, `#github-snapshot`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#waiting-for-azure-pipelines`, `#validation-only-handoff` and `#evidence-required-before-triggering`.
- [x] **The change follows repository conventions.**
  - The 17 paths are limited to `.github/skills/**` and `tools/ci/tests/**`. `pipeline.yaml` lines 112–126 (the existing CIHelpers job) and workflows are unchanged.
  - Version mentions appear only outside `AGENTS.md`/`CONTRIBUTING.md`, as `AGENTS.md` permits.
  - The MIT/Microsoft header is on line 1 of all new scripts and tests.
  - Tests use the existing `tools/ci/tests` pattern, such as `spec_from_file_location` with temporary `sys.path` insertion (`test_collect_ci_evidence.py` lines 22–33).
  - Tests are hermetic:
    - `--help`/import-only subprocess checks (lines 988–1038);
    - the readiness harness uses `pwsh -NoProfile -NonInteractive` with a 30-second timeout and a fake global `gh`, and asserts there are no unexpected trigger or review-request side effects (`test_get_pr_readiness.py` lines 249–302).
  - The TF_BUILD skip-versus-fail behavior is documented in `ci-triage.md` lines 98–106.
- [x] **Candidate observations I evaluated and did not raise:**
  - The duck-typed `collect_build(client, args)` contract shared by the collector and watcher is a refactor preference.
  - The constant `revisionScope`/`mergeReady` output fields are a naming matter.
  - In `review-gauntlet.md` lines 33–35, the TaskName sentence is unambiguous when read with lines 21–23 and 39–42.
  - `-RunPipeline` constraints are restated in `SKILL.md` step 9 and the script help, but the statements agree with the canonical gate.
  - The fallback for reused fork branches blocks association by design, and this gate is intentionally retained.
  - The PowerShell `.DESCRIPTION` wording about the "newest" review appears to predate this PR and does not affect behavior.
- [ ] **Test and Black runs were not reproduced.** This was a read-only review with no command execution. The caller's validation claims stand as provided.
- [ ] **CIHelpers headroom on a hosted agent is unproven.** The job has `timeoutInMinutes: 15` (`pipeline.yaml` line 114). The last full-directory local Linux baseline was 486.94 seconds and predates the latest test additions. Only a current-candidate Azure run can confirm headroom. This is a residual risk, not a defect.

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
