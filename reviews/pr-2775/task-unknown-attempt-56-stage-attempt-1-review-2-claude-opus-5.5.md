---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 56
stage_attempt: 1
round: 2
timestamp: "2026-10-19T00:08:22Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "8dc23daf892e72d56b23e5e8456091f309b208bf90e2846bf586e60a031b3c1b"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-56-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Helper layering and error handling:** Imports run one way, from the watcher to the collector to the verifier, with no import cycles and no new shared module.
  - `collect_ci_evidence.py:21` imports only `GuidanceError` and `trusted_executable`.
  - `command_json` (lines 74–96) converts `GuidanceError` to `EvidenceError` at line 93.
  - The watcher imports collector primitives at `watch_azure_pipeline.py:17–27`. It converts `EvidenceError` to `MonitorError` at lines 92 and 151.
  - This structure agrees with the no-bloat gate.
- [x] **Separate readiness states:** `Get-PrReadiness.ps1` computes `evidenceComplete`, `remoteGatesClear` and `humanApproval` separately at lines 559–578.
  - The `readiness` object is built at line 683.
  - `completeness.complete` is only a compatibility alias, at line 713.
  - These values match the output table in `ci-evidence.md` and the script's comment-based help.
- [x] **Comment-trigger gate has one source:** `ci-evidence.md` defines `### Comment-trigger gate` at line 94.
  - `SKILL.md:214`, `ci-triage.md:17`, `readiness-gates.md:101` and `ci-delegation.md:70` all link to it.
  - The code guards at `Get-PrReadiness.ps1:109–110` and `:613–626` match the documented gate. The conservative complete-collection requirement is retained.
  - The remaining `/azp run` mentions are preconditions or handoffs, not new trigger routes: `contributor-safety.md:26,76`, `branch-spark4-common.md:94`, and the external-contributor `SKILL.md` lines 41 and 66.
- [x] **Cross-file anchors:** All 20 `.md#anchor` links in the two skills resolve to existing headings. Examples include `#validation-only-handoff`, `#user-value-and-scope`, `#artifact-retention`, `#evidence-invalidation`, `#evidence-required-before-triggering` and `#azure-merge-and-named-tests`.
- [x] **Moved content is not duplicated:**
  - Step 8 of `SKILL.md` now delegates to `references/review-gauntlet.md`. That file matches the AGENTS.md rules for artifact placement: `reviews/pr-<number>/` once a PR exists, and session drafts before then.
  - The no-bloat contract is defined once, in the "User value and scope" section of `readiness-gates.md`. Retention rules are left to `loop-control.md#artifact-retention`.
- [x] **Docs and code agree:**
  - `collect_build` (line 436) reads `allow_skipped_job` only from the collector's own arguments (line 443). This matches `ci-triage.md:64–68`, which says the watcher has no skipped-job exemptions.
  - The watcher's exit-code map in `main` (lines 322–368, including `azure_completed: 4` at line 364) matches `ci-triage.md:55–57`.
  - The collector's 600-second deadline (`main`, line 723 onward) and the 15-second Windows cleanup limit in `run_windows_batch` (lines 39–71) match `ci-evidence.md`.
- [x] **Trusted-guidance design:** The verifier's `GUIDANCE_PATHS` and `REQUIRED` lists match `trusted-guidance.md`.
  - The verifier itself runs with `-I -B`; the collector and watcher run with `-E -s -B`, so they can still import their sibling scripts.
  - Each script sets `sys.dont_write_bytecode` before those sibling imports, so the verified directory stays clean.
- [x] **Repository conventions:**
  - All new Python files carry the MIT header.
  - The tests reuse the existing `spec_from_file_location` plus `sys.path.insert` pattern: `test_watch_azure_pipeline.py:19,29`, `test_collect_ci_evidence.py:22,28` and `test_trusted_guidance.py:15`.
  - The readiness tests' `skipUnless(pwsh or TF_BUILD)` guard (`test_get_pr_readiness.py:249–250`) matches the documented rule: fail in CI, skip locally.
  - The tests run unconditionally in the unchanged `CIHelpers` job (`pipeline.yaml:112–126`). No pipeline, workflow or dependency-pin change was needed.
- [ ] **Test execution not verified by me:** I did not run any tests; my mandate is read-only. I relied on the supplied pytest results from Windows and Linux and the supplied Black and PowerShell checks. How long the `CIHelpers` job takes on a hosted agent, against its 15-minute timeout, is not yet observed, because no Azure run for this candidate has been claimed.

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
