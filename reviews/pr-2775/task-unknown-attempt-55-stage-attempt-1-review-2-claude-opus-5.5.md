---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 55
stage_attempt: 1
round: 2
timestamp: "2026-10-18T22:24:32Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "42c93fff7c6a7ac085a1e9b27dfaf4648d4a1dc0f75f6e024349f8f3758ae5df"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-55-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage.** The inline diff was cut off after the `SKILL.md` hunks, so I reviewed the current contents of all 17 changed paths instead. That is 13 files under `.github/skills/` and the four suites in `tools/ci/tests/`. The `SKILL.md` hunks I could see match the files.
- [x] **One home for each rule.** Each shared rule is written once and the other files link to it:
  - **Comment-trigger gate:** defined in `references/ci-evidence.md` lines 94–124. It is linked from:
    - `SKILL.md` lines 213–215 and 227–228;
    - `readiness-gates.md` lines 100–101;
    - `ci-delegation.md` lines 69–74.
  - **No-bloat gate:** defined in `readiness-gates.md` lines 20–28, including the `loop-control.md#artifact-retention` link. `SKILL.md` links to it at lines 150–151.
  - **Six-round gauntlet:** moved to `references/review-gauntlet.md`. `SKILL.md` lines 259–262 now just link to it.
  - **Validation-only handoff:** the external-contributor `SKILL.md` links to it at lines 90–91.
  - Every anchor link resolves to a real heading.
- [x] **Readiness helper code matches the docs.** In `Get-PrReadiness.ps1`, `-RunPipeline` is guarded at:
  - lines 109–110: one PR, both expected SHAs, and no `-WaitForReview` or `-RequestReview`;
  - lines 613–619: `evidenceComplete` must be true;
  - line 621: the PR must be OPEN;
  - lines 626–648: it posts only when the `microsoft.SynapseML` check is absent (case-sensitive), and records the receipt.

  The output object at lines 684–713 matches the table in `ci-evidence.md` lines 73–81.
- [x] **Python helper structure.** The import chain is watcher → collector → verifier (`watch_azure_pipeline.py:17`, `collect_ci_evidence.py:21`).
  - The verifier uses only the standard library.
  - Launch flags are consistent across the docs: `python -I -B` for the verifier and `-E -s -B` for the helpers (`trusted-guidance.md:18`, `ci-triage.md:31`, `ci-evidence.md:140`).
  - The verified guidance paths (lines 15–23 of `verify_trusted_guidance.py`) include `AGENTS.md`, `CONTRIBUTING.md`, `.github/skills`, `CODEOWNERS`, `pipeline.yaml` and `templates`.
  - Watcher outcomes and exit behavior match `ci-triage.md` lines 28–97.
- [x] **AGENTS.md conventions are followed.**
  - No changes to product APIs, `pipeline.yaml`, workflows, release tooling or dependency pins.
  - The new suites run in the existing, unchanged CIHelpers job (`pipeline.yaml` lines 112–126).
  - They follow the `unittest.TestCase` style with a patched clock used in `test_watch_azure_pipeline.py` lines 75–91.
  - GitHub and Azure calls are mocked, and no credentials are embedded.
  - The `pwsh` handling (skip locally, fail under `TF_BUILD`) is documented in `ci-triage.md` lines 99–105.
- [x] **No stale or conflicting guidance.** Searches limited to `.github/skills` for `RunPipeline`, `/azp run`, `completeness.complete`, the helper names and launch flags found no contradictions. `completeness.complete` is described the same way, as a compatibility alias, at `SKILL.md:93`, `ci-evidence.md:81`, `readiness-gates.md:128` and `Get-PrReadiness.ps1:45`.
- [x] **Candidates considered and rejected:**
  - **Manual requeue preconditions.** The steps in `ci-evidence.md` lines 110–114 do not include the helper's OPEN check (line 621). Requeuing a closed PR has negligible impact, so this is not a concrete defect.
  - **`-Repo` difference.** The PowerShell helper accepts any `-Repo` value, but it still requires the exact `microsoft.SynapseML` check, so it fails safe.
  - **Constant fields.** The collector's always-the-same `mergeReady: false` and `revisionScope` fields are deliberate two-line markers.
- [ ] **CIHelpers 15-minute timeout headroom on hosted agents.** Not assessed here; this belongs to the performance lens. The supplied local full-directory baseline (~487 s) is not hosted-agent evidence.
- [ ] **Test execution.** Not done: this review was read-only. I inspected the tests and the supplied validation evidence but ran nothing, and no Azure run exists for this candidate.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5074 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 55,
  "completedPriorPasses": 54,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
