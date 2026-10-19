---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 58
stage_attempt: 1
round: 2
timestamp: "2026-10-19T04:10:54Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "0ca0eca801d6b39eb3ac6b099f20799cb3253fadd76f9acf0a142a203a6f7d83"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-58-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **The comment-trigger gate is defined once and every comment route links to it.** The definition is the `### Comment-trigger gate` section at `.github/skills/synapseml-pr-loop/references/ci-evidence.md` line 94. Routes that link to it rather than copying it:
  - `SKILL.md` step 7, lines 213–215
  - `references/ci-triage.md` lines 16–17
  - external `references/ci-delegation.md` step 3
  - `references/readiness-gates.md` around line 96

  A hand-posted requeue (`ci-evidence.md` lines 110–114) needs a fresh snapshot with both expected SHAs matching and `readiness.evidenceComplete=true`. The Build Update `retry=true` path (`ci-triage.md` lines 193–216) is an API retry under the one-retry budget, not a comment route. The gate stays as strict as before.
- [x] **The code enforces the gate as documented.** In `scripts/Get-PrReadiness.ps1`:
  - Lines 103–111: the expected SHAs apply to one PR only, and `-RunPipeline` requires both SHAs and refuses `-WaitForReview`/`-RequestReview`.
  - Line 613: it refuses when `evidenceComplete` is false and lists the gaps.
  - It posts only when the `microsoft.SynapseML` aggregate is absent.
  - The receipt fields `id`/`html_url`/`created_at` match `ci-evidence.md` line 80.
- [x] **Engineering, human and merge gates are reported separately.** `Get-PrReadiness.ps1` lines 559–580 and 683–697 produce `evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval`, `merge` and `unassessedGates`. These match the table at `ci-evidence.md` lines 73–81. `completeness.complete` (line 713) equals `$remoteGatesClear`, as both `ci-evidence.md` line 81 and `readiness-gates.md` lines 127–131 state. `humanApproval` is computed on its own, and `merge` never authorizes a merge.
- [x] **Each policy has one source, which follows AGENTS.md "prefer a link to the source of truth".**
  - Step 8 of `SKILL.md` (lines 259–262) now links to `references/review-gauntlet.md` instead of keeping an inline copy.
  - The no-bloat contract lives only in `readiness-gates.md#user-value-and-scope`, linked from step 4 (lines 150–151).
  - `loop-control.md` lines 53–69 keep AGENTS.md's preservation of attempt/round/model/SHA/resolution: superseded files are pruned only into reachable archive commits listed in an index. Deleting the only copy or pruning unresolved findings is still forbidden.
- [x] **All cross-file anchors resolve.** Checked: `#github-snapshot`, `#comment-trigger-gate`, `#azure-merge-and-named-tests`, `#waiting-for-azure-pipelines`, `#targeted-infrastructure-retry`, `#artifact-retention`, `#state-and-cost-discipline`, `#evidence-invalidation`, `#user-value-and-scope`, `#evidence-required-before-triggering` and `#validation-only-handoff`. The external `SKILL.md` links at lines 18–20 and 90–91 point to the shared pr-loop sources and don't copy them.
- [x] **The helpers depend on each other in one direction without cycles.** `watch_azure_pipeline.py` imports the collector, and `collect_ci_evidence.py` imports `trusted_executable`/`GuidanceError` from `verify_trusted_guidance.py`. The watcher's timeline mode reuses `collect_build` rather than a second implementation. The constants match `ci-triage.md` and `ci-evidence.md`:
  - 600-second polling
  - 120-minute deadline counted from kickoff
  - definition 17563
  - reason `pullRequest`/`manual`

  `--repo` is restricted to microsoft/SynapseML. Redirects are refused and the token is kept only in memory.
- [x] **Guidance docs and tests are tied together.** `verify_trusted_guidance.py` `REQUIRED` includes root `CODEOWNERS` and `pipeline.yaml`, and both exist. `tools/ci/tests/test_trusted_guidance.py` reads the `python -I -B` bootstrap and the `-E -s -B` helper command lines from the docs, so the two cannot drift apart silently. `trusted_executable` skips PATH entries at or under the current directory.
- [x] **The change follows AGENTS.md.**
  - Nothing changes in `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins or product APIs.
  - The four suites sit in `tools/ci/tests/`, so the existing CIHelpers job runs them (`pipeline.yaml` lines 112–126).
  - Tests load each script by path with `importlib` and `dont_write_bytecode`, matching the helpers' `-B` contract.
  - The readiness suite is skipped only when `pwsh` is missing outside `TF_BUILD`.
  - The `.py`/`.ps1` helpers under `.github/skills/` are not on the governance E2E skip list (`tools/ci/README.md` line 110), so changing them still runs the full E2E set.
- [x] **Candidate concerns rejected as non-defects:**
  - `getattr(args, "allow_skipped_job", [])` couples the watcher and collector, but it is documented and keeps the watcher free of skip exemptions. A dataclass would be a speculative abstraction.
  - The script-scope `$PSNativeCommandUseErrorActionPreference = $false` (line 100) follows the existing `$ErrorActionPreference` pattern. The comments at lines 195 and 302 are about StrictMode only.
  - `tools/ci/README.md` line 144 describes CIHelpers in general terms and is still accurate.
- [ ] **CIHelpers run time on hosted agents against `timeoutInMinutes: 15` is not verified.** I can't check this read-only. The supplied numbers are local only: 486.94 s for the whole directory before the latest readiness additions, and 160–220 s for the readiness suite. A timeout would show up as a red CIHelpers result in the final CI-qualified pass, so it is not reported as a defect.
- [ ] **I did not run any tests.** All results above come from the validation evidence supplied with the request.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 5157 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 58,
  "completedPriorPasses": 57,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
