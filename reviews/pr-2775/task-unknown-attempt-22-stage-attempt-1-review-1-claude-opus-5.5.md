---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 22
stage_attempt: 1
round: 1
timestamp: "2026-10-17T13:58:14Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "32e2a46ea02e519b04a5057b2d22e914a557e9173f38c0cd0e413cef0c73f261"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-22-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py` in full:
  - Checks on merge-parent order and build identity (about L168–210).
  - `$skip`/`$top` pagination with a guard against repeated pages.
  - In `collect_tests`: job-attempt provenance and required publications per job/suite.
  - The 600-second deadline, the stability recheck, and the `validationPassed` conjunction (around L681).
- [x] Read `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1` in full:
  - `#Requires -Version 7.2`, plus the SHA and `-RunPipeline` guards (L94–99, L581–600).
  - Collection errors are recorded for failed or empty REST pages (L226–259).
  - The workflow-run PR association is checked against the base repo URL and the head SHA (L261–268).
  - The compare uses immutable SHAs (L305–321), and the paginated run count is checked (L363–369).
  - Fork fallback (L376–399) and the revision recheck after collection (L507–522).
  - `evidenceComplete`, `remoteGatesClear` and `humanApproval` are kept separate (L523–541).
- [x] Read `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`:
  - Host, path and project allowlist, and an int32 bound on build IDs (L46–74).
  - The deadline is anchored to kickoff and cannot be extended (L99, L311–312).
  - Each outcome has its own exit code.
- [x] Read `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`:
  - Git isolation: `GIT_*` variables are stripped, with `--no-replace-objects` and `core.fsmonitor=false`.
  - HEAD must equal the expected SHA.
  - The tree may contain blobs only, and the index must be stage-0 and equal to the tree.
  - Untracked and ignored files are detected, and symlinked ancestors are rejected.
  - PATH entries relative to the current directory are excluded when finding executables.
- [x] Confirmed that the `GUIDANCE_PATHS`/`REQUIRED` paths (L15–33) exist: `AGENTS.md`, `CONTRIBUTING.md`, `CODEOWNERS`, `pipeline.yaml` and `templates/`.
- [x] Confirmed that every new anchor resolves:
  - `loop-control.md`: checkpoint, state/cost and evidence-invalidation sections.
  - `ci-triage.md`: waiting and targeted-retry sections.
  - `ci-delegation.md`: evidence-before-trigger section.
  - `ci-evidence.md`: GitHub snapshot section.
- [x] `pipeline.yaml` has top-level jobs and no stages (for example `CIHelpers` at L112 and `UnitTests` at L807), so `Phase.Job` identifiers match. Workflows triggered by `pull_request_target` run on the base SHA, so leaving them out of the head-SHA query is correct.
- [x] Scope: no product API, dependency pin, release tool, `pipeline.yaml` or `.github/workflows/` change. `references/review-gauntlet.md` keeps the pre-commit and final CI-qualified passes, frozen-patch identity, budgets and publication rules.
- [x] Compared the stage-hint logic (L308–347) with real task names in `pipeline.yaml` (L121, L126, L736, L740) and `templates/fabric_kv.yml` (L15, L28), and with the regression in `tools/ci/tests/test_collect_ci_evidence.py` (L567–597). This led to Issue 1.
- [x] Compared the container skip (L278–281) and issue aggregation (L290–304) with `ci-evidence.md` (L143–144) and the fixtures (L62–70, L521–540). This led to Issue 2.
- [ ] Not re-run: the driver's test results (29/75, 98/217 and 321/21 skipped). This round is read-only.
- [ ] Not verified: the PowerShell readiness tests on Linux. The `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu (L112–126), where `pwsh` is usually available, so these tests may run on Linux for the first time. From reading the code, the harness looks platform-neutral.
- [ ] Not verifiable with file-only tools: the live GitHub CLI and Azure observations.

## Issues

### Issue 1: The failure-stage hint labels real setup-step failures as `test_execution`
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 308–347 (pattern at 311–312; setup branch at 336–345; fallback at 346–347)
- **Description**: The code treats any task whose name contains the word "test" as a test step. Several real setup steps match:
  - `Install CI test dependencies` (`pipeline.yaml` L121, in the `CIHelpers` job that runs this PR's tests)
  - `Prepare for tests` (L736)
  - `Get Fabric Test Credentials/Certificate from Key Vault` (`templates/fabric_kv.yml` L15/L28)

  When one of these steps fails, the `setup_before_tests` precondition is false, because it requires every test step to have not run. The fallback then returns `test_execution`. The only regression uses the name `Install environment`, which the pattern never matches.
- **Risk**: This is diagnostic only, and the gate still fails closed. However, it sends triage of Key Vault, credential or pip failures toward test execution. That contradicts `ci-evidence.md` L159–160 and weakens infrastructure classification.
- **Suggested Fix**: Exclude setup words from the pattern (`prepare|install|setup|credential|certificate|key vault|dependenc`), or match only the names of actual test-runner steps. Add a regression that uses these real step names.

### Issue 2: Warnings on Stage/Phase ancestors bypass the automatic job gate
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py
- **Line(s)**: 278–304; consumed at 383–393 and 412–415
- **Description**: The code skips any container that has a job beneath it (L280–281). It counts issues only from the leaf job and that job's direct tasks (L290–304). As a result, a container's own `issues`, `warningCount` and `errorCount` never reach `issueCount`, `unresolvedJobs`, `jobGatesPassed` or `attentionRequired`. `ci-evidence.md` L143–144 says, without qualification, that timeline warnings, errors and untyped issues block the gate even when Azure reports success. No test fixture has a container with issues.
- **Risk**: A build can succeed while a container carries a warning. In that case `validationPassed` can be true without the manual disposition the spec requires. How often Azure attaches issues to containers is unconfirmed.
- **Suggested Fix**: Merge nonzero ancestor counts into each descendant job during the parent walk (L266–276), or report containers that have issues as unresolved. Add a fixture with a warning on a Phase above a succeeded Job, and expect `jobGatesPassed=false`. Alternatively, narrow the documented claim.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Excluded setup, dependency, and credential-retrieval names
  from the diagnostic test-step heuristic.
- **Why**: Real setup tasks containing "test" must not be classified as test
  execution when a later test runner never started.
- **How verified**: Four real-name cases failed before the fix. The complete
  collector/watcher suite now passes: `python -B -m pytest -q
  tools/ci/tests/test_collect_ci_evidence.py
  tools/ci/tests/test_watch_azure_pipeline.py --tb=short` — 72 tests and
  159 subtests passed. Existing publication-stage cases remain covered.

### Issue 2
- **Status**: Fixed
- **What changed**: Included each current job's actual timeline ancestors in
  issue aggregation, reusing the cycle-checked parent traversal.
- **Why**: A Stage/Phase warning, error, or untyped issue must not disappear
  merely because that container has a Job child.
- **How verified**: Six ancestor-issue cases failed before the fix and pass
  in the collector/watcher command above. Coverage also confirms that a clean
  retry does not inherit a warning from an earlier, unreferenced Phase attempt,
  and diagnostic messages stay out of evidence JSON.

The original artifact was retained before this resolution update; SHA-256:
`0656c0b6b78d20d18992ad1d5c1cb1d6f8daeb46908a525fb85b3fb8c3061096`.
Black 22.3.0 formatted both changed Python files. This pass remains
`issues_found`; it does not qualify the revised source.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4022 added and 146 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 22,
  "completedPriorPasses": 21,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
