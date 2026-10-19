---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 47
stage_attempt: 1
round: 2
timestamp: "2026-10-18T11:02:25Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "47da4e23d22963a030180e08e01e764675786a99b7e141e67eb39f368a8d32fd"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-47-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **The modules form a one-way chain with one source of truth.**
  - `verify_trusted_guidance.py` uses only the standard library, so it can run under `python -I`.
  - `collect_ci_evidence.py` L21 imports `GuidanceError` and `trusted_executable`. Its `command_json` function (L39-57) converts those errors to `EvidenceError`.
  - `watch_azure_pipeline.py` L17-27 imports the collector's constants (`REPOSITORY`, `AZURE_PROJECT_ID`, `MAX_BUILD_ID`), `AzureClient`, `verify_merge` and `collect_build`. It converts `EvidenceError` to `MonitorError` at L148-151.
  - Neither script duplicates the Azure client or the build-source checks.
- [x] **The watcher supplies every field the collector functions read.**
  - `verify_merge` (L167-179), `verify_build` (L182-220) and `collect_build` (L398-439) read `build_id`, `pull_request`, `head_sha`, `base_sha` and `merge_sha`.
  - The watcher's `parse_args` (L277-319) provides all five. It requires full 40-character base and merge SHAs whenever `--azure-timeline` is set.
  - `allow_skipped_job` is read with `getattr(..., [])` (L404), so the watcher has no skipped-job exemptions by design. `references/ci-triage.md` L61-66 documents this.
- [x] **The comment-trigger code matches every place the docs describe it.**
  - `Get-PrReadiness.ps1` enforces:
    - argument checks at L104-110: one PR, both expected SHAs, and `-WaitForReview`/`-RequestReview` refused;
    - complete-evidence and open-PR checks at L612-620;
    - a trigger only when the exact Azure check is missing (L625);
    - `pipelineRunRequested` taken from the POST exit code, with warning-only receipt handling and no retry (L625-647).
  - This matches:
    - `references/ci-evidence.md` (comment-trigger gate L94-124, including L105);
    - `SKILL.md` steps 7 and 9;
    - `ci-triage.md` L19-23;
    - `ci-delegation.md` step 3.
  - The other `/azp run` mentions only forbid triggering or describe verification; none is a second trigger route:
    - `contributor-safety.md` L26/L76
    - `ci-delegation.md` L8/L49/L93
    - `readiness-gates.md` L94
    - `trusted-guidance.md` L68
    - `branch-spark4-common.md` L94
- [x] **The readiness states are separate and backward compatible.**
  - The output object in `Get-PrReadiness.ps1` (L679-694) reports `evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval`, `merge` and `unassessedGates` separately.
  - `completeness.complete` is kept as a compatibility alias, so the existing output shape doesn't change.
  - How to read these states is explained once in `references/ci-evidence.md` and linked from `SKILL.md` step 1.
- [x] **The watcher's outcomes and exit codes match the docs.**
  - Outcomes are at `watch_azure_pipeline.py` L189-256. The exit-code map is at L357-367: `azure_completed` and `attention_required` return 4, timeout 124, interrupted 130.
  - These agree with `references/ci-triage.md` L45-66.
  - Timeline mode keeps the same polling interval and kickoff deadline; it does not add a second watcher.
- [x] **The collector's output matches its docs.**
  - `main` (L684-713) emits `collectionStable`, `revisionScope`, `validationPassed` and a constant `mergeReady: false`. The constant is tested at `tools/ci/tests/test_collect_ci_evidence.py` L1010-1011.
  - `collect_build` emits `timelinePending`, `jobGatesPassed`, `allowedSkippedJobs` and `unresolvedJobs`.
  - All of these match `references/ci-evidence.md` L126-216.
- [x] **Detail moved into references, and links resolve.**
  - `SKILL.md` step 8 now just links to `references/review-gauntlet.md`, and `SKILL.md` is 314 lines.
  - New cross-skill links use the existing `../../synapseml-pr-loop/references/...` pattern, as in `ci-delegation.md` and `synapseml-branches/references/branch-spark4-common.md` L100-103.
  - These anchors exist:
    - `ci-evidence.md#comment-trigger-gate` and `#azure-merge-and-named-tests`
    - `loop-control.md#evidence-invalidation`, `#artifact-retention` and `#state-and-cost-discipline`
    - `ci-triage.md#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`
    - `SKILL.md#validation-only-handoff`
- [x] **The change follows AGENTS.md.**
  - Nothing changes in `pipeline.yaml`, `.github/workflows/`, release tooling, dependency pins, `AGENTS.md`, `CONTRIBUTING.md`, or product/JVM APIs.
  - The existing CIHelpers job (`pipeline.yaml` L112-126) already runs everything under `tools/ci/tests/`, so the new test files run without pipeline changes.
  - Every new Python file has the repository license header.
  - Black is the only Python linter configured (`pyproject.toml`).
  - `GOVERNANCE_PREFIXES` in `tools/ci/e2e_impact.py` exempts only Markdown under `.github/skills/`, so the changed scripts still trigger E2E tests.
- [x] **Tests go through the public entry points.**
  - They call `main()` or launch `pwsh`, loading scripts with importlib, a scoped `sys.path` and bytecode writing turned off. This meets AGENTS.md's rule that a helper-only test is not proof.
  - Under `TF_BUILD`, `test_get_pr_readiness.py` fails instead of skipping when `pwsh` is missing.
  - The receipt regression test (about L901-929) checks for exactly one POST and that the receipt comes back unchanged.
- [x] **Considered and rejected as defects:**
  - **Watcher passing its argparse namespace to the collector:** the watcher supplies every field and the contract is documented.
  - **Repeated `completeness.complete` and `-RunPipeline` statements:** each agrees with the canonical gate. The step 9 rewrite corrects stale "external PRs only" text, because the helper now refuses for every PR, so it is a necessary fix rather than filler.
  - **CIHelpers timing against its 15-minute limit:** no hosted run shows a problem, and pipeline edits are out of scope.
- [ ] **I did not run any tests (read-only review).**
  - The Windows and Linux `test_get_pr_readiness.py` results (39 tests, 117 subtests), Black and `git diff --check` are evidence supplied by the caller.
  - The 370-test full-directory run predates the latest receipt change, as the caller disclosed.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4827 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 47,
  "completedPriorPasses": 46,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver disposition

Original feedback preserved. Observed model, full-input coverage, read-only
isolation, and public safety were verified. Reviewed patch SHA-256:
`2f9fc76614f340012f3a015c599c78993cfaaa007d231033d94b76481faa08af`.
This round was clean. A test change after round 5 invalidated the pass;
round 6 did not run. This is not qualifying final review or CI evidence.
