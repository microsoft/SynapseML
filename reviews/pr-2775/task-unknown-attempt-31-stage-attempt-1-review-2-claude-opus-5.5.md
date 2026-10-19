---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 31
stage_attempt: 1
round: 2
timestamp: "2026-10-17T19:47:45Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0eeb7510a32fcb389771614c832b361afe4fc64a11d11a2b17344d177ca29e85"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-31-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the PR-loop and external-contributor `SKILL.md` changes:
  - Both `compatibility` blocks and the trusted-guidance bootstrap.
  - Stage 4 no-bloat gate (lines 149-154) and stage 7 collector/watcher guidance (lines 238-253).
  - Stage 8's move into `references/review-gauntlet.md`.
  - The stage 9 `-RunPipeline -ExpectedHeadSha/-ExpectedBaseSha` command.
- [x] Reviewed `ci-evidence.md`, `trusted-guidance.md`, `review-gauntlet.md`, `readiness-gates.md`, `loop-control.md`, `ci-triage.md` and `ci-delegation.md`.
  - Every relative link and anchor added by the diff resolves.
  - The extracted gauntlet contract agrees with loop-control's invalidation and `-Execute` rules.
- [x] Read the helper scripts under `.github/skills/synapseml-pr-loop/scripts/`:
  - `collect_ci_evidence.py`: `AzureClient` 71-115, `summarize_timeline` 223-395, `collect_build` 397-439, `collect_tests` 458-475, CLI 646-710.
  - `watch_azure_pipeline.py`: timeline branch 135-175, outcomes 190-257, parser 272-315.
  - `verify_trusted_guidance.py`.
  - `Get-PrReadiness.ps1`: the separate evidence, engineering, human and merge states, and the `-RunPipeline`/`-RequestReview`/`-WaitForReview` guards.
- [x] Read the four helper suites under `tools/ci/tests/`, including fixture shapes, `dont_write_bytecode` import isolation, and the class-level `pwsh` skip at `test_get_pr_readiness.py:199`.
  - Checked `pipeline.yaml:112-125` (CIHelpers) and `tools/ci/README.md`.
  - Pipeline and workflow definitions are unchanged.
- [x] Checked AGENTS.md conventions. There are no product or JVM API changes, dependency pins, release-tool changes, or Scala/RDD code, and every new stage asks for approval before CI.
- [x] Validated and rejected these candidates:
  - The watcher exiting 0 while `attentionRequired` is set matches its documented contract.
  - Truncated review threads blocking `-RunPipeline` fail closed, as documented.
  - The GitHub/Azure disagreement check uses the build-level `result`, so it raises no false `attention_required`.
  - An unused `baseRefName` key in the shared `snapshot()` fixture (`test_watch_azure_pipeline.py:63`) could not be attributed to this diff; about 40 apparently pre-existing call sites use that fixture.
- [x] No commands or tests were executed. The supplied pytest, Black and diff-check results were reviewed as given, not re-run. `reviews/` and session-state were not accessed.

## Issues

### Issue 1: New helper test suites and the readiness suite's PowerShell requirement aren't documented in the skill
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/references/ci-triage.md`
- **Line(s)**: 85-87; also `references/loop-control.md:215` and `tools/ci/tests/test_get_pr_readiness.py:199`
- **Description**:
  - The diff adds three helper suites: `test_collect_ci_evidence.py`, `test_get_pr_readiness.py` and `test_trusted_guidance.py`.
  - The skill still points only to the watcher suite (`ci-triage.md:85-87`). A grep of `.github/skills` finds no other test-file or `pytest` reference.
  - `loop-control.md:215` says to run "existing helper regressions when helpers change" without saying where they are.
  - `PrReadinessTests` is gated by `@unittest.skipUnless(shutil.which("pwsh"), "PowerShell 7 is required")`. Without `pwsh` on PATH, all of `Get-PrReadiness.ps1`'s regression coverage turns into skips.
  - The script itself declares `#Requires -Version 7.2`, which doesn't match the "PowerShell 7" message.
  - The supplied Linux validation had to install PowerShell separately, so the skip path is realistic for local runs. Hosted Ubuntu CI images include `pwsh`.
- **Risk**: An agent changing the collector, verifier or readiness script runs only the documented watcher suite. It could also report a run where every readiness test skipped as a pass. That contradicts `readiness-gates.md:119` ("a skipped required scenario is a blocker") and AGENTS.md's "inspect skipped tests".
- **Suggested Fix**:
  - Turn the note at `ci-triage.md:85-87` into one short note that names all four suites with a focused command: `python -B -m pytest tools/ci/tests/test_watch_azure_pipeline.py tools/ci/tests/test_collect_ci_evidence.py tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_trusted_guidance.py -q -rs`.
  - State that the readiness suite needs PowerShell 7.2+ on PATH and that its skip is not a pass.
  - Change the skip message to "PowerShell 7.2+ is required".
  - No pipeline change is needed.

### Issue 2: `collect_build` reads its job-skip allowlist from whichever CLI's arguments it receives, so watcher output reports job-gate results the watcher can't control
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**: 397-439 (`getattr` at 404; gate fields at 436-438), 470-475; `watch_azure_pipeline.py` 135-175, 190-257
- **Description**:
  - `collect_build(client, args)` gets its skip allowlist with `getattr(args, "allow_skipped_job", [])`.
  - Only the collector's parser defines `--allow-skipped-job` (line 668). The watcher passes its own argparse result at line 147, which has no such option, so the empty default always applies.
  - The watcher puts the whole result into its timeout, `azure_completed`, `attention_required` and `success` payloads, including `allowedSkippedJobs: []`, `unresolvedJobs` and `jobGatesPassed`.
  - `e2e_impact.py` can skip seven notebook E2E jobs (`tools/ci/README.md:101-103`). For such a build, an exit-0 `success` result reports `jobGatesPassed: false` and lists those jobs as unresolved, and no watcher option can declare the skips.
  - The watcher's own decisions read only `attentionRequired`, `status` and `result`, and no watcher test checks the gate fields.
  - `collect_tests` line 474 uses `job.get("timelineType", "Job")`, but `summarize_timeline` always sets that key (line 373).
    - The default exists only because most tests pass raw records from the `job()` fixture (`tools/ci/tests/test_collect_ci_evidence.py:64-72`). Only lines 559-566 use the real summary shape.
    - A missing key falls back to "Job", the only type that counts as test-run attempt evidence.
- **Risk**: A diagnostic tool reports a gate failure that contradicts its own success outcome and can never pass. Because the shared functions accept loosely shaped inputs with silent defaults, the two CLIs and the test fixtures can drift from the production data shape without any test noticing.
- **Suggested Fix**:
  - Make the allowlist an explicit keyword parameter, e.g. `collect_build(client, args, allowed_skipped_jobs=())`, set only by the collector's `main`, and remove the `getattr`.
  - Have the watcher drop `allowedSkippedJobs`, `unresolvedJobs` and `jobGatesPassed` from its `azure` payload. Alternatively, state in `ci-triage.md` that these fields are not authoritative and point to the collector.
  - Index `job["timelineType"]` directly and add `"timelineType": "Job"` to the `job()` fixture.
  - None of this needs a new abstraction.

### Issue 3: The skills' `compatibility` metadata leaves out tools the new steps require
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/SKILL.md`
- **Line(s)**: 9-11; also `.github/skills/synapseml-external-contributor-review/SKILL.md:8-9`
- **Description**:
  - **PR loop:** The diff edits this `compatibility` line to "PowerShell 7.2+" but doesn't add the Azure CLI.
    - Stage 7 now requires the CI evidence collector (SKILL.md 251-253).
    - The collector's `AzureClient` always gets its token through `az account get-access-token` (`collect_ci_evidence.py:96-110`) and has no anonymous fallback.
    - `ci-evidence.md:180` confirms that authentication uses the existing Azure CLI login. "Network access to Azure Pipelines" is not the same as a logged-in `az`.
  - **External-contributor skill:** Its `compatibility` lists only git, GitHub CLI and pipeline access.
    - The bootstrap the diff now requires (line 17) runs `python -I -B <trusted-script>` (`trusted-guidance.md:18`) before any other skill loads.
    - The new `ci-delegation.md:78-80` sends evidence collection to the Python, Azure-CLI-based collectors.
- **Risk**: Skill-selection metadata understates what the environment needs. An agent without an `az` login, or without Python for the external skill, loads the skill and fails partway through at a required evidence step instead of reporting the missing prerequisite up front.
- **Suggested Fix**: Add "Azure CLI (logged in)" to the PR loop's `compatibility`. Add "Python" and "Azure CLI" to the external-contributor skill's `compatibility`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4371 added and 158 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 31,
  "completedPriorPasses": 30,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`c342d7e40ed462e5f4b6f288604f16f5c74cd46363579ad1b8ad99389e0b637e`.
Round 1 was clean; round 2 ended this pass. Neither qualifies the revised patch.
The reviewed diff SHA-256 was
`1ff9cba6fae1f79fb1dcfe75ad77f85331c4d113727af2b08c03582e7f95a194`;
the accompanying attempt-31 manifest records the complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
Both transports used the observed `claude-opus-5.5` model and only read-only
source, policy, and test tools. Narrow source globs did not expose review
artifacts, and neither reviewer accessed prior or sibling feedback.

- **Issue 1: Fixed.** CI triage now documents all four helper suites, the
  PowerShell 7.2+ prerequisite, and the fact that missing-PowerShell skips do
  not establish readiness coverage. The skip diagnostic names the same version.
- **Issue 2: Clarified; unnecessary refactors rejected.** CI triage explicitly
  distinguishes watcher success (the named check passed) from its diagnostic
  job gates, which have no skipped-job exemptions. The collector owns intentional
  skip assessment and actual test evidence. Keeping these diagnostics visible
  avoids losing evidence; a false job gate is not a contradiction or a bypass.
  The shared collector defaults to no exemptions, which is conservative.
  No demonstrated failure requires a new keyword parameter or changed payload.
  The `timelineType` fallback is internal: the public collection path always
  supplies `summarize_timeline` output with that field, including typed leaf
  containers. It does not allow unvalidated Azure records to become passing
  test evidence. Changing fixture shapes and this fallback solely to restate
  that invariant would not fix a public behavior defect.
- **Issue 3: Fixed.** Both skills' compatibility metadata now names the logged-in
  Azure CLI; the external-contributor skill also names Python for its bootstrap.

Only documentation, metadata, and a test skip message changed after the prior
validation. The production helpers and test assertions are unchanged:

- The prior Windows and Linux affected-helper runs both passed 42 tests and
  78 subtests, with 29 deselected; commands and results are preserved in attempt 30.
- A local subprocess with `PATH` set to an empty temporary directory ran
  `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py -k unsafe_pipeline_requests -rs --tb=short`.
  It reported exactly one skip with `PowerShell 7.2+ is required` and 31 deselected.
  This confirms the diagnostic, not readiness coverage.
- `python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py`
  passed; the file was unchanged.
- `git diff --check` passed.

No current-head Azure validation or final CI-qualified pass is claimed.
