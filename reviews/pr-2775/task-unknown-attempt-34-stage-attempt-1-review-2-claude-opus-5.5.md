---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 34
stage_attempt: 1
round: 2
timestamp: "2026-10-17T21:35:02Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bbdfb6d22c19510c0a1abbf09c4dd05a1c9edccbeb3329e4856d3819a75f91b3"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-34-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope.** The supplied diff stops after the step-8 hunk in `.github/skills/synapseml-pr-loop/SKILL.md`. I read the other changed docs, scripts and tests from the working tree, relying on the supplied manifest claim that the tree matches the frozen index. I did not hash it myself.
- [x] **Step 8 extraction.** I compared `SKILL.md:261-270` with `references/review-gauntlet.md:1-80`. Every removed requirement is still in the reference:
  - pre-commit and final passes
  - frozen-patch identity and the explicit diff file
  - artifact placement and collision rules
  - the `-Execute` runner and the blocker before a PR exists

  These anchors resolve: `#validation-only-handoff`, `loop-control.md#evidence-invalidation`, `#checkpoint-before-yielding` and `ci-triage.md#waiting-for-azure-pipelines`.
- [x] **Helper layering.** The helpers form one chain with no cycles:
  - `verify_trusted_guidance.py` is the base layer.
  - `collect_ci_evidence.py` imports it at line 21, and its `command_json` (lines 39-42) resolves executables through `trusted_executable`.
  - `watch_azure_pipeline.py` imports the collector (lines 17-27), and its `query_pr` uses `command_json`.

  Shared constants are defined once, in the collector. `sys.dont_write_bytecode` is set only under `__main__`. The PowerShell helper's normal command discovery is a documented exception (`references/trusted-guidance.md:47-48`).
- [x] **Watcher contract.** Outcomes and exit codes (`watch_azure_pipeline.py:181-258, 343-354`) match the watcher docs in `ci-triage.md`. The collector's `getattr(args, "allow_skipped_job", [])` (`collect_ci_evidence.py:404`) affects no watcher decision, which uses only `status`, `result`, `attentionRequired` and `jobs`. Not reported: changing it would be a refactor for hypothetical future changes.
- [x] **Readiness guards.** `Get-PrReadiness.ps1:101-109` matches the step-9 text in `SKILL.md`:
  - expected SHAs and `-RequestReview` each require exactly one PR;
  - `-RunPipeline` needs both SHAs and refuses `-WaitForReview` and `-RequestReview`.

  The Azure check is always checked for provenance in `$RequiredCheck` handling (lines 86, 458-469, 620, 728). This is deliberate and documented in the parameter comment, so not reported.
- [x] **CI integration.**
  - The CIHelpers job (`pipeline.yaml:112-126`, read only) runs `python3 -m pytest tools/ci/tests/ -q`.
  - The only skip guards in `tools/ci/tests` are `test_get_pr_readiness.py:199`, which skips the whole `PrReadinessTests` class (the file's only test class), and the POSIX-only `test_trusted_guidance.py:315`.
  - No Python 3.11+-only APIs appear in the helpers or tests.
- [x] **Cross-skill duplication.** I compared `ci-delegation.md:78-80, 95-98` with `loop-control.md:247-253`, pr-loop `SKILL.md:107-109`, and the new external-skill lines 18-20 and 90-91.
- [ ] **Tests not run.** This review is read-only. The pytest and Black results come from the driver and were not reproduced here.

## Issues

### Issue 1: Change-history wording in the always-loaded step 8 summary
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/SKILL.md
- **Line(s)**: 266-268
- **Description**: Two sentences describe this PR's refactor instead of stating a rule.
  - "Extraction into a reference does not waive policy." repeats the earlier "Follow the complete … contract."
  - "The contract also replaces manual prompt assembly with the installed `-Execute` runner…" contrasts with text the skill no longer contains.

  `references/review-gauntlet.md:30-32` already states the actual rule. The PR's own no-bloat gate (`SKILL.md:150-155`) says to remove duplicate documentation and verbose filler.
- **Risk**: SKILL.md loads on every invocation, so filler costs context each time. "Replaces" will confuse later readers who never saw manual assembly and may suggest it is still a fallback. The PR also fails the gate it introduces.
- **Suggested Fix**: Delete the "Extraction…" sentence. Rewrite the other as a rule, for example: "Run reviewers only through the installed `-Execute` runner; prompt generation alone is not review evidence." Or drop it and rely on the reference.

### Issue 2: CIHelpers can silently skip the readiness suite, contrary to the PR's "skip is not a pass" rule
- **Severity**: Low
- **File**: tools/ci/tests/test_get_pr_readiness.py; .github/skills/synapseml-pr-loop/references/ci-triage.md
- **Line(s)**: test_get_pr_readiness.py:199; ci-triage.md:89-96 (context: pipeline.yaml:125, tools/ci/README.md:144-148)
- **Description**: `ci-triage.md:89` says the four helper suites run in the existing CIHelpers job, and lines 95-96 say a skipped readiness suite is not a pass. Nothing enforces either statement:
  - The only check is a class-level `@unittest.skipUnless(shutil.which("pwsh"), …)`.
  - CIHelpers runs pytest with `-q` and no `-rs`, so skip reasons are not listed.
  - A missing `pwsh` therefore skips every `Get-PrReadiness.ps1` gate test while the job stays green. The supplied empty-PATH run confirms the skip path.
  - `tools/ci/README.md` describes CIHelpers without the new `pwsh` prerequisite.
- **Risk**: The risk is latent, because hosted Ubuntu images include `pwsh` today. A pool or image change would make the readiness tests a silent no-op behind a green check. AGENTS.md warns against exactly this ("Do not equate green checks with correctness").
- **Suggested Fix**: Fail instead of skip under Azure Pipelines, without changing pipeline.yaml:
  - Change the guard to `skipUnless(shutil.which("pwsh") or os.environ.get("TF_BUILD"), …)`, so a missing `pwsh` errors in CI but still skips locally; or
  - add a test outside the skipped class that asserts `shutil.which("pwsh")` when `TF_BUILD` is set.

  Optionally mention the prerequisite in the CIHelpers paragraph of tools/ci/README.md. If neither change is acceptable, `ci-triage.md:89` should stop claiming CIHelpers coverage and tell readers to check the job log for skips.

### Issue 3: New delegation acceptance rows repeat existing rules with diverging wording
- **Severity**: Low
- **File**: .github/skills/synapseml-external-contributor-review/references/ci-delegation.md
- **Line(s)**: 96-98 (compare ci-delegation.md:95; loop-control.md:247, 248, 253; pr-loop SKILL.md:107-109)
- **Description**: Each of the three new rows restates an existing rule:
  - **Row 96** (no maintainer edits) repeats `SKILL.md:107-109`, which the external SKILL.md now links at lines 90-91. It also repeats `loop-control.md:247`, with different wording ("artifact publication" there, "artifact handoff" here).
  - **Row 97** (approval gate behind a green rollup) overlaps row 95 in the same table ("Platform requires protected-workflow approval … delegation does not bypass it") and `loop-control.md:248`.
  - **Row 98** (wrong trusted source) repeats `loop-control.md:253` and the new bootstrap paragraph at external SKILL.md lines 18-20.

  The PR's no-bloat gate names duplicate documentation as something to remove.
- **Risk**: Each rule now exists in up to three copies whose wording already differs. A later edit to one copy can leave the two skills with contradictory acceptance contracts.
- **Suggested Fix**: Merge row 97 into row 95, for example: "Platform requires protected-workflow approval, even with a green rollup | Leave that gate blocked and report it; delegation does not bypass it." Drop rows 96 and 98, or reduce them to the delegation-specific detail and link to the loop's rules.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
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
  "changeSize": "17 source, test, and documentation paths; 4398 added and 162 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 34,
  "completedPriorPasses": 33,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Its original SHA-256 is
`49bb5bcf1dec10f9f4c6cde7ad5b4c4f4aaa250411c34645a3ed48291118ed4c`.
Round 1 was clean; round 2 ended the pass. Neither qualifies the revised patch.
The reviewed diff SHA-256 was
`81913541258d6550b1b4613c0c4b54f6c74b2e401fc992b0fc56a906852b47ad`;
the accompanying attempt-34 manifest records the complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
Both transports used the observed `claude-opus-5.5` model and read-only
source, policy, and test tools, without accessing earlier review feedback.

The scope caveat was checked against transport records: both runner inputs
contain the full frozen diff, and both recorded user payloads contain all
17 file diffs, including the final watcher-test diff. The payload comparison
found one platform-masked authorization-header expression, not file truncation.
The original reviewer statement remains above rather than being rewritten.

- **Issue 1: Fixed.** Step 8 now links directly to the complete gauntlet contract
  without repeating its rules or describing this PR's extraction history.
  The canonical reference retains all requirements.
- **Issue 2: Fixed.** A missing `pwsh` no longer skips readiness tests under
  Azure Pipelines (`TF_BUILD`); the subprocess fails instead. Local runs retain
  their explicit skip. CI triage documents both outcomes. No pipeline or
  workflow definition was changed.
- **Issue 3: Fixed.** Merged the green-rollup approval case into the existing
  protected-workflow row and removed duplicated no-edit and trusted-source rows.
  The linked canonical rules and shared acceptance scenarios remain intact.

Validation:

- On Windows and Linux, controlled subprocesses ran
  `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py -k review_request_requires_one_pr -rs --tb=short`
  (Linux used `python3` from `tools/ci/tests` with the filename alone).
  With an empty temporary PATH and no `TF_BUILD`, exactly one test skipped with
  the PowerShell 7.2+ diagnostic. With `TF_BUILD=True` and that empty PATH, the
  invocation failed with `FileNotFoundError`, as required. With `TF_BUILD=True`
  and PowerShell available, the test passed. Each case deselected 32 tests.
  These deliberately missing-tool cases verify the guard, not readiness coverage.
- Production helpers and existing assertions are unchanged since the preceding
  Windows/Linux runs of 5 tests and 19 subtests recorded in attempt 33.
- `python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_get_pr_readiness.py`
  passed; the file was unchanged.
- `git diff --check` passed.

No current-head Azure validation or final CI-qualified pass is claimed.
