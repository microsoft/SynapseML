---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 38
stage_attempt: 1
round: 2
timestamp: "2026-10-18T00:58:48Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1bd66c3f9a9ac628dca82b9b9e447bfaf515beadd603cf42d0b207bf848b0887"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-38-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **How the helpers are layered.** I read all of `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`. The watcher reuses `AzureClient`, `collect_build`, `verify_merge` and `command_json` from the collector (`watch_azure_pipeline.py` lines 17-27). The collector gets `trusted_executable` and `GuidanceError` from the verifier. No module reimplements Azure or tool-lookup logic.
- [x] **Trust conventions.** The Python helpers find `git`, `gh` and `az` through `trusted_executable`, which excludes the checkout. The bootstrap uses `-I -B` and the helpers use `-E -s -B`, as `references/trusted-guidance.md` documents. Direct CLI runs turn off bytecode writing (`watch_azure_pipeline.py` lines 14-16).
- [x] **No pipeline, workflow or dependency changes.** The `pipeline.yaml` CIHelpers job (lines 112-125) already installs pytest and pyyaml and runs `python3 -m pytest tools/ci/tests/ -q`, so the new test suites run without editing pipeline or workflow files. None of the 17 paths is a pipeline, workflow, dependency pin or release tool, which matches AGENTS.md "Ask before changing…".
- [x] **Readiness output shape.** `Get-PrReadiness.ps1` lines 556-576 and 665-695 report evidence, remote engineering gates, engineering, human approval, merge and unassessed gates as separate fields. `completeness.complete` (line 694) is kept as an alias for backward compatibility, and the table in `references/ci-evidence.md` describes it.
- [x] **Code and docs agree.**
  - The SKILL.md §9 `-RunPipeline -ExpectedHeadSha -ExpectedBaseSha` syntax matches the script's guard (lines 99-108).
  - The script's `#Requires -Version 7.2` (line 1) matches both SKILL.md compatibility fields, `ci-evidence.md` line 9, `ci-triage.md` line 97, and the skip reason in `tools/ci/tests/test_get_pr_readiness.py` (line 226).
- [x] **Doc structure and no-bloat.**
  - SKILL.md §8's gauntlet contract moved to `references/review-gauntlet.md`, leaving a pointer. It is moved, not duplicated.
  - The external-contributor SKILL.md links to `#validation-only-handoff` instead of repeating it.
  - All checked anchors resolve, including `waiting-for-azure-pipelines`, `evidence-invalidation`, `state-and-cost-discipline` and `targeted-infrastructure-retry`.
- [x] **Test conventions.**
  - The suites load scripts with `importlib.util.spec_from_file_location`, like existing `tools/ci` tests, and use unittest-compatible `TestCase`s.
  - I checked which parameters the PowerShell harness can pass (`test_get_pr_readiness.py` lines 102-129).
  - I compared the sibling bytecode tests (`test_collect_ci_evidence.py` lines 717-734; `test_watch_azure_pipeline.py` lines 740-760).
- [x] **Black version.** Black is pinned to `black[jupyter]==22.3.0` (`pipeline.yaml` line 167). The caller reports the `--required-version 22.3.0` check passed for all seven Python files. That is caller evidence; I did not rerun it.
- [x] **What this review did not do.** I used read-only tools only and ran no tests or commands. The caller's validation results are their claims, not something I re-verified. I did not open any earlier or parallel review artifacts.
- [ ] **Live GitHub/Azure behavior.** Not applicable: this read-only review can't call those APIs. I relied on the fixtures and the caller's API facts.

## Issues

### Issue 1: The Azure aggregate rule is split between an overridable `-RequiredCheck` default and hard-coded strings
- **Severity**: Low
- **File**: .github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1
- **Line(s)**: 80-86, 458-471, 620, 728 (harness: tools/ci/tests/test_get_pr_readiness.py 107-112)
- **Description**: The string `microsoft.SynapseML` now sets policy in five places, and they don't agree:
  - **Line 86:** it is only the default of the generic, overridable `-RequiredCheck` parameter.
  - **Line 461:** inside the generic presence check, a value equal to this string gets case-sensitive exact matching. Any other value gets case-insensitive prefix matching.
  - **Line 469:** `$unprovenRequiredChecks` always treats it as required, whatever `-RequiredCheck` says.
  - **Lines 620 and 728:** the `-RunPipeline` trigger and the Azure warning always use it.

  So whether the aggregate must be present depends on the parameter, while its result and the trigger are hard-coded. Separately, checks supplied through `-RequiredCheck` are checked for presence, failure and pending status, but are never flagged as unproven when skipped or neutral.
- **Risk**: Documented callers use the default today, so they are not affected. That is why this is Low. But the parameter is public, and these inputs break documented policy:
  - **Absent aggregate passes.** Passing any `-RequiredCheck` replaces the default and drops the Azure presence gate. If matrix legs exist, the aggregate is missing, and rulesets don't name it, `missingRequiredChecks` is empty. `remoteEngineeringGatesClear` and `completeness.complete` can then be true. This contradicts the loop-control scenario "Azure matrix legs exist but the aggregate is absent → Required Azure gate is missing". The same snapshot's trigger logic still treats Azure as required.
  - **Skipped checks pass.** A check required through `-RequiredCheck` that finishes SKIPPED or NEUTRAL clears the gates. This contradicts `references/ci-evidence.md` line 52: "Required skipped/neutral checks remain unproven execution."
  - **Wrong case falls back to prefix matching.** `-RequiredCheck microsoft.synapseml` uses prefix matching, which a single leg such as `microsoft.SynapseML (Python)` satisfies.

  The test harness has no way to pass `-RequiredCheck`, so none of these paths is tested.
- **Suggested Fix**:
  1. Always include the exact aggregate name in the presence check, as line 469 already does for the unproven check.
  2. Compute missing and unproven checks from the same required set: rule and classic contexts, the aggregate, and any matches for `-RequiredCheck` entries.
  3. Make `-RequiredCheck` add to the defaults, or document that it only covers extra checks, and update the comment at lines 80-81.
  4. Define the string once to avoid the five literals.
  5. Add a harness option for `RequiredCheck` and a regression test with a non-default value. It should cover a missing aggregate and a skipped check that the caller required.

### Issue 2: The watcher's bytecode regression test inherits `PYTHONDONTWRITEBYTECODE`; the collector's equivalent test does not
- **Severity**: Low
- **File**: tools/ci/tests/test_watch_azure_pipeline.py
- **Line(s)**: 740-760 (compare tools/ci/tests/test_collect_ci_evidence.py 717-734; guard in .github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py 14-16)
- **Description**: Both tests copy the helper scripts to a temporary directory, run the CLI with `--help`, and assert that no `__pycache__` appears. The collector test removes `PYTHONDONTWRITEBYTECODE` from the child process environment. The watcher test passes no `env`, so the child inherits whatever the caller has set. The same PR tests the same contract with different isolation.
- **Risk**: Container images and some developer shells set `PYTHONDONTWRITEBYTECODE`. With it set, the child never writes bytecode, so the watcher test passes even if the `if __name__ == "__main__": sys.dont_write_bytecode = True` guard is deleted. That guard stops a direct watcher run from writing `__pycache__` into the verified guidance snapshot when it imports `collect_ci_evidence` and `verify_trusted_guidance`. As a result, whether the test catches a regression depends on the environment.
- **Suggested Fix**: Pass the same filtered environment the collector test uses: `env={k: v for k, v in os.environ.items() if k != "PYTHONDONTWRITEBYTECODE"}`. This file doesn't import `os` yet, so add that import.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver triage
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting driver triage
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
  "changeSize": "17 source, test, and documentation paths; 4301 added and 164 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 38,
  "completedPriorPasses": 37,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}

## Driver resolution - superseded attempt

The original feedback above is unchanged. Original artifact SHA-256 values:

- Round 1: `4dae4da0b4f6f9fe433d7ecb054131111200193739fcafa58f69ecff1f06ba69`
- Round 2: `2652ad14d057c7a20983c949abd325394aa5255b618c5204eebaa579a24ca70c`

The reviewed diff SHA-256 was
`0f8c5496d00d3aea4c60a6334be5d8721ec06d04d7cf31a3f28ae39e2c224604`;
the attempt-38 manifest records its complete source scope.
The target and merge base were `0543bf7e5266c587207ae8c28c3b696deeed3ed1`;
the published predecessor was `a96e0370dab63a22a73f56c24ec7441cd36d3622`.
Both transports used the observed `claude-opus-5.5` model and read-only source,
policy, and test tools, without prior or sibling feedback. Prepared inputs
contain the exact full diff; both recorded message forms preserve all 5,063
lines except for one platform-masked header expression.

**Issue 1: Fixed.** The exact Azure aggregate is now always in the required
check set. `-RequiredCheck` adds case-insensitive prefixes without replacing
that gate; matched names enter the same set used to assess successful execution.
One Azure-name constant also drives the trigger and warning. The reference and
parameter comment describe the additive contract.

The new entry-point regressions failed before the fix in five subcases: empty,
custom, or differently cased options dropped the Azure gate, and skipped/neutral
custom checks escaped the unproven set. They now pass; absent custom checks
still block, and successful case-insensitive prefix matches still work.
This addresses reproduced non-default-input failures, not merely hypothetical
renaming or duplicate literals.

**Issue 2: Fixed.** The watcher CLI regression filters
`PYTHONDONTWRITEBYTECODE`, matching the collector test's isolation. A temporary
mutation removed the watcher's bytecode guard while the parent environment set
that variable to `1`. The old test incorrectly passed; the revised test fails
at the `__pycache__` assertion. The unmodified production helper passes under
the same environment. No production file was mutated by that experiment.

Validation after both fixes:

- Windows:
  `python -B -m pytest -q tools/ci/tests/test_get_pr_readiness.py tools/ci/tests/test_watch_azure_pipeline.py -rs --tb=short`
  passed: 75 tests and 165 subtests, without skips.
- Linux, from `tools/ci/tests` with PowerShell 7.6.7 on PATH:
  `python3 -B -m pytest -q test_get_pr_readiness.py test_watch_azure_pipeline.py -rs --tb=short`
  passed: 75 tests and 165 subtests, without skips.
- From that directory,
  `python3 -m black --check --required-version 22.3.0 test_get_pr_readiness.py test_watch_azure_pipeline.py`
  passed after formatting the new readiness cases.
- `git diff --check` passed.

The Python helpers, collector tests, and trusted-guidance tests did not change
after their previous Windows, Linux, and Python 3.10 full-suite runs. Round 1
was clean, but these fixes end this pass. Current-head Azure validation and the
final CI-qualified pass remain outstanding.
