---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 59
stage_attempt: 1
round: 5
timestamp: "2026-10-19T05:38:53Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "52265888ec537517c2cad4dc086c6b3f9ff0de99b9ad01ad018f1b939a1679a6"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-59-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Checked the watcher's timeline-mode result logic in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` L214–256 against every timeline test in `tools/ci/tests/test_watch_azure_pipeline.py`: L113–151, L617–657, L658–687, L689–708, L710–718, L720–742 and L744–789. Also compared it with what `references/ci-triage.md` L45–68 says the watcher does.
- [x] `pipeline.yaml` L111–126: the CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`, so all four helper suites run in hosted CI. The readiness suite skips only when `pwsh` is missing outside Azure (`skipUnless(shutil.which("pwsh") or TF_BUILD)`). Under Azure, a missing `pwsh` makes the tests fail instead of skip.
- [x] Compared the readiness test harness and tests (`tools/ci/tests/test_get_pr_readiness.py`) with `Get-PrSnapshot` and the wait loop in `Get-PrReadiness.ps1`. The tests cover:
  - legacy StatusContext values, both required and not required
  - a pending CheckRun that isn't required
  - a pending Azure check, which does not trigger CI again
  - classic branch-protection metadata and allowed fields
  - the evidence, engineering, human-approval and merge states, reported separately
  - workflow scoping and the fork unique-branch fallback, including closed PRs and lookup errors
  - the `-RunPipeline` refusal cases
  - the exact trigger and review-request side effects
- [x] Compared all tests in `tools/ci/tests/test_collect_ci_evidence.py` (through L1250) with `collect_ci_evidence.py`. The tests cover:
  - Calls through the public `main()` (L1151–1225):
    - they run the real `verify_merge`;
    - they check the exact `git/commits/<merge>` query with `timeout=60`;
    - reversed merge parents are rejected before any Azure query.
  - A retry that starts during collection invalidates success (L1226).
  - Results are checked per attempt, and one job cannot hide another job's failure.
  - Sharded suites and named tests are handled correctly, and allowed skips are limited.
  - Empty or 404 responses are accepted only from timeline endpoints.
  - Redirects are refused, and the deadline is checked before fetching credentials (L1040–1069).
  - Windows batch cleanup keeps the PID and chains exceptions when kill or wait fails (L920–987).
  - Importing the helpers does not change the caller's bytecode setting.
- [x] Compared `verify_trusted_guidance.py` with `tools/ci/tests/test_trusted_guidance.py`. The Python flags in the docs (`-I -B` in `trusted-guidance.md` L18; `-E -s -B` in `ci-evidence.md` L140 and `ci-triage.md` L31) match what the tests extract and run.
- [x] Every section link in the diff points to an existing heading:
  - `ci-evidence.md`: `#github-snapshot` (L7), `#comment-trigger-gate` (L94) and `#azure-merge-and-named-tests` (L126)
  - `loop-control.md`: `#artifact-retention` (L53) and `#evidence-invalidation` (L136)
  - `readiness-gates.md#user-value-and-scope`
  - `ci-triage.md#waiting-for-azure-pipelines`
  - `SKILL.md#validation-only-handoff`
- [x] Independence: I did not open or search any review, checkpoint, prompt or session location. Searches stayed within `.github/skills/` and `tools/ci/`, plus `pipeline.yaml` opened directly by name.
- [ ] I did not run any tests, because this review is read-only. The pass counts and timings in the request are your evidence, not something I reproduced. I also couldn't check how long CIHelpers takes on the hosted agent against its 15-minute limit. Your last full-directory local Linux run took 486.94 s, before the latest edits.

## Issues

### Issue 1: No test covers a failure that GitHub and Azure agree on, or the Azure details on a final success or failure, in timeline mode
- **Severity**: Low
- **File**: `tools/ci/tests/test_watch_azure_pipeline.py` (code under test: `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py`)
- **Line(s)**: tests L113–151, L266–274, L720–742 and L744–789; source L244–256
- **Description**: In `--azure-timeline` mode, `monitor` decides the final result with `(conclusion == "SUCCESS") != (azure["result"] == "succeeded")` (L244).
  - When the two disagree, it returns `attention_required` (exit 4).
  - When both report failure, it returns `failed` (exit 1) with the Azure details attached (L251–256).

  None of the current tests reach the second case:
  - The disagreement test cases (L751–756) only use mismatched pairs.
  - The two timeline success tests only check `outcome` (L147, L740).
  - `test_terminal_failures_do_not_pass` (L266–274) runs without `--azure-timeline`.
  - The only fixture with Azure `failed` (L642) has a pending GitHub check, so it ends as `azure_completed`.

  As a result, two code changes would pass every test:
  - (a) rewriting L244 as `conclusion != "SUCCESS" or azure["result"] != "succeeded"`;
  - (b) deleting `**({"azure": azure} if azure else {})` at L255.
- **Risk**:
  - Under (a), a real build failure that both systems agree on would be reported as "GitHub conclusion disagrees with Azure build evidence" with exit 4 instead of 1. That contradicts `ci-triage.md` L55–56, which reserves `attention_required` for disagreement, and sends triage toward a provenance problem instead of the failure itself.
  - Under (b), final timeline success and failure results would silently lose the documented diagnostics (`jobs`, `jobGatesPassed`, `unresolvedJobs`; `ci-triage.md` L49 and L65).
  - Both changes still fail safe (nonzero exit) or lose only diagnostics, so this is Low. Still, the main failure path of the new timeline mode has no regression test, which falls short of the "Test quality" section of `readiness-gates.md` (cover positive and negative paths, assert values).
- **Suggested Fix**:
  - Add one timeline-mode test where both sides agree on failure:
    - Inputs: `snapshot("COMPLETED", "FAILURE")`, and `collect_build` returns an in-progress result, then `{..., "status": "completed", "result": "failed"}`. Optionally add a `CANCELLED`/`canceled` case.
    - Assertions: `watcher.main(ARGV + ["--azure-timeline", ...]) == 1`, a final event with `outcome == "failed"` and `conclusion == "FAILURE"`, and `result["azure"]["result"] == "failed"`.
  - In `test_timeline_mode_waits_from_empty_queue_to_completed_jobs`, which already runs the real `collect_build`, also assert `proof["azure"]["result"] == "succeeded"`, `proof["azure"]["jobGatesPassed"] is True` and `proof["azure"]["unresolvedJobs"] == []`.
  - No production code change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
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
  "changeSize": "17 source, test, and documentation paths; 5197 added and 189 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 59,
  "completedPriorPasses": 58,
  "completedFinalCiQualifiedPasses": 0,
  "roundOrder": [5, 6, 1, 2, 3, 4],
  "verifiedTaskId": null
}

## Driver Disposition

The single low-severity finding is resolved by extending existing watcher tests;
the original feedback above is retained unchanged. The timeline matrix now
distinguishes agreement on success, failure and cancellation from disagreement,
asserts CLI exit codes and final conclusions, and checks retained Azure results.
The existing real-collector success test checks job diagnostics as well.
There are no production changes, new source files, or new test methods.

`python -B -m pytest tools/ci/tests/test_watch_azure_pipeline.py -q --tb=short -rs`
passed on Windows Python 3.14.6 (40 tests, 79 subtests, no skips, 3.09 s) and
Linux Python 3.10.20 (40 tests, 79 subtests, no skips, 7.02 s).
`python3 -m black --check --required-version 22.3.0 tools/ci/tests/test_watch_azure_pipeline.py`
passed. Isolated mutations of the agreement condition and retained Azure result
caused two and four targeted assertion failures respectively, without test
errors, skips, or production-file edits.

This pass stopped after lens 5; lenses 6, 1, 2, 3 and 4 did not run. The test
change invalidates attempt 59 as a qualifying pass; it is not a clean review.
The reviewed source diff was
`936de84fc3a78cdb8f47d9b3a580e3c2878856e6445adac4ebe1e08df9b702a0`,
against base `d34f762a785ee8a7c7c624806f8c112256168833` and local predecessor
`38685fad3d2e3ec93e6757adf36aa270cc3a24d1`.
Transport audit confirmed the requested model, exact embedded diff, read-only
source access without prior feedback, and publication-safe output.
