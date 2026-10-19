---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 68
stage_attempt: 1
round: 2
timestamp: "2026-10-19T14:16:14Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "5d0aefccea5d4d1d8af4915c18658fb1a98f7efdeb1ca7e440466ba7fe6de3cc"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-68-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **All 17 changed paths inspected on disk.**
  - Read in full: the nine skill documents (both `SKILL.md` files, `ci-delegation.md`, `ci-evidence.md`, `ci-triage.md`, `loop-control.md`, `readiness-gates.md`, `review-gauntlet.md`, `trusted-guidance.md`), the four helpers (`collect_ci_evidence.py`, `watch_azure_pipeline.py`, `verify_trusted_guidance.py`, and `Get-PrReadiness.ps1` lines 1–762), and `test_trusted_guidance.py`.
  - Read in part: `test_collect_ci_evidence.py` (header, the entry-point and deadline tests at lines 1160–1323, and its test list), `test_get_pr_readiness.py` (the PowerShell harness at lines 1–310 and its test list), and `test_watch_azure_pipeline.py` (its loader, lines 1–80).
  - Both latest fixes are present on disk:
    - In `verify_trusted_guidance.py`, the `path.is_file()` rejection at lines 141–142 runs after the symlink checks and before `read_bytes()` at line 144.
    - In `collect_ci_evidence.py`, `client.remaining_timeout()` at line 759 runs before `print` at line 760.
- [x] **Layering and reuse.**
  - `watch_azure_pipeline.py` (lines 14–27) imports `AZURE_PROJECT_ID`, `MAX_BUILD_ID`, `REPOSITORY`, `AzureClient`, `command_json`, `collect_build` and `verify_merge` from the collector rather than duplicating them.
  - The collector (lines 18–21) imports `trusted_executable` and `GuidanceError` from the verifier.
  - Errors are converted at each boundary: `GuidanceError` becomes `EvidenceError`, which becomes `MonitorError` in `query_pr` and in the timeline block.
  - Both command-line scripts set `sys.dont_write_bytecode` under `__main__` before their sibling imports.
  - The documented `-E -s -B` flags are parsed from `ci-evidence.md` and `ci-triage.md` and checked by `test_documented_credential_helpers_ignore_untrusted_pythonpath`.
  - The watcher builds a new `AzureClient` on each poll, which refreshes the token while keeping the original deadline.
- [x] **Documentation matches code.**
  - The `Get-PrReadiness.ps1` parameter guards (lines 103–111) match SKILL.md step 9, the "Comment-trigger gate" section of `ci-evidence.md`, step 3 of `ci-delegation.md`, and the `-WaitForReview` warning text.
  - The readiness and completeness fields (`evidenceComplete`, `remoteEngineeringGatesClear`, `engineering`, `humanApproval`, `merge`, `pipelineRunComment`, and the `complete` compatibility alias) match the table in `ci-evidence.md`.
  - The watcher's outcome-to-exit-code map (0, 1, 2, 3, 4, 124, 130) matches `ci-triage.md`.
  - The collector's JSON keys (`availableRunNames`, `expectedPublications`, `expectedTestPublications`, `publicationErrors`, `expected_suites_and_tests_only`, `timelinePending`, `jobGatesPassed`) match `ci-evidence.md`.
  - The intentionally kept complete-collection trigger gate (`-RunPipeline` with incomplete evidence throws) is present and was not flagged.
- [x] **Cross-links and the extracted gauntlet section.**
  - Every `#fragment` link in `.github/skills/` points to an existing heading, checked by grepping the headings.
  - Moving step 8 into `review-gauntlet.md` left no stale references: grep finds no remaining `REVIEW-PROMPTS`, `prompt generator` or `pipelineRunOutcome`.
  - The new acceptance rows in `loop-control.md` agree with `#validation-only-handoff` and the gauntlet contract.
  - Every new reference document and helper is linked from the pr-loop `SKILL.md`.
- [x] **AGENTS.md conventions.**
  - No changes to the pipeline, workflows, release tooling or dependency pins.
  - Lines 112–126 of `pipeline.yaml` already run `python3 -m pytest tools/ci/tests/ -q`, so CI picks up the four suites without a pipeline edit.
  - The tests mock every GitHub and Azure call, so they need no credentials.
  - The readiness suite's `skipUnless(pwsh or TF_BUILD)` (lines 249–251) matches what `ci-triage.md` says.
  - The new tests load the scripts the same way the existing watcher test does (`spec_from_file_location`, a temporary `sys.path` entry, and a `dont_write_bytecode` patch).
  - The new Python files have the MIT header.
  - Grep found no syntax or APIs that need Python 3.11 or later.
  - The root inputs the verifier requires all exist: `CODEOWNERS`, `pipeline.yaml` and `templates/*.yml`.
- [ ] **Tests not run by me.** I had read-only tools, so I did not run pytest, Black or the PowerShell checks. I relied on the supplied runs:
  - Windows, Python 3.14.6: 101 tests, 289 subtests.
  - Linux, Python 3.10.20 with pwsh 7.6.7: 101 tests, 286 subtests.
  - Readiness suite: 42 tests, 135 subtests.
- [ ] **Hosted evidence is baseline only.** Azure build 241650336 ran the pre-fix head `e102faa6…`, so the pending fixes have no Azure run yet.
  - The existing CIHelpers job runs `pytest -q` without publishing test runs, so `collect_ci_evidence.py` cannot provide named-test evidence for these helper suites.
  - This comes from the unchanged pipeline, which is out of scope; it is not a defect in this patch.
- [ ] **Diff not regenerated.** I could not run commands, so I did not recompute the merge-base diff or its hash. Where the diff text was supplied, I compared it with the files on disk.

## Driver completion evidence

The finalized receipt, retained file hashes and observed runtime model match.
All 56 tool calls were read-only and limited to named source paths/subtrees
or narrowly named root-file lookups; no prior or sibling feedback was accessed.
The original artifact is preserved with SHA-256
`9391e79e9d37e0618b326ad6dc5c74a112071990077b384e0a0b2f6f816445bb`.
This clean lens supplies pre-commit evidence, not current-head CI clearance.