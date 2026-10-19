---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 37
stage_attempt: 1
round: 1
timestamp: "2026-10-17T23:28:56Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "907857cd8b1869792c29fcc5e618cb6dcd4f6f28e7cf167a2d38017554d4f11b"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-37-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Collector** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, all 724 lines). Every path fails closed, and malformed fields raise sanitized `EvidenceError` with no tracebacks. I traced:
  - **`AzureClient`**:
    - only allow-listed endpoints, a fixed project, and redirects refused;
    - `$skip`/`$top` pagination that rejects duplicate IDs;
    - `totalTests` checked against the number of results.
  - **`verify_merge`** requires the merge parents in order (target, then head).
  - **`verify_build`** checks definition 17563, the repository, `refs/pull/<n>/merge`, the source version and the build reason.
  - **`summarize_timeline`**:
    - keeps the highest attempt of each job;
    - copies issues from ancestor records onto each job;
    - rejects parent cycles.
  - **`collect_tests`**:
    - counts only current attempts;
    - requires publications to carry forward per job;
    - a named expected test cannot be exempted as a skip.
  - **`main`** rechecks stability and always reports `mergeReady=false`.
- [x] **Readiness script** (`.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1`, all 739 lines):
  - Lines 101–110: `-RunPipeline` requires one PR and both expected SHAs, and refuses `-WaitForReview` and `-RequestReview`.
  - Lines 241–275: paginated and empty REST responses.
  - Lines 277–284: run associations are checked against the base URL and head SHA.
  - Lines 286–296 and 536–541: the legacy head-repository identity is compared on recheck.
  - Lines 400–425: the fork fallback uses `state=all` and requires a unique branch.
  - Lines 455–471: `microsoft.SynapseML` must match exactly, including case.
  - Lines 532–575: revision errors and the separate gates.
  - Lines 612–629: `/azp run` is posted only when evidence is complete and the Azure aggregate is absent.
- [x] **Trusted-guidance verifier** (`.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`) checks:
  - HEAD, the tree, and that the index (stage 0) matches the tree;
  - untracked and ignored files;
  - working files byte for byte, tolerating only CRLF line endings;
  - that guidance paths contain no symlinks or submodules.

  Its Git calls strip `GIT_*` variables and use `--no-replace-objects` and `core.fsmonitor=false`. `trusted_executable` excludes relative `PATH` entries and the current directory and its descendants. The PowerShell helper's normal command discovery is a documented exception in `references/trusted-guidance.md`.
- [x] **Watcher** (`watch_azure_pipeline.py`): lines 14–17 disable bytecode writes before importing the collector. The `azure_completed` and `attention_required` outcomes exit nonzero, as `references/ci-triage.md` documents.
- [x] **Docs match the code.** I checked `ci-evidence.md`, `ci-triage.md`, `trusted-guidance.md`, `review-gauntlet.md`, `readiness-gates.md`, `loop-control.md`, both `SKILL.md` files and `ci-delegation.md`. All linked anchors exist, including `#validation-only-handoff`, `#evidence-invalidation` and `#azure-merge-and-named-tests`.
- [x] **Test counts match the supplied totals.**
  - Readiness has 33 tests and trusted guidance 14, for 47 in total. That matches Windows (46 passed, 1 skipped) and Linux (47).
  - The watcher has 39, which matches.
  - The collector has 35.
  - The skip condition at `tools/ci/tests/test_get_pr_readiness.py:225-227` (no `pwsh` and no `TF_BUILD`) matches the documented behaviour.
  - I inspected the regressions at lines 897 (legacy identity) and 916 (shared fork branch).
- [x] **No bytecode in the guidance paths.** All three Python test loaders patch `sys.dont_write_bytecode`, and there is no `__pycache__` under `.github/skills`.
- [x] **Scope.**
  - `pipeline.yaml:111` uses top-level `jobs:`, which matches the collector's `phase.job` key.
  - The CIHelpers job runs `python3 -m pytest tools/ci/tests/ -q`, so the new suites are picked up with no pipeline change.
  - The diff changes no workflow, dependency pin or product API.
- [x] **Black line length.** Lines over 88 columns are strings Black cannot split, or PowerShell text inside a test string. Examples: `collect_ci_evidence.py:430,603,611`, `watch_azure_pipeline.py:198,239`, `verify_trusted_guidance.py:29,162`.
- [x] **Considered and rejected as defects:**
  - Timeline warnings block `jobGatesPassed`; this is deliberate and documented.
  - Pagination cannot detect a missing run because the API returns no total.
  - An explicit null issue count raises an error; this fails closed.
  - `STARTUP_FAILURE` is not a check failure state, but startup failures are covered by the workflow-run checks.
- [ ] I ran no tests, Black or Git commands; I had read-only tools only. All pass counts come from the supplied evidence.
- [ ] Black evidence was not supplied for `collect_ci_evidence.py`, `watch_azure_pipeline.py`, `verify_trusted_guidance.py` or `test_collect_ci_evidence.py`. I could only spot-check line length; GitHub's Black check will decide.
- [ ] I could not confirm that `verify_trusted_guidance.py`, which the collector imports, is unchanged since the last collector-suite run.
- [ ] I did not independently verify the external GitHub/Azure API facts, the patch SHA-256, or any CI for this candidate.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4258 added and 163 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 37,
  "completedPriorPasses": 36,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
