---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 44
stage_attempt: 1
round: 3
timestamp: "2026-10-18T07:13:32Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e76b57550cd695b890f29d5f8c64f7081365931ebcd84f6425bd3fff5f3a2e12"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-44-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I inspected all 17 changed paths:
  - Both external-contributor skill files.
  - `synapseml-pr-loop/SKILL.md`.
  - `Get-PrReadiness.ps1`, `collect_ci_evidence.py`, `watch_azure_pipeline.py` and `verify_trusted_guidance.py`.
  - Six references: `ci-evidence`, `ci-triage`, `loop-control`, `readiness-gates`, `review-gauntlet` and `trusted-guidance`.
  - Four test suites under `tools/ci/tests/`.

  The supplied diff was cut off at the step 9 hunk of `SKILL.md`. I read the remaining hunks from the working tree. I assumed the working tree matches the frozen index manifest the driver described, but I could not check that with read-only tools.
- [x] **Comment-trigger gate diagnostics.** The retained conservative gate itself was not re-argued; I checked how it reports and behaves.
  - **Argument guards** (`Get-PrReadiness.ps1:92-110`): SHA patterns are enforced, expected SHAs require exactly one PR, and `-RunPipeline` refuses `-WaitForReview` or `-RequestReview`.
  - **Error message** (`:617-624`): the thrown message lists every input to `evidenceComplete`. Those are revision errors, collection errors (reset per PR at `:307`) and truncated thread comments. It also says to run without `-RunPipeline` for a diagnostic snapshot.
  - **Classic-protection failures** (`:242-276`, hint at `:251-254`): a failed protection read adds a note that Administration read permission is required.
  - **Non-open PRs** (`:625-627`): rejected separately.
  - **Comment posting** (`:631-640`): at most once per PR per invocation, only when the exact case-sensitive aggregate is absent, with a warning if `gh` fails.
  - **Documentation**: the hand-posted requeue and pinned-queue alternatives in `ci-evidence.md:93-120` and `ci-delegation.md` match the code.
  - **Tests**: `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (`test_get_pr_readiness.py:719`) covers 11 unsafe variants.
- [x] **Link integrity.** All four comment-trigger routes resolve to the heading at `ci-evidence.md:93`:
  - `synapseml-pr-loop/SKILL.md:218` (`:231-232` refers back to it)
  - `ci-triage.md:17`
  - `ci-delegation.md:70`
  - `readiness-gates.md:99`

  The other anchors in changed docs also resolve: `ci-evidence.md:7,121`, `ci-triage.md:6,188`, `loop-control.md:53,71,138`, `ci-delegation.md:25`, and "Validation-only handoff" in `SKILL.md`.
- [x] **Collector failure modes** (`collect_ci_evidence.py`):
  - **Requests** (`:71-137`): endpoints are allow-listed, redirects are refused, and every timeout is clipped to the deadline. Only a timeline 404, empty or null response maps to "absent"; every other failure fails closed (test at `:456`).
  - **Pagination** (`:139-164`): pages that aren't lists, rows that aren't objects, and invalid or repeated IDs are all rejected.
  - **Merge check** (`:167-179`): parents must be exactly `[base, head]`.
  - **Arguments** (`:639-681`): SHAs are lowercased, build IDs must fit int32, and names must be nonempty.
  - **Main** (`:684-711`): runs under a 600 s monotonic deadline. A second build read sets `collectionStable`, `validationPassed` requires every condition, `mergeReady` is always false, and any `EvidenceError` produces sanitized error JSON.
- [x] **Watcher** (`watch_azure_pipeline.py`):
  - **Build URLs** (`:47-77`): build URLs must use the trusted host, organization, project, path and port, and build IDs must fit int32. Tested at `:387` and `:426`.
  - **Duplicates** (`:187`): duplicate results for one build are rejected.
  - **Deadline**: anchored to kickoff and measured monotonically, so restarting the watcher cannot extend it (test at `:181`).
  - **Exit handling** (`:322-362`): every outcome maps to an exit code. `MonitorError` and `KeyboardInterrupt` still emit a final JSON event.
- [x] **Guidance verifier** (`verify_trusted_guidance.py`):
  - **Tool lookup** (`:40-69`): relative PATH entries and anything under the current directory are skipped. Windows uses PATHEXT expansion and case-insensitive path comparison.
  - **Git calls** (`:72-92`): `GIT_*` variables are stripped, fsmonitor and replace objects are disabled, and each call has a 30 s timeout.
  - **Root check** (`:95+`): the resolved repository root must equal the checkout.
  - **Tests**: `test_trusted_guidance.py` covers `GIT_*` redirection, CRLF, dirty/staged files, untracked or ignored bytecode, a shadow `json.py`, a committed symlink and the fsmonitor hook.
- [x] **CI coverage and scope.** The CIHelpers job (`pipeline.yaml:112-125`) runs `python3 -m pytest tools/ci/tests/ -q` with a 15-minute timeout. The readiness suite skips only when `pwsh` is missing outside `TF_BUILD` (`test_get_pr_readiness.py:233`). `pipeline.yaml`, workflows, dependency pins and product APIs are not in the changed paths.
- [x] **Candidates rejected as fail-closed or documented:**
  - Ambiguous fork associations, a deleted head repository, truncated thread comments or missing Administration read block `-RunPipeline`. This is the retained gate, and it reports why.
  - One query error stops the watcher with exit 1. Its deadline cannot be extended.
  - Any timeline warning blocks `jobGatesPassed`. This is documented and requires a recorded disposition.
  - The watcher returns `azure_completed` while the GitHub rollup lags. This is documented as exit 4.
  - `STARTUP_FAILURE` check runs are not counted as failed. Apps cannot set that conclusion, and Actions startup failures are caught through `$failedWorkflows`.
  - Runs inserted during `$skip` pagination produce a duplicate, which is detected. Truncated results fail through the `totalTests` completeness check or a missing named test.
- [ ] **Test execution**: not done; this lens is read-only. The Windows/Linux pytest, Black 22.3.0 and diff-check results are recorded as supplied by the driver, not reproduced. No CI run exists for the current candidate, which matches the prompt.
- [ ] **Live service behavior**: not verifiable read-only. For example, I can't confirm whether GitHub ever creates an `EXPECTED` context for a required check that never reported. If it does, the helper treats it as pending (`Get-PrReadiness.ps1:449-452`) and the gates stay blocked, so it would not produce a false green.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary qualitative estimate",
    "models": "installed review-code complementary frontier policy and live task catalog",
    "stageBehavior": "repository sequential six-lens all-clean policy",
    "bounds": "cumulative attempts under user-authorized continuation"
  },
  "changeSize": "17 source, test, and documentation paths; 4423 added and 180 removed lines",
  "cyclomaticComplexity": "Moderate pagination, retry, readiness-state, and trusted-tool lookup interactions",
  "rationale": "Six independent complementary reviews of coupled validation helpers and contracts; no new product algorithms",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md",
  "reviewAttempt": 44,
  "completedPriorPasses": 43,
  "completedFinalCiQualifiedPasses": 0,
  "verifiedTaskId": null
}
