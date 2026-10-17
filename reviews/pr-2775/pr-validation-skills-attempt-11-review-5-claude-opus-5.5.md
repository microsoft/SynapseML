# Review Round 5 — Testing & Coverage

## Summary
- Round: 5
- Theme: Testing & Coverage
- Mode: manual prompt fallback
- Requested model: claude-opus-5.5 (max)
- Issues: 1
- Verdict: CHANGES REQUIRED

## Evidence Checklist
- [x] I read all 3,124 lines of `review.diff`, and its file headers match the 17 paths in `manifest.json`. I didn't recompute the SHA-256 because execution wasn't allowed.
- [x] `tools/ci/tests/test_get_pr_readiness.py:171`: a hidden `action_required` workflow approval correctly blocks an otherwise green check rollup. For every unsafe `-RunPipeline` path, the harness checks that no `/azp run` comment was posted.
- [x] `tools/ci/tests/test_collect_ci_evidence.py:170,239`:
  - A documented skip can't waive a named `--expected-test`.
  - Leaf Phase/Stage records are gated but can't supply test-attempt evidence.
  - A parent cycle raises an error.
- [x] `tools/ci/tests/test_watch_azure_pipeline.py:624,643`:
  - The pinned merge parents are still checked after the target branch moves.
  - An active same-build retry overrides a stale GitHub failure at the 600 s cadence.
- [x] `tools/ci/tests/test_trusted_guidance.py:90`: ignored bytecode is rejected.
- [x] `pipeline.yaml:113-125`: CI runs `tools/ci/tests` on hosted Ubuntu, where `pwsh` is preinstalled, so the PowerShell readiness tests aren't silently skipped.

## Issues

### Issue 1: The workflow failure and pending gates have no positive test, and the only failure assertion can't fail for the right reason
- **Severity:** Medium
- **Files:**
  - `tools/ci/tests/test_get_pr_readiness.py:442-458`
  - `.github/skills/synapseml-pr-loop/scripts/Get-PrReadiness.ps1:352-357,454-455`
- **Risk:**
  - `$failedWorkflows` and `$pendingWorkflows` are the only things that catch workflow runs with no job check in the rollup, such as `startup_failure` or queued/`waiting` runs. That's the hidden-gate case this change is meant to close.
  - The only failure fixture (line 450) is replaced by a newer run, and line 458 only asserts `failedWorkflows == []`. That assertion still passes if the failure/pending filters never match anything.
  - No test fixture uses `startup_failure`, `waiting`, `queued`, or `in_progress`, and `pendingWorkflows` is never asserted.
  - So if those filters broke, `remoteEngineeringGatesClear`, `completeness.complete`, and `readiness.merge = requires_engineering_evidence` could all report clear over a failed or unfinished workflow, and the suite would stay green.
- **Minimal fix:** Add harness cases:
  - A single run with `conclusion: "startup_failure"` that isn't replaced by a newer run and has no rollup entry: assert it appears in `failedWorkflows` and the gates are false.
  - A run with `status: "queued"` and `conclusion: null`: assert it appears in `pendingWorkflows` and the gates are false.
  - A run with `status: "waiting"`: assert it appears in `workflowApprovalsRequired`.
  - An old-run-only control for the rerun test, so its supersession assertion can actually fail.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `4af6a3a53f6b1b6ba7a52f18e640eb6ef8f98717bee36ac8af18d70fea1852b5`
- Reviewed diff SHA-256: `bffec5c72c2886366b0342d7b080da336e45895af8e222e4bc90b7ddf0d3aeb6`
- Original artifact SHA-256: `fa82afa77125efb810d662f9038120560a868cf8e4e629653a79d9cbc1ed7f83`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
