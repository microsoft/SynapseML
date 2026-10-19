---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 68
stage_attempt: 1
round: 5
timestamp: "2026-10-19T13:07:35Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "81ecb55fc1678826019c318499ab59e377bd14b7b5134d0d7a02c4b3ab4583e8"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-68-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Verifier fix is in the right place.** In `.github/skills/synapseml-pr-loop/scripts/verify_trusted_guidance.py`, the `is_file()` check (lines 141-142) runs after the per-path symlink check (135-140) and before `read_bytes()` (144).
  - The earlier Git calls (`ls-tree`, `ls-files -s`, `ls-files --others`, with `core.fsmonitor=false`) never open working files, so a FIFO cannot block before the check.
  - `main` (172-174) reports the rejection as `verified: false` JSON.
- [x] **The verifier regression would fail without the fix.** `tools/ci/tests/test_trusted_guidance.py` lines 181-219 run the real CLI with `-I -B` and a 30-second bound.
  - It replaces `AGENTS.md` with a directory on every platform, and with a FIFO where `os.mkfifo` exists.
  - It asserts exit 1, `verified` false, and "regular files" in the error.
  - Without the fix, the directory case returns an `IsADirectoryError` or `PermissionError` message without "regular files". The FIFO case reaches the timeout and the `self.fail` path.
  - The `finally` block restores the file, so the subtests don't affect each other.
- [x] **Collector fix rejects completion at the deadline.** In `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `main` calls `client.remaining_timeout()` (line 759) just before `print` (760).
  - `remaining_timeout` (134-142) raises when the remaining time is zero or less, so finishing exactly at the deadline is rejected.
  - The error goes through the existing `except EvidenceError` JSON path. No new option, error mechanism or worker was added, and the follow-up totals three production lines as claimed.
- [x] **The deadline regression uses real code where it matters.** `tools/ci/tests/test_collect_ci_evidence.py` lines 1241-1296 use a real `AzureClient(deadline=700)`, so the real `list()` pagination and the final deadline check both run.
  - Only `get`, `command_json` and `time.monotonic` are patched. The clock moves to 699, 700 or 701 on the second timeline read.
  - It asserts `deadline=700` was passed to the client factory, exactly two timeline reads, `validationPassed` with `currentResultCount == 1` at 699, and the exact error JSON at 700 and 701.
  - Without the fix, 700 and 701 return 0, so the test fails.
- [x] **Mock-client tests don't hide the new check.** The `client_fixture` Mock (lines 101-111) skips the final check by design. The entry-point matrix (1166-1222) still asserts `deadline=700` is passed, and only the dedicated regression owns the boundary behavior.
- [x] **Watcher matches the collector.** `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` rechecks the deadline after `collect_build` (lines 157-163) before storing Azure evidence (164).
  - `tools/ci/tests/test_watch_azure_pipeline.py` lines 643-691 cover 7199, 7200 and 7201 and assert that timeout output has no `azure` key.
  - Lines 594-641 cover a truncated response at both elapsed boundaries.
- [x] **Documentation matches tested behavior.**
  - `references/trusted-guidance.md` lines 35-36 cover non-regular files and named pipes.
  - `references/ci-evidence.md` lines 188-190 state that finishing at or after the deadline is an error.
  - `test_documented_bootstrap_rejects_shadow_imports_before_execution` (lines 128-172) reads the documented `-I -B` flags from the reference, so drift between docs and code fails the test.
- [x] **The readiness trigger guard is covered.** `Get-PrReadiness.ps1` lines 109-110 refuse `-RunPipeline` combined with `-WaitForReview` or `-RequestReview`, or without both expected SHAs.
  - `test_unsafe_pipeline_requests_fail_without_posting_a_comment` (`tools/ci/tests/test_get_pr_readiness.py` lines 902-972) covers `wait`, `request_review`, missing head or base, multiple PRs, head or base movement, truncation and closed PRs.
  - The complete-collection trigger gate is unchanged and not relaxed.
- [x] **Hosted CI will run the new regressions.** The `pipeline.yaml` CIHelpers job (lines 112-126) runs `python3 -m pytest tools/ci/tests/ -q` on Ubuntu, so the FIFO subtest and the PowerShell readiness suite will run once the fixed head is published.
  - The pipeline is stageless, so `Phase.Job` identifiers such as `UnitTests.scala` match the collector's run-to-job key.
- [x] **The supplied evidence is internally consistent.**
  - Subtests reconcile: 289 on Windows, minus 4 batch subtests skipped on Linux, plus 1 FIFO subtest, gives 286 on Linux.
  - The two Windows-only skips match `skipUnless(win32)` at lines 904 and 932.
  - Before the fix, Windows had 3 failures (directory, 700, 701) and Linux had 4 (the same plus the FIFO).
- [x] **The remaining untested branches are not defects.** I inspected these by reading the code:
  - `verify_merge` with malformed data (collector lines 221-233).
  - Rules without a `type` and the classic `checks[].context` form (readiness lines 333-334 and 362). GitHub also returns `contexts`, which is tested at test line 733.
  - The `KeyboardInterrupt` cleanup in `run_windows_batch` (lines 39-68).

  Each one either fails closed or duplicates a tested form. Adding tests for them would be blanket coverage, which the plan excludes.
- [x] **The timing-sensitive Windows test is acceptable.** `test_batch_timeout_stops_the_child_holding_output_pipes` (lines 932-965) has about 3 seconds of slack under `elapsed < 5`. It is skipped on the Linux CI agent and passed in the supplied Windows run, and I found no concrete sign of flakiness.
- [ ] **I did not run any tests**, because the review plan is read-only. Results come from the supplied command evidence plus static inspection.
- [ ] **Hosted CI has not run on the pending patch.** The supplied hosted build used the pre-fix head and was stopped early, so it is baseline only and not a final CI-qualified pass.

## Driver completion evidence

The finalized receipt, retained file hashes and observed runtime model match.
All 33 tool calls were read-only and limited to named source paths/subtrees
or an exact CODEOWNERS lookup; no prior or sibling feedback was accessed.
The original artifact is preserved with SHA-256
`3fef49b1739ed00ef2a83bae25a4f6f82ca3341ed35780b19903401d6185f2d3`.
This clean lens supplies pre-commit evidence, not current-head CI clearance.