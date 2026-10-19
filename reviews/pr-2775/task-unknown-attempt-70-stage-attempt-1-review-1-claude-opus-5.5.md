---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 70
stage_attempt: 1
round: 1
timestamp: "2026-10-19T19:33:26Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3ef0ced86c88c7335c6bec0227a945f4dbb4bd91dd0d400a75bcb798c8db4231"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Deadline wrapper matches the stdlib in every supported Python version.** In `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`, `DeadlineReader` (L126-144) and `DeadlineHTTPSHandler` (L147-166) were traced against CPython `http.client` and `urllib.request` for 3.10/3.11 and 3.12+.
  - `HTTPResponse.fp` is detached before any read. The original `SocketIO` is wrapped in a new `BufferedReader`.
  - `remaining_timeout()` runs before every raw `readinto`. That covers the status line, headers, chunk framing, sized reads, and read-to-EOF (through `RawIOBase.readall`).
  - The handler still uses the standard verified TLS context, through `super().__init__()` and `context=self._context`. On 3.10/3.11, leaving out `check_hostname` gives the same default as the stock handler.
  - `build_opener` drops the stock HTTPS and redirect handlers. `NoRedirect` covers 301, 302, 303, 307 and 308.
- [x] **An expired deadline closes the response and its socket.** `EvidenceError` is not caught by `except (URLError, OSError, ValueError, HTTPException)` (L229).
  - If the deadline expires in `getresponse()`, `http.client` closes the response and `do_open` closes the connection.
  - If it expires during the body read, the `with` exit closes the response. `do_open` has already closed `h.sock`, so dropping the last `SocketIO` reference fully closes the socket.
  - This matches the regression's `reader.fileno() == -1` check in `tools/ci/tests/test_collect_ci_evidence.py` (L1087-1168).
- [x] **Timeout error mapping matches the tests and docs.** `AzureClient.get` (L215-237) rechecks the original deadline first. A direct `TimeoutError`, or a `URLError` wrapping one (how `do_open` reports request-phase timeouts), becomes "query timed out". Everything else gets the generic message, and no response or error text is echoed.
  - This matches the four subcases of `test_http_timeout_errors_report_deadline_or_query_timeout` (L1251-1287) and the deadline paragraph in `ci-evidence.md`.
  - `test_final_collection_must_finish_before_the_original_deadline` (L1364-1419) covers the final recheck in `main()`.
- [x] **The streaming test's ordering is sound.** Body bytes are sent only after `response_started`. The clock moves to 101 only after the first byte plus 20 ms, so the client stops after at most 2 body bytes. `body_finished` needs byte 12, about 0.22 s later. The sender join is bounded.
- [x] **The watcher follows `ci-triage.md` "Waiting for Azure Pipelines".** In `watch_azure_pipeline.py` (full file):
  - Each poll creates `AzureClient(deadline=deadline)` against the same monotonic deadline derived from the kickoff time.
  - An `EvidenceError` after the deadline returns `timeout`.
  - The single confirmation poll runs only when more than 600 s remain.
  - `agent_loss` is printed once per identifier/attempt pair.
  - The exit-code map matches the documentation.
- [x] **The guarded trigger and readiness fields behave as documented.** In `Get-PrReadiness.ps1`:
  - `-RunPipeline` requires one PR, both expected SHAs, and no `-WaitForReview` or `-RequestReview` (L103-111). Around L610-650 it also requires `evidenceComplete`, an open PR, and no exact `microsoft.SynapseML` check on the head.
  - `humanApproval` is `unknown` when policy collection is incomplete (L569-579).
  - The output includes `ruleRequiredChecks` and `classicProtection`, which the exempt-diff guidance in `readiness-gates.md` "Review and validation" relies on.
- [x] **The CI-scope policy is consistent across the docs and matches the user's stated rules.** Checked in `SKILL.md` §7 and §9, `readiness-gates.md`, the `loop-control.md` acceptance rows, `ci-delegation.md`, and the external-contributor `SKILL.md`:
  - only `.github/skills/` and `reviews/` qualify, including helper scripts;
  - any `tools/` change disqualifies;
  - both sides of a rename are checked;
  - an enforced Azure check on an exempt PR is reported as a merge-policy mismatch;
  - the helper's blocked summary is never cited as clear.

  This PR changes four `tools/ci/tests/` files, so it is not exempt.
- [x] **All new cross-document anchors resolve to existing headings.** Checked `#7-run-and-triage-full-ci`, `#validation-only-handoff`, `#comment-trigger-gate`, `#user-value-and-scope`, `#review-and-validation`, `#github-snapshot`, `#azure-merge-and-named-tests`, `#artifact-retention`, `#evidence-invalidation`, `#evidence-required-before-triggering`, `#waiting-for-azure-pipelines` and `#targeted-infrastructure-retry`.
- [x] **The existing `CIHelpers` job runs all four helper suites.** `pipeline.yaml` is unchanged and outside the manifest. Its `CIHelpers` job (L112-126, ubuntu-22.04) runs `python3 -m pytest tools/ci/tests/ -q`.
  - The readiness suite's `skipUnless(pwsh or TF_BUILD)` (`test_get_pr_readiness.py` L249-251) fails rather than skips in Azure, as documented.
- [x] **Test counts match the supplied evidence.** There are 45 collector and 42 watcher `def test_` functions, 87 in total, matching the Windows count. On Linux, 85 passed plus the 2 `skipUnless(win32)` tests (`test_collect_ci_evidence.py` L906 and L934) gives the same 87.
- [x] **No unrelated or generated files are included.** The 17 manifest paths are the skill docs, four helper scripts and their four test suites.
  - Local `*.pyc` files are ignored by `.gitignore` L19.
  - There is no `__pycache__` under the skills scripts directory, which the guidance verifier would otherwise reject.
- [x] **The guidance verifier's protections are in place.** `verify_trusted_guidance.py`:
  - removes `GIT_*` environment variables and runs with `--no-replace-objects` and `core.fsmonitor=false`;
  - compares the pinned tree, index, working files, and untracked or ignored files;
  - rejects symlinks and FIFOs before reading them.

  `trusted_executable` skips PATH entries inside the current directory and resolved executables that land inside it.
- [ ] I did not run the tests, Black, the PowerShell harness or the real-TLS probe, because this review is read-only. I relied on the supplied local evidence for those results.
- [ ] No hosted CI result covers this pending patch. Full Azure validation of the current head is still required after publication.
- [ ] The supplied diff text was cut off partway through the `SKILL.md` §8 hunk. I reviewed the named files directly in the worktree but could not confirm that they match frozen index tree `5950b3b8…`.
- [ ] I checked `test_trusted_guidance.py`, `test_watch_azure_pipeline.py` and most of `test_get_pr_readiness.py` against their test lists and selected sections, not line by line.
- [ ] Live Azure and GitHub values were not checked against the services: the project GUID, definition `17563`, the `A365` alias, and API field shapes such as whether timeline `errorCount` can be null.
- [ ] Residual risk, consistent with the documented scope (not a defect): connect, TLS handshake and request send each use the budget computed once at `open()`, capped at 60 s per operation, and DNS lookup is not bounded by the socket timeout. An overrun cannot produce passing evidence, because `main()` rechecks the original deadline.