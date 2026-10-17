## Review Summary
- Round: 3
- Theme: Edge Cases & Robustness
- Mode: sequential direct-task fallback
- Requested Model: claude-opus-5.5 max (primary gpt-6-astra; no observed CLI receipt claimed)
- Issues Found: 1
- Verdict: ISSUES_FOUND

## Evidence Checklist
- [x] **Inputs:** `F\review.diff` is 132097 bytes and its SHA-256 matches `4b588f9a…761a3`. I read the full diff (2798 lines). W's HEAD is the base commit `0543bf7`, and all 17 staged W files match the manifest blobs. `F\round-3.md` was confirmed absent and I didn't touch it.
- [x] **Azure client exceptions:** `collect_ci_evidence.py` is a new file (+537 lines). In `AzureClient.get`, lines 101-113 wrap `json.load(response)` (line 105) and catch only `HTTPError`, `URLError`, `OSError` and `ValueError`. I checked the class hierarchy with standard-library Python only (no candidate code). `http.client.IncompleteRead`, `BadStatusLine` and `LineTooLong` are not subclasses of `OSError` or `ValueError`; only `RemoteDisconnected` is an `OSError`. Nothing in `tools\ci\tests` tests `URLError`, `HTTPException` or opener failures.
- [x] **Error propagation:**
  - In `watch_azure_pipeline.py`, lines 152-170 convert only `EvidenceError`. `main()` (lines 361-367) catches only `MonitorError` and `KeyboardInterrupt` before it prints the `finished` event.
  - In `collect_ci_evidence.py`, `main()` (lines 505-534) catches only `EvidenceError`.
- [x] **Deadline and retry:**
  - `watch_azure_pipeline.py:115-118` sets the deadline from `kickoff_at + timeout_minutes`. So an authorized infrastructure retry keeps the original deadline for the same build.
  - The deadline is re-checked after every GitHub and Azure call, and there is no automatic retry.
  - `collect_ci_evidence.py:67-75` (`remaining_timeout`) caps both the Azure token request and the Azure API calls at that same deadline.
- [x] **Checked and not defects:**
  - `Get-PrReadiness.ps1:295-304,519`: a failure on the classic `/protection` endpoint lands in `collectionErrors`, so the check fails closed.
  - `verify_trusted_guidance.py:111-122`: errors are mapped; the dirty-index, CRLF and symlink checks are fine.
  - The review gauntlet text was extracted unchanged except for relative-link fixes.

## Issues
### Issue 1
- **Severity:** Medium
- **File:** `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Lines:** 101-113 (`AzureClient.get`). The effect shows up at `watch_azure_pipeline.py:152-170, 361-367` and `collect_ci_evidence.py:505-534`.
- **Description:**
  - The Azure response body is read and parsed with `json.load(response)` inside `with self.opener.open(...)`.
  - A truncated body raises `http.client.IncompleteRead`. This happens with a connection reset mid-body, a chunked or Content-Length mismatch, or a proxy cutting the response.
  - A malformed status or header line raises `BadStatusLine` or `LineTooLong`. urllib's `do_open` wraps only `OSError` from `h.request` into `URLError`; errors from `getresponse()` pass through as-is.
  - All three are `http.client.HTTPException` subclasses, which are neither `OSError` nor `ValueError`. They escape as non-`EvidenceError` exceptions, unlike every other failure in this file.
- **Risk:**
  - In `--azure-timeline` mode, the watcher calls `collect_build(AzureClient(...))` on every poll until the original deadline. One truncated response skips `monitor()`'s `except EvidenceError`, including its "past the deadline, so return `timeout`" handling. It also skips `main()`'s `MonitorError` and `KeyboardInterrupt` handlers.
  - The process then dies with a raw traceback and never prints the `"event": "finished"` JSON record that every other exit path prints (line 367).
  - The diagnostics collected across polls are lost. These are the timeline and job-attempt data in `timeout_result["azure"]` (line 170), which the operator uses to judge whether the one authorized infrastructure retry is warranted.
  - The standalone collector likewise prints a traceback instead of its documented `{"outcome":"error","buildId":…}` JSON.
  - The file is new, so this defect comes from this change and isn't pre-existing.
- **Fix:**
  - Import `http.client` and add `http.client.HTTPException` to the second except tuple, mapping it to the same `EvidenceError`: `except (URLError, OSError, ValueError, http.client.HTTPException)`.
  - Add a test in `tools\ci\tests\test_collect_ci_evidence.py`: a stubbed opener whose response raises `http.client.IncompleteRead(b"{")` on read. It should assert that `AzureClient.get` raises `EvidenceError`.
  - Add a watcher test asserting that this case ends in a `finished` event with outcome `error`, or `timeout` when the deadline has passed.

## Driver disposition

Accepted: protocol/truncated-response failures need the same sanitized,
structured failure contract as other Azure transport errors. No patch has been
applied yet. The configured six pre-commit passes and five fix cycles have been
consumed; further correction and a new complete pass require a budget extension.
Pass 6 stops here; its first two clean rounds cannot be combined with a later
patch. Source base `0543bf7e5266c587207ae8c28c3b696deeed3ed1`; diff SHA-256
`4b588f9a9ecba2f7852b0db06b95571493c624cd40d6219f0142b386afc761a3`.

## Driver resolution after authorized continuation

The user directed autonomous continuation after the budget stop on 2026-10-16.
The prior cap is extended for that continuation; attempt numbering and the
complete six-clean-round requirement are unchanged.

- Added `HTTPException` to the Azure client's sanitized transport-error mapping.
- Regression coverage includes response-body truncation, malformed status and
  header errors, response closure, and failures while opening the response.
- A watcher entry-point test uses the real client error path and proves both
  structured `finished/error` and original-deadline `finished/timeout` outcomes,
  without leaking response fragments or sleeping.
- Validation passed: 70 tests and 87 subtests across the four helper suites;
  Black 22.3.0 left all seven Python files unchanged; `git diff --check` passed.
- A new complete pass will review the corrected patch. Earlier clean rounds are
  retained only as historical evidence.

## Publication record

- PR: microsoft/SynapseML#2775
- Source publication commit: `66d08e29cc9baf9d74a71f69ce7f59c6b9f1d299`
- Reviewed input manifest SHA-256: `241b78cfe3bbbcc46dd8c4cd3ab8b5594d0521bc4bebc5745e2b6a3daa591e93`
- Reviewed diff SHA-256: `4b588f9a9ecba2f7852b0db06b95571493c624cd40d6219f0142b386afc761a3`
- Original artifact SHA-256: `b8615ef8522d198889526c6b7679af288d058ee7579ec6f0888a84208bac3c20`
- Dispatch mode: manual prompt fallback; no observed CLI receipt is claimed.
