---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 69
stage_attempt: 1
round: 5
timestamp: "2026-10-19T17:51:34Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6017fdc29df3ebb98e52f2b4c38c0edd5b9c76c6109408faf99410cf9785845a"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-69-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Streaming deadline test.** I traced `test_streaming_response_stops_at_the_original_deadline` (`tools/ci/tests/test_collect_ci_evidence.py:1087-1168`) through `DeadlineReader` and `DeadlineHTTPSHandler` (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:126-166`) and the standard library's urllib/http.client code:
  - The expired row can only pass if each receive gets the remaining budget again.
  - `fileno() == -1` (line 1162) can only pass if `DeadlineReader.close()` releases the `SocketIO` reference.
  - The `deadline expired` regex tells deadline expiry apart from a socket timeout.
  - The fixture socket keeps a 2 s timeout, so a regression fails the test rather than hanging it.
- [x] **Header reads.** The response factory replaces `fp` before `HTTPResponse.begin()`, so status, header and chunk-size reads all go through the same deadline reader. If the deadline expires during headers, the socket is still closed through the cleanup in `getresponse` and `do_open`. Tests only cover expiry during the body, but I treat header reads as covered by this structure and raise no separate finding.
- [x] **Watcher timeline branches.** The watcher's timeline branches in `.github/skills/synapseml-pr-loop/scripts/watch_azure_pipeline.py` each have a test in `tools/ci/tests/test_watch_azure_pipeline.py`:
  - absent timeline / pending: lines 113-156
  - one `agent_loss` per attempt: 693-736
  - pending / missing / stale × deadline and latency: 773-868
  - agreement vs `attention_required`: 937-993
  - 7199 / 7200 / 7201 s overrun: 643-691
  - truncated response becomes error or timeout: 594-641
  - single merge verification, with the deadline passed to `AzureClient`: 901-935
- [x] **Collector gates.** Tests cover:
  - the 10-minute budget, via `factory.assert_called_once_with(deadline=700)` at line 1366;
  - redirect refusal with no follow-up request (1057-1077);
  - sanitized protocol and truncation errors (1226-1249);
  - detection of repeated pagination pages (856-872);
  - the failed batch-cleanup matrix, including the `__cause__` chain (969-1003).
- [x] **Readiness harness** (`tools/ci/tests/test_get_pr_readiness.py:21-120, 252-306`):
  - The `gh` stub throws on any unexpected call.
  - It requires `--jq tojson` paging and immutable compare SHAs.
  - It records the trigger and review POSTs and asserts their exact argv.
  - The `pwsh`/`TF_BUILD` skip-or-fail rule (lines 249-251) matches `.github/skills/synapseml-pr-loop/references/ci-triage.md:111-113`.
- [x] **CI discovery** (`pipeline.yaml:112-126`). The `CIHelpers` job runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04, which picks up all four suites. Test-function counts match the supplied totals:
  - collector 44 + watcher 42 = 86, with the two `skipUnless(win32)` tests skipped on Linux;
  - readiness 42;
  - verifier 17 + 44 + 42 = 103, minus two platform skips on each OS = 101.
- [x] **No stray bytecode.** A glob found no `__pycache__` under `.github/skills/` after the local runs. This matches the import-time `dont_write_bytecode` patches and the `--help` subprocess tests (collector 1005-1022, watcher 995).
- [ ] **Tests not executed.** This is a read-only review, so I relied on the supplied Windows and Linux results.
- [ ] **Private real-TLS probe not inspected.** It is not part of the PR, and I was told not to open it. No committed test runs a real TLS handshake.
- [ ] **No interrupt-cleanup test.** The `KeyboardInterrupt` branch of `run_windows_batch` (`collect_ci_evidence.py:49`) has no direct test. It shares the kill path that the Windows-only real test covers, and the docs only promise cleanup on timeout. Under the no-blanket-coverage scope I did not raise it as a finding.
- [ ] **No hosted CI for this patch.** No Azure or final CI-qualified result is claimed for the frozen patch.

## Issues

### Issue 1: The streaming deadline test mocks the HTTPS connection factory but never checks its arguments
- **Severity**: Low
- **File**: `tools/ci/tests/test_collect_ci_evidence.py`, testing `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`
- **Line(s)**:
  - test: 1087-1168, with the patch at 1139-1142
  - production: 147-166, especially the `connection` factory at 161-164 and `do_open(..., context=self._context)` at 166
- **Description**:
  - This is the only test of `DeadlineHTTPSHandler`. It replaces `http.client.HTTPSConnection` with a MagicMock that returns an already-connected plain `HTTPConnection`. The mock is never bound to a name, so nothing checks how it was called.
  - The handler's `connection` factory is the only place that passes `timeout=req.timeout` and `context=self._context` from `do_open` to the real connection.
  - Because the fixture already has a socket, `connect()` and the TLS handshake never run.
  - So the test proves the deadline is enforced on receives, but nothing checks that the connect/handshake timeout or the standard verified HTTPS context reaches the connection. The design claims both.
- **Risk**:
  - A refactor to `http_client.HTTPSConnection(host)` (dropping `**options`), or one that passes an unverified context, would pass every row.
  - Without the timeout, TCP connect, the TLS handshake and the request send would have no timeout at all, because `DeadlineReader` only resets the timeout before receives. A stalled endpoint could then keep the attached watcher or the collector running past its original deadline. That is the liveness failure this change set out to close.
  - The code is correct today; the gap is lost protection against regressions.
- **Suggested Fix**: Make these changes inside the existing test. No new fixture, dependency or mechanism is needed.
  1. Bind the patch: `patch("http.client.HTTPSConnection", ...) as https_connection`.
  2. Find the handler: `handler = next(h for h in client.opener.handlers if isinstance(h, evidence.DeadlineHTTPSHandler))`.
  3. In both rows, assert `https_connection.assert_called_once_with("dev.azure.com", timeout=1, context=handler._context)`. The expected timeout is `min(2, 100 - 99)`.
  4. Import `ssl`. When `handler._context` is not `None` (Python 3.12+ builds it in `HTTPSHandler.__init__`), also assert `verify_mode == ssl.CERT_REQUIRED` and that `check_hostname` is true.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver adjudication

### Issue 1: declined as a blocking requirement

- **Disposition**: optional test hardening, not a demonstrated defect. The
  observation that the mock's constructor arguments are not asserted is
  accurate; the reviewer explicitly confirms that the implementation is correct.
- **Why no source change**: the requested regression is interruption of a
  streaming response at the original deadline. The committed test exercises that
  public urllib path, normal completion, interruption before body completion,
  and socket cleanup. Its expired row fails against the previous implementation.
  The proposed additional assertions check constructor forwarding and the
  standard library's default SSL configuration against hypothetical future
  edits, rather than expose incorrect behavior in this patch.
- **Evidence**: `DeadlineHTTPSHandler` forwards the connection options unchanged
  and passes its standard HTTPS context at
  `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:161-166`.
  The existing regression is at
  `tools/ci/tests/test_collect_ci_evidence.py:1087-1168`. The recorded real-TLS
  Python 3.10 probe additionally verified normal completion, original-deadline
  interruption, and peer/socket cleanup with a verified certificate.
- **Scope decision**: retain the focused regression and do not turn optional
  mock-argument coverage into another content-change/review cycle. No production
  or test file changed. There are no accepted unresolved findings from this lens.
  The original `ISSUES_FOUND` verdict and feedback above remain unchanged; this
  disposition is the driver's assessment, not a rewritten reviewer verdict.