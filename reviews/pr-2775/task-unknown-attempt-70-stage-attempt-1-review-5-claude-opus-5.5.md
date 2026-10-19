---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 70
stage_attempt: 1
round: 5
timestamp: "2026-10-19T19:16:25Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "125a8b0d85c2454ccd1dc9bf06d78490e060e841d7edc2455b15d3a79eb50dae"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2775/task-unknown-attempt-70-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Collector source read in full** (`.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`).
  - `DeadlineReader` (126–144) sets the socket timeout to `min(60, remaining)` before every raw `readinto`.
  - `DeadlineHTTPSHandler` (147–166) installs the reader in the response factory before `begin()`. So the status line, headers, chunk framing and body are all bounded.
  - `EvidenceError` (36) subclasses `Exception`, so it is not caught by the `except` tuple at 229 or by urllib's cleanup.
  - `close()` releases the detached `SocketIO`, so the file descriptor closes on cleanup.
- [x] **Streaming test mocks are adequate** (`test_streaming_response_stops_at_the_original_deadline`, `tools/ci/tests/test_collect_ci_evidence.py` 1087–1168).
  - It uses a real opener, `HTTPConnection` and `HTTPResponse` over a `socketpair`. Only `HTTPSConnection` (with `debuglevel=0`, needed by the 3.12+ handler init), `getproxies` and `time.monotonic` are mocked.
  - Without the per-read check, the 20 ms/byte trickle would finish well inside the socket timeout, so the expired row would fail.
  - Without the `close()` override, `fileno() == -1` would fail, because the saved traceback keeps the response alive.
  - This matches the red/green results you supplied. I did not reproduce those historical runs.
- [x] **Timeout diagnostic test catches both halves of the change** (1251–1287).
  - The expired rows (clock exactly at the deadline, 100) would fail if the deadline recheck at 230 were removed.
  - The non-expired rows would fail if the classification at 231–234 were removed.
  - The read-failure rows assert `response.close`, and private text is excluded.
- [x] **Grep confirms the gap in Issue 1.**
  - In the test module, `URLError` appears only at the import (21) and at 1261, where it wraps a `TimeoutError`.
  - No test asserts "failed or returned invalid JSON".
  - The protocol test (1226–1249) checks only the exception type with `assertRaises`.
- [x] **Considered and not reported: `HTTPError` skips the recheck.**
  - The `HTTPError` branch (223–228) runs before the deadline recheck, although `references/ci-evidence.md` 197–199 says "HTTP failures also recheck".
  - To reach that branch, the status and headers must be received through `DeadlineReader`, which raises first once the deadline has passed.
  - The watcher also turns any error raised after the deadline into a `timeout` result (`watch_azure_pipeline.py` 157–161).
- [x] **Watcher reuses the collector client.**
  - `watch_azure_pipeline.py` imports the collector's `AzureClient` and `EvidenceError` (17–22) and wraps every message the same way in `MonitorError`. So Issue 1 can only affect diagnostics.
  - I inspected the timeline, agent-loss and single-extra-poll tests (`test_watch_azure_pipeline.py` 594–691, 773–993).
- [x] **Readiness tests inspected.**
  - The harness in `test_get_pr_readiness.py` (21–144) mirrors `gh --paginate --jq tojson`.
  - Trigger and review-request side effects are asserted (902–1046).
  - Evidence, engineering, human-approval and merge states are tested separately, for example in `test_human_approval_is_separate_from_engineering_evidence`, `test_zero_approval_rules_…` (7 rows × classic/ruleset = 14 subtests) and `test_unevaluated_merge_rules_…`.
- [x] **Trusted guidance read in full**: `verify_trusted_guidance.py`, `references/trusted-guidance.md` and `test_trusted_guidance.py`, including documented-flag parsing (128–172, 221–261), FIFO and directory-replacement cases, and the fsmonitor case.
- [x] **Hosted CI path checked.**
  - `pipeline.yaml` CIHelpers (112–126) runs `python3 -m pytest tools/ci/tests/ -q` on ubuntu-22.04. This is documented in `tools/ci/README.md` at line 144.
  - The readiness suite uses `skipUnless(shutil.which("pwsh") or TF_BUILD)`, so under Azure it fails rather than skips if `pwsh` is missing.
- [x] **Policy docs are consistent.** These all agree:
  - the step 7 scope exception in SKILL.md: any `tools/` change, including tests and either side of a rename, disqualifies it;
  - how `references/ci-evidence.md` treats an exempt PR's Azure entry, using `ruleRequiredChecks` and `classicProtection.required_status_checks`;
  - the `ci-delegation.md` links and acceptance rows.

  This PR changes four `tools/ci/tests` files, so it is correctly not exempt.
- [ ] **Tests not executed.** This lens is read-only. All pass counts and the Black result are your local evidence, not reproduced here.
- [ ] **No hosted CI qualifies this pending patch.** The predecessor's fast checks ran before the collector change.
- [ ] **Diff display was truncated** inside the pr-loop `SKILL.md`. I read the scripts in full and the test files in the ranges cited above. Reference Markdown was reviewed in targeted ranges, not line by line.
- [ ] **No real-TLS test in the suite.** Per-receive timeouts on TLS sockets were reasoned from CPython behavior. I did not open the private TLS probe, as instructed.
- [ ] **Windows-only batch-launcher paths** (including the `KeyboardInterrupt` cleanup branch) were not executed. Hosted CIHelpers runs only on Linux, so these depend on your local Windows run.

## Issues

### Issue 1: The new timeout test never checks that other failures keep the generic message
- **Severity**: Low
- **File**: `tools/ci/tests/test_collect_ci_evidence.py` (production: `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py`)
- **Line(s)**: tests 1226–1249 and 1251–1287; production 229–237
- **Description**:
  - Lines 231–234 report "Azure evidence query timed out." only for a `TimeoutError` or for a `URLError` whose reason is a `TimeoutError`. Any other transport, protocol or JSON failure with budget left must keep "Azure evidence query failed or returned invalid JSON."
  - The four new rows feed in only timeout errors.
  - The only other test that reaches this branch (`test_protocol_and_truncated_body_errors_are_sanitized`) checks just the exception type and that private text is absent.
  - No test sends a non-timeout `URLError` or `OSError` (connection refused, DNS `gaierror`, `SSLCertVerificationError`, connection reset) through `AzureClient.get`. No test checks the generic message.
  - As a result, the suite would still pass if the check were simplified to `isinstance(error, URLError)` or `isinstance(error, OSError)`, or if every failure were reported as "timed out".
- **Risk**: This does not block the PR. The code is correct today, and the watcher handles every `EvidenceError` the same way. However, a later edit could make DNS, refused-connection or TLS-verification failures show up as a temporary "query timed out". That would steer triage toward waiting or retrying instead of fixing configuration or trust, and no test would catch it. That would undo the precision this patch adds.
- **Suggested Fix**: Add one negative case to the existing test (or a sibling test) with budget remaining:
  - `opener.open` raises `URLError(ConnectionRefusedError("private response"))`;
  - `response.read` raises `ConnectionResetError("private response")`.

  For both, assert the anchored message `^Azure evidence query failed or returned invalid JSON\.$`, that neither "timed out" nor the private text appears, and that `response.close` is called for the read case. A simpler alternative is to make the protocol test assert that anchored generic message instead of only the type. No production change is needed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending. The fix is one focused negative case. If it is deferred, the reason should be that the timeout/generic difference only affects diagnostics.
- **How verified**: Pending. Expected check: the collector suite command you supplied passes with the new case. Ideally the case also fails if the timeout check is temporarily broadened.

## Driver adjudication

### Issue 1: nonblocking optional coverage

- **Disposition**: no required change. The reviewer explicitly states that this
  does not block the PR and that the current implementation is correct.
- **Reason**: the new regression exercises both timeout shapes before and at
  deadline expiry, including sanitization and read cleanup. The existing
  protocol/truncation test exercises the unchanged generic failure path and
  sanitization. The suggested assertion would harden the diagnostic wording
  against a hypothetical future broadening of an otherwise correct type check;
  it does not identify a present behavioral defect.
- **Evidence**: the conditional at
  `.github/skills/synapseml-pr-loop/scripts/collect_ci_evidence.py:229-237`
  admits only `TimeoutError` or `URLError` wrapping `TimeoutError`. Other
  exceptions still reach the existing generic error. Current Windows and
  Linux collector/watcher runs passed, as recorded in the review context.
- **Scope**: retain the focused tests without another source change or review
  restart for optional diagnostic-message coverage. No accepted unresolved
  finding remains. The original `ISSUES_FOUND` result is preserved above;
  this is a driver disposition, not a replacement reviewer verdict.