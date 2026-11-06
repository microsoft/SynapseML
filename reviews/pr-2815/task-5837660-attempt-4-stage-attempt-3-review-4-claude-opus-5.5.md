---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 3
round: 4
timestamp: "2026-11-06T22:39:56Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "72fa83e1eb8946ff966f5a6f4c6de9767bf190c7575d3bf759db3c242375a062"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-3-review-4-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Submission diagnostics:** I traced the corrected `safeSubmission` flow (`ContentUnderstandingProtocol.scala` 540-556) through `decodeSubmission`, `uncertainSubmission`, `SubmissionLocation.assess` and `withDiagnostic` (`ContentUnderstandingResponseParsing.scala` 113-160).
  - An unsuccessful response with a location always becomes `Unknown` because the non-accepted status and location make `uncertainSubmission` true. The location validation error is added to the service error.
  - When 203/206 is terminal, `unacceptedSuccess` now produces `UnacceptedOperationLocation` or `InvalidOperationLocation`, and the service error is kept as `details` or `innererror`.
  - A handle is used only when the status is accepted and the location validates (`location = if (accepted) validatedLocation`).
- [x] **Merged-error size:** I checked whether merged errors stay within parser limits. A service error parsed from the body has depth ≤ 999. Wrapping it in `innererror` or `details` adds at most one level, so later `asObject` calls stay within `MaxDepth = 1000`. `compact` also catches `StackOverflowError` and falls back to the original error.
- [x] **Creation 404/410 lookup:** `decodePoll` (566-577) only wraps the error as `OperationLookupFailed` when `raw.diagnostic.isEmpty && !raw.transportFailure`.
  - `retainPollState` (579-595) now uses `parsed.status != "ResultUnavailable"`. For creation lookups, the previous `Running` state, raw body and handle are kept, and the HTTP status and error are updated.
  - The loop stops because `parsed` is `Failed`, is not ongoing and is not retryable. `createAnalyzer` (771-776) then throws with the retained state.
  - `ResponseTooLarge` and `TransportError` bypass the wrap through the `raw.diagnostic` guard.
  - `OperationLookupFailed` was added to `DiagnosticCodes` (`ContentUnderstandingException.scala`).
- [x] **Analysis 404/410 under the new `nonTerminalHttpError` condition:** A readable body still gives `ResultUnavailable` with the handle. An oversized or unreadable body is retained through `raw.diagnostic` or `raw.retryable`, or becomes `Unknown` when there is no previous state. This matches the updated documentation paragraph.
- [x] **DNS admission and cancellation (`ContentUnderstandingDnsResolver.scala`):**
  - The `acquire` FIFO uses the head check plus `tryAcquire(0, ms)` on a fair semaphore that has no parked threads. `wait(min(remaining, 50))` is always at least 1 ms because `remaining == 0` throws first.
  - The waiter is removed and `notifyAll` is called on every exit path. `release` runs on every path that does not submit, and capacity is held until the native resolve finishes.
  - `awaitResolution` uses its own elapsed-time limit, so even a constant budget provider has a finite wait.
  - An interrupt is converted to `InterruptedIOException` with the flag restored, and `CancellationException` passes through `MainClientExec`'s `RuntimeException` abort path.
- [x] **Task ownership (`Clients.scala` 50-93):**
  - `Owner` is captured on the task thread and restored or removed in `finally`.
  - An ended task is rejected before the request body runs.
  - `InterruptedException` (which `NonFatal` treats as fatal) is converted to `CancellationException` and the pool thread's interrupt flag is cleared.
  - `rowHandler` catches only `ContentUnderstandingException`, so cancellation is not swallowed. I checked that `HTTPClients.scala` 63-67 adds no catch.
- [x] **Deadline and cancellation abort (`ContentUnderstandingProtocol.scala` 133-168, 380-401):** The periodic poller shares the `fired` CAS with the deadline abort and is cancelled in `release`. The executor has `setRemoveOnCancelPolicy(true)`. The UnknownHost and IO paths call `checkInterrupted()` before building a diagnostic, so an abort caused by cancellation is not misreported as a timeout. `pause` checks for cancellation at least every 50 ms and still sleeps for at least the requested delay.
- [x] **Python wrappers:**
  - The `setParams` override checks service `None` and binding conflicts for all arguments before any setter runs, then skips ordinary `None`.
  - `_transfer_params_from_java` now syncs the JVM default for every non-service param, or drops the Python default when the JVM has none.
  - `_service_value_to_python` turns Scala `Seq` into a list before the `Map` branch, and raw bytes return early.
- [x] **`SimpleHTTPTransformer.errorSchema` against runtime behaviour:**
  - The exact-name `coalesce` matches `findWiderCommonType`, which reduces to `findWiderTypeForTwo` for two struct inputs.
  - Nullability is `existing.forall(_.nullable)` because the UDF side is nullable.
  - Case-only replacement follows the resolver, and the `unparsedOutput` column is nullable after `NullifyResponseUDF`.
- [ ] **Execution evidence:** Not checked. Reviewer tools were read-only, so I did not run tests. I relied on the driver-supplied validation provenance.

## Issues

### Issue 1: Analyzer-creation PUT with a non-accepted 2xx status and `Operation-Location` throws a response that can report `Succeeded`, with no diagnostic or recovery guidance
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 758-761 (`createAnalyzer`: `if (raw.location.isDefined && !AcceptedSubmissionStatuses(raw.httpStatus)) throw new ContentUnderstandingException(decode(raw, None))`)
- **Description**: This delta made non-standard 203/206 submission responses explain the discarded handle, using `UnacceptedOperationLocation`, `InvalidOperationLocation` and the kept service error. The analyzer-creation submission path has the same "location returned with a non-accepted status" case, but it still throws a bare `decode(raw, None)`.
  - For HTTP 203/206 with `{"status":"Succeeded"}`, `decode` gives `status = "Succeeded"` and `error = None`. `successful` is true, `serviceStatus` maps, and no `httpError`, `missingStatusError` or `failedError` applies.
  - The exception message therefore reads "status Succeeded, HTTP 203" with no diagnostic code.
  - The handle is discarded silently, and there is no `ManagementRecovery` ("Use getAnalyzer…; do not repeat creation") guidance.
  - With a creation-style body such as `{"status":"creating"}`, the default `StatusNames` gives `Failed`/`InvalidResponse`. That suggests the service sent a malformed body rather than an untrusted handle.
  - No test covers this branch; only the submission suite covers 203/206.
- **Risk**: The service may already have accepted the creation request remotely. A caller who inspects `error.response` sees `Succeeded` or `InvalidResponse` with no explanation and no advice not to repeat the creation. That invites a retry, which conflicts or, with `allowReplace=true`, replaces the analyzer. This contradicts the stated contract that non-standard terminal submission responses must explain uncertainty and discarded handles.
- **Suggested Fix**: In that branch, build the response with `decode(raw, None, CreationStatusNames).copy(status = "Unknown")`. Then attach a diagnostic with `SubmissionLocation.withDiagnostic`:
  - `UnacceptedOperationLocation` when `validateOperationLocation(endpoint, location, Some(analyzerId))` succeeds;
  - `InvalidOperationLocation` when it fails;
  - in both cases, a message that includes `ContentUnderstandingTransport.ManagementRecovery`.

  For unsuccessful statuses, keep the existing `HttpError` or service error and append the location diagnostic the same way the submission path does. Never follow the handle. Add a `ContentUnderstandingSubmissionSuite` case covering creation PUT with 203/206 × {valid, unsafe} location × {Succeeded, creating, object/string error}. It should assert status `Unknown`, the diagnostic code, the kept service detail, `getAnalyzer` guidance and `Seq("PUT")` only.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver disposition after targeted attempt 3

The invocation passed the finalized receipt, actual input/runtime identity and
scoped-tool independence audit, with no violations or warnings. The original
ISSUES_FOUND verdict and issue text above are preserved.

Issue 1 is confirmed and locally corrected. Git history places the rejected
creation branch in native feature commit `085928a9b1`; it was not introduced by
the preceding targeted corrections. The new public createAnalyzer regression
failed before the fix: HTTP 203 with a valid management handle reported Succeeded
instead of Unknown (`cu-focused-t4-red.log`).

The correction uses creation status names and reuses the bounded diagnostic
merger. Analyzer-specific handle validation selects UnacceptedOperationLocation
or InvalidOperationLocation. The exception response now reports Unknown and
getAnalyzer recovery guidance, retaining primary service/HTTP errors and nested
details. The handle is not followed and the PUT is not repeated.

Affected main/test style and incremental compilation passed. All 173 offline CU
Scala tests and three affected Python public-path tests passed
(`cu-focused-t4-green.log`). The new matrix covers 203/206/500, Succeeded/creating
and object/string errors, with valid, cross-origin and wrong-analyzer handles.
Independent targeted re-review remains pending; no live-service, packaged-install,
Azure CI or merge-readiness claim is made. Changes remain uncommitted and unpushed.
