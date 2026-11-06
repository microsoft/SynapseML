---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 21
round: 1
timestamp: "2026-11-05T07:59:21Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "8e886e51082fecdeb87e7a429098ad75d3c5a1c5e040fd40f240df3cfd071246"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-21-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Production sources swept.** All CU production files under `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\` were reviewed: `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, `ContentUnderstandingParams.scala`, `ContentUnderstandingDnsResolver.scala`, `ContentUnderstandingResponseParsing.scala`, `ContentUnderstandingTransport.scala`, `ContentUnderstandingBudget.scala` and `ContentUnderstandingHeaderValidation.scala`. Required dependencies were also read: `CognitiveServiceBase.scala` (ServiceAuthHeaders, HasCustomHeaders, addHeaders), HTTPSchema, SimpleHTTPTransformer, HTTPTransformer, HTTPClients, SharedVariable, DatasetExtensions, `core\...\codegen\Wrappable.scala` and `core\src\main\python\synapse\ml\core\schema\Utils.py`.
- [x] **Shared stop conforms** (`ContentUnderstandingProtocol.scala`):
  - rowHandler (656-681) keeps a per-task `stopped` AtomicBoolean and checks it before `execute` for non-poll modes.
  - The nested innererror is added only when `rawResponse` is empty and the code is one of TransportError, OperationTimedOut, ResponseTooLarge or ResponseBodyReadFailed, so service messages that look like connector diagnostics are not echoed.
  - `checkSharedFailure` (~496-509) requires no accepted handle and a status other than `Unknown`.
  - `isSharedAuthFailure` requires: no row-credential marker, and either a scalar credential fingerprint was sent or `allAuthScalar` is true, and HTTP 401/403.
  - `rowUsesColumnAuth` in `ContentUnderstanding.scala` follows the same precedence as `ServiceAuthHeaders.resolve`: subscriptionKey, then AADToken, then CustomAuthHeader, then a credential embedded in customHeaders.
- [x] **Admission classification conforms.** In `exchangeBeforeDeadline` (361-411):
  - A 401/403/429 POST without a handle whose body read fails gives `ResponseBodyReadFailed` and `Rejected`.
  - `ResponseTooLarge` is a RuntimeException, caught first, and stays `Rejected` through `decodeSubmission`.
  - A contradictory handle gives TransportError and `Unknown`.
  - An unreadable 404 stays `Unknown` through `uncertainSubmission` and does not stop submissions.
  - A header-parse failure leaves code 0 and no handle.
  - `notSent` comes from the request-write-started context attribute.
- [x] **No automatic POST/PUT replay.** `WireResponse.retryable` (157-158) is used only by the poll loop and the management GET loop, and `expiredBeforeDispatch` is never retryable. `createAnalyzer` keeps `raw.diagnostic` ahead of `InvalidOperationLocation` for unsafe handles. A final-retrieval failure keeps the retrieval error, httpStatus and raw response.
- [x] **Error precedence conforms.** The top-of-loop path in pollWithClient (586-619) lets a prior error win over timeout. `retainPollState` (570-584) keeps a prior error on pre-dispatch expiry. The management `get` loop (698-713) returns the previous WireResponse, including its body. Newer dispatched failures win. This was cross-checked against `ContentUnderstandingAdmissionSuite.scala` 98-182.
- [x] **Budget and DNS conform.** HTTP phase timeouts use min(timeout, remaining) (366-371), and DeadlineControl aborts at the remaining budget. `BudgetedDnsResolver.diagnostic` (DnsResolver 84-87) and the capacity, post-acquisition recheck and worker-lifetime behaviour match the spec and `ContentUnderstandingAdmissionSuite.scala` 184-206.
- [x] **Parsing bounds conform:**
  - JsonBounds limits nesting depth to 1000 and numeric tokens to 100 characters, and is string- and escape-aware.
  - Parsing failures are caught for NonFatal and StackOverflowError, and error-code traversal is iterative.
  - Numeric Retry-After allows at most 19 digits and saturates; HTTP dates still work.
  - Header lines are limited to 64 KiB and 200 headers.
  - The output envelope is shallow, because rawResponse and error are escaped strings.
- [x] **Resolver and session handling conform.** `validateParamSchema` requires an exact name plus exactly one resolver match. Output/error collisions use the resolver. `transform` uses `DatasetExtensions.resolverFor(dataset)` without changing SQLConf. Schema-only calls use `SQLConf.get.resolver`.
- [x] **Python wrapper conforms.** I checked the generated `isSet`, ServiceParam getter and stub exclusion in `Wrappable.scala`, plus `hasDefault`, `getOrDefault`, `extractParamMap`, `explainParam` redaction, `clear` and the transfer methods in `Utils.py`. PySpark's `isDefined` delegates to `isSet` and `hasDefault`, which matches the assertions in `test_ContentUnderstanding.py` at lines 289, 478, 496, 501, 573, 577 and 585.
- [x] **Shared-trait changes are safe.** `CognitiveServiceBase.scala` `addContentType` removes case variants only when the service supplies a content type, so duplicate Content-Type headers are no longer sent. The new public `getCustomHeaders`, `setCustomHeadersCol` and `getCustomHeadersCol` do not conflict with the protected row overload.
- [x] **Docs match the spec.** In `docs\Explore Algorithms\AI Services\Content Understanding.md`:
  - Lines 72-135 cover credential persistence, Python-only redaction ("Scala inspection … still contain configured values") and MLflow parameter logging.
  - Lines 200-412 cover the shared stop, resumable selection by handle plus NotStarted/Running/Unknown, bounds, rejection of concurrentTimeout, and the best-effort duration.
- [x] **`pipeline.yaml` 821-822** only adds the `services.contentunderstanding` test-package selector, which was human-approved.
- [x] **Dismissed candidates:**
  - Python `isDefined` divergence: PySpark delegates to the overridden methods.
  - TransportError wording on a 202 with a handle: "may have been accepted; it was not resubmitted" is accurate.
  - Scala `explainParams` showing credentials: documented as intended.
  - Per-operation HttpClient: a performance cost, not a correctness defect.
- [ ] I ran no tests or commands; this was a read-only review. The validation results (Scala 119/120 plus the 1/1 rerun, Python 25/25, codegen, Black) are author-supplied and were not reproduced.
- [ ] Live CU service and Azure CI evidence were unavailable. This is an external limit, not a code defect.

## Issues

### Issue 1: Expiry before dispatch with no prior error overwrites the kept poll state's httpStatus, and connect deadlines report RequestNotSent instead of the timeout
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`. Related: `ContentUnderstandingTransport.scala` and `cognitive\src\test\scala\...\contentunderstanding\ContentUnderstandingAdmissionSuite.scala`.
- **Line(s)**:
  - `ContentUnderstandingProtocol.scala`: 576-580 (`retainPollState`); compare with 600-602 (top-of-loop timeout) and 604-612 (path after the wait).
  - Where `expiredBeforeDispatch` is produced: 353-355, 391-395 and 397-407.
  - `ContentUnderstandingTransport.scala` 27-30.
  - `ContentUnderstandingAdmissionSuite.scala` 111-127.
- **Description**:
  - **Where the paths differ.** `retainPollState` treats `raw.expiredBeforeDispatch` specially only when `previous` already has an error. If the previous state has no error (a 200 Running poll, or a clean 202 submission in analyze mode), the synthetic unsent `WireResponse(0, …)` falls through to the generic transient branch (`raw.diagnostic.isDefined`). That branch returns `previous.copy(httpStatus = 0, error = parsed.error)`.
  - **When this path is taken.** The budget runs out during `budget.pause`, during DNS, or during connect before the request is written.
  - **What the other path returns.** When the same situation is caught by the top-of-loop guard (`budget.remainingMs == 0 || delay >= budget.remainingMs`), the result is `previous.copy(error = OperationTimedOut)` with the previous httpStatus kept.
  - **Connect deadline.** When `notSent && expired`, `ContentUnderstandingTransport.failure` returns `RequestNotSent: <Exception>: The HTTP request was not sent.` instead of `OperationTimedOut`. A DNS deadline or expiry at the start of `exchange` does give `OperationTimedOut`.
  - **Net effect.** An attempt that never reached the service still changes the output. Depending on timing, httpStatus is either 0 or the previous 200/202, and the error is either OperationTimedOut or RequestNotSent.
- **Risk**:
  - **No data loss.** No handle, remote status or rawResponse is lost. Resumable selection (handle plus NotStarted/Running/Unknown) still works.
  - **Inconsistent rows.** A row can show `status=Running`, `rawResponse` holding a 200 Running body, and `httpStatus=0`.
  - **Misleading triage.** The docs use HTTP status zero for unsent or skipped rows, and the spec says "No prior useful error means timeout remains appropriate." Consumers who triage by httpStatus or error code may read budget exhaustion as a connectivity or unsent failure.
  - **No test guard.** The new deterministic no-prior-error test (AdmissionSuite 111-127) asserts status, error, raw response, handle and request count, but not httpStatus. The divergence is untested.
- **Suggested Fix**: In `retainPollState`, handle every `raw.expiredBeforeDispatch` case where `previous` is defined the same way as the top-of-loop path:
  ```scala
  if (raw.expiredBeforeDispatch && previous.isDefined) {
    val prior = previous.get
    if (prior.error.isDefined) prior else prior.copy(error = Some(timeoutError))
  }
  ```
  Keep the existing behaviour when `previous` is `None`; optionally normalise its error to `timeoutError` as well. Then:
  - Add `httpStatus` assertions to the no-prior-error test.
  - Add a variant with a connect-phase deadline (`notSent && expired`) after a successful Running poll.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Driver resolution - attempt 22
- **Status**: Fixed locally; independent re-review pending. Original finding and verdict retained.
- **What/why**: `ContentUnderstandingProtocol.scala` now retains every defined previous poll state on an expired unsent attempt, rather than only states already carrying an error. It preserves the previous HTTP status, remote state, body and handle, and fills an absent error with `OperationTimedOut`, matching the top-of-loop deadline path. Existing errors remain authoritative. A first failure without previous state keeps its original diagnostic.
- **Regression evidence**: `ContentUnderstandingAdmissionSuite.scala` asserts retained HTTP 200 after a clean Running poll and HTTP 202 after accepted submission. Scripted connect deadlines test clean and failed prior states, with genuine DNS and immediate-capacity-rejection controls. Real occupied-DNS-capacity tests exercise four GET routes, and a first-capacity-failure control preserves `DnsCapacityExceeded`. The pre-fix admission run failed seven meaningful cases; the fixed full CU run passed 127/127.
- **Validation**: Cognitive main/test style, compilation, codegen/publication passed; offline Python 25/25 passed against matching built/published/loaded JAR hashes. Black: 205 unchanged; whitespace clean. Full hashes, evidence paths and the next driver decision are retained in the GPT attempt-21 artifact.
- **Review audit/runtime**: Original Opus result persisted with matching canonical input SHA-256 and independent read-only transport audit: 1535.264 seconds, 63 tool calls. Healthy relative to recent completed Opus runs; not the cancelled attempt-17 outlier.