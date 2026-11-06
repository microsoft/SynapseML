---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 3
round: 1
timestamp: "2026-11-03T21:32:06Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "25a6a1697cbcedde636d5eab94e3eeb60d495b48043cd04c2c2494f48e129b85"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: null
attempt: 1
stage_attempt: 3
round: 1
timestamp: 2026-11-03T20:58:11Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: null
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-3-review-1-grok-4.7.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (`readBounded` at 234-264, `exchangeBeforeDeadline` at 277-328, poll/submit/create paths through 537).
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala` (endpoint/auth overrides, `validateValue` at 138-169, `rowError` at 234-241, request and management paths).
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala` Python `setDocumentBytes` (179-181) and `_transfer_params_to_java` (228-235), plus constructor dispatch in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` (476).
- [x] Checked parent HTTP/auth behavior in `CognitiveServiceBase.scala`, `HTTPSchema.scala`, `HTTPClients.scala`, `Clients.scala` (55-62), and `AsyncUtils.scala` (16-35), and the CU docs bounds section (`Content Understanding.md` 268 and 283).
- [x] Compared size-limit tests: `ContentUnderstandingSuite.scala` 388 covers only `readBounded`; `ContentUnderstandingRecoverySuite.scala` 35-52 expects `ResponseTooLarge` only when `response.close()` does not throw.
- [ ] Tests were not executed; this was a read-only source review. `input_sha256` and `execution_backend` were not supplied and are null. Timestamp is the known session start converted to UTC, not a fresh clock read.

## Issues

### Issue 1: response.close() can replace ResponseTooLarge, a parsed success, or cancellation
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 251-264, 292-321
- **Description**: `readBounded` deliberately does not close the stream because close-after-abort can mask `ResponseTooLarge`. `exchangeBeforeDeadline` then calls `response.close()` in an inner `finally`. In Scala, an exception from `finally` replaces the pending `ResponseTooLarge`, a returned `WireResponse`, or an `InterruptedException`. The outer `IOException` handler turns that into `TransportError` (or `OperationTimedOut` only when the budget is already zero). Headers captured before the body read are kept, but the diagnostic is wrong. The cleanup test invokes only `readBounded`, so a throwing `CloseableHttpResponse.close()` is untested. That throw is realistic after `request.abort()` on a partial or gzip stream.
- **Risk**: Oversized responses are reported as uncertain transport failures, so callers retry or resume instead of raising `maxResponseBytes` or narrowing `range`. A close failure after a fully read body drops a successful result. An interrupt during read can be swallowed, so Spark cancellation does not propagate.
- **Suggested Fix**: Close in a nested try and ignore or `addSuppressed` close failures when a primary result or `ResponseTooLarge` already exists. Preserve the interrupt flag and rethrow `InterruptedException`. Add a test whose `HttpResponse.close()` throws after an oversized body and after a successful read.

### Issue 2: Unsafe header values escape row isolation and can inject headers
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 65-70, 138-169, 234-241, 273-282
- **Description**: `subscriptionKey`, `AADToken`, `CustomAuthHeader`, `customHeaders`, and `telemHeaders` skip `validateInputValue`. Nothing rejects CR, LF, or NUL in header names or values, and POST requests are not stripped of `Host` or `Content-Length`. `rowError` catches only `IllegalArgumentException` from `validateRow`. Apache HttpClient raises that exception later, while writing the request inside `exchangeBeforeDeadline`, whose catch handles only `ResponseTooLarge` and `IOException`. The exception leaves `rowHandler` and fails the Spark task. A client that does not reject those characters will emit the injected header.
- **Risk**: One row with a hostile or corrupted credential or `customHeaders` value fails the partition instead of becoming `InvalidInput`, and can split the request or override `Host` if the HTTP client accepts the value. The message may also expose header contents in the task failure.
- **Suggested Fix**: Reject CR, LF, and NUL in every header name and value inside `validateValue`, including auth params, without copying the value into the error text. Strip hop-by-hop headers (`Host`, `Content-Length`, `Transfer-Encoding`) on every request, not only poll GETs. Catch client `IllegalArgumentException` in the exchange and return a row diagnostic that does not echo the header.

### Issue 3: Python documentBytes transfer silently encodes strings as bytes
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala
- **Line(s)**: 179-181, 193-199, 228-235
- **Description**: Generated construction and `setParams` call `set` plus the param name. `_transfer_params_to_java` does the same for every `ServiceParam` in the Python param map. `setDocumentBytes` always executes `bytearray(value)`. In Python 3, `bytearray("body")` succeeds and becomes the scalar document `b"body"`, rather than calling `setDocumentBytesCol`. The JVM setter would have rejected a string; this override hides that error. Scalar byte copies work; a column name passed through the constructor, `setParams`, or `copy({documentBytes: "..."})` does not.
- **Risk**: A caller can submit the column name as the document, or analyze the wrong bytes, with no type error. Column configuration via `setDocumentBytesCol` is unaffected.
- **Suggested Fix**: If the transferred value is a `str`, call `setDocumentBytesCol`. Accept only `bytes`, `bytearray`, or an integer sequence in `setDocumentBytes`, and raise `TypeError` otherwise.

### Issue 4: One-second concurrentTimeout slack does not cover handler start delay
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 49, 98-105
- **Description**: For concurrency above one, configuration requires `concurrentTimeout` to exceed `maxOperationDuration` by one second, and the docs say that is enough for the operation budget to finish. The async client schedules work on `ExecutionContext.global` and starts `Await.result` when the future is created, not when the handler starts (`Clients.scala` 55-62, `AsyncUtils.scala` 16-35). Await does not cancel the future. Queueing longer than one second fails the task while the POST can still complete.
- **Risk**: A task timeout drops an accepted handle and can cause a duplicate submission on retry, despite a configuration check that claims to prevent that. Unset `concurrentTimeout` remains unbounded and is not affected.
- **Suggested Fix**: Do not treat one second as a sufficient scheduling bound. Start the await clock only after the handler begins, or require a slack that covers executor queue delay and document that the minimum does not guarantee the budget returns first.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: `exchangeBeforeDeadline` now calls a new `closeQuietly(response)` instead of `response.close()`. It ignores `NonFatal` close failures, so a completed `WireResponse`, a `ResponseTooLarge` diagnostic, or the propagating exception is no longer replaced. `NonFatal` does not match `InterruptedException`, so interruption still propagates.
- **Why**: Releasing the response is best-effort cleanup and must not change the outcome of the read.
- **How verified**: Added the `ContentUnderstandingRecoverySuite` test "response close failures cannot replace a completed read or the size-limit diagnostic". Its client's `CloseableHttpResponse.close()` throws `IOException`. A normal body still returns `(200, body, None)`. An oversized body still returns the `ResponseTooLarge` diagnostic. All 58 `ContentUnderstanding*Suite` tests pass.
- **Files**: `ContentUnderstandingProtocol.scala`, `ContentUnderstandingRecoverySuite.scala`.

### Issue 2
- **Status**: Resolved
- **What changed**: Added `ContentUnderstandingProtocol.validateHeaderSetting`. `validateValue` now applies it to every authentication and header param (`subscriptionKey`, `AADToken`, `CustomAuthHeader`, `customHeaders`, `telemHeaders`) for both scalar and column values.
  - Header names must be RFC 9110 tokens.
  - Names may not be `Content-Length`, `Host`, or `Transfer-Encoding`, which the connector manages.
  - Values may not contain control characters other than TAB, so CR, LF and NUL are rejected.
  - Error messages give the parameter name but never the value.
  - A scalar violation is rejected by `transformSchema`. A column violation becomes that row's `InvalidInput` error, so the rest of the partition still runs.
- **Why**: Header injection must be rejected before any request is built. A bad value must stay a row error and must not leak into task failures.
- **How verified**:
  - Schema-validation cases added for `Content-Length`, an invalid header name, a NUL value, and a CRLF `CustomAuthHeader`.
  - New recovery test "header injection in column credentials is a row error that does not echo the value": the CRLF row is `Failed` with `InvalidInput` and no echoed text, the valid row is `Succeeded`, the server sees exactly one request, and no injected header reaches it.
  - All 58 `ContentUnderstanding*Suite` tests pass.
- **Files**: `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, `ContentUnderstandingSuite.scala`, `ContentUnderstandingRecoverySuite.scala`, `Content Understanding.md`.

### Issue 3
- **Status**: Resolved (defensive guard; the claim as stated was not reproduced)
- **What changed**: The generated Python `setDocumentBytes` now raises `TypeError("documentBytes requires bytes; use setDocumentBytesCol to select a column.")` when given a `str`.
- **Why**: The reported silent encoding does not happen. In Python 3, `bytearray("body")` raises `TypeError: string argument without an encoding`. The existing error, however, did not tell the user to call `setDocumentBytesCol`, so an explicit guard with that guidance was added.
- **How verified**: The new Python test `test_document_bytes_rejects_text_values` asserts that the `TypeError` mentions `setDocumentBytesCol`. 10/10 offline Python tests pass.
- **Files**: `ContentUnderstandingParams.scala`, `test_ContentUnderstanding.py`.

### Issue 4
- **Status**: Resolved (human decision: reject `concurrentTimeout`)
- **What changed**: Removed the one-second slack rule. `validateConfiguration` now rejects any configured `concurrentTimeout` with "concurrentTimeout is not supported; maxOperationDuration bounds each operation." `getInternalTransformer` no longer passes it to the HTTP layer. The docs now list the param as not supported and explain why.
- **Why**: `Await` starts timing before the handler runs and does not cancel the future, so no fixed slack can stop a task timeout from discarding an accepted handle. `maxOperationDuration` already bounds each operation. The user chose rejection over a larger slack.
- **How verified**: The schema-validation list now expects `setConcurrentTimeout(3600.0)` to throw `IllegalArgumentException`. All 58 `ContentUnderstanding*Suite` tests pass.
- **Files**: `ContentUnderstanding.scala`, `ContentUnderstandingSuite.scala`, `Content Understanding.md`.