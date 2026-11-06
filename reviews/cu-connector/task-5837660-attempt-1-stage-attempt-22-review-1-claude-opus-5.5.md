---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 22
round: 1
timestamp: "2026-11-05T14:54:17Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e2bf699b0f3639a35e93a3bfcb51824fd7f39c1c8df507bfc10d349ce42a2caf"
---

I found one Low-severity issue: when the analyzer lookup after a successful create expires before it is sent, the thrown result says `Failed` and drops the HTTP 200 and response body. The docs and spec say that state should be kept. I ran no tests and inspected the code read-only. I left out schema frontmatter because I can't supply the timestamp or execution backend without inventing them; the evidence writer will need to add it.

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-22-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Submission and admission** (`ContentUnderstandingProtocol.scala`: `exchange` L348-358, `exchangeBeforeDeadline` ~L360-410, `decodeSubmission`/`safeSubmission`/`SubmissionLocation.assess` ~L510-545):
  - Work that is not sent (expired before dispatch, DNS or connect deadline) is `Rejected` with `RequestNotSent`. A failure after writing starts stays ambiguous, using the transport write marker.
  - 401/403/429 without a handle stay `Rejected` with `ResponseBodyReadFailed` or `ResponseTooLarge`.
  - A 404 with a body-read diagnostic stays `Unknown`. Contradictory handles give `Unknown`.
  - No automatic POST/PUT replay.
- [x] **Polling retention** (`retainPollState` L570-585, `pollWithClient` from L587):
  - Prior status, raw response and handle survive transient errors.
  - Expiry before dispatch with prior state adds `OperationTimedOut` only when no prior error exists.
  - When `delay >= remaining`, the retained state is returned without shortening `Retry-After`.
  - `PollAttemptsExhausted` is added only to a clean state.
- [x] **Management calls** (`getAnalyzerWithClient` L693-722, `createAnalyzer` L730-788, `analyzerBody` L684-691; `ContentUnderstanding.scala` management ~L425-465):
  - The previous GET response is kept on expiry before dispatch.
  - The current response is returned when the retry delay exceeds the budget.
  - An unsafe create handle keeps `raw.diagnostic` (L744-748).
  - `createAnalyzer` requires `isSet(analyzerId)`, runs only on the driver with scalar settings, and validates the definition before HTTP. `getAnalyzer` keeps the default.
- [x] **Shared stop and auth classification** (`rowHandler`/`checkSharedFailure`/`FailureClassifier` ~L640-680; `ContentUnderstanding.scala` `settings` L114-139, `rowUsesColumnAuth` L148-158; `CognitiveServiceBase.scala` `ServiceAuthHeaders.resolve` L376-436):
  - One stop flag per task closure. Later unsent rows are skipped whatever their credential source. Accepted handles and `Unknown` rows are excluded.
  - Nested diagnostics are limited to connector codes {TransportError, OperationTimedOut, ResponseTooLarge, ResponseBodyReadFailed} with an empty raw response (L674).
  - Scalar fingerprints plus `allAuthScalar` follow the spec, and row-credential precedence matches `resolve`.
- [x] **Header safety** (`CognitiveServiceBase.scala`):
  - The new `addContentType` replaces `Content-Type` case-insensitively.
  - `addHeaders` (L549-563) drops a user-supplied implicit-Fabric marker, and `HTTPSchema.toHTTPCore` strips it (L209).
  - The new `HasCustomHeaders` getter and column setter compile against the existing `ServiceParam` shape.
- [x] **DNS, transport and budget**:
  - `BudgetedDnsResolver` has bounded permits and rechecks the budget after acquiring a permit, releasing it on expiry.
  - A submitted worker holds its permit until the physical lookup finishes.
  - `RejectedExecution` gives `DnsCapacityExceeded`, which is not treated as a deadline.
  - HTTP phase timeouts are capped at `min(timeout, remaining)`.
- [x] **Parsing bounds**:
  - JSON is limited to depth 1000 and 100-character numeric tokens, with a string- and escape-aware scan.
  - The parse wrapper catches `NonFatal` and `StackOverflowError`. Error-code traversal is tail-recursive.
  - Numeric `Retry-After` accepts at most 19 ASCII digits and saturates; HTTP dates are still accepted.
  - Header limits are 64 KiB per line and 200 headers. Entity reads are bounded at `maxResponseBytes` + 1.
- [x] **Validation and session handling** (`ContentUnderstanding.scala`):
  - `concurrentTimeout` is rejected, and the output/error collision check uses the resolver.
  - `validateParamSchema` requires an exact field name and a resolver match count of 1.
  - `transform` uses `DatasetExtensions.resolverFor(dataset)` without changing SQLConf. Schema-only calls use `SQLConf.get.resolver`.
  - `getVectorParamMap` is filtered to active params (L83-86). Only column-bound values are checked per row. `maxDocumentBytes` is enforced.
- [x] **Python wrappers** (`core\...\codegen\Wrappable.scala` L83-91; `core\src\main\python\synapse\ml\core\schema\Utils.py` L61-152; CU Python overrides):
  - The base `isSet` override is kept because every `pyAdditionalMethods` override calls super.
  - Column bindings are excluded from `isSet`/`hasDefault`/`isDefined`/`extractParamMap`, and `getOrDefault` raises `KeyError` for them.
  - Maps become dicts. Bytes are not pickle-decoded.
  - Output/error defaults are refreshed when loading from the JVM.
- [x] **Generic credential exposure across services**: considered and dismissed.
  - `test_ContentUnderstanding.py` L543-600 deliberately covers `TextSentiment` and `AnalyzeDocument`.
  - The repo-wide `docs\Use with MLFlow\Autologging.md` L12-19 documents credential logging for cognitive-service stages, matching the spec.
- [x] **Docs checked against code** (`docs\Explore Algorithms\AI Services\Content Understanding.md` L102-118, L220-262, L374-400, L416-440): redaction limits, shared-stop semantics, resumable selection by handle plus NotStarted/Running/Unknown, best-effort budget, management behavior.
- [x] **Tests inspected, not executed**:
  - `ContentUnderstandingAdmissionSuite.scala` L105-131: the clean-state retention matrix covers only `poll` and `analyze`.
  - Also inspected: Admission L173-262 (DNS capacity and pre-dispatch), `ContentUnderstandingConcurrencySuite.scala` L191-215, `ContentUnderstandingSuite.scala` L765-787.
- [x] **Dismissed after checking**:
  - Null map values give `InvalidInput`, not an NPE (type check in `validateValue`).
  - The implicit-auth marker cannot be spoofed or leaked.
  - The `findUnusedColumnName` resolver change is more conservative.
  - With `coalesce` after transform, the stop covers the merged output partition. This is conservative, and skipped rows are retryable.
- [ ] Caller-supplied results (127/127 Scala, 25/25 Python, JAR hashes) were not re-run; this review is read-only. Live CU and external Azure CI evidence are unavailable; that is not a code defect.

## Issues

### Issue 1: Expiry before the post-create analyzer GET drops the successful creation's HTTP 200, body and status
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 352-355 (`exchange` unsent response), 699-716 (`getAnalyzerWithClient` first attempt with `previous = None`), 755-766 (`createAnalyzer` rewrap)
- **Description**:
  - After the creation poll returns `completed` (`Succeeded`, HTTP 200, operation JSON), `createAnalyzer` runs the final `getAnalyzerWithClient`.
  - Suppose the budget runs out before the first final GET is dispatched. This can happen when `remainingMs == 0` after a slow terminal poll, on a connect deadline (L408), or on DNS-capacity expiry.
  - `exchange` then returns `WireResponse(0, "", …, OperationTimedOut or DnsCapacityExceeded, expiredBeforeDispatch = true)`.
  - With no previous retrieval response, `get` returns that unsent response, and `analyzerBody` throws `decode(raw, None)`.
  - The rewrap at L760-765 then throws `completed.copy(status = "Failed", httpStatus = 0, rawResponse = "", error = OperationTimedOut or DnsCapacityExceeded)`.
  - Expected behavior:
    - The spec says expiry before dispatch "must not replace a prior error/status/body", and that with clean prior poll state the connector should "preserve HTTP200/202 and add OperationTimedOut".
    - The CU doc (L376-386) applies this to "the final analyzer GET after creation succeeds": "With a retained clean poll/submission state, its HTTP status and raw response remain intact and the diagnostic becomes `OperationTimedOut`."
    - Poll mode already behaves this way through `retainPollState` (L576-578), which also turns DNS-capacity expiry with prior state into `OperationTimedOut`. `createAnalyzer` does not.
  - The clean-state test matrix (`ContentUnderstandingAdmissionSuite.scala` L116) loops only `poll` and `analyze`. The `create` cases elsewhere cover only retention of a prior GET error.
- **Risk**:
  - The exception reports a remotely successful creation as `Failed`, with HTTP 0 and an empty raw response, discarding the HTTP 200 and `Succeeded` JSON that prove it completed.
  - A caller acting on `status`/`httpStatus` may re-run `createAnalyzer(..., allowReplace=True)`. That issues another non-idempotent PUT and replaces the analyzer just created. With `allowReplace=False` the caller gets a confusing conflict instead.
  - The impact is limited: the path is narrow, and the handle, id and `OperationTimedOut` hint are kept.
- **Suggested Fix**:
  - Distinguish a first-attempt retrieval that expired before dispatch from a dispatched retrieval failure. For example, have `getAnalyzerWithClient` surface the `expiredBeforeDispatch` flag through a private exception subtype or an `Either` carrying the `WireResponse`.
  - In that case, throw `completed` with its status, `httpStatus`, `rawResponse`, `operationLocation` and `id` intact, adding `OperationTimedOut` if it has no error, mirroring `retainPollState`.
  - Keep the current rewrap for dispatched retrieval failures, where the last GET error wins (Concurrency L191 and Suite L765 still hold).
  - Add a `create` case to the clean-state matrix and a DNS-capacity case on the first final GET. Assert HTTP 200, raw equal to the completed body, `OperationTimedOut`, the retained handle, and request methods `Seq("PUT", "GET")`.
  - If "Failed" is the intended overall status, still keep the HTTP status and body, and update the CU doc wording to match.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Driver resolution - attempt 23
- **Status**: Fixed locally and validated; independent re-review pending. Original finding/verdict retained.
- **Change**: Management GET retry handling returns its existing private `WireResponse` until the caller chooses body validation. `finishCreation` preserves the completed creation response on first final-GET pre-dispatch expiry and adds the timeout diagnostic. It does not convert the retained Succeeded state to Failed. Actual retrieval failures retain the existing Failed wrapper and their GET status/body/error.
- **Why this preserves precedence**: When a prior retrieval GET failed, the retry helper returns that prior response rather than the expired unsent response. It therefore still takes precedence over completed creation state. Direct analyzer lookup retains its original body/error behavior.
- **Regression evidence**: Deterministic expiry immediately after terminal polling and real occupied-DNS-capacity expiry before the first final GET retain Succeeded/HTTP200/body/id/handle, with only PUT+poll GET observed. Dispatched403, not-ready200, malformed200 and disconnected retrieval controls remain authoritative. Existing503/429/transport retry retention tests and successful final retrieval tests remain green.
- **Validation/audit**: Full CU134/134, serializer10/10, Python28/28, style/compile/codegen/publication and hashes passed; full evidence in GPT attempt22 driver section. Original Opus audit passed with canonical input hash,65 read-only tool calls and1533.529s duration.