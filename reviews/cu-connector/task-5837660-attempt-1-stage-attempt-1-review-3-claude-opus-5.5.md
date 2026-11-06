---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 3
timestamp: "2026-11-05T19:49:33Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "003a300e436ce4329d45e08303076518de0378ede1f363fd568ec4b0018622d3"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Row validation boundary** (`ContentUnderstanding.scala` lines 177–206 and 260–288). `validateValue` checks types first: `key.isInstanceOf[String] && item.isInstanceOf[String]`. A null map key or value therefore fails with an `IllegalArgumentException` before `ContentUnderstandingHeaderValidation.validateSetting` (lines 18 and 21) or `validateNonemptyValue` (line 206) calls `toString`. `rowError` turns that exception into an `InvalidInput` diagnostic, so it does not fail the task. I suspected a null-map-value NPE here and this check ruled it out. Column `operationLocation` values are also checked per row through `validateOptionValue` → `validateOperationLocation`, so poll mode cannot reach `prepareRequest` with an unsafe handle.
- [x] **Shared-stop semantics** (`ContentUnderstandingProtocol.scala` lines 495–508 and 660–685):
  - The `AtomicBoolean` is created per handler closure, so each task gets its own.
  - Poll mode skips the stop check, and polls pass `acceptedHandle = true`, so they never throw.
  - `Unknown` and accepted-handle responses are exempt.
  - The nested `innererror` is built only from connector codes and only when `rawResponse` is empty.
- [x] **Credential provenance**:
  - `rowUsesColumnAuth` (lines 152–162) uses the same precedence as `ServiceAuthHeaders.resolve` in `CognitiveServiceBase.scala` (around lines 380–388): subscriptionKey, then AADToken, then CustomAuthHeader, then embedded key, then embedded AAD.
  - The row-auth marker suppresses fingerprint matching.
  - `HTTPSchema.scala` line 209 strips the marker before the request goes on the wire.
- [x] **Submission classification** (lines 344–410 and 510–553):
  - A 401/403/429 without a handle stays `Rejected`, even with `ResponseBodyReadFailed` or `ResponseTooLarge`.
  - A 404 with a diagnostic becomes `Unknown`.
  - A rejection that carries a contradictory or unsafe location becomes `Unknown` through `SubmissionLocation.assess` (lines 63–75), and that handle is not followed.
  - Write-started tracking (`ContentUnderstandingTransport.scala`) separates `RequestNotSent` from an ambiguous `TransportError`.
- [x] **Polling and deadline state** (lines 571–627):
  - `retainPollState` keeps the prior body and handle, and the newest dispatched error wins.
  - An expired-before-dispatch result keeps the prior state and adds `OperationTimedOut` only when the retained state has no error.
  - With no prior response, the first diagnostic is kept.
  - The `remaining == 1` check marks `PollAttemptsExhausted` only when the operation is still running and has no error.
- [x] **Creation flow** (lines 687–780):
  - In `getAnalyzerResponse`, a dispatched failed retrieval keeps its own response when a later retry expires before sending. `finishCreation` maps an undispatched first GET to the completed result plus `OperationTimedOut`.
  - Creation without a URL keeps `InvalidResponse`, `OperationFailed` and service errors, and falls back to `MissingOperationLocation`.
  - Poll status decoding uses the operation vocabulary only.
- [x] **DNS resolver** (`ContentUnderstandingDnsResolver.scala`):
  - It rechecks expiry after acquiring a permit and releases the permit on expiry or executor rejection.
  - The worker releases its permit in `finally`, so a submitted lookup holds the permit until it physically completes.
  - `DnsResolutionFailed` and immediate `DnsCapacityExceeded` are not classed as expired-before-dispatch.
  - Interrupts restore the interrupt flag and are rethrown at the `exchangeBeforeDeadline` guard.
- [x] **JSON and HTTP bounds** (`ContentUnderstandingResponseParsing.scala`):
  - The iterative scanner tracks quotes and escapes, with depth limit 1000 and numeric-token limit 100.
  - `safely` catches `NonFatal` and `StackOverflowError`.
  - `errorCodes` is tail-recursive.
  - Retry-After accepts at most 19 digits and saturates through `BigInt` before `Long.MaxValue`.
  - Message constraints are 65536-byte lines and 200 headers.
  - `readBounded` reads at most limit + 1 bytes, then aborts and throws `ResponseTooLarge`.
- [x] **Column-name safety** (`buildInternalTransformer`, lines 390–420): output and error columns cannot overwrite inputs under the dataset resolver, and internal temporary names avoid all reserved names case-insensitively. `concurrentTimeout` is rejected up front (lines 108–110), so `HTTPTransformer` never applies a shared future wait.
- [x] **Persistence and Python lifecycle**:
  - `ComplexParamsSerializer.scala` lines 21–48: the opt-in `saveDefaultParamsAsExplicit = false` encodes only `instance.paramMap`; `ServiceParam` is a `JsonEncodableParam`, not a `ComplexParam`, so defaults are not turned into explicit values.
  - `ContentUnderstandingParams.scala` lines 155–255: `_transfer_params_from_java` drops JVM-backed service params rather than inventing `None`, `_transfer_params_to_java` routes pending values through the dedicated setters, and the generated service setters pop pending state (`Wrappable.scala` lines 209 and 215).
- [ ] **Not executed**: no tests, builds or live service calls were run, as this review is read-only. I relied on the supplied validation claims (143 Scala and 29 Python tests, black) without re-running them. Actual MLflow autologging, current-target integration and external CI remain unverified, as stated in the plan.