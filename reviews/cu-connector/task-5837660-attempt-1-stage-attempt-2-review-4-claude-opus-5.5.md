---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 4
timestamp: "2026-11-05T20:30:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e70917fafb813c6d65597cf1a8f5ccb32ea64019924b563655ff30b7353e175a"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Auth precedence and provenance.** In `CognitiveServiceBase.scala`, `ServiceAuthHeaders.resolve` (lines 376-435) applies precedence in this order: subscriptionKey, then AADToken, then CustomAuthHeader, then the embedded subscription-key header, then the embedded AAD header, then the Fabric fallback. `ContentUnderstanding.rowUsesColumnAuth` follows the same order. It uses `getValueOpt` (line 102) and `getCustomAuthHeader(row)` (line 512), so a column binding stays row-level and is never treated as scalar just because its value matches a fallback. `settings.scalarAuthFingerprints` uses only scalar or default values. `allAuthScalar` covers all five auth-capable parameters.
- [x] **Header assembly and marker stripping.** `addContentType` (lines 358-361) removes every case variant of `Content-Type` before inserting the canonical one. Calling `keys.filter(...).toSeq` builds a strict copy, so nothing is removed while the map is being iterated. `addHeaders` (lines 549-563) strips `FabricAuthMarkerHeader` and skips marker names before the request goes on the wire.
- [x] **Overloads compile cleanly.** The new `getCustomHeaders`, `setCustomHeadersCol` and `getCustomHeadersCol` (lines 226-230) only overload the existing `protected getCustomHeaders(row)` (line 543) and the OpenAI override (`OpenAI.scala:590`). Nothing clashes.
- [x] **Exchange and deadline handling.** In `ContentUnderstandingProtocol.scala`, `exchange` and `exchangeBeforeDeadline` (lines 351-420) were checked:
  - An expired budget returns `notSent` and `expiredBeforeDispatch` without dispatching.
  - Phase timeouts are clamped to `max(1, min(timeout, remaining))`.
  - `ResponseTooLarge` keeps the HTTP code and handle.
  - A body-read failure on a POST 401/403/429 with no `Operation-Location` maps to `ResponseBodyReadFailed`.
  - `IOException` sets `expiredBeforeDispatch` only when the request was not sent and the deadline had expired.
  - `WireResponse.retryable` (lines 163-164) excludes expired-before-dispatch responses.
- [x] **DNS budget handling.** In `ContentUnderstandingDnsResolver.scala`:
  - Expiry is checked before and after acquiring capacity, and the permit is released when expiry is found after acquire.
  - A rejected submission releases its permit and does not count as deadline expiry (`deadlineExpired=false`).
  - The worker's `finally` keeps the permit until the lookup physically completes.
  - A genuine `DnsResolutionFailed` keeps its own diagnostic even when it is classified as timed out. `expiredBeforeDispatch` is true only for `OperationDeadlineExceeded` and for capacity waits that ran out of time.
  - A plain `UnknownHostException` that yields a non-JSON message is unreachable, because delegate failures are wrapped through `ExecutionException`.
- [x] **Deadline release race is harmless.** `DeadlineControl.release` (lines 186-192) can, in a narrow window, call `releaseConnection` at the same moment the scheduled `abort()` runs. HttpClient's cancellable handling is thread-safe, and the outcome has already been decided at that point, so this is not reported.
- [x] **Input validation.** `ContentUnderstanding.scala` lines 160-330 were checked: `validateValue`, `validateNonemptyValue`, `validateParamSchema`, `validateRow`, `rowError`, `requestBody` and `prepareRequest`.
  - The type check runs before header validation, so null map values become a row-level `InvalidInput`, not an NPE.
  - Output and error column names are compared with the dataset resolver.
  - A set `concurrentTimeout` is rejected.
- [x] **Protocol state, creation, JSON bounds and Retry-After.** The diff was traced through `safeSubmission`/`SubmissionLocation.assess`, `decode`/`CreationStatusNames`, `retainPollState`, `pollWithClient`, `getAnalyzerResponse`, `finishCreation`, URL-less creation and `checkSharedFailure`/`rowHandler`.
  - Unknown outcomes and poll mode do not trigger the shared stop.
  - Accepted handles are preserved.
  - Nested service-message lookalikes are filtered.
  - JSON limits hold: nesting 1000, numeric tokens 100.
  - Retry-After digits saturate at 19.
- [x] **Persistence and Python wrappers.** `ComplexParamsSerializer.scala` uses the opt-in `saveDefaultParamsAsExplicit`. Python `_transfer_params_to_java` checks membership in `_paramMap`, not the JVM `isSet`. Every `pyAdditionalMethods` override (OpenAIPrompt:168, AzureMapsTraits:23, ContentUnderstandingParams:214, CognitiveServiceBase:284/636/679) calls `super`, so the `isSet` stub reaches every subclass.
- [ ] **Tests and live service.** I ran no tests and made no live service calls (read-only review). The validation claims in the prompt were not re-run.