---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 4
timestamp: "2026-11-06T19:52:13Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "b3b9d2f2e61a77b811921ea889c921c66b360bf3749cf6cbbfe0ff66d369745f"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Response size bound** (`ContentUnderstandingProtocol.scala`, `readBounded`/`exchange`): the reader reads at most `limit + 1` bytes and rejects only when `size > limit`. A response of exactly `maxResponseBytes` is accepted. `ResponseTooLarge` keeps the status, location and Retry-After values (L392-394), so an operation handle is not lost when the body is too large.
- [x] **Deadline and budget boundaries** (`ContentUnderstandingProtocol.scala` L353-361, L587-592, L610-618, L737-742): `remainingMs == 0` returns `expiredBeforeDispatch`/`notSent` without sending a request. A poll whose `delay >= remainingMs` keeps the previous state and adds `timeoutError` only when no earlier error exists. `decodeSubmission` (L533-540) turns a budget-expired submission that was never sent into `RequestNotSent`, not a completion. `finishCreation` throws with the completed response and keeps its existing error.
- [x] **Timeout conversion** (`ContentUnderstanding.scala`, `validateConfiguration`/`settings`): timeout must be finite, positive and `<= Int.MaxValue / 1000` seconds, so `math.ceil(timeout * 1000).toInt` cannot overflow. `concurrentTimeout` is rejected. `outputCol` and `errorCol` must differ under the dataset's resolver.
- [x] **DNS budget** (`ContentUnderstandingDnsResolver.scala` L28-80, L95-112): permits (32) equal pool size (32), and the queue holds 32 more, so `RejectedExecutionException` is effectively unreachable. Every acquire is released on each path: in the task's `finally`, on rejection, and on the post-acquire deadline check. `get(remainingMs())` with 0 produces a timeout, which maps to `OperationDeadlineExceeded`. `InterruptedIOException` sets the interrupt flag again, so `exchange` rethrows it (Protocol L400).
- [x] **`expiredBeforeDispatch`** (`ContentUnderstandingDnsResolver.scala` L83-93; `ContentUnderstandingProtocol.scala` L395-399): only budget-exhaustion DNS failures are marked as expiring before dispatch. `DnsResolutionFailed` and `DnsCapacityExceeded` keep their JSON diagnostics. Any other `UnknownHostException` cannot come from this resolver, because delegate failures are wrapped through `ExecutionException`.
- [x] **Auth provenance matches header resolution** (`ContentUnderstanding.scala`, `rowUsesColumnAuth`; `CognitiveServiceBase.scala` L376-433, L512-555): `rowUsesColumnAuth` uses the same precedence as `ServiceAuthHeaders.resolve`. The first non-blank of subscriptionKey, AADToken and CustomAuthHeader wins, then credentials embedded in customHeaders (subscription key, then AAD). Both use `nonBlank`, so the shared-failure classification matches the header actually sent. CU disables the Fabric fallback (`getFabricFallbackAuthHeader = None`) and global OpenAI parameter transfer.
- [x] **Content-Type change** (`CognitiveServiceBase.scala` L358-361, L431): `addContentType` removes every case variant before inserting the canonical header, so the `TreeMap` can no longer emit duplicate mixed-case Content-Type headers. For an exact-case `Content-Type` key, the old overwrite behavior is unchanged. A missing or empty content type leaves customHeaders untouched.
- [x] **New HasCustomHeaders accessors** (`CognitiveServiceBase.scala` L226-230): the zero-argument `getCustomHeaders` overloads the existing protected `getCustomHeaders(row)` without ambiguity. `setCustomHeadersCol`/`getCustomHeadersCol` follow the existing vector-parameter pattern, and `getVectorParamMap` (L67-72) picks up column bindings.
- [x] **Active column bindings** (`ContentUnderstanding.scala`, `activeParams`/`getVectorParamMap`): poll mode exposes only the auth parameters plus `operationLocation`. Analyze and submit modes exclude `operationLocation`. Inactive column bindings therefore cannot become required input columns or mask schema errors.
- [x] **Row validation** (`ContentUnderstanding.scala` L285-292, `validateValue`/`validateNonemptyValue`/`validateOptionValue`): `IllegalArgumentException` becomes an `InvalidInput` JSON diagnostic and does not fail the task. Type checks run before the `asInstanceOf` casts. The `documentBytes` length check is inclusive (`<= maxDocumentBytes`). Map keys and values are checked for type and emptiness. The `stringEncoding` and `processingLocation` lists are exact, case-sensitive matches.
- [x] **Python parameter lifecycle** (`core/src/main/python/synapse/ml/core/schema/Utils.py`; `Wrappable.scala` getter change): `_transfer_params_to_java` checks `in _paramMap` / `in _defaultParamMap`, so the JVM-aware `isSet`/`hasDefault` overrides cannot cause re-entry or double transfer. Getters prefer pending Python values. The CU wrapper rejects `None` and `str`/`int`/`bool` byte values before transfer, so a failed transfer leaves pending invalid values in place.
- [ ] **Execution evidence:** I did not run tests or builds; this review is read-only. The supplied evidence (57 offline Python tests, scalastyle, codegen, Black) was taken as input but not re-verified. Live CU, packaged-runtime and Azure CI gates remain outstanding.
