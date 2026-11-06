---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 4
timestamp: "2026-11-06T19:45:16Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "bb3f75d8547526e1b8d94e3baa691c1b6a63f6590ace97349fff376913e20785"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Bounded body reads have no off-by-one or overflow.** In `ContentUnderstandingProtocol.scala:304-329`, `readBounded` computes `remaining` as `limit.toLong - size + 1`, which is a Long, so `limit = Int.MaxValue` cannot overflow. It aborts only when `size + count > limit`, so a body of exactly `limit` bytes is accepted and one extra byte is rejected. On interrupt, it calls `request.abort()` and rethrows. The `maxResponseBytes`, `maxDocumentBytes`, `maxOperationDuration` and `maxPollAttempts` limits are all validated as `gt(0)`, and `pollingDelay` as `gtEq(0)` (`ContentUnderstandingParams.scala:106-154`).
- [x] **The poll loop counts and stops correctly.** In `ContentUnderstandingProtocol.scala:603-630`, `loop(settings.maxPollAttempts, …)` sends exactly N GETs. `remaining == 1` is the last attempt. `PollAttemptsExhausted` is attached only when the final state is still ongoing and has no error. The loop stops early on `!raw.retryable && !ongoing(parsed)`. It returns the retained state plus a timeout when `delay >= budget.remainingMs`, and returns `Unknown` with the location when there is no prior state.
- [x] **Poll retention and decoding are consistent.** In `ContentUnderstandingProtocol.scala:438-472` and `573-596`, a transport failure gives `Unknown`. A diagnostic or non-2xx response gives `Failed`. A successful response without a recognized status gives `Failed` with `InvalidResponse`. `retainPollState` keeps the earlier `Running` handle on retryable, diagnostic, missing-status or non-terminal HTTP errors, updating only `httpStatus` and `error`. A 404 or 410 becomes `ResultUnavailable` only for analysis handles (`analyzer.isEmpty`), not management operations.
- [x] **Submission classification is consistent.** In `ContentUnderstandingProtocol.scala:524-565` and `632-658`:
  - `decodeSubmission` checks in this order: not sent → `Rejected`, admission rejected without a location → `Rejected`, uncertain → `Unknown`.
  - `safeSubmission` drops a location from a non-accepted response before decoding.
  - `execute` resumes an `Unknown` result only when it has a validated `operationLocation`, so an uncertain POST is never replayed.
  - `checkSharedFailure` is skipped for accepted handles and for `Unknown`.
- [x] **Management create/get paths are correct.** In `ContentUnderstandingProtocol.scala:681-791`:
  - A creation response that carries a location but has a non-accepted status throws.
  - An unsafe location is not requested.
  - Polling starts from the decoded PUT state, and the result must be `Succeeded` with no error before `finishCreation` requires the analyzer to be `ready`.
  - `getAnalyzerResponse` limits retryable GETs to `ManagementGetAttempts` and returns the previous response when the deadline expires before dispatch.
  - The `None`-location branch requires `status == ready`.
- [x] **Auth precedence matches the shared resolver.** In `CognitiveServiceBase.scala:376-410`, `ServiceAuthHeaders.resolve` checks a non-blank `subscriptionKey`, then `AADToken`, then `CustomAuthHeader`, then embedded custom headers. `ContentUnderstanding.rowUsesColumnAuth` uses the same order and the same non-blank rule, so row-versus-scalar provenance matches the header actually sent. `addContentType` (lines 358-361) materializes the matching keys with `.toSeq` before removing them, so there is no concurrent modification, and it replaces any differently cased `Content-Type`. The new no-argument `getCustomHeaders` coexists with the existing `getCustomHeaders(row)` overload. CU overrides `getFabricFallbackAuthHeader` to `None` and makes `transferGlobalParamsToParamMap` a no-op.
- [x] **The provenance marker never reaches the wire or Fabric auth.** In `core\src\main\scala\…\io\http\HTTPSchema.scala:169-173` and `209`, `toHTTPCore` strips the Fabric marker header. `usesFabricAuth` requires the value `"true"` plus an MWC token, so the CU marker `content-understanding-row-auth` cannot trigger Fabric token handling. The shared-failure fingerprints lowercase header names on both the settings side and the request side.
- [x] **Schema, row validation and URL building are correct.** In `ContentUnderstanding.scala:239-319`:
  - `validateParamSchema` requires an exact field match, which is what `row.getAs` needs. It also requires exactly one field under the active resolver, so case-insensitive duplicates are rejected. Map value nullability is ignored in type matching.
  - `validateRow` re-checks only column-sourced values. It enforces the XOR between document URL and document bytes, requires a non-null input, and requires non-null `analyzerId` and `apiVersion`.
  - `analyzeUrl` sorts the query parameters and builds them with `URIBuilder` on top of the validated endpoint.
  - The timeout cap (`Int.MaxValue / 1000`) prevents the `ceil(...).toInt` conversion from overflowing.
- [x] **DNS budget and deadlines are released on every path.** `BudgetedDnsResolver` releases its semaphore on rejection, on expiry after acquire, and in the worker's `finally`. The pool has 32 threads, a queue of 32 and 32 permits, so no spurious rejection is possible. `DeadlineControl.release` skips `releaseConnection` once the deadline has aborted the request.
- [x] **The Python scalar-view lifecycle matches the stated contract.**
  - `core\src\main\python\synapse\ml\core\schema\Utils.py`: in `ComplexParamsMixin`, `isSet`, `hasDefault`, `getOrDefault`, `extractParamMap` and `clear` give pending Python values precedence and exclude column bindings. `_transfer_params_to_java` uses direct `_paramMap`/`_defaultParamMap` membership.
  - The CU wrapper override in `ContentUnderstandingParams.scala` transfers values through the validating setters and keeps invalid pending values when a transfer fails.
  - Bytes are never routed through pickle.
- [x] **Considered and rejected as non-defects:**
  - A column map value that is null is rejected per row with a generic "invalid value type" `InvalidInput` message. This fails closed and only affects diagnostic wording.
  - If the operation budget expires before the first creation poll, after an oversized accepted PUT body, the analyzer creation exception reports `Failed` while still keeping the operation location. The error still identifies the cause.
  - `findUnusedColumnName` now uses the session resolver. This prevents case-insensitive column collisions.
- [ ] **Tests, live CU requests and Azure CI were not executed by this reviewer.** This is a read-only review. The test, compile, Black and codegen results in the prompt's validation provenance were treated as supplied input, not as reproduced evidence.
