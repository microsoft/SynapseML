---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 18
round: 1
timestamp: "2026-11-05T05:35:30Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f3c3a97d12b34e08367eaa75f90f64a66533fe56b96da08fcfb448b6bfebfcc8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-18-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `cognitive\src\main\scala\...\contentunderstanding\ContentUnderstanding.scala`. Checks covered:
  - The overridden `transform` uses `DatasetExtensions.resolverFor(dataset)`.
  - Input bindings must match the exact name and be unambiguous under the resolver (`validateParamSchema`, ~231-243).
  - `rowError` (278-285) catches only `IllegalArgumentException`. Every path that reaches it (`parseUri` 200-207, the `validateValue` type guard 174-186, header validation) raises only that exception, including null map values rejected by the type guard.
  - The validation UDF and null-gating in `inputTransformer` (355-368).
  - Output/error collisions and case-insensitive internal names (387-401).
  - Bindings are restricted to active params by `getVectorParamMap` (77-89).
  - Management stays driver-only and scalar-only (420-428).
  - `ContentUnderstandingOutputParser` parses only the connector's shallow envelope (473-494).
- [x] Traced `ContentUnderstandingProtocol.scala` against the contract:
  - `decode` gives the connector diagnostic precedence (444-477). `uncertainSubmission` keeps a 404 with a diagnostic as `Unknown`.
  - `decodeSubmission`/`safeSubmission` (532-564) report Rejected for not-sent and 401/403/429 cases. Unsafe handles do not overwrite the raw diagnostic.
  - `retainPollState`/`pollWithClient` (580-625): the last error wins at budget exhaustion (608-610) and at the attempt limit.
  - `rowHandler` (659-688): stop state is held per task closure. Poll mode is exempt. The nested `innererror` is included only when `rawResponse` is empty and the code is one of the four connector codes.
  - Only `checkSharedFailure` (518) throws inside `execute`. `SubmissionLocation.validate` (50-61) reuses `validateOperationLocation`, so re-validation during polling cannot throw.
- [x] Checked auth classification against `ServiceAuthHeaders.resolve` (`CognitiveServiceBase.scala` 375-433):
  - Exactly one credential is sent, with precedence subscriptionKey > AADToken > CustomAuthHeader > embedded key > embedded Authorization. `rowUsesColumnAuth` follows the same order.
  - `addHeaders` (549-563) filters user-supplied marker headers, and `toHTTPCore` (`HTTPSchema.scala` 209) strips the marker from the wire.
  - `usesFabricAuth` (169-173) requires the value `true` plus an MwcToken, so the CU marker cannot trigger the Fabric path.
  - `addContentType` removes case-variant duplicates.
- [x] Confirmed `concurrentTimeout` has no default anywhere. `validateConfiguration` rejects it if set, `SimpleHTTPTransformer.scala:133` forwards `get(concurrentTimeout)` (None clears it), and `HTTPTransformer.scala:111` then waits without a bound. Handles are therefore never lost to a shared async wait.
- [x] Reviewed the CU Transport, DnsResolver, ResponseParsing, HeaderValidation and Params sources, `Wrappable.scala` codegen, and the `Utils.py` ComplexParamsMixin. Confirmed:
  - 64KiB/200-header MessageConstraints and bounded decoded-entity reads.
  - Retry-After: at most 19 digits with saturation; HTTP dates; `pollingDelay` fallback.
  - DNS permits released only in the worker's `finally`.
  - JSON depth 1000 and numeric-token limit 100, with string/escape-aware scanning, plus NonFatal/StackOverflow guards.
  - Python scalar view: KeyError on column bindings; bytes never pickle-decoded; loaded output/error defaults refreshed; stubs keep PySpark's `isSet` signature.
- [x] Checked the docs (`docs\Explore Algorithms\AI Services\Content Understanding.md`):
  - Resumable selection uses a non-null handle plus NotStarted/Running/Unknown (~291-297).
  - Retries run only from saved results (~336-345).
  - Bounds and best-effort deadline wording (~357-389).
  - Credential persistence and MLflow warnings (~95-118).
- [x] Inventoried tests: 105 Scala tests (Suite 40, Recovery 35, Concurrency 30) and 23 Python tests, matching the supplied 40/40 and 23/23 counts. `ContentUnderstandingRecoverySuite.scala` 581-590 asserts `OperationTimedOut` for a management 503 (relevant to Issue 1). The stub binds to `127.0.0.1` (`ContentUnderstandingStub.scala:72`). `pipeline.yaml` 821-822 is the approved two-line entry.
- [ ] Tests, builds and codegen were not executed (read-only plan). Validation evidence is taken as supplied.
- [ ] No live CU, external CI or fresh remote-base evidence exists. This prevents a merge-ready claim but is not a code defect.

## Issues

### Issue 1: Management GET retry replaces the last service or transport error with a generic `OperationTimedOut`
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala (also ContentUnderstandingRecoverySuite.scala and docs/Explore Algorithms/AI Services/Content Understanding.md)
- **Line(s)**: ContentUnderstandingProtocol.scala 704-718 (overwrite at 709-710); compare `pollWithClient` 608-610 and the `createAnalyzer` wrap at 756-764; ContentUnderstandingRecoverySuite.scala 581-590; docs lines 364 and 370-373
- **Description**:
  - `getAnalyzerWithClient` serves both `getAnalyzer()` and the final retrieval in `createAnalyzer`. It handles retryable responses: transport failures, and 408/429/5xx responses without a diagnostic.
  - When the next Retry-After or `pollingDelay` does not fit in the remaining `maxOperationDuration`, it returns `response.copy(diagnostic = Some(timeoutError))`.
  - `decode` gives `raw.diagnostic` precedence over the service error and over the original `TransportError`. The exception therefore reports `OperationTimedOut` even though the service answered (for example 503 `Unavailable` or 429) or the I/O failed for a specific reason.
  - The poll path (608-610) handles the same situation differently: it keeps the last error and adds `OperationTimedOut` only when no error exists.
  - The docs say the original diagnostic takes precedence (370-373) and that the budget covers management calls (364).
  - Within the same function, ending retries at the attempt limit keeps the service error. The reported code therefore depends on which limit is reached first.
  - RecoverySuite 581-590 locks in the overwrite.
- **Risk**:
  - Operators see a generic timeout from explicit driver management calls instead of throttling, unavailability or the transport cause.
  - For transport failures `rawResponse` is empty, so the I/O detail is lost entirely. For HTTP errors the detail survives only in `rawResponse`.
  - In `createAnalyzer`, the wrapped exception reports `OperationTimedOut` after provisioning completed, which misstates what happened.
  - No handle is lost and nothing is replayed, hence Low.
- **Suggested Fix**:
  - Apply the poll-path rule: when the delay cannot fit, return `response` unchanged if it already has a connector diagnostic or a non-success status/service error. Optionally nest `OperationTimedOut` as `innererror`. Set `timeoutError` only when nothing more useful exists.
  - Update RecoverySuite 581-590 to assert that `Unavailable` is preserved with one GET. Add a variant that checks `TransportError` is preserved.
  - Alternatively, if the overwrite is intended, document management calls as an explicit exception.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Driver resolution: Issue 1 (2026-11-05)
- **Status**: Fixed locally; independent re-review pending. Original finding and verdict above are preserved.
- **What changed**: `getAnalyzerWithClient` returns the last retryable response unchanged when its retry delay cannot fit in the remaining budget. It no longer replaces an existing service/transport failure with generic `OperationTimedOut`. Documentation explicitly covers both direct management GET and final retrieval after successful creation.
- **Why**: Preserve the useful diagnostic consistently with polling and attempt-limit stops, without replaying provisioning or losing the analyzer management handle.
- **How verified**: Updated the existing management-budget assertion and added a six-case matrix: 503/Unavailable, 429/Throttled and disconnected/TransportError, each with direct GET and final GET after successful creation. Assertions cover error precedence, raw body, status, retained management handle and exact request methods/counts. The two selected management tests failed for the expected overwrite before the production fix; all 109 CU Scala tests passed afterward, along with style/compilation/codegen/publication and all 24 Python tests.
- **Validation history**: Initial regression insertion accidentally nested a Scala test inside a fixture helper and failed compilation. Placement was corrected before obtaining the meaningful pre-fix failing run; no fixture or safety behavior was weakened.
- **Evidence**: Session logs `cu-attempt19-scala-red2.log`, `cu-attempt19-validation.log` and `cu-attempt19-python-green.log`. Attempt 19 decision is recorded in the corresponding GPT attempt-18 artifact; original review remains `issues_found`.