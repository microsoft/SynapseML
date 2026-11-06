---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 12
round: 1
timestamp: "2026-11-04T19:18:40Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "de334e0cbdcf4a068c4257ce8df02243d0037099bde9d41d53191c632c57ce18"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-12-review-1-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` submit, poll, shared-failure, and deadline paths
- [x] Inspected `ContentUnderstandingDnsResolver.scala`, `ContentUnderstanding.scala`, and `CognitiveServiceBase.scala` auth/header resolution
- [x] Inspected `HTTPSchema.scala` marker stripping and `SimpleHTTPTransformer.scala` pipeline error handling
- [x] Compared retry classification with `docs/Explore Algorithms/AI Services/Content Understanding.md` and the preserve-handles contract
- [ ] Tests were not re-executed; supplied 82/82 and Python 15/15 results were not treated as proof of this classification

## Issues

### Issue 1: Unsent connect and pool timeouts are classified as ambiguous submissions
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 409-416, 540-548
- **Description**: Only `UnknownHostException` and a budget that is already zero set `notSent`. `ConnectTimeoutException` and connection-pool acquisition timeouts are `IOException`s raised before any request bytes are written, but the handler always returns `transportFailure` with `notSent` false. If the deadline also fired, the exception class is replaced by `OperationTimedOut`. `decodeSubmission` then marks the row `Unknown` instead of `Rejected` / `RequestNotSent`. A post-send read timeout follows the same status and error code.
- **Risk**: Unsent deadline and connect failures are not distinguishable from work that may have been accepted. Callers following the documented retry filter skip `Unknown`, so documents that never reached CU are left unretried. Treating every `OperationTimedOut` as safe to resubmit can also duplicate a POST that was already written.
- **Suggested Fix**: Classify `ConnectTimeoutException` and connection-pool timeout exceptions as `notSent` before any bytes are written, and keep `SocketTimeoutException` after execute has started as ambiguous. Preserve that distinction when the operation deadline fires instead of collapsing both to `OperationTimedOut` plus `Unknown`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver triage

- The original report/verdict is preserved. This overlaps Opus issue 4.
- Added local regressions for a refused loopback connection and a deadline during a stalled TLS handshake, with no CU HTTP request written. Existing post-send disconnect/time-budget tests remain in scope to ensure ambiguity is preserved after writing starts.
- The first Scala reproduction run stopped on one overlong assertion, now wrapped without suppression. `cu-attempt12-scala-red2` is running against unchanged production Scala. Runtime classification is not yet independently confirmed.
- No concurrency cap, unbounded cancellation workers or retry of ambiguous POSTs is proposed.

### Reproduction and implementation update

- `cu-attempt12-scala-red2.log`: the TLS-setup deadline returned `Unknown` after the loopback connection was accepted but before any HTTP request was written. This concretely reproduces a pre-send deadline misclassification.
- The initial closed-port test passed, but was not adequate proof of the refusal path: it took 9.684s with a 2s budget and did not assert the exception type. It could pass through the existing unsent-budget path. Increased its budget and now require `HttpHostConnectException` alongside `Rejected`/`RequestNotSent`.
- Implemented a per-exchange context marker set immediately before Apache `HttpRequestExecutor.doSendRequest`. An IOException before that marker and before any response status is unsent; after the marker it stays ambiguous, including partial writes. This also covers connection acquisition/TLS/deadline failures without guessing from exception names. The marker is independent for each request and adds no concurrency admission limit.
- The first post-fix run stopped at the exchange method's cyclomatic-complexity limit. Extracted diagnostic construction without suppressing the rule or changing its messages. `cu-attempt12-fixes-validation2` is running; green validation and independent re-review remain pending.

### Completed local verification

- The new tracking/diagnostic code now lives in private `ContentUnderstandingTransport.scala`; extraction and singleton naming satisfy repository lint limits without suppressions.
- `cu-attempt12-fixes-validation4.log`: main/test style and compilation passed, followed by 85/85 CU tests. The strengthened refusal test passed with the actual `HttpHostConnectException`; the TLS deadline passed in 1.01s. Existing post-send ambiguity, poll-handle, deadline fallback and no-replay tests also passed.
- Codegen/local publication succeeded, and fingerprint-verified Python passed 16/16. The reported classification problem is locally addressed; no concurrency cap or hard deadline guarantee was introduced. Independent re-review remains required, and the original verdict is not overwritten.