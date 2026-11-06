---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 7
round: 1
timestamp: "2026-11-04T06:36:52Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "6ebeb60a765f2d7810b66043e928c1d0094632e2a3ded48459103eeea205e616"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-7-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the frozen connector diff against `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, `ContentUnderstandingParams.scala`, and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` (`ServiceAuthHeaders.resolve` ~370–430, `addHeaders`).
- [x] Traced auth precedence, the row-auth marker, marker stripping in `HTTPRequestData.toHTTPCore`, and `FailureClassifier.checkSharedFailure` (~490–530). Equal column credentials stay row-specific; marker spoofing via custom headers is rejected.
- [x] Traced submission location rules (`AcceptedSubmissionStatuses`, 429/5xx not followed), poll-handle retention, operation-location path checks, and the documented no-journal / already-dispatched shared-failure tradeoff in `docs/Explore Algorithms/AI Services/Content Understanding.md`.
- [x] Reviewed invalid-row short-circuit, output-parser round-trip, Python setter `_paramMap.pop` in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`, and the `services.contentunderstanding` pipeline leg. Tests were not executed; caller validation was not treated as proof. `reviews/` artifacts were not opened.

## Issues

### Issue 1: Case-variant Content-Type can be sent beside the connector Content-Type
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 97, 239–245
- **Description**: `ConnectorManagedHeaders` blocks only `content-length`, `host`, and `transfer-encoding`. `ServiceAuthHeaders.resolve` then writes `Content-Type` with a case-sensitive map update (`CognitiveServiceBase.scala` ~426). An exact `Content-Type` custom header is silently overwritten. A case-variant such as `content-type` remains and is also sent on analyze POST and analyzer PUT. Poll GETs already strip `content-type` case-insensitively.
- **Risk**: A POST/PUT can carry two Content-Type values. Servers may reject the request or honor the user value instead of `application/json`, so a valid document body is misread as a row or management failure.
- **Suggested Fix**: Treat `content-type` as connector-managed whenever the connector sets it: reject any casing in `validateHeaderSetting` for those requests, and drop every content-type key case-insensitively before adding the connector value. Document the block.

### Issue 2: Analyzer retrieval failure is reported as Succeeded
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 748–753
- **Description**: After a successful creation poll, a failed ready-check copies the retrieval error onto `completed` but leaves `completed.status` unchanged. `pollWithClient` only reaches this path when that status is `Succeeded`, and `ContentUnderstandingException.getMessage` prints `response.status`.
- **Risk**: Callers and logs see `status=Succeeded` for a creation that threw. A handler that treats `Succeeded` as success can record a ready analyzer that was not confirmed.
- **Suggested Fix**: Copy `status = "Failed"` (or the retrieval status when it is already failed) in the same `completed.copy` that replaces `error`, `httpStatus`, and `rawResponse`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: Content Understanding now rejects `Content-Type` custom headers regardless of casing; shared header assembly removes case-insensitive duplicates before adding the connector's JSON content type. The restriction is documented.
- **Why**: Case-sensitive map insertion could otherwise emit a user-supplied variant alongside the connector-managed JSON header.
- **How verified**: Added casing and canonical-header regressions; both CU suites passed 71/71 tests and `AzureSearchAuthSuite` passed 48/48.

### Issue 2
- **Status**: Resolved
- **What changed**: Analyzer retrieval exceptions now report `Failed` and provide an explicit fallback diagnostic if the retrieval response has no error.
- **Why**: A completed provisioning poll does not mean the final analyzer retrieval/readiness check succeeded.
- **How verified**: Updated the failed-retrieval test to assert `Failed` while retaining the completed operation handle; both CU suites passed 71/71 tests.