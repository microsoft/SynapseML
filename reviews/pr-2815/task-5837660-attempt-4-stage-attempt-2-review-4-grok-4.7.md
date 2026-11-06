---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 2
round: 4
timestamp: "2026-11-06T22:11:46Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "33f8a26fb31230c2f4e958d981e6e0c090e7de99be6aa30f2ea4c0973519a0bb"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-2-review-4-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] DNS FIFO admission, permit release on every submit failure, and elapsed-time waits checked in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala` and `ContentUnderstandingBudget.scala`
- [x] Cancellation polling, task-context cleanup, and creation 404/410 decode/retain path checked in `ContentUnderstandingProtocol.scala` and `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\Clients.scala`
- [x] Submission diagnostics and non-object error retention checked in `ContentUnderstandingResponseParsing.scala` (`SubmissionLocation.retainFailure`)
- [x] Shared HTTP `errorSchema` compared with runtime `coalesce`/`withColumn` in `SimpleHTTPTransformer.scala` (lines 118–176) and `ErrorUtils.addErrorUDF`
- [x] Python `setParams` validation order checked against `_validate_service_param_arguments` / `_set_params_via_setters` in `core\src\main\python\synapse\ml\core\schema\Utils.py`

## Issues

### Issue 1: Planned error-column nullability does not match runtime coalesce
- **Severity**: Medium
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\SimpleHTTPTransformer.scala
- **Line(s)**: 124–133, 164–172
- **Description**: `errorSchema` sets the error field nullable only when every exact-name existing field is nullable (`existing.forall(_.nullable)`). When an exact-name column exists, runtime uses `coalesce(col(getErrorCol), addErrorUDF)`, and that UDF returns `Option`, so Catalyst marks the expression nullable even if the input field is non-nullable. Planning therefore emits `nullable = false` while `withColumn` emits `nullable = true`.
- **Risk**: `transformSchema` disagrees with the executed schema for a non-nullable preparation-error column. Pipeline schema checks and the equality this change is supposed to guarantee fail, or downstream stages treat the column as non-nullable when the analyzed plan is nullable.
- **Suggested Fix**: Plan the error field as nullable whenever the runtime expression includes `addErrorUDF` or `coalesce` with that UDF (always, for this Lambda). Add a schema-equality test whose input error field is explicitly `nullable = false`.

### Issue 2: Creation 404/410 rewrite drops transport failures and non-object errors
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 566–576
- **Description**: The new analyzer-lookup branch runs for every 404/410, including `transportFailure` after a status line was read and the body failed. It also does `response.error.flatMap(asObject).getOrElse(JsObject())`, so a non-object service error becomes an empty `innererror` and the original text is discarded. `ContentUnderstandingException.message` publishes only the top-level code, so the visible code becomes `OperationLookupFailed` instead of the transport or service diagnostic.
- **Risk**: A truncated or unreadable creation lookup is reported as a recoverable lookup miss. Non-object 404/410 payloads, which submission handling now preserves, are erased, so the retained accepted state has no service detail.
- **Suggested Fix**: Apply this branch only when `!raw.transportFailure`. Nest a non-object error as `JsString`, matching `SubmissionLocation.retainFailure`, and keep object errors as `innererror`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver disposition and resolution history: targeted iteration 3

The original response is preserved. This invocation is valid: the finalized
receipt, actual model identity, input hash and all 33 read-only calls passed the
transport audit with no warnings or scope violations.

### Issue 1: false positive

Coalesce is nonnullable when at least one child is nonnullable; a nullable UDF
does not make the entire expression nullable if the existing error is guaranteed
nonnull. The existing shared HTTP regression constructs a nonnullable preparation
error with struct, compares planned and actual schemas, and verifies preservation
of that error and nullification of the result. It passed in the 12-test shared
HTTP suite. The proposed always-nullable change would contradict that behavior.
No production change is made for this claim.

### Issue 2: confirmed diagnostic preservation/classification gap

The nonobject error loss overlaps both other reviewers and reproduced in the
expanded creation test. The additional body-failure regression reproduced an
oversized 404 body being relabeled OperationLookupFailed instead of ResponseTooLarge.
The same wrapper also enclosed transport diagnostics in the inspected source;
the regression includes truncated responses as well.

The wrapper is now limited to readable lookup responses without a transport or
body diagnostic. Nonobject service errors retain their serialized value rather
than becoming an empty object. The existing accepted state, handle and no-replay
behavior remain unchanged.

Status: **implemented locally; green validation and fresh targeted review pending**.
Evidence: `cu-focused-t3-red.log`. The original reviewer verdict remains intact.
