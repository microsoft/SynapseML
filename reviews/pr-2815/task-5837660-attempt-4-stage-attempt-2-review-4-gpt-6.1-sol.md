---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 2
round: 4
timestamp: "2026-11-06T22:07:53Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "f626a39e36089aa555ffc456395aa7f3c34ba4e89e2462ad4c265858a64579d4"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-2-review-4-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced atomic service-argument validation and ordinary-`None` handling in `ContentUnderstandingParams.scala` and `core\schema\Utils.py`, including corresponding Python regressions.
- [x] Inspected JVM-default restoration, sequence/map conversion, and raw-byte guards against the copy/load/transfer tests.
- [x] Traced task ownership and cleanup in `Clients.scala`, cancellation checks during pauses/DNS waits, and scheduled HTTP aborts in `ContentUnderstandingProtocol.scala`.
- [x] Checked DNS FIFO admission, timeout calculations, and permit-release paths against admission and saturation tests.
- [x] Inspected explicit HTTP schema planning and resolver/ANSI/nullability regressions in `SimpleHTTPTransformerSuite.scala`.
- [x] Traced submission diagnostics and creation lookup state retention; identified nonobject diagnostic loss in the new management-error wrapper.
- [x] Considered the supplied driver validation evidence. No commands or tests were executed during this read-only review.

## Issues

### Issue 1: Creation lookup wrapper discards nonobject service errors
- **Severity**: Low
- **File**: C:\Users\boraelkin\SynapseML-cu-native\cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 571–574
- **Description**: For creation-operation HTTP 404/410 responses, `response.error.flatMap(asObject).getOrElse(JsObject())` replaces string or array service errors with `{}`. For example, `{"error":"lookup detail"}` loses its detail. `retainPollState` subsequently preserves the earlier accepted `rawResponse` (line 588), so the discarded lookup diagnostic is unavailable anywhere in the returned exception response. The new creation regression covers only object-valued errors.
- **Risk**: Callers retain the correct accepted state and recovery guidance but lose service information needed to diagnose the lookup failure.
- **Suggested Fix**: Preserve nonobject errors in `innererror`, using a string fallback as `SubmissionLocation.retainFailure` does. Extend the creation 404/410 regression with string and array errors, asserting their preservation alongside the accepted state and recovery guidance.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: The new wrapper silently discards supported nonobject error content.
- **How verified**: Source tracing through `decode`, `decodePoll`, and `retainPollState`; inspected the creation lookup and nonobject submission regressions. No tests executed by this reviewer.

## Driver resolution history: targeted iteration 3

The original response is preserved. The finalized receipt, actual model identity,
input hash and all 42 read-only calls passed the transport audit.

The creation lookup regression was expanded to errors with object, message-only
object, string and array shapes. It failed before correction when the string
detail was discarded. This is a shape issue, not a requirement for a code field:
the message-only object was already preserved. The original accepted state and
handle remained intact throughout the reproduction.

The correction keeps object errors intact and preserves other serialized errors
as string-valued innererror, using safe compaction. The final matrix also includes
numeric and boolean errors and asserts the exact retained innererror, accepted
state, recovery guidance and PUT/GET request sequence.

Status: **implemented locally; green validation and fresh targeted review pending**.
The red evidence is retained in `cu-focused-t3-red.log`; no original verdict was
changed.
