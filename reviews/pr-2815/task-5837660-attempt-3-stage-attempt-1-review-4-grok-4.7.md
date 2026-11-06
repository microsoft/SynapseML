---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 4
timestamp: "2026-11-06T19:45:14Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "126a3393fd8d5260b0760789dc28206bbb12a81d07bfdcfb741e3c5d0513e962"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-4-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Line-checked timeout bound versus millisecond conversion in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala` (`validateConfiguration`, `settings`).
- [x] Traced poll/submit retention, shared-stop, and deadline classification in `ContentUnderstandingProtocol.scala` against the documented running-is-not-success contract; those paths retain handles and do not treat Running/NotStarted as completion.
- [x] Checked auth precedence and Content-Type replacement in `CognitiveServiceBase.scala`, Python scalar inspection/pending-value preservation in `core/src/main/python/synapse/ml/core/schema/Utils.py` and generated setters in `Wrappable.scala`. Column bindings stay JVM-owned; pending values are popped only after a successful JVM set.
- [x] Checked DNS acquire/release pairing in `ContentUnderstandingDnsResolver.scala` and null-input skip behavior via `CustomInputParser` plus `Option(row)` in `SimpleHTTPTransformer.scala`. No additional logic defect found on those paths.

## Issues

### Issue 1: Maximum timeout can wrap to a negative socket timeout
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 111-113, 133
- **Description**: `validateConfiguration` allows `timeout <= Int.MaxValue / 1000.0` and states that the value is representable in milliseconds. `settings` then does `math.ceil(getTimeout * 1000).toInt`. At that bound, IEEE-754 rounding can make the product slightly larger than `Int.MaxValue`. `Double.toInt` keeps only the low 32 bits, so the result is negative (`Int.MinValue` for `2147483648.0`). Apache `RequestConfig` treats a negative socket/connect timeout as unset, so the per-request timeout is not applied.
- **Risk**: A caller who sets the documented maximum can disable the request timeout instead of being rejected. Only the extreme bound is affected; ordinary timeouts are unchanged.
- **Suggested Fix**: Convert in `Long` (`math.ceil(getTimeout * 1000.0).toLong`), require `1L <= millis && millis <= Int.MaxValue`, and pass `millis.toInt` into `Settings`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s4t1-grok-4.7-i1: Claimed Double-to-Int wrap

- Disposition: **false-positive**.
- Evidence: JDK11 probe: (int)2147483648.0 and computed maximum timeout both yield2147483647, not a negative value.
- Consolidated topic: `timeout-overflow`.
