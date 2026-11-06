---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 2
timestamp: "2026-11-06T19:03:54Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "806b641f41e9feda231545195fdc9977b855a5659ba6a42be719c9290049b40b"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-2-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared `ContentUnderstanding` with existing `CognitiveServicesBaseNoHandler` stages and `HasCustomHeaders` / `ServiceParam` setter-getter shape in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\CognitiveServiceBase.scala` and `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala`.
- [x] Checked shared serialization and column-naming extensions: `saveDefaultParamsAsExplicit` in `core\src\main\scala\org\apache\spark\ml\ComplexParamsSerializer.scala`, and session-aware `findUnusedColumnName` in `core\src\main\scala\com\microsoft\azure\synapse\ml\core\schema\DatasetExtensions.scala` plus `SimpleHTTPTransformer.scala`.
- [x] Checked Python bridge conventions in `core\src\main\python\synapse\ml\core\schema\Utils.py` (`isSet`, `hasDefault`, `getOrDefault`, `extractParamMap`, `clear`) against PySpark's fluent `Params.clear` contract, and the CU override in `ContentUnderstandingParams.scala`.
- [x] Confirmed the Fabric marker reuse is stripped in `HTTPRequestData.toHTTPCore` and is not treated as MWC auth unless the value is `true` (`core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\HTTPSchema.scala`). Documented limit, not a defect.
- [x] Scala-first stage, `Wrappable`, `copy`/`defaultCopy`, logging, and driver-only analyzer management match AGENTS.md. Static DNS/deadline pools and HTTP-200 failure envelopes are documented bounds, not new defects.

## Issues

### Issue 1: Shared ServiceParam clear() drops PySpark's fluent return
- **Severity**: Low
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: 248-254
- **Description**: `ComplexParamsMixin.clear` clears JVM and Python state but returns `None` on both the ServiceParam and non-ServiceParam branches. PySpark `Params.clear` returns `self`. `ContentUnderstanding`'s generated `clear` does `return super().clear(param)`, so it also returns `None`.
- **Risk**: `.clear(param).set...()` chaining fails for every stage using this mixin, not only Content Understanding. Callers that rely on the inherited fluent contract get `AttributeError`.
- **Suggested Fix**: Return `self` after the ServiceParam branch, and `return super().clear(param)` on the other branch. Keep the CU override as a single `return super().clear(param)` so it does not double-clear.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver adjudication

Issue 1 is **rejected as a false positive**. The target's installed PySpark
3.5 `pyspark/ml/param/__init__.py:509-514` defines
`def clear(self, param: Param) -> None` and has no return statement.
There is no inherited fluent-return contract to preserve. Returning the stage
would introduce a new API behavior, not fix a regression. The source was
rechecked directly for this report; no production change was made.

The original reviewer verdict is preserved. This disposition does not
silently convert the runner's issues_found receipt into a clean receipt.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s2t1-grok-4.7-i1: Claimed fluent clear return

- Disposition: **false-positive**.
- Evidence: Target PySpark Params.clear:509-514 returns None. No fluent contract exists.
- Consolidated topic: `clear-return`.
