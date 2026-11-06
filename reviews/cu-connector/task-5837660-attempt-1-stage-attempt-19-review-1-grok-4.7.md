---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 19
round: 1
timestamp: "2026-11-05T06:06:49Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "91b4b295cdb86eddcf5727009b65fbca1f00dd11746d8311442f61b3c2041039"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-19-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed admission, poll retention, shared-stop, and analyzer-create handle handling in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` against the handle/stop contract.
- [x] Reviewed Dataset resolver validation versus internal struct typing in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala` (`validateParamSchema`, `transform`, `inputTransformer`).
- [x] Reviewed DNS permit lifetime in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala` and auth precedence in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala`.
- [x] Reviewed Python scalar/column inspection in `core/src/main/python/synapse/ml/core/schema/Utils.py` and generated getter/isSet changes in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`.
- [ ] Did not execute tests; caller-supplied compile and suite results were treated as evidence of prior runs, not re-verified here.

## Issues

### Issue 1: Internal input schema uses the active session resolver
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 359
- **Description**: Direct `transform` validates bindings with the Dataset resolver and requires an exact column name, but `inputTransformer` builds the UDF struct type with `schema(name)`. Spark's `StructType.apply` uses the active `SQLConf` resolver and can cache that lookup. If the Dataset session is case-sensitive and the active session is not, a case-variant column can supply the declared field type even though execution selects the exact bound column.
- **Risk**: Schema planning can disagree with the executed struct, causing an analysis failure or a wrong internal type when case-variant columns differ. That breaks the requirement that direct transform planning follow the Dataset session.
- **Suggested Fix**: Resolve each bound field with an exact `schema.fields.find(_.name == name)` lookup, and fail if that field is absent. Do not call `schema(name)`.

### Issue 2: DNS lookup can start after the budget is exhausted
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala
- **Line(s)**: 33-45
- **Description**: `tryAcquire` waits for the budget captured at entry. If the permit is granted only as that budget expires, the code still submits `delegate.resolve` before `get(remainingMs())` observes zero and times out. The worker then holds an executor-wide DNS permit until the lookup finishes.
- **Risk**: A caller that has already missed its deadline occupies DNS capacity and can force other in-budget operations to return `DnsCapacityExceeded` without having sent HTTP.
- **Suggested Fix**: If `remainingMs()` is zero after acquire, release the permit and throw `OperationDeadlineExceeded` before `Executor.submit`. Keep the existing rule that a lookup already submitted is not cancelled or replaced.

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

### Driver triage (2026-11-05)
- **Issue 1: Invalid for the target runtime.** Inspected the actual Spark 3.5.0 `spark-sql-api_2.12` JVM bytecode with JDK 11 `javap -p -c`. `StructType.apply(String)` invokes `nameToField.getOrElse`; `nameToField` is built from exact `StructField.name -> field` tuples. Neither lookup nor construction consults SQLConf or a case-insensitive resolver. The proposed exact lookup is already the effective behavior. No source change is warranted; existing Dataset/session matrix coverage remains intact.
- **Issue 2: Confirmed and fixed locally; validation in progress.** Added a post-acquire budget check which releases the permit and throws before DNS worker submission. Submitted work still owns its permit until physical lookup completion. A deterministic budget callback expires between entry and post-acquire checks; repeated attempts exceed the capacity count to detect leaked permits, assert no delegate invocation, and then verify a live-budget lookup succeeds.
- **Red evidence**: The DNS regression failed before production changes. The new admission suite ran four tests: three expected failures covering DNS, shared missing authentication, and implicit analyzer creation; its column-auth preservation test passed. See session `cu-attempt20-scala-red.log`.
- **Review audit**: 31 read/search calls, no forbidden tools or prior/sibling feedback reads detected. Canonical full-input hash `91b4b295cdb86eddcf5727009b65fbca1f00dd11746d8311442f61b3c2041039`. Original finding and verdict remain unchanged.

### Driver validation completed
- **Issue 2**: Fixed and locally validated. All 113 CU Scala tests and 25 Python tests passed, including the new no-submission/no-permit-leak DNS regression and existing blocked-worker capacity/recovery tests. Cognitive main/test style, compilation, codegen/publication, Black (205 unchanged) and diff whitespace checks passed. Independent re-review remains pending.
- **Evidence**: Session `cu-attempt20-validation.log` and `cu-attempt20-python-green.log`. Built/published/loaded cognitive SHA256 `e3584edd245a601a5519f55d64ad9b417fc395a861fac15d1d218aa31ffc3076`; core SHA256 `8bd5e1e05d73a81831a6795d00597110a0f8133905295702e78a9f1d0054a05e`.