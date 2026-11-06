---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 4
timestamp: "2026-11-06T21:15:22Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "90a967f24681a262ec6ccb785398b1793549747eca4c089637de860f660329d2"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-1-review-4-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced atomic scalar/column and explicit-None validation through `ContentUnderstandingParams.scala` and `core\src\main\python\synapse\ml\core\schema\Utils.py`.
- [x] Inspected ordinary-default restoration, recursive sequence/map conversion, raw-byte bypasses, and corresponding Python persistence regressions in `test_ContentUnderstanding.py`.
- [x] Traced task ownership and cleanup in `Clients.scala`, cancellation polling and request abortion in `ContentUnderstandingProtocol.scala`, and monotonic DNS waits and permit release in `ContentUnderstandingDnsResolver.scala`.
- [x] Checked discarded-handle diagnostics and creation lookup state retention against `ContentUnderstandingSubmissionSuite.scala` and the changed documentation.
- [x] Compared `SimpleHTTPTransformer.errorSchema` with runtime `coalesce`/`withColumn` behavior and the new schema tests; identified the two uncovered discrepancies below.
- [x] Considered the supplied driver validation evidence. No commands or tests were executed during this read-only review.

## Issues

### Issue 1: Case-only error-column collisions produce a phantom planned column
- **Severity**: Medium
- **File**: C:\Users\boraelkin\SynapseML-cu-native\core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\SimpleHTTPTransformer.scala
- **Line(s)**: 118–125
- **Description**: The new schema function matches column names case-sensitively, whereas runtime `withColumn` uses Spark's resolver. With default case-insensitive resolution, input columns `data, ERROR` and `setErrorCol("error")` make planning retain `ERROR` and append `error`. Runtime instead replaces `ERROR` with `error`. With the suite's JSON parser configuration, planning therefore returns `data, ERROR, error, results`, while transformation returns `data, error, results`. Previously, the Lambda-derived schema reflected runtime replacement.
- **Risk**: Downstream schema validation can accept a column that will not exist, and planned field counts and positions disagree with actual output.
- **Suggested Fix**: Use a consistent, explicit name-resolution/replacement policy in both planning and runtime without creating a session. Add schema-versus-runtime regressions for case-only collisions under both Spark resolver modes.

### Issue 2: Planning accepts incompatible existing error-column types
- **Severity**: Low
- **File**: C:\Users\boraelkin\SynapseML-cu-native\core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\SimpleHTTPTransformer.scala
- **Line(s)**: 116–120
- **Description**: `errorSchema` unconditionally substitutes `ErrorUtils.ErrorSchema` for an existing error column without checking its type. For example, an input `error: StringType` passes `transformSchema`, but runtime constructs `coalesce(error, serviceError)`, whose string and struct arguments are incompatible. The previous Lambda-based schema calculation rejected this schema during analysis.
- **Risk**: Public schema validation now approves unsupported input and advertises a struct output that transformation cannot produce.
- **Suggested Fix**: Validate the existing error field's compatibility with the runtime coalesce expression and derive the corresponding output type. Add a negative schema test using an existing string error column.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Planning and runtime resolve case-only collisions differently.
- **How verified**: Source inspection of `errorSchema`, runtime `withColumn`, and parser schema propagation; no tests executed.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: Explicit planning omits runtime error-column type compatibility checks.
- **How verified**: Source inspection of unconditional schema replacement versus runtime `coalesce`; no tests executed.

## Driver resolution history: targeted iteration 2

The original review above is preserved. Its finalized receipt, actual model,
input hash and 45 scoped read-only calls passed the transport audit.

Both findings were reproduced before correction. The case-only test showed a
phantom `ERROR` field in the planned schema. The incompatible-string test showed
that schema validation accepted an input the runtime could not coalesce.
A directly related regression also demonstrated incorrect planned types under
ANSI coercion (`response: string` planned versus `response: long` at runtime).
All three failures are retained in `cu-focused-t2-red-core.log`.

The correction uses Spark's resolver for replacement and its ANSI/non-ANSI type
coercion rules for existing exact-name preparation errors. It rejects ambiguous
resolved names and incompatible types without creating a Spark session or RDD.
The existing runtime distinction between exact-name coalescing and resolver-based
column replacement is preserved, rather than silently changing that behavior.

Status at this entry: **implemented locally; green validation and fresh targeted
review pending**. The first-pass verdict remains ISSUES_FOUND.
