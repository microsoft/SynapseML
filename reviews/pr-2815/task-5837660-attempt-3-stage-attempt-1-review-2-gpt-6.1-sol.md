---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 2
timestamp: "2026-11-06T19:06:18Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "87953e42a8b925667bc0aa8c426bd8e4c47ac0152a3ed198b165bc435ed4f1a9"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-2-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied contribution against repository conventions in `AGENTS.md` and the master branch reference.
- [x] Inspected the Scala stage, management boundary, snapshot-bound handler, protocol state handling, authentication provenance, and bounded transport helpers.
- [x] Traced schema planning through `ContentUnderstanding.scala`, `SimpleHTTPTransformer.scala`, `NamespaceInjections.scala`, and `Lambda.scala`.
- [x] Reviewed shared Python parameter inspection, generated setter delegation, binary conversion, persistence changes, and corresponding lifecycle regressions.
- [x] Reviewed supplied Scala/Python tests, documentation, sidebar registration, and the authorized CI matrix addition.
- [ ] No commands or tests executed during this read-only review. Supplied validation reports local passes; live-service, packaged-runtime, and Azure CI validation remain outstanding.

## Issues

### Issue 1: CU schema planning still enters an RDD-based helper
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`
- **Line(s)**: 411–423
- **Description**: The new pipeline gives CU’s own `Lambda` stages explicit schema callbacks, but its `SimpleHTTPTransformer` dependency still creates `parseErrors` without one (`SimpleHTTPTransformer.scala:138–148`). Consequently, public CU schema planning traverses `SimpleHTTPTransformer.transformSchema` into `Lambda.transformSchema`, which calls `SparkContext.getOrCreate()` and constructs `sc.emptyRDD[Row]` (`Lambda.scala:60–64`). The outer callbacks therefore do not make the complete CU planning path DataFrame/Dataset-only.
- **Risk**: On managed Spark modes that prohibit SparkContext/RDD operations, even a correctly configured CU stage fails during schema planning, before dispatch. Ordinary local Spark tests do not expose this incompatibility with the repository’s managed-mode convention.
- **Suggested Fix**: Give the shared HTTP error-parsing `Lambda` an explicit, runtime-equivalent schema callback, preserving existing error-column handling and response nullability. Add a focused CU planning regression that rejects SparkContext/RDD access. This removes the inherited dependency without replacing the HTTP architecture or expanding Fabric integration scope.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: The new CU planning path retains an RDD-dependent schema-inference boundary.
- **How verified**: Static call-path inspection only; no tests executed.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s2t1-gpt-6.1-sol-i1: Inherited schema planning enters an RDD helper

- Disposition: **confirmed-source-dependency**.
- Evidence: SimpleHTTPTransformer.scala:138-148 constructs parseErrors Lambda without schema callback; Lambda.scala:60-64 creates SparkContext/emptyRDD. Actual restricted-runtime failure not reproduced.
- Consolidated topic: `schema-rdd`.
