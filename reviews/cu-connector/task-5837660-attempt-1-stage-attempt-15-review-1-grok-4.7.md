---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 15
round: 1
timestamp: "2026-11-04T22:58:05Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "5c2f6f3fab7da72319ebc72b3458591482d47e33b34e0116e0ce05696fdeb6a9"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-15-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `ContentUnderstandingProtocol.scala` decode, `safeSubmission`, `checkSharedFailure`, `rowHandler`, poll retention, and deadline/DNS exchange paths
- [x] Read `ContentUnderstanding.scala` transform/session resolver, row auth marker, input schema planning, and validation
- [x] Read `BudgetedDnsResolver.scala`, `ContentUnderstandingTransport.scala`, and `HTTPSchema.scala` marker stripping before the wire send
- [x] Read `Utils.py` scalar/column Params view and CU/MLflow docs for credential persistence wording
- [ ] Tests were not executed in this review (read-only)

## Issues

### Issue 1: Unreadable HTTP 404 still trips the shared analyzer stop
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 442-468, 508-522, 530-548, 670-700
- **Description**: A pre-acceptance HTTP 404 is a shared analyzer failure whenever `sharedAnalyzer` is set and status is not `Unknown`. `ResponseTooLarge` sets a diagnostic but not `transportFailure`, so `decode` yields `Failed`, `uncertainSubmission` is false when no `Operation-Location` is present, and `checkSharedFailure` throws. `rowHandler` then sets the partition stop and replaces the top-level error with `SharedConfigurationFailure`. Truncated reads stay `Unknown` via `transportFailure` and do not stop the partition. HTTP 401/403/429 are specified to stay definitive when the body cannot be read; HTTP 404 is specified to be shared only when definitive.
- **Risk**: An oversized or otherwise incomplete 404 is treated as a scalar analyzer/API configuration failure. Later unsent rows in the partition are skipped, and the documented retry filter selects `Failed` plus `SharedConfigurationFailure` instead of a response-size failure.
- **Suggested Fix**: Trip the shared analyzer stop on HTTP 404 only when the status line is complete and no connector body-read diagnostic is present, or when a fully read body contains `AnalyzerNotFound`. If the diagnostic is `ResponseTooLarge` or `TransportError`, keep that diagnostic, do not mark the row a shared configuration failure, and do not set the partition stop.

### Issue 2: Internal input schema lookup ignores the dataset resolver
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 363-376
- **Description**: `buildInternalTransformer` validates bindings with the dataset resolver and exact field names, but `inputTransformer` builds the planned struct type with `schema(name)`. `StructType.apply` uses the active session resolver. When the active session is case-insensitive and the dataset is case-sensitive, a case-variant column can be selected for the planned field type and name. Runtime projection still uses the quoted binding name.
- **Risk**: Direct `transform` planning can advertise the wrong internal struct field when case-variant columns differ, which violates using the dataset session for internal planning. Execution values remain correct; public output schema usually still matches because those columns are dropped.
- **Suggested Fix**: Resolve each binding with the same exact-name lookup used in `validateParamSchema`, and build `StructField`s from those fields without calling `schema(name)`.

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

## Driver resolution update: attempt 15

Original findings and verdict are retained. The final collection audit verified
unchanged source fingerprints, patch hash, HEAD and path set. All 39 Grok calls
were read/search tools with no prior/sibling feedback queries.

- **Issue 1 - reproduced and implemented; final validation pending.**
  `cu-attempt15-grok-probe.log` confirms that an oversized 404 returned a first
  row `Failed`/`SharedConfigurationFailure` (with nested `ResponseTooLarge`),
  skipped the second row as `Rejected`, and sent only one POST.
  Submission uncertainty now includes a 404 with a connector response-read
  diagnostic. It retains `Unknown` and the original diagnostic without a shared
  stop. A regression covers oversized/truncated responses, scalar/column
  analyzers, and a subsequent successful submission. The oversized case failed
  before the fix in `cu-attempt15-scala-red.log`. Complete definitive 404
  configuration-failure behavior is unchanged.
- **Issue 2 - rejected as a false positive; no source change.**
  Spark 3.5.0 `StructType.apply(name: String)` uses the exact-name `nameToField`
  map, not the active session resolver. Verified canonical source:
  <https://raw.githubusercontent.com/apache/spark/v3.5.0/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala>.
  The local JVM probe also checked distinct `document`/`DOCUMENT` fields with
  different types, nullability and metadata under both active sensitivities.
  Each lookup returned the exact requested field. Evidence:
  `cu-attempt15-grok-probe.py` and `.log`. CU already rejects actual resolver
  ambiguity before this schema lookup; there is no corrective change to make.

The source freeze was lifted only after all reviewers completed. Max stage 1
has not advanced; final local validation and independent re-review are pending.

### Completed local validation

The valid finding is locally resolved, pending independent re-review.
`cu-attempt15-fixes-validation2.log` records core/cognitive main/test style,
affected compilation, 1/1 generated-wrapper and 98/98 CU tests, codegen and local
publication passing. `cu-attempt15-python-fixed.log` records 22/22 Python tests
against verified rebuilt artifacts. Full Black passed. The false-positive
schema claim remains disproved by source and runtime evidence; no code was
changed for it. No external CI, live CU request, commit, push or ADO edit ran.