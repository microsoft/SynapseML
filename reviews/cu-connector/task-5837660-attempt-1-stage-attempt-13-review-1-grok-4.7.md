---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 13
round: 1
timestamp: "2026-11-04T20:31:22Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "70aad7ff50b9e6349c526d14a88dfe969bd56e56e0c28fd6d9fba9a2e0159a69"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-13-review-1-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

```yaml
producer: review-code
schema_version: 1
tier: max
contract: direct
execution_backend: run-review-round
attempt: 1
stage_attempt: 13
round: 1
timestamp: 2026-11-04T20:25:15Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: fb18f5f61ab57ea46d86d460177c0e5d69f4daae5c99611df1bb34863cd958d7
```

## Evidence Checklist
- [x] Read `ContentUnderstanding.scala` transform, schema validation, row handling, and internal pipeline construction.
- [x] Read `ContentUnderstandingProtocol.scala` submission classification, shared-failure circuit, poll retention, deadlines, and analyzer management.
- [x] Read `ContentUnderstandingDnsResolver.scala`, `ContentUnderstandingTransport.scala`, and `ContentUnderstandingHeaderValidation.scala`.
- [x] Checked `HTTPSchema.scala` marker stripping, `HTTPTransformer.scala` null-request skip, and `SimpleHTTPTransformer.scala` internal column selection.
- [x] Checked `Utils.py` scalar/column transfer and redaction against the credential and bytes contract.
- [ ] Tests were not executed in this review; cited suites were inspected only as source.

## Issues

### Issue 1: Schema planning ignores the dataset column resolver
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 59-64, 355-362
- **Description**: `transform` correctly builds the plan with `DatasetExtensions.resolverFor(dataset)`. `getInternalTransformer`, which `transformSchema` and pipeline planning use, still passes `SQLConf.get.resolver`. Output and error collision checks therefore follow the active session, not the Dataset session. A case-sensitive Dataset can be rejected by an active case-insensitive session, or accepted by schema planning and then fail in `transform`.
- **Risk**: Pipeline `fit`/`transformSchema` can disagree with execution. Valid case-sensitive plans fail early, or collisions are missed until action time.
- **Suggested Fix**: Thread the same resolver used for execution into schema planning. Where only a schema is available, document that the caller must supply the Dataset resolver, and do not consult a different active `SQLConf`.

### Issue 2: Column bindings are resolved with exact names only
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 230-238
- **Description**: `validateParamSchema` finds a bound column with `schema.fields.find(_.name == name)` and never uses the `ColumnResolver` already passed to `validateInputSchema`. Output and error names use that resolver, so case sensitivity is inconsistent.
- **Risk**: In a case-insensitive session, `setDocumentBytesCol("Body")` fails as a missing column when the field is `body`, even though Spark would resolve that reference. This is fail-closed, but it rejects otherwise valid plans.
- **Suggested Fix**: Resolve the configured column with the same resolver used for output and error checks, and require exactly one matching field.

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

## Driver resolution and evidence - attempt 13

The original report and verdict are preserved. Both observations are now
documented compatibility constraints rather than silently broadened APIs.
The shared decision, transport audit and validation logs are recorded in the
driver section of this attempt's GPT artifact.

### Issue 1 - documented schema-only session limitation

Verified against Spark 3.5.0 `Pipeline.scala`: `Pipeline.fit` and
`PipelineModel.transform` call `transformSchema(dataset.schema, ...)`, and
stage schema validation receives only a `StructType`, not the source Dataset
or its session. SynapseML's `Lambda.transformSchema` also uses an active-session
empty DataFrame. A new resolver parameter on CU would not be threaded through
Spark's existing Pipeline interface and would introduce an unrelated API.

Direct CU transform continues to use the Dataset resolver without mutating
shared SQLConf. Documentation now requires aligning the active session with
the Dataset session for schema-only validation and pipeline fit/transform.
The new `schema-only pipeline planning requires the matching active session`
test reproduces rejection under mismatched case-sensitivity, then verifies
matching planned/executed schemas, preserved input and successful submission
after activating the owning session. It passed in
`cu-attempt13-fixes-validation2.log`; existing direct-transform tests for both
session directions also pass. This does not claim arbitrary cross-session
Pipeline planning now works automatically.

Reference inspected:
https://raw.githubusercontent.com/apache/spark/v3.5.0/mllib/src/main/scala/org/apache/spark/ml/Pipeline.scala

### Issue 2 - retained existing SynapseML service binding contract

Verified independently at HEAD and in the current shared base:
`CognitiveServiceBase.scala` validates binding names with exact set membership
and reads values with exact `Row.getAs` / `fieldIndex`. Output collision
resolution is a separate concern. Merely relaxing CU schema lookup would
depart from the shared binding contract and requires a complete row-access
design, not a local equality substitution.

Documentation now explicitly requires exact top-level binding names, including
case. The new `service input bindings require exact column names regardless of
case sensitivity` regression checks missing-name rejection with zero requests
and successful exact-name execution under both case-sensitivity settings.
It passed with all 89 CU tests. No case-insensitive binding feature or new
resolver API is claimed; original findings remain visible for independent
re-review.