---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 25
round: 1
timestamp: "2026-11-05T19:22:39Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "42f0af59271d64e4a42359c5f3eb1e2cf1427500ad080a3514c5e04d8e8bb211"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-25-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Credential provenance matches the auth header precedence.** `ContentUnderstanding.rowUsesColumnAuth` (ContentUnderstanding.scala ~153-163) checks subscription key, then AAD token, then the custom auth header, then credentials embedded in customHeaders. `ServiceAuthHeaders.resolve` (CognitiveServiceBase.scala ~375-385) uses the same order. A column value that matches a fallback value is not treated as scalar. Settings fingerprint only the scalar Authorization and subscription-key headers, and `allAuthScalar` covers all five auth-capable parameters.
- [x] **The row-auth marker is not sent over the wire.** `prepareRequest` adds `FabricAuthMarkerHeader` only for rows that use column auth (ContentUnderstanding.scala ~330). `HTTPRequestData.toHTTPCore` removes it before sending (core HTTPSchema.scala:209). `addContentType` replaces any Content-Type header regardless of case, so no duplicate is sent (CognitiveServiceBase.scala ~358).
- [x] **Column resolution follows the contract.** `transform` uses `DatasetExtensions.resolverFor(dataset)` and does not change SQLConf. `validateParamSchema` requires an exact name match that is unique under the resolver. `buildInternalTransformer` stops output/error columns from overwriting inputs under the resolver. Temporary column names avoid collisions case-insensitively. Schema-only `getInternalTransformer` uses `SQLConf.get.resolver`, as the contract allows.
- [x] **Inactive column bindings are skipped.** The `getVectorParamMap` override (ContentUnderstanding.scala:89-91) limits input columns to the parameters active for the current mode. Poll-mode snapshots therefore do not reference a missing document column.
- [x] **Row validation returns diagnostics instead of failing tasks.** The row-validation helpers `parseUri`, `validateOperationLocation`, `validateAnalyzerId` and `ContentUnderstandingHeaderValidation.validateSetting` only throw `IllegalArgumentException` (`URISyntaxException` is converted). `rowError` catches that type and returns an `InvalidInput` row diagnostic. Driver-side validation also enforces `maxDocumentBytes`.
- [x] **Creation without an operation URL matches the contract** (ContentUnderstandingProtocol.scala 746-790, `decode` 436-470, `CreationStatusNames` 92-93).
  - `analyzerBody` runs first, so explicit service errors, malformed responses and HTTP/transport diagnostics take precedence.
  - A `ready` status returns the original body.
  - A missing or unknown status keeps `InvalidResponse`.
  - A `failed` status without a service error keeps `OperationFailed`.
  - Other valid non-ready states get `MissingOperationLocation` only when there is no primary error.
  - No operation id is made up from analyzerId, and the PUT is not replayed.
- [x] **Creation with an operation URL and final retrieval match the contract.** Unsafe operation URLs are rejected before any request (`InvalidOperationLocation`). Non-2xx responses with an operation URL are decoded, not followed.
  - In `finishCreation`, if the first retrieval GET expires before it is sent, the completed state is kept and `OperationTimedOut` is added.
  - `getAnalyzerResponse` keeps the earlier sent response when a later unsent retry expires.
  - A successful final retrieval requires `ready` (`AnalyzerNotReady` otherwise).
- [x] **Poll state is retained as required.** `pollWithClient` (ContentUnderstandingProtocol.scala 590-620) keeps the previous state on budget expiry and adds `OperationTimedOut` only when no error is already present. `PollAttemptsExhausted` applies only to ongoing responses with no error. `checkSharedFailure` is called with `acceptedHandle = true`, so polls never trigger the shared stop.
- [x] **Driver-side management checks are in place.** `createAnalyzer` requires `isSet(analyzerId)`, checks that the definition JSON is within limits before the PUT, and is driver-only. Management also requires scalar auth, analyzerId and apiVersion. `concurrentTimeout` is rejected as the contract specifies.
- [x] **The generated Python `isSet` stub is present.** Every `pyAdditionalMethods` override (AzureMapsTraits:23, OpenAIPrompt:168, CognitiveServiceBase:284/636/679, ContentUnderstandingParams:214, PyCodegenSuite:37) calls `super`, so the Wrappable `isSet` default reaches every generated wrapper.
- [ ] **Tests not run.** This was a read-only review, so no tests were executed. The validation logs listed in the plan were not independently re-run.