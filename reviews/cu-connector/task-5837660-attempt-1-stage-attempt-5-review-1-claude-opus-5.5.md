---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 5
round: 1
timestamp: "2026-11-04T00:36:48Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1bc94a87a42b75a6e6d52063ee8438d9c9d17436f13807ae669cce9951b9b5bd"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-5-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all three new main sources in full: `ContentUnderstanding.scala`, `ContentUnderstandingParams.scala` and `ContentUnderstandingProtocol.scala`. Compared them with the base classes in `CognitiveServiceBase.scala`, specifically `ServiceAuthHeaders.resolve` (lines 365–425) and `addHeaders` (lines 538–583), and with `SimpleHTTPTransformer`, `HTTPSchema.stringToResponse` (line 308) and `Lambda`.
- [x] Traced the protocol state machine through `decode` (431), `safeSubmission` (523), `retainPollState` (559), `pollWithClient` (573), `execute` (608), `rowHandler` (635) and `createAnalyzer` (672). Checked the results against the docs (`Content Understanding.md` lines 158–297) and against tests in `ContentUnderstandingSuite.scala` (lines 196–410, 641–734) and `ContentUnderstandingRecoverySuite.scala` (lines 46–360). I found no defect in the handle retention, Rejected/Unknown, ResultUnavailable, PollAttemptsExhausted, OperationTimedOut or Retry-After paths.
- [x] Security review found no defect:
  - Endpoints must use HTTPS, except literal loopback addresses.
  - Poll URLs must have the same origin as the endpoint, an exact path, and a single `api-version` (`validateOperationLocation`, line 263).
  - Redirects, retries and cookies are disabled (`withClient`, line 408).
  - Header control characters and framing headers are rejected (`validateHeaderSetting`, line 209).
  - `analyzerId` is restricted to a safe path segment.
  - Reads are bounded after gzip decompression (`readBounded`, line 317).
  - Exception and transport messages exclude service text and credentials (lines 39–44).
  - The Fabric fallback and OpenAI global settings are disabled.
- [x] Traced shared versus row-level failure classification: `settings` (`ContentUnderstanding.scala` lines 101–116), `FailureClassifier` (Protocol lines 467–489) and `checkSharedFailure` (line 491). Compared it with docs lines 188–195 and 244–248 and RecoverySuite lines 176–205 and 257–330. This produced Issues 1 and 2.
- [x] Core changes:
  - `Wrappable.scala`: ServiceParam setters now pop `_paramMap` (lines 200–209). ServiceParam getters read the JVM (lines 286–290), so getters do not regress. The hand-written `LangchainTransform.py` is pure Python and is unaffected.
  - `DatasetExtensions.findUnusedColumnName` now uses the SQL resolver (lines 51–55). This only adds a suffix where names collide case-insensitively, so it is safer for its existing callers (88 calls in 33 files). It is covered by `ContentUnderstandingSuite` line 165.
- [x] Python/JVM synchronization overrides (`ContentUnderstandingParams.scala` lines 179–270) behave correctly in PySpark's `copy` and `_from_java` flows. Getters, setters and `clear` stay consistent, and the `outputCol`/`errorCol` defaults do not depend on the uid.
- [x] Internal pipeline (`ContentUnderstanding.scala` lines 294–350): invalid rows are nulled before HTTP, so no request is sent, and `errorCol` is populated. It uses only DataFrame APIs and adds no RDDs.
- [x] `pipeline.yaml` lines 821–822 add a `services.contentunderstanding` leg. `PipelineTestCoverageSuite` requires it. AGENTS.md says to ask before changing `pipeline.yaml`; the driver must confirm that approval and CI evidence exist.
- [x] `FeatureNames.scala` lines 7–8 define the feature name, and `logClass` is called in both new stages. The fuzzing suites use the loopback stub with `testFitting = false`.
- [ ] I did not run builds or tests; this was a read-only review. Caller results were treated only as context.
- [ ] Live Content Understanding (CU) REST assumptions are unverified, because no live or credentialed runs were made:
  - the `inputs[].data` request field;
  - the `stringEncoding` and `processingLocation` query parameters;
  - the Operation-Location path and origin shapes;
  - the error codes `AnalyzerNotFound`, `DeploymentNotFound`, `ModelDeploymentNotFound` and `DefaultsNotSet`;
  - `usage.documentPagesBasic`;
  - the preview API version `2026-06-01-preview`.
- [ ] I did not recompute the diff's SHA-256 because I cannot run commands. The caller supplied `4830C0C528F04855528AFEF1209FCD702B300624D87427888D8502F728AC547E`.

## Issues

### Issue 1: Live test for a missing shared analyzer uses a column-bound apiVersion, so the action cannot fail
- **Severity**: Medium
- **File**: cognitive/src/test/python/synapsemltest/services/test_ContentUnderstandingE2E.py
- **Line(s)**: 10, 47–60 (`setApiVersionCol` at line 55), 181–192
- **Description**: `test_missing_shared_analyzer_fails_the_action` builds its stage with `self.analyzer()`, which binds `apiVersion` to a column.
  - In `settings`, `sharedAnalyzer` requires `isScalar(apiVersion)` (`ContentUnderstanding.scala` line 111). With a column-bound API version it is false.
  - So `checkSharedFailure` never throws on `AnalyzerNotFound`. The row comes back as `Failed`, `collect()` succeeds, and `assertRaises` fails.
  - This is exactly the behavior asserted by RecoverySuite's "per-row API versions keep analyzer and deployment errors row-specific" test (lines 176–205).
  - The module docstring (line 10) says these cases are rejected "before analysis". The docs (lines 192–194) instead describe executor-side detection, not a driver preflight.
- **Risk**: The only real-service acceptance suite fails every time credentials are supplied. Someone might then "fix" the connector and break the documented row-level behavior for per-row API versions. The defect stayed hidden because the live tests were skipped.
- **Suggested Fix**: Use a scalar API version, for example `self.analyzer().setApiVersion(GA_VERSION).setAnalyzerId(missing)`; the scalar setter replaces the column binding. Alternatively, assert the row-level `Failed`/`AnalyzerNotFound` result. Confirm that the live service reports `AnalyzerNotFound`, and correct the docstring.

### Issue 2: A row missing its credential in a customHeaders column fails the whole action instead of returning `Rejected`
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala; ContentUnderstanding.scala
- **Line(s)**: Protocol 478–502; ContentUnderstanding.scala 101–116
- **Description**: This applies when credentials come from a customHeaders column and no scalar credential is set.
  - `sharedAuth` is true, because unset auth params count as scalar.
  - `hasRowSpecificCustomAuth` is true only if this particular request carries a non-blank auth header.
  - A row whose map has no key, or a blank key, is sent without credentials and gets a 401. That 401 is classified as shared, and `ContentUnderstandingException` aborts the task.
  - The equivalent `setSubscriptionKeyCol` row with a null key returns `Rejected` instead.
  - The docs say shared failures mean "401/403 with scalar authentication", and `Rejected` covers 401/403 "when authentication is configured per row" (lines 188 and 244–246). No test covers rows that mix present and missing credentials.
- **Risk**: One bad row fails the whole action. Spark task retries can then resubmit already processed valid rows, causing billable duplicates. The behavior also contradicts the documented semantics.
- **Suggested Fix**: When `customHeadersColumn && !scalarAuthCredential`, classify a 401/403 as row-specific regardless of whether that request had a credential. If keeping fail-fast is intended, document it instead. Either way, add a RecoverySuite test with one row that has a credential and one that does not.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed.
- **What changed**: The missing-analyzer E2E case now sets a scalar API version.
- **Why**: Shared analyzer failure classification requires scalar analyzer and API-version configuration.
- **How verified**: The E2E test configuration was checked against `sharedAnalyzer`; no live CU request was run.

### Issue 2
- **Status**: Fixed.
- **What changed**: Custom-header credential columns are classified as row-specific even when a particular row omits its credential. Shared failures before acceptance fail fast; failures after acceptance return a row result retaining the operation handle.
- **Why**: A row missing credentials must not be mistaken for a shared scalar credential failure, and accepted operations must remain addressable without resubmission.
- **How verified**: `ContentUnderstandingRecoverySuite` covers missing row credentials and pre-/post-acceptance handling; all 70 CU Scala tests passed.