---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 1
timestamp: "2026-11-03T17:51:17Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6fe870940ebd72dbefcd28062f158f8fa144c650eae225cc73bc5641076b7f78"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala` (1-365). Traced `activeParams` and `getVectorParamMap` (63-76), `settings` and `isScalar` (95-105), schema and row validation (189-225), the validation UDF pipeline and `addServiceErrors` (276-324), and driver-only management (326-364).
- [x] Read all of `.../contentunderstanding/ContentUnderstandingProtocol.scala` (1-606). Traced:
  - endpoint and Operation-Location validation (144-195)
  - `readBounded` and the deadline abort (234-326)
  - the per-operation client with no retries, redirects, or cookies (328-337)
  - `decode` (353-384)
  - shared-failure classification, submission, and poll-state retention (396-473)
  - the poll loop and `execute` (475-527)
  - management (535-606)
- [x] Read `.../contentunderstanding/ContentUnderstandingParams.scala`: defaults at 127-133, and the Python overrides including `_transfer_params_from_java` at 188 and `_transfer_params_to_java` at 200. Cross-checked against `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` (125-235, 454-497), `core/src/main/scala/com/microsoft/azure/synapse/ml/param/PythonWrappableParam.scala` (28-38), and `core/src/main/python/synapse/ml/core/serialize/java_params_patch.py`.
- [x] Checked the base pipeline in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` (transform, global parameter transfer, auth resolution, `outputCol`/`errorCol` defaults at 703-705) and `core/.../io/http/{SimpleHTTPTransformer,HTTPTransformer,HTTPClients,Clients,HTTPSchema,Parsers}.scala`. Null inputs are not dispatched, and the error struct matches the `ErrorUtils` shape. Async failures and `concurrentTimeout` fail the task.
- [x] Confirmed these security properties hold:
  - Operation-Location must have the same origin as the endpoint, a single safe path segment, and exactly one `api-version`; `%2e%2e` is rejected (Protocol 176-195).
  - HTTP is allowed only for literal loopback addresses (144-153).
  - Redirects, automatic retries, and cookies are disabled (84-89, 328-331).
  - POST and PUT requests are never replayed (508-527, 571-606).
  - Exception messages exclude service text (37-42).
  - The OpenAI global and Fabric credential fallbacks are disabled (ContentUnderstanding.scala 52-55).
  - `analyzerId` is limited to a safe path segment (Protocol 129-135).
  - `documentUrl` rejects user info and fragments (ContentUnderstanding.scala 155-160).
  - The response size limit applies after gzip decompression.
- [x] Read the tests without running them: `cognitive/src/test/scala/.../contentunderstanding/ContentUnderstandingSuite.scala` (160-240 plus grep), `ContentUnderstandingRecoverySuite.scala` (60-200), and `cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py`. The shared-failure tests (Recovery 92-104, 186-195) cover analyze mode only. No test covers poll-mode classification of analyzer or deployment failures, or a non-transient GET 4xx after an accepted submission.
- [x] Read all of `docs/Explore Algorithms/AI Services/Content Understanding.md` and checked its claims at lines 43, 162, 183-187, and 204-227 against the code.
- [ ] Builds and tests were not run by this reviewer, because the review was read-only. This review relies on the supplied validation evidence: scalastyle, compile, 47 Scala Content Understanding tests, 5 VerifyFeatureNames tests, 5 offline Python tests, codegen, and black. Live Content Understanding tests were not run because they need credentials.

## Issues

### Issue 1: Poll mode treats analyzer and deployment failures as shared and aborts the whole action
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala; .../ContentUnderstandingProtocol.scala
- **Line(s)**: ContentUnderstanding.scala 63-71, 95-105; ContentUnderstandingParams.scala 128; ContentUnderstandingProtocol.scala 396-406, 496
- **Description**:
  - In poll mode, `analyzerId` and `modelDeployments` are not active inputs (63-71). `settings` still computes `sharedAnalyzer = isScalar(analyzerId)` and `sharedModels = isScalar(analyzerId) && isScalar(modelDeployments)` from them (100-101).
  - On a poller, `analyzerId` keeps its scalar default `prebuilt-read` (Params 128), and `isScalar` treats an unset `modelDeployments` as scalar (104-105). Both flags are therefore true for the documented poller (docs 211-219).
  - `pollWithClient` runs `checkSharedFailure` on every GET (Protocol 496). Any polled operation whose `error` or `innererror` code is `AnalyzerNotFound`, `DefaultsNotSet`, `DeploymentNotFound`, or `ModelDeploymentNotFound` throws `ContentUnderstandingException` and fails the task.
  - This happens even when the handle came from a submit that used `setAnalyzerIdCol` or `setModelDeploymentsCol`. In analyze mode, that same failure stays a row-level failure.
  - The poller's analyzer and deployment params play no part in a GET, so they cannot show whether a failure was shared. This contradicts docs 183-187. The only workaround is setting an unused `setAnalyzerIdCol` on the poller, which is undocumented.
- **Risk**: One row-specific terminal failure stops polling for every saved handle. The terminal result doesn't change, so re-running the poll fails the same way every time. Results for the other handles in that action are lost until the user finds and excludes the bad handle, and the exception message deliberately omits details. This breaks the documented submit, save, then poll recovery workflow.
- **Suggested Fix**:
  - Set `sharedAnalyzer = false` and `sharedModels = false` when `getOperationMode == "poll"`. Keep `sharedAuth`, because the poller uses its own credentials. An explicit opt-in parameter is an alternative.
  - Add an offline test: poll mode with `operationLocationCol`, where one handle returns `{"status":"Failed","error":{"code":"ResourceError","innererror":{"code":"DeploymentNotFound"}}}` and another returns `Succeeded`. Assert that both rows are emitted, the first as Failed with its error and the second as Succeeded.
  - Document how poll mode classifies these failures.

### Issue 2: A non-transient HTTP error on a status GET turns a known Running operation into "Failed"
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 360-366, 454-473, 496-499
- **Description**:
  - Consider a GET that returns a non-2xx status other than 404 or 410 and outside `TransientStatuses`: for example 400, or 401/403 when authentication is set per column (`sharedAuth = false`).
  - `retainPollState` returns `parsed` unchanged (467-471). `decode` has already set its status to `"Failed"`, only because `!raw.successful` (362-363), and the loop stops (498).
  - In analyze mode, an accepted operation last known as `Running` is emitted with `status = "Failed"`, and `rawResponse` holds the GET error body instead of the operation JSON. That status describes the poll request, not the remote operation, which may still be running or may already have succeeded.
  - Malformed, oversized, and transport-failure poll responses keep the previous state (467-469; Recovery test 82-90, "malformed polling responses do not turn a known Running operation into a terminal failure"). HTTP error GETs do not.
  - The docs define `status` as the service status or a connector diagnostic state (line 162), and describe only the 404/410 case (line 227).
- **Risk**: For example, a per-row AAD token expires mid-operation (the connector never refreshes tokens), so the GET returns 401 and the row reads "Failed". Users who treat "Failed" as final may resubmit and pay twice, or discard a document whose analysis actually succeeded. Users who re-poll only Running, NotStarted, or Unknown rows will skip a handle that can still be resumed.
- **Suggested Fix**:
  - In `retainPollState`, treat `!raw.successful && !UnavailableResults(raw.httpStatus)` as a failed poll request. Keep the previous status, or use `"Unknown"` if there is none, and attach `httpStatus` and the HTTP or service error while keeping the handle. Analyzer management is unaffected, because `createAnalyzer` still requires `Succeeded` with no error.
  - Add tests for POST 202 followed by GET 400, and POST 202 followed by GET 403 with `setSubscriptionKeyCol`. Each should end in Running or Unknown, with the handle and the error kept.
  - Document this behavior next to docs line 227.

### Issue 3: Docs promise stage-unique default output/error columns, but Python defaults are fixed per class
- **Severity**: Low
- **File**: docs/Explore Algorithms/AI Services/Content Understanding.md
- **Line(s)**: 43 (related: CognitiveServiceBase.scala 703-705; Wrappable.scala 140-160)
- **Description**:
  - The docs say: "`outputCol` and `errorCol` are optional; defaults are unique to the stage." In Scala the defaults are `uid + "_output"` and `uid + "_error"`.
  - The generated Python wrapper takes `thisStage.getDefault(p)` from the single instance created during codegen. It writes that value into `self._setDefault(outputCol="ContentUnderstanding_<codegen-uid>_output", ...)` (`pyParamArg`/`pyParamDefault`).
  - PySpark's `_transfer_params_to_java` then copies these Python defaults to the JVM object before `transform`. Every Python `ContentUnderstanding()` therefore has the same default column names.
- **Risk**: Python users who chain stages with default column names get "outputCol and errorCol must not overwrite input columns" at the second stage. For example, the submit-then-poll recipe without `setOutputCol` fails this way, because `pending` keeps the submit stage's output column (docs 204-219). The error is immediate and explicit, but the documented guarantee is false in the main documented language.
- **Suggested Fix**: Reword line 43, for example: "Defaults derive from the stage UID in Scala; in Python they are fixed per class, so set distinct names when chaining stages such as submit and poll." Alternatively, override how `ContentUnderstandingPython` renders the `outputCol`/`errorCol` defaults so they derive from `self.uid` when the object is constructed.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Poll mode no longer classifies analyzer or deployment failures as shared failures; documentation now describes row-level poll behavior.
- **Why**: Poll requests do not use the poller's analyzer/deployment settings, so those settings cannot distinguish a shared failure.
- **How verified**: Added poll-mode row-failure regression; the CU Scala suites passed (50/50).

### Issue 2
- **Status**: Fixed
- **What changed**: Non-transient poll GET errors retain the known operation state and handle rather than marking the remote operation failed.
- **Why**: A failed status request does not establish that the asynchronous operation itself failed.
- **How verified**: Added non-transient poll-error regressions; the CU Scala suites passed (50/50).

### Issue 3
- **Status**: Fixed
- **What changed**: Generated Python output/error defaults are now runtime-instance-specific, with the related documentation corrected.
- **Why**: Prevent default names from colliding when users compose multiple Python stages.
- **How verified**: Added distinct-default and pipeline-composition regressions in `test_ContentUnderstanding.py`; offline CU Python tests passed (7/7), as did Black formatting validation.