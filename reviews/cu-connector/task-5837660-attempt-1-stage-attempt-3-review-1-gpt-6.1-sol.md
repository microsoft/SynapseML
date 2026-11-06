---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 3
round: 1
timestamp: "2026-11-03T21:09:50Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "6c9ad4215955a945f8c0aa744d37675fdb7715cd135d9a1b406e22e86ff7d068"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-3-review-1-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected all 16 supplied changes, including implementation, Scala/Python tests, fixtures, core schema helper, telemetry, documentation, and sidebar registration.
- [x] Traced submission, polling, retries, deadlines, response bounds, and URL validation in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`.
- [x] Compared generated Python synchronization against `CognitiveServiceBase.scala`, `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`, and `core\src\main\python\synapse\ml\core\schema\Utils.py`.
- [x] Inspected the recovery and endpoint-replacement tests; neither unlisted 5xx polling statuses nor `setCustomServiceName` replacement is covered.
- [x] Considered the caller-reported compilation, style, and test results as supplied evidence, not independently executed results.
- [ ] No commands or tests executed. Live-service behavior remains unverified.

## Issues

### Issue 1: Unlisted 5xx polling errors incorrectly become terminal operation failures
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 467–473
- **Description**: `nonTerminalHttpError` excludes every 5xx response, while `raw.retryable` covers only selected statuses. Consequently, a polling GET returning HTTP 501 or 505 falls through to the decoded `Failed` response, replacing a known `Running` state. Poll mode without previous state likewise reports `Failed` instead of `Unknown`. This contradicts the documented preservation of operation state on non-transient GET errors.
- **Risk**: Callers can mistake a retrieval failure for failure of the remote analysis, abandon pending work, or resubmit an already accepted billable document.
- **Suggested Fix**: Preserve previous state—or use `Unknown`—for unsuccessful polling HTTP responses other than the explicit unavailable-result cases. Keep retry eligibility separate from operation terminality. Add regression cases for unlisted 5xx responses in analyze and poll modes.

### Issue 2: Python parameter transfer silently undoes `setCustomServiceName`
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 223–235
- **Description**: The new Python `setEndpoint` stores `url` in both Python and JVM state, but the inherited `setCustomServiceName` updates only the JVM. After `stage.setEndpoint(old).setCustomServiceName("new-resource")`, Python still holds the old URL. The subsequent superclass parameter transfer writes that stale URL back into the JVM. The same problem occurs when changing the custom service name after loading a stage, because loading populates the Python URL parameter.
- **Risk**: Transform, save, and management calls silently continue using the previous resource, potentially sending documents and credentials to an unintended endpoint.
- **Suggested Fix**: Synchronize Python `url` in the inherited endpoint-changing convenience method as well, or explicitly reject that method if unsupported. Extend Python endpoint-replacement coverage to include `setCustomServiceName` after initial configuration and loading.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: `retainPollState` now treats every poll HTTP status of 300 or higher as a non-terminal GET error, except 404/410 (`UnavailableResults`). An unlisted 5xx such as 501 or 505 keeps the previous status, or `Unknown` when there is none, keeps the operation handle and attaches the `ServerError` diagnostic. Retry eligibility (`raw.retryable`) is still decided separately.
- **Why**: A failed status read must never be reported as a failure of the remote analysis.
- **How verified**: The `ContentUnderstandingRecoverySuite` test "non-transient polling HTTP errors…" now also covers 501 and 505. In analyze mode the result is `Running` with the handle kept and requests POST, GET. In poll mode it is `Unknown` with the handle kept and a single GET. All 58 `ContentUnderstanding*Suite` tests pass.
- **Files**: `ContentUnderstandingProtocol.scala`, `ContentUnderstandingRecoverySuite.scala`.

### Issue 2
- **Status**: Resolved
- **What changed**: The generated Python wrapper now overrides `setCustomServiceName`. It calls the JVM setter and then copies the normalized JVM URL back into Python's `url` param (`self._set(url=self._java_obj.getUrl())`), as `setEndpoint` already does.
- **Why**: Every Python method that changes the endpoint must leave Python and the JVM pointing at the same resource.
- **How verified**: Ran `cognitive/pyCodegen`. The new Python test `test_custom_service_name_survives_python_parameter_transfer` checks the JVM URL after `_transfer_params_to_java()` on a fresh stage and on a saved-and-loaded stage. 10/10 offline Python tests pass and Black is clean.
- **Files**: `ContentUnderstandingParams.scala`, `test_ContentUnderstanding.py`.