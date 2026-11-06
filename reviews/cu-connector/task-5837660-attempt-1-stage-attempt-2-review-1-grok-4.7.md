---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 1
timestamp: "2026-11-03T19:25:47Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "0c225246f7710c157efd8316a3c1dda77b44e32c9cc51a621c6908426aa09d95"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: null
attempt: 1
stage_attempt: 2
round: 1
timestamp: null
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: null
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-1-grok-4.7.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed `ContentUnderstanding.scala` validation, request construction, internal pipeline, and `sharedAuth` (`authParams` at lines 61-62, assignment at line 102).
- [x] Reviewed `ContentUnderstandingProtocol.scala` origin checks, `decode` (lines 360-363), and `retainPollState` (lines 455-468) against the poll contract in `docs/Explore Algorithms/AI Services/Content Understanding.md` lines 230-234.
- [x] Traced Python load through `Utils.py` `from_java` (lines 33-36), generated `__init__` defaults (`ContentUnderstandingParams.scala` lines 166-172), custom `_transfer_params_from_java` / `_transfer_params_to_java` (lines 206-229), and `Utils.py` `setDefault` (lines 117-123). UID defaults are set in `CognitiveServiceBase.scala` lines 705-708.
- [x] Compared redirect behavior with `ContentUnderstandingSuite.scala` lines 280-289, which expects `Failed` for both submit and poll redirects.
- [x] Confirmed `ServiceAuthHeaders.resolve` strips auth names from telemetry headers, so a `telemHeaders` column is not a credential source.
- [ ] Supplied Scala/Python results were not re-executed. No live CU service tests were run. `input_sha256` and `execution_backend` were not observed.

## Issues

### Issue 1: Python load overwrites uid-based output and error defaults
- **Severity**: High
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala
- **Line(s)**: 166-172, 206-229
- **Description**: JVM defaults are `uid + "_output"` and `uid + "_error"`. Load does `py_type()`, which builds a throwaway Java object and records that object's column names as Python defaults, then swaps in the saved Java stage. Custom `_transfer_params_from_java` copies only `isSet` values, so it leaves those throwaway defaults in place. The next `_transfer_params_to_java` (transform, `createAnalyzer`, or `getAnalyzer`) calls `setDefault` and replaces the loaded stage's unset defaults. Explicit `setOutputCol` / `setErrorCol` survive; the documented load-then-set-key path does not.
- **Risk**: A saved Python stage or pipeline silently writes analysis to a new column. `getOutputCol()` before the action returns the saved name and a different name after it, so downstream selections miss the result.
- **Suggested Fix**: After `_java_obj` is the loaded stage, refresh unset `outputCol` and `errorCol` defaults from that object before any transfer back to Java. Add a Python test that saves default column names, loads, transforms, and asserts the written names still match the loaded uid.

### Issue 2: Poll-time redirects are reported as operation failure
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 360-363, 461-468
- **Description**: `decode` maps every non-success response to `Failed`. `retainPollState` preserves the previous status for retryable responses, diagnostics, missing status, and HTTP 400-499 except 404/410. HTTP 3xx matches none of those conditions. After an accepted POST, a 302 poll therefore stops as `Failed` even though the handle is still present. With no prior state, poll mode also returns `Failed` rather than `Unknown`. The docs say non-transient GET errors retain the previous status, or `Unknown` when there is no prior state, and do not prove the remote operation failed. `ContentUnderstandingSuite.scala` lines 280-289 currently expects `Failed` for both a submit redirect and an accepted-then-redirect poll.
- **Risk**: Callers treating `Failed` as terminal can abandon a still-running operation or resubmit billable work. The handle is retained, but the status contract is wrong.
- **Suggested Fix**: In `retainPollState`, treat 3xx like other non-terminal GET errors: keep the previous status when present, otherwise use `Unknown`, attach the poll error, and keep the handle. Leave a submit-only redirect, which has no accepted operation, as `Failed`. Split the suite assertion accordingly.

### Issue 3: A telemetry column disables shared authentication failure
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 61-62, 102
- **Description**: `sharedAuth` is `authParams.forall(isScalar)`, and `authParams` includes `telemHeaders`. Telemetry defaults to a scalar map, but `setTelemHeadersCol` makes `sharedAuth` false even when the subscription key, AAD token, and custom auth header are scalar. `ServiceAuthHeaders.resolve` strips api-key and Authorization from telemetry, so that column cannot vary the credential. The docs require HTTP 401/403 with scalar authentication to fail the action.
- **Risk**: A shared expired or wrong key becomes a per-row error instead of failing the action. Already-dispatched work continues, and a systemic auth failure looks like ordinary row failure.
- **Suggested Fix**: Compute `sharedAuth` from `subscriptionKey`, `AADToken`, `CustomAuthHeader`, and `customHeaders` only. Keep `telemHeaders` in the row payload and in the scalar management check. Add a scalar-key plus `telemHeadersCol` 401 test that expects `ContentUnderstandingException`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed (re-reviewed in stage attempt 3)
- **What changed**: The Python `_transfer_params_from_java` override now refreshes the unset `outputCol` and `errorCol` defaults from the loaded JVM stage (`self._setDefault(**{name: getter()})`). This happens before any later `_transfer_params_to_java`.
- **Why**: Loading built a throwaway Java object and kept its UID-derived default column names. The next transform, `createAnalyzer` or `getAnalyzer` pushed those defaults onto the loaded stage, so a saved stage silently wrote to different columns.
- **How verified**: New offline Python test `test_load_preserves_uid_based_default_columns` saves a stage with default columns, loads it, transforms, and asserts the written names still match the loaded UID. The offline Python tests pass 8/8, and the generated Python wrapper was regenerated with `cognitive/pyCodegen`.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala`; `cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py`.

### Issue 2
- **Status**: Fixed (re-reviewed in stage attempt 3)
- **What changed**: `retainPollState` now treats HTTP 3xx on a poll as a non-terminal GET error. It keeps the previous status, or `Unknown` when there is none, attaches the poll error and keeps the handle. A redirect on the initial submit still ends as `Failed`.
- **Why**: An accepted operation whose poll was redirected was reported as `Failed` although the handle was still present. That contradicts the documented contract and could make callers abandon a running operation or resubmit billable work.
- **How verified**: The `ContentUnderstandingSuite` case "submission redirects fail and polling redirects retain the operation state" now asserts the split behavior. Scala 56/56, scalastyle 0 errors.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingSuite.scala`.

### Issue 3
- **Status**: Fixed (re-reviewed in stage attempt 3)
- **What changed**: `sharedAuth` is now computed from a new `credentialParams` set (`subscriptionKey`, `AADToken`, `CustomAuthHeader`, `customHeaders`), which excludes `telemHeaders`. `telemHeaders` stays in the row payload and in the scalar management check.
- **Why**: Telemetry headers cannot carry the credential, because `ServiceAuthHeaders.resolve` strips api-key and Authorization from them. A telemetry column therefore must not turn a shared HTTP 401/403 into per-row errors.
- **How verified**: `ContentUnderstandingRecoverySuite` "telemetry columns do not make shared authentication failures row-specific" uses a scalar key plus `telemHeadersCol` and expects `ContentUnderstandingException` on 401. Scala 56/56, scalastyle 0 errors.
- **Files**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingRecoverySuite.scala`.