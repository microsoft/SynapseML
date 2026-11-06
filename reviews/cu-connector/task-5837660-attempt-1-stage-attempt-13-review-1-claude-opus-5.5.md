---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 13
round: 1
timestamp: "2026-11-04T21:01:05Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "aed929d0d1fb96f03093c0b6c51ca6190a0dffe3fe956641ad20b5458bc55760"
---

I found four issues, all Low, so the verdict is ISSUES_FOUND. Nothing was executed, and the validation results you supplied were not re-run.

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-13-review-1-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Main transformer read in full.** File: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala`.
  - `transform` (L59-64) uses the dataset's own resolver and does not change shared SQLConf.
  - `validateConfiguration` (L98-112) rejects `concurrentTimeout`.
  - Scalar values are validated on the driver: `validateParamSchema` (L231-242) calls `validateValue` (L174-189). Column values are validated per row by the validation UDF in `buildInternalTransformer` (L369-376), which returns `InvalidInput` without sending a request.
  - `activeParams` (L77-85) includes the auth params, so column credentials get header validation on every row.
  - `maxDocumentBytes` is enforced at L201.
- [x] **Submission classification.** File: `ContentUnderstandingProtocol.scala` (`SubmissionLocation` ~L52-76, `exchangeBeforeDeadline` ~L380-411, `uncertainSubmission` L529-537, `decodeSubmission`/`safeSubmission` ~L538-575).
  - Unsent DNS, connection and deadline failures become Rejected or RequestNotSent.
  - These become Unknown: a write that had started, a truncated body after the status line, 408, 5xx, an unreadable 2xx, a non-accepted status that carries a location, and an invalid Operation-Location.
  - No path automatically repeats a POST.
- [x] **Shared-failure detection.** `FailureClassifier` (L481-512) and `checkSharedFailure` (L514-527) throw only when there is no accepted handle and the status is not Unknown.
  - An auth failure counts only when a scalar credential fingerprint was sent and the request has no row-auth marker.
  - A 404 or AnalyzerNotFound counts only when analyzerId and apiVersion are scalar. Model codes also require scalar modelDeployments.
  - `execute` (~L641-667) passes `acceptedHandle` correctly, and poll mode never stops the partition.
- [x] **Partition stop.** `rowHandler` (L669-688) creates its `stopped` flag inside the handler closure, and Spark deserializes that closure separately for each task (`HTTPTransformer` mapPartitions plus the `SharedVariable` holder). Behaviour after a stop:
  - The triggering row keeps its status, HTTP status and `rawResponse`. `ContentUnderstandingRecoverySuite` L382-386 asserts the canary text stays in `rawResponse`.
  - Later unsent rows return `Rejected` with HTTP status 0.
  - The retry-selection SQL matches the docs (suite L566-576; docs L282-294).
- [x] **Row-auth marker.** `prepareRequest` (~L313-329) adds the marker only when `rowUsesColumnAuth` (L147-157) is true.
  - The base `addHeaders` strips user-supplied marker headers, and `HTTPRequestData.toHTTPCore` strips the marker before sending.
  - `usesFabricAuth` requires the value "true" plus an MWC token, so the CU marker cannot trigger the Fabric retry path.
- [x] **Shared base class (diff).** File: `CognitiveServiceBase.scala`.
  - The new `getCustomHeaders`, `setCustomHeadersCol` and `getCustomHeadersCol` (L226-230) are correct.
  - `addContentType` (L358-361) only removes case-variant duplicate Content-Type headers, the same result exact-case keys already got.
  - `ServiceAuthHeaders.resolve` sends exactly one auth header and follows the precedence that `rowUsesColumnAuth` mirrors.
- [x] **Transport and DNS.** Each operation gets its own client with no retries, redirects or cookies.
  - HTTP timeouts are capped by the remaining budget, and the deadline abort executor is best-effort.
  - `BudgetedDnsResolver` releases a permit only when the lookup physically completes. Its diagnostics are DnsCapacityExceeded, DnsResolutionFailed and OperationTimedOut.
  - All of this matches the human concurrency and timeout choices.
- [x] **Polling and Operation-Location.** Polling keeps the handle on transient errors and when the budget or attempt limit runs out.
  - Operation-Location must match the endpoint's origin and expected path and carry one api-version, so credentials cannot be forwarded to another host.
  - Plain HTTP is allowed only for literal loopback hosts.
  - Management calls are driver-only, need scalar configuration, and send PUT once.
- [x] **Shared Python mixin.** File: `core\src\main\python\synapse\ml\core\schema\Utils.py`.
  - `getOrDefault` (L67-79) returns bytes directly, with no pickle loading, and raises KeyError when the param is column-bound.
  - `extractParamMap` (L81-97) leaves out column bindings.
  - `explainParam` redacts the scalar credential params.
  - `_transfer_params_to_java` (L160-190) checks `param in self._paramMap`, so the generated `isSet` override cannot cause a KeyError there.
- [x] **CU Python overrides.** In `ContentUnderstandingParams.scala` (~L198-262), `_transfer_params_from_java` and `_transfer_params_to_java` pop ServiceParams and skip `None`. Column names therefore never become scalar values or `None` placeholders.
- [x] **Docs.** `docs\Explore Algorithms\AI Services\Content Understanding.md` discloses credential persistence and the limits of redaction (L96-123), including SAS tokens in scalar URLs. Recovery semantics are at L196-216 and explicit management at L345-372; both match the spec.
- [x] **Live E2E test.** `test_ContentUnderstandingE2E.py` (L34-39) reads credentials only from the environment and skips when they are absent. No secrets are embedded.
- [ ] **Tests not executed.** This was a read-only review. The supplied results (85 Scala CU tests, 16 Python tests, codegen, Black, scalastyle) were not re-run here.
- [ ] **No external Azure CI or live CU evidence.** Per the plan this is not a code defect, but it prevents any merge-ready claim.
- [ ] **Remote base not verified.** The branch is behind the cached origin/master, and gh authentication is unavailable.

## Issues

### Issue 1: Python `hasDefault`/`isDefined` ignore ServiceParam defaults that exist only on the JVM, while `getOrDefault`/`extractParamMap` return them
- **Severity**: Low
- **File**: `core\src\main\python\synapse\ml\core\schema\Utils.py`; `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`; `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala`; `ContentUnderstanding.scala`
- **Line(s)**:
  - Utils.py L61-97 and L184
  - Wrappable.scala L83-89 and L155-163
  - ContentUnderstandingParams.scala L127-132 and L167-174
  - ContentUnderstanding.scala L49
- **Description**:
  - The `analyzerId` default (`"prebuilt-read"`) and the `apiVersion` default are set only on the JVM.
  - Codegen emits no Python `_setDefault` for ServiceParams, and the CU override mirrors only outputCol and errorCol.
  - The new mixin `getOrDefault` and `extractParamMap` consult JVM `isDefined`, so they return those defaults. `hasDefault` is not overridden, so it returns False.
  - PySpark's `isDefined` is `hasDefault(p) or self.isSet(p)`. It reaches the generated `isSet` override, which asks JVM `isSet`, and that is False for a default.
  - On a fresh stage, `isDefined(analyzerId)` and `hasDefault(analyzerId)` are both False. Yet `getOrDefault(analyzerId) == "prebuilt-read"`, `getAnalyzerId()` returns it, and `analyzerId in extractParamMap()` is True. The same holds for `apiVersion`.
  - Values that were set explicitly are consistent, because `isDefined` reaches the `isSet` override.
  - Not tested: the Python params tests (test_ContentUnderstanding.py ~L246-303) check isSet, getters and extractParamMap, but not hasDefault or isDefined.
- **Risk**:
  - This breaks the PySpark Params contract (`isDefined` means "set or has a default"), which the plan asks to preserve.
  - Generic code that checks `isDefined` first (param dumps, logging or tuning helpers, user code) silently skips the effective analyzer and API version, while `extractParamMap()` includes them.
  - There is no credential exposure, because no credential param has a default.
- **Suggested Fix**:
  - In `ComplexParamsMixin`, override `hasDefault` for ServiceParams so it also returns True when the JVM has a scalar default (`self._java_obj.hasDefault(java_param)`).
  - Change `_transfer_params_to_java` (L184) to check `param in self._defaultParamMap` instead of `self.hasDefault(param)`. Otherwise it would raise KeyError or push raw values as JVM defaults.
  - Do not copy ServiceParam defaults into `_defaultParamMap`; `_make_java_param_pair` would send a raw value instead of an `Either`.
  - Add Python tests that `hasDefault`, `isDefined`, `getOrDefault` and `extractParamMap` agree in the default, scalar and column-bound states. Document that `getOrDefault` deliberately raises KeyError for column-bound params.

### Issue 2: Map-valued ServiceParams come back to Python as raw py4j Scala-Map objects
- **Severity**: Low
- **File**: `core\src\main\python\synapse\ml\core\schema\Utils.py`; `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`; `ContentUnderstandingParams.scala`; `CognitiveServiceBase.scala`; `cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py`
- **Line(s)**:
  - Utils.py L67-79 and L81-97
  - Wrappable.scala L183-200 (setter) and L290-295 (getter)
  - ContentUnderstandingParams.scala L70-73
  - CognitiveServiceBase.scala L226
  - test_ContentUnderstanding.py L134-160
- **Description**:
  - Generated setters convert a Python dict to a Scala Map with `ServiceParam.toMap`, but nothing converts it back.
  - These all return a py4j `JavaObject` wrapping `scala.collection.immutable.Map`:
    - `getModelDeployments()`
    - the new `getCustomHeaders()`
    - `getOrDefault(modelDeployments | customHeaders | telemHeaders)`
    - `extractParamMap()`
  - The cause is that `_java2py` passes through objects that are neither picklable nor JavaList.
  - So `stage.getModelDeployments() == {"completion": "my-deployment"}` is False, and `dict(...)`, `.items()` and `json.dumps(...)` do not work.
  - The existing test sets a dict but only checks the outgoing request body (L152-154). It never reads the value back in Python.
- **Risk**:
  - The Python API is asymmetric: users cannot inspect or compare map options without py4j internals.
  - `copy(extractParamMap())` works only because the setters accept the JavaObject unchanged.
- **Suggested Fix**:
  - In the mixin's scalar path, convert Scala Maps to Python dicts, for example with a JVM helper that returns a `java.util.LinkedHashMap` that is then wrapped in `dict(...)`.
  - Generate map-typed ServiceParam getters that use the same conversion, keeping the bytes passthrough.
  - Add tests that `getModelDeployments()`, `getCustomHeaders()`, `getOrDefault` and `extractParamMap` return dicts and that `copy`/save/load still round-trips.

### Issue 3: A failure of the scalar fallback credential also stops rows that send their own column credential
- **Severity**: Low
- **File**: `ContentUnderstanding.scala`; `ContentUnderstandingProtocol.scala`; `docs\Explore Algorithms\AI Services\Content Understanding.md`
- **Line(s)**:
  - ContentUnderstanding.scala L114-139, L147-157 and ~L313-329
  - ContentUnderstandingProtocol.scala L481-527 and L669-688
  - Docs L196-216
- **Description**:
  - Example setup: a column credential with a lower-priority scalar fallback, such as `setSubscriptionKeyCol("key")` plus `setAADToken(token)`.
  - A row whose key cell is null or blank falls back to the scalar token. It gets no row marker, and its fingerprint is in `scalarAuthFingerprints`.
  - A 401 or 403 on that row counts as shared and sets the single partition-wide `stopped` flag. A realistic trigger is the scalar AAD token expiring mid-job.
  - Every later row in the partition is then returned `Rejected` with HTTP status 0 and `SharedConfigurationFailure`, without being sent. That includes rows that would have sent their own column key (with the row marker) and never used the rejected credential.
  - The existing test (`ContentUnderstandingRecoverySuite` L448-452) covers the opposite setup: a scalar key over a token column, where every row sends the scalar key.
- **Risk**:
  - It fails safe: there are no duplicate submissions and handles are kept.
  - However, rows with valid credentials are skipped and must be resubmitted, which adds cost and latency for mixed-credential workloads.
  - This goes beyond the documented meaning of a shared credential failure. The docs say column-configured credentials produce row-level failures, but never say those rows are skipped because of a scalar failure.
- **Suggested Fix**:
  - Have `rowHandler` record why it stopped.
  - After an auth-only shared failure, skip only requests that would send the same scalar fingerprint without the row marker. Analyzer and model failures should still stop the whole partition.
  - Add a recovery test with two rows: a null key that falls back to the scalar credential and gets a 401, then a valid column key. Assert that the second row is sent.
  - Alternatively, document the partition-wide stop for mixed-credential setups explicitly.

### Issue 4: The unreachable `handlingFunc` override would lose the partition stop if it were ever used
- **Severity**: Low
- **File**: `ContentUnderstanding.scala`; `CognitiveServiceBase.scala`
- **Line(s)**:
  - ContentUnderstanding.scala L334-335, L349-350, L352-395 and L59-64
  - CognitiveServiceBase.scala L723-724 and L730-761 (the only caller is at L754)
- **Description**:
  - The base declares `handlingFunc` abstract, and only the base `getInternalTransformer` calls it. CU overrides both `transform` and `getInternalTransformer`, so CU's `handlingFunc` is dead code.
  - If it were reached, it would build a new `rowHandler`, with a fresh `stopped` flag, for every request. Shared failures would then never stop the partition.
  - It would also call `settings()` for every request. That reruns driver validation on the executor using the executor's `SQLConf.get.resolver`, so configuration errors would fail the task instead of producing row diagnostics.
- **Risk**: No current code path reaches it. A future refactor of the base class or a subclass could silently lose the documented stop and handle-preservation behaviour without any test failing.
- **Suggested Fix**: Make the override throw `UnsupportedOperationException`, explaining that ContentUnderstanding uses the handler built once in `buildInternalTransformer`. Alternatively, route it to that single snapshot-built handler.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver triage
- **How verified**: pending

## Driver resolution and evidence - attempt 13

Original findings and verdict are retained. The shared effort decision,
transport audit, red/green logs and artifact fingerprints are recorded in this
attempt's GPT artifact. Independent clearance is pending another review.

### Issue 1 - addressed

The JVM-aware `hasDefault` override and Python-default-map transfer guard are
implemented in `ComplexParamsMixin`. Fresh/copy/load/override/clear cases pass
for CU and AnalyzeDocument. Loaded `isSet` follows actual JVM metadata state;
the existing serializer saves effective defaults as explicit values too, so
the first test's unset-after-load expectation was corrected, not forced into
production serialization. Column bindings remain JVM-backed and scalar
`getOrDefault` intentionally raises KeyError, now documented.

### Issue 2 - addressed

Shared scalar conversion recognizes Scala Maps and converts entries to Python
dictionaries. Generated ServiceParam getters use that conversion; raw
bytes/bytearray still bypass PySpark's pickle-oriented `_java2py` byte path.
Empty and nonempty model/custom-header maps now round-trip through dedicated
getters, generic access, extracted parameter maps, copy and persistence.
Telemetry maps are checked through generic access: their pre-existing
generated dedicated getter has no matching JVM method and was not repaired
as an unrelated API addition.

The new conversion regression was red before regeneration and green afterward.
All 18 Python tests, including media/pickle-shaped-byte and column-binding
regressions, passed against fingerprint-verified rebuilt JARs.

### Issue 3 - documented conservative partition policy, no behavior change

The new two-row regression confirms the observation: a null column key selects
the scalar token; its 401 stops the partition, and the later column-key row is
unsent with HTTP 0 and SharedConfigurationFailure. It passed before fixes.
The accepted recovery policy stops new submissions in the partition on a
definitive shared failure; it is not a per-credential circuit registry.

Following the report's documentation alternative, the docs now explicitly
distinguish row-specific failure classification from the scope of an already
tripped shared stop, including mixed column/scalar fallbacks. No narrower
credential-specific circuit, concurrency change or operation cap was added.
Accepted/in-flight handle preservation remains covered by the existing suite.

### Issue 4 - hardening applied; no current normal-path defect claimed

The unused abstract-base hook now throws UnsupportedOperationException with
an explanation that CU uses its snapshot-bound handler. It cannot silently
create a new partition circuit for each call. A reflection regression checks
the explicit guard; before the change the intentionally unusable arguments
produced NullPointerException instead. This test proves the guard, not a
failure of the normal transform path or a reproduced future refactor.

All 89 CU tests, the generated-wrapper check, style/compilation, codegen/local
publication and final 18 Python tests passed. Original review outcomes remain
`issues_found`; Max stage 1 has not advanced.