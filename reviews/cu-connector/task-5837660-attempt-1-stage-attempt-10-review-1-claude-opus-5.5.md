---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 10
round: 1
timestamp: "2026-11-04T14:45:59Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ce687e3c6f5114cf54a0a8c8a7c4c96593c279d64ccedd608b7dda07c2543619"
---

I found 3 issues: one Critical, one Medium, one Low. One Critical issue breaks all generated Python wrappers, including CU's own. Nothing was executed.

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: null  # not known to the reviewer; supplied by the script-owned runner
attempt: 1
stage_attempt: 10
round: 1
timestamp: 2026-11-04T14:35:44Z
outcome: issues_found
reviewer_model: claude-opus-5.5
input_sha256: 11be39c1b114db87cdd16eb3f0f6b3c3bd618b81fc914cb88b47c737970d5b64
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-10-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `cognitive\src\main\scala\...\contentunderstanding\ContentUnderstanding.scala` (lines 1–480): `transform`, `settings`, `rowUsesColumnAuth`, validation, `prepareRequest`, `buildInternalTransformer`, management calls and `ContentUnderstandingOutputParser`.
- [x] Read `ContentUnderstandingProtocol.scala`:
  - `exchange` and `exchangeBeforeDeadline` (374–433).
  - `withClient` (435–445).
  - `FailureClassifier` and `checkSharedFailure` (497–543).
  - Submission decoding and safety (545–584).
  - `pollWithClient` (606–648).
  - `execute` and `rowHandler` (650–678).
- [x] Read `ContentUnderstandingDnsResolver.scala` (1–72) and `ContentUnderstandingParams.scala`, including the Python overrides (155–268).
- [x] Traced Python codegen in `core\src\main\scala\...\codegen\Wrappable.scala`:
  - `isSet` override (83–89).
  - ServiceParam setters (182–213).
  - `__init__`, which calls the setters (474–482).
  - `setParams`, which uses `_set` (487–501).
  - Class template, where `pyAdditionalMethods` goes into the class body (505–556).
  - Cross-checked against `core\src\main\python\synapse\ml\core\schema\Utils.py`, `ComplexParamsMixin._transfer_params_to_java` (95–123), and `java_params_patch.py` (no override).
- [x] Checked how exceptions propagate. `core\...\io\http\HTTPTransformer.scala` (`mapPartitions` and the client holder), `Clients.scala` (`AsyncClient`, 51–62) and `core\...\utils\AsyncUtils.scala` (`bufferedAwait` rethrows, 11–19) show that an exception thrown in the row handler fails the Spark task.
- [x] Refuted a suspected leak of the internal auth marker header. `HTTPSchema.scala` 188–221 (`toHTTPCore`) filters `isFabricAuthMarker` headers before they reach the wire, and CU's exchange uses `toHTTPCore` (Protocol 391).
- [x] Reviewed `CognitiveServiceBase.scala`:
  - New `getCustomHeaders`/`setCustomHeadersCol`/`getCustomHeadersCol` (226–230).
  - `addContentType` deduplication.
  - `addHeaders` (549–563).
  - `resolveServiceAuthHeaders` (577–590).
  - Conclusion: the Content-Type deduplication is consistent with the earlier exact-case overwrite, so it is not a regression.
- [x] Reviewed `DatasetExtensions.scala` 51–76. The default `findUnusedColumnName` is now resolver-aware, which is safe because it only avoids more collisions.
- [x] Checked the requirements:
  - No RDD APIs in the CU sources; the HTTP stage uses the existing Dataset `mapPartitions`.
  - No Spark session conf mutation in production code. `resolverFor` only reads `spark.sql.caseSensitive`.
  - Scalar `concurrentTimeout` is rejected only when set explicitly.
  - Poll-time shared failures are non-fatal (`acceptedHandle = true`).
- [x] Compared `docs\Explore Algorithms\AI Services\Content Understanding.md` 162–280 (failure semantics, submit/poll, `Rejected`, bounds) with the implementation.
- [x] Checked test evidence. `ContentUnderstandingSuite.scala` 636–655 only asserts that the generated Python *text* contains `def isSet(self, param):`. `test_ContentUnderstanding.py` 104–116 and 170–258 exercise `copy`, `transform`, `write` and `_transfer_params_to_java` at runtime, but per the supplied evidence those Python tests have **not** passed.
- [ ] No tests, builds or codegen were executed. This review is read-only, per the plan.
- [ ] External Azure pipeline CI is still outstanding, and the Python offline runtime checks are not passing evidence. This review does not certify merge readiness.
- [ ] Whether the removed CU persistence APIs (`writeToTable`, `readTable`, etc.) ever shipped could not be checked against the base branch without git. No versioned website docs reference CU (only `website\sidebars.js`), so this is not raised as a finding.

## Issues

### Issue 1: The repo-wide Python `isSet` override, combined with setters that pop `_paramMap`, breaks `_transfer_params_to_java` for every generated wrapper, including CU
- **Severity**: Critical
- **File**: `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`. The defect surfaces through `core\src\main\python\synapse\ml\core\schema\Utils.py` and `cognitive\src\main\scala\...\contentunderstanding\ContentUnderstandingParams.scala`.
- **Line(s)**:
  - Wrappable.scala 83–89 (`pyAdditionalMethods` `isSet`), 205–206 and 211–212 (`self._paramMap.pop(...)` in the ServiceParam setters), 480–482 (`__init__` routes kwargs through the setters), 500 (`setParams` uses `self._set`).
  - Utils.py 100–115.
  - ContentUnderstandingParams.scala 236–245.
- **Description**: `PythonWrappable.pyAdditionalMethods` now generates this method into **every** wrapper class:
  ```python
  def isSet(self, param):
      param = self._resolveParam(param)
      if param.doc.startswith("ServiceParam:"):
          return self._java_obj.isSet(self._java_obj.getParam(param.name))
      return super().isSet(param)
  ```
  The generated `setX`/`setXCol` ServiceParam setters also now remove the param from `self._paramMap`. All generated wrappers inherit `ComplexParamsMixin._transfer_params_to_java` (Utils.py 95–115), which does this:
  ```python
  if self.isSet(param):
      ...
      if is_service_param:
          getattr(self._java_obj, "set...")(self._paramMap[param])   # KeyError
  ```
  1. **KeyError after any setter call.** A ServiceParam set through its setter or constructor kwarg (for example `TextSentiment(subscriptionKey=k)`, `.setSubscriptionKey(k)` or `.setDocumentBytesCol("body")`) is set on the JVM and absent from `_paramMap`. `isSet` then returns True, and `self._paramMap[param]` raises `KeyError`. `_transfer_params_to_java` runs on every `JavaTransformer._transform` (reached via `HasCustomCogServiceDomain._transform` → `super()._transform`), `JavaParams.copy`, `JavaMLWritable.write()` and `JavaParams._to_java`, which Pipeline save uses.
  2. **CU's own override does not avoid it.** CU's `_transfer_params_to_java` (ContentUnderstandingParams.scala 236–245) moves pending ServiceParam values through the setters and then calls `super()._transfer_params_to_java()`. That re-enters the same loop and raises `KeyError` for `subscriptionKey` and `documentBytes`, which the `analyzer()` fixture sets at test lines 104–116.
  3. **Silent drop on the `setParams` path.** For non-CU wrappers, `setParams(subscriptionKey=k)` and `copy({stage.subscriptionKey: k})` store the value only in Python `_paramMap`. The JVM-backed `isSet` returns False, so the value is **never transferred**. The request goes out without the credential, or with stale JVM values.

  The only Scala guard (`ContentUnderstandingSuite` line 642) checks that the generated source contains `def isSet(self, param):`. It does not execute the Python. Per the supplied evidence, the Python runtime checks have not passed.
- **Risk**: A repo-wide regression in the Python API.
  - Every generated cognitive-service wrapper, and any other wrapper with ServiceParams, can fail on `transform`, `copy`, `write`/`save` and inclusion in a saved `Pipeline`.
  - Python `setParams`/`copy(extra)` may silently drop ServiceParam values, leading to unauthenticated or misconfigured requests.
  - It breaks the "Python generated wrappers" requirement for CU itself.
- **Suggested Fix**:
  - Do not drive the transfer loop from a JVM-backed `isSet`. In `ComplexParamsMixin._transfer_params_to_java`, iterate over `param in self._paramMap` (Python-side pending values) instead of `self.isSet(param)`, and push pending ServiceParam values through the generated setters, as CU's override does.
  - Alternatively, scope the `isSet` override to CU and make CU's `_transfer_params_to_java` not delegate to the mixin for ServiceParams.
  - Add Python runtime regression tests for:
    - A non-CU service (for example `TextSentiment().setSubscriptionKey("k")`): `copy()`, `write().save()`, `_transfer_params_to_java()`, and `setParams(subscriptionKey=...)` reaching the JVM.
    - CU: `copy`, `transform` and `save`/`load`.
  - Run these tests before advancing.

### Issue 2: Shared pre-acceptance failures throw from the executor row handler, so Spark task retries discard sibling handles and re-POST already-accepted rows
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**:
  - Protocol 530–543 (`checkSharedFailure`), 657–660 (submission call site), 677–678 (`rowHandler`).
  - Related: `core\...\utils\AsyncUtils.scala` 11–19, `core\...\io\http\Clients.scala` 56–62, and `docs\Explore Algorithms\AI Services\Content Understanding.md` 191–203 and 256–258.
- **Description**: `checkSharedFailure` throws `ContentUnderstandingException` inside the per-row handler when any of these occur before acceptance:
  - A 401/403 is returned after a scalar credential was sent.
  - With a scalar analyzer, the error code is `AnalyzerNotFound`, **or the response is any HTTP 404**.
  - With scalar deployments, the error code is `DefaultsNotSet`, `DeploymentNotFound` or `ModelDeploymentNotFound`.

  The handler runs inside `HTTPTransformer`'s `mapPartitions`. `AsyncUtils.bufferedAwait` rethrows the failure, so the **whole Spark task** fails. This has three consequences:
  1. Operation handles already accepted for earlier rows of the same partition are discarded with the failed task's output. Up to `concurrency - 1` sibling futures may still be mid-POST, and their results are dropped too.
  2. Spark retries the task (`spark.task.maxFailures`, default 4 on clusters) and re-POSTs every earlier row in the partition. If the shared failure was transient, those rows become **duplicate billable submissions** and the job may then succeed with duplicates. Transient triggers include:
     - RBAC or role propagation delay causing a 403.
     - A key or AAD token expiring mid-job and being refreshed.
     - Analyzer propagation delay right after `createAnalyzer`, or a gateway 404.
  3. If the failure is persistent (for example, a scalar AAD token expiring partway through a long job), accepted work in the failing partition still loses its handles. The usual remedy, re-running the job, resubmits all of those rows.

  The docs say accepted handles are retained "so Spark task retries do not discard accepted work". That holds only for the failing row, not for sibling rows that the connector-initiated task failure discards. The bare `httpStatus == 404` rule also widens the trigger beyond the "recognized" codes the docs describe.
- **Risk**: This conflicts with the requirement for conservative failure semantics that never submit a document twice. It can cause duplicate billing and permanently lost handles in realistic long-running jobs or right after provisioning, with no way to reconcile them afterwards.
- **Suggested Fix**:
  - Do not throw from the row handler after any row in the task may have been accepted. Instead, use a per-partition (per-handler-instance) circuit breaker: once a shared pre-acceptance failure is seen, mark that row and all **not-yet-sent** rows as `Rejected` with a distinct code such as `SharedConfigurationFailure`, without sending, and return the accepted handles normally. If fail-fast semantics are wanted, raise on the driver after the results are materialized, or offer an opt-in.
  - Optionally add an explicit driver-side preflight (a scalar-credential GET of the analyzer) before dispatch, to catch shared misconfiguration before any POST.
  - Narrow the analyzer rule to recognized error codes rather than any 404.
  - Update the docs so they accurately describe what happens to sibling rows.
  - Add a test in which row 1 is accepted, row 2 gets a shared 401/404, and the result keeps row 1's handle with only one POST for row 1 across task attempts.

### Issue 3: The JVM-wide 32-thread DNS pool rejects lookups under load and reports them as `DnsResolutionFailed`/`Rejected`
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala`
- **Line(s)**: 27–35, 44–49, 60–72. Related: `ContentUnderstandingProtocol.scala` 435–445 (per-operation `HttpClient`) and 545–549 (`dnsFailedBeforeSend` → `Rejected`).
- **Description**:
  - **Shared pool.** `BudgetedDnsResolver.Executor` is a single `ThreadPoolExecutor(0, 32, SynchronousQueue)` shared by every task and every CU operation in the executor JVM. `withClient` builds a new `HttpClient` per row or operation, so every operation performs its own resolver submission.
  - **Saturation produces false failures.** When more than 32 lookups are in flight (for example, many cores × `concurrency`, or a slow resolver), `submit` throws `RejectedExecutionException`. That surfaces as `DnsResolutionFailed` ("DNS resolution failed before the HTTP request was sent."), and the row becomes `Rejected` even though DNS never failed.
  - **Pinned threads.** On timeout, `resolution.cancel(true)` cannot interrupt `InetAddress` resolution, so stuck lookups keep pool threads busy. A short DNS stall can therefore cascade into rejecting every row in the JVM.
  - **Per-row clients.** Building a client per operation also gives up connection reuse, adding a TCP/TLS handshake per row.
- **Risk**: Under realistic executor parallelism, users see spurious `Rejected` rows with a misleading DNS diagnostic, and have to resubmit them. Nothing is sent twice, but reliability and diagnosability suffer.
- **Suggested Fix**:
  - Wait for a slot within the operation budget instead of rejecting immediately, for example with a `Semaphore.tryAcquire(remaining, MILLISECONDS)` around a cached pool or a bounded queue.
  - Use a distinct diagnostic such as `DnsCapacityExceeded` when no slot becomes free.
  - Consider sizing the pool relative to the configured concurrency, and reusing one connection manager per partition with per-request deadlines.
  - Add a test that saturates the pool and asserts the new behavior.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Implemented; runtime revalidation pending
- **What changed**: Transfer only pending Python-map entries through generated setters, preserve JVM-backed state, honor pending values in `isSet`, and clear both representations. Added non-CU TextSentiment runtime regressions.
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Implemented; runtime revalidation pending
- **What changed**: Per-task circuit returns shared failures as row diagnostics and skips new submissions after detection; accepted and in-flight sibling results are retained. Updated documentation and live-test expectations; added sibling-handle and task/action-isolation regressions.
- **Why**: The user selected preserve_handles when explicitly asked to choose between this change and retaining action-failure semantics. No exactly-once guarantee is claimed.
- **How verified**: pending

### Issue 3
- **Status**: Implemented; runtime revalidation pending
- **What changed**: Wait for bounded DNS capacity within the operation budget, report `DnsCapacityExceeded`, and retain permits until lookups physically complete. Keep bounded worker and queue counts.
- **Why**: Prevent transient saturation from immediately rejecting new work without allowing abandoned native lookups to grow without bound. Connection pooling redesign is outside this focused fix.
- **How verified**: pending

## Driver Decision

Effective decision: Max, direct contract, explicitly selected by the user. Stage 1 (broad sweep), attempt 10. Primary driver: gpt-6-astra.
Change size: broad, 26 diff paths and 288650 bytes across Scala runtime/codegen, Scala/Python tests, docs and CI selection. Complexity: high; concurrent networking, deadlines, distributed Spark transforms, credentials, asynchronous polling and persistence interact.
Reviewers resolved against the current live task catalog: gpt-6.1-sol xhigh (latest GPT flagship slot); grok-4.7 low (explicit human reasoning override); claude-opus-5.5 xhigh (latest Opus). Three independent concurrent invocations, one shared frozen diff. Human allows unavailable Grok to be excluded only if the other two are clean; findings from available Grok still count.
Gate: do not advance a dirty Max stage. Retain all findings and evidence. No new cycle/time/credit limits are introduced and existing controls remain unchanged.
Snapshot: cu-stage1-attempt10.patch, SHA256 11be39c1b114db87cdd16eb3f0f6b3c3bd618b81fc914cb88b47c737970d5b64.
Scope: review the supplied changes and canonical supporting files only. Do not read prior/sibling feedback or reviews/cu-connector artifacts. Read-only: do not edit, execute shell commands, delegate, or make remote changes.
Requirements: SynapseML CU transformer must support analyze/submit/poll, binary and URL inputs, explicit driver-side analyzer management, SparkML persistence, Python generated wrappers, durable operation handles and conservative no-duplicate-submission failure semantics. Dataset processing must not mutate shared Spark session settings or use RDD implementations.
Validation already completed on these source changes: Scala style, compile and test compilation; CU recovery 34/34, concurrency 2/2, main suite 40/40, dataset extensions 8/8; codegen; Black (205 files unchanged). PipelineTestCoverageSuite passed 1/1 after local YAML approval.
Python offline runtime checks are being prepared independently using existing Python 3.11.8/PySpark 3.5.0 and locally published JVM artifacts. They are NOT yet passing evidence. No live CU tests were run.
Known external limitation: user now explicitly approved the existing two-line pipeline.yaml contentunderstanding UnitTests entry. Azure pipeline execution was previously declined and has not been queued. Local approval is resolved, external CI evidence is still outstanding; do not misreport full merge readiness or waive this gate. Review production code independently rather than treating missing external CI as proof of a code defect.
Target remains master per established session context (Spark 3.5.0/Scala 2.12.17). Fresh GitHub base verification is unavailable because gh authentication is absent; do not invent remote evidence.

## Follow-up validation (2026-11-04)

- All three resolutions above are locally verified; independent rereview remains pending.
- Issue 1: `cu-attempt10-python-fixed4.log` records 13/13 passing tests against fingerprint-verified fresh core/cognitive JARs, covering CU runtime behavior and non-CU `AnalyzeDocument` pending/JVM state, transfer, copy overrides, persistence and clear. The original `TextSentiment` fixture encountered unrelated legacy-UDF deserialization protection; it was replaced with a non-UDF stage without removing assertions or enabling unsafe deserialization.
- Issue 2: the recovery suite passed accepted-sibling retention and shared-failure cases in validation4. The misplaced isolation test was corrected; validation5 and `cu-attempt10-dns-recovery.log` both pass concurrency 3/3, including separate partitions and repeated actions. The change follows the user's explicit preserve_handles choice; it is not an exactly-once or durable-journal guarantee.
- Issue 3: the strengthened DNS regression proves bounded blocked workers, distinct saturation diagnostics, waiting admission and successful recovery after actual lookup completion. No extra unbounded workers or early permit release were introduced.
- Scala style/compilation, codegen, local publication and Black passed. The original `issues_found` verdict remains historical evidence; external CI/live CU validation and subsequent Max stages are not complete.
