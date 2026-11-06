---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 10
round: 1
timestamp: "2026-11-04T14:44:31Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "ae74318612b265c0ef8fe9ef6ee94049a9cd4654092ad480b92f13e58c7ea68d"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: direct
attempt: 1
stage_attempt: 10
round: 1
timestamp: 2026-11-04T14:35:44Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: 11be39c1b114db87cdd16eb3f0f6b3c3bd618b81fc914cb88b47c737970d5b64
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-10-review-1-grok-4.7.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed frozen diff `cu-stage1-attempt10.patch` (SHA256 `11be39c1b114db87cdd16eb3f0f6b3c3bd618b81fc914cb88b47c737970d5b64`) for analyze/submit/poll, handle retention, auth classification, and persistence.
- [x] Traced Python transfer against `core/src/main/python/synapse/ml/core/schema/Utils.py` and generated setters in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`.
- [x] Inspected deadline/DNS and shared-failure logic in `ContentUnderstandingProtocol.scala` and `ContentUnderstandingDnsResolver.scala`.
- [x] Confirmed operation-location checks reject off-origin and non-result paths; Fabric auth marker is stripped in `HTTPSchema.scala`.
- [ ] Python offline runtime checks and live CU/Azure pipeline runs are not passing evidence; missing external CI is not treated as a code defect.

## Issues

### Issue 1: Generated ServiceParam setters break Python transfer
- **Severity**: High
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 84-90, 206-212
- **Description**: Generated setters pop the Python ParamMap after writing the JVM, and the new `isSet` reports JVM `isSet` for every ServiceParam. `JavaMMLParams._transfer_params_to_java` then does `self._paramMap[param]` whenever `isSet` is true. After `setSubscriptionKey` (or any generated ServiceParam setter), transfer raises KeyError. This runs on Python `transform` for Content Understanding and every other wrappable stage. CU's override only drains keys still present in the map, then calls `super()`.
- **Risk**: Python pipelines fail after ordinary setters, or skip transferring map-only values. Scala-only tests would not catch this. Python runtime checks are not yet passing evidence.
- **Suggested Fix**: On setter, `_set` the same value into the ParamMap instead of popping it. Make `isSet` true if either the JVM or the ParamMap has the param, and make transfer read the map only when the key exists.

### Issue 2: Timed-out DNS lookups occupy the shared pool
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala
- **Line(s)**: 38-64
- **Description**: Lookups run on a 32-thread `SynchronousQueue` pool. On budget timeout the future is cancelled, but JVM DNS usually ignores interruption, so the worker stays busy until resolution finishes. Further calls get `RejectedExecutionException`, mapped to `DnsResolutionFailed`.
- **Risk**: A few slow lookups on an executor turn later pre-send resolutions into false rejections, or stall CU traffic, without a request having been sent.
- **Suggested Fix**: Use an interruptible resolver or a dedicated per-call timeout that abandons and replaces stuck workers. Do not treat pool saturation as a DNS failure of the target host; surface a distinct retryable saturation error and bound queue growth.

### Issue 3: Any HTTP 404 is a shared analyzer failure
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 530-541
- **Description**: With a scalar analyzer and API version, `checkSharedFailure` treats every HTTP 404 as `AnalyzerNotFound`, not only that error code. The exception escapes the row handler and fails the Spark action.
- **Risk**: A document-level or gateway 404 aborts the partition. Task retry can resubmit sibling rows whose operations were already accepted, violating conservative no-duplicate-submission behavior.
- **Suggested Fix**: Fail the action only for a recognized analyzer-not-found code when the analyzer id is scalar. Keep other 404 responses as row results and retain any accepted handle.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Implemented; runtime revalidation pending
- **What changed**: Transfer only pending Python-map entries; preserve JVM service values, include Python state in `isSet`, and clear both representations. Added existing-wrapper runtime regressions.
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Implemented; runtime revalidation pending
- **What changed**: Budget-aware semaphore admission, bounded executor queue, and distinct `DnsCapacityExceeded` diagnostics. Permits remain occupied until the physical lookup returns.
- **Why**: Waiting avoids immediate rejection on transient saturation. Replacing uninterruptible workers was rejected because it would undo the worker bound.
- **How verified**: pending

### Issue 3
- **Status**: Implemented alternative; runtime revalidation pending
- **What changed**: Shared submission failures no longer escape the row handler. A per-task circuit returns row diagnostics, retains accepted sibling handles, and skips unsent rows.
- **Why**: The user explicitly selected preserve_handles rather than fail-fast. Generic scalar-analyzer pre-acceptance 404 still stops further submissions conservatively, but no longer fails the action or induces task retries. Poll responses and accepted handles remain unaffected.
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
- Issue 1: offline Python tests pass 13/13 (`cu-attempt10-python-fixed4.log`), including existing non-CU `AnalyzeDocument` transfer, pending values, copy overrides, save/load and clear. The earlier `TextSentiment` persistence fixture had an unrelated protected legacy UDF; changing the fixture preserved the ServiceParam assertions without enabling unsafe deserialization. Loaded JARs were fingerprint-verified and driver/worker versions aligned.
- Issue 2: `cu-attempt10-dns-recovery.log` passes 3/3. The bounded-worker regression now proves callers wait while saturated, report distinct capacity exhaustion, and successfully proceed after physical lookups release capacity. The bound is not bypassed by replacing blocked threads.
- Issue 3: recovery tests passed in validation4; the corrected partition/action-isolation test passed in validation5 and the final concurrency rerun. Shared errors remain row results under the user's preserve_handles decision. Generic scalar-analyzer pre-acceptance 404 still stops new submissions conservatively; the implementation does not claim that every such response proves an analyzer is missing.
- Scala style/compilation, regenerated wrappers, local publication and Black passed. Live CU and external CI remain outstanding. Original findings and verdict are retained.
