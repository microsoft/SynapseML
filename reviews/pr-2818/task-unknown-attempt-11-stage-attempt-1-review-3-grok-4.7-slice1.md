---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 3
timestamp: "2026-11-09T06:15:01Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "bf75356729c4d53717ed56f5c8acb7ce56d613a73c455050af2658d74e981f05"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-3-grok-4.7-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full across the required ranges [1, 150], [151, 300], and [301, 437].
- [x] Inspected `core/src/main/scala/org/apache/spark/ml/Serializer.scala` session swap (`PipelineArraySerializer.withSession`, lines 278-292) and the no-arg/null constructors that reach it.
- [x] Compared that order with `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala` lines 15-20, which snapshots the active session before mutating it.
- [x] Traced nested save/load through `ComplexParamsSerializer.scala` and `ComplexParam.save`/`load`, which pass the caller session into `PipelineSerializer`/`PipelineArraySerializer`.
- [x] Checked restoration tests in `ValidateComplexParamSerializer.scala` (unassigned case, lines 219-227) and `VerifyUDFTransformerPersistence.scala`. They always install an active or default session before the fallback path. Caller-supplied compile/test evidence was not executed here.

## Issues

### Issue 1: Unassigned session fallback snapshots the active session after getOrCreate can replace it
- **Severity**: Medium
- **File**: core/src/main/scala/org/apache/spark/ml/Serializer.scala
- **Line(s)**: 278-284
- **Description**: `withSession` resolves `SparkSession.builder().getOrCreate()` before reading `SparkSession.getActiveSession`. The no-arg constructors and the documented null-session fallback (`Option(null)` becomes `None`) both take this path. On Spark 3.5, `getOrCreate` calls `setActiveSession` when it must create a session: no live active session and no live default session. It can also do so, then throw, from the creation path (for example while registering the context listener). The `finally` block then restores the session `getOrCreate` just installed, or never runs if that call throws before `previous` is captured. Assigned-session calls are unaffected because they do not call `getOrCreate`. The new unassigned tests always set either the active or the default session first, so they do not cover this path.
- **Risk**: A caller that had no active session, or only a stopped one, is left bound to a session they did not select after success or after a creation failure. Later unassigned pipeline reads and writes on that thread use that session's SQL config, Hadoop config, and legacy-deserialization policy. That is the isolation failure this change is meant to prevent, including a stopped caller session being discarded when no live default exists.
- **Suggested Fix**: Snapshot `previous` before any `getOrCreate` call, and restore it if session resolution throws. Keep the created default session; only the thread-local active session must be restored.

```scala
private def withSession[T](action: SparkSession => T): T = {
  val previous = SparkSession.getActiveSession
  def restore(): Unit =
    previous.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
  val session = spark.getOrElse {
    try SparkSession.builder().getOrCreate()
    catch { case error: Throwable => restore(); throw error }
  }
  SparkSession.setActiveSession(session)
  try action(session)
  finally restore()
}
```

Add a test that clears both the active and default sessions, runs a null-session save and a failing save, and asserts the active session is empty afterward.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No product code. Preserve the existing implicit-session initialization behavior of the public no-argument and null-session helpers.
- **Why**: This is not a regression in the assigned-session fix. At the target revision, `PipelineSerializer` and `PipelineArraySerializer` delegate to `Pipeline.write.save` and `Pipeline.load`. Spark 3.5's `BaseReadWrite.sparkSession` calls `SparkSession.builder().getOrCreate()` when no session was supplied, and that builder installs the created active/default sessions when both are absent. The target does not restore an empty active session after that initialization. The changed assigned-session branch never calls the builder. Capturing/restoring before implicit initialization would change legacy behavior rather than repair the assigned-session isolation contract.
- **How verified**: A standalone Spark 3.5/JDK 11 probe compared the target's exact Pipeline delegation with the actual compiled current single/array serializers, through both no-argument and null-session constructors. All 18 cases matched: successful save, existing-path failing save, and successful read with both sessions initially cleared retained the implicitly created active/default session. Source provenance was checked against the assigned-session classes. Spark's tagged `ReadWrite.scala` and `SparkSession.scala` also confirm the call order. The suggested context-listener-registration failure was not fault-injected; that same builder call and its mutation/throw behavior already exist in the target fallback.