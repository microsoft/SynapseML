---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 3
timestamp: "2026-11-09T03:33:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "81634232942d42b86f71072bf68145b2ea61625f8064d72d2f74bf7107712a20"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-10-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 393 lines of the assigned frozen diff in the required order: [1,150], [151,300], [301,393]. The current source matches it in these places:
  - `core/src/main/scala/org/apache/spark/ml/Serializer.scala` lines 59–60, 71 and 258–296
  - `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` lines 66 and 174
  - all three changed test files
- [x] **Failure handling in `withSession`** (`Serializer.scala` lines 278–285):
  - The session is chosen (line 279) and the previous active session is saved (line 280) before anything changes. If `getOrCreate()` fails, thread state is untouched.
  - The `try/finally` restores the previous session after success or any exception.
  - The restore calls only set or remove a thread-local value, so they cannot hide the original exception.
  - The previous session is saved after `getOrCreate()`. This keeps Spark's behavior when that call creates and activates a new session.
  - If there was no previous session, it calls `clearActiveSession()`. The default session is never made active by mistake. This matches Spark's own `SparkSession.withActive`.
- [x] **Concurrency:**
  - Neither serializer has shared mutable fields. Each holds one fixed `Option[SparkSession]` and swaps a thread-local value. Threads that persist models at the same time with different sessions cannot affect each other.
  - Nested use, such as a pipeline param inside a child stage, restores sessions in the reverse order they were set.
  - The `Test / parallelExecution := false` setting in `build.sbt` keeps the new default-session assertions predictable.
- [x] **Null and boundary inputs:**
  - `this(spark: SparkSession)` turns null into `None` (lines 262 and 276). Before this change, `ComplexParam.save/load(..., null, ...)` worked because the serializers ignored the session; that still works.
  - With `None`, the session is looked up on each call. The constructors never call `getOrCreate()` early.
  - An empty pipeline still fails on `read(path)(0)` as before.
  - A stage that cannot be written is still rejected as before. The rejection happens inside `withSession`, so the active session is restored.
- [x] **Compatibility:**
  - The public no-argument constructors and the `write`/`read` signatures are unchanged.
  - The `Option` primary constructors are private, so callers always reach the `SparkSession` constructor and calls are not ambiguous.
  - The saved format is unchanged: the same `Pipeline` writer and reader are used, with only `.session(...)` added (lines 288 and 294).
  - `if (overwrite) writer.overwrite()` (line 289) behaves like the removed `writeMLWritable` path, because `MLWriter.overwrite()` changes the writer in place and returns it.
- [x] **The tests would fail without the fix.**
  - `ValidateComplexParamSerializer.scala` lines 156–189 cover nested writes that succeed and fail, with and without an active session. After an error thrown inside the nested child, they check that the active and default sessions are restored (lines 180–186).
  - `VerifyUDFTransformerPersistence.scala` lines 46–84 cover nested reads that succeed or throw `SecurityException`. They use an active session that differs from the default.
  - Code without the fix would fail the `allowed=false` cases. `TestBase.sparkConfiguration` turns on the legacy flag for the default session, and the `active` session also sets it to `true`, so the old code loads where the test expects an error.
  - Lines 191–214 cover the null (unassigned) session for both a single transformer param and an array of stages.
- [x] **Test fixture move:**
  - `TestEstimatorBase` and `TestableSessionParamTest` keep their fully qualified names in package `com.microsoft.azure.synapse.ml.core.serialize` (`ComplexParamTestFixtures.scala` lines 23–51).
  - `withActiveSession` (lines 15–20) restores in `finally`, so a failed assertion cannot leave session state for later tests.
  - Test names and temp paths are unique per variant, so names don't clash and runs don't overwrite each other's files.
- [x] **Related files**, opened by exact path:
  - `ComplexParam.scala`: save/load go through `typeToSerializer`.
  - `TransformerParam.scala` and `PipelineStageParam.scala`: no overrides, so they use `PipelineSerializer`.
  - `EstimatorParam.scala`: estimators also use `PipelineSerializer`.
  - `TransformerArrayParam.scala` and `EstimatorArrayParam.scala`: Scala arrays are invariant, so these still go to `ObjectSerializer`. This is existing behavior and the diff does not touch it.
  - `ComplexParamsTestHelper.scala`: still exercises the older overloads that take no session.
- [ ] **Tests not run by this reviewer** (read-only contract). I relied on the caller's local evidence:
  - compile, Test/compile and production/test scalastyle passed on JDK 11, Spark 3.5.0 and Scala 2.12.17
  - 29 tests passed with 0 skipped, which matches the 18 + 11 tests defined in the two suites
  - remote CI remains a separate gate
- [ ] **Spark 3.5.0 source not opened.** It is outside the scope roots. I relied on these documented upstream behaviors instead:
  - `Pipeline.SharedReadWrite` creates child writers and readers without a session.
  - `BaseReadWrite.session(null)` falls back to the active or default session.
  - `Builder.getOrCreate()` returns the active session first, then the default.
  - `MLWriter.overwrite()` changes the writer in place.