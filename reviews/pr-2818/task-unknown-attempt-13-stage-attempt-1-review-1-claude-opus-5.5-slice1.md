---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 1
timestamp: "2026-11-10T03:15:26Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "1753f7dad4d528cf32929cb0569aeb4a1fa6e835f8d3b404c5abd09b516e03e8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-13-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read all 455 lines of the assigned frozen diff, in the required order: [1,150], [151,300], [301,450] and [451,455]. All five inventory files in the current tree match the post-change side of the diff. I used exact-path `view` calls only. I did not search, and I did not open any review, checkpoint or session directory.
- [x] **Session routing.** In `core/src/main/scala/org/apache/spark/ml/Serializer.scala` L59-60, `typeToSerializer` now passes the assigned session to `PipelineSerializer` and `PipelineArraySerializer`. `DFSerializer` and `ObjectSerializer` already received it. The params `TransformerParam`, `EstimatorParam` and `PipelineStageParam` use `ComplexParam.save`/`load` from `core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala` without overriding them. Every param typed as a pipeline stage therefore receives the assigned session.
- [x] **Active-session restore.** `withSession` (Serializer.scala L278-285) makes the resolved session active. This is needed because Spark 3.5's `Pipeline` creates child readers and writers without a session, so they look one up with `getOrCreate()`. A `finally` block restores the previous active session, or clears it if there was none, whether the action succeeds or throws. Nested calls restore in the right order, and the default session is never changed. The pattern matches Spark's own internal `withActive`.
- [x] **Behavior and on-disk format unchanged.** Writing with `.write.session(s)`, an optional `overwrite()` and then `save` does the same thing as the removed `writeMLWritable` path. Reading with `Pipeline.read.session(s).load(..).getStages` does the same as `Pipeline.load(..).getStages`. `PipelineSerializer` delegates through `Array(stage)` and `(0)`, so the pipeline format on disk is identical. An empty pipeline still fails with the same index error.
- [x] **Compatibility.** Both public no-argument constructors are kept through `def this()`. The new public constructor that takes a `SparkSession` is an addition. The `Option` primary constructor is private, so external code calling `new PipelineSerializer(null)` resolves without ambiguity. The `read`/`write` signatures, `writeMLWritable`, and the 3-argument `saveComplexParams`/`getAndSetComplexParams` overloads are unchanged; the overloads only gained accurate doc comments. `core/src/test/scala/org/apache/spark/ml/ComplexParamsTestHelper.scala` still exercises those overloads.
- [x] **Null session.** `Option(spark)` falls back to the active or default session each time a read or write runs. This keeps working for callers that already passed null into these params, because the old pipeline serializers ignored the session. The four "pipeline params with an unassigned session" tests cover this case.
- [x] **One edge case checked, not a defect.** If there is no active session, no default session and no assigned session, `getOrCreate()` runs before `previous` is captured. It creates a new session and makes it active, and that session stays active afterwards. The old code path did exactly the same, and this is Spark's documented `getOrCreate` behavior, so it is not a regression.
- [x] **Security.** Nested readers now check `spark.synapseml.legacy.allowUnsafeJavaDeserialization` on the assigned session instead of the caller's active or default session. Before, a permissive active session could allow unsafe Java deserialization of nested stages even when the reader was assigned a restrictive session. The nested matrix in `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` (allowed × array × active) tests both directions. `TestBase` sets this flag to true on the default session, so the cases with no active session and allowed=false also catch a regression.
- [x] **Tests catch the bug.** In the nested writer matrix in `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` (L157-225), the child's `ByteArrayParam` requires the assigned session's config key. The old serializers ignored the session, so they would fail on the first parent save. The tests also check:
  - active and default sessions are restored after each operation;
  - overwrite=false on an existing artifact raises an "already exists" `IOException` and keeps the saved payload;
  - overwrite=true with the wrong writer session fails;
  - a parent save fails when the writer session is wrong.

  The test counts (20 + 11 = 31) match the caller's claim.
- [x] **Fixture move.** `TestEstimatorBase` and `TestableSessionParamTest` moved to `ComplexParamTestFixtures.scala` in the same package, so their fully qualified names are unchanged and the existing `FuzzingTest` exemptions still apply. Adding no-argument constructors does not pull them into fuzzing, because `pipelineStages` in `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` skips simple names that start with `Testable`.
- [x] **Repository rules.** The change adds no RDD usage and no edits under `target/`. It has no credentials and no dependency, pipeline or release changes. The new file has a license header. The test-only refactor directly supports the new tests, and it touches no AIPrompt or optional-recovery code.
- [ ] Not run: tests, compile and scalastyle, because this review is read-only. I took the caller's local evidence (JDK 11, Spark 3.5.0, Scala 2.12.17, 31 tests, 0 skips) as unverified input. I did not evaluate CI on the current head.