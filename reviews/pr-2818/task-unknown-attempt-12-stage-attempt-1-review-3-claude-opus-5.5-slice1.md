---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 3
timestamp: "2026-11-09T22:30:06Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "85fa10d4d35a58d28809b7c404da75bd58e6a753d24c0d8f1fd03f37a58b130c"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-12-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 437 lines of the assigned frozen diff using exactly the frozen ranges [1,150], [151,300] and [301,437]. That covers all 5 files: `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala`.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the base branch is master) and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Session restored on success or failure:** In `Serializer.scala:278-285`, `withSession` finds the session to use first. It then sets that session as the thread's active session and runs the action inside `try`. The `finally` block restores the previous session with `previous.fold(clearActiveSession)(setActiveSession)`.
  - These failures all pass through that `finally`: `validateStages` in the `PipelineWriter` constructor, `handleOverwrite`, nested `require` failures and a `SecurityException` on read.
  - The restore calls cannot throw, so they never hide the original exception.
  - Nested calls (a pipeline inside a pipeline, through child `ComplexParamsWriter`/`ComplexParamsReader`) restore in reverse order.
- [x] **Concurrency:** Spark stores the active session per thread, and the code reads, sets and restores it only on the calling thread. `typeToSerializer` (`Serializer.scala:59-60`) creates a new serializer for each call, and its state never changes. The diff adds no shared mutable state and no locks.
  - Child threads created during the call inherit the session. Spark's own `SparkSession.withActive` does the same on every Dataset action, so this is not a new kind of leak.
- [x] **Null or missing session:** `Option(spark)` (`Serializer.scala:262, 276, 279`) turns a null session from `typeToSerializer(tpe, null, …)` into the active-or-default fallback. That matches the old behavior, when the session argument was ignored.
  - If no session exists at all, `getOrCreate()` runs before `previous` is captured. The end state is the same as the old `Pipeline.load`/`writeMLWritable` path.
  - `ValidateComplexParamSerializer.scala:215-258` tests both the active-session and default-only cases, including a failing overwrite.
- [x] **Compatibility and file format:**
  - The public no-argument constructors are kept (`def this()`), and `this(SparkSession)` is only an addition.
  - The `write`/`read` signatures are unchanged.
  - Saved files keep the same layout: `new Pipeline().setStages(...)` on write, and `getStages` or `(0)` on read.
  - `writer.overwrite()` changes the writer itself, so it behaves the same as `writeMLWritable`.
  - Behavior for an empty array with `(0)`, and for stages that can't be written, is unchanged.
- [x] **Which params use the changed code:** `core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala` sends `TransformerParam`, `EstimatorParam`, `PipelineStageParam` and exact `Array[PipelineStage]` values to the changed serializers.
  - `TransformerArrayParam` and `EstimatorArrayParam` don't match `Array[PipelineStage]` (Scala arrays are invariant), so they go to `ObjectSerializer`. That class already uses the session it is given.
  - `ComplexParamsWriter.saveImpl` and `ComplexParamsReader.load` pass their own session. The older overloads without a session argument only gained doc comments (`ComplexParamsSerializer.scala:66, 174`).
- [x] **Test strength and isolation:** Every combination of active/default × array × allowed tells the assigned session apart from the ambient one. This works because `TestBase.sparkConfiguration` sets legacy deserialization to `"true"` on the base session, and the base session has no `ConfigKey`.
  - The failure paths check that the active and default sessions are restored (`ValidateComplexParamSerializer.scala:198-210`, `VerifyUDFTransformerPersistence.scala:71-81`).
  - The default-session change in the null-session tests is undone in `finally`. Paths and test names are unique for each combination.
  - `build.sbt` sets `Test / parallelExecution := false`, so the checks on the global default session can't race with other suites.
- [x] **Stage-discovery tests:** In `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala`, `pipelineStages` skips classes whose names contain `$` or start with `Testable`.
  - So the new no-argument constructors on `TestableSessionParamTest` and `TestableNestedParamTest` won't cause fuzzer-coverage failures.
  - `TestEstimatorBase` keeps its fully qualified name, so its existing exemptions still apply.
- [x] **Existing Spark behavior, not caused by this diff:** Spark's `FileSystemOverwrite` deletes the destination before writing, so an overwrite isn't atomic. Spark 3.5's `Pipeline` writer also uses `SparkContext`. Neither changes with this diff, so neither is counted as a finding.
- [ ] **Tests not run:** This was a read-only review. I treated the caller's compile, test, scalastyle and Azure results as context only and did not verify them.
- [ ] **Spark internals not checked against source:** Three Spark 3.5 behaviors were judged from knowledge of upstream Spark, because its sources are outside the review scope:
  - `getOrCreate` does not make the default session active.
  - `Pipeline` does not pass its session to child writers and readers.
  - `Instrumentation` rethrows the original exception.