---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 3
timestamp: "2026-11-09T06:09:54Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "b32079be428bc3381918880c3f9c7fd21d2f426dbd54e8f9e6758dca2fec054e"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **What I read:** I read the assigned frozen diff using exactly the frozen ranges [1, 150], [151, 300] and [301, 437]. That covers all 437 lines across 5 files. I also read the current versions of these source files:
  - `core/src/main/scala/org/apache/spark/ml/Serializer.scala`
  - `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala`
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala`
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala`

  I also read the guidance files: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the master target) and `.github/skills/code-review/SKILL.md`. Versions came from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Session restore on failure** (`Serializer.scala` 278–285, `withSession`):
  - The session is resolved before `previous` is captured. If `getOrCreate()` throws, for example off the driver, nothing has been changed yet.
  - If `getOrCreate()` creates a new session, that session stays active afterwards. This matches the old `Pipeline.load`/`writeMLWritable` behavior.
  - Every failure point runs inside the `try`: Pipeline stage validation, the `IOException` when a path already exists, exceptions from child writers and readers, and the deserialization-policy `SecurityException`.
  - The `finally` only sets or removes a thread-local value, which cannot throw. So the original exception is never hidden.
  - Nested calls restore their sessions in last-in, first-out order.
- [x] **Null and boundary inputs** (`Serializer.scala` 259–262, 273–276, 269):
  - A null session becomes `None` and falls back to the active or default session. The unassigned-session tests cover this using `Option.empty[SparkSession].orNull` (`ValidateComplexParamSerializer.scala` 215–258).
  - `PipelineSerializer.read` uses `(0)`. On an empty artifact it throws the same `ArrayIndexOutOfBoundsException` as the old `getStages(0)`.
  - Empty or null stage arrays still go through Spark's existing validation inside the guarded block. None of this behavior is new.
- [x] **Concurrency:** Serializer instances are immutable, and a new one is created for each `typeToSerializer` call (`Serializer.scala` 59–60; `core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala` save/load). The active-session swap only affects the current thread. Threads started during the swap inherit the assigned session. Spark already does the same when it temporarily switches the active session to run Dataset writes under that session, such as the existing `ComplexParamsWriter.saveMetadata`. So the change adds no new kind of risk.
- [x] **Compatibility and serialized format:**
  - The public no-argument constructors are kept.
  - The new `(SparkSession)` constructors are explicit overloads, with no default arguments.
  - The `Option` primary constructors are private.
  - The `write`/`read` signatures are unchanged.
  - Nested artifacts are still written and read by Spark's `Pipeline` writer and reader, so the on-disk layout is the same.
  - `ComplexParamsSerializer.scala` lines 66 and 174 and `Serializer.scala` line 71 only add doc comments.
- [x] **Code in other files that uses these classes** (opened by exact path):
  - `TransformerParam`, `PipelineStageParam` and `EstimatorParam` inherit `ComplexParam.save/load`, so they now receive the caller's session.
  - `TransformerArrayParam` and `EstimatorArrayParam` use invariant arrays, so they already go through `ObjectSerializer(sparkSession)` and are unaffected.
  - `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` skips classes whose names start with `Testable` or contain `$`. It also exempts `core.serialize.TestEstimatorBase` by its full class name. So the new no-argument constructors on the `Testable*` fixtures, and moving `TestEstimatorBase` to another file, don't add any classes to the fuzzing checks.
- [x] **The tests would catch regressions:**
  - In `VerifyUDFTransformerPersistence.scala` 46–84, three cases would fail if child readers used the caller's active or default session instead of the assigned one: `allowed=false` with and without an active session, and `allowed=true` with an active session.
  - `ValidateComplexParamSerializer.scala` 156–213 would catch two wrong ways of restoring the session: switching to the default session (when there was no active session) and always clearing it (when there was one). It checks this after successful saves, overwrites and failures.
  - `withActiveSession` (fixtures lines 15–20) and the `try/finally` around the default session (lines 215–258) restore state even when an assertion fails.
  - Every test name and path is unique across the test matrix.
- [x] **Limitations that already existed (not regressions):**
  - On Spark 3.5, the wrapper Pipeline's own metadata is still read and written through `SparkContext`. Session-level Hadoop settings apply to the child stages but not to that wrapper metadata, which was also true before this change.
  - Saving or loading ML models on executors, where `getActiveSession` returns `None`, is not supported.
- [x] **Validation evidence:** I relied on the caller's reported results: compile, Test/compile, 31 tests with 0 skipped, and scalastyle, on JDK 11, Spark 3.5.0 and Scala 2.12.17. I did not rerun any of them.
- [ ] **Not done here:** I did not run any tests, including the root-project `FuzzingTest` or the full `pipeline.yaml` suites. This review is read-only and doesn't allow running commands; remote CI is a separate gate.