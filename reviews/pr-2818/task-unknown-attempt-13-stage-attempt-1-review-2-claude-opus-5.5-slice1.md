---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 2
timestamp: "2026-11-10T03:40:56Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ba25617865842a0b80bf5d88637f3faf390124f09d2d7472c90f96d15094441a"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-13-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read every line of the assigned frozen diff using the frozen sequence [1, 150], [151, 300], [301, 450], [451, 455]. On the reviewed tree (caller-supplied source HEAD b21f52d), the current versions of all five inventory files match the diff hunks.
- [x] **Session passing in `typeToSerializer`:** In `core/src/main/scala/org/apache/spark/ml/Serializer.scala` L59–60, `typeToSerializer` now passes `sparkSession` to `PipelineSerializer` and `PipelineArraySerializer`. This follows the same constructor-injected session pattern as `DFSerializer`, `ObjectSerializer` and `FilteredObjectSerializer` in the same file.
- [x] **Constructors and compatibility:** In `Serializer.scala` L258–296, the public no-argument constructors are kept as auxiliary `def this()` (L259, L273), so the JVM `<init>()V` signatures are unchanged. The new `SparkSession` constructors (L262, L276) add API without removing any. The primary `Option` constructors are `private`. A null session falls back to the active or default session at call time. This keeps existing callers that pass a null session to `ComplexParam.save`/`load` for pipeline-typed params working, because the old serializers ignored the session.
- [x] **Single place for session handling:** `PipelineSerializer` delegates to `PipelineArraySerializer` (L264–269), so the session logic lives in one private `withSession` helper (L278–285). That helper does three things:
  - It resolves the fallback through `SparkSession.builder().getOrCreate()`, the same approach as the legacy overloads in `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` (L66–69, L174–177).
  - It sets the thread-local active session and restores or clears it in `finally`, so nested calls unwind in the right order.
  - Its inline comment explains why the swap is needed. The pattern matches Spark's `SparkSession.withActive`, which is `private[sql]` and can't be called from `org.apache.spark.ml`.
- [x] **Serialized format unchanged:** Writes still persist `new Pipeline().setStages(...)`, and reads still use `getStages` or `(0)`. This matches `PipelineStageWrappable.pyLoadLine`/`rLoadLine` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/EstimatorParam.scala`, which load a `Pipeline` and take stage 0. No production Param definitions changed. `writeMLWritable`, 3-arg `saveComplexParams` and 3-arg `getAndSetComplexParams` received doc comments only (L71, L66, L174).
- [x] **The session reaches nested writers and readers at every step:**
  - Write path: `ComplexParamsWriter.saveImpl` → `saveComplexParams(..., sparkSession)` → `ComplexParam.save` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala`) → `typeToSerializer` → the pipeline serializer.
  - Read path: `ComplexParamsReader.load` → `getAndSetComplexParams(..., sparkSession)` → `ComplexParam.load`.
  - Which params are affected: `TransformerParam`, `EstimatorParam` and `PipelineStageParam` go through `PipelineSerializer`. `TransformerArrayParam` and `EstimatorArrayParam` hold `Array[Transformer]` and `Array[Estimator[_]]`. Scala arrays are invariant, so those types don't match `Array[PipelineStage]` and stay on the object-serializer path, which this PR doesn't change.
- [x] **Test fixtures:** `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala` stays in the same package, so fully qualified names don't change. This matters because other modules depend on core with `test->test`. Both `TestableSessionParamTest` and the new `TestableNestedParamTest` follow the repo convention: a `uid` constructor, a random-UID no-arg constructor, and a `ComplexParamsReadable` companion.
- [x] **Test coverage:**
  - `ValidateComplexParamSerializer.scala` (new L145–272) covers both serializer branches (`TransformerParam` and an anonymous `ComplexParam[Array[PipelineStage]]`), with and without an active session.
  - It also covers overwrite=true and overwrite=false, the existing-path `IOException` with the saved payload kept, the unassigned/null fallback, and session restoration after failures.
  - `stages/VerifyUDFTransformerPersistence.scala` L46–85 checks that the assigned session's deserialization policy is applied in both directions. Those cases would fail if the default `spark` session leaked into child readers, because TestBase's `sparkConfiguration` turns legacy deserialization on.
  - The count is 20 + 11 = 31 tests, which matches the caller's evidence.
  - `Test / parallelExecution := false` in `build.sbt` keeps the tests' global default-session changes isolated, following the pattern the existing legacy-helper test already uses.
- [x] **Repository conventions:**
  - The new file has the license header and explicit imports, and no wildcard imports were added.
  - New lines stay within 120 characters, and the files are well under 800 lines.
  - No RDD usage, dependency, `pipeline.yaml`, workflow, codegen or release changes were added.
  - The PR targets master, as `.github/skills/synapseml-branches/references/branch-spark3p5.md` expects. `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (`pyspark==3.5.0`) match the caller's runtime evidence.
- [x] **Choices I looked at and decided not to report:**
  - The 3-line overwrite logic is inlined instead of adding a session overload to the public `Serializer.writeMLWritable`. This avoids adding public API.
  - The new session constructors are public rather than `private[ml]`, which matches `DFSerializer` and `ObjectSerializer`.
  - Tests use `Option.empty[SparkSession].orNull` rather than a literal `null`. This avoids a style suppression, as `.github/skills/scala-code/SKILL.md` asks.
  - Session assertions are repeated inline in the tests. That keeps ScalaTest failure positions precise.
- [ ] **Not executed:** I ran no build, test or style commands because this review is read-only. The compile, Test/compile, 31-test and both scalastyle results come from the caller, ran on a pre-commit tree, and are not current-head CI.
- [ ] **Not searched:** I did not search the repository for other callers of `Serializer.writeMLWritable` or direct `new PipelineSerializer()`/`new PipelineArraySerializer()` constructions, because the required read profile forbids search tools. Those signatures are unchanged, so any such callers keep their previous active/default-session behavior.