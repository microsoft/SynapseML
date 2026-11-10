---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 2
timestamp: "2026-11-09T21:56:33Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "c2100fb9999b3aa5e210e8d31fa03eb52dc764b6276ca10364fcecc01d38de69"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-12-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 437 lines of the assigned frozen diff using the three frozen ranges: [1–150], [151–300] and [301–437]. The current source matches the diff's post-image in these places:
  - `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, lines 59–60, 71 and 258–296
  - `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, lines 66 and 174
  - all three touched test files
- [x] **Guidance and versions:** I applied `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (target is `master`) and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` lines 31–34 (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Session flow traced end to end:**
  - `ComplexParamsWriter.saveImpl` (`ComplexParamsSerializer.scala` 36–41) and `ComplexParamsReader.load` (line 167) pass the assigned session.
  - It then flows through `ComplexParam.save/load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala` 23–30), then `Serializer.typeToSerializer` (lines 59–60), then `PipelineSerializer`/`PipelineArraySerializer(session)`.
  - `withSession` (lines 278–285) makes the assigned session active, so Spark's Pipeline child readers and writers pick it up. Its `finally` block restores the caller's active session. It never changes the default session.
  - Deeper nesting also works. Each nested `ComplexParamsWriter`/`ComplexParamsReader` picks up the active (assigned) session and goes through the same path again.
- [x] **Abstraction fit:**
  - `withSession` follows the same save/set/restore pattern as Spark's own `SparkSession.withActive`. That method is `private[sql]`, so code in `org.apache.spark.ml` can't call it.
  - The pipeline serializer is the narrowest place for this logic, and it also covers callers that use `ComplexParam.save/load` directly.
  - `PipelineSerializer` now delegates to `PipelineArraySerializer`, which removes duplicated Pipeline-wrapping code.
  - I considered asking for a session-aware `Serializer.writeMLWritable` overload and decided against it. Calling `.session(...)` alone would not reach Pipeline children, so it would be a misleading half-fix. The new doc comment at line 71 correctly limits the legacy helper to the active or default session.
- [x] **Compatibility:**
  - The public no-arg constructors are preserved through auxiliary `this()` constructors.
  - The new `(SparkSession)` constructors are additive and match the sibling `DFSerializer(spark)` and `ObjectSerializer(spark)` classes. The private `Option` primary constructor keeps `Option` out of the public JVM surface.
  - A null session falls back to the active or default session. This keeps the pre-PR behavior, where pipeline-typed params ignored the session. The "unassigned session" tests cover this.
  - The on-disk format is unchanged: still a `Pipeline` wrapping the stages, read back with `getStages`. This matches `PipelineStageWrappable.pyLoadLine`/`rLoadLine` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/EstimatorParam.scala`.
- [x] **Fixture conventions:**
  - `JarLoadingUtils.AllClasses` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/utils/JarLoadingUtils.scala`) does not filter out test classes, so fixture naming matters.
  - In `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala`, line 424 excludes class names starting with `Testable`, and the exemption list names `core.serialize.TestEstimatorBase` by its full name (for example, line 52).
  - The new `TestableNestedParamTest` follows the `Testable` prefix rule. The moved `TestEstimatorBase` keeps its package and name. The fuzzing checks are therefore unaffected.
  - The fixtures take a `uid`, have a random-UID no-arg constructor and `ComplexParamsReadable` companions, use explicit imports and have the license header.
- [x] **Style config:**
  - `scalastyle-test-config.xml` turns on `NullChecker`, which explains the `Option(spark)` and `Option.empty[SparkSession].orNull` idioms.
  - It does not include `MagicNumberChecker`, so the literal byte payloads are allowed.
  - The longest new line is about 119 characters, within the 120 limit.
- [x] **Test design:**
  - There are 8 new cases in `ValidateComplexParamSerializer.scala` and 8 in `VerifyUDFTransformerPersistence.scala`.
  - They cover single and array params, with and without an active session, assigned and null sessions, overwrite, writer failure and the reader's `SecurityException` policy path.
  - After both success and failure, they check that the active and default sessions are restored.
  - They exercise the public `write.session(...)` and `read.session(...)` paths, using a real production stage (`UDFTransformer`).
  - The source contains 20 + 11 = 31 tests, matching the supplied evidence.
- [x] **Scope:** There are no AIPrompt, recovery, dependency, release, workflow, `pipeline.yaml` or RDD changes, and serialized Params are unchanged.
- [ ] **Runtime validation (not performed):** I did not run anything, per the read-only review contract. The compile, Test/compile, 31-test and scalastyle results are evidence supplied by the caller only.