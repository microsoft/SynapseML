---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 2
timestamp: "2026-11-09T03:18:13Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "f194cf043d5f7fa27398b29123f21248a1c322924a1324b82e04f38fffa3b5ea"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-10-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read every line of the assigned frozen diff using exactly the ranges [1, 150], [151, 300] and [301, 393]. The diff's after-state for all five files matches the current source in `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala`.
- [x] I applied the conventions in `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (target `master`), `.github/skills/code-review/SKILL.md` and `.github/skills/scala-code/SKILL.md` (linked from `AGENTS.md`). I took versions from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Consistency with nearby serializers:** In `Serializer.scala`, `typeToSerializer` now passes `sparkSession` to `PipelineSerializer` and `PipelineArraySerializer`. This matches how `DFSerializer(sparkSession)` and `ObjectSerializer(sparkSession)` already work.
  - The session is added through separate constructor overloads, not default parameters, as the code-review skill's binary-compatibility checklist requires.
  - The public no-argument constructors, the `write`/`read` signatures and `Serializer.writeMLWritable` are all kept.
- [x] **Saved format is unchanged:** `PipelineSerializer` now hands its work to `PipelineArraySerializer`. It still saves a Spark `Pipeline` with one stage and reads back `getStages(0)`, and overwrite behaves the same as the old `writeMLWritable` path. `TransformerParam.rLoadLine` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/TransformerParam.scala` relies on this layout (`ml_stages(...)[[1]]`) and is unaffected.
- [x] **The session switch is in the right, narrowest place:** `PipelineArraySerializer.withSession` sets the active session only where Spark's `Pipeline` creates child readers and writers without passing a session along.
  - Outer and nested complex params already receive the session explicitly: `ComplexParamsWriter.saveImpl` / `ComplexParamsReader.load` → `ComplexParam.save/load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala`) → `typeToSerializer`.
  - The previous active session is restored, or cleared, in `finally`. The helper never sets the default session, and nested pipelines restore correctly at each level.
  - This avoids re-implementing Spark's pipeline persistence, which could drift from Spark's format on the port branches. It follows the same pattern as Spark's own `SparkSession.withActive`, which is `private[sql]` and can't be called from here.
- [x] **Null session behaviour:** `Option(spark)` turns a null session into the old behaviour of picking the active or default session when the save or load runs.
  - This keeps direct `ComplexParam.save/load(..., null, ...)` calls on pipeline-typed params working. They worked before because the pipeline serializers ignored the session.
  - The other serializers' behaviour is unchanged. The null handling is documented on both new constructors and covered by the "unassigned session" tests.
- [x] **Test structure:**
  - The fixtures moved to `ComplexParamTestFixtures.scala` keep their fully qualified names, so the `TestEstimatorBase` exemptions in `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` still match.
  - The new `TestableNestedParamTest` uses the `Testable*` prefix, which `FuzzingTest.pipelineStages` skips.
  - It follows the stage rules in `AGENTS.md`: a uid constructor, a random-UID no-argument constructor, a `ComplexParamsReadable` companion, `defaultCopy` and `transformSchema`.
  - `TestableSessionParamTest` gains the no-argument constructor it was missing.
  - `withActiveSession` replaces two existing hand-written try/finally blocks and is reused by the new tests. It can't reuse the production helper, which is private to the class.
- [x] **Coverage:** The tests go through the public `write.session(...)` / `read.session(...)` path, not only internal helpers.
  - They cover both the `PipelineStage` and `Array[PipelineStage]` serializer branches, with and without an active session.
  - They check that sessions are restored after a writer failure (`IllegalArgumentException`) and after a reader failure (`SecurityException` in `VerifyUDFTransformerPersistence.scala`).
  - The reader tests also show a lenient active session can't override the assigned session's deserialization policy.
- [x] **Housekeeping:**
  - The new file has the license header and only explicit imports.
  - The removed imports (`Transformer`, `StructType`, `DataFrame`, `Dataset`) are no longer used, and the remaining ones (`ParamMap`, `Path`, `SparkSession`, `PipelineStage`) are.
  - There are no RDD APIs, `target/` edits, or dependency, workflow, release or AIPrompt changes.
  - The longest changed lines are within the 120-character limit.
- [x] **Caller-supplied validation (I did not run it):** compile, Test/compile, production and test scalastyle, and 29 tests with 0 skips on JDK 11 / Spark 3.5.0 / Scala 2.12.17. The count matches the 18 + 11 tests in the two changed suites. `FuzzingTest` was not part of that run; reading the code shows the new fixture is excluded by its name.
- [x] **Considered but not raised as issues:**
  - The session-aware overwrite/save logic is three inlined lines rather than a new `writeMLWritable` overload. The old helper is kept and documented, and adding public API would go beyond this change's scope.
  - With no assigned session, the active-session switch either does nothing or briefly makes the default session active. Child stages resolve the same session either way.
  - Only the pipeline serializers accept a null session. This is deliberate, to keep existing callers working.
- [ ] Not checked: callers of `Serializer.writeMLWritable` or the no-argument pipeline serializer constructors outside the files reachable by import. The required read profile forbids search tools, so these conclusions cover the diff and the files listed above.