---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 1
timestamp: "2026-11-09T05:13:18Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "16e88c1a7b586a04c3c03f36c317e71a868016c80c0a17d290ff23107c6b6c83"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read:** I read all 437 lines of the assigned frozen diff in the frozen sequence [1,150], [151,300], [301,437]. It covers 5 files. I also read the current versions of all 5 inventory files, plus `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0). This is master-branch context.
- [x] **Public signatures and format:** checked in `core/src/main/scala/org/apache/spark/ml/Serializer.scala` (L56–L68 and L258–L296).
  - Both serializers keep their public no-argument constructor through `def this()`. `write` and `read` signatures are unchanged.
  - The new `this(SparkSession)` constructor is an addition. `Option(spark)` turns a null session into the fallback path.
  - The overwrite flag still works. `writer.overwrite()` changes the same writer object. With `overwrite=false`, Spark still throws when the path already exists.
  - The on-disk layout is still a `Pipeline` wrapper, so Python/R `Pipeline.load(...).getStages()[0]` keeps working (`PipelineStageWrappable` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/EstimatorParam.scala`).
- [x] **Session handling (`Serializer.scala` L278–L285):**
  - The `finally` block restores the caller's active session after both success and exceptions.
  - The fallback session is resolved before `previous` is captured. This keeps the old side effect when `getOrCreate()` has to create a new session.
  - The default session is never changed, and nested calls restore the outer session correctly.
  - Setting the active session is needed. In Spark 3.5, Pipeline does not pass its session to the readers and writers of its child stages; they fall back to `getOrCreate()`, which uses the thread's active session. This is based on known Spark 3.5.0 behavior; Spark source is not in this checkout.
- [x] **Dispatch coverage:**
  - `ComplexParam.save/load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala` L23–L30) always goes through `Serializer.typeToSerializer`.
  - `TransformerParam`, `PipelineStageParam` and `EstimatorParam` do not override `save` or `load`.
  - `ComplexParamsWriter.saveImpl` and `ComplexParamsReader.load` already pass their own session explicitly (`ComplexParamsSerializer.scala` L36–L41 and about L167). The new doc comments on the legacy overloads are accurate.
- [x] **Security (CWE-502):** nested reads now check `spark.synapseml.legacy.allowUnsafeJavaDeserialization` on the assigned session, not on whatever session is currently active or default (`Serializer.readFromHDFS`). This closes a policy bypass. The 8-case matrix in `VerifyUDFTransformerPersistence.scala` covers both policy directions, with and without an active session. No new deserialization surface was added.
- [x] **Test quality:**
  - Without the fix, the targeted cases would fail. The assigned session's settings differ from the active/default session set up by `TestBase`, which has the legacy deserialization flag on.
  - The failure paths check that both the active and default sessions are restored.
  - Test names and output paths are unique.
  - `Test / parallelExecution := false` in `build.sbt` makes the temporary default-session swap safe.
  - `FuzzingTest` (`src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala`, about L420–L433) skips stages named `Testable*`. `TestEstimatorBase` keeps its fully qualified name, so its existing exemptions still apply.
- [x] **Repository conventions:** the license header is present and the longest new line is about 119 characters. The diff adds no RDD usage, dependencies, pipeline or release changes, or native AIPrompt work. Moving the fixtures is needed because both suites share `TestableNestedParamTest`.
- [x] **Caller-provided validation evidence:** compile, Test/compile, 31 tests with 0 skips and scalastyle, all on JDK 11. The 31 tests match this tree's suite sizes (20 + 11). I did not run these myself.
- [ ] **Not done — repository-wide caller search:** I did not search the whole repository for other direct `new PipelineSerializer()` or `Serializer.writeMLWritable` callers, because the read profile forbids search tools. Any such caller keeps its previous behavior through the unchanged no-argument constructors.