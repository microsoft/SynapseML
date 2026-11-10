---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 1
timestamp: "2026-11-09T03:03:53Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "5385856e827a6d068c290aa3cca14e32fe21311d5c756a6e3ea1d10706f3a2af"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-10-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read all 393 lines of the assigned frozen diff in the required order: [1, 150], [151, 300] and [301, 393]. It covers 5 files. The current checkout of every inventory file matches the diff's post-image.
- [x] I read the guidance and version files: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `references/branch-spark3p5.md` (the master target), `.github/skills/code-review/SKILL.md`, `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Session routing** — `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, lines 59–60:
  - `typeToSerializer` now passes the caller's session to `PipelineSerializer` and `PipelineArraySerializer`.
  - `TransformerParam`, `PipelineStageParam` and `EstimatorParam` all reach this code through `ComplexParam.save`/`load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala`).
  - `TransformerArrayParam` and `EstimatorArrayParam` hold `Array[Transformer]` and `Array[Estimator[_]]`. Scala arrays are invariant, so these are not `Array[PipelineStage]` and still use `ObjectSerializer(sparkSession)`. That path is unchanged and already uses the assigned session.
- [x] **Restoring the active session** — `Serializer.scala`, lines 278–285 (`withSession`):
  - `getOrElse` only evaluates its fallback when needed, so `getOrCreate()` runs only when no session was assigned.
  - The caller's active session is saved after the session is chosen. It is restored in `try`/`finally` with `previous.fold(clearActiveSession)(setActiveSession)`, which cannot throw and runs for every exception type.
  - In Spark 3.5, `Pipeline.SharedReadWrite` creates child readers and writers without calling `.session(...)`. Those children get their session from `getOrCreate()`, which checks the thread's active session first. Temporarily making the assigned session active is therefore both needed and enough. The default session is not touched.
  - The comment on line 281 says the same thing. The tests with no active session show this behavior directly.
- [x] **Saved format unchanged** — `Serializer.scala`, lines 258–296:
  - Writes still save `new Pipeline().setStages(...)`. `writer.overwrite()` sets the flag on the same writer object, so it behaves like the old `writeMLWritable`.
  - Reads still use `Pipeline...load(...).getStages`. `PipelineSerializer` delegates with `Array(stage)` and `(0)`, exactly as before.
  - No serialized format or Param shape changes.
- [x] **Public API compatibility**:
  - Both public no-argument constructors are kept as secondary constructors, so their bytecode signature is unchanged.
  - The `write`/`read` signatures are unchanged.
  - The new `(SparkSession)` constructor is an added explicit overload, not a default argument, as the code-review skill requires. The `Option` primary constructor is private.
  - `Option(spark)` turns a null session into the documented fallback (lines 261 and 275). The calls at lines 59–60 and line 264 pick the public `SparkSession` overload.
- [x] **Security**:
  - No new deserialization path is added.
  - Nested `UDFParam` reads now check `spark.synapseml.legacy.allowUnsafeJavaDeserialization` on the assigned session instead of the current active or default session. This matches how top-level complex params already behave since microsoft/SynapseML#2809. It grants nothing beyond what the caller's assigned session allows.
  - No RDD use, credentials or new dependencies.
- [x] **Writer tests** — `ValidateComplexParamSerializer.scala`:
  - Lines 156–189 run 4 variants (array or single stage, with or without an active session). They check that nested writes and reads use the assigned session, include a failing write, and confirm the active and default sessions are restored afterwards.
  - Lines 191–214 check that a null session falls back to the active session rather than the default one.
- [x] **Reader-policy tests** — `VerifyUDFTransformerPersistence.scala`, lines 46–84, run 8 variants.
  - `TestBase` turns the legacy flag on for the default session (`sparkConfiguration`).
  - Because of that, 3 of the 4 allowed/active combinations for each shape would fail on the code before this change, so the tests detect the fix.
- [x] **Test fixtures and FuzzingTest**:
  - The fixtures moved to `ComplexParamTestFixtures.scala` keep their fully qualified class names.
  - In the root `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala`, the `pipelineStages` setup (around line 424) skips classes whose names start with `Testable`. `TestEstimatorBase` is still exempted by its full name.
  - So the new `TestableNestedParamTest` and the now no-argument-constructible `TestableSessionParamTest` do not trigger FuzzingTest's coverage checks.
- [x] **Doc comments**:
  - The comments on the older overloads are accurate: `ComplexParamsSerializer.scala` lines 66 and 174, and `Serializer.scala` line 71.
  - Production code (`ComplexParamsWriter.saveImpl` line 40 and `ComplexParamsReader.load`) already calls the overloads that take an explicit session.
- [x] **Style**:
  - The new file has the license header and explicit imports.
  - The longest touched line is 119 characters (`ValidateComplexParamSerializer.scala`, line 182).
  - The test-fixture move is small and needed so both suites can share fixtures.
- [x] **Edge cases checked, no issue found**:
  - If no usable session exists at all, `getOrCreate()` creates one and leaves it set as active and default. The old code did the same.
  - The `overwrite = true` branch is not covered by the 29 local tests. By inspection it is correct.
- [x] **Local test results supplied by the caller (I did not run any tests)**:
  - compile, Test/compile, production and test scalastyle, and 29 tests with 0 skips, on JDK 11, Spark 3.5.0 and Scala 2.12.17.
  - The 29 tests are exactly the 18 in `ValidateComplexParamSerializer` plus the 11 in `VerifyUDFTransformerPersistence`.
  - Root FuzzingTest and remote CI are not part of that evidence.
- [ ] Not done: a repository-wide search for other direct `new PipelineSerializer()` / `new PipelineArraySerializer()` callers, or for docs that describe nested session behavior. The required read profile forbids search tools; every path I could reach from the listed files was inspected.