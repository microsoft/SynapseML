---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 6
timestamp: "2026-11-09T07:58:32Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "e59fe13d0e344729f661d5cb67b7a99d64da44abbb01d9ad2fb0eb894076bb53"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-6-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read:** I read every line of the assigned frozen diff in exactly three chunks: [1, 150], [151, 300] and [301, 437]. It covers 437 lines across 5 files. The current source of all five files matches the diff's post-image: `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala`.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. `build.sbt` sets Spark 3.5.0 and Scala 2.12.17. `environment.yml` pins pyspark 3.5.0.
- [x] **Performance (`Serializer.scala`):** `withSession` in `PipelineArraySerializer` only adds one thread-local read, set and restore per nested read or write.
  - With an assigned session, `getOrElse` never calls `SparkSession.builder().getOrCreate()`.
  - Without one, it makes the same single `getOrCreate()` call the `MLWriter`/`MLReader` made before. Passing `.session(session)` prevents a second call.
  - There is no extra I/O, no caching and no new locking. `PipelineSerializer` only allocates one extra delegate object.
- [x] **Observability and failure paths:** `try`/`finally` has no `catch`, so the original exceptions reach the caller unchanged. The tests assert the `IllegalArgumentException` and `SecurityException` types. Spark's Pipeline instrumentation is unchanged. `getOrCreate()` with no options logs no "existing session" warning, so the fallback path adds no log noise.
- [x] **Documentation accuracy:**
  - The comment "Spark's Pipeline does not forward its session to child readers and writers" matches Spark 3.5 behavior. Pipeline's shared read/write code passes only the SparkContext, and child `stage.write`/`read` calls pick up the active session.
  - The constructor note "A null session falls back to the active or default session when reading or writing" matches the code: `Option(spark)` plus a session lookup deferred to read/write time.
  - The new notes on the older overloads in `ComplexParamsSerializer.scala` (lines 66 and 174) match behavior pinned by the `legacy complex parameter helpers use the $selected Spark session` tests, via `core/src/test/scala/org/apache/spark/ml/ComplexParamsTestHelper.scala`.
  - The `writeMLWritable` note is accurate.
- [x] **Compatibility:** Both serializers keep their public no-argument constructors. The new `this(SparkSession)` constructors only add API, and the private `Option` constructors can't be called from outside. The `read`/`write` signatures are unchanged. The on-disk format is identical: the same `new Pipeline().setStages(...)` layout, still a single-stage pipeline for `PipelineSerializer`, read back with `(0)`. Both `typeToSerializer` overloads pass the session through. `TransformerParam`, `EstimatorParam` and `PipelineStageParam` reach these serializers through `ComplexParam.save`/`load`, which call `typeToSerializer`.
- [x] **Naming:** `withSession`, `delegate`, `hasActiveSession`, `expectedActive`, `previousDefault` and `unassigned` are clear and consistent with nearby code. `TestEstimatorBase` keeps the same fully qualified name after moving into the fixtures file.
- [x] **Fuzzing harness:** `pipelineStages` in `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` (line 424) skips classes whose simple name starts with `Testable`. So the new no-argument constructors on `TestableSessionParamTest` and `TestableNestedParamTest` add no un-exempted stages. The existing `TestEstimatorBase` exemptions still match.
- [x] **Tests detect a missing fix:**
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` turns on legacy deserialization for the root session. In the UDF suite, 3 of the 4 allowed×active combinations per `array` value would fail without the fix.
  - In the nested-writer tests, every combination would fail if the root session (which lacks `ConfigKey`) were used.
  - Session restoration is asserted after both successful and failed operations. The default session is restored in `finally`. `tmpDir` is a fresh temporary directory per suite.
- [x] **Restore-order note (not an issue):** without an assigned session, `previous` is captured after `getOrCreate()`. This only matters when no session exists at all. In that case Spark's builder itself makes the new session active, which is the same as before this PR.
- [x] **Test count, by reading only:** the two suites register 20 + 11 = 31 tests, which matches the caller's evidence. I did not run any tests.
- [x] **Scope and style:** the diff has no AIPrompt, dependency, release or pipeline/workflow changes and adds no RDD code. The new file has its license header. The longest new lines are 120 characters or fewer.
- [ ] **Not done, full caller audit:** I didn't check for direct `new PipelineSerializer()`/`new PipelineArraySerializer()` calls outside `typeToSerializer`, because the required read profile forbids search tools. The pipeline-typed params I inspected all go through `typeToSerializer` and receive the session.