---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 5
timestamp: "2026-11-09T07:28:59Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "74e1024e5c538f489378cab7a6712bd44b3a5e06681d31461473b4aafb0db603"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-5-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff and guidance read.** I read the assigned frozen diff exactly as `[1,150]`, `[151,300]` and `[301,437]`: all 437 lines across 5 files. I also read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the reference for the `master` target) and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Every new production path has a test.** The paths are in `core/src/main/scala/org/apache/spark/ml/Serializer.scala`: `typeToSerializer` (L59–60), `PipelineSerializer` (L258–270) and `PipelineArraySerializer` (L272–296: `withSession` L278–285, `write` L287–291, `read` L293–295). Coverage:
  - **Assigned session (`Some`) for both serializers:** `ValidateComplexParamSerializer.scala` L156–213 runs an `array` × `active` matrix. `TransformerParam` covers the single-stage serializer and the fixture's `ComplexParam[Array[PipelineStage]]` covers the array serializer.
  - **Null session (`None`):** L215–258 covers falling back to the active session (`active=true`) and to the default session (`active=false`).
  - **`overwrite` false then true:** L180–196. Changing the payload proves the overwrite actually replaced the artifact.
  - **Session restored after a write failure:** L198–210 and L246–252.
  - **Session restored after a read failure:** the `SecurityException` cases in `VerifyUDFTransformerPersistence.scala` L76–81.
- [x] **Tests fail if the fix is removed.** Each of these changes breaks at least one case:
  - removing the session from `typeToSerializer`;
  - dropping `setActiveSession`;
  - dropping the `finally`;
  - always clearing the active session, or setting it to the default;
  - using `Some(spark)` instead of `Option(spark)`.

  The writer tests set `ConfigKey` only on the assigned session. `TestBase.scala` L55 turns legacy deserialization on in the default SparkConf, so even the UDF case `allowed=false, active=false` catches a fallback to the default session. During each operation the active session differs from `expectedActive` in at least one case per family, so restoration is actually checked.
- [x] **Assertions and fixtures are sound.** The fixtures use real Spark `Pipeline` persistence, with session-checking `require` guards (`ComplexParamTestFixtures.scala` L36–46), instead of mocks. Exception types and messages are checked. Active and default sessions are asserted after every success and failure.
- [x] **Tests are isolated.** `withActiveSession` (fixtures L15–20) restores the active session in `finally`. The default session is restored in `finally` (L254–256). Each case writes to its own tmpDir path. Sessions come from `newSession()` and are never closed, which is correct because closing would stop the shared SparkContext. `build.sbt` sets `Test / parallelExecution := false`.
- [x] **No breakage in repository-wide stage checks.** The root `FuzzingTest` (`src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` L420–431) can see core test classes because root depends on `core % "test->test"`. It skips class names starting with `Testable` (L424), so the new no-arg constructor and the new `TestableNestedParamTest` need no fuzzers or exemptions. `TestEstimatorBase` moved files but keeps the fully qualified name used in existing exemptions.
- [x] **Scope and serialized format are unchanged.** Scala arrays are invariant, so `TransformerArrayParam` and `EstimatorArrayParam` still use `ObjectSerializer` and are unaffected. `EstimatorParam` and `PipelineStageParam` take the same `PipelineStage` path that `TransformerParam` already covers. Writes still go through Spark's `Pipeline` writer and reads through `Pipeline.read`, so existing `pyLoadLine`/`rLoadLine` consumers keep working.
- [x] **Considered and not raised:**
  - No test that `overwrite=false` refuses an existing nested path. This behavior is unchanged from before, and the top-level `MLWriter` already rejects an existing root.
  - The case with neither an active nor a default session. `getOrCreate()` creates and activates a session exactly as the old lazy `Pipeline` load and write did.
  - The public no-arg constructors are not called directly. They only pass `None`, and the null-session tests exercise that same state.
- [x] **Validation evidence is the caller's, not mine.** The caller reports compile, Test/compile, 31 tests with 0 skips, and scalastyle passing. The 31 matches my count: 20 in `ValidateComplexParamSerializer` plus 11 in `VerifyUDFTransformerPersistence`. `FuzzingTest` was not in that run; I checked it by reading the source only.
- [ ] **Tests not run by this reviewer.** The review contract is read-only, so I executed nothing and accessed no cloud resources.