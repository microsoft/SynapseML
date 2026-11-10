---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 4
timestamp: "2026-11-09T23:11:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "df3fcf74c92ccb8f73788d5109b203f24b2db1d48337390911698aab4eba9871"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-12-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read every line of the assigned frozen diff using the required ranges: [1,150], [151,300] and [301,437]. I compared it with the current source of all five inventory files: `core/src/main/scala/org/apache/spark/ml/{Serializer,ComplexParamsSerializer}.scala`, `core/src/test/scala/.../core/serialize/{ComplexParamTestFixtures,ValidateComplexParamSerializer}.scala` and `core/src/test/scala/.../stages/VerifyUDFTransformerPersistence.scala`.
- [x] I read `AGENTS.md`, `.github/skills/code-review/SKILL.md`, `.github/skills/synapseml-branches/SKILL.md` and `references/branch-spark3p5.md`. I took the versions from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Write path:** I traced `ComplexParamsWriter.saveImpl` → `saveComplexParams(..., sparkSession)` → `ComplexParam.save` → `Serializer.typeToSerializer`. That function now passes `sparkSession` to `PipelineSerializer` and `PipelineArraySerializer` (`Serializer.scala`, around lines 59–60). The call then reaches `withSession`, which uses `.write.session(session)` and makes `session` the active session. Spark 3.5's Pipeline creates child writers with `stage.write`, and they get their session from `getOrCreate()`, which returns the active session. So nested children now use the assigned session.
- [x] **Read path:** It mirrors the write path. `ComplexParamsReader.load` → `getAndSetComplexParams(..., sparkSession)` → `Pipeline.read.session(session)`. Spark creates the child readers without a session, so they also resolve to the active session.
- [x] **`withSession` logic:**
  - `getOrElse` and `fold` evaluate their fallback lazily, so an assigned session never calls `getOrCreate()`.
  - `previous` is captured before `setActiveSession`.
  - The `finally` block restores or clears the active session on both success and failure.
  - The default session is never changed.
  - Spark's `getOrCreate` sets the active session only when it creates a new session. Restoring to that new session matches the previous behaviour and Spark's own contract.
- [x] **Null session:** `Option(null)` becomes `None`, which falls back to the active or default session. This keeps the old `typeToSerializer(tpe, null)` behaviour for pipeline types. The "unassigned" tests cover it with and without an active session.
- [x] **Saved format and overwrite:** The output is still a `Pipeline` saved at the same path. `delegate.read(path)(0)` reads the same stage as the old `getStages(0)`, so there is no off-by-one change. Overwrite behaves the same because `writer.overwrite()` changes that same writer.
- [x] **JVM compatibility:**
  - Both classes keep their public no-argument constructors.
  - The `(SparkSession)` constructor is new, and the `Option` primary constructor is private.
  - The `write` and `read` signatures are unchanged.
  - `writeMLWritable` and the three-argument `saveComplexParams` and `getAndSetComplexParams` overloads remain. Only their doc comments changed.
- [x] **Other pipeline-typed params:** `TransformerParam`, `PipelineStageParam` and `EstimatorParam` don't override `save` or `load`, so they all use `typeToSerializer` and get the fix. `TransformerArrayParam` and `EstimatorArrayParam` use `ObjectSerializer(session)` because Scala arrays are invariant. That serializer already uses the session, and the PR doesn't change it.
- [x] **Tests fail without the fix:**
  - Without the fix, the default and active `spark` sessions lack `ConfigKey`, so the nested writer and reader tests would fail.
  - The UDF tests set the deserialization policy differently on the active and assigned sessions, so they would also fail.
  - Session restoration on failure is checked for writes (`IllegalArgumentException`) and reads (`SecurityException`).
  - Test names and file paths are unique.
  - The 20 tests in `ValidateComplexParamSerializer` plus 11 in `VerifyUDFTransformerPersistence` add up to the 31 cases in the supplied evidence.
- [x] **Fixture move and reflection-based suites:**
  - Moving the fixtures keeps the same package and the `TestEstimatorBase` name, so the existing `FuzzingTest` exemptions still match.
  - Root `src/test/scala/.../fuzzing/FuzzingTest.scala` skips classes whose names start with `Testable` when building `pipelineStages`. The new no-argument constructors therefore don't add unexempted stages.
  - The new companion objects follow the same pattern as the existing `ComplexParamTest` companions, which `readers` already instantiates.
- [x] **Global session state in tests:** `build.sbt` sets `Test / parallelExecution := false`. The "unassigned" tests temporarily change the global default session, but they can't race other suites, and `try/finally` restores it.
- [x] I ran no tests or commands. I rely on the caller's evidence for compile, Test/compile, 31 tests and scalastyle on this tree.
- [ ] Not checked: I did not search the repo for other callers of the three-argument `saveComplexParams` or `getAndSetComplexParams` overloads, or for direct `new PipelineSerializer()` calls. The required read profile forbids search tools. Those call sites are outside the diff and keep their existing behaviour.