---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 4
timestamp: "2026-11-09T04:05:37Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "26a7ed785cb23620f7f9f0d7ae60b7d41b0547ee87e39cc5920c402edfe91946"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-10-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read the assigned frozen diff in exactly the required ranges: [1, 150], [151, 300] and [301, 393]. All 393 lines came back without truncation. The diff touches 2 main files and 3 test files.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. The target is `master`. `build.sbt` sets Spark 3.5.0 and Scala 2.12.17, and `environment.yml` sets pyspark 3.5.0.
- [x] **Session data flow:**
  - `core/src/main/scala/org/apache/spark/ml/Serializer.scala:59-60` now passes the caller's session to `PipelineSerializer` and `PipelineArraySerializer`.
  - Write path: `ComplexParamsSerializer.scala:40` → `ComplexParam.scala:23-26` → the pipeline serializers.
  - Read path: `ComplexParamsSerializer.scala:167` → `ComplexParam.scala:28-30` → the pipeline serializers.
  - `TransformerParam`, `PipelineStageParam` and `EstimatorParam` go to `PipelineSerializer`. `ComplexParam[Array[PipelineStage]]` goes to `PipelineArraySerializer`.
  - `TransformerArrayParam` and `EstimatorArrayParam` are not subtypes of `Array[PipelineStage]` (Scala arrays are invariant), so they keep the existing `ObjectSerializer` path, which already uses the session it is given.
- [x] **`withSession` logic (`Serializer.scala:278-285`):**
  - The fallback arguments to `getOrElse` and `fold` are evaluated lazily, so `getOrCreate()` runs only when no session was assigned, and only one delegate is built (`:264`).
  - The previous active session is saved before it is changed, and the new one is set before the `try`.
  - The `finally` block puts back a previous session with `setActiveSession`, or calls `clearActiveSession` if there was none. The default session is never changed.
  - Errors are covered too, including the stage check that runs when `Pipeline.write` is built (`:288`). Nested calls restore sessions in the correct reverse order.
- [x] **Spark 3.5 contracts:**
  - Spark's `Pipeline` creates child writers and readers without passing its session down, so the comment at `:281` is accurate. Those children fall back to `getOrCreate()`, which checks the thread's active session first. This is why setting the active session carries the assigned session through.
  - `MLWriter.overwrite()` changes the writer in place and returns it, so ignoring the return value at `:289` still keeps the overwrite flag.
- [x] **Indexing and saved format:**
  - `PipelineSerializer.read` takes element `(0)` (`:269`), the same as the old `getStages(0)`.
  - `write` wraps the stage as `Array(stage)` (`:267`), which produces the same single-stage `Pipeline` layout as before. Params JSON and paths do not change.
  - The edits at `ComplexParamsSerializer.scala:66,174` are doc comments only.
- [x] **API compatibility:**
  - The public no-argument constructors are kept through `def this()` (`:259,273`).
  - The new public `(SparkSession)` constructor only adds to the API.
  - The private `(Option)` constructor does not accept a `SparkSession` argument, so overloads are not ambiguous.
  - `write` and `read` signatures are unchanged. A null session falls back to the active or default session, which keeps the old tolerance of null.
- [x] **Unassigned fallback order:** `getOrCreate()` runs before the previous session is saved. This only matters when no session exists at all. In that case Spark creates and activates a session, exactly as the old `MLWriter`/`MLReader` path did, so this is not a regression.
- [x] **Tests detect the old bug:**
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala:55` turns on legacy deserialization for the default session.
  - In `VerifyUDFTransformerPersistence.scala:46-84`, the `allowed=false, active=None` case fails if nested readers use the default session. The cases where the active session has the opposite setting fail if nested readers use the active session.
  - `ValidateComplexParamSerializer.scala:156-189` fails on the old code because neither `spark` nor the default fallback has `ConfigKey` set.
  - `:191-214` shows that a null session falls back to the active session rather than the default.
  - Both failure paths (write with `IllegalArgumentException`, read with `SecurityException`) check that the active and default sessions are restored.
- [x] **Test hygiene:**
  - Test names are unique. Each suite gets its own `tmpDir` from `Files.createTempDirectory`, and each case writes to its own path.
  - Sessions from `newSession()` are correctly left open, since they share one SparkContext.
  - `withActiveSession` (`ComplexParamTestFixtures.scala:15-20`) restores the session in `finally`.
  - Imports match what each file uses, and no line exceeds 120 characters.
- [x] **Other suites:**
  - In `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala`, `pipelineStages` skips classes whose names start with `Testable`, so the new test classes need no exemptions.
  - The moved `TestEstimatorBase` keeps its fully qualified name, so its existing exemptions still apply.
  - `SerializationFuzzing.testSerializationHelper` in `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/Fuzzing.scala` calls `write.overwrite()`, which runs the `overwrite = true` branch.
- [ ] **Tests not run here:** this review is read-only, so I did not run any tests. I relied on the caller's evidence: compile, Test/compile, 29 tests with 0 skips (18 in `ValidateComplexParamSerializer` and 11 in `VerifyUDFTransformerPersistence`), and scalastyle. `FuzzingTest` and the serialization-fuzzing suites were not part of that evidence, so they remain a remote-CI gate.