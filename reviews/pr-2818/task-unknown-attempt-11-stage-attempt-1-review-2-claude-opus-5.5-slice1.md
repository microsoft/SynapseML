---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 2
timestamp: "2026-11-09T05:35:33Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "0caa2d22d05b1027e81632ca7adb9d8a2db040ccf630d114641d7c9e3624e87d"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 437 lines of the assigned frozen diff in the required ranges: [1, 150], [151, 300] and [301, 437]. The checked-out versions of all five files in the source inventory match the diff's result.
- [x] **Guidance and versions:** I applied `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (master target) and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0). The change contains no code tied to a particular Spark version, so it fits master.
- [x] **Consistent session handling:** In `core/src/main/scala/org/apache/spark/ml/Serializer.scala` L59–60, `typeToSerializer` now passes `sparkSession` to `PipelineSerializer` and `PipelineArraySerializer`. This matches how it already passes the session to `DFSerializer`, `ObjectSerializer` and `FilteredObjectSerializer`.
  - When no session is assigned, the code falls back to `SparkSession.builder().getOrCreate()` (L279).
  - That is the same fallback the older overloads use in `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` L66–69 and L174–177. Those overloads now document that callers with an assigned session must pass it explicitly.
- [x] **Public API compatibility** (`Serializer.scala` L258–296):
  - The public no-argument constructors are kept as auxiliary `this()` constructors.
  - The `write`/`read` signatures and the `Serializer[...]` parent classes are unchanged.
  - The new `(SparkSession)` constructors only add to the API.
  - The `Option` primary constructors are `private`, so the public Scala constructors are only `()` and `(SparkSession)`.
  - Nothing changes in Params, codegen/`Wrappable`, dependencies, `pipeline.yaml` or workflows.
- [x] **Saved format unchanged:** `PipelineSerializer` now passes `Array(stage)` to `PipelineArraySerializer` and reads back element `(0)` (L264–269). This produces the same single-stage `Pipeline` layout as before. Writes and reads still store a Spark `Pipeline` at the same path, now through `.write.session(session)` and `Pipeline.read.session(session)` (L287–294).
- [x] **Where the session is set:**
  - The temporary active session is limited to the one serializer branch that creates nested `MLWriter`/`MLReader` objects.
  - The comment at L281 explains why: Spark's `Pipeline` does not pass its session to child readers and writers.
  - `withSession` (L278–285) picks the session once and sets it as the active session for the current thread. In `finally` it restores the previous active session, or clears it if there was none.
  - It never changes the default session, and nested calls restore in reverse order.
  - Other pipeline-related params I checked do not open a second nested-writer path:
    - `TransformerParam` and `PipelineStageParam` go through `PipelineSerializer`.
    - `TransformerArrayParam` and `EstimatorArrayParam` use `Array[...]` types, which Scala treats as invariant, so `typeToSerializer`'s `<:<` checks do not route them to the pipeline serializers.
- [x] **Test fixtures:**
  - The fixtures moved to `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParamTestFixtures.scala` keep the same package and fully qualified names. This matters because `build.sbt` publishes test artifacts and other modules depend on `core % "test->test"`.
  - The new and updated test stages follow the `AGENTS.md` convention: a `uid` constructor plus a no-argument constructor with a random UID.
  - Their companion objects extend `ComplexParamsReadable`, like the existing `ComplexParamTest` and `MixedParamTest`.
- [x] **Tests separate correct from incorrect behaviour and clean up after themselves:**
  - `ValidateComplexParamSerializer.scala` L156–213 and `VerifyUDFTransformerPersistence.scala` L46–84 would fail if the code used the active or default session instead of the assigned one. The failure would come from `require(...)` ("Incorrect writer/reader session") or from a deserialization-policy `SecurityException`.
  - Both success and failure paths check that the active and default sessions are restored.
  - `withActiveSession` restores state in `finally`, and so does the default-session test at L215–258.
  - `build.sbt` sets `Test / parallelExecution := false`, so changing the JVM-wide default session cannot affect other suites.
- [x] **Coding conventions:**
  - The new file has the license header.
  - Imports are explicit and in the `our, scala, java, other` order.
  - Lines are 120 characters or fewer.
  - Scalastyle's ban on the literal `null` (NullChecker) is respected: main code uses `Option(spark)`, and the test at L233 uses `Option.empty[SparkSession].orNull`.
  - No RDD code is added, and the change adds no AIPrompt, recovery, dependency, release or unrelated refactoring work.
- [x] **Validation evidence (supplied by the caller, not run by me):** compile, Test/compile, 31 tests with 0 skips, and main and test scalastyle all passed on JDK 11 / Spark 3.5.0 / Scala 2.12.17, with no live service calls. Remote CI remains a separate gate.
- [ ] **Not done — repository-wide search for other callers or subclasses** of `PipelineSerializer`, `PipelineArraySerializer` and `Serializer.writeMLWritable`. The required read profile does not allow search tools. My compatibility conclusion rests on the unchanged public signatures listed above.