---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 6
timestamp: "2026-11-10T09:02:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "664a36b28c25e4ff9d6dbbb0636eae8f176b6126d2bf60da80abdb3dee663506"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-13-stage-attempt-1-review-6-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 455 lines of the assigned frozen diff, covering 5 files, in the required order: `[1,150]`, `[151,300]`, `[301,450]` and `[451,455]`. Every chunk came back complete. The changed content matches the current `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, `ComplexParamTestFixtures.scala`, `ValidateComplexParamSerializer.scala` and `VerifyUDFTransformerPersistence.scala`.
- [x] **Guidance and versions:** I applied `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `references/branch-spark3p5.md` (master target), `.github/skills/code-review/SKILL.md` and `.github/skills/scala-code/SKILL.md`. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0).
- [x] **Performance (`Serializer.scala:258-296`):**
  - Each nested save or load adds two small object allocations and two thread-local writes. It adds no I/O and no Spark jobs.
  - `SparkSession.builder().getOrCreate()` (line 279) runs only when no session is assigned. It replaces the same lookup that `MLWriter`/`MLReader` already did, so it adds no cost.
  - `PipelineSerializer` now delegates to `PipelineArraySerializer`, so there is one code path. Both still save the same single-stage `Pipeline` wrapper, so the on-disk format is unchanged and `TransformerParam.rLoadLine`'s `ml_stages(...)[[1]]` still works.
- [x] **Session restoration (`withSession`, lines 278-285):**
  - It saves `SparkSession.getActiveSession`, which reads only the thread-local value and never falls back to the default session.
  - A `finally` block restores the saved session after both success and failure. The default session is never changed.
  - This follows the same pattern as Spark's private `SparkSession.withActive`.
  - It resolves the session before saving `previous`. When no session exists at all, the newly created session stays active, which is the same as the old behaviour.
  - It only changes thread-local state, and each serializer holds an immutable `Option`, so concurrent use is safe.
- [x] **Documentation accuracy:**
  - The new Scaladoc at `ComplexParamsSerializer.scala:66,174` and `Serializer.scala:71` matches the code: these older overloads call `SparkSession.builder().getOrCreate()`.
  - The doc on the `SparkSession` constructors (lines 261 and 275) matches the code: a null session becomes `None`, and the fallback session is chosen at read/write time.
  - The comment at line 281 matches how Spark 3.5's `Pipeline.SharedReadWrite` works: it creates child writers with `stage.write` and child readers with `loadParamsInstanceReader`, without passing a session. This comes from my knowledge of Spark's source, not from this repository.
- [x] **Compatibility:**
  - The public no-argument constructors still exist (`def this()`), and `this(spark: SparkSession)` is a new addition. The primary constructor is private.
  - The `write`/`read` signatures are unchanged, and `writeMLWritable` is kept.
  - `typeToSerializer` (lines 59-60) passes its session through. `ComplexParam.save`/`load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala`) pass their session on, so assigned sessions now reach nested stages. A null session still gets the old fallback.
- [x] **Observability:** A low-level serializer doesn't need new logging. Exceptions propagate unwrapped, and Spark's `Instrumentation` already logs save/load failures. The `SecurityException` message still names `spark.synapseml.legacy.allowUnsafeJavaDeserialization`, which the tests check.
- [x] **Naming:** `delegate`, `withSession`, `withActiveSession`, `setNested`/`getNested`, and the test names that include `array`/`active`/`allowed` are clear and unique. The moved fixtures keep their fully qualified names, so modules that depend on `core % "test->test"` are unaffected.
- [x] **Tests catch regressions:**
  - `ValidateComplexParamSerializer.scala:157-270` and `VerifyUDFTransformerPersistence.scala:46-84` cover every combination of single-stage/array and with/without an active session.
  - They cover success, overwrite, `overwrite=false` throwing `IOException` while keeping the saved payload (line 200), and session restoration after write and read failures.
  - `TestBase.scala:55` enables legacy deserialization on the base session. So if nested reads fell back to the active or default session, the `allowed=false` cases would load successfully and fail the test.
- [x] **Test isolation:**
  - `build.sbt` sets `Test / parallelExecution := false`, so changing the default session in the tests is safe, and a `finally` block restores it.
  - `withActiveSession` restores the active session.
  - `TestBase.tmpDir` (lines 161-164) is a separate temporary directory per suite, so the non-overwrite save paths can't collide.
- [x] **Style and scope:** The new file has the license header and explicit imports. By inspection, every changed line is 120 characters or fewer. The diff adds no RDD use, no `target/` edits, no dependency, pipeline, workflow or release changes, and no native AIPrompt or optional-recovery code.
- [ ] **Not executed:** I didn't run tests, compilation or scalastyle; this was a read-only review. I did not check the caller-reported results (31 tests with 0 skips, compile, Test/compile and both scalastyle checks). I also didn't search the repository's documentation because the read rules forbid discovery tools, and the file list includes no user-facing docs.