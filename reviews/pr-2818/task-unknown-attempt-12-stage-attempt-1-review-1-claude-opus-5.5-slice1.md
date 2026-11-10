---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 1
timestamp: "2026-11-09T21:28:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "2c1d942b4c2eee76f61d022f214afe2d234254d1ebffc94abff708716ea22b87"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-12-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read every line of the assigned frozen diff (437 lines) using exactly the ranges `[1,150]`, `[151,300]` and `[301,437]`. The current contents of all five changed files match the diff.
- [x] **Repository rules:** I applied `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. `build.sbt` pins Spark 3.5.0 and Scala 2.12.17, and `environment.yml` pins pyspark 3.5.0. The change adds no RDD code, dependencies, release changes, `target/` edits, workflow changes or `pipeline.yaml` changes.
- [x] **The session now reaches nested stages:**
  - `core/src/main/scala/org/apache/spark/ml/Serializer.scala:59-60` passes the caller's session to both pipeline serializers.
  - `ComplexParam.save`/`load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala:23-30`) receive that session from `ComplexParamsSerializer.scala:40` and `:167`.
  - In Spark 3.5, `Pipeline` creates child writers and readers without passing its session on. To work around this, `withSession` (`Serializer.scala:278-285`) makes the chosen session active only while the `Pipeline` call runs.
  - A `finally` block then puts back the previous active session, or clears it if there was none. This happens on success and on failure, and nested calls unwind in the correct order.
- [x] **Saved format and public API are unchanged:**
  - Writes still save `new Pipeline().setStages(...)` with the same overwrite behavior (`Serializer.scala:287-291`).
  - Reads still return the pipeline's stages, or the first stage (`:269`, `:293-295`). The on-disk layout and the Python/R `Pipeline.load` loaders in `PipelineStageWrappable` (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/EstimatorParam.scala`) are unaffected.
  - The public no-argument constructors remain (`:259`, `:273`). The new `SparkSession` constructors only add API.
  - The `Option` primary constructor is private, so callers' `new X(session)` and `new X(null)` resolve unambiguously.
  - The `typeToSerializer` and `writeMLWritable` signatures are unchanged. The edits at `ComplexParamsSerializer.scala:66,174` are documentation only.
- [x] **Which params are affected:** `TransformerParam`, `PipelineStageParam`, `EstimatorParam` and `ComplexParam[Array[PipelineStage]]` reach the changed serializers. `TransformerArrayParam` and `EstimatorArrayParam` use invariant array types, so they never matched the pipeline serializers. Before and after this change, they go through `ObjectSerializer` with the caller's session.
- [x] **Security:** Nested readers now follow the assigned session's `spark.synapseml.legacy.allowUnsafeJavaDeserialization` setting instead of the active or default session's. This closes a gap where the two settings could disagree. The change adds no deserialization path, and the class filtering for `ByteArrayParam` and `UDFParam` is unchanged.
- [x] **Tests (inspected, not run):**
  - The tests at `ValidateComplexParamSerializer.scala:156-213` and `VerifyUDFTransformerPersistence.scala:46-84` would fail on the old code. In those tests, the assigned session's config key and legacy flag differ from the active or default session's; `TestBase.scala:55` sets the flag to `true` on the base session.
  - They check that the active and default sessions are restored after:
    - a successful nested write and read,
    - an overwrite,
    - a write failure (`IllegalArgumentException`),
    - a read failure (`SecurityException`).
  - `ValidateComplexParamSerializer.scala:215-258` covers the fallback when the session is null, and restores the default session in `finally`.
  - The test helper at `ComplexParamTestFixtures.scala:15-20` also restores the active session in `finally`.
- [x] **Test isolation:**
  - `build.sbt:282` sets `Test / parallelExecution := false`, so suites that change the global default session do not run at the same time.
  - `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala:424` skips stages whose names start with `Testable`. The new fixtures therefore need no fuzzing exemptions.
  - The fixtures moved to a new file keep their fully qualified names.
- [x] **Considered and dismissed:**
  - If no session is passed and there is neither an active nor a default session, `getOrCreate()` creates a session and makes it active before the code records the previous one. The old write path behaved the same way. In normal writer and reader persistence, a session is always resolved before this code runs.
  - Threads started during the call inherit the temporary active session. Spark's own `withActive` behaves the same way.
  - In Spark 3.5, `Pipeline` writes its own metadata using SparkContext's Hadoop configuration. That is Spark's behavior and predates this change.
- [ ] **Build and test runs:** Not performed, because this review is read-only. The caller-supplied evidence (compile, `Test/compile`, 31 tests with 0 skips, scalastyle) and the Azure validation were noted but not reproduced.