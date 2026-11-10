---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 4
timestamp: "2026-11-09T07:05:18Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "2dc25e0bb5d0106513816697618dc7cfe51dfc2eb7791acfbf4286dcabfcdd69"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read every line of the assigned frozen diff using the required chunks [1, 150], [151, 300] and [301, 437]. The current contents of all five files match the diff's new version: `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, and the three test files under `core/src/test/scala/...`.
- [x] **Constructor selection in `typeToSerializer`** (`Serializer.scala` lines 59–60): `new PipelineSerializer(sparkSession)` and `new PipelineArraySerializer(sparkSession)` each resolve to the public `this(spark: SparkSession)` constructor. The private `Option` constructor can't be called from `object Serializer`, and the argument's declared type is `SparkSession`. `Option(spark)` turns a null session into `None`, so code that passes no session still works as before. The order of the type checks is unchanged, so `Array[PipelineStage]` still uses the array serializer.
- [x] **Session swap in `withSession`** (`Serializer.scala` lines 278–285):
  - The session is resolved first, then `previous` is captured, then the active session is set, and `try`/`finally` restores it. A nested call restores the outer session before the outer call restores the caller's.
  - Only the thread's active session changes, never the default session.
  - If `spark` is `None` and only a default session exists, `getOrCreate()` returns the default without making it active, so the restore step clears the active session. The unassigned `active=false` tests check this.
  - If no session exists at all, `getOrCreate()` creates one and makes it active before `previous` is captured. The old code path through `MLWriter.sparkSession` did the same, so this is not a regression.
- [x] **Write and read paths** (lines 258–270 and 287–295):
  - `.write.session(session)` and `if (overwrite) writer.overwrite()` together match the removed `Serializer.writeMLWritable` call, apart from the explicit session. `overwrite()` changes the writer in place, so ignoring its return value is safe.
  - `delegate.read(path)(0)` is the same as the old `getStages(0)`, so there is no off-by-one change.
  - `Array(stage)` has type `Array[PipelineStage]`.
  - The saved Pipeline layout (metadata plus `stages/`) is unchanged, so the serialized format is preserved.
  - In Spark 3.5, `Pipeline`'s shared save/load code writes and reads child stages without passing the session on. Child readers and writers fall back to `getOrCreate()`, which returns the active session. Setting the active session is therefore what carries the assigned session into nested `ComplexParamsWriter`/`ComplexParamsReader`. The code comment on line 281 is accurate.
- [x] **Public JVM signatures:** Both classes keep their public no-argument constructors and the same `write`/`read` signatures and bridge methods. The diff only adds a constructor and private members. The `ComplexParamsSerializer.scala` changes (lines 66 and 174) are comments only. They correctly describe the active/default fallback that `core/src/test/scala/org/apache/spark/ml/ComplexParamsTestHelper.scala` exercises.
- [x] **Cross-file contracts:**
  - `ComplexParam.save/load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala`) passes its session to `typeToSerializer`.
  - `TransformerParam` is a `ComplexParam[Transformer]`, so it uses `PipelineSerializer`. The test fixture's `ComplexParam[Array[PipelineStage]]` uses `PipelineArraySerializer`.
  - `UDFTransformer` (`core/src/main/scala/com/microsoft/azure/synapse/ml/stages/UDFTransformer.scala`) loads its UDF param through `readFromHDFS(session, …)`, which applies `LegacyObjectDeserializationConfig` on the session passed in.
- [x] **The tests would catch a broken fix:**
  - In the fixtures, the `require` check fails on the base `spark` session because it lacks the config key. Without the fix, both the single-stage and array branches of the nested writer/reader tests (`ValidateComplexParamSerializer.scala` lines 156–213) would fail.
  - The overwrite loop writes a different payload, so it actually exercises `writer.overwrite()`.
  - The unassigned-session tests (lines 215–258) would catch a `Some(null)` mistake, which would clear the active session and fall back to `spark`.
  - The UDF reader tests (`VerifyUDFTransformerPersistence.scala` lines 46–84) give the active and assigned sessions opposite deserialization settings. The `active=true` cases therefore fail in both directions if the wrong session is used.
  - Every failure path asserts that the active and default sessions are restored.
- [x] **Test hygiene:**
  - Each parameter combination writes to its own path.
  - `tmpDir` is a fresh temporary directory per suite (`TestBase.scala`).
  - `Test / parallelExecution := false` in `build.sbt` means the temporary change to the global default session can't affect other suites, and the test restores it in `finally`.
  - `TestEstimatorBase` keeps its fully qualified name, so the exemptions in `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` still apply. FuzzingTest skips the new `Testable*` fixtures by name.
  - The moved classes stay in the same package, and unused imports were removed correctly.
  - The suites contain 20 + 11 = 31 tests, which matches the supplied evidence.
- [x] Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17) and `environment.yml` (pyspark 3.5.0). For the `master` target I followed `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `AGENTS.md` and `.github/skills/code-review/SKILL.md`. The diff adds no RDD use, dependencies, release changes or unrelated refactoring.
- [ ] I did not run any tests; this review is read-only. The compile, test and scalastyle results come from the evidence you supplied.
- [ ] I could not search the whole repository for other places that call `new PipelineSerializer()`, `new PipelineArraySerializer()` or `Serializer.writeMLWritable` directly, because the required read profile doesn't allow search tools. Any such callers keep their previous active/default-session behavior, so this limits coverage but cannot introduce a regression.