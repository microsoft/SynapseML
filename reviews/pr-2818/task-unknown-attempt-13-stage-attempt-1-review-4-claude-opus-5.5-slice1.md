---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 4
timestamp: "2026-11-10T04:45:43Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "9448a1290084bf1ed4bf6a4dfbe96fa4c15e3377f8b1a7ed15895c2e9459bdef"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-13-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read all 455 lines of the assigned frozen diff in the required order: [1,150], [151,300], [301,450], [451,455]. The current source of all five inventory files matches the diff's post-image.
- [x] **Write path** (`core/src/main/scala/org/apache/spark/ml/Serializer.scala` 59-60, 272-291): I traced the session from `ComplexParamsWriter.saveImpl` through `saveComplexParams(..., sparkSession)`, `ComplexParam.save` and `typeToSerializer` into `PipelineSerializer(sparkSession)` and `PipelineArraySerializer(sparkSession)`. From there, `withSession` calls `Pipeline.write.session(session)`.
  - Spark 3.5's Pipeline creates child writers without a session. They fall back to `getOrCreate()`, which returns the active session that `withSession` sets.
  - `if (overwrite) writer.overwrite()` mutates the same writer instance, so it behaves the same as the old `writeMLWritable`.
  - When the path already exists, the `IOException` comes from `MLWriter.save`/`handleOverwrite`. That runs inside `withSession`, before `saveImpl`.
- [x] **Read path** (`Serializer.scala` 269, 293-294): `ComplexParamsReader.load` passes its `sparkSession` to `getAndSetComplexParams`, which calls `Pipeline.read.session(session)`. Child readers come from `loadParamsInstanceReader` without a session and end up on the assigned active session. `delegate.read(path)(0)` is equivalent to the old `getStages(0)`, so there is no off-by-one.
- [x] **Session restoration** (`Serializer.scala` 278-285):
  - `previous` is captured after the fallback session is resolved. `getOrCreate()` only changes sessions when no live session exists, and the old `Pipeline.load`/`stage.write` path had the same side effect.
  - `finally` restores both `Some` and `None` correctly, and nested calls restore in last-in, first-out order.
  - The default session is never changed. A stopped "previous" session is restored as it was.
- [x] **Null handling and public API** (`Serializer.scala` 258-276):
  - A null session becomes `None` through `Option(null)`. The fallback is resolved on each call, not at construction.
  - Overload resolution is unambiguous: `Option` arguments only match the private constructor, and `SparkSession` arguments only match the public one.
  - Public no-argument constructors and the `write`/`read` signatures are unchanged.
  - The on-disk format is unchanged: still a `Pipeline` directory under each param path.
- [x] **Tests can tell sessions apart:**
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` sets the legacy-deserialization flag to `true` in SparkConf. As a result, the allowed=false/active=false case and both active=true cases in `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/VerifyUDFTransformerPersistence.scala` (lines 46-84) fail if the active or default session is used instead of the assigned one.
  - In `core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala` (lines 157-225 and 227-270), a uid-keyed `require` rejects any session other than the intended one.
  - The overwrite=false case checks that the earlier (4,5,6) payload is preserved.
  - The case count (20 + 11 = 31) matches the caller's evidence.
- [x] **Cross-file test contracts:**
  - In `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala`, `pipelineStages` excludes classes whose simple name starts with `Testable`. So the new `TestableNestedParamTest` and the added no-arg constructor create no new fuzzing requirements.
  - The fully qualified name of `TestEstimatorBase` is unchanged, so existing FuzzingTest exemptions still apply.
  - `build.sbt` sets `Test / parallelExecution := false`. The temporary change to the global default session in the unassigned-session tests is restored in `finally` and cannot race other suites.
- [x] **Types and style:** the `if` expressions have type `PipelineStage` before the `asInstanceOf` casts. `Option.empty[SparkSession].orNull` avoids a `null` literal. The longest added lines are within 120 characters (`ValidateComplexParamSerializer.scala:218` is 119; `Serializer.scala:287` is 112).
- [ ] Not done: I did not run any tests. This was a read-only review, and pass/fail status comes only from the caller's local evidence.
- [ ] Not done: I did not list other callers of the three-argument `private[ml]` helpers, because the read profile forbids searching. The new Scaladoc on those helpers is accurate for the code shown.