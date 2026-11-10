---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 5
timestamp: "2026-11-10T05:24:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3467cdc8ba44e3cf2404ac9e46c8d2005c67c3a9e113bf8712d5dbdee12207ff"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-13-stage-attempt-1-review-5-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff and source match.** I read all 455 lines of the assigned frozen diff in the required ranges: [1,150], [151,300], [301,450] and [451,455]. The current `core/src/main/scala/org/apache/spark/ml/Serializer.scala` matches the diff at lines 59–60 and 258–296. The current `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` also matches. I opened only the exact files under review and did not open any review artifacts.
- [x] **Call path.** I traced the path from `ComplexParam.save/load` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala` lines 23–30) to `Serializer.typeToSerializer` (lines 59–60), then to `PipelineSerializer(session)` and `PipelineArraySerializer.withSession` (lines 278–285).
  - `TransformerParam`, `EstimatorParam` and `PipelineStageParam` all take the single-stage branch.
  - The custom `ComplexParam[Array[PipelineStage]]` fixture (`ComplexParamTestFixtures.scala` lines 57–58) covers the array branch.
  - `TransformerArrayParam` and `EstimatorArrayParam` don't match `Array[PipelineStage]` because Scala arrays are invariant. Their existing routing is unchanged by this diff.
- [x] **The session check catches regressions.** The `require` checks in `ComplexParamTestFixtures.scala` (lines 38 and 43) accept only the assigned session. The fallback session (`spark`, active or default) lacks the config key.
  - The positive write and read checks in `ValidateComplexParamSerializer.scala` (lines 157–225) fail if any of these is undone: the active-session switch, the session handoff in `typeToSerializer`, or passing the session to `PipelineSerializer`'s delegate.
  - Removing `.session(session)` alone (lines 288 and 294) changes no behaviour while the active session is switched, so it doesn't need its own assertion.
- [x] **Session restored on success and failure.** Active- and default-session assertions follow every successful call and every failure:
  - the `IOException` (line 200);
  - the `IllegalArgumentException` cases (lines 211–222 and 259–264);
  - the nested-read `SecurityException` (`VerifyUDFTransformerPersistence.scala` lines 76–81).

  These run both with and without a caller active session. Faulty restores (always clearing, never restoring, restoring only on success, or restoring the default) would fail.
- [x] **Overwrite behaviour (line 289).** The test runs this sequence (`ValidateComplexParamSerializer.scala` lines 186–208):
  - overwrite=false on a new path succeeds;
  - overwrite=true on the existing path replaces the payload;
  - overwrite=false on the existing path throws an `IOException` ("already exists") and leaves payload [4,5,6] intact.

  Making overwrite unconditional, never applied, or inverted would each fail. This matches the caller-reported mutation check, which I did not re-run.
- [x] **Null-session fallback (lines 262 and 276).** `ValidateComplexParamSerializer.scala` lines 227–270 cover a null session with the target session set as active in one case and as default in the other, for both serializers.
  - Through the delegate, this also exercises the no-argument `PipelineArraySerializer()` path.
  - The test checks that the temporary active session is cleared, and restores the default in `finally`.
- [x] **Reader security policy.** In `VerifyUDFTransformerPersistence.scala` lines 46–84, 6 of the 8 cases would fail without the change. The two cases that pass either way (allowed=true with no active session) are expected: the default `spark` session already allows legacy deserialization via `TestBase.sparkConfiguration`.
- [x] **No interference with other suites.**
  - `FuzzingTest.scala` (`src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/`, line 424) skips classes whose simple name starts with `Testable`. The new public no-argument constructors therefore don't add unfuzzed stages.
  - `TestEstimatorBase` keeps the same fully qualified name and stays exempt.
  - `build.sbt` line 282 sets `Test / parallelExecution := false`, so changing the global default session can't race other suites.
  - Sessions from `newSession()` are correctly left open, because closing one would stop the shared SparkContext.
- [x] **Test hygiene and evidence.**
  - Shared session state is restored in `try/finally` blocks (`ComplexParamTestFixtures.scala` lines 15–20; `ValidateComplexParamSerializer.scala` lines 266–268), and every case writes to its own path.
  - 20 + 11 cases equals the caller's reported 31 tests with no skips. I did not run any tests; `FuzzingTest` was checked by reading the code only.
  - The trivial constructor and the comment-only edits rightly have no dedicated tests.