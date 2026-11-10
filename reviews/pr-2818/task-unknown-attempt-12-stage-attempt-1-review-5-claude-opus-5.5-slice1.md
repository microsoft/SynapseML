---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 5
timestamp: "2026-11-10T02:45:40Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "8f723449627b59aee55ab40f4d83c99cdc323668b2d64ac259b4a683cf1878c9"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2818/task-unknown-attempt-12-stage-attempt-1-review-5-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read every line of the assigned frozen diff (437 lines, 5 files) in the required ranges [1,150], [151,300] and [301,437]. The current `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `ComplexParamsSerializer.scala`, `ComplexParamTestFixtures.scala`, `ValidateComplexParamSerializer.scala` and `VerifyUDFTransformerPersistence.scala` match the diff.
- [x] Guidance and versions read: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. `build.sbt` gives Spark 3.5.0 and Scala 2.12.17; `environment.yml` gives pyspark 3.5.0. Target branch is master.
- [x] Each new code path in `Serializer.scala:59-60` and `258-296` has a test:
  - Assigned-session write and read, for both single and array params: `ValidateComplexParamSerializer.scala:156-213`.
  - Null session falling back through `this(null)` → `None` to the no-arg delegate, with an active-only session and with a default-only session: `:215-258`.
  - Active session restored after write failures: `:198-210` and `:246-252`.
  - Read-side policy routing, and restoration after read failures (`allowed=false`): `VerifyUDFTransformerPersistence.scala:46-84`.
- [x] Static mutation check: each of these changes fails at least one test permutation:
  - removing the active-session swap or the `finally` restore in `withSession`;
  - dropping the session argument in `typeToSerializer`;
  - using `Some(spark)` instead of `Option(spark)`.

  `TestBase.scala:55` sets the legacy deserialization flag to `true` on the base session, so the `allowed=true, active=false` UDF case can't catch a regression. The `allowed=false, active=false` case does. The only change that passes every test is "always overwrite" (Issue 1).
- [x] Fixture side effects: `pipelineStages` in `src/test/scala/com/microsoft/azure/synapse/ml/core/test/fuzzing/FuzzingTest.scala` skips classes named `Testable*` and nested `$` classes, so the new no-arg constructors add no FuzzingTest requirements. The moved `TestEstimatorBase` keeps its fully qualified name, which is already exempted, so downstream `test->test` modules in `build.sbt` are unaffected.
- [x] Test isolation: `build.sbt` sets `Test / parallelExecution := false`. The unassigned-session tests restore the global default session in `finally`, `withActiveSession` restores the active session, and every permutation writes to its own temp path.
- [x] The legacy overloads in `ComplexParamsSerializer.scala` only gained doc comments. Their documented behavior is covered by the existing "legacy complex parameter helpers" tests through `core/src/test/scala/org/apache/spark/ml/ComplexParamsTestHelper.scala`.
- [x] Considered and not raised:
  - No test calls `PipelineSerializer()` directly, but it behaves the same as the covered `PipelineSerializer(null)`.
  - The case with no active and no default session in `withSession` is untested, but it ends in the same state as before this PR.
  - The persisted format is unchanged by construction: the same Spark `Pipeline` writer and reader are used.
- [ ] Not executed: this is a read-only review, so I ran no tests or builds. The caller reports 31 tests with 0 skips, which matches the 20 + 11 tests in the two touched suites. I did not reproduce that.

## Issues

### Issue 1: `overwrite = false` over an existing nested pipeline artifact is never tested
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/core/serialize/ValidateComplexParamSerializer.scala (the production branch is in core/src/main/scala/org/apache/spark/ml/Serializer.scala)
- **Line(s)**: Test lines 180-196; production lines `Serializer.scala:287-290`
- **Description**: The PR replaces the overwrite handling in the shared `Serializer.writeMLWritable` helper with an inline `if (overwrite) writer.overwrite()` in `PipelineArraySerializer.write`, which both pipeline serializers now go through. The new overwrite loop writes with `overwrite = false` only to a fresh `nestedPath`, then with `overwrite = true` over the existing artifact. Every other nested save in the diff also targets a fresh path:
  - the top-level `write.session(...).save` calls;
  - the unassigned-session tests;
  - the UDF suite.

  So no test writes with `overwrite = false` over an existing nested artifact. If the code always called `overwrite()` (for example, by chaining `.overwrite()` onto the writer), every test would still pass.
- **Risk**: Not overwriting an existing artifact when `overwrite = false` is existing behavior of `ComplexParam.save`, `PipelineSerializer.write` and `PipelineArraySerializer.write`. This PR reimplements it, but no test checks it. A regression would silently replace an existing nested pipeline artifact instead of raising Spark's "already exists" `IOException`. Severity is Low because the normal `MLWriter.save` path still checks at the top level, so only callers that use these serializers or `ComplexParam.save` directly are exposed.
- **Suggested Fix**: After the `Seq(false, true)` loop, while `nestedPath` exists and `assigned` is still configured correctly, add a check for both `array` values:
  1. Call `saveNested(overwrite = false)` inside `intercept[java.io.IOException]`.
  2. Assert the message contains `"already exists"`.
  3. Reload `nestedPath` and assert the payload is still `Array[Byte](4, 5, 6)`.
  4. Assert `SparkSession.getActiveSession == expectedActive` and `SparkSession.getDefaultSession == previousDefault`.

  This also checks that sessions are restored when Spark's overwrite check fails before any child writer runs.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Extended all four scalar/array and active/default session cases to attempt a non-overwriting save with a distinct payload after the artifact already exists. Each case checks the IOException, preservation of the saved payload, and restoration of both sessions.
- **Why**: The prior fresh-path checks could not detect accidental unconditional overwrite.
- **How verified**: Temporarily making overwrite unconditional failed all four affected cases with the expected missing-IOException assertion (zero skips). After restoring the production file byte-for-byte, all 31 tests in both serializer suites passed with zero skips, alongside Compile, Test/compile, and both scalastyle gates. Evidence: overwrite156-before-verified.json and overwrite156-full-verified.json. Only test source changed.