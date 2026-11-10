---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 5
timestamp: "2026-11-09T04:36:15Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "eee173c33c55070fc78b62f8bdc9436e53f269aa7873c48d9c30ca7792cea1c7"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2818/task-unknown-attempt-10-stage-attempt-1-review-5-grok-4.7-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full (lines 1–393) and the current `Serializer.scala` session wrappers, `ComplexParam.save`/`load`, and both touched suites.
- [x] Compared the new matrices with `PipelineArraySerializer.withSession` and Spark 3.5 Pipeline save/load, which does not forward a reader or writer session to child stages.
- [x] Confirmed assigned-session success, inverse reader policy, and failure restoration for both `PipelineStage` and `Array[PipelineStage]`, including active-session present and absent.
- [x] Checked master context in `build.sbt` (Spark 3.5.0, Scala 2.12.17) and the code-review test checklist. Counted 29 cases in the two touched suites; that matches the supplied inventory only.
- [ ] Did not execute compile or tests. This read-only pass did not independently confirm the supplied JDK 11 result.

## Issues

### Issue 1: Overwrite branch of pipeline persistence is untested
- **Severity**: Low
- **File**: core/src/main/scala/org/apache/spark/ml/Serializer.scala
- **Line(s)**: 287-290
- **Description**: `PipelineArraySerializer.write` applies the assigned session and then calls `writer.overwrite()` only when `overwrite` is true. Every new nested save uses the default non-overwrite path: `stage.write.session(assigned).save` in `ValidateComplexParamSerializer.scala` (lines 171 and 182) and `VerifyUDFTransformerPersistence.scala` (lines 66–67). The direct null-session calls hard-code `overwrite = false` (lines 203–204). No test saves twice through `MLWriter.overwrite()` or `ComplexParam.save(..., overwrite = true)`.
- **Risk**: Dropping or reordering the overwrite flag would still pass. Re-saving a nested pipeline-stage parameter with the normal Spark `write.session(session).overwrite().save` path could then fail with an existing-path error, or a later overwrite-only writer could lose the assigned session. Session assertions never observe that branch.
- **Suggested Fix**: For both the single-stage and array parameters, save once, save again with overwrite to the same path, and assert the nested payload still comes from the assigned session while the caller active and default sessions are unchanged. Repeat one failure overwrite and assert restoration.

### Issue 2: Null-session default fallback is not locked
- **Severity**: Low
- **File**: core/src/main/scala/org/apache/spark/ml/Serializer.scala
- **Line(s)**: 275-284
- **Description**: The null and no-arg constructors document a fallback to the active or default session. The unassigned tests always install an active session before passing null (`ValidateComplexParamSerializer.scala`, lines 191–204). They never clear the active session, install a distinct default session, and let `getOrCreate()` select that default. Those restoration asserts are also weak: the fallback object is the already active session, so they still pass if `finally` does not run. The older helper test covers active and default only for non-pipeline params.
- **Risk**: Replacing `getOrCreate()` with an active-session-only lookup would still pass. A no-arg or null pipeline read/write with no active session could then ignore the default session’s configuration, including the legacy deserialization policy, or fail instead of using that session. The suite would not catch it.
- **Suggested Fix**: Add an unassigned case with no active session and a distinct default session carrying the expected config. Assert the nested read and write use that default session, that the active session remains empty, and that a forced writer failure still clears it.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Extended the assigned-session matrix to save each direct nested parameter twice at the same path, with a different payload on overwrite, then force an overwrite failure. Both individual and array parameters verify the payload and active/default session restoration.
- **Why**: Calling `ComplexParam.save` directly exercises the nested existing-path overwrite branch without an outer stage writer pre-deleting it. This addresses a coverage gap, not a demonstrated production failure.
- **How verified**: `sbt "set core / Test / fork := false" core/compile core/Test/compile "core/testOnly com.microsoft.azure.synapse.ml.core.serialize.ValidateComplexParamSerializer com.microsoft.azure.synapse.ml.stages.VerifyUDFTransformerPersistence" core/scalastyle core/Test/scalastyle` passed: 31 tests, zero skips, both compilation and style gates.

### Issue 2
- **Status**: Addressed
- **What changed**: Expanded the null-session matrix to both active-session and default-only selection for individual and array parameters, including successful round trips and forced writer failures. Each path asserts the caller's active/default sessions and restores the original default in `finally`.
- **Why**: Clearing the active session and installing a distinct configured default directly exercises the documented fallback and its restoration behavior.
- **How verified**: The same 31-test command above passed on the updated source with zero skips, including both new default-only cases.