---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 6
timestamp: "2026-11-09T08:15:29Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "666bfaf59ed99453c1813461dd6262adb05479996eaec1a9ae8ff32ad30278ab"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2818/task-unknown-attempt-11-stage-attempt-1-review-6-grok-4.7-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full (ranges 1–150, 151–300, 301–437) for `core/src/main/scala/org/apache/spark/ml/Serializer.scala`, `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala`, and the three test files.
- [x] Checked `Serializer.scala` lines 71–75 and 258–296 against the previous `writeMLWritable` / `Pipeline.load` path and `ComplexParam.save`/`load` in `core/src/main/scala/com/microsoft/azure/synapse/ml/core/serialize/ComplexParam.scala`.
- [x] Checked new comments at `ComplexParamsSerializer.scala` lines 66–69 and 174–176, plus nested restore assertions in `ValidateComplexParamSerializer.scala` and `VerifyUDFTransformerPersistence.scala`.
- [x] Read `AGENTS.md`, `.github/skills/code-review/SKILL.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `build.sbt` (Spark 3.5.0, Scala 2.12.17), and `environment.yml` (pyspark 3.5.0).
- [x] Assigned-session read/write installs that session with `.session(session)` and restores the caller’s active session in `finally`. That path is covered by the nested success and failure tests. No extra per-stage session allocation.
- [ ] Did not execute tests. Caller-supplied evidence of compile, Test/compile, 31 tests, 0 skips, and scalastyle was not re-run.

## Issues

### Issue 1: Absent-session fallback creates and retains a SparkSession
- **Severity**: Medium
- **File**: core/src/main/scala/org/apache/spark/ml/Serializer.scala
- **Line(s)**: 261–262, 275–279, 284
- **Description**: The new constructor docs say a null session falls back to the active or default session. `withSession` instead calls `SparkSession.builder().getOrCreate()` when the serializer was built with no session (no-arg constructor or null). That call returns the active session, otherwise the default session, otherwise creates a session and installs it as the JVM default. `finally` restores only the thread’s active session, so a session created here remains the default after success or failure. The replaced `writeMLWritable` and `Pipeline.load` paths used Spark’s writer/reader session, which resolves through `SparkSession.active` and throws if neither an active nor a default session exists. The nested tests always leave an active or default session set, so they do not cover this branch.
- **Risk**: A no-arg or null-session read/write with no existing session can start a SparkContext, which is expensive and process-wide, and then leave that session as the default for every later thread. Nothing logs the creation. The new public comment describes the old fail-fast contract, not this side effect.
- **Suggested Fix**: Resolve an absent session with `SparkSession.active`, not `getOrCreate()`, so null and no-arg callers keep the previous throw-if-missing behavior and the constructor text stays true. Do not create or retain a default session in this loan. If creation is intentional, document it, log it, clear it before returning, and test the neither-active-nor-default case.

### Issue 2: Public session comments hide the thread-local loan
- **Severity**: Low
- **File**: core/src/main/scala/org/apache/spark/ml/Serializer.scala
- **Line(s)**: 71, 261–262, 275–285
- **Description**: The new public comments only say which session is selected. They do not say that `PipelineArraySerializer.read` and `write` replace the calling thread’s active session for the whole call and restore the previous active session on both success and failure (`Serializer.scala` lines 280–284). The same “active or default” wording is also added to `ComplexParamsSerializer.scala` lines 66 and 174, whose no-arg helpers call `getOrCreate()` and can create a default session. `writeMLWritable` (line 71) really does use the active-or-default writer session and does not create one. The private loan method is named `withSession`, which reads as a pure lookup; the test helper for the same restore pattern is `withActiveSession`.
- **Risk**: The thread-local swap is the behavior this change adds, and same-thread child readers, writers, and driver callbacks observe it. A later cleanup can drop `setActiveSession` or the `finally` restore while the constructor docs still look complete. Reusing one phrase for `SparkSession.active` and `getOrCreate()` hides the create-versus-throw difference.
- **Suggested Fix**: Document on both pipeline serializers that read/write install the resolved session as this thread’s active session and always restore the previous one. Rename `withSession` to `withActiveSession`. Keep “active or default” only for `SparkSession.active`; state explicitly where `getOrCreate()` may create and retain a default session.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No source changes; the original finding and reviewer verdict are retained.
- **Why**: The claimed baseline `SparkSession.active` throw-if-missing contract is incorrect for the target Spark runtime. Baseline no-argument Pipeline reader/writer operations already create and retain active/default sessions when neither exists. Switching to `SparkSession.active` would change that legacy behavior rather than preserve it.
- **How verified**: Reused the existing 18-case baseline/current runtime comparison after verifying the exact current `Serializer.scala` hash and target revision. All 18 cases confirm that baseline and current operations retain the created active/default sessions. The separately qualified 31-test suite covers assigned-session and existing-session restoration. No new execution is claimed for this repeated finding.

### Issue 2
- **Status**: Rejected
- **What changed**: No source changes; the optional naming and documentation suggestion is recorded.
- **Why**: The create-versus-throw distinction repeats Issue 1's disproved baseline premise. The constructor comment describes fallback selection, not a guarantee against session creation. The private scoped helper already explains why child stages require active-session installation and explicitly restores state in `finally`; renaming it is a preference, not an identified defect.
- **How verified**: Inspected the unchanged helper and its existing comment. The qualified nested success/failure matrix asserts both active/default preservation, and the 18-case baseline/current comparison establishes fallback compatibility. Original reviewer text remains unchanged above this resolution log.