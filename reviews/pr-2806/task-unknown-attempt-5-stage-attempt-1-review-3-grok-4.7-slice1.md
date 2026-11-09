---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 3
timestamp: "2026-11-07T08:55:53Z"
outcome: "clean"
reviewer_model: "grok-4.7"
input_sha256: "0007e57624d382b2c9f6ce4cba906adb475e2dd878af12bd3173919106767a66"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-3-grok-4.7-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Assigned frozen diff read in full (lines 1–150 and 151–211): synchronization added to all five `GlobalParams` methods; test probe, null contract, argument rejection, and isolated-JVM harness reviewed before and after.
- [x] `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`: both maps are private; every lookup and mutation takes the same singleton monitor, so `getParam`’s two-map read is one critical section. Public signatures and single-threaded semantics, including a stored null as `Some(null)`, are unchanged. `HasGlobalParams` only calls the now-guarded lookup.
- [x] `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`: bad arguments fail before the executor or global writes; workers share a start latch; each round joins before rechecking prior registrations or reset absence. `require` cannot be elided. Jar streams and the child process are closed through `Using`, including timeout and interrupt paths. `exitValue` is reached only after a completed exit.
- [x] `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`: this suite is not parallel; `tmpDir` is removed in `afterAll` only after the test returns, and the harness waits for or forcibly stops the child first.
- [x] Branch and compatibility context: master, Spark 3.5.0, Scala 2.12.17 (`build.sbt`); code-review resource and binary-compatibility checks. Caller-supplied 12/12 and child-JVM results were treated as external evidence and were not re-executed.