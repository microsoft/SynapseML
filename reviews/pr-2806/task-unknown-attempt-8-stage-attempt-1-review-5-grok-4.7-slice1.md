---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 5
timestamp: "2026-11-07T15:51:21Z"
outcome: "clean"
reviewer_model: "grok-4.7"
input_sha256: "8801b6a6a5847a972e4e54b19161ea58505d4de2b3e88db4a5c41c9e9b5ad063"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-5-grok-4.7-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Assigned frozen diff read in full, ranges 1–150 and 151–261. Before/after covers only the five `GlobalParams` locks and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`.
- [x] `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`: both maps are private. `setGlobalParam`, `getGlobalParam`, `resetGlobalParam`, `registerParam`, and `getParam` share one object lock. `getParam` reads both maps inside that lock. `HasGlobalParams` is unchanged.
- [x] `VerifyGlobalParams.scala` registers 14 tests: existing scalar/register behavior, present-null versus absence, in-process rejection of missing/unknown/extra probe args, isolated registration and values probes, and bounded timeout/interrupt cleanup.
- [x] Probe arithmetic matches the asserted token: 8 workers × 2000 entries × 4 rounds = 64000. Offsets `(round * Workers + worker) * Entries` partition `0 until TotalEntries`. `verifyCompletedRound` rechecks that prefix after each joined round. `require` failures skip the success line and yield a non-zero child exit.
- [x] Harness assertions cover the stated failure outcomes: invalid args must throw `IllegalArgumentException` with the mode message; timeout requires `completed == false`, cleanup `terminated == true`, and a dead child within 15 seconds; interrupt requires `InterruptedException`, a restored interrupt flag, and a dead child. `awaitProbe` destroys a live child before restoring the interrupt flag. The blocked-test `finally` clears the flag and destroys a remaining child.
- [x] `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`: sequential suite, lazy `tmpDir` removed in `afterAll`, and no Spark use on these paths. Parent `beforeEach`/`afterEach` reset the three shared keys. Child JVMs keep permanent registrations and map corruption off the test JVM. Distinct mode names avoid log/classpath collisions.
- [x] Compared with `.github/skills/code-review/SKILL.md` and `.github/skills/scala-code/SKILL.md`: new synchronization has regression coverage, negative cases, and `Using.resource` cleanup. Master baseline from `build.sbt` is Spark 3.5.0 / Scala 2.12.17. Public signatures are unchanged.
- [ ] Tests were not executed in this read-only review. Caller-supplied qualification (14/14, both style gates, and failed mutations of forced termination and interrupt restoration) was inspected only as external evidence.