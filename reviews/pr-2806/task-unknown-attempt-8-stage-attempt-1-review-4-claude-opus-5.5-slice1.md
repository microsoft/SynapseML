---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 4
timestamp: "2026-11-07T15:40:18Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "1e8edefd6c80d88499e94ff589c716037edf93ba29c72f9f2593a02252182681"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read the whole assigned frozen diff using exactly the two required ranges, [1,150] and [151,261]. I reviewed the before and after versions of both files. The current versions of `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` match the post-change version. The hunk offsets are consistent: +93 lines in the first test hunk, +3 lines in the second, and the file ends at line 302.
- [x] **Production lock coverage (`GlobalParams.scala` lines 20–38):**
  - All five public operations lock on the same `GlobalParams` object.
  - Both maps are `private` and only these methods access them.
  - `getParam` reads both maps under one lock, so it sees a consistent view.
  - `untypedKey` has no side effects.
  - While holding the lock, the code only calls `hashCode`/`equals` on Spark `Param` (based on its string fields) and on case-object or case-class keys. Nothing else locks inside it, so there is no lock-ordering or deadlock risk.
  - `HasGlobalParams` (lines 41–53) takes the lock once per parameter and calls `set` after releasing it.
- [x] **Compatibility and existing behaviour:**
  - Method signatures, type parameters, return types and compiled method descriptors are unchanged. The JVM-facing method signatures are the same whether the compiler marks the methods as synchronized or locks inside them.
  - Serialized Params are untouched.
  - Storing a null still returns `Some(null)` until reset, and reset still removes the key. The test at lines 198–204 pins this behaviour.
  - No dependency, workflow, `pipeline.yaml` or release-file changes.
- [x] **Probe arithmetic (`VerifyGlobalParams.scala` lines 19–95):**
  - The key ranges `(round*Workers + worker)*Entries + index` do not overlap and leave no gaps.
  - At line 51, `*` binds tighter than `until`, so `0 until (round+1)*Workers*Entries` covers exactly the completed rounds. After the last round that is 64,000, which equals `TotalEntries`. There is no off-by-one.
  - `Range.map` builds the list eagerly, so all eight tasks are submitted before `countDown` at line 86. The thread pool size equals the worker count.
  - Worker failures surface through `Future.get` at line 87, before the success line is printed.
  - `Param` equality (same parent and name) makes the recreated `registrationParam(index)` lookups valid, and `WorkerKey` value equality makes the reset checks valid.
- [x] **Argument handling (lines 61–62 and 206–213):** The `&&` short-circuits, so `args.head` is never evaluated on an empty array. The in-process negative test fails before the thread pool is created or `SharedKey` is set, so nothing leaks into the shared test JVM.
- [x] **`awaitProbe` (lines 238–256):** I traced three cases against `Using.resource` (cleanup runs on both the normal and the exception path, and a second error is attached as suppressed) and JDK `Process.waitFor(long, TimeUnit)`:
  - **Success:** `completed` and `terminated` are both true.
  - **Timeout:** `completed` is false, the child is forcibly killed, and `terminated` is true.
  - **Interrupt already set:** `waitFor` throws and clears the flag, so the 10-second cleanup wait is not cut short. The flag is restored only after cleanup.

  The test at lines 295–297 checks `terminated` and `completed` before calling `exitValue()`, so `exitValue()` cannot run on a live process.
- [x] **Blocked-child tests (lines 258–286):**
  - The child blocks on standard input, which is a pipe by default and is never closed by the parent.
  - The `finally` block clears the interrupt flag before its own bounded wait.
  - The 15-second limit is above the worst case (100 ms + 10 s), so it should not cause flaky failures.
- [x] **`startProbe` (lines 215–236):**
  - `Iterator.iterate(...).takeWhile(_ != null)` never calls `getParent` on null, because `iterate` is lazy in both Scala 2.12 and 2.13.
  - `File.toURI` adds a trailing `/` to existing directories, so the manifest `Class-Path` treats them as directories.
  - `toASCIIString` encodes spaces and non-ASCII characters.
  - Each mode gets its own file names in `tmpDir`, and `TestBase.afterAll` deletes them after the child processes have exited.
- [x] **Guidance and versions:** I read `AGENTS.md`, the branch skill, the master branch reference and the code-review skill. From `build.sbt`: Scala 2.12.17, Spark 3.5.0, tests run in-process with no `Test / fork`, and `Test / parallelExecution := false`. I also read `environment.yml`.
- [x] **Considered and dismissed:**
  - **Blocked-child log is never checked:** a child that died on its own after the 100 ms wait could satisfy the asserts. That only happens if the child cannot launch, and the two concurrency tests would then fail loudly in the same run.
  - **Probes only cover different keys:** they don't race on the same key. That is a coverage choice, not a defect, because all operations share one lock.
  - **Child standard-input pipe is never closed:** the JDK reclaims it, so the effect is negligible.
- [ ] **Tests and `scala.util.Using` not verified by me:** This was a read-only review, so I ran no tests. The 14/14 pass, compile and style results come from the caller. I also did not check that `scala.util.Using` is available on Scala 2.12 through the transitive scala-collection-compat dependency, because dependency caches were outside the allowed scope.