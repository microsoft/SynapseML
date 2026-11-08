---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 3
timestamp: "2026-11-07T08:55:49Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ce62cba7ba0bbda76ea5b715f7154ba0c79892da580477d177580a0ac00e33e4"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read every line of the assigned frozen diff in the two required ranges, [1,150] and [151,211]. The checked-out files match the patched versions shown in the diff. In `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, I checked lines 20–38. In `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, I checked lines 11–95, 171, 176–177 and 191–251. I compared the code before and after the change in both files.
- [x] **Every map access is locked:** In `GlobalParams.scala`, both maps (lines 13–14) are `private`, and the object has no companion class. Every read and write happens inside `synchronized` on the `GlobalParams` object (lines 21, 25, 29, 33 and 37). Because `getParam` resolves the registration and the value inside one locked block, each lookup is now atomic.
- [x] **No deadlock or class-loading risk:** `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–52) reaches the maps only through `getParam`. It calls `set` after the lock has been released. Inside the locked blocks, the code takes no other lock and makes no Spark calls. It also cannot trigger class initialization, because `Param` hashing and equality are `final`. Nothing else is locked while this lock is held, so it cannot form a deadlock cycle.
- [x] **Public API and behavior unchanged:** Method signatures and erased return types are the same. In `resetGlobalParam`, the result of `-=` is still discarded, so it returns `Unit` as before. The new test at lines 191–197 records the existing behavior: a stored `null` counts as present until reset. The test at lines 168–177 now also checks that a registered parameter has no value before the key is set and none after it is reset. No serialized `Params` or dependency changes.
- [x] **Child test program (`GlobalParamsConcurrencyProbe`) failure handling:**
  - Argument checks run before the thread pool is created or any global value is set (lines 60–65). The in-process bad-argument test (lines 199–206) therefore has no side effects.
  - The checks use `require`, which the compiler cannot strip out the way it can `assert`.
  - `Range.map` builds the list eagerly, so all 8 tasks are submitted before the start signal is released.
  - If a submit fails, a worker throws, or a 30-second wait times out, `finally shutdownNow()` interrupts any workers still waiting to start.
  - Key ranges never overlap between workers (maximum index 62,000), so there is no overflow.
  - After the last round, `verifyCompletedRound` rechecks all 64,000 entries, which equals the `TotalEntries` value the parent test asserts.
- [x] **Child JVM cleanup and timeouts (lines 227–249):**
  - The child's stdout and stderr go to a log file, so a full output pipe cannot block it.
  - The parent waits 45 seconds. A cleanup handler then force-kills the child if needed and waits up to 10 more seconds. This runs whether the child finishes, times out or the wait is interrupted.
  - On interruption, the test restores the thread's interrupt flag and rethrows.
  - The assertions run in the order terminated, completed, exit code, so `exitValue()` is only called on a finished process.
  - `TestBase.afterAll` deletes the temporary files (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`).
- [x] **Classpath handling across Java versions** (per `.github/skills/synapseml-branches/references/branch-spark3p5.md`):
  - Walking up the class-loader chain stops at `null` and never calls `getParent` on `null`.
  - On Java 9 and later, the default class loader is not a `URLClassLoader`; reading `java.class.path` covers that case.
  - Paths are written as absolute ASCII `file:` URIs. This escapes spaces and non-ASCII characters, and the JVM accepts these entries in a manifest classpath. Raw Windows drive paths would be rejected.
  - Directories that exist get a trailing `/`, which the JVM needs to treat them as directories.
  - Passing one short JAR path with `-cp` avoids Windows command-length limits.
  - The process APIs used (`waitFor(long, TimeUnit)`, `isAlive`, `destroyForcibly`) exist in Java 8.
- [x] **Considered but not reported:**
  - The lock is the public `GlobalParams` object. This is a common Scala pattern, and outside code would have to lock it on purpose to cause trouble.
  - Reading several global parameters takes the lock once per parameter, so a group of reads is not atomic. This was already true before the change and is outside this PR's stated scope.
  - `ParamToKeyMap` grows without limit. This was also already true before the change.
  - If a regression made the child loop forever, its non-daemon workers would keep it alive. The parent's 45-second wait and forced kill cover this; the child would only be orphaned if the parent JVM were killed on its own.
- [x] **Versions:** `build.sbt` shows Scala 2.12.17 and Spark 3.5.0 for master. I also read `environment.yml`, `AGENTS.md` and `.github/skills/code-review/SKILL.md`.
- [ ] **Tests not run by this reviewer:** this was a read-only review. Test results here come from the caller: 12/12 on Java 8 locally and on both Java 17 Spark-port copies, with CI for the pending commit not yet run. I did not independently confirm that the transitive scala-collection-compat dependency provides `scala.util.Using`, because I was not allowed to access dependency caches.