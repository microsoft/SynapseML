---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 4
timestamp: "2026-11-06T16:35:50Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "81e2adb026626f121f6b2f4967c4ea9da7b05e71be64923e424145acdca3bbd5"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-4-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] I read all 171 lines of the assigned frozen diff in two parts (lines 1–150 and 151–171). This covered the before and after versions of both files. The after version matches the current `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`. The test file grows from 101 lines to 223 (101 + 61 + 61), which matches both hunk headers.
- [x] **Production change (`GlobalParams.scala` lines 13–39):**
  - Both maps are private, and all five public accessors now lock the same `GlobalParams` object, so no code reads or writes the maps without that lock.
  - In `getParam`, both map lookups and the strict `Option.flatMap`/`map` calls finish inside the lock, so no lazy view leaks out.
  - Method signatures and return types (`Unit`, `Option[T]`) are unchanged, and `untypedKey` is still a pure cast.
  - While the lock is held, the only outside code that runs is the `hashCode`/`equals` of the `Param` (final in Spark) and of the `GlobalKey` (generated case-object/case-class methods). No second lock is taken, so locks cannot be acquired in conflicting orders and cause a deadlock.
  - `HasGlobalParams` (lines 41–53) is unchanged and still reads through `getParam`.
- [x] **Callers keep the same behaviour:**
  - In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDefaults.scala`, every get, set and reset is a single call to `GlobalParams`. None checks a value and then acts on it, so none needs a wider lock.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` calls `registerParam` once while each object is constructed. The lock covers only the map update.
- [x] **Probe argument checking and setup (`VerifyGlobalParams.scala` lines 19–68):**
  - The `require` runs before the thread pool is created and before `SharedKey` is set.
  - Because `&&` stops early, `args.head` is never read from an empty array.
  - The mode is decided once, before any work is submitted.
- [x] **Probe arithmetic and threading:**
  - Each entry number is `(round*8 + worker)*2000 + index`. Every (round, worker, index) combination maps to exactly one value in [0, 64000), with no overlap, no off-by-one and no `Int` overflow.
  - `Range.map` runs immediately, so all 8 tasks are submitted before `countDown`. The pool has 8 threads for 8 tasks per round, so waiting on the start latch cannot deadlock.
  - A worker failure surfaces as an `ExecutionException` from `get`, which makes `main` exit with a non-zero code. The `finally` calls `shutdownNow`.
- [x] **Child-JVM test (lines 181–221):**
  - The classpath JAR is built from canonical, de-duplicated paths. `File.toURI` adds a trailing slash for existing directories, and `toASCIIString` encodes spaces.
  - Closing the `JarOutputStream` also closes the `FileOutputStream`, and closing it a second time is harmless.
  - `Redirect.to` creates the log file when the process starts, so `readAllBytes` cannot fail with "file not found".
  - On close, the process is force-killed and given 10 seconds to stop. If the thread is interrupted, the interrupt flag is restored before the exception is re-thrown.
  - `terminated` is checked before `exitValue()` is called, so `exitValue()` cannot throw `IllegalThreadStateException`.
  - `tmpDir` is deleted by `TestBase.afterAll` (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` lines 152–156 and 181–186).
- [x] **Null test:** `mutable.HashMap.get` returns `Some(null)` for a key stored with a null value. The test therefore tells "stored as null" apart from "missing", and after reset it sees `None`. This pins the current behaviour and would catch a switch to `ConcurrentHashMap`, which rejects null values.
- [x] **Negative test:** Scala's `require` throws `IllegalArgumentException("requirement failed: <msg>")`, so `contains` matches. The exception is thrown before any thread pool or global state is touched, so it cannot leak state into the shared test JVM.
- [x] **Versions and compatibility:**
  - `build.sbt` sets Scala 2.12.17 and Spark 3.5.0. `scala.util.Using` comes in transitively: `spark-mllib` is listed in `coreDependencies`, and the `breeze` exclusion is not applied to it. This is consistent with the caller's evidence that `scala-collection-compat` 2.7.0 is on the test classpath.
  - The `Process` methods used (`isAlive`, `destroyForcibly`, `waitFor(timeout)`) all exist on Java 8.
  - I counted the longest new lines at about 115 characters or fewer, within the 120-character limit.
- [x] Public JVM signatures and serialized `Params` are unchanged. No dependency, workflow or pipeline files are touched.
- [ ] I did not run any tests or builds. This was a read-only review, and the validation results above come from the caller. CI on the pending head has not run yet and is deferred until the change is published.
- [ ] I did not read any prior or parallel review artifacts, as the plan requires.