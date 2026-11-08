---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 2
timestamp: "2026-11-06T14:51:04Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "c867abae1aab48314a69b127e06c078de7ccbd3799061a639e8a103d9210649d"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-3-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read all 159 lines of the assigned frozen diff in two views (lines 1–80 and 81–159). Neither view was truncated. I checked the before and after sides of every hunk:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` (`@@ -17,23 +17,23 @@`)
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` (`@@ -8,6 +8,64 @@` and `@@ -98,4 +156,56 @@`)
- [x] The checked-out copies of both files match the diff's post-change side: `GlobalParams.scala` lines 20–38, and `VerifyGlobalParams.scala` added lines 11–68 and 159–210.
- [x] I read these guidance files by exact path: `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. The target is `master`, which uses Spark 3.5.0 and Scala 2.12.17 (`build.sbt` lines 31 and 34) and `pyspark==3.5.0` (`environment.yml`).
- [x] Lock design (`GlobalParams.scala` lines 13–38):
  - All five public methods lock the same `GlobalParams` object.
  - The two private maps can only be reached through those methods, so `getParam`'s lookup across both maps is atomic and no unsynchronized path remains.
  - Only map operations and the keys' `hashCode`/`equals` run while the lock is held. There are no callbacks and no nested locks.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 41–53) reads through `getParam` and calls `set` outside the lock.
- [x] Compatibility:
  - Method names, type parameters, parameter types and return types are unchanged.
  - `synchronized` does not change JVM method signatures or the static forwarder methods.
  - No `Params`, serialized parameter shape, `Wrappable` or codegen surface is touched.
  - The diff has no dependency, workflow or `pipeline.yaml` change.
- [x] Existing behavior is preserved:
  - The PR keeps `mutable.Map` behind a lock instead of switching to `ConcurrentHashMap`, which rejects null values. A stored null therefore still reads back as present.
  - The new test at `VerifyGlobalParams.scala` lines 160–166 locks in that null behavior.
  - `scalastyle-test-config.xml` treats `NullChecker` as an error, which explains the use of `Option.empty[String].orNull`.
- [x] Resource-management convention:
  - The repository's `StreamUtilities.using` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala` lines 26–34) catches only `Exception` into a `Try`. A failure in `close()` also hides the original error.
  - The process cleanup here needs `scala.util.Using.resource`, which keeps the cleanup error as a suppressed exception and gives interrupts priority. Departing from the repository helper is justified.
  - Relying on a library that arrives through Spark rather than a declared dependency matches existing practice: `TestBase.scala` imports commons-io and breeze, and `StreamUtilities.scala` imports commons-io, none of which `build.sbt` declares directly.
  - That `Using` is available on Scala 2.12 comes from the caller's evidence. I did not verify it.
- [x] Running the concurrency check in a separate JVM is a new pattern, and it is justified:
  - There is no API to unregister a parameter. Registering 64,000 parameters in-process would permanently grow `ParamToKeyMap` in the shared forked test JVM, a constraint already noted at `VerifyGlobalParams.scala` lines 76–77.
  - The launcher reuses `TestBase.tmpDir` (`TestBase.scala` lines 161–164), which `afterAll` deletes (lines 194–199).
  - Child output goes to a file, so a full pipe cannot deadlock the process.
  - The assertions are ordered so `exitValue()` is never called on a process that is still running.
- [x] JDK floor (the class-loader caution in `branch-spark3p5.md`):
  - `ProcessBuilder`, `Process.isAlive`, `destroyForcibly`, `waitFor(long, TimeUnit)` and `JarOutputStream` are all Java 8 APIs.
  - The classpath comes from `URLClassLoader` URLs with a `java.class.path` fallback, which also covers newer JDKs whose application class loader is not a `URLClassLoader`.
- [x] I checked style limits from `scalastyle-test-config.xml` by reading the code:
  - The probe's `main` method is 41 lines; the limit is 50.
  - The longest added line is 115 characters; the limit is 120.
  - Import grouping matches nearby files such as `StreamUtilities.scala` lines 8–11.
- [x] Considered but not raised, because they are existing behavior outside this prerequisite:
  - Locking on a public object's built-in lock is idiomatic Scala, and the reviewed sources contain no outside code that locks on `GlobalParams`.
  - `transferGlobalParamsToParamMap` reads each key separately rather than taking one consistent snapshot.
  - `ParamToKeyMap` grows with every stage instance that registers parameters.
  - Global parameters apply to the whole JVM, which matters under Spark Connect.
  - The `var terminated` set inside the cleanup's `close()` is deliberate, and the caller's fault-injection evidence covers it.
- [ ] I did not run any builds, tests or style checks; this was a read-only review. All validation results mentioned above come from the caller.

## Issues

### Issue 1: Probe mode is passed as a plain string, and any unrecognized value silently runs the values workload
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 37, 45, 62, 168, 187, 208
- **Description**: The suite and `GlobalParamsConcurrencyProbe` agree on the mode only through repeated string literals.
  - The suite lists `Seq("registration", "values")` (line 168) and passes each value as a command-line argument (line 187).
  - The probe checks only `args.head == "registration"` (line 37). Its `else` branch (line 45) handles every other value: `"values"`, a typo, a renamed label, or no argument at all. The probe never checks for `"values"`.
  - The check also runs inside every worker in every round instead of once in `main`. A missing argument therefore shows up as `NoSuchElementException` failures wrapped in `ExecutionException`, not as one clear usage error.
  - The success marker is also a repeated literal (lines 62 and 208), but a mismatch there fails loudly.
- **Risk**: Someone might rename the `"registration"` label on only one side, for example to improve the generated test name. The registration test would then quietly run the values workload and still pass.
  - `registerParam` and `getParam` would lose their only concurrency coverage while the test name still claims it.
  - A later regression on that path, such as removing `synchronized` from `registerParam`, would go unnoticed.
  - The impact is limited to how well the tests catch regressions; production behavior is unaffected.
- **Suggested Fix**: Work out the mode once at the top of `main`, before creating the executor, with a match that rejects anything unknown:
  ```scala
  val registration = args match {
    case Array("registration") => true
    case Array("values") => false
    case other => throw new IllegalArgumentException(s"Unknown probe mode: ${other.mkString(" ")}")
  }
  ```
  Then branch on that boolean inside `call()`. Optionally, define the mode names and the success marker as constants on `GlobalParamsConcurrencyProbe` and use them from the suite, so the contract is written in one place.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Validate that exactly one supported mode was supplied before creating the executor or changing global state; resolve the registration/value branch once before submitting work. Add negative coverage for missing, unknown, and extra arguments.
- **Why**: A misspelled mode must not silently substitute the values workload, and missing input must fail directly with a clear usage error rather than a worker's wrapped exception.
- **How verified**: The new negative test failed against the previous probe because the missing argument produced `ExecutionException`. The final input-validation and resolved-boolean form passed `core/compile`, `core/Test/compile`, `core/testOnly com.microsoft.azure.synapse.ml.param.VerifyGlobalParams` (12/12, including both isolated child-JVM workloads, no skips), `core/scalastyle`, and `core/Test/scalastyle`.