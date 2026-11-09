---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 2
timestamp: "2026-11-07T14:53:27Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "6ba2bb336e7c8b94774d3f394f069f4b3f000430da65ab7d99d9fd557e448539"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read the whole assigned frozen diff in two bounded reads: lines 1–150, then lines 151–261. I compared the before and after versions of the `GlobalParams.scala` hunk (`@@ -17,23 +17,23 @@`) and all three `VerifyGlobalParams.scala` hunks (`@@ -8,6`, `@@ -82,10`, `@@ -98,4`). The current checkout files match the diff's post-image.
- [x] **Locking covers every access path:** In `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, both maps are private (lines 13–14). The five public methods (lines 20–38) are the only way to reach them, and each now runs under the module monitor.
  - `getParam` reads two maps (line 33), and that read is now atomic.
  - Writes become visible to readers on other threads.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–52) doesn't hold the lock while it calls `set`.
  - Only key `hashCode`/`equals` run under the lock. There is no nested locking, so lock ordering can't deadlock.
- [x] **Compatibility and semantics:** Signatures, `GlobalKey`, `HasGlobalParams` and serialized Params are unchanged. `synchronized` over `mutable.Map` keeps stored null values, which `ConcurrentHashMap` would reject. It also keeps the existing overwrite and reset behavior. `VerifyGlobalParams.scala` lines 198–204 pin the null behavior. Lines 176–184 pin two more behaviors: registering a key doesn't create a value, and reset clears the value for a registered param.
- [x] **Locking on the public object (considered, not raised):** The lock is the public module instance, and the private field `GlobalParams` (line 14) shadows the object's name inside its body. The patch always uses plain `synchronized`, which means the module itself, so nothing locks the wrong monitor. No compound-operation contract is exposed. A private lock object would be a preference here, not a fix for a defect.
- [x] **Test conventions:**
  - The suite extends `TestBase`, and its files go under `TestBase.tmpDir`, which is deleted in `afterAll`.
  - Cleanup always runs through `finally` blocks or `close()` (lines 238–256 and 278–284).
  - Imports are explicit, and the tests don't use Spark sessions or RDDs.
  - `Option.empty[String].orNull` (line 199) works with `NullChecker` in `scalastyle-test-config.xml`.
  - Object field names (lines 20–23) match `objectFieldRegex`.
- [x] **Branch and JDK guidance:** `.github/skills/synapseml-branches/references/branch-spark3p5.md` warns that class-loader APIs differ across JDKs. `startProbe` (lines 215–236) uses only Java 8 APIs:
  - the `URLClassLoader` chain plus `java.class.path`;
  - `Process.isAlive`, `destroyForcibly` and `waitFor(long, TimeUnit)`;
  - a manifest Class-Path instead of JDK 9+ @argfiles.

  `build.sbt` sets Scala 2.12.17 and Spark 3.5.0. `environment.yml` is unaffected because there are no Python changes.
- [x] **`scala.util.Using` (line 17):** It isn't in the Scala 2.12 standard library. Caller evidence says `scala-collection-compat` on the core test classpath supplies it, and all active trees compile. The repo helper `StreamUtilities.using` (`core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/StreamUtilities.scala`, lines 26–34) is a worse fit for `awaitProbe`:
  - it returns a `Try` and catches `Exception`, which would turn an `InterruptedException` into a `Failure`;
  - an exception from `close()` replaces the original error.

  `Using.resource` keeps the original exception and supports restoring the interrupt flag. No dependency pin was added. Not raised.
- [x] **Child-JVM isolation:** The probe objects (lines 19–102) and the concurrency tests (lines 288–301) run in a separate JVM, for the reasons in the comment at line 288:
  - there is no unregister API, so 64,000 permanent registrations would stay in the shared JVM;
  - map corruption before the fix could spin in a way that interrupts can't stop.

  Timeouts are layered: 30 s per worker future, a 45 s parent wait, then forced termination bounded at 10 s. If the parent dies, the blocking probe exits when its standard input closes. The harness self-tests (lines 258–286) check bounded cleanup and interrupt restoration. The argument test (lines 206–213) fails before any global state changes (line 65).
- [x] **Helper shape:** `awaitProbe` returns a tuple of two Booleans, and the classpath JAR is named per mode. These are single-use private helpers, and both call sites destructure the tuple by name. Neither is a defect.
- [x] **Scope:** The change touches 2 files and targets master. It changes no dependency, workflow, `pipeline.yaml`, release, codegen or `target/` files. No public JVM signature or serialized Param shape changes.
- [x] **Caller-provided validation (I did not run it):** Java 8/Scala 2.12.17 compile, Test compile, `VerifyGlobalParams` 14/14 and both style gates, plus the Java 17 port snapshots. The count of 14 matches the diff: 8 existing tests plus 6 new ones.
- [ ] **Not done:** I ran no compile, test or style commands because this review is read-only. I didn't search the repository for other `synchronized`, `scala.util.Using` or child-JVM patterns because searching was forbidden. Convention comparison was limited to files viewed by exact path: AGENTS.md, the skills, `build.sbt`, `environment.yml`, `scalastyle-test-config.xml`, `TestBase.scala` and `StreamUtilities.scala`.