---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 1
timestamp: "2026-11-06T15:44:15Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "2032d5e23ea8c8944ce38596a031f28dba3d3aabb65c7686af215f5620e84acd"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-4-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read:** I read all 171 lines of the assigned frozen diff in two views (lines 1–150 and 151–171), with nothing truncated. That covers both changed files and both hunks of `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, before and after. Both files in the current checkout match the after side of the diff.
- [x] **Production change is correct.** In `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` (L20–38), the only change wraps the bodies of the five public methods in `synchronized` on the `GlobalParams` object.
  - Both maps (L13–14) are `private` and only these five methods use them. `untypedKey` holds no state. So every read and write goes through the same lock.
  - `getParam` reads both maps while holding the lock once, so the lookup is consistent.
  - Every method declares its return type, so JVM method signatures and the static forwarders don't change. The serialized form of Params doesn't change either.
- [x] **No deadlock risk found.**
  - While holding the lock, the code only does `mutable.HashMap` operations. These call Spark's `Param` `hashCode`/`equals`, which are final and have no side effects, and the hashing of the case-object/case-class `GlobalKey`s. Nothing inside the lock calls out to code that takes another lock.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (L43–52) calls `set(...)` only after `getParam` has released the lock.
  - The lock is reentrant, and the object has no lazy vals that would share it.
- [x] **Existing behaviour is preserved.**
  - Setting a key to `null` still returns `Some(null)`. The new test at L163–169 pins this down, so a later switch to a `ConcurrentHashMap` (which rejects `null` values) would fail it.
  - Overwrite, reset and unregistered-param behaviour are unchanged. The 8 existing tests are untouched; with the 4 new ones that makes 12, matching the reported 12/12.
- [x] **The concurrency probe is sound** (`GlobalParamsConcurrencyProbe`, L19–70).
  - `require` (L27–28) rejects missing, unknown and extra arguments before the thread pool is created (L30) and before any global value is set (L31). So the in-process negative test (L171–178) can't change the parent JVM's state.
  - `Range.map` is strict, so all workers are submitted before the start latch is released.
  - Each round/worker pair uses its own key range; the highest offset is 63,999.
  - Failures show up as a non-zero exit code with a stack trace. The pool is shut down with `shutdownNow` in `finally`, and the probe never calls `System.exit`.
- [x] **The child-JVM test harness is sound** (L180–222).
  - Classpath discovery: `Iterator.iterate` is lazy, so `takeWhile(_ != null)` never calls a method on `null`. Walking the `URLClassLoader` chain plus reading `java.class.path` covers both Java 8 and Java 9+ class loaders.
  - Classpath safety: only `file:` URLs are kept, and spaces in paths are percent-encoded by `File.toURI`.
  - Manifest JAR: putting the classpath in a manifest-only JAR avoids Windows command-line length limits. Java 8, 11 and 17 all accept absolute `file:` entries in a local JAR's `Class-Path`.
  - Streams and output: the nested `Using.resource` blocks close the JAR and file streams in the right order. Child output goes to a file, so a full pipe can't stall it.
  - Process cleanup: `close()` force-kills the child if it is still running and waits at most 10 seconds. The thread's interrupt status is restored before the exception is rethrown.
  - Assertion order: the order (terminated → completed → `exitValue` → output marker) means `exitValue()` is only called on a finished process.
  - Temporary files: the JAR and log go in `TestBase.tmpDir`, which `TestBase.afterAll` deletes.
- [x] **Security:** `ProcessBuilder` gets an argument list, so no shell is involved. The `java` binary comes from `java.home`, and the class name and mode are constants. There is no user input, no credentials, no deserialization and no network access.
- [x] **Branch, versions and style:**
  - The PR targets master, so the master context (`.github/skills/synapseml-branches/references/branch-spark3p5.md`) applies. `build.sbt` sets Scala 2.12.17 and Spark 3.5.0, and `environment.yml` pins pyspark 3.5.0.
  - Only Java 8 APIs are used (`Process.isAlive`, `destroyForcibly`, `waitFor` with a timeout).
  - Against `scalastyle-test-config.xml`:
    - Line length: lines are at most 120 characters.
    - Field names: the object fields use the required upper-camel names (`Workers`, `Entries`, `Rounds`).
    - Method length: `main` is about 44 lines, under the 50-line limit.
    - Null usage: the only `null` token is a `!= null` check.
    - File format: the license header and final newline are present.
  - There are no dependency, workflow, pipeline or release changes.
- [x] **Relies on caller-provided evidence:** I ran no commands or tests. That `scala.util.Using` resolves on Scala 2.12 through the transitive `scala-collection-compat` dependency, the 12/12 result, the baseline failures and the style-gate results all come from the caller's validation evidence.
- [x] **Existing behaviour noted but not counted as findings:**
  - `ParamToKeyMap` grows without limit because there is no unregister API.
  - `transferGlobalParamsToParamMap` reads each param separately, not as one snapshot.
  - Both predate this PR and are unchanged by it, which fits its "without changing global semantics" scope.
- [x] **Read rules followed:** I used only exact-path `view` calls on these files:
  - the assigned frozen diff;
  - `AGENTS.md` and the three skill/reference files;
  - `build.sbt` and `environment.yml`;
  - the two changed source files;
  - `TestBase.scala`, opened by its import path;
  - `scalastyle-test-config.xml`, opened by the path named in `build.sbt`.

  I used no search or discovery tools and read no review artifacts or other reviewers' output.