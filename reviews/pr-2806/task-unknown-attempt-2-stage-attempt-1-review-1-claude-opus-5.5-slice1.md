---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 1
round: 1
timestamp: "2026-11-06T13:55:39Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "7ec4bff5fdbd2b1375452065ca6c2fc2c811179c131b2acbd6a016f42ca8b352"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-2-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read all 147 lines of the assigned frozen diff in one bounded view. I checked the before and after state of both files. The after state matches the checkout. I also checked the hunk arithmetic: `-17,23/+17,23`, `-8,6/+8,64` and `-98,4/+156,44`.
- [x] `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` lines 20–38: all five public methods now lock the same `GlobalParams` module monitor. The private maps `ParamToKeyMap` and `GlobalParams` are only accessed inside these methods. `HasGlobalParams.transferGlobalParamsToParamMap` (lines 41–52) only calls `getParam`, and it calls `set` outside the lock.
- [x] Compatibility: method signatures and explicit return types are unchanged, so binary and source compatibility are preserved. `resetGlobalParam` still discards the value the same way. The change does not alter any serialized Param shape or Python/codegen surface.
- [x] Deadlock and reentrancy: each locked section only runs `mutable.HashMap` operations and the `hashCode`/`equals` of `Param` and `GlobalKey`. Spark declares the `Param` versions `final` and they acquire no locks. No code under the lock takes another lock, calls a user callback or triggers class initialization. `getParam`'s two-map lookup is now atomic, which is strictly stronger than before.
- [x] Semantics guard: the new null test at `VerifyGlobalParams.scala` lines 160–166 pins the existing `Some(null)` behavior. Keeping `mutable.Map` under a lock preserves that behavior. Switching to `ConcurrentHashMap` would have broken it.
- [x] I traced the child-JVM failure paths at lines 168–197. Failure to start throws, and a non-zero exit fails the test with the child's output. A hang is handled by a 45 s wait, then `destroyForcibly`, then a 10 s wait. A missing completion marker also fails the test. Output goes to a file, so the child cannot block on a full pipe. The parent's 45 s timeout is longer than the child's 30 s per-future timeout, so child stack traces reach the log. The child's `finally` calls `shutdownNow`, and a probe that cannot be stopped fails the test. Each mode writes its own files under `tmpDir`, which `TestBase.afterAll` deletes.
- [x] Probe determinism (lines 18–66): each worker and round uses a disjoint key range. `Range.map` is strict, so all tasks are submitted before the latch is released. Results are deterministic under the lock. The suite has 11 tests, which matches the caller's 11/11.
- [x] Branch and JDK fit: per `build.sbt` (Spark 3.5.0, Scala 2.12.17), `environment.yml` and `.github/skills/synapseml-branches/references/branch-spark3p5.md`, master's oldest JDK is 8. The process and NIO APIs used all exist in Java 8. Classpath rebuilding covers both URLClassLoader chains (Java 8 and in-process sbt) and `java.class.path` (forked runs and Java 9+). `toURI.toASCIIString` escapes spaces and `#`. `build.sbt` sets `Test / parallelExecution := false`, so the two probes run one after another.
- [x] Scope and security: only two files change. There are no dependency-pin, workflow, `pipeline.yaml` or release changes. `ProcessBuilder` receives an argument list with no shell, and the mode values are hard-coded. `Files.createTempDirectory` is owner-only on POSIX, and the change includes no secrets. `scala.util.Using` on 2.12 relies on caller-provided evidence that it comes from the existing transitive scala-collection-compat. That matches how `TestBase` already relies on Spark's transitive commons-io. I could not verify it myself because dependency caches are outside my read-only boundary.
- [ ] I did not run any builds or tests because this review is read-only. Validation results are the caller's evidence only.

## Issues

### Issue 1: Classpath JAR stream leaks if the `JarOutputStream` constructor throws
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 180–181
- **Description**: `Using.resource(new JarOutputStream(new FileOutputStream(classpath), manifest))(_ => ())` evaluates its argument before `Using` takes ownership. The `JarOutputStream(out, man)` constructor does real I/O: it writes the manifest entry, which here holds a large `Class-Path`. If that write throws, the `FileOutputStream` that is already open is never closed. This conflicts with the repository's `.github/skills/code-review/SKILL.md` checklist under Resource Management: "Cleanup runs even when assertions or exceptions are thrown (especially in tests)".
- **Risk**: Only the failure path is affected, for example disk-full or I/O errors. The file handle stays open until GC finalization. On Windows that can make `TestBase.afterAll`'s `FileUtils.forceDelete(tmpDir)` fail. The result is a second suite-level error that hides the original failure, and the temp directory is left behind. The normal passing path and the probe's verdicts are unaffected.
- **Suggested Fix**: Give the file stream its own owner, for example `Using.resource(new FileOutputStream(classpath)) { out => Using.resource(new JarOutputStream(out, manifest))(_ => ()) }`. Closing the `FileOutputStream` twice is harmless. Alternatively, use `Using.Manager { use => use(new JarOutputStream(use(new FileOutputStream(classpath)), manifest)) }.get`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: The file stream now has an outer `Using.resource` owner before the manifest-writing constructor executes.
- **Why**: Constructor failure must close the already-open file stream without replacing the write error.
- **How verified**: See the retained Driver resolution below for the original-versus-corrected fault-injection proof and passing compilation, full suite and style gates.

## Driver resolution

The original review above and its `issues_found` verdict are retained.

Issue 1 is addressed in the pending patch. The file stream now has an outer
`Using.resource` owner before `JarOutputStream` writes its manifest. A
fault-injection check extracted the original and corrected method bodies and
substituted only a failing file-stream factory: the original left the stream
open after constructor failure; the corrected body closed it while propagating
the original write error.

On Windows 11 with Temurin Java 8 and Scala 2.12.17, the pending patch passed
`sbt core/compile core/Test/compile
"core/testOnly com.microsoft.azure.synapse.ml.param.VerifyGlobalParams"
core/scalastyle core/Test/scalastyle`: 11 tests passed, including both isolated
child-JVM modes, with no failures or skips and no style errors or warnings.

This source correction ends attempt 2. It is not a clean completed gauntlet;
the corrected patch requires a new complete pre-commit pass and subsequent
current-head CI.