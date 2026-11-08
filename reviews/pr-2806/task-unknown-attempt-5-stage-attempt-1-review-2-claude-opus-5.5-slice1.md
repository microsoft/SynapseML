---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 2
timestamp: "2026-11-06T22:21:06Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4ae5caf29aa29fd437b59fac310c4784ecb114b31e590b1d4aec4cd764160b71"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-2-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read every line of the assigned frozen diff, using `[1,150]` and `[151,211]`. That covers 211 lines and both sides of every hunk in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`. The new side matches the current source: `GlobalParams.scala` lines 20–38, and `VerifyGlobalParams.scala` lines 11–95, 171, 176–177 and 191–251.
- [x] **Guidance read by exact path:** `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the `master` target), `.github/skills/code-review/SKILL.md`, `.github/skills/scala-code/SKILL.md`, `build.sbt`, `environment.yml`, `scalastyle-test-config.xml`, and the imported `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`.
- [x] **Production change (`GlobalParams.scala` lines 20–38):**
  - All five public methods lock the same object, so `getParam` (lines 32–34) reads both maps consistently.
  - `untypedKey` has no side effects and needs no lock.
  - Only the `hashCode`/`equals` methods of keys and `Param` objects run while the lock is held.
  - `HasGlobalParams` (lines 41–53) is unchanged and calls `set` after the lock is released, so locks cannot be taken in conflicting orders.
  - Public signatures, return types and serialized Params are unchanged, as `AGENTS.md` and the API checklist require.
  - Keeping `mutable.Map` behind a lock still allows stored `null` values, which a `ConcurrentHashMap` would reject. The new test at lines 191–197 locks in that behavior.
- [x] **Test design:**
  - Running the concurrency checks in a separate JVM is justified. `GlobalParams` is shared across the whole process, entries cannot be unregistered, and a regression could corrupt the maps or hang the test JVM.
  - The child program (lines 19–95) calls the real `GlobalParams` object, not a copy, which follows the `AGENTS.md` rule against helper-only tests.
  - Argument checks (lines 61–62) run before the thread pool is created and before shared values change (lines 64–65). The in-process negative test (lines 199–206) therefore leaves no state behind.
  - The child process is stopped and waited for (lines 230–243) before any assertion runs (lines 245–249). `terminated` is asserted before `exitValue()` is read.
  - Temporary files go in `TestBase.tmpDir`, which `afterAll` deletes.
- [x] **Branch and JDK rules (`branch-spark3p5.md`):**
  - The process calls (`isAlive`, `destroyForcibly`, `waitFor(long, TimeUnit)`) all exist in Java 8.
  - The classpath lookup (lines 210–216) reads `URLClassLoader` chains on Java 8 and `java.class.path` on newer JDKs.
  - Passing the classpath through a manifest-only JAR (lines 217–223) avoids Windows command-length limits.
- [x] **Style rules:**
  - Imports are named and follow the existing layout used in `TestBase.scala`.
  - Lines stay within 120 characters (longest about 114), and the file is about 252 lines.
  - There are no `scalastyle:off` comments. The `NullChecker` rule passes without suppression: the test builds its null with `Option.empty[String].orNull`, and `!= null` comparisons are allowed.
- [x] **Dependencies:** On master's Scala 2.12 build, `scala.util.Using` is not in the standard library. It comes from the scala-collection-compat library, which is pulled in indirectly. I relied on the caller's classpath evidence for this and did not inspect the JAR myself. On the Scala 2.13 port branches, the same import uses the standard library. If the indirect dependency ever disappeared, `Test/compile` would fail visibly rather than silently. No dependency versions, `pipeline.yaml`, workflows or release tooling changed.
- [x] **Considered and not raised:**
  - `synchronized` locks the public `GlobalParams` object itself. This is a common Scala pattern, and the object has no lazy vals that would share that lock.
  - The local `val java` (line 225) hides the `java` package only inside that test, which uses no fully qualified `java.` names.
  - The mode flag passed as a Boolean, and the mode names repeated as string literals, stay inside test code.
  - None of these change behavior or break a documented convention.
- [ ] **Not done: repository-wide search.** I did not search for an existing helper that launches a child JVM, or for other callers of `GlobalParams`, because the required view-only profile forbids searching and directory listings. `TestBase` has no process helper, and the public signatures are unchanged, so callers in other modules remain source- and binary-compatible.
- [ ] **Not done: build or tests.** This was a read-only review. I took into account the caller's evidence (both child-JVM modes fail before the fix and pass after it, 12/12 tests pass on the pending patch, and the style checks pass) but did not reproduce it.