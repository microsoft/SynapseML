---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 4
timestamp: "2026-11-07T09:15:23Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "bfda7e4c3a9e44bb60b96f4665f0572edc85bf39b006fa3f4c059e630c1310ba"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read every line of the assigned frozen diff in exactly two `view` calls, `[1,150]` and `[151,211]`. Both views returned complete content. I reviewed the before and after side of each hunk.
- [x] **Diff matches checkout:** The after-state matches the files in the checkout:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, lines 17–39
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, lines 11–95, 171, 176–177 and 187–252
- [x] **Read profile:** I used only `view`. I did not use `rg`, `glob`, a shell, directory listings, sibling reviews or session storage.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. `build.sbt` sets Spark 3.5.0 and Scala 2.12.17. `environment.yml` pins pyspark 3.5.0.
- [x] **Locking in `GlobalParams.scala` (lines 20–38):**
  - All five accessors lock the same monitor, the `GlobalParams$` module instance. The unqualified `synchronized` means `this.synchronized`; it does not lock the private map that shares the `GlobalParams` name.
  - Both maps are private and only these methods touch them, so every read and write is ordered by the same lock.
  - `getParam` (line 33) reads both maps inside one critical section. `Option.flatMap` and `Option.map` run immediately, so both lookups finish while the lock is held.
  - Return types are unchanged. In `resetGlobalParam`, the map returned by `-=` is still discarded to produce `Unit`.
  - Map types and null-value behaviour are unchanged.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–51) still calls `set` outside the lock. Only key `hashCode`/`equals` run under the monitor, and JVM monitors are reentrant.
  - Public JVM signatures and serialized Params are unchanged.
- [x] **Probe arithmetic and off-by-one checks (`VerifyGlobalParams.scala` lines 20–23, 51 and 73):**
  - The offset formula `(round*8+worker)*2000` gives each worker a separate range, `[16000r+2000w, 16000r+2000w+2000)`.
  - After round r, the completed ranges cover exactly `[0, 16000(r+1))`. That equals `0 until (round + 1) * Workers * Entries`, because `*` binds tighter than `until`.
  - The final round checks 64,000 indices, which equals `TotalEntries`.
  - `Range.map` runs eagerly, so all registrations and task submissions happen before the code moves on.
  - Recreated keys and params match the stored ones: `WorkerKey` is a case class, and Spark `Param` equality uses parent and name.
- [x] **Probe control flow (lines 60–94):**
  - `require` short-circuits before `args.head`, before the thread pool is created and before any global write. The in-process negative test (lines 199–206) therefore has no side effects.
  - That test's expected message is a substring of `requirement failed: …`, so it matches.
  - A worker's `require` failure comes back through `Future.get` and makes the child exit non-zero.
  - `finally shutdownNow()` stops the non-daemon workers, including any still waiting on the latch.
- [x] **Child-JVM launch and cleanup (lines 208–251):**
  - The class-loader walk stops at `null` without calling `getParent` on it.
  - The classpath is collected from both Java 8 `URLClassLoader`s and Java 9+ `java.class.path`.
  - Neither `build.sbt` nor `project/CodegenPlugin.scala` sets `Test / fork`, so the in-process sbt loaders are the main path covered.
  - `Manifest-Version` is set, which Java needs before it will write `Class-Path`. Entries are absolute `file:` URIs, and directories get a trailing `/`.
  - Once `start()` returns, the process is always covered by the `AutoCloseable`.
  - The assertions run in the order `terminated`, then `completed`, then `exitValue()`, so `exitValue()` cannot throw `IllegalThreadStateException`.
  - On `InterruptedException`, the interrupt flag is restored before rethrowing.
  - Each mode uses its own jar and log names. `TestBase.afterAll` deletes `tmpDir`.
- [x] **Sequential tests:**
  - The assertions at lines 171 and 176–177 depend on the `beforeEach` reset at lines 112–115, which runs first.
  - In the null test (lines 191–197), Scala 2.12's `mutable.HashMap.get` returns `Some(null)` for a stored null, so the assertion matches current behaviour.
  - The after-state has 12 tests, which matches the caller's reported 12/12.
- [x] **Compatibility:**
  - All JDK APIs used exist in Java 8.
  - On Scala 2.12, `scala.util.Using` comes from the transitive scala-collection-compat jar (caller-verified). Scala 2.13 on the port branches includes it natively.
  - No RDD use, dependency, workflow or pipeline changes.
- [x] **Non-blocking observations (not defects):**
  - The marker's `verified=` value is the same constant the parent test expects, so it only confirms the probe finished. It is accurate for the current loop bounds.
  - The values-mode post-round check only tests that keys are absent. This is by design; `exerciseValues` checks each entry while it is set.
  - The lock is the public object monitor. This is normal Scala practice, and I found no outside locking in the reviewed scope.
  - The child's stdin pipe is not closed explicitly. It is released when the child exits (UNIX) or at garbage collection (Windows).
- [ ] **Tests not run by me:** This was a read-only review. The Java 8 and Java 17 compile, 12/12 test and style-gate results are caller-provided. CI on the pending head is deferred until publication.