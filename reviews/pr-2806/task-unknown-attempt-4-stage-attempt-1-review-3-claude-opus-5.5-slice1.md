---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 3
timestamp: "2026-11-06T16:22:34Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "f9d5ae19e35d19407c0a43b7ce3d1c155d5909258795265db068a2eb37aac10d"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-4-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Full diff read.** I read all 171 lines of the assigned frozen diff in two ranges (1–100 and 101–171). That covers the before and after versions of both files. The current sources match the after version: `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` lines 17–39, and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` lines 8–74 and 159–223.
- [x] **Locking covers every access.** In `GlobalParams.scala` (lines 13–14 and 20–38), the two private maps are only reachable through five methods. All five now synchronize on the same lock, the `GlobalParams` object itself. The bare `synchronized` call binds to that object, not to the private map that is also named `GlobalParams`.
  - `getParam` reads both maps inside one locked block, because `Option.flatMap` runs immediately.
  - While the lock is held, the only other code that runs is `hashCode`/`equals` on keys and `Param`. Nothing takes another lock, calls back out or blocks, so lock-order or initialization deadlocks can't happen.
  - The lock is released if an exception is thrown. Method signatures are unchanged, so binary compatibility holds.
- [x] **Existing behavior is preserved.**
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–52) calls `getParam` once per parameter and calls `set` outside the lock, so locks are never nested.
  - A stored null still comes back as `Some(null)`. The new test at `VerifyGlobalParams.scala` lines 163–169 checks this, which would catch a switch to a map type that rejects nulls.
  - The transfer reads each parameter separately, not as one atomic snapshot, and `ParamToKeyMap` grows without limit. Both behaviors predate this PR and are in scope to preserve, so I did not raise them.
- [x] **Probe failure modes** (`VerifyGlobalParams.scala` lines 26–69):
  - `require` checks the arguments before `args.head` is read, before the thread pool is created and before any global value is set. The negative test (lines 171–178) can therefore safely call `main` in the test JVM, and the probe never calls `System.exit`.
  - All eight tasks are submitted before the start latch opens. The main thread opens the latch, so it cannot deadlock.
  - Each worker uses its own range of keys and parameter names. The largest value is 63,999, so there is no overflow.
  - `get(30s)` reports worker failures or hangs as errors. `shutdownNow` in `finally` interrupts any waiting workers.
  - Pool threads are only created when work is submitted, so the `setGlobalParam` call before `try` cannot leak threads.
- [x] **Child process lifecycle** (lines 199–220):
  - Nothing that can throw sits between `start()` and the cleanup block.
  - Cleanup force-kills the child if it is still running, then waits at most 10 seconds for it to exit.
  - If the test thread is interrupted, `waitFor` clears the interrupt flag, so cleanup can still wait. The outer `catch` then restores the flag and rethrows.
  - The checks run in the order `terminated`, then `completed`, then `exitValue()`. `exitValue()` is never called on a live process, so it can't throw `IllegalThreadStateException`.
  - The test checks both the exit code and the success marker, and the probe prints the marker only after every round. A broken run can't pass.
  - Each test is limited to about 55 seconds, even if the child hangs.
- [x] **Classpath and launch** (lines 182–198):
  - The classpath is built from the class-loader chain plus `java.class.path`, which works on both Java 8 and Java 9+. This follows the class-loader warning in `.github/skills/synapseml-branches/references/branch-spark3p5.md`.
  - Neither `build.sbt` nor `project/CodegenPlugin.scala` sets `Test / fork`. The rebuilt classpath handles tests run inside sbt or in a forked JVM. The test loader's own entries come first, so the module under test is found before library JARs.
  - `toURI.toASCIIString` escapes spaces and non-ASCII characters, and directory entries keep their trailing `/`. Manifest lines are wrapped at 72 bytes, and absolute `file:` entries are accepted.
  - Using a manifest JAR avoids Windows command-line length limits. The nested `Using.resource` closes the file stream even if creating the JAR stream fails.
- [x] **Test isolation.** `tmpDir` in `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` is a separate temporary directory per suite and is deleted in `afterAll`. Each mode uses its own JAR and log file names, so they don't collide. The `beforeEach`/`afterEach` reset (lines 81–95) covers the key used by the null test. The child processes never change globals in the test JVM. `build.sbt` sets `Test / parallelExecution := false`.
- [x] **Considered and not raised:**
  - Locking on the public `GlobalParams` object instead of a private lock object: this is normal Scala style, and no code in scope fails because of it.
  - The fixed 45s/30s/10s timeouts: they are generous for a workload that should finish in about a second.
  - The child's standard input pipe is never closed: it is one handle per test, and the child never reads it.
- [x] **Guidance and versions read:** `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` (Scala 2.12.17, Spark 3.5.0) and `environment.yml`. The PR changes no dependencies, workflows or public signatures.
- [ ] **Tests not run.** This review is read-only. The caller-provided validation results are context only and were not reproduced.