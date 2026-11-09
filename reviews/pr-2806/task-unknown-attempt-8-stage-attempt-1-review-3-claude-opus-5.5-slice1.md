---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 3
timestamp: "2026-11-07T15:22:57Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "3d5bae4dc0cc1e3304b7f95084c204b0dd32a94d6327fde143467522ce8ad751"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read every line of the assigned frozen diff in the two required ranges, [1,150] and [151,261]. I checked the before and after versions of `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`. The current checkout matches the after version: GlobalParams.scala L20–38, and VerifyGlobalParams.scala L11–103, L178, L183–184 and L197–301.
- [x] **Production locking (`GlobalParams.scala` L20–38):**
  - All five accessors lock the same object (the `GlobalParams` singleton).
  - While the lock is held, the code only does map operations and calls `hashCode`/`equals` on `Param` and `GlobalKey`. It doesn't wait on anything, call back into other code, take a second lock or initialise another class, so there's no new deadlock path.
  - `value` is evaluated before the lock is taken, and the lock is released if an exception is thrown.
  - Unchanged: signatures, return types, a stored `null` coming back as `Some(null)`, and resetting a missing key doing nothing.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (L43–52) still looks up each param separately. It never promised a consistent snapshot across several params, and adding one is outside this PR.
- [x] **Wait-and-cleanup helper `awaitProbe` (VerifyGlobalParams.scala L238–256):** I traced each path through `Using.resource`:
  - Normal exit returns `(true, true)`.
  - Timeout force-kills the child, waits up to 10 s, and returns `(false, terminated)`.
  - If the thread is already interrupted, the wait throws and clears the flag. Cleanup's bounded wait then runs before the `catch` sets the flag again and rethrows.
  - An interrupt during cleanup arrives after the kill was already sent. `Using` keeps one exception as primary and the other as suppressed, and the flag is set again.
  - On every `InterruptedException` path, the thread's interrupt flag ends up set.
- [x] **Blocked-child tests (L258–286):**
  - The child starts before `try`, with no code in between that can throw.
  - `finally` clears the interrupt flag before the emergency `destroyForcibly`/`waitFor(10 s)`, so a leftover interrupt can't break cleanup.
  - The 15 s limit is more than the 100 ms wait plus the 10 s cleanup.
  - The blocking child (L97–102) waits on a stdin pipe that the test JVM holds open. It can't exit early, and it exits by itself if the test JVM dies.
- [x] **Concurrency child and its tests (L19–95, L288–301):**
  - Each round and worker uses its own key range, so no two workers share a key.
  - `Range.map` is strict, so all tasks are submitted before the start latch opens. The highest index is 63,999, so there's no overflow.
  - A failed `require` in a worker becomes an `ExecutionException` and a non-zero exit code.
  - A hang from map corruption is bounded by the 45 s timeout, after which the test kills the child.
  - The asserts check `terminated`, then `completed`, then `exitValue()`. This avoids `IllegalThreadStateException` and always includes the child's output.
  - Values mode does roughly 0.7 million short locked operations, which fits well within 45 s.
- [x] **Child launcher `startProbe` (L215–236):**
  - `Iterator.iterate(...).takeWhile(_ != null)` is lazy, so it never calls `getParent` on `null`.
  - It uses only Java 8 APIs (no `@argfile`), as `.github/skills/synapseml-branches/references/branch-spark3p5.md` requires.
  - The manifest `Class-Path` uses ASCII-encoded absolute `file:` URIs, with a trailing `/` on directories. Paths are canonical with duplicates removed, and `Manifest-Version` is set.
  - `ProcessBuilder` takes a list of arguments, so nothing passes through a shell.
  - Each test uses its own file names in the suite's `tmpDir`. `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` (L160–164, L194–199) deletes `tmpDir` only after every child has exited.
- [x] **In-process tests (L198–213):**
  - The negative-argument test fails at `require` before any thread pool is created or `GlobalParams` is touched, so nothing leaks into the shared JVM.
  - `beforeEach` and `afterEach` reset the test keys (L113–127).
- [x] **Guidance and versions:** I checked `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` (Scala 2.12.17, Spark 3.5.0, `Test / parallelExecution := false`) and `environment.yml`.
  - No change to dependencies, workflows, the pipeline, signatures or serialized Params.
  - No RDD use, and resources are closed through `Using`.
- [x] **Considered and judged not to be defects:**
  - Locking on the public `GlobalParams` object: none of the reviewed code holds that lock while waiting.
  - Cleanup's post-kill wait can be interrupted: the kill is already sent, and an interrupt should take effect promptly.
  - The blocked-child tests don't read the child's log: they only need a live child, and the concurrency tests show launch failures with output.
  - The child's stdin pipe isn't closed on Windows: it's released by garbage collection, and `destroy` closes it on Unix.
  - The `verified=` count in the success message is a constant: `verifyCompletedRound` still checks all 64,000 entries by construction.
- [x] **Validation evidence (from the caller; I ran nothing in this read-only review):**
  - core compile, Test compile and both style checks passed.
  - `VerifyGlobalParams` passed 14/14 on Java 8 and on the Java 17 Spark port snapshots.
  - The targeted mutation checks failed as intended.
  - `scala.util.Using` comes from the transitive scala-collection-compat dependency on Scala 2.12.
  - This is consistent with the source I inspected.
- [ ] **Not checked:** callers of `GlobalParams` outside the two changed files. The required read profile forbids searching, and neither changed file imports them. Signatures and behaviour are unchanged, so their contracts are unaffected.