---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 4
timestamp: "2026-11-07T13:38:43Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4f813220be009b17c91d6fef5426f3504a94a6c39edd931e61fcccef981c5560"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-7-stage-attempt-1-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage.** I read every line of the assigned frozen diff in exactly two views: lines 1–150 and 151–214. All 214 lines came back with no truncation.
  - The diff has one `GlobalParams.scala` hunk changing five methods.
  - It has three `VerifyGlobalParams.scala` hunks: the probe object, the extra `registerParam and getParam` assertions, and four new tests.
  - The current `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` match the post-image line for line.
- [x] **Guidance and branch context.** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`.
  - `build.sbt`: Spark 3.5.0, Scala 2.12.17.
  - `environment.yml`: Python 3.11.8, pyspark 3.5.0.
  - The target is `master`, so the master reference applies, including its rule that shared helpers use only APIs from the oldest JDK that builds them.
- [x] **Production locking (`GlobalParams.scala:20-38`).**
  - A bare `synchronized` locks the `GlobalParams` object itself. It does not lock the `private val GlobalParams` map that shares its name.
  - Both maps (`:13-14`) are private and fully built when the object is first loaded. All five methods that touch them are now locked, so no access is left unguarded.
  - `getParam` now reads both maps in one critical section.
  - The lock covers only map operations and the keys' `hashCode`/`equals`. Spark `Param` makes these `final` and string-based, and the keys are case objects or case classes. Nothing under the lock calls out or takes another lock, so there is no deadlock path.
  - Signatures, type parameters, the unchecked `asInstanceOf` cast and storage of a present `null` (plain `update`, no `Option(...)` wrapping) are unchanged. `HasGlobalParams` (`:41-53`) is unchanged.
- [x] **Probe arithmetic (`VerifyGlobalParams.scala:20-23, 51, 73`).**
  - Each worker covers `(round*8+worker)*2000` through `+1999`. These ranges are disjoint and contiguous.
  - `until` binds more loosely than `*`, so `0 until (round+1)*16000` checks all rounds so far and ends at index 63,999. That matches `TotalEntries` = 64,000 and the success marker (`:90`, `:252`). There is no off-by-one.
  - `Range.map` is strict (`:31, 69, 75`), so all eight tasks are submitted before `countDown` (`:86`).
  - The pool has eight threads for eight tasks per round, and every round waits on each task's `get`, so the start latch cannot deadlock.
- [x] **Argument validation (`:61-62`).**
  - `require` checks `args.length == 1` first, so `args.head` is never read on an empty array.
  - Validation runs before the executor is created (`:64`) and before `SharedKey` is set (`:65`), and `main` never calls `sys.exit`.
  - The in-process rejection test (`:199-206`) therefore leaves no threads or global state behind, and the `require` message contains the asserted text.
- [x] **Child process lifecycle (`:229-252`).**
  - Normal run: `waitFor(45)` returns true, then `close()` sees the process has exited and `waitFor(10)` returns true at once.
  - Timeout: `close()` calls `destroyForcibly` and waits up to 10 seconds.
  - Interrupt: `waitFor` clears the interrupt flag as it throws, so the cleanup wait is not cut short. `Using` runs `close()` before the catch at `:243-245` restores the flag, and the `InterruptedException` remains the exception that propagates.
  - `exitValue()` (`:250`) is reached only after `completed`, so it cannot throw `IllegalThreadStateException`.
  - The log file is created at `start()`. Because stderr is merged and the marker check uses `contains`, extra JVM output does not break it.
- [x] **Classpath jar (`:211-225`).**
  - `Iterator.iterate` with `takeWhile` never calls `getParent` on `null`, in both the Scala 2.12 and 2.13 implementations.
  - `File.toURI.toASCIIString` escapes spaces and non-ASCII characters and adds a trailing `/` to existing directories. The manifest writer wraps long lines, and absolute `file:` Class-Path entries are accepted from a `file:` jar.
  - Only Java 8 APIs are used: `Process.isAlive`, `destroyForcibly`, `waitFor(long, TimeUnit)` and NIO `Files`. This satisfies the master-branch JDK rule.
  - Each mode writes differently named files under `TestBase.tmpDir`, which `afterAll` deletes.
- [x] **Test additions and comments.**
  - `:171` checks the value is empty before it is set, and `:176-177` check it is empty again after reset, going through the cross-map `getParam` path.
  - `:191-196` stores `null` and expects `Some(null)`. That would catch any change to `Option(value)` semantics.
  - The three new comment lines (`:208`, `:218`, `:232`) contain no code and accurately describe what follows.
- [x] **Non-blocking, outside this round's theme.** On master, `scala.util.Using` (`:17`) comes only from the indirect `scala-collection-compat` dependency, which you have confirmed is present. It is not declared in `build.sbt`. If that dependency ever disappeared, the build would fail to compile rather than change behavior silently, and adding a direct dependency needs approval under AGENTS.md. I am not raising this as a finding.
- [x] **Validation evidence consistency.** The post-image defines 12 tests, which matches the "12/12" result for the current patch. The separate "11/11" environment note cannot describe this exact test source, so it should not be cited as evidence for this head.
- [ ] **Execution.** I did not run any tests, builds or style checks; this review was read-only. All execution results above are as you reported them.
- [ ] **Callers outside the two files.** I did not open callers such as the OpenAI defaults, per the required read profile. Both maps are `private`, so every access already goes through the five reviewed methods.