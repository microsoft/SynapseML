---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 3
timestamp: "2026-11-07T12:58:35Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4e12d729c057082cf78d2692ef8edb293953e987fe91370cdeacb3335e8afd13"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-7-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage and match:** I read the assigned frozen diff in exactly two ranges, [1,150] and [151,214], with no truncation. It matches the checked-out files:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, lines 20–38.
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, new-file ranges 8–99, 168–180 and 187–255. The hunk line counts agree with the current file.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. The target is master. `build.sbt` sets Scala 2.12.17 and Spark 3.5.0. `environment.yml` pins pyspark 3.5.0.
- [x] **Locking covers all shared state:** Both private maps (`GlobalParams.scala` lines 13–14) are used only inside the five methods at lines 20–38. All five lock the same object, so `getParam` reads both maps in one consistent step.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 43–51) takes the lock only inside each `getParam` call. It releases it before calling `set(...)`.
  - The maps are never iterated or exposed. Code running under the lock is limited to Spark's final `Param.hashCode`/`equals` and the key's `hashCode`/`equals`. The lock is reentrant and never nested with another lock, so I found no deadlock or lock-ordering risk.
  - Public signatures and saved parameter formats are unchanged.
- [x] **Edge-case behavior is unchanged and tested:**
  - A stored `null` is still returned as `Some(null)` (`VerifyGlobalParams.scala` lines 191–197). This protects against a later switch to a map type that rejects `null`.
  - Resetting a key that was never set does nothing; `beforeEach`/`afterEach` exercise this (lines 106–120).
  - A registered param with no value returns `None`, and resetting the value clears the param lookup (lines 169–177).
- [x] **Probe ranges and argument checks:**
  - Each worker in each round uses its own index range (line 73). The verification loop's upper limit, `(round + 1) * Workers * Entries` (line 51), matches the highest index, so there is no off-by-one.
  - `TotalEntries` = 64,000 (line 23) matches the final verified range.
  - Argument validation (lines 61–62) runs before the thread pool is created and before `SharedKey` is written (lines 64–65). So the in-process negative test (lines 199–206) leaves no global state behind.
- [x] **Child-process failure paths:**
  - Output is redirected to a log file (lines 229–230). The child cannot block on a full pipe, and it can finish even if the parent dies.
  - `redirectOutput(File)` truncates the log, so an old success line cannot produce a false pass.
  - Cleanup is bounded: a forced kill, then at most a 10-second wait (lines 234–241).
  - If the test thread is interrupted, the wait throws and clears the interrupt flag. Cleanup runs first, then the `catch` restores the flag and rethrows (lines 242–246).
  - The assertion order (lines 248–250) means `exitValue()` is only called after the process has exited.
  - The success check uses `contains`, so extra JVM startup messages in the log do not break it.
- [x] **Java 8 compatibility** (the oldest-JDK rule in `branch-spark3p5.md`):
  - Only Java 8 APIs are used: `Process.isAlive`, `destroyForcibly`, `waitFor(long, TimeUnit)`, `Files.readAllBytes`.
  - The manifest sets `Manifest-Version`. Without it, Java 8 silently drops the other main attributes, including `Class-Path`.
  - Class-path entries use `toURI.toASCIIString`, so spaces and non-ASCII characters are escaped and existing directories get the required trailing `/`.
  - On Java 9+, where the system class loader is not a `URLClassLoader`, the code falls back to `java.class.path`.
- [x] **Comment-only correction:** The three comments at `VerifyGlobalParams.scala` lines 208, 218 and 232 accurately describe the code next to them. Under the read profile I had no copy of the earlier file, so I cannot confirm the "byte-for-byte" claim.
- [x] **Caller test evidence:** The current file defines 12 tests: 8 existing (one modified) plus 4 new (lines 191 and 199, and two generated at lines 209–210). That matches the 12/12 claim. The separate "11/11" claim does not match the current test count, so I treated it as referring to an earlier head and did not rely on it. I ran no tests.
- [x] **Considered and not reported (no real impact):**
  - The child's stdin pipe is never closed. The child never reads stdin, and the handle is released on exit or garbage collection.
  - No thread dump is captured when the child times out. The failure is still detected and the log is included.
  - The lock is the public `GlobalParams` object. This is the usual Scala pattern, and no reviewed code locks on it externally.
  - A child would be left running only if a future regression made it hang and the parent JVM died at the same time.
  - On Scala 2.12, `scala.util.Using` comes from the existing transitive `scala-collection-compat` dependency. The caller confirmed it is on the classpath; if it were missing, compilation would fail loudly.
- [ ] **Not inspected:** callers of `GlobalParams` outside the source list (for example, the OpenAI defaults in the cognitive module). The read profile forbids searching, so my concurrency analysis covers only `HasGlobalParams` in the same file.