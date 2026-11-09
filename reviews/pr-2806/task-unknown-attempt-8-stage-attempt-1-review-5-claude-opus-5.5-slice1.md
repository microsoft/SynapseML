---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 5
timestamp: "2026-11-07T15:54:10Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a717833319e12cbfdfcbfcec83bbe1545faa1d10e992d1dc0bb2075f31b5b2a1"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-8-stage-attempt-1-review-5-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff read and compared:** I read every line of the assigned frozen diff in the two required ranges, [1,150] and [151,261]. This covers the before and after state of the `GlobalParams.scala` hunk (`@@ -17,23 +17,23 @@`) and all three `VerifyGlobalParams.scala` hunks (`@@ -8,6 +8,99 @@`, `@@ -82,10 +175,13 @@`, `@@ -98,4 +194,109 @@`). The checkout copies of `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` and `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala` match the after state.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` (the reference for the `master` target) and `.github/skills/code-review/SKILL.md`. Versions come from `build.sbt` and `environment.yml`. The code-review Test Checklist is met:
  - Negative cases are tested.
  - Cleanup uses `Using.resource` or `try`/`finally`.
  - The production diff adds no imports, so no test-only dependency enters main scope.
- [x] **Every changed method has tests:** All five newly synchronized methods (`GlobalParams.scala` lines 20–38) are tested in two ways:
  - **In the shared test JVM, one call at a time:** the existing tests, plus the new checks at `VerifyGlobalParams.scala` lines 178, 183–184 and 198–204.
  - **Under concurrency, in a separate child JVM:** the `registration` mode covers `registerParam` and `getParam` (lines 74–79, 52–53). The `values` mode covers all five methods (lines 30–48, 55).
- [x] **Existing behaviour is pinned:**
  - Lines 198–204 check that a stored `null` reads back as `Some(null)` and that reset gives `None`. This would catch a later switch to a map type that rejects nulls.
  - Lines 178 and 183–184 check that a registered param reads `None` before its key is set and after reset.
  - The end-of-round checks look params up with newly created, equal `Param` instances (lines 27–28, 52–53), which pins lookup by equality.
- [x] **The concurrency tests should catch a regression (judged by reading the code):** Workers use disjoint keys, so the first round forces about 11 table resizes on each Scala 2.12 `mutable.HashMap` as concurrent inserts reach 16,000. Removing `synchronized` from any one method leaves an unlocked reader or writer racing those resizes. That would show up as one of:
  - a failed `require` inside a worker;
  - a failed end-of-round check;
  - a non-zero exit code;
  - hitting the 45-second limit.

  Detection is probabilistic, but very likely with true parallelism on multi-core agents.
- [x] **Helper tests catch the likely helper bugs** (lines 258–286):
  - If the child is not force-killed, line 274 or line 276 fails.
  - If the interrupt flag is not restored, line 270 fails.
  - If the returned pair is swapped, line 273 fails.
  - If the wait timeout is ignored, the `completed` check in the concurrency tests (line 296) fails.

  Setting the interrupt flag before the call makes the interrupt path deterministic with the Java 8 and Java 17 `Process.waitFor` code. The 100 ms wait plus at most 10 s of cleanup stays inside the 15 s bound checked at line 277.
- [x] **Isolation and cleanup:**
  - The bad-argument test (lines 206–213) fails at `require` (lines 61–62), before the thread pool is created or `SharedKey` is set (lines 64–65). It leaves no shared-JVM state.
  - The `finally` block (lines 278–284) clears the test's interrupt flag and kills any surviving child process.
  - Logs are read only after the child is confirmed stopped (lines 293–294).
  - Each mode uses its own JAR and log names inside `TestBase.tmpDir`, a per-suite temp directory deleted in `afterAll`.
  - `build.sbt` sets `Test / parallelExecution := false`.
- [x] **Child classpath (lines 215–235) works whether or not sbt forks the test JVM:** It uses `URLClassLoader` URLs (sbt's in-process class loaders and the Java 8 app loader) and also `java.class.path` (forked JVMs and Java 9+). Project classes come first. Spaces in paths are percent-encoded in the manifest `Class-Path`. The child loads only `GlobalParams` and Spark's `Param` class, so it needs no extra JVM options.
- [x] **No extra transformer-path test is needed:** `HasGlobalParams.transferGlobalParamsToParamMap` (`GlobalParams.scala` lines 41–53) is unchanged. It reads values only through the synchronized `getParam`, whose behaviour is pinned above.
- [ ] **No tests were run in this review** (read-only contract). I relied on your validation evidence: compiles, style checks and VerifyGlobalParams 14/14 on Java 8 `master` and on both Spark port snapshots under Java 17, plus the helper mutations failing as intended. That evidence does not include a run of the concurrency tests against the base code without `synchronized`, so the regression point above rests on reading the code. CI on the pending head is still deferred.