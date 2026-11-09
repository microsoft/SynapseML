---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 5
timestamp: "2026-11-07T09:36:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d5634b966ee19f6bc6b32806e4bdca8eb8a6d02f562255484df11eebbcdcf8f5"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2806/task-unknown-attempt-5-stage-attempt-1-review-5-claude-opus-5.5-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read every line of the assigned frozen diff in two bounded reads: lines 1–150 and lines 151–211. It contains these hunks:
  - `GlobalParams.scala` `@@ -17,23 +17,23 @@`
  - `VerifyGlobalParams.scala` `@@ -8,6 +8,92 @@`, `@@ -82,10 +168,13 @@` and `@@ -98,4 +187,66 @@`
- [x] The current checkout matches the diff's post-image in these ranges:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala`, lines 20–38
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala`, lines 11–96, 168–178 and 190–251
- [x] Production change in `GlobalParams.scala`:
  - All five public methods now use bare `synchronized`. That locks the `GlobalParams` object itself, not the private map that is also named `GlobalParams`.
  - Method signatures and return types are unchanged.
  - Both maps are private, so no unsynchronized access remains.
  - `HasGlobalParams.transferGlobalParamsToParamMap` (lines 41–52) is unchanged and reaches the maps only through the synchronized `getParam`.
- [x] Single-threaded coverage:
  - Existing tests cover set, get, overwrite, multiple keys, reset and unregistered parameters.
  - New checks cover a registered parameter before a value is set, while set, and after reset (lines 171 and 176–177).
  - New test lines 191–197 confirm that a stored null is still returned as present.
- [x] Concurrent coverage:
  - Values mode runs all five methods concurrently on separate keys (lines 30–48).
  - Registration mode runs `registerParam` and `getParam` concurrently (lines 74–79).
  - After each round finishes, lines 50–58 recheck all earlier entries. The total is 8 workers × 2,000 entries × 4 rounds = 64,000.
- [x] The argument check (lines 61–62) runs before the thread pool is created or any global is set (lines 64–65). The in-process negative test (lines 199–206) therefore cannot leak threads or global state.
- [x] Child-JVM harness cleanup is sound:
  - The classpath JAR is written inside nested `Using.resource` blocks (lines 221–223).
  - Child output goes to a file, which avoids a full-pipe deadlock.
  - On timeout or interrupt, the child is killed and waited for (lines 231–238), and the interrupt flag is restored (lines 240–242).
  - `exitValue()` is only called after confirming the child terminated (lines 245–247).
  - `TestBase.afterAll` deletes `tmpDir`.
- [x] JDK compatibility:
  - Classpath discovery handles the Java 8 class-loader chain and falls back to the `java.class.path` property on Java 9+ (lines 210–216).
  - The `Process` methods used exist in Java 8.
  - No `Test / fork` setting appears in the viewed `build.sbt` settings or in `project/CodegenPlugin.scala`. `Test / parallelExecution := false` is set.
- [x] Guidance and config read:
  - `AGENTS.md`, the branch skill, the master branch reference and the code-review skill
  - `build.sbt` (Scala 2.12.17, Spark 3.5.0), `environment.yml` and `scalastyle-test-config.xml`
- [ ] I did not run any tests; this was a read-only review. The 12/12 result, the earlier Azure 11/11 and the baseline failures are all caller-provided.
- [ ] I did not confirm that `scala.util.Using` comes from scala-collection-compat. That is the caller's statement, and dependency caches were out of bounds.

## Issues

### Issue 1: The success line reports a fixed count, not the number of entries actually checked
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala
- **Line(s)**: 23, 50–58, 90, 248–249
- **Description**: The child prints `verified=$TotalEntries`. `TotalEntries` is the constant `Workers * Entries * Rounds`, and the parent test checks for that same constant. Nothing counts the entries that `verifyCompletedRound` or the worker loops actually checked. So the `verified=` part can never fail on its own. The line only shows that the child finished in the expected mode; it does not show that 64,000 entries were checked.
- **Risk**: A later edit could quietly check fewer entries, for example:
  - an off-by-one in `(round + 1) * Workers * Entries`
  - skipping the post-round check for some rounds
  - a worker that stops early

  The child would still print `verified=64000` and the test would still pass, while the validation evidence keeps claiming full coverage.
- **Suggested Fix**: Count entries where the checks actually happen. For example, have `verifyCompletedRound` return how many indices it checked, or add up the worker checks in an `AtomicLong`. Print that counted value, and keep the parent's comparison with `TotalEntries` so any shortfall fails.

### Issue 2: No check is designed to fail if only the lookup methods lose their lock
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyGlobalParams.scala (protects `core/src/main/scala/com/microsoft/azure/synapse/ml/param/GlobalParams.scala` lines 24–26 and 32–34)
- **Line(s)**: 35, 40–41, 45–46, 50–58, 77–79
- **Description**: In Scala's mutable `HashMap`, an unlocked lookup that runs while another thread resizes the table can miss an entry and return `None`. The probe can rarely see such a miss:
  - **Lookups that expect `None`.** Line 35 runs while other workers are still registering, and lines 45–46 run after reset. Both expect `None`, so a missed entry looks the same as a pass.
  - **Lookups that expect a value.** Lines 40–41 and 77–79 run only after the worker has finished its own writes. They overlap another worker's resize only when thread scheduling happens to drift.
  - **Post-round checks.** Lines 50–58 run on one thread after all workers finish, so nothing races them.
  - **Value map growth.** Keys are removed every round and the map never shrinks, so the value map stops resizing after round 0.

  The probe therefore targets corrupted writes, which the baseline failures show it catches. No check is designed to fail when only `getGlobalParam` and `getParam` lose `synchronized`.
- **Risk**: Lookups look harmless, so a later performance change might drop their lock. That change could pass this suite intermittently while bringing back missed global defaults when stages are built and transformed concurrently. Synchronized lookup is half of this PR's stated purpose.
- **Suggested Fix**:
  - Preferred: add lookups that expect a value and run while the maps are growing. Before each round, register sentinel keys and parameters with known values. Then either have every worker check them for `Some` after each of its writes, or run dedicated reader tasks over them until the writers finish.
  - Optional: add a deterministic in-process test. A helper thread holds the `GlobalParams` monitor, and the test asserts that each of the five methods stays blocked until release and then returns the expected value. Use only the suite's reset keys and a random-UID parameter so no state leaks. This test depends on the current locking choice, so the first option is better.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not actionable for the current patch
- **What changed**: No source change. Retained the completion marker as a mode-and-workload contract, not an independent telemetry counter.
- **Why**: The current eager loops check exactly the advertised 64,000 distinct entries. Each worker's range is `(round * 8 + worker) * 2000` through the next 1,999 indices; the four rounds cover indices 0 through 63,999 without gaps or overlap. Every submitted worker is joined, every failed `require` propagates through `Future.get`, and the final post-round check covers all 64,000 indices before the marker is printed. The finding describes hypothetical future changes to those loops, not an incorrect count or false-pass path in the current patch. Adding a second counter that restates the loops is not required to verify the production behavior.
- **How verified**: Inspected both worker modes, loop bounds, post-round checks, joined futures and the parent exit-status assertions. The exact current probe also completed both modes twice in isolated Java 8 JVMs with all production locks intact; all four executions passed.

### Issue 2
- **Status**: Not actionable; the alleged missing regression coverage was disproved
- **What changed**: No source change. Ran a bounded negative control using the exact current compiled concurrency probes, replacing only the production lookup methods with copies that omit `synchronized`. All three writer locks remained intact.
- **Why**: The existing positive-value assertions do detect unlocked-reader regressions while other workers grow the maps. Concurrency stress tests are schedule-dependent, so neither the existing probes nor the proposed sentinel readers guarantee failure on every execution. A deterministic test asserting ownership of this particular monitor would couple the tests to an implementation choice rather than strengthen the public behavior contract.
- **How verified**: On Java 8 / Scala 2.12.17, both fixed modes passed twice each (4/4). With only `getGlobalParam` and `getParam` unlocked, registration failed in 3/4 fresh JVMs with `Lost registration`, and values failed in 3/4 with `Lost global value` or `Lost parameter value`. The two passing mutation executions are retained as evidence of scheduling dependence, not counted as mutation detection. Production and test source blobs were checked against the previously validated snapshot and were not modified; mutant classes were compiled into a separate private evidence directory.