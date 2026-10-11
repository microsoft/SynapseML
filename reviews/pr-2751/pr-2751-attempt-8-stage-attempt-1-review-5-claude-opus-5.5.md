## Review Summary
- **Round**: 5
- **Theme**: Testing & Coverage
- **Mode**: Sequential final CI-qualified pass, lens 5 of 6, Medium tier from the primary-agent estimate with the repository's sequential stage-order override, top-level review attempt 8, stage attempt 1; resolved-plan for supplied review context
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2751/pr-2751-attempt-8-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Scope. I reviewed microsoft/SynapseML#2751 at head `0c453310a47ee8f1ce34befd6fc7f99badd540ca` against integrated master `861c3a1e14a9511b5604563ff1e3976cefa82e90`. The worktree HEAD equals the frozen head, and every hunk I cite matches the worktree source. I used the driver's manifest SHA256 `2727c176620165e4481fc5a98b71d3d1c9923cdd729755ad6080fcb9c7eb299b` and complete-diff SHA256 `495592f029896f0209c41752958137fdbd8f26aa71d8183fcca32cac092d65c5` as supplied and did not recompute them.
- [x] I read the complete frozen diff for all nine paths: `docs/Explore Algorithms/LightGBM/Overview.md`, `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`, `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/booster/LightGBMBooster.scala`, `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/DatasetAggregator.scala`, `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`, `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/BaseTrainParams.scala`, `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala`, `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala` and `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingOmpRegressionSuite.scala`.
- [x] I read both test files in full. `StreamingLayoutSuite.scala` adds `NativeOmpResetDelegate` at lines 15-26 and new tests at lines 90-247. `StreamingOmpRegressionSuite.scala` holds the parent harness at lines 18-78 and the child probe at lines 80-151.
- [x] Coverage map from new production code to tests:
  - Tests at `StreamingLayoutSuite.scala` lines 137-180 cover `NativeOmpThreadRegistry` and `positiveNumThreads`. That includes first-duplicate keys, a canonical key plus an alias, conflicting aliases, and invalid or nonpositive values.
  - The test at lines 182-247 covers all six registration sites end to end:
    - A streaming fit covers sampled-column, serialized-reference and booster-create.
    - A bulk dense fit with an `n_jobs` reset delegate covers `LGBM_DatasetCreateFromMat`, booster-create and reset-parameter.
    - A bulk sparse fit covers `LGBM_DatasetCreateFromCSR`.
  - Lines 123-135 cover `firstOmpTeamSize`, `parseCpuAffinityList` and the three-argument `osReportedProcessorCount`.
  - Lines 90-121 cover the nine-argument `streamingOmpAllocationBound`.
  - No test directly covers `linuxProcessAffinityCount`, the no-argument `osReportedProcessorCount`, `firstCommandOutput`, or the three-argument `ReferenceDatasetUtils.streamingOmpAllocationBound` wrapper and its call site. See Issues 1 and 2.
- [x] Hand mutation analysis of the bound table, comparing `StreamingLayoutSuite.scala` lines 111-120 with `LightGBMUtils.scala` lines 209-238.
  - The table kills mutants that drop `configuredMaxThreads`, `configuredNumThreads`, `defaultTeam`, `registeredMaxThreads` or `osProcessorCount`.
  - It also kills a mutant that prefers affinity over `OMP_NUM_THREADS`, and one that drops the fallback floor, because line 119 then hits an empty `max`.
  - Two mutants survive. One removes `availableProcessors` from the fallback at line 228. The other removes the outer 16-slot floor at line 232. See Issue 3.
- [x] Hand mutation analysis of the production wiring at `ReferenceDatasetUtils.scala` lines 122-125 and 147-160. No assertion in this PR's tests fails if the history, affinity, OS-count or JVM-count argument becomes a neutral value, or if the call site swaps `numThreads` and `executorPartitionCount`. See Issue 2.
- [x] Registration order and sites.
  - `ReferenceDatasetUtils.scala` line 57 registers the driver-side sampled-column dataset.
  - Line 220 registers the executor-side serialized reference before line 122 computes the bound.
  - `LightGBMBooster.scala` lines 242 and 322 and `DatasetAggregator.scala` lines 413 and 529 cover the other four sites.
  - Each registration precedes its native call, so a failed native call leaves a conservative over-registration. No test covers that failure path, and I consider it safe.
- [x] Single-writer threading. In `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/StreamingPartitionTask.scala`, the training task initializes the dataset at line 118, and helper tasks wait at line 122 before pushing. This supports the claim that one external writer initializes and pushes on the same thread.
- [x] Isolation. `build.sbt` line 282 sets `Test / parallelExecution := false`, and I found no fork or test-grouping setting in `build.sbt`. The registry singleton at `LightGBMUtils.scala` line 84 has no reset. In `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`, every `TestBase` suite shares the companion object's session through `sparkProvider` at line 154, and `afterAll` at lines 194-199 never stops it. See Issue 4.
- [x] Child-process lifecycle in `StreamingOmpRegressionSuite.scala`:
  - The wait is bounded at 5 minutes, with `destroyForcibly` and a final `waitFor` at lines 57-66.
  - Line 74 deletes the temp directory in `finally`.
  - Lines 50-56 clear the environment.
  - Lines 45-48 confine tmp and error files to that directory.
  - Lines 38-42 pick the core-dump flag by Java version. See Issue 5.
- [x] Flakiness. Fixtures use fixed data with `deterministic=true` and no random seeds. `freePort()` leaves a small close-then-bind window, which is acceptable for one child JVM. The only timing dependency is the 5-minute guard, against supplied runs of about 23 to 24 seconds per case. Nothing uses the network beyond loopback.
- [x] Supplied evidence, read but not re-run:
  - Azure build https://dev.azure.com/msdata/A365/_build/results?buildId=239492673 succeeded with 65 of 65 jobs.
  - Published results show both regression cases passing, layout 8/8 and lifecycle 1/1.
  - The JDK 11 targeted run passed 11/11, and the Temurin 8u504 clean rebuild passed.
  - With the target's production source substituted, dense crashed in `SparseBin<unsigned char>::Push` and sparse PASSED. Issue 1 relies on this.
  - I do not treat green CI as proof of correctness.
- [x] Public API. The `maxStreamingOMPThreads` default stays 16, and `LightGBMParams.scala` and `BaseTrainParams.scala` change only doc text. No new param or serialized shape needs a persistence test.
- [ ] Coverage or branch-coverage report. None was supplied, and this lens forbids builds and test runs.
- [ ] Mutation tooling. None was supplied or run. Every mutation statement above comes from reading the source.
- [ ] Local execution. I ran no suite. Two statements are inferences, not observations. Issue 1 infers that the child's WARN log level hides the allocation log line. Issue 5 infers JDK 8 core-dump behavior on Linux.
- [ ] Registry concurrency stress. No test runs concurrent `register` calls. `AtomicInteger.accumulateAndGet` with `ConcurrentHashMap.computeIfAbsent` makes this an acceptable gap, not a finding.
- [ ] Driver and executor separation. All tests run in Spark local mode, where the driver and executors share one registry. No test can tell the driver-side sampled-column registration at `ReferenceDatasetUtils.scala` line 57 apart from executor history. This is a limitation, not a finding.
- [ ] Non-Linux hosts. The regression is Linux-only by design, so no test exercises the OS-count fallback that macOS and Windows depend on. The docs already call that fallback best effort.

## Issues

### Issue 1: The child-JVM regression never asserts the allocation it guards, and its sparse case cannot fail before the fix
- **Severity**: Medium
- **File**: lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingOmpRegressionSuite.scala
- **Line(s)**: 31-77, with the oracle at 70-72; probe lines 104, 111-112, 123 and 137-143; related log statement at lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala lines 126-132
- **Description**:
  - **What the test checks.** The parent passes when the child exits with 0 and prints `STREAMING_OMP_OK`, at lines 70-72. The child prints that marker only after its row-count, finite-value, parity and nonconstant checks, at lines 111-112 and 141-143. No check observes the value this PR changes, which is the `maxOmpThreads` that `LGBM_DatasetInitStreaming` receives.
  - **Why that is indirect.** Underallocation causes an out-of-range or cross-writer native write. The test detects it only if that undefined behavior crashes the child or moves a prediction by at least 1e-12. Supplied baseline evidence shows this is unreliable. With the target's production source substituted on the same fixture, dense crashed in `SparseBin<unsigned char>::Push` but sparse PASSED.
  - **The sparse case.** The case named "streaming sparse ingestion covers a wider ambient OpenMP team" cannot fail on the defect it names. It is a parity smoke test that costs about 23 seconds of CI.
  - **The dense case.** Dense did discriminate in that run. The outcome still depends on heap layout, the allocator and libgomp scheduling, not on an assertion.
  - **Unchecked premise.** The probe never confirms its own setup of 4 external writers and a bound of at least 32. If a partitioning change left a single writer, the test would stop exercising the multi-writer path and still pass.
  - **The existing log line.** `ReferenceDatasetUtils.scala` lines 126-132 predate this PR and log `externalThreads=` and `allocationBound=` when verbosity is above 1. The probe sets verbosity 2 at line 104. But line 123 calls `resetSparkSession(numCores = Some(4))`. In `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala`, that method defaults to `logLevel = "WARN"` at line 85, and `getSession` applies the level at line 73. That level appears to suppress the INFO line. I inferred this from source and did not run the child.
- **Risk**: A later change can narrow or misroute the streaming allocation and still pass whenever the bad native write corrupts memory quietly instead of crashing. Half of this suite's CI time adds no fail-before power. Two green cases read like two proofs when only one discriminated.
- **Suggested Fix**: Make the allocation the oracle.
  1. In the child, enable INFO for the `ReferenceDatasetUtils` logger, or pass `logLevel = "INFO"` to `resetSparkSession`. The supplied measurement harness already verified writers and width through a scoped allocation logger, so the approach works out of tree.
  2. Have the parent assert that `probe.log` contains `externalThreads=4` and `allocationBound=32` from the streaming fit. The removed formula, `math.max(configuredMaxThreads, configuredNumThreads)`, gives 16 for this fixture. Both cases would then fail deterministically on the baseline and on any regression that narrows this fixture's bound below 32, whether or not the native write crashes.
  3. Optionally, have the child print `ReferenceDatasetUtils.streamingOmpAllocationBound(16, 2, 4)` in the OK marker for the parent to compare with 32.
  4. Keep the crash and parity checks as well.
  5. If the sparse case keeps no allocation assertion, rename it as a parity smoke test.

### Issue 2: No test exercises the production inputs to the bound, so history, affinity and argument-order regressions would still pass CI
- **Severity**: Low
- **File**: lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala
- **Line(s)**: 122-125 and 147-160; also lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala lines 157-162 and 182-207
- **Description**: The tests feed the nine-argument bound injected values but never check how production builds those values.
  - **History.** The three-argument wrapper at lines 147-160 reads `OMP_NUM_THREADS`, Linux affinity, the OS and JVM counts, and the global high-water mark. No test calls it directly or checks what it returns.
    - No assertion fails if the `LightGBMUtils.nativeOmpThreadHighWaterMark` argument at line 158 becomes `0`.
    - The child runs streaming before any wider history exists, at lines 137-138.
    - The layout end-to-end test asserts only registry values, and its `numThreads` of at least 32 dominates the bound anyway.
    - `docs/Explore Algorithms/LightGBM/Overview.md` lines 174-179 list registered history as an input. Only the supplied measurement harness, which is not part of this PR, checked it, observing width 64 through its logger. No committed test does.
  - **Affinity.** `linuxProcessAffinityCount` at `LightGBMUtils.scala` lines 157-162 accepts an injectable `statusPath`, yet no test passes a fixture. It is the default Linux source whenever `OMP_NUM_THREADS` is unset. The child sets `OMP_NUM_THREADS=32`, so replacing the call at line 155 with `None` also survives. None of these has a test:
    - the `Cpus_allowed_list:` lookup
    - the substring after the colon
    - the missing-line case
    - the I/O-failure case
  - **OS count.** `osReportedProcessorCount()` and `firstCommandOutput` at `LightGBMUtils.scala` lines 182-207 have no test either. That leaves the macOS `sysctl` subprocess, its non-zero-exit path and the untimed `waitFor()` at line 188 uncovered.
  - **Argument order.** Lines 122-125 pass three `Int` arguments by position, as `streamingOmpAllocationBound(configuredMaxOmpThreads, numThreads, executorPartitionCount)`. Swapping the last two fails no current assertion. The child uses `numThreads=2` with 4 writers, and both orders give 32 there. The layout fit uses `numThreads` of at least 32 with 2 writers, but asserts only registry values. In production, the swap would turn `numThreads=1` with several local writers into `externalThreads=1`, and then:
    - the function returns `-1`
    - LightGBM sizes slots from the initializing thread's one-thread team
    - helper threads push with their wider ambient teams
  - **Single writer.** The `-1` path is safe only because `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/StreamingPartitionTask.scala` line 118 initializes on the same thread that pushes. No case runs that path under a wide ambient team.
- **Risk**: A refactor that drops or reorders a wrapper input, or a regression in `/proc/self/status` parsing, would weaken the mitigation without failing CI. The exposed cases are:
  - Linux executors without `OMP_NUM_THREADS`
  - pooled threads that keep wider historical teams
  - `numThreads=1` with several local writers

  This is a gap in regression protection. By inspection, today's wiring is correct.
- **Suggested Fix**:
  1. Add a unit test that writes temporary status files and calls `linuxProcessAffinityCount(path)`:
     - `Cpus_allowed_list:\t0-3,8` gives `Some(5)`.
     - A file without that line gives `None`.
     - A missing path gives `None`.
  2. In the isolated child probe, where `OMP_NUM_THREADS=32` is fixed, assert after the fits:
     - `ReferenceDatasetUtils.streamingOmpAllocationBound(16, 2, 4) == 32`
     - `ReferenceDatasetUtils.streamingOmpAllocationBound(16, 32, 1) == -1`
     - `ReferenceDatasetUtils.streamingOmpAllocationBound(16, 1, 4) != -1`
  3. Then call `LightGBMUtils.registerNativeOmpThreads(NativeOmpCallSite.BoosterCreate, "num_threads=40")` and assert `ReferenceDatasetUtils.streamingOmpAllocationBound(16, 2, 4) == 40`. This tests history without raising the shared test JVM's registry, which Issue 4 discusses.
  4. To pin the call-site argument order, add a child case with `setNumThreads(1)` and 4 writers, and assert that the Issue 1 log line reports `allocationBound=32`. Swapping lines 124 and 125 turns that into `-1`. The current fixture cannot tell the orders apart, because both give 32.
  5. Optionally, add a child case with `numTasks=1` and automatic `numThreads` to run the `-1` path under the 32-thread ambient team.

### Issue 3: The bound table leaves the documented 16-slot floor and the JVM processor count unguarded
- **Severity**: Low
- **File**: lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala
- **Line(s)**: 111-120; production terms at lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala lines 228 and 232
- **Description**: `docs/Explore Algorithms/LightGBM/Overview.md` line 174 promises that "Streaming allocation uses at least 16 OpenMP slots". Lines 182-185 say that several pushing threads with neither an environment nor a Linux-affinity source use the maximum of the OS-reported and JVM-reported processor counts with the 16-thread floor. Two mutants of `LightGBMUtils.streamingOmpAllocationBound` pass all nine rows.
  - **Outer floor.** The first mutant removes the outer `MinStreamingOmpThreads` at line 232. That floor decides the result only when every other term is under 16.
    - Rows 112 and 113 pass a `configuredMaxThreads` of 16 or 32, which masks it.
    - Rows 114 to 116 have a team or history of 24 or more.
    - Rows 117 to 119 take the fallback, which has its own floor.
  - **JVM count.** The second mutant removes `availableProcessors` from the fallback at line 228. Row 117 passes an OS count of 32 with a JVM count of 8, and rows 118 and 119 pass JVM counts of 8 and -1. No fallback row has a JVM count above both the OS count and 16.
  - **Warnings.** Line 120 checks how many warnings fire, not their text.
- **Risk**: A later simplification could drop the floor without failing a test. Code would then allocate 8 slots for `OMP_NUM_THREADS=8` or an affinity of 8, with automatic `numThreads`, a nonpositive hint and no wider history. That breaks the documented minimum and removes the margin for teams the hints miss. Dropping the JVM count would likewise go unnoticed on hosts where only the JVM reports the larger count.
- **Suggested Fix**: Add rows that make each term decide the result, then update the warning count:
  - `assert(bound(4, 0, 0, Option("8"), None, Option(64), 64, 0) == LightGBMUtils.MinStreamingOmpThreads)` checks an environment team below the floor. It returns 8 without line 232.
  - `assert(bound(4, 0, 0, None, Option(8), Option(64), 64, 0) == LightGBMUtils.MinStreamingOmpThreads)` checks affinity below the floor.
  - `assert(bound(4, 0, 0, None, None, Option(8), 48, 0) == 48)` makes the JVM count decide. It returns 16 without `availableProcessors`.
  - Change line 120 to `warnings.size == 4`. Optionally, check that the warning text names the OS and JVM fallback.

### Issue 4: The end-to-end registration test permanently widens streaming allocation for later suites in the same JVM
- **Severity**: Low
- **File**: lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala
- **Line(s)**: 182-247, mainly 185-186, 227, 235 and 243; related state at lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala line 84, build.sbt line 282 and core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala lines 154 and 194-199
- **Description**:
  - **How the mark rises.** Line 227 sets `streamingThreads = math.max(32, nextThreadCount(...))`. Lines 235 and 243 then raise the dense and sparse counts above the booster-create mark, so the global high-water mark ends at 34 or more.
  - **Why it persists.** That mark lives in the private `NativeOmpThreads` singleton at `LightGBMUtils.scala` line 84, which has no reset. `build.sbt` line 282 runs suites one after another, with no fork or grouping setting in `build.sbt`, so the mark carries into every later suite in that JVM.
  - **Effect on later suites.** After this test, every multi-writer streaming dataset in the JVM gets at least 34 slots per writer, whatever its own settings. Later streaming suites therefore depend on order:
    - Run alone, a suite sees its configured or environment width.
    - Run after `StreamingLayoutSuite`, it sees a history-driven width that can hide narrow-width defects.
    - The supplied measurements also show that width changes memory use and slot traversal.
  - **The 32 floor is not needed.** The equality assertions at lines 229, 241, 245 and 246 need only a value above the current per-site marks, which `nextThreadCount` already guarantees.
  - **Possible pooled-thread effect.** This effect is likely but I have not verified it. Native calls with `num_threads` of 32 or more set the OpenMP team on pooled Spark task threads. Every `TestBase` suite shares one session through `sparkProvider` at line 154 of `TestBase.scala`, and `afterAll` at lines 194-199 never stops it. Later suites may then run native code with 32 or more OpenMP threads on small CI agents.
- **Risk**: Results depend on suite order. Later streaming suites can no longer catch width regressions on the configured or environment path. The pooled-thread effect may add oversubscription and timing variance.
- **Suggested Fix**:
  1. Remove the `math.max(32, ...)` floor at line 227 and use the smallest counts that keep the equalities meaningful.
  2. If the 32-thread team is deliberate, move this end-to-end check into the isolated child-JVM harness instead.
  3. Add a comment that the registry is process-global and monotonic. Tests in the shared JVM should keep registered counts at or below `MinStreamingOmpThreads`, where the history term has no effect.

### Issue 5: On Java 8 the chosen flag likely does not stop Linux core dumps from a crashing child, an unverified concern
- **Severity**: Low
- **File**: lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingOmpRegressionSuite.scala
- **Line(s)**: 38-48, with cleanup at 74
- **Description**:
  - **The flags.** The harness means to turn off core dumps. It passes `-XX:-CreateMinidumpOnCrash` on Java 8 and `-XX:-CreateCoredumpOnCrash` on later releases.
  - **Why Java 8 is different.** My understanding of HotSpot is that JDK 8 reads `CreateMinidumpOnCrash` only in its Windows port. On Linux, JDK 8 calls `abort()` after a fatal error, and the kernel writes a core whenever `RLIMIT_CORE` and `core_pattern` allow it. JDK 9 renamed the option to `CreateCoredumpOnCrash` and made it apply on every platform. JDK 8 accepts the legacy flag. The supplied evidence says the earlier head failed CI on Java 8 only because it passed the modern option, and the Temurin 8u504 run now passes. On Linux, though, the legacy flag has no effect.
  - **When it matters.** CI runs this test on Java 8, and the test exists to catch a native crash. Suppose a regression or host change makes the child crash on a host with cores enabled. The host then writes a core of a JVM with a 2 GiB heap ceiling.
    - With a relative `core_pattern`, the core lands in the temp directory that line 74 deletes.
    - With a piped handler such as systemd-coredump, the core persists outside the test's cleanup.
  - **Content and limits.** Clearing the environment keeps CI variables out of the image, so its contents are mostly test data. The disk use and persistence still contradict the stated safeguard. I have not checked the runner's limits or run the child.
- **Risk**: On developer machines or agents with cores enabled, a crashing run can leave a multi-gigabyte core outside cleanup. That contradicts the stated "disables core dumps" behavior on the JDK that CI uses.
- **Suggested Fix**: The test already runs only on Linux, so launch the child through the shell with a zero core limit. Put `"/bin/sh", "-c", "ulimit -c 0 && exec \"$0\" \"$@\""` before `java` in the `ProcessBuilder` arguments at lines 43-47. The shell then runs `java` as `$0` with the other arguments as `"$@"`, on every JDK. Both paths are absolute, so the cleared environment needs no `PATH`. Keep `-XX:-CreateCoredumpOnCrash` for Java 9 and later. Otherwise, document that Java 8 relies on the host's `RLIMIT_CORE`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver dispositions, corrective cycle 4

The reviewer text remains unchanged. These dispositions supersede its open
placeholders. The corrective commit containing this section includes the test
changes. Actual command outcomes and the product fingerprint are in
`pr-2751-attempt-9-precommit-low.md`.

| Finding | What changed and why | Verification |
|---|---|---|
| 1 | Enable only the `ReferenceDatasetUtils` INFO logger in each isolated child. Both dense and sparse cases now require the actual four-writer, 32-slot allocation record, plus successful exit, evaluated nonconstant predictions and bulk parity. | Returning the old width 16 cannot satisfy either allocation assertion, even if undefined native behavior happens not to crash. Historical evidence remains accurately labeled: the original dense replay crashed, while the original sparse replay passed. The separate new stacked case has an observed public native fail-before. |
| 2 | Add valid, missing-line, malformed and missing-file affinity cases; bounded subprocess success/failure/timeout cases; direct production-wrapper/history checks inside the isolated child; and named production arguments. Multi-writer fixtures now request one native thread. | Assertions check width 32 before history registration and 40 after it. The actual public allocation record protects the four-writer/one-native-thread mapping. The stacked input stays lazy and forces a real same-thread prediction reset. Automatic/static `-1` behavior has helper and production-wrapper assertions; no separate automatic-team public fit is claimed. |
| 3 | Add cases where the outer floor alone decides the bound and where the JVM count dominates the OS count and floor. Update the expected fallback-warning count. | The new environment and affinity rows require 16 with all other candidate widths below 16; the fallback row requires 48 from the JVM count. Removing either production term fails its corresponding assertion. |
| 4 | Remove the forced 32-thread minimum from the shared-JVM registration test. Use the smallest increasing value needed to test each site. Wider allocation/history assertions run in the fresh child process instead. | Existing end-to-end assertions still require all six registrations. No unsafe global registry reset is introduced: pooled native threads may retain their width. Pre-existing history from another test can still raise the mark, which is intentional production behavior, not a promise of global test-state isolation. |
| 5 | Confirmed the concern, then use and verify zero soft/hard Linux core limits for every child, retaining the modern flag only where supported. | Official JDK-8074354 and the actual Java 8 native fail-before confirm the distinction between an accepted legacy flag and effective file-core suppression. Environment clearing, synthetic inputs and cleanup remain in place. |
