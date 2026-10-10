## Review Summary
- **Round**: 2
- **Theme**: Architecture & Patterns
- **Mode**: resolved-plan (Medium tier; sequential independent lens 2 of 6; final CI-qualified pass 1; top-level review attempt 8, stage attempt 1)
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2751/pr-2751-attempt-8-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist

Path keys: `<lgbm>` = `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm`,
`<lgbm-test>` = `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm`.

- [x] Reviewed microsoft/SynapseML#2751 at head `0c453310a47ee8f1ce34befd6fc7f99badd540ca` against integrated master `861c3a1e14a9511b5604563ff1e3976cefa82e90`. `git merge-base` returns `861c3a1e…`. `git diff --stat 861c3a1e… 0c453310… -- docs lightgbm core` lists exactly nine paths (601 insertions, 28 deletions), and `git status --porcelain -- lightgbm core docs` is empty. The prompt's embedded diff is byte-identical to the worktree `git diff` for those paths. The driver supplied manifest SHA256 `2727c176620165e4481fc5a98b71d3d1c9923cdd729755ad6080fcb9c7eb299b` and binary-diff SHA256 `495592f029896f0209c41752958137fdbd8f26aa71d8183fcca32cac092d65c5`; this reviewer did not recompute either.
- [x] Repository conventions checked against `AGENTS.md`: follow nearby code before introducing a new pattern; keep public JVM signatures and serialized parameter shapes; use DataFrame APIs only (no RDD); tests extend `TestBase`. The change adds no RDD code and no hand-written Python.
- [x] Public surface: the `maxStreamingOMPThreads` name, type and default `16` are unchanged (`<lgbm>/params/LightGBMParams.scala:169-181`). The `ExecutionParams` fields are unchanged; only the scaladoc changed (`<lgbm>/params/BaseTrainParams.scala:181-194`). New production members are `private[lightgbm]` or `private` (`<lgbm>/LightGBMUtils.scala:22-77,82-85,112-238`; `<lgbm>/dataset/ReferenceDatasetUtils.scala:14,147-180`). Nonpositive values no longer disable the safety bound; this is an intentional, documented semantic change.
- [x] Native registration coverage: I searched `lightgbm/src/main/scala` for `lightgbmlib.LGBM_` dataset, booster, predict and network calls. All six registrations sit immediately before their native calls:
  - `<lgbm>/dataset/ReferenceDatasetUtils.scala:57-58` and `:220-221`
  - `<lgbm>/booster/LightGBMBooster.scala:242-243` and `:322-323`
  - `<lgbm>/dataset/DatasetAggregator.scala:413-414` and `:529-530`

  Prediction (`LightGBMBooster.scala:529,551`) passes a fixed parameter string with no thread key. `LGBM_BoosterLoadModelFromString`, `LGBM_BoosterMerge`, `LGBM_NetworkInit` and `LGBM_DatasetCreateByReference` take no parameter string.
- [x] Thread-history inputs: dataset parameters carry `num_threads` from `ExecutionParams.numThreads` (`<lgbm>/LightGBMBase.scala:527-539,875-877`). The new `positiveNumThreads` (`<lgbm>/LightGBMUtils.scala:112-118`) reuses the existing first-duplicate-wins `parseLightGBMParams` (`:92-103`) and takes the maximum across distinct aliases. This matches the supplied lightgbmlib 3.3.510 parser evidence; tests are at `<lgbm-test>/split1/StreamingLayoutSuite.scala:137-180`.
- [x] Concurrency: the registry uses `AtomicInteger` with `ConcurrentHashMap.computeIfAbsent(...).accumulateAndGet(...)` (`<lgbm>/LightGBMUtils.scala:60-77`). It is monotonic and lock-free, and I found no publication race. Constructing the registry does not call back into `LightGBMUtils`, so the back-reference in Issue 1 is not an initialization cycle.
- [x] Dependency direction: `booster/` and `dataset/` already depended on the root `LightGBMUtils` at the merge base, so the new references add no package-level edge and no library dependency. The only new cycle is the registry↔owner reference inside one file (Issue 1).
- [x] Allocation entry point and lifecycle: `getInitializedReferenceDataset` (`<lgbm>/dataset/ReferenceDatasetUtils.scala:105-145`) is called only by the training task (`<lgbm>/StreamingPartitionTask.scala:117-118`). Registration in `deserializeReferenceDataset` runs before the bound is computed, and `initializeOwnedDataset` still closes the dataset on failure.
- [x] Compared against nearby patterns:
  - The new child-JVM test (`<lgbm-test>/split1/StreamingOmpRegressionSuite.scala:19,37,60-64`, with bounded `waitFor` plus `destroyForcibly`) mirrors `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/CodegenDiscoverySuite.scala:172`.
  - It uses the `TestBase.LinuxOnly` tag (`core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala:108-110,148`) together with an `assume` (`StreamingOmpRegressionSuite.scala:32-33`).
  - OS-name detection already appears in `core/src/main/scala/com/microsoft/azure/synapse/ml/core/env/NativeLoader.java:77-111` and `core/src/main/scala/com/microsoft/azure/synapse/ml/core/utils/OsUtils.scala:7`.
  - A search of `core/src/main` and `lightgbm/src/main` found no other `ProcessBuilder`, `scala.sys.process` or `Runtime.exec` use.
- [x] Compared the documentation in `docs/Explore Algorithms/LightGBM/Overview.md:169-197`, `<lgbm>/params/LightGBMParams.scala:171-178` and `<lgbm>/params/BaseTrainParams.scala:181-184` against the policy in `<lgbm>/LightGBMUtils.scala:209-238` (Issue 3).
- [x] Style limits in `scalastyle-config.xml`: file length 800 (line 4), maxParameters 12 (line 51), method length 60 (line 53), cyclomatic complexity 10 (line 59). The nine-parameter methods and the 352-line `LightGBMUtils.scala` are within these limits.
- [x] Test isolation: `build.sbt:282` disables parallel test execution. When suites share a test JVM, `<lgbm-test>/split1/StreamingLayoutSuite.scala:182-247` permanently raises the JVM-global registry for later suites: at least 32 at the streaming sites, then higher. This only adds safety margin, and the dedicated regression suite runs in a fresh child JVM, so I record it as an observation, not an issue.
- [x] Observations not raised as issues (nits or documented tradeoffs):
  - (a) `private val Logger` (`ReferenceDatasetUtils.scala:14`) reuses the name of the imported slf4j type. The inline `LoggerFactory.getLogger(getClass)` at `:127` predates this PR and was left unchanged.
  - (b) `MinStreamingOmpThreads` (`LightGBMUtils.scala:85`) lives outside `LightGBMConstants`, and the param default `16` (`LightGBMParams.scala:179`) is a separate literal.
  - (c) The per-site registry accessors and `NativeOmpCallSite.name`/`Values` exist only for tests.
  - (d) The pre-existing `max` in `maxStreamingOMPThreads` does not describe a cap, and renaming it would break the public API.
  - (e) The sticky JVM-global history and the memory cost of allocation width are documented tradeoffs (`Overview.md:177-179,193-197`).
  - (f) Registering before each native call is a convention across six sites; coverage is complete today.
  - (g) The duplicated `runtimeClasspath` helper follows the existing CodegenDiscoverySuite pattern.
- [ ] This reviewer did not run builds, tests, scalastyle or codegen (the effective policy prohibits it). Driver-supplied results were not re-verified: JDK 11 compile/scalastyle, the Java 8 CI rebuild, and Azure build https://dev.azure.com/msdata/A365/_build/results?buildId=239492673. Green CI is not treated as proof of correctness.
- [ ] Native LightGBM sources were not inspected. lightgbmlib 3.3.510 OpenMP team semantics, `SparseBin` indexing and parser precedence come from driver-supplied evidence.
- [ ] macOS and Windows behavior, including the Issue 2 probe paths, was not executed; those findings come from source inspection.
- [ ] Performance: the supplied samples are small and noisy, and their direction reverses between runs, so this reviewer neither attributes nor rules out a slowdown. The exact private Issue2333 incident is not proven; the supplied reproduction is synthetic.

## Issues

### Issue 1: Streaming OpenMP allocation has no single owner: a duplicated nine-argument forwarder, same-named overloads with reordered `Int` parameters, and a registry↔utilities back-reference
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`; `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: `ReferenceDatasetUtils.scala:147-180`; `LightGBMUtils.scala:22-77, 84, 112-125, 209-238`
- **Description**: One concern is split across two objects.
  - **Split ownership:** The PR adds the call-site enum, the JVM-global thread registry, alias parsing, Linux-affinity and OS probing, subprocess execution and the allocation policy to the general-purpose `LightGBMUtils` object, which grows by 195 lines (157 → 352). The production probe wiring lives elsewhere, in `ReferenceDatasetUtils.streamingOmpAllocationBound(configuredMaxThreads, configuredNumThreads, externalThreads)` (`:147-160`).
  - **Duplicate forwarder:** `ReferenceDatasetUtils` also declares a second nine-parameter `streamingOmpAllocationBound` (`:162-180`) that passes all nine arguments unchanged to `LightGBMUtils.streamingOmpAllocationBound` (`LightGBMUtils.scala:209-238`). Apart from the three-argument wrapper, its only caller is the unit test (`lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala:100`).
  - **Inconsistent parameter order:** The three same-named methods order identical `Int` parameters differently. The three-argument entry point puts `externalThreads` last because it was appended to the pre-existing two-argument signature; both nine-argument methods put it first. Overloads resolve by argument count and the leading parameters are all `Int`, so a call with transposed arguments compiles.
  - **Back-reference:** `NativeOmpThreadRegistry.register` calls back into `LightGBMUtils.positiveNumThreads` (`LightGBMUtils.scala:67-68`), while `LightGBMUtils` owns the registry instance (`:84`). The registry type and its owner therefore depend on each other, and a pure data structure is mixed with parameter-string parsing.
- **Risk**: This is a maintainability hazard, not a defect at this head; the production call at `ReferenceDatasetUtils.scala:122-125` passes the arguments in the right order.
  - A future edit could follow the nine-argument order through the three-argument entry point, for example `streamingOmpAllocationBound(ctx.executorPartitionCount, hint, numThreads)`. That call compiles and treats the writer count as a hint.
  - Whenever the value that lands in `externalThreads` is `1`, the policy returns `-1` (`LightGBMUtils.scala:218-220`). A multi-writer dataset then gets native auto-sizing, which is the underallocation this PR mitigates.
  - The unit test calls the forwarder, so it does not cover the production wrapper's argument mapping (`ReferenceDatasetUtils.scala:150-159`). That mapping is covered only indirectly. By source reasoning (not executed), the supplied regression fixture still yields a bound of 32 for some transpositions, such as swapping `externalThreads` with `configuredNumThreads`, because its ambient OpenMP team is 32.
  - Each further allocation feature will keep growing a general-purpose utilities object.
- **Suggested Fix**:
  - Move the call-site enum, registry, team and OS probes, and allocation policy into one dedicated `private[lightgbm]` object and file (for example `StreamingOmpAllocation`).
  - Have the registry accept an already-parsed thread count, or own the alias parsing, so it no longer calls back into `LightGBMUtils`.
  - Delete the nine-argument forwarder in `ReferenceDatasetUtils` and point the unit test at the canonical function.
  - Give the production entry point a distinct name and the same leading parameter order, or take a small input case class or named arguments (`externalThreads = ctx.executorPartitionCount`), so transposed arguments cannot compile silently.

### Issue 2: Host probes run eagerly on every streaming initialization, and the macOS probe adds an unbounded production subprocess
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`; `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: `ReferenceDatasetUtils.scala:150-159`; `LightGBMUtils.scala:157-162, 182-207, 218-229`
- **Description**:
  - **Eager probes:** The three-argument entry point evaluates every probe as a strict argument before the policy decides whether it needs it: `System.getenv("OMP_NUM_THREADS")`, `linuxProcessAffinityCount()` (reads `/proc/self/status`), `osReportedProcessorCount()`, `availableProcessors()` and the registry. The policy discards all of them when `externalThreads == 1` (`LightGBMUtils.scala:218-220`), and it consults the OS count only when neither `OMP_NUM_THREADS` nor Linux affinity is available (`:222-229`).
  - **Unbounded subprocess:** On macOS, `osReportedProcessorCount()` (`:194-207`) launches `sysctl -n hw.logicalcpu` through `firstCommandOutput` (`:182-192`). That helper calls `process.waitFor()` with no timeout and no `destroyForcibly()`. The result is not cached, although the logical CPU count is fixed for the life of the process.
  - **New pattern:** A search of `core/src/main` and `lightgbm/src/main` found no other `ProcessBuilder`, `scala.sys.process` or `Runtime.exec` use, so this introduces a new executor-side pattern. That goes against `AGENTS.md`'s "Follow nearby code before introducing a new pattern". It also differs from the PR's own child-process handling, which bounds the wait and destroys the process (`lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingOmpRegressionSuite.scala:60-64`).
- **Risk**:
  - On macOS local or development Spark, every streaming dataset initialization (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/StreamingPartitionTask.scala:117-118`) forks a process. This includes single-writer fits and runs with `OMP_NUM_THREADS` set, where the result is thrown away.
  - If the child process ever stalls, the training task blocks with no time limit.
  - Linux and Windows paths spawn no process. Linux re-reads `/proc/self/status` on each initialization, which is cheap.
  - This was not observed at runtime, and this reviewer did not run macOS; the finding comes from source inspection.
- **Suggested Fix**:
  - Pass probes lazily, as by-name parameters or thunks evaluated only on the branch that needs them.
  - Cache the OS-reported processor count in a `lazy val`.
  - If the subprocess stays, bound it with `waitFor(timeout, unit)` plus `destroyForcibly()`. Alternatively, drop it and rely on `availableProcessors()` with the 16-slot floor on non-Linux hosts; the documentation already describes those counts as best-effort.

### Issue 3: The documentation describes the scope of the 16-slot floor differently from the policy
- **Severity**: Low
- **File**: `docs/Explore Algorithms/LightGBM/Overview.md`; `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala`
- **Line(s)**: `Overview.md:174-182`; `LightGBMParams.scala:172-177` (policy: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala:218-236`)
- **Description**: The policy has two regimes:
  - With one external writer it returns `-1`, and LightGBM sizes the allocation from the init thread's current team. No SynapseML floor, hint or registered history applies (`LightGBMUtils.scala:218-220`).
  - With several writers it always takes `max(16, hint, numThreads, team, registered)` (`:231-236`).

  The documentation does not match either regime cleanly:
  - **Overview overstates the floor:** `Overview.md:174` says without qualification that "Streaming allocation uses at least 16 OpenMP slots and is raised to cover a positive `maxStreamingOMPThreads` hint…". The later single-writer sentence (`:181-182`) says only that LightGBM measures the thread's team. That reads as one more input to the maximum, not as a bypass of the floor and hint.
  - **Example:** With the bundled lightgbmlib 3.3.510, the single-writer init thread has just run `LGBM_DatasetCreateFromSerializedReference` with `num_threads` taken from `ExecutionParams.numThreads` (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMBase.scala:536,875-877`; `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala:220-221`). So with `numThreads=2` and one writer, native sizing gives a width of 2, not at least 16.
  - **Parameter doc understates the floor:** It ties "the 16-thread floor" only to the fallback with no team source, and says SynapseML "raises this value as needed" (`LightGBMParams.scala:172-177`). That implies a hint below 16 yields the smaller value when `OMP_NUM_THREADS` or affinity is available, but the policy still applies 16.
- **Risk**:
  - The Overview, the parameter doc and the scaladoc (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/BaseTrainParams.scala:181-184`) give users and maintainers contradictory contracts.
  - A user who raises `maxStreamingOMPThreads` as a manual safety margin in a one-writer-per-executor layout silently gets native auto-sizing instead.
  - Memory estimates based on allocation width (`Overview.md:193-197`) can be misapplied.
  - This is documentation only; allocation safety at this head is unaffected.
- **Suggested Fix**:
  - Scope the Overview sentence to several pushing threads, and state that with exactly one pushing thread SynapseML passes `-1`, so the hint, the 16-slot floor and registered history do not apply.
  - In the parameter doc, state that the 16-slot floor applies to every multi-writer bound, not only to the fallback with no team source, so a hint below 16 cannot lower it.
  - Keep the `BaseTrainParams.scala` scaladoc consistent with both.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver disposition
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver disposition
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver disposition
- **How verified**: pending

## Driver dispositions, corrective cycle 4

Original feedback is preserved. The following dispositions supersede the open
placeholders. The corrective commit contains this section and the named changes;
exact local outcomes and the product fingerprint are recorded in
`pr-2751-attempt-9-precommit-low.md`.

| Finding | What changed and why | Verification |
|---|---|---|
| 1 | Production calls now use named arguments, and the three-argument wrapper calls the canonical helper directly. The existing nine-argument forwarder is retained. Moving the entire implementation or removing helper signatures was not justified by a current defect and would unnecessarily restructure the contributor's work. The registry has no initialization cycle, as the reviewer established. | The isolated fixture checks production-wrapper results before and after a history increase. Multi-writer cases use native `numThreads=1`, and assert the actual four-writer/32-slot allocation, rather than relying only on helper injection. |
| 2 | Affinity and OS probes are by-name inputs. The OS result is cached lazily. The short subprocess has a five-second deadline, explicit warnings, forced cleanup of a live process, and stream cleanup. Interruption still propagates through `finally`. | A throwing unused probe checks lazy evaluation. Linux subprocess tests cover success, nonzero exit and a short timeout. No native macOS runtime execution is claimed; the production lifecycle uses Java 8 APIs. |
| 3 | All three documentation locations now describe the same fixed floor and conditional auto-sizing policy, including the single-writer correction. | Layout cases separately exercise the fixed floor and both single-writer policies. The overview also distinguishes total header memory from incremental memory. |
