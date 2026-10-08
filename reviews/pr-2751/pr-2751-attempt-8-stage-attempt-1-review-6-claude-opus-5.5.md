## Review Summary
- **Round**: 6
- **Theme**: Polish & Hardening — performance, observability, documentation, naming clarity
- **Mode**: resolved-plan for the supplied review context. Medium tier from the primary-agent estimate. Sequential independent lens 6 of 6, per the target PR-loop stage-order override. Top-level review attempt 8, stage attempt 1, final CI-qualified pass 1. Independent source inspection only; no builds.
- **Model**: claude-opus-5.5
- **Artifact**: `reviews/pr-2751/pr-2751-attempt-8-stage-attempt-1-review-6-claude-opus-5.5.md`
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

All four issues are Low severity. None is a performance defect: I found no attributable runtime regression. The wider allocation's memory cost is a documented tradeoff, and the supplied benchmark is statistically inconclusive (see the checklist).

## Evidence Checklist
- [x] **Identity and scope.**
  - The lens prompt's SHA-256 matched `0dd90805e983043bb40937872ddf1de1ee76b08596744c634931297001f07c02`, and I read the whole prompt.
  - Read-only `git --no-optional-locks` confirmed:
    - HEAD is `0c453310a47ee8f1ce34befd6fc7f99badd540ca` and the merge base is `861c3a1e14a9511b5604563ff1e3976cefa82e90`.
    - The tracked tree is clean.
    - Exactly nine product paths changed: eight modified and one added, with 601 insertions and 28 deletions (`reviews/` excluded).
  - I compared the supplied complete product diff with `git diff 861c3a1e..0c453310 -- . ':(exclude)reviews'`. The only difference is one trailing blank line.
  - Manifest `2727c176…` and binary-diff `495592f0…` are driver-supplied; I did not recompute them.
- [x] **Changed sources inspected with line numbers.**
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`: call sites 22-58, registry 60-77, alias parsing 112-118, OMP/affinity/OS helpers 127-207, bound 209-238.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`: 14, 57, 117-145, 147-180, 220.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/booster/LightGBMBooster.scala`: 242, 322.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/DatasetAggregator.scala`: 413, 529.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala`: 169-181.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/BaseTrainParams.scala`: 181-184.
  - `docs/Explore Algorithms/LightGBM/Overview.md`: 169-197.
  - Both changed test suites.
- [x] **Unchanged context inspected.**
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/StreamingPartitionTask.scala`: the training task initializes the dataset at 117-118; rows are pushed at 219 and 257.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/TrainUtils.scala` 119-124.
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMBase.scala` 439-442.
  - Prediction parameters in `LightGBMBooster.scala` 529 and 551, which carry no thread keys.
  - The module logger convention in `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/LightGBMDataset.scala` 206.
- [x] **Hot-path check (not a defect).** Each of the six registrations runs once per native dataset or booster creation, plus once per `resetParameter`. `TrainUtils.scala` 119-124 calls `resetParameter` only when the learning rate changes. Each registration parses the parameter string and applies an `AtomicInteger`/`ConcurrentHashMap` max-update. The registry does no work per row or per batch, and none per iteration while the learning rate is unchanged.
- [x] **Performance classification: documented tradeoff, not a defect.** A wider multi-writer width increases sparse push-buffer headers and slot traversal. `Overview.md` 193-197 discloses this and explains why there is no cap below the team. Realistic triggers:
  - The default single-dataset `numThreads = numTasksPerExec - 1` (`LightGBMBase.scala` 441) with a 64-CPU affinity mask moves width from 16 to 64.
  - A Kubernetes affinity mask broader than the CPU quota.
  - A large `OMP_NUM_THREADS` with a smaller `OMP_THREAD_LIMIT` over-allocates. An optional safe refinement is to cap at a positive `OMP_THREAD_LIMIT`.

  Small positive hints, such as 1, are now raised to 16; the docs state "at least 16".
- [x] **Performance classification: the benchmark is statistically inconclusive, with no attributable regression.**
  - **Fresh scenario.** Both revisions allocate width 16: verified per fit in the counterbalanced sample and analytically identical in the sequential one. The deltas therefore cannot come from the allocation change. Sequential showed +32.8% first fit and +17.9% warm; counterbalanced showed −12.2% and −14.5%. The direction reverses between samples, which points to ordering or noise.
  - **History scenario (width 16→64).**
    - Header arithmetic predicts +9 MiB (3→12 MiB).
    - Measured whole-process RSS moved −2.1% in the sequential sample and +30 MiB (+1.1%) in the counterbalanced sample.
    - Counterbalanced warm process-median ranges overlap: base 3.446-5.666 s, PR 3.915-4.072 s.
  - Neither sample establishes a slowdown, a speedup or broad non-regression, and I did not pool them. All 80 measured fits passed per-row parity.
- [x] **Backward compatibility.**
  - None of the following changed:
    - public JVM signatures;
    - the `maxStreamingOMPThreads` name or its default of 16;
    - the `ExecutionParams` shape;
    - serialization;
    - Python wrapper signatures.
  - New members are `private[lightgbm]`.
  - The param description and semantics did change:
    - a nonpositive hint no longer selects native sizing when there are multiple writers;
    - small hints are raised to 16.

    Both changes are documented.
- [x] **Hygiene.**
  - No TODO, FIXME or HACK lines were added; two regex hits were `toDouble`.
  - No commented-out code was added.
  - Every new import in `LightGBMUtils.scala` is used.
  - Production code that only tests use is reported in Issues 3 and 4.
- [x] **Tests reviewed for observability.**
  - `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala`:
    - bound table 90-121 (single-writer −1, the floor, three fallback warnings);
    - helpers 123-135;
    - registry 137-180;
    - production paths 182-248.
  - `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingOmpRegressionSuite.scala` 31-78 keeps an 8000-character diagnostic tail. It confines temporary and crash files and deletes them by design.
- [x] **Driver-supplied CI and regression evidence considered, not treated as proof.**
  - Azure build 239492673: 65 of 65 jobs succeeded.
  - 414 LightGBM tests passed.
  - Regression cases: dense passed in 23.827 s, sparse in 22.966 s.
  - Substituting the target source crashed the dense case in `SparseBin::Push`, while the sparse baseline passed. There is therefore no public sparse fail-before.
- [x] **Pre-existing observations, outside the diff and not counted.**
  - These multi-writer tests set `setMaxStreamingOMPThreads(1)`, which is now raised without notice:
    - `split1/GroupIdManagerSuite.scala` 80
    - `split1/StreamingDatasetLifecycleSuite.scala` 29
    - `split1/StreamingFeaturePreflightSuite.scala` 46
    - `split1/StreamingFeatureSizeSuite.scala` 36
  - `lightgbm/src/test/python/fabric_streaming_regression.py` 123, 177 and 357 report the configured hint, not the effective width.
  - The sparse aggregator logs "generating dense dataset" (`DatasetAggregator.scala` 526).
  - `maxStreamingOMPThreads` now names a hint rather than a maximum, but renaming it would break compatibility.
- [ ] Builds, scalastyle, black, codegen and tests: not run by this reviewer, because this lens prohibits them. I relied on driver-supplied CI.
- [ ] Native `lightgbmlib` 3.3.510 source is not in the worktree. I took the push-buffer layout and the feature-group/multi-value-group structure from driver statements only; see the unverified note in Issue 1.
- [ ] The macOS and Windows fallback paths were not executed. Issues 2 and 3 come from reading the code.
- [ ] The benchmark was not reproduced. Small samples (three and two JVMs per scenario and revision) and whole-process RSS limit any memory or time attribution.

## Issues

### Issue 1: Streaming-allocation docs overstate the 16-slot floor, disagree with each other, and misdescribe the memory formula
- **Severity**: Low
- **File**: `docs/Explore Algorithms/LightGBM/Overview.md`; also `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala` and `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/BaseTrainParams.scala`
- **Line(s)**: `Overview.md` 171-174, 181-186 and 193-197; `LightGBMParams.scala` 171-178; `BaseTrainParams.scala` 181-184. Code reference: `LightGBMUtils.scala` 218-236.
- **Description**:
  1. **The 16-slot floor is stated unconditionally.** `Overview.md` 174 says "Streaming allocation uses at least 16 OpenMP slots". However, `LightGBMUtils.scala` 218-220 returns −1 when `externalThreads == 1`, and LightGBM then sizes the buffers from the initializing thread's team. That team can be smaller than 16, for example with `OMP_NUM_THREADS=4`. Lines 181-182 describe this exception only afterwards, so the paragraph contradicts itself.
  2. **The param description limits the floor to the fallback case.** `LightGBMParams.scala` 171-178 is the user-visible description, and codegen copies it into the Python docstrings. It mentions the floor only in the OS/JVM fallback clause ("with the 16-thread floor"). Yet `LightGBMUtils.scala` 231-236 includes `MinStreamingOmpThreads` in every multi-writer bound. A user who sets the hint to 4 with `OMP_NUM_THREADS=4` and multiple local partitions gets 16, which this text does not predict.
  3. **The scaladoc is incomplete and overstates certainty.** `BaseTrainParams.scala` 181-184 lists only `OMP_NUM_THREADS` and Linux affinity. It omits `numThreads`, previously registered thread keys and the floor. It also calls these inferred values "native teams observed", which contradicts `Overview.md` 185-186: "hints, not a proved upper bound".
  4. **The memory paragraph is imprecise** (`Overview.md` 193-197):
     - "Additional" reads as the increment this change causes. The formula actually gives the total header footprint at the current width. The increment attributable to the change is `(new width − old width) × …`: +9 MiB, not 12 MiB, in the supplied history benchmark.
     - "Upper bound of approximately" mixes a bound with an estimate.
     - "For sparse streaming data" can be read as `matrixType=sparse`. Dense input also fills sparse bins: the supplied fail-before crashed the dense case in `SparseBin::Push`.
     - *Unverified:* the unit may need to be sparse bins rather than feature groups. That holds if LightGBM 3.3.510 multi-value groups hold one sparse bin per feature and dense bins hold no push buffers. The native source is not in the worktree, so this is a wording risk to check, not an established error.
  5. **"Allocation width" has no pointer to where users can see it.** The new paragraph introduces the term without saying where to observe it. The existing verbosity-2 log already prints `allocationBound` (`ReferenceDatasetUtils.scala` 126-133, where −1 means native-sized). The neighbouring, pre-existing sentence at `Overview.md` 171-172 lists only partition IDs, row count and external-thread count.
- **Risk**: Users planning capacity, or lowering `maxStreamingOMPThreads` to save memory, can mispredict the width and the memory used. Users with dense input may assume the memory note does not apply to them. The three descriptions disagree with each other. There is no runtime failure.
- **Suggested Fix**:
  - Qualify the floor: "With several pushing threads, allocation uses at least 16 slots…; with exactly one pushing thread, LightGBM sizes it natively."
  - State the always-on multi-writer floor in the `LightGBMParams` description, and align the `BaseTrainParams` scaladoc with it: list all inputs and use "hints".
  - Rewrite the memory sentence as push-buffer vector headers for sparse bins, which come from dense or sparse input. Use the total, not "additional", wording, and check the unit against the 3.3.510 source.
  - Mention that verbosity 2 logs `allocationBound`, and that −1 means native-sized.

### Issue 2: The macOS processor-count fallback starts an unbounded, uncached `sysctl` subprocess on every streaming initialization
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`; also `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`
- **Line(s)**: `LightGBMUtils.scala` 182-192 (`waitFor()` at 188), 194-207 and 218-230; `ReferenceDatasetUtils.scala` 150-159 (the hints are evaluated eagerly at 154-158).
- **Description**:
  - **Every hint is evaluated before it is known to be needed.** The 3-argument `ReferenceDatasetUtils.streamingOmpAllocationBound` evaluates every hint as a strict argument, including `LightGBMUtils.osReportedProcessorCount()` at 156. This happens before `LightGBMUtils.streamingOmpAllocationBound` checks `externalThreads == 1` (218) or whether `OMP_NUM_THREADS` or affinity already decides the team (222-223).
  - **On macOS this starts a subprocess every time, and the result is often thrown away.** `osReportedProcessorCount()` (194-207) runs `sysctl -n hw.logicalcpu` from the Spark task thread on each streaming training-dataset initialization (`StreamingPartitionTask.scala` 117-118). The value is constant for the process, yet it is never cached. It is discarded for single-writer partitions and whenever `OMP_NUM_THREADS` is set.
  - **The wait has no timeout or cleanup.** `firstCommandOutput` (182-192) blocks reading the first line and then calls `process.waitFor()` without a timeout. It never destroys the child. `Try` does not catch `InterruptedException`, so if the task is killed during the wait, the exception propagates and the child is left behind.
- **Risk**: A stalled or slow `sysctl` blocks dataset initialization indefinitely for a value that is only a fallback hint. This can happen under endpoint-security hooks or process-limit pressure. Every fit also pays for a fork/exec. The impact is limited to macOS hosts, mostly local or development use, and the cost is off the per-row path: Linux evaluates no subprocess, and Windows reads an environment variable. This is a hardening gap, not a measured regression.
- **Suggested Fix**:
  - Memoize the OS count in a `lazy val`, so there is at most one probe per JVM.
  - Evaluate it only on the fallback branch, by passing a thunk or by-name parameter or by moving the probe into the `getOrElse`.
  - Bound the probe: `if (process.waitFor(timeout, unit))`, then read the single short line; otherwise call `destroyForcibly()` and return `None`. Also destroy a live child in `finally`. Both APIs exist on Java 8.

### Issue 3: Allocation-width provenance is not observable, including the silent, process-wide registry history
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`; also `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`
- **Line(s)**: `LightGBMUtils.scala` 22-58, 60-77, 84, 121-125, 222-230 and 231-236; `ReferenceDatasetUtils.scala` 126-133. Registration call sites: `ReferenceDatasetUtils.scala` 57 and 220, `LightGBMBooster.scala` 242 and 322, `DatasetAggregator.scala` 413 and 529.
- **Description**:
  - **Only the result is logged.** The width is the maximum of five inputs:
    - the floor;
    - the hint;
    - `numThreads`;
    - the OMP, affinity or OS default team;
    - the process-wide high-water mark.

    The only record is a verbosity-2 line showing `configuredMaxStreamingOMPThreads` and `allocationBound` (`ReferenceDatasetUtils.scala` 126-133). It does not say which input decided the width.
  - **Registry history changes later fits without trace.**
    - The registry is process-wide, never decreases and is never reset (60-77, 84). In a long-lived executor JVM, one earlier fit or booster with `numThreads=64` or `n_jobs=64` permanently quadruples the width for every later fit; this is the supplied history scenario (16→64).
    - Nothing records when the high-water mark rises, which call site raised it, or which parameter caused it.
    - `register` returns the new maximum, but all six call sites discard it.
  - **The fallback warning carries no values.** The WARN at 225-227 does not include the OS count, the JVM count, the resulting bound, or the variable that would silence it (`OMP_NUM_THREADS`). It repeats on every multi-writer initialization on hosts without Linux affinity.
  - **The per-site state is used only by tests.** These production members are read only by `StreamingLayoutSuite.scala` 139-147 and 229-246:
    - `NativeOmpCallSite.name` and `Values` (22-58);
    - the per-site map (62 and 69-70);
    - `current(site)` (76);
    - `nativeOmpThreadHighWaterMark(site)` (125).

    Production code therefore carries native-API names and per-site state that no log or metric uses.
- **Risk**: Suppose memory grows because of history or a host-wide affinity mask. Operators cannot tell from the logs that an earlier fit or the environment raised the width, so investigating the documented tradeoff requires reading source and reproducing at verbosity 2. The width multiplies directly into header memory. By the documented formula, 20,000 feature groups × 16 local partitions × 24 B is about 117 MiB at width 16 and about 469 MiB at width 64. This is formula arithmetic, not a measurement.
- **Suggested Fix**:
  - At verbosity > 1, or at debug level, log each input value and the deciding source next to `allocationBound`.
  - Log once per high-water-mark increase, with `site.name` and the new value, at debug or info level. `register` already returns the maximum.
  - Add the OS count, JVM count and chosen bound to the fallback warning, and emit it once per JVM.
  - Alternatively, if the per-site map and names exist only for tests, remove them from production.

### Issue 4: Inconsistent overload parameter order, a pass-through overload and misleading names invite transposition and misuse
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`; also `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: `ReferenceDatasetUtils.scala` 10, 14, 23, 78, 97, 122-127, 147-159 and 162-180; `LightGBMUtils.scala` 74-76, 123-125 and 209-217. Convention: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/LightGBMDataset.scala` 206.
- **Description**:
  1. **Parameter order differs between overloads.** `streamingOmpAllocationBound` has three positional entry points whose leading arguments are all `Int`, in different orders:
     - the 3-argument form (`ReferenceDatasetUtils.scala` 147-149) is `(configuredMaxThreads, configuredNumThreads, externalThreads)`;
     - both 9-argument forms (`ReferenceDatasetUtils.scala` 162-170 and `LightGBMUtils.scala` 209-217) start with `(externalThreads, configuredMaxThreads, configuredNumThreads, …)`.

     A future caller who uses the 9-argument order on the 3-argument form will still compile. For example, `(executorPartitionCount = 4, hint = 16, numThreads = 1)` passes 1 as `externalThreads` and returns −1. Four concurrent writers would then get single-team native sizing, which is the underallocation this PR fixes. The only current caller (122-125) uses the correct order.
  2. **The 9-argument `ReferenceDatasetUtils` overload is a pure pass-through.** Lines 162-180 only forward to the identical `LightGBMUtils` signature. It is reached only from the 3-argument wrapper (150) and from `StreamingLayoutSuite.scala` 100.
  3. **The new logger reuses a type name.** `private val Logger` (14) has the same name as the imported `org.slf4j.Logger` type (10), which types the `log: Logger` parameters in the same object (23, 78 and 97). The module convention is `private val Log: Logger` (`LightGBMDataset.scala` 206). The pre-existing verbosity log at 127 still calls `LoggerFactory.getLogger(getClass)` inline instead of reusing the new value.
  4. **`current` returns a history, not a current value.** `NativeOmpThreadRegistry.current` and `current(site)` (`LightGBMUtils.scala` 74-76) return a historical maximum that never decreases, not the current native team. The facade name `nativeOmpThreadHighWaterMark` (123-125) is accurate, but the class API invites reading it as the current team.
- **Risk**: Today this affects maintainability only, because every current call site is correct. However, a silent transposition can reintroduce the native out-of-range write, and the naming slows review and debugging.
- **Suggested Fix**:
  - Use one parameter order with `externalThreads` first, or use named arguments at call sites.
  - Have the 3-argument wrapper call `LightGBMUtils.streamingOmpAllocationBound` directly, point the test at it, and remove the pass-through.
  - Rename the logger to `Log: Logger` and reuse it at 127.
  - Rename `current` to `highWaterMark`.

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

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending driver disposition
- **How verified**: pending

## Driver dispositions, corrective cycle 4

Original feedback is preserved. These dispositions supersede the open
placeholders. The corrective commit containing this section includes the changes;
`pr-2751-attempt-9-precommit-low.md` records exact local outcomes and the product
fingerprint. These notes do not convert the original pass to a clean verdict.

| Finding | What changed and why | Verification |
|---|---|---|
| 1 | Align overview, parameter description and scaladoc with fixed bounds and restricted native auto-sizing. Name the logged `allocationBound`, explain -1, and describe total sparse-bin header memory separately from its increment. State that sparse bins can come from dense or sparse input. | Compare the conditions to the helper and its boundary tests. The public dense fail-before reached `SparseBin::Push`. The 24-byte term is explicitly an approximate typical-64-bit header estimate, not measured native allocation or a universal ABI guarantee. |
| 2 | Use lazy inputs, cache the OS count, wait with a five-second deadline, and destroy any live subprocess in `finally`. Emit warnings for timeout, unsuccessful exit or probe failure. | Tests exercise skipped throwing probes and subprocess success/nonzero exit/timeout using Java 8-compatible APIs. No native macOS benchmark or execution is claimed. |
| 3 | Add guarded debug logging for writer count, configured hint, native thread count, derived default-team hint, registered history, dynamic mode and chosen bound. Point users to that logger in the overview. | Source inspection confirms only numeric sizing inputs and a boolean are logged, with no parameter strings or credentials. This identifies history-driven increases without adding per-row logs, mutable warning-suppression state, or a new logging abstraction. Per-site state remains useful for registration coverage; tracing every historical request is not needed to correct the reported observability gap. |
| 4 | Use named arguments at production entry points, call the canonical helper directly, rename the private logger to `Log` and reuse it. Keep existing forwarding signatures and registry method names rather than broadening a correctness follow-up into API restructuring. | The child checks production argument mapping and history, and the public allocation assertions protect writer count and width. The existing facade remains accurately named `nativeOmpThreadHighWaterMark`. No current transposition defect was found in the original source. |
