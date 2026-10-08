## Review Summary
- **Round**: 1
- **Theme**: Broad Sweep (Correctness, security vulnerabilities, logic errors, spec conformance)
- **Mode**: resolved-plan; sequential final CI-qualified six-lens review, lens 1 of 6; Medium tier (primary-agent estimate); top-level review attempt 8, stage attempt 1, final pass 1
- **Model**: claude-opus-5.5
- **Artifact**: `reviews/pr-2751/pr-2751-attempt-8-stage-attempt-1-review-1-claude-opus-5.5.md`
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Scope: microsoft/SynapseML#2751 at head `0c453310a47ee8f1ce34befd6fc7f99badd540ca` against master `861c3a1e14a9511b5604563ff1e3976cefa82e90`. I reviewed the supplied complete frozen diff (nine product paths) and the matching worktree source. The manifest SHA256 `2727c176…` and diff SHA256 `495592f0…` were supplied by the driver; I did not recompute them.
- [x] Six registration sites. Each `registerNativeOmpThreads` call passes exactly the string that the next native call receives:
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/booster/LightGBMBooster.scala:242` (`LGBM_BoosterCreate`) and `:322` (`LGBM_BoosterResetParameter`).
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/DatasetAggregator.scala:413` (`LGBM_DatasetCreateFromMat`) and `:529` (`LGBM_DatasetCreateFromCSR`).
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala:57` (`LGBM_DatasetCreateFromSampledColumn`) and `:220` (`LGBM_DatasetCreateFromSerializedReference`).
  - The only other `lightgbmlib.LGBM_*` calls in `lightgbm/src/main` that take a parameter string are the prediction calls at `LightGBMBooster.scala:529-538` and `:551-558`. Their string `max_bin=255 predict_disable_shape_check=…` has no thread key. This matters for Issue 1.
- [x] Parser parity:
  - `parseLightGBMParams` (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala:92-104`) is unchanged context in this diff. It splits on space, tab, newline and carriage return, drops empty `=` pieces, accepts only `key=value` tokens, strips quotes, and keeps the first duplicate. That matches the supplied native first-duplicate evidence (`n_jobs=11 n_jobs=29` applies 11).
  - A one-part `num_threads` token (empty value) blocks a later duplicate natively but is ignored by Scala. Scala therefore only over-registers, which is the safe direction.
  - `positiveNumThreads` (`:112-118`) takes the maximum across aliases. That is never below the native canonical-then-shortest-alias choice. Tested at `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala:152-181`.
  - Device-key detection uses the same unchanged parser, so device behavior is unaffected.
- [x] Registry thread safety and visibility: `NativeOmpThreadRegistry` (`LightGBMUtils.scala:60-77`) uses an `AtomicInteger` plus a `ConcurrentHashMap` of `AtomicInteger`s with monotonic `accumulateAndGet`. Registration happens before each native call, so a failed native call can only over-register. The registry is per executor JVM. A concurrent widening after allocation is a documented limitation (`docs/Explore Algorithms/LightGBM/Overview.md:189-191`).
- [x] Positive-width invariant: the multi-writer branch (`LightGBMUtils.scala:221-237`) takes `max` over a sequence that always contains `MinStreamingOmpThreads = 16` (`:85`). The no-team-source fallback (`:224-230`) is also at least 16. Trace: `bound(4, 0, 0, None, None, None, -1, 0) == 16` (`StreamingLayoutSuite.scala:119`).
- [x] Sample input/output traces, worked by hand from source:
  - The traces use the effective `numThreads`. By default (`dataTransferMode=streaming`, `useSingleDatasetMode=true`, `numThreads` unset), `executionParams.numThreads` is `numTasksPerExec - 1` (`LightGBMBase.scala:439-442`; defaults at `params/LightGBMParams.scala:95` and `:108`). The same value becomes dataset `num_threads` (`LightGBMBase.scala:536`, `:714-716`, `:875-877`).
  - 4 local writers, 4 task slots (effective `numThreads` 3), `maxStreamingOMPThreads=16`, `OMP_NUM_THREADS` unset, affinity 32, no history: PR 32. Master gave `max(16, 3) = 16`, below the 32-thread team of helpers that have not run a LightGBM call. That is the original failure.
  - The same case with explicit `numThreads=0`: PR 32; master `-1`, meaning the initializing thread's team is measured.
  - 4 writers with registered history 64: PR 64.
  - 1 writer with effective `numThreads` 0: PR `-1`; master `-1`.
  - 1 writer with effective `numThreads` 3 (unset, 4 task slots, one local partition) or explicit 8: PR `-1`; master 16 (Issue 1).
- [x] Thread ownership:
  - Streaming forces single-dataset mode (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/TrainingContext.scala:52`).
  - The writer count is the executor-local partition list length (`BasePartitionTask.scala:88`), including empty partitions.
  - The training task initializes the dataset (`StreamingPartitionTask.scala:118`), then samples and pushes rows on the same thread (`:126`, `:128`). Helpers wait for initialization (`:122`) and push with their own index.
  - The training input reaches this task without a shuffle when the barrier-mode input already has no more partitions than `numTasks`, or through `coalesce` in non-barrier mode (`LightGBMBase.scala:178-216`). In those cases, lazily evaluated upstream stages run on the pushing thread.
- [x] Defaults, public API and serialization are unchanged:
  - `numThreads` default 0 (`params/LightGBMParams.scala:165`) and `maxStreamingOMPThreads` default 16 (`:179`). Only the parameter doc text changed (`:171-178`, `params/BaseTrainParams.scala:181-184`).
  - The `ExecutionParams` shape is unchanged (`BaseTrainParams.scala:186-194`).
  - The changed `streamingOmpAllocationBound` overloads are `private[lightgbm]`.
- [x] Dataset lifecycle: the allocation bound and `LGBM_DatasetInitStreaming` still run inside `initializeOwnedDataset` (`ReferenceDatasetUtils.scala:117-145`), so an initialization failure closes the owned dataset as before.
- [x] Security sweep found no injection, secret or path-traversal surface:
  - `firstCommandOutput` (`LightGBMUtils.scala:182-192`) runs only the fixed `sysctl -n hw.logicalcpu` on macOS, with no user input.
  - `/proc/self/status` is a fixed path.
  - Environment and system-property reads are null-guarded with `Option`.
  - The new child-JVM regression clears inherited environment variables before launch, confines tmp, crash and log files to a temp directory, passes the JVM option that suppresses crash core dumps (`-XX:-CreateCoredumpOnCrash`, or the legacy `-XX:-CreateMinidumpOnCrash` on Java 8, whose Linux effect I did not verify), applies a 5-minute timeout with a forced destroy, and deletes the directory (`split1/StreamingOmpRegressionSuite.scala:32-75`).
- [x] Requirement-to-test mapping:
  - Allocation bound: `StreamingLayoutSuite.scala:90-121`.
  - Env, affinity and OS helpers: `:123-135`.
  - Six-site monotonic registry: `:137-150`.
  - Alias safety bound: `:152-181`.
  - Production call-path registration: `:182` onward.
  - Linux child-JVM dense and sparse regression with a 32-thread ambient team and 4 writers: `StreamingOmpRegressionSuite.scala`.
  - Gaps: no test covers a single writer whose team changes after initialization; no test covers a malformed `OMP_NUM_THREADS` tail; the regression pins `OMP_DYNAMIC=FALSE` (`StreamingOmpRegressionSuite.scala:55`).
- [x] CI and runtime evidence (supplied by the driver, not re-run here) is treated as necessary, not as proof of correctness:
  - JDK11 compile and scalastyle passed.
  - Targeted suites passed 11/11.
  - Java 8 regression passed 2/2.
  - Azure build 239492673 passed all 65 jobs.
  - The synthetic fail-before is dense-only (sparse baseline passed), so I make no sparse fail-before claim. The original issue's exact production incident is not reproduced.
- [x] Performance: the two non-poolable before/after samples reverse direction, so they establish neither an attributable slowdown nor a speedup. The per-writer memory cost of the allocation width is real. It is a documented tradeoff (`Overview.md:193-197`), not counted here as a defect.
- [x] Evidence limitations:
  - I ran no build, test or native probe.
  - Native behavior was reasoned from upstream sources, not inspected in the bundled lightgbmlib 3.3.510 or the deployed OpenMP runtime: LightGBM 3.x `OMP_SET_NUM_THREADS` and `InitStreaming`, and GNU libgomp list parsing and `dyn-var`.
  - Pre-existing or documented limits, not counted as issues:
    - macOS `firstCommandOutput` calls `waitFor()` with no timeout, though the command is fixed.
    - On Linux, when neither `OMP_NUM_THREADS` nor `/proc/self/status` yields a team, the multi-writer bound falls back to the JVM-reported processor count and the 16-slot floor, with a warning. This is documented at `Overview.md:182-185`.
    - Registry tests raise the per-JVM high-water mark seen by later suites in the same JVM. That over-allocation could mask a later under-allocation regression.
    - Foreign native code and concurrent widening after allocation remain outside the mitigation, as documented.

## Issues

### Issue 1: A single writer can under-allocate when its own OpenMP team grows after `InitStreaming`
- **Severity**: Medium
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: 218-220. Related:
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala:121-141`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/StreamingPartitionTask.scala:118-128`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/booster/LightGBMBooster.scala:529-538, 551-558`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMBase.scala:196-216, 439-442, 536, 714-716, 875-877`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala:95, 108, 175`
  - `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala:111`
  - `docs/Explore Algorithms/LightGBM/Overview.md:181-182`
- **Description**:
  - **What changed.** For `externalThreads == 1` the bound now always returns `-1`, so LightGBM sizes the writer's slots from one `OMP_NUM_THREADS()` measurement inside `LGBM_DatasetInitStreaming` (`ReferenceDatasetUtils.scala:134`). The comment calls this the thread's "exact team". It is really the team at initialization time. The pushes happen later on the same thread while the input iterator is consumed (`StreamingPartitionTask.scala:126` and `:128`), and the team can grow in between in two reachable ways.
  - **Trigger (a): same-thread SynapseML prediction.**
    - `CreateFromSerializedReference` applies dataset `num_threads=<executionParams.numThreads>` on the initializing thread (`LightGBMBase.scala:536`, `:714-716`, `:875-877`). When that effective value N is positive, the measured width is N.
    - N is positive when `numThreads` is set explicitly. It is also positive by default: with `useSingleDatasetMode=true` (`params/LightGBMParams.scala:108`), an unset `numThreads` becomes `numTasksPerExec - 1` (`LightGBMBase.scala:439-442`).
    - Suppose the training DataFrame comes lazily from an upstream SynapseML LightGBM model (a stacked pipeline) and is not shuffled before training (`LightGBMBase.scala:178-216`). Then each upstream row is scored on the same task thread by `LGBM_BoosterPredictForCSRSingle` or `LGBM_BoosterPredictForMatSingle`. Their parameter strings have no thread key (`LightGBMBooster.scala:529`, `:551`).
    - In upstream LightGBM 3.x, `num_threads <= 0` makes `OMP_SET_NUM_THREADS` restore the process default team on the calling thread. Later pushes therefore run with the default team, which is wider than N whenever N is below it.
    - The multi-writer branch covers this case through `defaultTeam` (`LightGBMUtils.scala:222-236`). The single-writer branch never consults it.
  - **Trigger (b): `OMP_DYNAMIC=true`.** libgomp caps each parallel region at online CPUs minus the 15-minute load average. If load falls during a long ingestion, a later push team is wider than the team measured at initialization.
  - **Concrete trace for (a).**
    - Setup: 8-CPU executor host, `OMP_NUM_THREADS` unset, one task slot per executor (one local partition), `numThreads=4`, default `maxStreamingOMPThreads=16`, training input is uncached `model1.transform(df)`.
    - Master allocates `max(16, 4) = 16` slots, which covers the default team of 8.
    - The PR passes `-1`, so LightGBM measures 4. Upstream prediction then resets the thread to 8. OpenMP thread IDs 4-7 index past the `1 × 4` SparseBin push slots. That is a native out-of-range write, the failure class this PR fixes.
    - Dense input can hit it too when LightGBM chooses sparse feature groups. The supplied crash evidence shows dense input reaching `SparseBin::Push`.
    - Default-parameter variant:
      - Setup: 16-CPU host without a cpuset (default team 16), 4 task slots per executor, `numThreads` unset (effective 3), and an executor that receives only one training partition. For example, the automatic `numTasks` is capped at a small input partition count (`LightGBMBase.scala:196-201`) and the tasks spread across executors.
      - Master allocated `max(16, 3) = 16`. The PR measures 3, and the reset to 16 then overflows the `1 × 3` slots.
  - **New versus pre-existing.**
    - New whenever there is one writer and both the effective `numThreads` and `maxStreamingOMPThreads` are positive. That includes the default configuration on executors with several task slots. Master used the fixed width `max(cfg, numThreads)`, and `StreamingLayoutSuite.scala:111` now asserts `-1` even for `numThreads=32`.
    - When the effective `numThreads` is 0 (explicit `numThreads=0`, or unset with one task slot per executor), trigger (a) is harmless because dataset creation and prediction both reset to the default team. In that case trigger (b) existed before this PR.
    - When the default team is wider than `max(16, numThreads)`, both master and the PR are exposed to (a). The PR's mitigation does not cover single writers.
    - Neither trigger is among the documented limitations, which name foreign native code and concurrent fits (`Overview.md:189-191`).
  - **Unverified at runtime.** This comes from source reasoning about upstream LightGBM 3.x `OMP_SET_NUM_THREADS` and libgomp `dyn-var` semantics. I did not reproduce it.
- **Risk**: Native heap corruption, executor crashes or silently corrupted training rows in streaming fits where an executor has one local partition and either:
  - a positive effective `numThreads` below the process default team (explicit, or the default `numTasksPerExec - 1`) plus a lazily evaluated upstream SynapseML LightGBM prediction (stacking), or
  - OpenMP dynamic adjustment enabled.

  For the first configuration this is a safety regression relative to master.
- **Suggested Fix**:
  - Stop returning `-1` for a single writer when `configuredNumThreads > 0`. The simplest option is to apply the same positive bound (configured values, default team, registered history, floor) for every writer count. That also covers the default team restored by prediction and any dynamic narrowing.
  - If `-1` is kept for automatic `numThreads`, either treat a truthy `OMP_DYNAMIC` as requiring the positive bound or document dynamic mode as unsupported for streaming.
  - Update `StreamingLayoutSuite.scala:111`.
  - Add a single-partition streaming regression in which the pushing thread's team widens after initialization, for example by lazily scoring an upstream LightGBM model with `numThreads` below the default team.

### Issue 2: `firstOmpTeamSize` accepts `OMP_NUM_THREADS` values that GNU libgomp rejects, which skips the affinity fallback
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: 127-132 and 222-223. Tests: `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala:124-126`, `:130`.
- **Description**:
  - The helper validates only the first comma-separated element. GNU libgomp parses `OMP_NUM_THREADS` as a whole list. If any later element is empty, zero, negative or non-numeric (for example `8,`, `8,0` or `8,x`), libgomp rejects the variable and uses its default team, the CPU count from affinity.
  - For those values the PR takes 8 as `defaultTeam` and never consults `affinityCount`.
  - Trace:
    - Setup: 64 affinity CPUs, `OMP_NUM_THREADS="8,"`, `maxStreamingOMPThreads=16`, 4 local writers, no registered history.
    - With explicit `numThreads=0` the bound is `max(16, 16, 0, 8, 0) = 16`. With `numThreads` unset on 4 task slots (effective 3) it is `max(16, 16, 3, 8, 0) = 16`.
    - Pooled helpers that have not run a LightGBM call keep libgomp's 64-thread default team. So does the initializing thread when the effective `numThreads` is 0. Their thread IDs overlap other writers' slots, and the last writer indexes past the 64 allocated slots.
  - New versus pre-existing:
    - With explicit `numThreads=0`, master returned `-1`. The initializing thread measured the 64-thread default, so uniform default teams were covered. The PR's 16 is a regression.
    - With the default effective 3, master gave `max(16, 3) = 16` and was equally exposed. There the PR leaves the original failure unfixed for this input rather than introducing it.
  - This is a new code path, reachable only with a malformed variable (libgomp prints a warning at load).
  - The docs call detected counts hints, not proved bounds (`Overview.md:185-186`). This case is different: it is a deterministic parsing divergence on an input libgomp has already rejected, not an unknowable runtime team.
  - It is also inconsistent with the affinity parser in the same change, which rejects a partially invalid list (`parseCpuAffinityList("0-3,bad")` is empty, `StreamingLayoutSuite.scala:130`).
  - Unverified at runtime: based on libgomp's list-parsing semantics, not checked against the deployed runtime.
- **Risk**:
  - Under-allocation and native out-of-range writes on hosts with more than 16 affinity CPUs when `OMP_NUM_THREADS` has a malformed tail.
  - The documented claim that the team is "derived from `OMP_NUM_THREADS`" (`Overview.md:176`) is wrong in exactly this case.
- **Suggested Fix**:
  - Return the first element only if every trimmed comma-separated element is a positive integer. Otherwise return `None` so the affinity and OS fallbacks apply.
  - Add `8,`, `8,0` and `8,x` cases to `StreamingLayoutSuite`.

### Issue 3: Overview states an unconditional 16-slot floor that the single-writer path does not provide
- **Severity**: Low
- **File**: `docs/Explore Algorithms/LightGBM/Overview.md`
- **Line(s)**: 174, contradicted by 181-182. Code: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala:218-220`.
- **Description**:
  - Line 174 says "Streaming allocation uses at least 16 OpenMP slots" with no qualification.
  - With one pushing thread, SynapseML passes `-1` and LightGBM sizes the allocation from the measured team. That can be below 16, for example 4 or 8 on small executors or with `numThreads=4`.
  - Lines 181-182 describe the single-writer exception only afterward, so the paragraph contradicts itself.
  - The parameter docs (`params/LightGBMParams.scala:171-178`, `params/BaseTrainParams.scala:181-184`) are accurate.
- **Risk**: Users reasoning about memory or safety margins from the documentation get a floor that does not hold for single-partition executors. Low impact; documentation accuracy only.
- **Suggested Fix**:
  - Qualify the sentence, for example: "With several pushing threads, streaming allocation uses at least 16 OpenMP slots …". Alternatively, state the single-writer behavior first.
  - If Issue 1 is fixed by applying the positive bound to every writer count, the current wording becomes accurate as written.

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

The original review above is unchanged. These dispositions supersede its open
placeholders, not its original `ISSUES_FOUND` verdict. The correction is in the
commit containing this section. Local command results and the corrected product
fingerprint belong to `pr-2751-attempt-9-precommit-low.md`; remote checks must cover
the subsequently published head.

| Finding | What changed and why | Verification |
|---|---|---|
| 1 | `LightGBMUtils.streamingOmpAllocationBound` retains the conservative positive bound for a single writer with positive configured native threads or `OMP_DYNAMIC=true`. Native auto-sizing remains only for automatic, static teams. This preserves the contributor's multi-writer approach while covering lazy prediction and dynamic growth. | Independently confirmed on published lightgbmlib 3.3.510 that prediction restores the default team. The new public stacked-model case against unchanged `0c453310` logged one writer and bound -1, then crashed with SIGSEGV in `SparseBin<unsigned char>::Push+0x33`, child exit 134. The corrected case requires bound 16, evaluated predictions and bulk parity. The layout suite separately checks the dynamic-team branch; no load-dependent dynamic crash reproduction is claimed. |
| 2 | `firstOmpTeamSize` validates the complete list before trusting its first element. Invalid tails fall back to affinity. Leading `+` remains valid because the actual bundled GNU runtime accepts it; rejecting it could under-read a valid large request. | Actual native probes rejected `8,`, `8,0`, `8,x`, and `8,-1`, using the affinity default instead. They accepted `8,2` and `+8`. Parser tests cover these forms; the bound test checks malformed input with a larger affinity count. |
| 3 | Overview, parameter documentation and `ExecutionParams` scaladoc now distinguish fixed bounds from the restricted auto-size case. | Source comparison checks the documented conditions against the actual branch. New tests cover one writer with automatic and positive configured thread counts and the fixed 16-slot floor. |
