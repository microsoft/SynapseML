## Review Summary
- **Round**: 4
- **Theme**: Detailed Correctness
- **Mode**: Sequential six-lens final CI-qualified pass (top-level review attempt 8, stage attempt 1, final pass 1); resolved-plan for supplied review context
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2751/pr-2751-attempt-8-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Scope and integrity. The prompt hash matched the assigned value. Worktree HEAD is `0c453310a47ee8f1ce34befd6fc7f99badd540ca` with no tracked modifications. The merge-base diff against master `861c3a1e14a9511b5604563ff1e3976cefa82e90` covers exactly the nine product paths in the supplied frozen diff. Only read-only `git rev-parse`, `git status` and `git diff --stat` were used.
- [x] Registration completeness. Each of the six sites registers the exact string that is passed to the native call:
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`: line 57 (`LGBM_DatasetCreateFromSampledColumn`) and line 220 (`LGBM_DatasetCreateFromSerializedReference`).
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/booster/LightGBMBooster.scala`: line 242 (`LGBM_BoosterCreate`) and line 322 (`LGBM_BoosterResetParameter`).
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/DatasetAggregator.scala`: line 413 (`LGBM_DatasetCreateFromMat`) and line 529 (`LGBM_DatasetCreateFromCSR`).
  - A grep of every `lightgbmlib.LGBM_*` call under `lightgbm/src/main/scala` found only prediction calls among the remaining parameterized calls. Their strings are built internally without thread keys (`LightGBMBooster.scala` lines 529 and 551).
- [x] Data flow into initialization.
  - `getDatasetCreationParams` always appends `num_threads=<execution numThreads>` (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMBase.scala`, lines 527-539). The single-dataset default is `numTasksPerExec - 1` (line 441).
  - The initializing thread therefore applies that value in `LGBM_DatasetCreateFromSerializedReference` immediately before `LGBM_DatasetInitStreaming` (`ReferenceDatasetUtils.scala`, lines 106-141).
  - Writer IDs are the sorted position in `executorPartitionIdList`, and the count is its length (`BasePartitionTask.scala`, lines 84 and 88). IDs are therefore in `[0, count)`, and the `externalThreads == 1` test is correct because the count is at least 1.
- [x] Bound arithmetic (`lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`).
  - Lines 222-236: both `Seq(...).filter(_ > 0).max` expressions always contain `MinStreamingOmpThreads`. They cannot throw and always return at least 16 for multiple writers.
  - Lines 134-162 (affinity parsing): the inclusive range uses `end - start + 1` in `Long`. The total is range-checked before `toInt`, and `indexOf(':') + 1` is safe after the prefix match.
  - The change has no floating-point comparisons. The OS-name check uses `Locale.ROOT`, and `toInt` is locale-independent.
- [x] Parser equivalence. I compared the pre-existing `parseLightGBMParams` (lines 92-104) with the supplied LightGBM `Str2Map`/`KV2Map` evidence. Both use the same whitespace delimiters, skip empty `=` segments, strip quotes and keep the first duplicate. Canonical/alias and alias/alias conflicts register their maximum, which is conservative as intended. Bare keys and non-ASCII digits cause over-registration, which is the safe direction. One divergence in the unsafe direction is reported in Issue 2(b).
- [x] Thread safety and visibility (lines 60-78). `AtomicInteger.accumulateAndGet` and `ConcurrentHashMap.computeIfAbsent` are monotonic and lock-free, and registration happens on the same thread before each native call. The site mark and global mark are not updated atomically together, but only the global mark sizes allocations. A concurrent increase after allocation is a documented limitation (`docs/Explore Algorithms/LightGBM/Overview.md`, lines 189-191).
- [x] Public API and serialization.
  - The `maxStreamingOMPThreads` default stays 16 (`params/LightGBMParams.scala`, line 179).
  - The `ExecutionParams` shape is unchanged; only its scaladoc changed (`params/BaseTrainParams.scala`, lines 181-184).
  - The new types are `private[lightgbm]`, and the existing 3-argument overload is retained.
- [x] Tests walked by hand. Every assertion in `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala` agrees with the implementation:
  - layout, lines 57-86;
  - the 9-argument bound table, lines 111-120, including exactly three fallback warnings;
  - environment and affinity helpers, lines 124-134;
  - the registry, lines 137-150;
  - aliases and duplicates, lines 152-179;
  - production-path registration, from line 181.

  `StreamingOmpRegressionSuite.scala` forces `OMP_NUM_THREADS=32`, `OMP_THREAD_LIMIT=32`, `OMP_DYNAMIC=FALSE` and `OMP_PROC_BIND=FALSE` (lines 53-56), so it cannot exercise Issues 1-3.
- [x] Encoding observation (not counted). `linuxProcessAffinityCount` decodes `/proc/self/status` as strict UTF-8 through `Files.readAllLines` (`LightGBMUtils.scala`, line 158). If the process `Name:` field were not valid UTF-8 (for example, a multibyte launcher name truncated by the kernel), decoding would throw. `Try` would swallow the error, and the code would fall back to the warned processor-count path. This is practically unreachable for `java`. Decoding as ISO-8859-1 would remove the hazard.
- [x] Other observations (not counted):
  - The 3-argument overload eagerly reads `/proc` and, on macOS, spawns `sysctl` on every initialization, even for a single writer (`ReferenceDatasetUtils.scala`, lines 154-158).
  - `firstCommandOutput` waits with no timeout (`LightGBMUtils.scala`, lines 182-193).
  - The fallback warning repeats on every multi-writer initialization on hosts that have neither `OMP_NUM_THREADS` nor Linux affinity.
  - The registry is scoped to the classloader through the object singleton, not to the process.
- [x] Evidence limits.
  - I ran no builds, tests or native runs, as the policy requires.
  - LightGBM parser and `SparseBin` indexing semantics come from the driver-supplied evidence.
  - The GNU libgomp behavior that Issues 1-3 rely on comes from upstream libgomp source, not from the bundled binary. That behavior is load-based team sizing under `OMP_DYNAMIC`, rejection of the whole list for an invalid `OMP_NUM_THREADS`, and capping teams at `OMP_THREAD_LIMIT`. I did not verify it against the OpenMP runtime that the `lightgbmlib` 3.3.510 binary actually links.
  - Green Azure build 239492673 and the 11/11 targeted suites do not exercise these configurations.
  - The sparse synthetic case passes on the baseline, and the private issue2333 incident remains unproven.
  - The performance samples are small and change direction between runs. The header-memory cost is real: +9 MiB at width 64 compared with width 16.
- [ ] Runtime traces or failing-then-passing output for Issues 1-3. Not produced, because this lens forbids builds and native execution.

## Issues

### Issue 1: Single-writer native auto-sizing (`-1`) is unstable under `OMP_DYNAMIC`, and the docs overstate the 16-slot floor
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: 218-220. Related: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala` lines 121-141, and `docs/Explore Algorithms/LightGBM/Overview.md` lines 174 and 181-182.
- **Description**:
  - With one external writer, the bound returns `-1`. `LGBM_DatasetInitStreaming` then measures one parallel team on the initializing thread and allocates exactly that many slots.
  - Each later micro-batch push opens a new parallel region and indexes by `omp_get_thread_num()`. The comment ("can measure its exact team") assumes every later push has the same team size as the measurement, which holds only while dynamic adjustment is off.
  - With `OMP_DYNAMIC=true`, GNU libgomp sizes each region as the minimum of the thread setting and online CPUs minus the 15-minute load average. A team measured under load can therefore be smaller than a later push team on the same thread.
  - Master avoided this. For a single writer with positive `numThreads` and a positive hint, it passed `max(hint, numThreads)`. Because `datasetParams` always applies `num_threads=numThreads` on that same thread just before initialization (the premise of the `SerializedReferenceDataset` registration), every push team was at most `numThreads`, which never exceeded the allocation, even with dynamic teams.
  - This PR uses `-1` for every single-writer executor. An example is one local partition on a multi-slot executor, where the default `numThreads = numTasksPerExec - 1` is greater than 0. In that case the change only saves memory and removes the dynamic-team margin. `maxStreamingOMPThreads` no longer affects this path, so users cannot override it.
  - Separately, `Overview.md` line 174 says unconditionally that streaming allocation "uses at least 16 OpenMP slots and is raised to cover" the hint, `numThreads`, the process team and registered values. None of that applies to one pushing thread, where the allocation can be 3. Lines 181-182 acknowledge the exception only afterwards.
- **Risk**: The trigger is narrow and this is a regression from master only in that case. It needs a truthy `OMP_DYNAMIC`, which is not the default, a single-writer executor, and positive `numThreads`. The consequence is the defect class this PR fixes: writer 0 pushes indices at or beyond the allocated slots, causing out-of-range native writes or a crash. The documentation also promises a floor that does not exist for single-writer executors.
- **Suggested Fix**:
  - In the single-writer branch, keep `-1` only when no positive bound is provable and dynamic adjustment is off. For example, when `configuredNumThreads > 0`, return `max(MinStreamingOmpThreads, configuredMaxThreads, configuredNumThreads)` (master's behavior, which covers the same-thread team). Otherwise, use the multi-writer bound whenever `OMP_DYNAMIC` is truthy.
  - Add a `StreamingLayoutSuite` case for `externalThreads == 1` with positive `numThreads`.
  - Limit the "at least 16 OpenMP slots" sentence to multiple pushing threads.

### Issue 2: Thread-count string parsing can under-read the native team (malformed `OMP_NUM_THREADS` lists; out-of-range thread values)
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: 127-132 and 222-223 (`firstOmpTeamSize` flowing into `defaultTeam`); 112-118 (`positiveNumThreads`, with its comment at 113-114)
- **Description**:
  - **(a) Malformed `OMP_NUM_THREADS` list.** `firstOmpTeamSize` validates only the first comma-separated element.
    - GNU libgomp parses `OMP_NUM_THREADS` as a whole list. If any element is empty, zero or non-numeric (`"8,"`, `"8, "`, `"8,0"`, `"8,x"`), it reports `Invalid value for environment variable OMP_NUM_THREADS`, ignores the variable, and keeps its default team: the CPU count of the process affinity mask.
    - Scala instead returns `Some(8)`, so `.orElse(affinityCount)` is never consulted.
    - Worked example: 64-CPU affinity, 4 local writers, `OMP_NUM_THREADS="8,"`, default hint 16 and `numThreads=3` produce width `max(16, 16, 3, 8, registry) = 16`.
    - In a fresh executor, helper task threads still have libgomp's 64-thread default and push with 64-thread teams. Writer 0's indices 16-63 overwrite the slots of writers 1-3, and writer 3's indices 64-111 run past the end.
    - The suite only covers an invalid first element (`StreamingLayoutSuite.scala` lines 125-126, and the bound case at line 115), so the tail case is untested.
    - This is not a regression from master, which also used 16 here. It is a gap in the new claim that the team is "derived from `OMP_NUM_THREADS`" (`Overview.md` lines 175-176).
  - **(b) Out-of-range thread values (unverified, pathological).** `Try(value.toInt)` silently drops thread values outside the 32-bit range.
    - From memory of upstream source, LightGBM's `Common::Atoi` accumulates into an `int` without overflow detection; I did not verify this against the bundled binary.
    - Native code may therefore accept a wrapped value, for example `num_threads=4294967360` becoming 64, while Scala registers nothing.
    - In that case the comment's claim that the maximum "cannot miss a larger native team" is not strictly true.
- **Risk**: Under-allocation, which causes cross-writer slot aliasing or out-of-range native writes. Case (a) needs only a plausible malformed environment value, such as a templated nested setting with an empty inner value; libgomp reports it only on stderr. Case (b) needs absurd input. CI exercises neither.
- **Suggested Fix**:
  - For (a), accept `OMP_NUM_THREADS` only when every comma-separated element is a non-empty positive base-10 integer, allowing surrounding whitespace. Otherwise return `None` so the affinity and fallback sources apply. Alternatively, take `max(first, affinity)` when affinity is known.
  - Add tests for `"8,"`, `"8,0"` and `"8,x"` with a larger affinity count.
  - For (b), treat an all-digit value that does not fit in `Int` as unknown. Reject the parameter string with a clear error, or mirror the native 32-bit wrap, and soften the comment.

### Issue 3: Allocation width has no ceiling or 32-bit overflow guard, and one oversized registered value persists for the executor JVM
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: 60-78 (monotonic global registry), 112-118, 231-236 (unbounded maximum). Related: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala` lines 134-141.
- **Description**:
  - Several inputs flow unbounded into the width:
    - every positive thread value accepted by `positiveNumThreads`, including any user-supplied `passThroughArgs` or `resetParameter` alias;
    - `numThreads`;
    - the hint;
    - `OMP_NUM_THREADS`.
  - The registry's high-water mark never decreases while the classloader lives.
  - The width and writer count reach native code as 32-bit ints. Per the driver evidence, native code allocates `writers * width` slots and indexes `writerId * width + omp_get_thread_num()`.
  - Nothing clamps the width to `OMP_THREAD_LIMIT`, even though libgomp never forms a team larger than the thread limit.
  - Worked example:
    - A long-lived session runs with `OMP_THREAD_LIMIT=32`, as in this PR's own fixture.
    - A bulk fit runs with `passThroughArgs="n_jobs=1000000"`. Native training can still run because teams are capped at 32, but the registry now holds 1,000,000.
    - Every later multi-writer streaming fit on that executor allocates width 1,000,000 until the executor restarts. With the supplied header-only model and 2048 groups × 4 writers × 24 bytes, that is about 183 GiB, so native allocation fails or the executor runs out of memory.
  - At a width of at least 2^31 divided by the writer count, the 32-bit slot-count and index arithmetic overflows. That can leave an undersized buffer or a negative index instead of a clean error. I did not re-verify the exact native integer types.
  - On memory: the supplied history measurement already allocates width 64 while `OMP_THREAD_LIMIT=16` proves every team has at most 16 threads. The extra 9 MiB of headers buys no safety.
  - Master already passed an unbounded `max(hint, numThreads)`, so the overflow surface itself predates this PR. What is new is that registry values persist across fits.
- **Risk**: The overflow needs pathological input. However, because the registry persists across fits, a single misconfiguration causes failures in every later fit until the executor restarts. Thread-limited environments also over-allocate memory with no safety benefit.
- **Suggested Fix**:
  - Accept `OMP_THREAD_LIMIT` only when it parses as a positive integer under the runtime's grammar, and then clamp the multi-writer width to it. This cannot under-allocate with libgomp.
  - Before calling native code, fail fast with a descriptive error when `externalThreads.toLong * width > Int.MaxValue`, or when the width exceeds a documented sanity ceiling.
  - Add unit cases for both.

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

## Driver dispositions, corrective cycle 4

The original review and its uncertainty labels are preserved. These notes
supersede the open placeholders. Changes are in the corrective commit containing
this section; exact local results and the product fingerprint are in
`pr-2751-attempt-9-precommit-low.md`.

| Finding | What changed and why | Verification |
|---|---|---|
| 1 | A positive configured native count or `OMP_DYNAMIC=true` selects the conservative fixed bound even for one writer. Documentation states the actual remaining auto-size condition. | Bundled-native disassembly confirms auto-sizing samples a parallel team. Pure tests exercise dynamic/static and positive/automatic branches. A separate, reproducible lazy-prediction single-writer case crashes on unchanged `0c453310`, so the correction also has public fail-before evidence without relying on varying host load. |
| 2a | Validate the entire `OMP_NUM_THREADS` list, preserving valid GNU leading-plus syntax, before using its first element. | Actual published-native probes and parser/fallback unit cases distinguish malformed tails from valid lists. |
| 2b | A direct probe confirmed the originally unverified concern. The shared numeric parser now rejects integer overflow in LightGBM thread aliases and inspected OpenMP list elements. It does not emulate native truncation. Booster creation validates before allocating its output handle. | Published lightgbmlib 3.3.510 accepted `num_threads=4294967360` and reported native maximum 64; the bundled GNU runtime also interpreted `OMP_NUM_THREADS=4294967360` as 64. The old Scala parser dropped both values. Tests reject positive/negative overflow, preserve valid signed-Int boundaries, require an unchanged registry on rejection, and reject an oversized `n_jobs` through real fitted models' reset path. |
| 3 | Guard `externalThreads.toLong * bound <= Int.MaxValue` before native allocation and reject nonpositive writer counts. Do not introduce an arbitrary memory ceiling or an unverified cross-runtime `OMP_THREAD_LIMIT` clamp. A clamp would change the contributor's retained-width policy and the measured history behavior. | Unit cases require descriptive rejection for overflowing slot counts and zero writers. Earlier measurements explicitly record the 16-versus-64 retained-width memory cost with actual teams capped at 16. Large valid requests can still consume substantial memory; the documentation retains this tradeoff rather than claiming a memory cap. |
