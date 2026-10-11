## Review Summary
- **Round**: 1
- **Theme**: Broad Sweep — correctness, security vulnerabilities, logic errors, spec conformance
- **Mode**: resolved-plan (supplied review context); sequential independent lens 1 of 6; Medium tier with stage override from target `.github/skills/synapseml-pr-loop/SKILL.md`; review attempt 8, stage attempt 2; read-only source review of frozen head `6bf3744b1516de0f9712bdcc876a274bc6e77688` against integrated master `861c3a1e14a9511b5604563ff1e3976cefa82e90`
- **Model**: claude-opus-5.5
- **Artifact**: `reviews/pr-2751/pr-2751-attempt-8-stage-attempt-2-review-1-claude-opus-5.5.md`
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope.** I read the frozen nine-path diff and the matching worktree sources:
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/DatasetAggregator.scala`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/booster/LightGBMBooster.scala`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala`
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/BaseTrainParams.scala`
  - `docs/Explore Algorithms/LightGBM/Overview.md`
  - `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingLayoutSuite.scala`
  - `lightgbm/src/test/scala/com/microsoft/azure/synapse/ml/lightgbm/split1/StreamingOmpRegressionSuite.scala`

  I also read the unchanged callers in `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMBase.scala` (439-452, 527-540) and `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/TrainingContext.scala` (52). Searches stayed within the permitted `lightgbm/`, `core/` and `docs/` scopes.
- [x] **Policy conformance in `LightGBMUtils.scala` 247-292, checked line by line.**

  | Lines | Behavior |
  | --- | --- |
  | 258 | Rejects zero writers. |
  | 259-261 | Returns `-1` only when `externalThreads == 1 && configuredNumThreads <= 0 && !dynamicThreads`, as the single-writer auto-size policy requires. |
  | 263-271 | Default team: first member of a wholly valid `OMP_NUM_THREADS` list (137-143), then Linux affinity (178-183), then a warned `max(16, OS count, JVM count)`. The affinity and OS probes are by-name, so they run only on the fixed path. The OS probe is lazy, cached and has a 5 s timeout (203-245). |
  | 272-277 | Fixed request is the maximum of the 16 floor, the hint, `numThreads`, the default team and the registry. |
  | 278-280 | A positive decimal signed-Int `OMP_THREAD_LIMIT` (145-153) caps the request, never below 16. Invalid or unsupported syntax logs a warning and never shrinks the bound. |
  | 281-282 | The overflow `require` runs before native allocation. |
  | 62-79 | The registry is monotonic (`AtomicInteger.accumulateAndGet(max)`). It has no reset and no byte budget. |

  The code conforms to the stated policy, apart from the environment-interpretation gaps in Issues 1-3.
- [x] **Each of the six registration sites runs before its parameterized native call:**
  - sampled column: `ReferenceDatasetUtils.scala` 57;
  - serialized reference: `ReferenceDatasetUtils.scala` 222, inside the `try` that frees the native byte array;
  - booster create: `LightGBMBooster.scala` 241;
  - booster reset parameter: `LightGBMBooster.scala` 322;
  - dense dataset: `DatasetAggregator.scala` 413;
  - sparse dataset: `DatasetAggregator.scala` 529.

  From the `LGBM_*` call inventory under `lightgbm/src/main`:
  - Prediction parameter strings carry no `num_threads`, so prediction restores the process default team, which the default-team term models.
  - `LightGBMBase.scala` 527-540 always appends `num_threads=<executionParams.numThreads>` to dataset parameters and never adds `passThroughArgs`. The out-of-Int `require` (`LightGBMUtils.scala` 116-121) can therefore fire only at booster create or reset, before the native call.
- [x] **Native parser consistency.** Native behavior is recalled from public LightGBM 3.x sources, not inspected in the shipped binary.
  - `parseLightGBMParams` (96-108) mirrors `Config::Str2Map`/`KV2Map`: it splits on whitespace, drops empty `=` parts, keeps the first duplicate and strips quotes.
  - `positiveNumThreads` (123-128) takes the maximum across `num_threads`, `num_thread`, `nthread`, `nthreads` and `n_jobs`. That is at least the value native alias precedence selects.
  - The divergences I found (value-less keys, Java `trim` inside tokens) only over-register, or occur just before a native parse failure.
  - `OMP_THREAD_LIMIT` parsing uses `\s`, which equals C `isspace`. Rejecting `+16` is deliberately conservative.
- [x] **Sample input/output traces**, reproduced by hand against the pure function and the `StreamingLayoutSuite.scala` expectations.

  | Lines | Input | Result | What it shows |
  | --- | --- | --- | --- |
  | 115 | `(1,16,0,"8",…)` | `-1` | Auto-size gate. |
  | 116-117 | `(1,16,32,…)` and `(1,16,2,…)` | 32 and 16 | Positive single writer. |
  | 120 | `(4,-1,2,"32,8",…)` | 32 | Ambient team wider than `numThreads`, the target defect; master gave 16. |
  | 121, 129 | `"invalid,64"` with affinity 24; `"8,"` with affinity 48 | 24 and 48 | Whole-list validity. |
  | 122 | registry 40 | 40 | History is retained. |
  | 168, 172 | `(4,16,1,"32", registry 1000000, limit "16")`; same without a limit | 16; 1000000 | Limit ceiling. |
  | 178-179 | `(1,16,32,"64", registry 64, limit "16")` | 16 | Limit applied to a single writer. |

  `StreamingOmpRegressionSuite.scala` 56-62 clears the child environment, then sets `OMP_NUM_THREADS`, `OMP_THREAD_LIMIT`, `OMP_DYNAMIC=FALSE` and `OMP_PROC_BIND=FALSE`.
- [x] **Master baseline (removed `ReferenceDatasetUtils` code).** Master returned `-1` when the hint or `numThreads` was ≤ 0, and `max(hint, numThreads)` otherwise.
  - When master used a fixed bound and no limit is set, the PR bound is never smaller, because both terms remain in the maximum.
  - Master auto-sized multi-writer runs with nonpositive `numThreads`. One example is streaming with `useSingleDatasetMode=false` and the default `numThreads=0`: `LightGBMBase.scala` 440-442 and `LightGBMParams.scala` 165 give `numThreads=0`, and `TrainingContext.scala` 52 forces a single streaming dataset. For these runs the PR replaces native measurement of the runtime's default team with a Scala-side derivation. Any disagreement between that derivation and the runtime becomes a regression in those configurations (Issues 1-3).
  - The public `maxStreamingOMPThreads` default of 16 (`LightGBMParams.scala` 181) and the `ExecutionParams` shape are unchanged. The changed `ReferenceDatasetUtils` helper and the new `LightGBMUtils` helpers are `private[lightgbm]`.
- [x] **Security.** I found no injection, SSRF, path-traversal or credential exposure surface.
  - `firstCommandOutput` (`LightGBMUtils.scala` 203-228) runs only on macOS. It uses a fixed argv (`sysctl -n hw.logicalcpu`), no shell and no user-controlled arguments. It uses a `PATH` lookup only when `/usr/sbin/sysctl` is not executable.
  - The probe reads the first line only, after the process exits. It calls `destroyForcibly` on timeout and closes its streams in `finally`.
  - `/proc/self/status` is a fixed path.
  - Raw environment values are never logged. Warnings are fixed text, and the debug log contains only parsed integers.
  - No pattern is prone to regex denial of service. No secrets appear.
- [x] **Error handling and null safety.**
  - Every `getenv` and `getProperty` result is wrapped in `Option`.
  - Affinity and OS-probe failures fall through to the next source with a warning.
  - `require` failures (out-of-Int thread counts, zero writers, slot overflow) throw before any native allocation. At the serialized-reference site, the throw happens inside the native-buffer `try`.
- [x] **Requirement-to-test mapping.**

  `StreamingLayoutSuite.scala`:

  | Lines | Coverage |
  | --- | --- |
  | 94-133 | Policy, fallbacks, four warnings, overflow and zero-writer rejection |
  | 135-151 | List, affinity and OS parsing |
  | 153-161 | Dynamic gate and lazy probes |
  | 163-183 | Limit parsing, including `"\u000116"` and `+16` |
  | 185-206 | Affinity file and the Linux-only OS probe |
  | 208-263 | Six sites, monotonic history, aliases, duplicates, out-of-Int rejection |
  | 265+ | Registration on production call paths |

  `StreamingOmpRegressionSuite.scala` runs Linux-only child JVMs with `-XX:ActiveProcessorCount=8`. It covers:
  - dense and sparse: 4 writers, width 32;
  - stacked: 1 writer, `OMP_NUM_THREADS=8`, width 16;
  - limited: limit 16, width 16;
  - `verifyHistory`, through the production environment overload.

  Dynamic detection is tested at the environment level only with `OMP_DYNAMIC=FALSE`. Other values are tested only through the `dynamicThreads` argument.
- [x] **Driver-supplied evidence considered but not relied on for findings:**
  - the published `lightgbmlib:3.3.510` hashes;
  - the GNU limit probe, which formed a team of 16 under a 1,000,000-thread request;
  - the exact-head CI build 239706730, with all jobs succeeding;
  - the benchmarks.

  This evidence supports the Linux/GNU path only. It includes no Windows or macOS native execution.
- [ ] Not run: no builds, tests, native code or network access by this reviewer. This was a read-only source inspection.
- [ ] Not verified: I recalled native LightGBM, GNU libgomp, LLVM/Intel libomp and MSVC `vcomp` behavior from public sources and did not inspect the shipped binaries. In particular, I did not verify which OpenMP runtime the Windows and macOS libraries in `lightgbmlib:3.3.510` link.
- [ ] Not established: no Windows or macOS execution and no OOM reproduction. The exact Issue2333 incident is not proven, which is consistent with the stated claim.
- [ ] Not analyzed: the unchanged validation-dataset `CreateByReference` allocation path.
- [ ] Documented limitations and tradeoffs, not counted as issues:
  - The registry is per JVM classloader, while OpenMP state is per process.
  - Taking the maximum across aliases retains ignored large values for the JVM lifetime. For example, a canonical `num_threads` together with `n_jobs=1000000` keeps 1000000. Without an enforced `OMP_THREAD_LIMIT`, later streaming fits in that JVM can then allocate very large slot arrays or fail the overflow `require`.
  - A concurrent widening after allocation, foreign native code and affinity or binding discrepancies are out of scope. Clamping the native push index remains the complete fix.

## Issues

### Issue 1: The `OMP_THREAD_LIMIT` ceiling is applied on every platform, but the smaller allocation is safe only on runtimes that enforce the limit
- **Severity**: Medium
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`; `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`; `docs/Explore Algorithms/LightGBM/Overview.md`
- **Line(s)**:
  - `LightGBMUtils.scala` 278-280 (ceiling) and 145-153 (limit parser)
  - `ReferenceDatasetUtils.scala` 161 (environment read on every platform)
  - `Overview.md` 194-200 and 210-212
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/LightGBMParams.scala` 175-176
  - `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/params/BaseTrainParams.scala` 184
  - expectations encoded in `StreamingLayoutSuite.scala` 168 and 178-179
- **Description**: Unverified platform concern. On an affected runtime it is also a regression from master.

  **What happens.** The fixed bound becomes `max(16, min(requestedBound, OMP_THREAD_LIMIT))` regardless of OS or OpenMP runtime. That is safe only if the runtime caps every team at the limit. The driver probe shows GNU libgomp does.

  **Why another runtime may not.** `OMP_THREAD_LIMIT` was introduced in OpenMP 3.0. MSVC's `/openmp` runtime (`vcomp`) implements OpenMP 2.0. Microsoft's documentation, as recalled, lists only `OMP_SCHEDULE`, `OMP_NUM_THREADS`, `OMP_DYNAMIC` and `OMP_NESTED` for it. A Windows `lib_lightgbm.dll` built that way would ignore the limit. I did not verify the shipped Windows build's runtime.

  **Why the docs don't cover it.** `Overview.md` 195-196 states without qualification that "The runtime cannot form a team wider than this limit". Lines 199-200 already argue that a value another runtime might ignore must not become a small ceiling; that is why `+16` is rejected. The same reasoning applies when a runtime ignores the variable entirely. The declared scope-out of "non-Linux fallback limits" covers weak processor-count fallbacks. It does not cover this ceiling, which actively lowers a bound that `numThreads` or `OMP_NUM_THREADS` already established.

  **Traces on a runtime that ignores the limit:**

  | Trace | Configuration | PR bound | Master bound | What the native code does | Classification |
  | --- | --- | --- | --- | --- | --- |
  | B (`StreamingLayoutSuite.scala` 178-179) | One writer, `numThreads=32`, `OMP_THREAD_LIMIT=16` | 16 | `max(16, 32) = 32` | Dataset creation calls `omp_set_num_threads(32)` on the pushing thread. The push loop then forms a team of 32, so `omp_get_thread_num()` reaches 31 against 16 slots. | Regression |
  | C | Four writers, `numThreads <= 0` (for example streaming with `useSingleDatasetMode=false`), `OMP_NUM_THREADS=32`, limit 16 | 16 | `-1`; native measured the real default team of 32 | Writer threads push with teams of 32. | Regression |
  | A (`StreamingLayoutSuite.scala` 168) | Four writers, `numThreads=1`, `OMP_NUM_THREADS=32`, limit 16 | 16 | 16 | Default-team writers index up to `3*16+31 = 79` against 64 slots. Slot ranges also overlap between writers. | Master was equally unsafe, but the PR's documented protection does not hold. |
- **Risk**: The docs present `OMP_THREAD_LIMIT` as the way to bound allocation memory (210-212). Users who follow that advice on such a runtime get the out-of-range native write the PR is meant to prevent: a crash in `SparseBin::Push` or silent heap corruption. In traces B and C, master did not have this failure. Exposure is limited to non-GNU (likely Windows) executors with the variable set.
- **Suggested Fix**:
  - Apply the ceiling only where enforcement is established: Linux/GNU, plus macOS/LLVM after verifying the shipped library. Elsewhere, pass `None` from the environment overload (`ReferenceDatasetUtils.scala` 161) and log a warning, matching the existing `+16` rationale.
  - Qualify `Overview.md` 195-196 and the parameter docs, for example "on OpenMP runtimes that enforce `OMP_THREAD_LIMIT`, such as GNU libgomp".
  - Add a unit test showing that a non-Linux OS or unknown runtime ignores the limit.

### Issue 2: Environment detection accepts fewer forms than OpenMP runtimes do (`OMP_DYNAMIC` spellings and `_ALL` host variables)
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/dataset/ReferenceDatasetUtils.scala`
- **Line(s)**: 160 (`OMP_DYNAMIC`) and 154 (reads only `OMP_NUM_THREADS`); policy text in `docs/Explore Algorithms/LightGBM/Overview.md` 181-185
- **Description**: A gap against the stated policy. It is partly a regression from master in a non-default configuration. Runtime behavior is recalled, not verified.

  **`OMP_DYNAMIC` spellings.** Dynamic detection is `Option(getenv("OMP_DYNAMIC")).exists(_.trim.equalsIgnoreCase("true"))`.
  - GNU libgomp accepts only `true` and `false`, so this matches it.
  - LLVM/Intel libomp, which the macOS library likely uses (unverified), also treats `1`, `on`, `yes`, `y`, `t`, `.true.` and `enabled` as true.
  - The repository already has a broader truthy set for native flags (`LightGBMUtils.isEnabledParameterValue`).
  - With `OMP_DYNAMIC=1` on such a runtime, one writer and `numThreads <= 0`, the code still auto-sizes (`-1`).
  - A dynamic team that is narrower when measured at initialization can grow during later pushes. That is exactly the case `Overview.md` 184-185 says keeps the fixed bound.
  - Master behaved the same here, so this part is a gap, not a regression.

  **`_ALL` host variables.** The OpenMP 5.x device-suffixed `_ALL` variables (`OMP_DYNAMIC_ALL`, `OMP_NUM_THREADS_ALL`) also set host values when the unsuffixed variable is absent. I recall GNU libgomp supporting them from GCC 13; I did not verify this. The code reads neither.
  - With `OMP_NUM_THREADS_ALL=64`, affinity 8, four writers and `numThreads=3`, the bound is 16. Writer threads that never set their own team push with the default team of 64. This is the PR's target defect reached through another spelling; master was also 16.
  - With four writers and `numThreads <= 0`, master's native auto-sizing measured 64, while the PR's fixed bound of 16 is a regression in that configuration.
- **Risk**: The allocation can be too small, causing out-of-range native writes when users configure OpenMP with spellings their runtime accepts. This requires uncommon variables or values.
- **Suggested Fix**:
  - Treat dynamic teams as enabled unless the value is absent or a recognized false spelling. Over-detection only selects the conservative fixed bound.
  - When the unsuffixed variable is unset, consult `OMP_DYNAMIC_ALL`, and `OMP_NUM_THREADS_ALL` for the default team. Otherwise, document that only the unsuffixed variables and the value `true` are recognized.
  - Add unit tests for `1`, `yes` and the `_ALL` forms.

### Issue 3: `firstOmpTeamSize` accepts control characters that GNU libgomp rejects, so it underestimates the default team
- **Severity**: Low
- **File**: `lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMUtils.scala`
- **Line(s)**: 137-143 (the `.map(_.trim)` at 139)
- **Description**: A parser inconsistency that errs in the unsafe direction. The input is contrived. It is a regression from master only in configurations where master auto-sized.

  **The mismatch.** Each list member is trimmed with Java `String.trim`, which strips every character ≤ U+0020, including U+0001-U+0008 and U+000E-U+001F. libgomp skips only C `isspace` characters (space, `\t`, `\n`, `\v`, `\f`, `\r`). For anything else it rejects the whole variable and uses the affinity CPU count.

  **Example.** `OMP_NUM_THREADS="\u00018"` gives a Scala default team of 8, while libgomp uses the affinity count, for example 64.
  - With four writers, `numThreads=3` and no history, the bound is 16 while default teams are 64. Master gave the same bound.
  - With four writers and `numThreads <= 0`, master auto-sized to 64, and the PR's fixed bound of 16 is a regression.

  **Inconsistency within the PR.** The PR's own limit parser matches `\s*[0-9]+\s*` and explicitly rejects `"\u000116"` (`StreamingLayoutSuite.scala` 173). The team parser has no equivalent check or test, although the stated policy requires the whole `OMP_NUM_THREADS` list to be valid before its first member is used.
- **Risk**: The default team can be underestimated, which can re-enable out-of-range pushes. Reaching it requires control characters in the environment variable.
- **Suggested Fix**:
  - Before parsing, validate each member with `\s*[+]?[0-9]+\s*` (Java `\s` equals C `isspace`), or strip only `[ \t\n\x0B\f\r]`.
  - Add `"\u00018"` and `"8\u0001"` to the test of rejected lists.

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

## Driver triage after round 1

The original review above is preserved. These dispositions supersede its
pending resolution entries, not its original findings or uncertainty.
No product, test, or documentation changes were made during this triage.
The reviewed head remains `6bf3744b1516de0f9712bdcc876a274bc6e77688`.

### Issue 1: confirmed, unresolved merge blocker

The published `lightgbmlib:3.3.510` JAR contains a Windows x86-64
`lib_lightgbm.dll` importing `VCOMP140.DLL`, including `_vcomp_fork`,
`omp_set_num_threads`, and the team-size/thread-ID query functions. This is
binary evidence, not an assumption about how the Windows package was built.

A fresh Windows subprocess loaded that exact DLL and its linked runtime.
With a requested team of 32, the runtime executed 32 callbacks with thread IDs
0 through 31, both without `OMP_THREAD_LIMIT` and with `OMP_THREAD_LIMIT=16`.
Dynamic teams were disabled. Each subprocess had a 30-second deadline.
The probe only observed a parallel team; it did not push training rows or
deliberately perform out-of-bounds writes.

The current Scala allocator returns 16 for one writer, `numThreads=32`, and
limit 16. Master returned 32. The measured native team therefore exceeds the
new allocation in the review's trace B. The ceiling in maintainer commit
`6bf3744b` introduces this Windows sizing regression. It must not narrow the
allocation where the runtime does not enforce the limit.

The Windows DLL SHA-256 is
`78abb9649c730dbb13b62f6f2d5a02bc64c29e00de55b0ce58cab0e884ae7db8`.
The containing JAR SHA-256 is
`f2b1b13172699832594303ab4c04f3bc8fc2d24737e3e8c11d98d69a88c09272`.
[Microsoft's environment-variable documentation](https://learn.microsoft.com/en-us/cpp/parallel/openmp/reference/openmp-environment-variables?view=msvc-170)
lists the four supported MSVC variables and does not list `OMP_THREAD_LIMIT`.
The runtime execution, rather than that omission alone, establishes the finding.
The macOS runtime was not executed.

### Issue 2: `_ALL` behavior confirmed; other-runtime spellings unverified

Fresh Linux subprocesses loaded the published Linux library and its linked GNU
OpenMP runtime. `OMP_NUM_THREADS_ALL=64` without `OMP_NUM_THREADS` produced a
real 64-thread team, with 64 callbacks and thread IDs 0 through 63. This machine
had 16 affinity CPUs. Adding `OMP_NUM_THREADS=8` selected an 8-thread team,
confirming unsuffixed-variable precedence for valid values.

`ReferenceDatasetUtils.scala` reads only `OMP_NUM_THREADS`. With four writers,
`numThreads=0`, hint 16, no retained positive request and no limit, its new fixed
allocation is 16, while master auto-sized to the native team. The production
default-team derivation therefore misses a supported 64-thread setting. This
is an unresolved sizing regression, not merely a documentation gap.

`OMP_DYNAMIC_ALL=true` also enabled dynamic teams in the actual linked GNU
runtime, although the current Scala environment reader treats them as disabled.
The probe confirmed that state discrepancy; it did not reproduce a team growing
between dataset initialization and a later push.

GNU rejected `OMP_DYNAMIC=1` and `OMP_DYNAMIC=yes`, as the reviewer expected.
Those values are not confirmed bugs for this tested runtime. LLVM/Intel
spellings remain unverified and are not promoted to proven failures.

### Issue 3: parser mismatch confirmed, high-CPU failure remains conditional

The actual linked GNU runtime rejected `OMP_NUM_THREADS="\u00018"`,
`"8\u0001"`, and `"8,\u00012"`, reported an invalid environment value and
selected the 16-CPU affinity default. It accepted `" \t8\r "` and selected 8.
The current Scala `trim` and integer parsing accept the rejected examples.

This confirms an inconsistency with the whole-list validation contract.
On this 16-CPU machine the allocation floor masks underallocation, so no
greater-than-16-CPU execution or crash is claimed. The review's higher-CPU
underallocation trace remains conditional. The parser should reject those
control characters before choosing a smaller default-team estimate.

### Probe evidence and review outcome

Task-local driver commands and results:

| Command | Result |
| --- | --- |
| `python3 pr2751-final2-environment-probe.py` | Passed 11 isolated native environment-query cases. Invalid input diagnostics were retained as evidence, not ignored. |
| `python3 pr2751-final2-team-probe.py` on Linux | Passed both actual-team probes, 64 threads for `_ALL` alone and 8 with the unsuffixed override. |
| `python pr2751-final2-team-probe.py` on Windows | Passed both actual-team probes, 32 threads with and without the purported 16-thread ceiling. |

Linux probes used fresh cleared environments, a 2 GiB address-space limit,
disabled core files and per-child deadlines. Windows probes used a small
explicit environment, disabled error dialogs and per-child deadlines. No
dataset, credentials, confidential log, or private incident evidence was used.
These are native/runtime probes plus a production-source sizing trace, not new
public Spark crash regressions or a full Windows Spark validation.

The second final pass stops after round 1 because confirmed findings require
reviewed-content changes. Rounds 2 through 6 were not dispatched; this is not
a clean six-round pass. Five automatic fix cycles and the user-initiated CI
retry have been consumed, as have both final-pass attempts. Further source
correction and another complete final pass require an explicit budget extension.

Azure build `239706730` has meanwhile passed all 65 jobs after the user's
targeted retry. That green result belongs to this unchanged head and does not
cover these newly identified configurations. GitHub now records an approval
by `BoraElkin` on this head, but approval does not resolve these engineering
findings. No response from the contributor to the sign-off request was present
at this triage check.

## Corrective follow-up after explicit continuation

The user subsequently instructed the driver to continue. The driver recorded
a bounded extension for this correction, validation, one replacement complete
final pass and its artifact publication, rather than resetting earlier usage.

All three findings now have local corrections in the pending nine-path
manifest `904169e8fcc396ab41db2b4b0bd4f0386beb140635b174f78ccd256ee9955059`.
They remain **pending publication**, not remotely resolved:

| Finding | Correction | Local proof |
| --- | --- | --- |
| Windows ceiling | The production environment adapter accepts a ceiling only on Linux; other platforms warn and retain the larger bound. | Isolated Windows-policy child failed at width 16 before and passes at width 32 after. The earlier actual MSVC 32-thread callback probe remains the native enforcement evidence. |
| `_ALL` settings | Team fallback considers `_ALL` without narrowing host fallback on older runtimes. Dynamic detection covers `_ALL` and selects fixed allocation for unknown spellings. | Missing and invalid-primary `_ALL` cases failed at width 16 before; dynamic `_ALL` failed at `-1`. All now pass width 32 assertions and real streaming/bulk prediction parity. |
| Control-character validation | Validate each entire list member against standard whitespace and decimal syntax before trimming. | The prior parser accepted `Some(8)` for a control-prefixed input; corrected rejection tests pass. Larger-host crash execution is still not claimed. |

The exact corrected source passes JDK 11 main/test compilation, both styles,
all 24 targeted tests, codegen, three generated-wrapper contract checks and
pinned Black. The sixth mandatory Low precommit review returned no significant
issues; its original response and commands are preserved in
`pr-2751-attempt-11-precommit-low.md`. Final-source measurements, clean Java 8
confirmation, publication and new-head remote/final-review gates are separate
requirements and were not complete at this local correction checkpoint.

## Completed local evidence before publication

The source-matched primary 32-fit and reversed-order 16-fit samples are now
complete. Both passed parity, allocation and runtime-hash checks. First-fit
time and fresh peak RSS increased in both samples; the warm timing direction
reversed. The tables and limitations remain separate in attempt 11.

Java 8 main/test compilation and both styles passed. Its unpacked-classpath
test run hit two five-minute child deadlines and was stopped. A countercheck
packaged the same compiled bytes into verified JARs and ran all three suites
through ScalaTest, retaining the original child deadlines and assertions.
All 24 tests passed with zero failures, skips or aborted suites at
`2026-10-09T09:12:49Z`. This is not a claim that the stopped SBT run passed.
Exact commands, provenance and limitations are appended to attempt 11.

All three findings are locally corrected on manifest `904169e8fcc396ab41db2b4b0bd4f0386beb140635b174f78ccd256ee9955059`.
Publication, exact-new-head remote checks and a complete replacement final
pass remain required. The original second-pass verdict is not rewritten.
