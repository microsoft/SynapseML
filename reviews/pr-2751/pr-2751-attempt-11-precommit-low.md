# Mandatory precommit review

Published parent: `6bf3744b1516de0f9712bdcc876a274bc6e77688`.
Integrated target: `861c3a1e14a9511b5604563ff1e3976cefa82e90`.
Pending product manifest SHA-256:
`904169e8fcc396ab41db2b4b0bd4f0386beb140635b174f78ccd256ee9955059`.
Complete nine-path diff SHA-256:
`0fdb7859b7ad5b1ce9e815fdc2e73362863550cf3a6ab9f278c581b2c0a08b7f`.

## Original harness response

No significant issues found in the reviewed changes.

**Reviewer model:** `gpt-6-astra`. This Low review covered the complete supplied nine-path merge-base-to-index diff, including the staged corrections.

**Evidence limitations:** Read-only static review; driver-reported validation was not independently reproduced. No tests, builds, native probes, or network operations were run. The simulated-Windows case does not establish Windows Spark behavior, and this review does not establish safety beyond the documented mitigation boundaries.

This is not a merge-readiness verdict. Java 8 confirmation, the final-source benchmark, publication and new-head remote checks, and the six final independent lenses remain pending.

## Driver decision and correction scope

The driver estimated Low effort for this mandatory corrective precommit stage.
The harness selected `gpt-6-astra`; no model override was supplied. This does
not replace the requested six independent final lenses after current-head CI.
The review read the complete effective nine-path patch, not just the seven
files changed in this correction. Earlier review reports were excluded from
the supplied diff and prohibited from reviewer searches.

This correction preserves the contributor's multi-writer allocation and
thread-history mechanism, public defaults and serialized parameter shapes.
It addresses the independently verified findings preserved in
`pr-2751-attempt-8-stage-attempt-2-review-1-claude-opus-5.5.md`:

- `ReferenceDatasetUtils` uses `OMP_THREAD_LIMIT` as an allocation ceiling only
  on Linux. Other platforms warn and do not narrow the bound. The actual
  published Windows DLL's linked MSVC runtime formed 32 threads despite a
  limit of 16, so the old unconditional ceiling was unsafe.
- `OMP_NUM_THREADS_ALL` can raise the fallback when the unsuffixed variable
  is absent or invalid. It cannot reduce the affinity or processor fallback,
  because older runtimes may ignore `_ALL`. A valid unsuffixed list retains
  precedence.
- Dynamic-team detection considers `OMP_DYNAMIC_ALL` when the unsuffixed
  variable is absent. Unknown spellings warn and select fixed allocation
  conservatively. Only a recognized `false` value disables that selection.
- OpenMP team-list validation checks standard whitespace before trimming,
  rejecting control characters that the native parser rejects.

An additional native precedence probe confirmed that GNU uses a valid
`OMP_NUM_THREADS_ALL=64` when the unsuffixed value is empty, invalid, zero,
malformed or control-prefixed. A valid unsuffixed value of 8 overrides it.
Invalid dynamic spellings can similarly fall back to `OMP_DYNAMIC_ALL=true`;
therefore treating `0` or `off` as universally false would be unsafe.

## Fail-before evidence

With production source unchanged from `6bf3744b`, Java 8 compiled the new
tests and ran this selection:

```text
lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite -- -z "all ingestion" -z "all-invalid ingestion" -z "windows-limit ingestion" -z "helpers parse environment"
```

Result: all five selected tests failed at their intended assertions, with
zero aborted, canceled, ignored or pending tests. The two `_ALL` cases
observed width 16 instead of 32. Dynamic `_ALL` observed native auto-sizing
`-1` instead of fixed width 32. The isolated Windows-policy child observed
width 16 instead of 32. The control-character parser returned `Some(8)`
instead of rejecting the value.

These assertions stop before unsafe native writes. They are not new Spark
crash reproductions. After correction, the three new Linux environment
scenarios continue through real public streaming and bulk fits and compare
evaluated predictions. The Windows-policy child changes only its own
`os.name` temporarily and restores it; actual MSVC enforcement behavior was
established by the separate native callback probe, not simulated Spark.

An initial shell attempt failed on CRLF formatting before tests started.
Its output was retained separately and is not fail-before evidence.

## Exact local validation

The repository SBT wrapper selected JDK 11 explicitly. It ran:

```bash
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" --jdk /usr/lib/jvm/java-11-openjdk-amd64 -- "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingDatasetLifecycleSuite" "lightgbm/codegen"
```

Result: main and test compilation passed, both styles passed, and all
**24 tests passed**, with zero failures, cancellations, ignored, pending or
aborted tests. Tests completed at `2026-10-09T07:57:36Z`; codegen completed
at `2026-10-09T07:59:56Z`.

The 24 tests comprise seven public Spark regression scenarios, one isolated
Windows allocation-policy child, 15 layout/helper tests and one public
lifecycle test. The parent checks actual allocation log widths for the public
scenarios. All public regression scenarios require finite, nonconstant
predictions and per-row streaming/bulk parity.

The initial JDK 11 attempt passed compilation and main style but failed test
style because the expanded runner exceeded the complexity limit. Extracting
environment verification fixed the failure without suppressing the rule.
The successful rerun above covers the final reviewed source.

```bash
python3 -m black --check --extend-exclude 'docs/' .
```

Result: pinned Black `22.3.0` passed, 208 files unchanged.
The driver also parsed all three regenerated estimator wrappers and checked
their getter/setter methods, unchanged default 16, `_ALL` documentation and
Linux-only ceiling documentation. All checks passed. These are generated
wrapper contract checks, not Python Spark end-to-end execution.

## Remaining validation at precommit review

Clean Java 8 confirmation and final-source paired timing/memory measurements
were still pending when the reviewer returned its original response.
Publication, current-head Azure CI, current-head automated review and a
complete new six-round final pass also remained pending. Later evidence is
appended below without rewriting the original review or these historical facts.

## Final-source timing and memory

Both new samples used the reviewed product manifest above. The primary sample
ran 32 fits in eight JVMs, two JVMs per revision and scenario, with four fits
per JVM. Fresh runs used ABBA order; history runs used BAAB. Every fit passed
per-row bulk parity and the four-writer, width-16 allocation assertion.

The baseline substitutes the previously archived target-production LightGBM
main JAR. All other runtime and test JARs, including the compiled benchmark
fixture, were held identical. The corrected main JAR was snapshotted from this
validated source. This is not a separately rebuilt full target checkout.
All 205 runtime artifact hashes were checked before and after each sample.

The workload has 65,536 rows and 2,048 sparse features, four Spark tasks, three
iterations, three leaves, `numThreads=2`, hint 16, a 3 GiB heap, eight active JVM
processors, and `OMP_NUM_THREADS=OMP_THREAD_LIMIT=16`. Dynamic teams and binding
are disabled. History runs first request 64 threads through a bulk fit, with
actual native teams capped at 16. Each measured streaming fit includes training
but not the later prediction/parity action.

Warm time is the median of the three warm fits within each JVM, then the median
across the two JVMs. Peak RSS is the median of each JVM's maximum sampled fit
RSS, with 10 ms sampling. It is whole-process memory, not isolated native
buffer memory.

| Primary sample | Metric | Baseline | Corrected | Change |
| --- | --- | ---: | ---: | ---: |
| Fresh | First fit, seconds | 8.260 | 9.302 | +12.6% |
| Fresh | Warm fit, seconds | 3.912 | 5.517 | +41.0% |
| Fresh | Peak RSS, MiB | 2655.0 | 2731.6 | +2.9% |
| History | First fit, seconds | 9.738 | 11.011 | +13.1% |
| History | Warm fit, seconds | 5.327 | 5.084 | -4.6% |
| History | Peak RSS, MiB | 2747.6 | 2760.2 | +0.5% |

Fresh per-JVM warm medians ranged from 3.558 to 4.266 seconds for the baseline
and 5.178 to 5.856 seconds for the correction. History ranges were 4.175 to
6.479 and 3.876 to 6.292 seconds, respectively.

The fresh warm increase prompted a separate reversed-order countercheck, not
its dismissal. This ran 16 more fits in four JVMs using fresh BAAB order and
matched GC logging on both revisions. All parity, allocation and runtime-hash
checks passed again.

| Fresh countercheck | Baseline | Corrected | Change |
| --- | ---: | ---: | ---: |
| First fit, seconds | 7.787 | 10.555 | +35.6% |
| Warm fit, seconds | 3.888 | 3.772 | -3.0% |
| Peak RSS, MiB | 2618.6 | 2682.7 | +2.4% |

Countercheck per-JVM warm medians ranged from 3.503 to 4.274 seconds for the
baseline and 3.370 to 4.175 seconds for the correction. Whole-JVM GC pause
totals were 630/773 ms for the baseline and 909/733 ms for the correction.
Those totals include setup and prediction outside the timed fits and do not
establish a cause for the measured differences.

The warm slowdown reversed, so these small samples do not establish a
consistent warm-fit regression or speedup. **First-fit time and fresh peak RSS
increased in both samples.** Those observations remain visible; this is not a
claim of broad performance non-regression or proof that all differences are
noise. The samples are reported separately and are not pooled with each other
or with earlier measurements.

Both revisions allocated width 16 in these measurements. Without an enforced
limit, larger retained requests still have real memory and traversal costs.
For 2,048 sparse-bin groups and four writers, width 64 rather than 16 adds
about 9 MiB of empty-vector headers at 24 bytes each, before row payload and
allocator overhead. Ignoring an unenforced Windows ceiling deliberately
retains that safety cost instead of allowing an undersized native array.

## Java 8 confirmation after the review

The exact frozen product was rebuilt using CI-matching Temurin `1.8.0_504`:

```bash
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" --jdk "$JAVA8_HOME" -- "core/clean" "lightgbm/clean" "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingDatasetLifecycleSuite"
```

Main/test compilation and both styles passed. The dense and sparse children
passed in 272.594 and 286.615 seconds. Stacked and limited-history children
hit the unchanged five-minute process deadline. The driver then stopped this
run, exit 143, rather than spending the remaining time on the same setup.
This is **not a passing SBT test run**. Its failure output remains separate.

The driver packaged the freshly compiled core/LightGBM main and test class
directories into four JAR snapshots without recompiling or changing a byte.
Every JAR entry was compared with its class-directory hash, every compiled
class had Java class version at most 52, and all 204 runtime artifact hashes
were verified before and after execution. The recorded dependency classpath
was unchanged apart from replacing those four class directories with JARs.
An initial preparation attempt detected a core main/test ordering mismatch
and stopped before executing tests; correcting the mapping did not change
product code or compiled classes.

```text
python3 pr2751-cycle7-java8-packed.py
```

Result: the evidence driver exited 0. Its Java 8 invocation ran
`org.scalatest.tools.Runner` with all three suites listed above, a 5 GiB
parent heap, eight active processors and the verified runtime classpath.
All **24 tests passed**, three suites completed and zero tests failed,
aborted, canceled, ignored or remained pending. Execution finished at
`2026-10-09T09:12:49Z`, in 10 minutes 40 seconds.

The unchanged public child tests still clear inherited environment variables,
disable core files and enforce the five-minute deadline. Stacked passed in
36.730 seconds and limited history in 175.297 seconds. This confirms Java 8
behavior with the same compiled source and original assertions. It does not
establish the precise cause of the slower unpacked-classpath run, nor replace
the still-required new-head Azure SBT run.

The product manifest remains `904169e8fcc396ab41db2b4b0bd4f0386beb140635b174f78ccd256ee9955059`
and the diff remains `0fdb7859b7ad5b1ce9e815fdc2e73362863550cf3a6ab9f278c581b2c0a08b7f`.
No timeout, test assertion, production source or dependency was changed for
this confirmation.
