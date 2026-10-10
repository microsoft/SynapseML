No significant issues found in the reviewed changes.

**Effective decision: Low.** This is the explicitly requested bounded fifth follow-up for microsoft/SynapseML#2751: seven staged corrective paths within the existing design, reviewed in the complete nine-file product diff with moderate concurrency context. Reviewer model exposed by this runtime: `gpt-6-astra`. No agents or model override were used.

**Frozen identity:** Confirmed HEAD `7ca3f932f51dabaa90b8d60734e4c93aa59f413d` and merge base `861c3a1e14a9511b5604563ff1e3976cefa82e90`. The matching worktree product diff was byte-for-byte identical to the supplied frozen diff, with SHA-256 `8e7d0fa9b9998f1224d5e3c6140b1a90f4b87ae8c68a4500dc2abc9e0a45580b`. The frozen identity records manifest SHA-256 `859d750f97b6629fa234db149311b8fc58bd8e9e4ca70fe34f1f88e023f12c97`; that manifest digest was not independently reconstructed.

**Limitations:** Review was read-only, covering source and relevant callers. No builds, tests, native execution, network access, or mutations were performed. Reported validation and native-team measurements remain supplied evidence. Java 8 has not been rerun for this correction; the older-head Azure run does not establish current-source coverage. The pending benchmark and earlier separate measurements support no causal performance conclusion.

This result does not establish safety against foreign native code, concurrent team widening, affinity/platform limitations, or every allocation-related OOM. It is neither the final six-lens review nor merge approval.

## Driver decision and validation

The original response above is preserved verbatim. Its phrase "explicitly
requested" refers to the driver handoff, not a human-selected tier. The primary
agent estimated Low for this localized correction. The user requested the final
six-round review, which remains a separate gate. This is the fifth Low pass;
the harness selected the reviewer without an override.

### Current-head findings and correction

Copilot review `5463884482` at `7ca3f932` reported two findings:

| Finding | Correction and rationale | Evidence |
| --- | --- | --- |
| [Retained oversized thread requests](https://github.com/microsoft/SynapseML/pull/2751#discussion_r4225009674) | A positive decimal `OMP_THREAD_LIMIT` within signed-Int range limits fixed allocation widths above the existing 16-slot floor. Keep the original history instead of clearing it or imposing an arbitrary memory budget that could under-allocate. Unsupported syntax never becomes a ceiling and emits a warning. | Published native runtime and a real public bulk-fit history case, followed by repeated streaming/bulk parity, as detailed below. Unit cases cover limits, floor, overflow, malformed inputs, positive single-writer and dynamic-team paths. |
| [Incomplete Scaladoc sentence](https://github.com/microsoft/SynapseML/pull/2751#discussion_r4225009773) | Correct the sentence and keep overview, parameter text and scaladoc consistent with the native limit. | Main/test compilation, styles and generated-wrapper documentation checks pass. |

The first-pass round-4 disposition declined an unverified limit clamp. Later
native evidence justified the narrower correction here. That earlier judgment
and the original review are preserved rather than rewritten.

### Native and public-path proof

The published Linux native library SHA256 remains
`c91e863e914ee107814bad0982f33980f5cabdcfe6c12395ccf626a80578e104`.
With `OMP_THREAD_LIMIT=16`, the linked GNU runtime reported a requested maximum
of 1,000,000 but formed an actual parallel team of 16, observed through all 16
callbacks. GNU also accepts a leading plus, but the allocation parser deliberately
does not use that spelling as a ceiling because other runtimes can reject it.
Malformed lists, zero and negative limits do not restrict the native runtime.
This agrees with the [GNU runtime contract](https://gcc.gnu.org/onlinedocs/libgomp/OMP_005fTHREAD_005fLIMIT.html).
It is not a native macOS or Windows execution claim.

The new Linux child case first fits a real bulk estimator with
`num_threads=1 n_jobs=1000000`. Native output confirms the canonical value wins
and the alias is ignored. SynapseML's conservative registry retains 1,000,000.
On unchanged `7ca3f932` production, the next expected-width-16 assertion fails
before attempting an unsafe allocation. No 183 GiB allocation or OOM is induced.
With the correction, that assertion passes and two subsequent public streaming
fits both match every bulk prediction. The parent verifies four writers and
actual logged allocation width 16.

An earlier discarded fixture reset a loaded model and crashed in
`GBDT::ResetConfig`. That was the wrong failure boundary, not evidence for this
allocation finding. The final fixture avoids that path. The historical
single-writer `SparseBin::Push` reproduction remains separate evidence.

### Exact local validation

Using the repository's selected JDK 11:

```text
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" -- "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingDatasetLifecycleSuite" "lightgbm/codegen"
```

Result: main/test compilation, both styles, all 17 tests and code generation
passed. No tests failed, canceled, skipped or were ignored. Tests completed at
`2026-10-08T23:46:09Z`; code generation completed at `23:47:37Z`.
The four child cases cover dense, sparse, lazy stacked and limited-history
ingestion. An earlier run stopped at test-method complexity; simplifying a
redundant matrix-selection branch fixed it without disabling a rule.

Read-only AST checks passed for generated classifier, regressor and ranker
wrappers, their getter/setter methods, unchanged default 16 and limit
documentation. These are wrapper contract checks, not Python Spark E2E.

```text
python3 -m black --check --extend-exclude 'docs/' .
```

Result: passed with pinned Black 22.3.0, all 208 files unchanged.

This latest source still needs its own published-head CI and automated review.
The independent final six-round pass remains outstanding.

### Final-source counterbalanced measurements

The limit-aware correction completed a separate 32-fit sample across eight JVMs.
All fits asserted four writers and width 16, including the historical-request
case. Every fit passed per-row bulk prediction parity, and all 205 frozen runtime
hashes remained unchanged. Earlier 112 measurements remain separate evidence for
their revisions; none are pooled with this sample.

As in attempt 9, the baseline substitutes the archived target-production
LightGBM main JAR into identical frozen common/test runtime artifacts. It is not
a complete independently rebuilt target checkout. The workload has 65,536 rows,
2,048 sparse features, four tasks, three boosting iterations, three leaves, two
training threads, hint 16 and a 3 GiB heap. Both `OMP_NUM_THREADS` and
`OMP_THREAD_LIMIT` are 16; dynamic teams and binding are disabled. The historical
case first requests 64 threads through a bulk fit.

Two JVMs per revision/scenario run four measured fits each. Fresh order is
baseline, corrected, corrected, baseline; history reverses it. First-fit values
are medians across JVMs. Warm values are medians of each JVM's three warm-fit
medians. Peak RSS is the median of per-JVM fit maxima, sampled every 10 ms.

| Scenario | Metric | Target production | Final correction | Change |
| --- | --- | ---: | ---: | ---: |
| Fresh | First fit, seconds | 7.865 | 8.564 | +8.9% |
| Fresh | Warm fit, seconds | 3.221 | 3.667 | +13.9% |
| Fresh | Peak process RSS, MiB | 2642.5 | 2601.2 | -1.6% |
| History | First fit, seconds | 7.382 | 9.039 | +22.4% |
| History | Warm fit, seconds | 3.706 | 3.632 | -2.0% |
| History | Peak process RSS, MiB | 2744.5 | 2753.2 | +0.3% |

Fresh warm per-JVM medians range from 3.102 to 3.339 seconds for baseline and
3.082 to 4.253 seconds for the correction. History ranges are 3.177 to 4.236 and
3.553 to 3.712 seconds. The measured fresh increase is visible, not omitted.
Two JVMs per case, overlapping ranges and prior direction reversals do not
establish an attributable slowdown or speedup, nor broad performance
non-regression. Whole-process RSS does not isolate native allocation.

The new limit eliminates the previous width-64 allocation in this thread-limited
history case. Both revisions now use width 16, about 3 MiB of vector headers
under the 24-byte-header estimate. The earlier 9 MiB header increment is no
longer incurred here. Without a usable runtime limit, retaining larger widths
still has the documented memory and traversal cost.

### Earlier published-head CI

[Azure build 239693412](https://dev.azure.com/msdata/A365/_build/results?buildId=239693412)
tested `7ca3f932`, not this pending correction. It finished with 64 successful
jobs and one failed causal job. All 418 LightGBM tests passed, including the
three public child regressions, 11 layout cases and lifecycle test. One existing
`Performance testing` utility was ignored.

The causal job reached both existing 30-minute SBT process deadlines. Its first
command started at `23:04:54Z` and Spark invoked its JVM shutdown hook at
`23:34:58Z`. The retry invoked the same hook at `00:05:12Z`. The three published
`VerifyOrthoDMLEstimator` failures report a canceled job or a stopped SparkContext.
The existing runner uses `timeout 30m` with one flaky-suite retry at
`pipeline.yaml:978-979`. The log explicitly reports no LightGBM tests in this
job; these causal tests retain the Spark `GBTRegressor` defaults.

This identifies the termination boundary, not why the causal workload exceeded
its budget. It is not proof of an underlying infrastructure fault or a new
LightGBM slowdown. No unrelated causal code or CI policy was changed. The full
build remains failed; the next published correction needs its own complete run.

### Final-source Java 8 validation

The same frozen product passed a clean rebuild with CI-matching Temurin
`1.8.0_504`. The selected JDK was passed explicitly, not taken from the machine
default.

```text
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" --jdk "$JAVA8_HOME" -- "core/clean" "lightgbm/clean" "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingDatasetLifecycleSuite"
```

Result: compilation, both styles and all 17 tests passed at
`2026-10-09T00:27:58Z`. All three suites completed, with zero failed, canceled,
ignored, pending or aborted tests. This covers all four public child cases,
including the new limited-history regression, on the exact pending correction.
Product manifest `859d750f97b6629fa234db149311b8fc58bd8e9e4ca70fe34f1f88e023f12c97`
and diff `8e7d0fa9b9998f1224d5e3c6140b1a90f4b87ae8c68a4500dc2abc9e0a45580b`
remain the validation identity.
