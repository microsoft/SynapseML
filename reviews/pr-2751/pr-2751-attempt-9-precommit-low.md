No significant issues found in the reviewed changes.

**Outcome: clean for this bounded Low precommit review of microsoft/SynapseML#2751.** This is not final CI-qualified approval and does not replace the requested six-round review.

## Effective review decision

| Field | Decision |
|---|---|
| Tier and source | Low, from the primary-agent estimate for the corrective follow-up, not a human-selected tier |
| Change size | Eight staged corrective source/test/docs paths, reviewed within the complete nine-file product patch |
| Complexity | Localized allocation branching, integer/list validation, subprocess lifecycle handling, and fixture assertions within the existing native-registration/concurrency design |
| Rationale | One incremental direct-contract review; no separate lenses or agents |
| Primary model | `gpt-6-astra` |
| Actual reviewer model | `gpt-6-astra`, exposed by the runtime |
| Model selection | Harness-owned; no override |

## Reviewed identity

- HEAD: `0c453310a47ee8f1ce34befd6fc7f99badd540ca`
- Target and merge base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`
- Frozen diff SHA256, independently checked: `4769d283c96bb79a8b2d7fa8cc7eec22914aff5d0b6295caf7bd4028c130a0a2`
- Normalized product manifest SHA256, recorded in the supplied identity: `6996edad4da720d21e67b60dc920e79da18b7fb454be63f450b69fa5da055119`

All nine working-tree product files matched the resulting blob identities in the frozen diff.

## Evidence limits

This was a read-only static review. I did not execute builds, tests, or native code, contact remote services, or modify files. The supplied runtime, compatibility, code-generation, and formatting results remain driver-reported evidence, not independently reproduced results.

The allocation changes remain a mitigation, not a native index clamp. Foreign native changes, concurrent team-width increases after allocation, and documented affinity/platform limitations remain outside its guarantee.

Historical high-water allocation retains its documented memory and traversal cost. The conflicting timing samples do not establish attributable performance improvement or non-regression; the pending final sample has no reviewed outcome.

Published-head CI does not validate the staged corrections. Current corrective-head remote validation and the final six-round review remain outstanding.

## Driver validation record

The original reviewer response above is preserved verbatim. This section is
driver evidence, not independent testing by the reviewer. This is the fourth
bounded Low precommit pass. The first final six-round pass remains
`ISSUES_FOUND`; its six original reports and individual dispositions are retained
alongside this file. One complete final pass remains after current-head CI.

### Reproduced defects

The published native dependency is `lightgbmlib:3.3.510`, not a custom build.
Its JAR SHA256 is
`f2b1b13172699832594303ab4c04f3bc8fc2d24737e3e8c11d98d69a88c09272`;
the Linux native SHA256 is
`c91e863e914ee107814bad0982f33980f5cabdcfe6c12395ccf626a80578e104`.

A new synthetic public-estimator stacked-model case against unchanged
`0c453310` production used one writer, `numThreads=2`, and
`OMP_NUM_THREADS=8`. Lazy upstream LightGBM prediction widened the same task
thread after initialization. The child logged `allocationBound=-1` and exited
134 with SIGSEGV in `SparseBin<unsigned char>::Push+0x33`. The corrected case
requires the actual bound 16, evaluated finite and nonconstant predictions,
and per-row bulk parity. It passes on both tested Java generations.

Direct native probes separately verified prediction's default-team reset,
whole-list rejection of malformed OpenMP settings, and integer truncation.
Both `num_threads=4294967360` and `OMP_NUM_THREADS=4294967360` produced native
maximum 64 while the old Scala parser discarded those values. The correction
rejects numeric overflow instead of reproducing platform-dependent wrapping.
Tests cover signed-Int boundaries, no registry mutation on rejection, and
oversized aliases through real fitted models' reset method.

The fresh-JVM dense and sparse cases now also assert four actual writers and
allocation width 32. Neither depends solely on undefined behavior causing a
crash. Earlier historical evidence is unchanged: the target-source dense replay
crashed, while the original sparse replay passed.

The shell sets zero Linux core limits and the child checks both soft and hard
limits before Spark work. The actual Java 8 native fail-before reported core
dumps disabled. An accepted Java 8 minidump flag was not proof of Linux
suppression; the older evidence file now carries that correction. No guarantee
is made about arbitrary host-configured piped core handlers.

### Local commands and outcomes

The repository SBT wrapper ran with an explicit repository and selected JDK.
`JAVA8_HOME` below denotes an isolated official Temurin `1.8.0_504` installation.
It is not a dependency-pin or global toolchain change.

```text
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" --jdk "$JAVA8_HOME" -- "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingDatasetLifecycleSuite"
```

Result: compile, test compile, both styles and 15 tests passed, with zero failed,
canceled, ignored or pending tests, at `2026-10-08T21:52:05Z`. This run proves
the public stacked pass-after and precedes the subsequent integer-overflow guard.
Two earlier attempts stopped at style checks, first an unnamed timeout constant,
then the enlarged probe method's length/complexity. Both were corrected without
disabling style rules. Those attempts are not counted as test passes.

After adding integer-range rejection and real model-reset assertions, the exact
latest frozen product passed this Java 8 command:

```text
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" --jdk "$JAVA8_HOME" -- "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite"
```

Result: every compile/style task and all 11 layout tests passed, with no skips
or cancellations, at `2026-10-08T22:03:17Z`.

The exact latest product also passed the full targeted checks and code generation
through the default repository JDK 11 wrapper:

```text
bash .github/skills/synapseml-local-setup/scripts/synapseml-sbt.sh --repo "$PWD" -- "lightgbm/compile" "lightgbm/Test/compile" "lightgbm/scalastyle" "lightgbm/Test/scalastyle" "lightgbm/testOnly com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingOmpRegressionSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingLayoutSuite com.microsoft.azure.synapse.ml.lightgbm.split1.StreamingDatasetLifecycleSuite" "lightgbm/codegen"
```

Result: main/test compilation, both styles, all 15 tests and module code
generation passed. Tests completed at `2026-10-08T22:30:39Z`; code generation
completed at `2026-10-08T22:32:25Z`. No tests failed, skipped or were canceled.

Read-only Python AST checks passed for generated `LightGBMClassifier`,
`LightGBMRegressor` and `LightGBMRanker`: syntax, getter/setter methods, default
`maxStreamingOMPThreads=16`, and updated fixed-bound/dynamic-team documentation.
No generated files were manually edited. These are generated-wrapper contract
checks, not a separate Python Spark integration run.

```text
python3 -m black --check --extend-exclude 'docs/' .
```

Result: passed with pinned Black `22.3.0`; all 208 files would remain unchanged.

The complete nine-path merge-base-to-index diff was applied to a disposable
index and its path/mode/blob manifest matched the frozen product. The fingerprint
was rechecked after validation. Raw logs, machine paths, crash reports,
credentials and private data are not included in these review artifacts.

### Corrected-source timing and memory

The final bounded comparison completed all eight fresh JVMs and all 32 measured
streaming fits. Each fit asserted four external writers and the expected
allocation bound, and compared every prediction with bulk transfer. All 205
runtime artifact checksums remained unchanged.

This is a separate sample from the 80 earlier fits recorded in attempt 6.
The baseline substitutes the archived target-production LightGBM main JAR into
the same frozen common/test runtime as the corrected product. It is not a
complete independently rebuilt target checkout. The baseline JAR SHA256 is
`f8ee3aad9783410f4baa1ceca07ba5d3d4ae33fefc5ac1851a4090fc3d1c770f`.
The corrected source is the nine-file product manifest recorded above.

The workload uses 65,536 rows, 2,048 sparse features, four tasks, three boosting
iterations, three leaves, two native training threads, hint 16, and a 3 GiB Java
heap. OpenMP default and thread limit are both 16, with dynamic teams and binding
disabled. The history case first requests 64 threads through a bulk fit while
the actual team remains capped at 16. Streaming allocation is 16 for both fresh
variants and baseline history, and 64 for corrected history.

There are two JVMs per revision and scenario, four measured fits each. Fresh
ordering is baseline, corrected, corrected, baseline; history reverses that
ordering. First-fit seconds are the median across JVMs. Warm seconds are the
median of each JVM's three warm-fit medians. Peak RSS is the median of each
JVM's largest fit RSS, sampled at 10 ms.

| Scenario | Metric | Target production | Corrected PR | Change |
| --- | --- | ---: | ---: | ---: |
| Fresh | First fit, seconds | 11.220 | 9.829 | -12.4% |
| Fresh | Warm fit, seconds | 4.993 | 3.874 | -22.4% |
| Fresh | Peak process RSS, MiB | 2709.1 | 2721.8 | +0.5% |
| History | First fit, seconds | 9.084 | 9.049 | -0.4% |
| History | Warm fit, seconds | 3.381 | 3.517 | +4.0% |
| History | Peak process RSS, MiB | 2754.9 | 2724.0 | -1.1% |

Fresh warm per-JVM medians range from 3.738 to 6.247 seconds for baseline and
3.122 to 4.627 for corrected. History ranges are 3.117 to 3.646 and 3.407 to
3.626 seconds. These small, noisy samples do not establish attributable speedup
or slowdown and do not prove broad performance non-regression.

RSS includes the JVM, Spark and native code; it is not an isolated native
allocation measurement. Retaining width 64 instead of 16 does increase native
storage and traversal work. For 2,048 features and four writers, assuming
24-byte vector headers, the header-only estimate is 3 MiB at width 16 versus
12 MiB at width 64, an additional 9 MiB before element payloads and other storage.
The lower measured history RSS does not negate that allocation cost.
