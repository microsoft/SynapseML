# Shared Spark 4 branch context

Originally developed in [#2645](https://github.com/microsoft/SynapseML/pull/2645)
and [#2646](https://github.com/microsoft/SynapseML/pull/2646). Those PRs and the
follow-up syncs [#2659](https://github.com/microsoft/SynapseML/pull/2659) and
[#2661](https://github.com/microsoft/SynapseML/pull/2661) have merged.

The repository snapshot below was checked on 2026-09-16 against `master`
`1305587a4a`, `spark4.0` `ecec8dd58b`, and `spark4.1` `06897e5b27`.
These are target-branch commits, not proposed sync results. Both ports now
contain the OpenAI Python overrides, package-export guard tests, R codegen
guards and nested-stage loading, Petastorm compatibility layer, and the shared
`synapseml-build-14.3-gpu` pool. Older failure measurements below are history,
not current-head validation.

Recheck values with `git show <remote>/<branch>:<path>` before acting. Quote
the commit, file, and value checked; a local sync worktree is not evidence of
what has already landed on its target.

## Purpose and sync

- Spark 4 branches are maintained ports, not feature branches. Land ordinary
  work on `master`, then merge it into the port branch.
- Resolve conflicts per hunk and compare content with the merge base and
  `master`; blanket `ours`/`theirs` and reachability are insufficient.
- Publish review evidence with commit identifiers and repository-relative
  source paths. Omit machine-local worktree, tool, and session-artifact paths.
  Preserve original findings with appended resolutions. Historical review
  reports are not the current gate ledger; keep the PR description current.
- Earlier sync PRs were squash-merged. Their master commits can therefore be
  absent from ancestry even when their content is present. #2659 and #2661 both
  integrated master `a6fd536ad7`. Compare the target against that recorded
  content baseline as well as the actual merge base when resolving repeated
  conflicts. Retain real merge parents in new sync branches; never fabricate
  ancestry or assume an ahead/behind count measures missing functionality.
  At this snapshot GitHub reports `allow_merge_commit: false` and allows squash
  merges. Do not change repository merge settings as part of a sync. Record the
  integrated master SHA in the PR description so a later sync can identify its
  content baseline even if the PR is squash-merged.
- Diff `spark4.0` and `spark4.1` before debugging or merging
  (`git diff spark4.0 spark4.1 -- <path>`). `spark4.1` descends from `spark4.0`'s
  upgrade commit and is maintained more actively, so it has usually already hit
  and solved the same problem — it has directly supplied the fixes for R parsing,
  the R Spark connection, nested-stage loading in R, the generated-wrapper
  `super()` bug, the stale `__init__.py` shims, the local setup skill, the
  notebook runtime, and a failing training test. This is the single
  highest-value habit on these branches. Two cautions when porting: substitute
  the target branch's version strings, and confirm the fix is not specific to
  the source branch's Spark/Python version.

## Fabric LightGBM baseline

- At the 2026-08-26 baseline, Fabric Runtime 2.0 supplies Python `lightgbm`
  4.6.0 on Spark 4.1, but its JVM/SWIG classes load from
  `com.microsoft.ml.lightgbm:lightgbmlib:3.3.510`.
- Fabric has no separate managed Spark 4.0 runtime. Treat Runtime 2.0/Spark 4.1
  as the Fabric comparison point for the `spark4.0` port.
- Runtime 2.0 uses the same byte-for-byte Maven JAR as Runtime 1.3 (SHA-256
  `f2b1b13172699832594303ab4c04f3bc8fc2d24737e3e8c11d98d69a88c09272`).
- Do not infer the Maven dependency version from the Python package version.
  Changing the JNI/SWIG artifact is a separate compatibility change.

## Portable sync lessons

The follow-up [#2719](https://github.com/microsoft/SynapseML/pull/2719) carries
shared fixes discovered while validating [#2718](https://github.com/microsoft/SynapseML/pull/2718)
and [#2720](https://github.com/microsoft/SynapseML/pull/2720). These are proposed
changes, not additions to the target snapshot above.

- Python wrapper default lookup also rejects foreign-owned parameters on
  Spark 3.5. Preserve the guard in runtime constructor arguments, defaults, and
  stub defaults. It is not a Spark 4-only workaround.
- Discovery must distinguish production JARs from `-tests.jar`, including
  snapshot aliases and exploded `classes`/`test-classes` directories. Test real
  packaged JARs together, require generated production outputs, and prove test
  fixtures are excluded. An exploded-classpath pass is insufficient.
  `CodegenDiscoverySuite` derives the Scala binary version and uses an isolated
  SBT-like URL loader for the subprocess. Non-forked SBT dependencies are not all
  listed in `java.class.path`.
  SBT's `bgRunMain` uses `Runtime / fullClasspathAsJars`; overriding ordinary
  `fullClasspath` can leave codegen using a released dependency. Verify the
  loaded candidate version and nonempty generated wrappers and stubs.
- Generated OpenAI stubs may belong to the public class on master or a private
  generated base on a port. Inspect the public class's inheritance and test its
  actual stub and runtime methods; do not hardcode one layout for all branches.
- A minor-series Python pin is valid. Candidate Maven versions can themselves
  contain `-pythonX.Y`; this is not evidence of an accidentally copied Conda
  local version. Assert coordinates against the build's exact published version.
- Test the declared Python environment as well as focused installed-wheel
  checks. An SDK can import an optional HTTP transport only when it is present;
  omitting an incompatible declared transport from a minimal environment can
  hide the failure seen in CI. Report such isolation explicitly and obtain
  approval before changing dependency constraints.
- Internal branch layouts differ. The older direct-OSS-codegen build has no
  typing adapter. Recognize that specific layout without skipping packaging,
  but fail for a referenced missing helper or an unknown layout. See
  [the CI helper documentation](../../../../tools/ci/README.md).
- Read the first Fabric provisioning response. A workspace warehouse-limit
  error can leave a partial artifact; retries then report a name conflict and
  obscure the quota failure. Names already include a timestamp and UUID.
  Another uniqueness patch or blind retry does not restore runtime coverage.
  Cleanup of shared resources requires verified ownership and authorization.
  This is diagnostic guidance, not a quota fix in the master follow-up.

## Common deliberate differences from master

- Spark 4 uses Scala 2.13 and Java 17-era tooling, so generated Python lands in
  `target/scala-2.13/generated/src/python/` rather than master's `scala-2.12`
  path. The `tools/docker/*/Dockerfile` files set `JAVA_HOME` to Java 17 on
  both Spark 4 branches, where master sets Java 11 — check the branch, not
  master, if you are verifying this. `pipeline.yaml`
  drops master's `-XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled` from
  `SBT_OPTS`: CMS was removed in Java 17 and the JVM refuses to start with those
  flags, so a sync that restores them fails before any test runs.
- `environment.yml` moves pins forward for the branch's Python, and each pin
  carries a comment saying why. Those comments are the mechanism that stops a
  later sync from "restoring" master's value, so preserve them through conflict
  resolution. The recurring reasons: master's `pip` is too old to install for
  these interpreters, `torch`/`torchvision` need their first releases supporting
  the version, and `pandas`/`horovod` come from interpreter-specific wheel URLs.

  Measured, since these are easy to get backwards:

  | | master | spark4.0 | spark4.1 |
  | --- | --- | --- | --- |
  | `python` | 3.11.8 | 3.12.11 | 3.13 |
  | `pyarrow` | 10.0.1 | 18.0.0 | 18.0.0 |
  | `mlflow` | 2.21.3 | 2.21.3 | 2.21.3 |
  | `numpy` | 1.26.4 | 1.26.4 | unpinned |

  Each branch reached its `pyarrow` by a different route, so do not carry the
  reasoning across: `spark4.0` needs a release with cp312 wheels (older ones
  such as 11.0.0 have none and would build from source), `spark4.1` needs cp313
  wheels, and master is held *down* at
  10.0.1 because Petastorm uses legacy Parquet and fsspec APIs removed after
  PyArrow 10. Both ports now pair PyArrow 18 with MLflow 2.21.3.
- The `pyarrow` and `mlflow` pins are coupled, and the pinned versions are not
  the same on every branch — read both live values on the branch you are editing
  before changing either. The bound comes from MLflow: `mlflow==2.21.3` declares
  `pyarrow<20,>=4.0.0`, so on a branch pinning that MLflow, `pyarrow` must stay
  under 20 or move together with `mlflow`; bumps inside the bound are fine. Older
  MLflow pins carry different bounds, so check the pinned version's own metadata
  rather than assuming this one. Do not trust the inline comments: they disagree
  with each other and with the pins they sit next to.
- Scala 2.13 collection boundaries must produce immutable `Seq` values. The
  failure mode is why this matters: code that yields a `mutable.ArraySeq` where
  an `immutable.Seq` is expected throws `ClassCastException` **at runtime, not
  at compile time**, so a green compile proves nothing and the break surfaces
  one or two layers away from its cause. Prefer `toIndexedSeq` over `toList`
  when converting, because it preserves O(1) indexing. Both ports now normalize
  column-backed service parameter values in
  `CognitiveServiceBase.scala` through `getValueOpt` and
  `asImmutableCollection`; master does not have that conversion. Preserve it
  when merging unrelated authentication changes. It does not cover every
  direct `row.getAs[Seq[...]]` elsewhere, so test newly imported request paths
  with Spark-produced rows rather than only hand-built immutable fixtures.
- Preserve each port's actual Spark 4 adaptations. The pinned `spark4.0`
  target's `SAR.scala` uses `SAR.ItemAffinity` with explicit `itemIndex` and
  `affinity` struct fields after the old `Seq[Row]` UDF caused
  `UnboundRowEncoder`. The pinned `spark4.1` target still uses `Seq[Row]`;
  the same failure has not been established there by this source audit.
  Both targets qualify `col("sarUserFactors.flatList")` in `SARModel.scala`
  to avoid `DetectAmbiguousSelfJoin`.
  Both pinned ports have `Wrappable.safeGetDefault`; the shared follow-up above
  also proves the need on master. `RWrappable.rParamArg` still calls `getDefault`
  directly. Do not describe the Python guard as an R fix.
  `VerifyTrainClassifier`'s vector fixture no longer feeds `Double.NaN` to the
  trainer, because Spark 4 does not tolerate a NaN feature reaching logistic
  regression the way 3.5 did. That test is about training on a vector column,
  not about NaN, so the value was replaced rather than the assertion weakened.
  Master still has the `Double.NaN` at `VerifyTrainClassifier.scala:121`, so a
  sync will try to restore it; do not let it, and do not weaken the assertion
  instead.
- `OpenAIPrompt` sets `pyInternalWrapper = true`, so codegen emits
  `class _OpenAIPrompt` and a hand-written `OpenAIPrompt.py` supplies the public
  name. Python emitted into that class must use zero-argument `super()`; a
  hardcoded `super(OpenAIPrompt, self)` raises `NameError` because that name does
  not exist inside the generated module. See `OpenAIPromptPythonOverrides.scala`,
  which is present on both ports.
- `PythonInitMerger` makes hand-written `__init__.py` files live package code by
  splicing them *after* the generated imports; before it, codegen overwrote them
  and their contents were inert, so a stale one is now a real bug. Keep the HTTP
  initializer empty — it listed `HTTPFunctions` and `ServingFunctions`, which are
  modules of free functions with no same-named class, and the failed import broke
  `PythonTests core` plus seven website samples. Remove initializers that only
  duplicate generated exports, and do not narrow `import *` by redefining
  `__all__` as a hand-maintained list. Keep the ones that add exports codegen
  does not emit. `test_http_package.py` and `test_package_exports.py` guard this
  on both ports. The master follow-up above adds those guards; the pinned master
  target does not yet contain them.
- `cyber/utils/spark_utils.py` differs between the branches without either form
  being version-specific: `spark4.0` builds its indexed frame with
  `rdd.toDF(schema)` and `spark4.1` uses `spark.createDataFrame(rdd, schema)`.
  `toDF` was measured working on both 4.0.1 and 4.1.1, so this is a portable
  choice rather than a hazard. Adopting 4.1's form only reduces reliance on the
  monkey-patched RDD API, which does not exist under Spark Connect, and buys
  little on its own while the surrounding `df.rdd.zipWithIndex()` remains an RDD
  call.
- R generation requires ANSI double-quoted identifiers — `RTestGen.scala` sets
  `spark.sql.ansi.enabled=true` and `spark.sql.ansi.doubleQuotedIdentifiers=true`,
  because sparklyr emits `SELECT 0L AS "class", ...` and without the second flag
  Spark 4 reads `"class"` as a string literal and fails with
  `PARSE_SYNTAX_ERROR`. It also requires the validated sparklyr
  1.9.5 pin from the PR snapshots, `SPARK_HOME` connection behavior, and JVM
  loading of nested stages. Both ports now pin sparklyr 1.9.5 with
  `r-base=4.4`. Keep that pairing: 69/69 `RTests` was historically measured for the
  combination, not for the sparklyr pin alone. Interleaved failures with
  successful tests between them point to selection/proxy behavior, not a dead
  Spark session; read the backtrace. Under sparklyr 1.9.3 with dbplyr 2.6 the
  tell is a frame chain through `dbplyr:::select.tbl_lazy`,
  `sparklyr:::tidyselect_data_proxy.tbl_spark`
  and `simulate_vars_spark`, which surfaces as `invoke_static`/`hive_context`
  being called on `NULL` and reads misleadingly like a dead session.
- `RCodegenSuite` asserts cheap R generation invariants on both ports. Run it before spending a
  pipeline run on an R failure, and keep its assertions in step when changing
  generated R.
- Nested stages load off the JVM on both ports. `PipelineStageWrappable.rLoadLine` emits
  `sparklyr:::new_ml_pipeline_stage(invoke(spark_jobj(x), "getStages")[[1]])`
  rather than `ml_stages(x)[[1]]`. `new_ml_pipeline_stage` is sparklyr-internal
  but has an identical signature in every release from v1.8.0 to v1.9.5.
  `EstimatorParam`, `ModelParam`, `PipelineStageParam` and `TransformerParam` all
  inherit this single implementation — do not reintroduce per-class overrides,
  and keep the three `rLoadLine` assertions in
  `VerifyModelParam`/`VerifyPipelineStageParams` in step with it. Be accurate
  about its status: on a branch whose R tests died earlier on `ml_load`, this
  line was never reached, so it is alignment with the working branch rather than
  a proven fix.

## Runtime and CI

- Spark 4 Databricks builds contend for scarce GPU capacity, and the pool names
  differ by branch — read them from `pipeline.yaml`/the Databricks test config on
  your branch rather than assuming. Instance pools are runtime-agnostic, so a
  GPU pool is often deliberately shared across branches to avoid duplicating
  scarce quota; where it is shared it holds three workers
  (`GpuWorkersPerRun` 1 x `GpuConcurrentRuns` 3), so two concurrent builds can
  exhaust it. Queue Spark 4 builds sequentially. Where the pool *is* shared, the
  sibling branch is a free control: an outcome that tracks the branch rather
  than the timing is a code difference, not contention.
- `areLibrariesInstalled == false` is a timeout, not a capacity verdict, and the
  logic inverts the way people expect. The check *throws* `Library Installation
  Failure` with the offending statuses if any library reports `FAILED`, so
  returning `false` means the opposite: nothing failed, the libraries simply had
  not all reached `INSTALLED` before the retry budget ran out (`60 * 10` attempts
  at 1s, about 10 minutes). A slow install reads exactly like a starved pool.
  Read statuses and notebook duration before classifying it.
- `DatabricksCPUStreamingTests` is defined on the Spark 4 branches but neither
  target currently schedules it in `pipeline.yaml`. Spark 4.0's original port
  scheduled it before later syncs aligned the matrix. Record this coverage gap
  explicitly; an unscheduled class produces no skipped-test result. It
  is a separate class because the streaming notebook's `server.stop()` cancels
  concurrent SparkContext jobs, so it needs its own cluster instead of a slot on
  an existing leg, which is why scheduling it costs pool capacity. The in-repo
  comment attributes that behaviour to Spark 4.0 and it has not been
  re-confirmed on 4.1. If a sync drops the leg while leaving the class defined,
  nothing fails to compile and nothing reports the gap, so check deliberately.
- The Databricks GPU suite was split and then deliberately re-merged. Both
  ports now use the consolidated suite. Historically, #2538
  split it into `DatabricksGPUTests1/2/3`, each building its own cluster with two
  workers and running exactly one notebook via `gpuNotebook(0)`, `(1)`, `(2)`.
  #2573 (`fix: restore SynapseML Azure pipeline`) reverted that to a single
  `DatabricksGPUTests` because the split could not fit: three clusters times two
  workers needs six GPU nodes, against a pool holding
  `GpuWorkersPerRun` 1 x `GpuConcurrentRuns` 3 = three. Master's current form
  runs the whole `GPUNotebooks` set on one cluster sized at `GpuWorkersPerRun`
  (one worker, so concurrent builds can share the pool), pins the driver to the
  **CPU** pool (`driverInstancePoolId = Some(PoolId)`) so it does not consume a
  GPU node, and rather than failing on a starved pool waits for one through
  `createActiveCluster` with `maxAttempts = Int.MaxValue` and
  `maxRetryDurationMs` of three hours. `SYNAPSEML_GPU_SMOKE_TESTS` passes
  `synapseml_ci_smoke` through to the notebooks, and the job takes a 300-minute
  timeout to absorb the sequential run. Read the file rather than this paragraph
  for the mechanism: it changed between #2573 and now, and an earlier draft of
  this bullet described the #2573 snapshot as if it were current.
- Keep the consolidated form. The former hardcoded indices tested exactly
  three notebooks and would miss `Quickstart - End-to-end Local RAG with Phi Model`.
  That notebook is now on both ports. `DatabricksGPUTests` reads the complete
  `GPUNotebooks` set; verify the actual selected notebook list and test results.
- Petastorm calls pyarrow APIs the pinned pyarrow no longer ships, so Horovod's
  Spark backend needs a compatibility layer. Both ports have one. This is a
  library-version problem, not a Python-version one, so a branch on the same
  pyarrow is not exempt. Deep-learning unit tests will not reveal the gap:
  without a usable Horovod the estimators are stubbed and the Petastorm path
  never runs.
- `/azp run` was verified to queue these targets after the ADO pull-request
  trigger filter changed on 2026-08-17. It previously allowed only `master`;
  the updated filter covered `master`, `spark3.5`,
  `spark4.0` and `spark4.1`, verified then by builds recording `reason=pullRequest`
  and `requestedFor=GitHub` rather than `reason=manual`. Those two fields are
  the reliable way to tell a trigger-driven run from one you queued by hand. If
  a comment produces no build, re-read the
  definition's trigger filter before assuming flakiness, and fall back to
  queueing the PR merge ref (`refs/pull/<N>/merge`), never
  `refs/heads/<branch>`, which fails service-connection authorization.
- **The trigger filter lives on the ADO definition, not in `pipeline.yaml`.**
  The `pr:` block in `pipeline.yaml` is a red herring: a UI-defined trigger
  overrides it silently, so editing the YAML changes nothing. The proof is on
  the branch itself — `spark4.0`'s own `pipeline.yaml` `pr:` block lists
  `master`, `spark3.3` and `spark3.5` and does **not** list `spark4.0`, yet PRs
  targeting `spark4.0` build. Read the real value from the definition instead:

  ```
  GET .../_apis/build/definitions/17563?api-version=7.0
  ```

  The recorded `triggers[].branchFilters` was `+master, +spark3.5, +spark4.0,
  +spark4.1`, and the `continuousIntegration` trigger reports
  `settingsSourceType: 2`, which means UI-defined rather than YAML-defined.
  Consequence for a future release branch: adding it to `pipeline.yaml` does not
  give it PR builds. Someone has to add it to the definition's filter.
- GitHub checks compile/lint but do not replace full Azure, Databricks, native,
  R, or service validation.
- Intermittent ONNX OOM (`OutOfMemoryError` in `ImageFeaturizerSuite`, under the
  `UnitTests onnx` leg) and R package HTTP failures
  (a conda `HTTP 403` in `RTests vw`) require log evidence and a controlled
  rerun; they are not automatic product regressions or exemptions.

## Where the Java version is declared

There is no single source of truth for the JDK. Each branch declares it in
several files, and a sync can silently disagree with itself if only some are
updated. Measured values:

| File | master | spark4.0 | spark4.1 |
| --- | --- | --- | --- |
| `.github/workflows/pr-validation.yml` | 11 | 17 | 17 |
| `environment.yml` (`openjdk`) | absent | 17 | 17 |
| `environment.dev.yml` (`openjdk`) | no file | 17 | 17 |
| `templates/java_setup.yml` (`versionSpec`) | 11 | 17 | 17 |
| `pipeline.yaml` (`JAVA_VERSION`, ReleaseBranchCompat) | 17 | 17 | 17 |
| `tools/docker/*/Dockerfile` (`JAVA_HOME`) | 11 | 17 | 17 |

`JAVA_VERSION` describes the replay target, not the branch owning the pipeline.
All three pipelines currently replay `RELEASE_BRANCH: spark4.1`; this does not
prove Spark 4.0 compatibility. Validate the Spark 4.0 sync directly.

[#2652](https://github.com/microsoft/SynapseML/pull/2652) has merged.
`templates/java_setup.yml` exists on all three branches and is included by
InternalCompat. Preserve each port's JDK 17 while accepting master's independent
action/template updates. Restoring JDK 11 can cause `Class java.lang.Record not found`.
Do not describe an add/add conflict as one-time: squash-merging a sync can make
it recur. Verify the final value with:

```
git show <branch>:templates/java_setup.yml | grep versionSpec
```

## Hand-written `__init__.py` files

`PythonInitMerger` came from master and **preserves** hand-written `__init__.py`
content by splicing it after the generated imports. Codegen previously
overwrote these files, so their contents were inert; they are now live code in
the shipped package, which makes a stale one a real bug rather than dead text.
This is why the Spark 4 branches had to audit them.

| Path | State | Why |
| --- | --- | --- |
| `core/.../io/http/__init__.py` | must stay empty | Listed free-function modules; see below |
| `vw/`, `services/openai/` | removed | Duplicated codegen output |
| `recommendation/` | redundant on the pinned targets; removed by the master follow-up | All nine class exports are already generated |
| `dl/`, `hf/`, `cognitive/`, `mmlspark/` | kept | Add exports codegen omits |

`core/.../io/http/__init__.py` listed `HTTPFunctions` and `ServingFunctions`,
which are modules of free functions with no same-named class, so the import
failed and broke `PythonTests core` plus seven website-sample docs. The `vw/`
and `services/openai/` files also redefined `__all__`, which narrowed
`import *` to a hand-maintained list.

Do not add new `__init__.py` files that re-list generated classes. On the Spark 4
branches this is guarded by two tests,
`core/src/test/python/synapsemltest/io/http/test_http_package.py` and
`core/src/test/python/synapsemltest/recommendation/test_package_exports.py`. Note
where they are and are not: both are on both ports, and **neither is on
`master` at the pinned target snapshot**, which carries `PythonInitMerger`
without them. The master follow-up removes the redundant recommendation
initializer so generated model exports are no longer narrowed by its stale
`__all__`. Its guard checks both the previously missing names and every generated
model module. Verify whether it has landed rather than assuming
the guards from the merger's presence:
`git ls-tree -r --name-only ms/<branch> | grep -E 'test_http_package|test_package_exports'`.

## Before merging a sync

1. Recheck the target's live versions, pins, triggers, and skips.
2. Prove master content survived conflict resolution.
3. Run full Azure validation without a concurrent Spark 4 build.
4. Diff the sibling Spark 4 branch and explain every remaining difference.
