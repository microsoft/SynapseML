# Shared Spark 4 branch context

Condensed from the branch guides developed in
[#2645](https://github.com/microsoft/SynapseML/pull/2645) and
[#2646](https://github.com/microsoft/SynapseML/pull/2646).

**Read this as describing the branches as of those two sync PRs, not as a
snapshot of the live branches.** Both guides were written on the sync branches,
so they describe the merged result. Until those PRs land, a live branch can lack
things described here — at the time of writing, live `spark4.0` has no
`OpenAIPromptPythonOverrides.scala`, no `test_http_package.py` /
`test_package_exports.py`, no `new_ml_pipeline_stage` in generated R, and a
different GPU pool. Verify every item against the live target branch with
`git show <branch>:<path>` or `git grep <pattern> <branch>`; do not read it out
of a local sync worktree, which is a PR result rather than branch state. That
specific mistake produced several wrong claims in earlier revisions of this
file.

## Purpose and sync

- Spark 4 branches are maintained ports, not feature branches. Land ordinary
  work on `master`, then merge it into the port branch.
- Resolve conflicts per hunk and compare content with the merge base and
  `master`; blanket `ours`/`theirs` and reachability are insufficient.
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

## Common deliberate differences from master

- Spark 4 uses Scala 2.13 and Java 17-era tooling, so generated Python lands in
  `target/scala-2.13/generated/src/python/` rather than master's `scala-2.12`
  path. `tools/docker/*/Dockerfile` set `JAVA_HOME` to Java 17 and
  `.github/workflows/pr-validation.yml` uses JDK 17. `pipeline.yaml` drops
  master's `-XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled` from
  `SBT_OPTS`: CMS was removed in Java 17 and the JVM refuses to start with those
  flags, so a sync that restores them fails before any test runs.
- `environment.yml` moves pins forward for the branch's Python, and each pin
  carries a comment saying why. Those comments are the mechanism that stops a
  later sync from "restoring" master's value, so preserve them through conflict
  resolution. The recurring reasons: master's `pip` is too old to install for
  these interpreters, `torch`/`torchvision` need their first releases supporting
  the version, and `pandas`/`horovod` come from interpreter-specific wheel URLs.
- The `pyarrow` and `mlflow` pins are coupled, and the pinned versions are not
  the same on every branch — read both live values on the branch you are editing
  before changing either. The bound comes from MLflow: `mlflow==2.21.3` declares
  `pyarrow<20,>=4.0.0`, so on a branch pinning that MLflow, `pyarrow` must stay
  under 20 or move together with `mlflow`; bumps inside the bound are fine. Older
  MLflow pins carry different bounds, so check the pinned version's own metadata
  rather than assuming this one. Do not trust the inline comments: they disagree
  with each other and with the pins they sit next to.
- Scala 2.13 collection boundaries must produce immutable `Seq` values; keep
  the central `asImmutableCollection` conversion rather than per-service fixes.
- Preserve the Spark 4 adaptations. In `SAR.scala`/`SARModel.scala` the affinity
  pairs use a named `case class` with explicit struct fields because Spark 4
  rejects the old `Seq[Row]` UDF shape with `UnboundRowEncoder`, and the join
  column is qualified (`col("sarUserFactors.flatList")`) because a self-join now
  trips `DetectAmbiguousSelfJoin`. `Wrappable.safeGetDefault` guards
  `getDefault`, which throws on Spark 4 where Spark 3 returned a default.
  `VerifyTrainClassifier`'s vector fixture no longer feeds `Double.NaN` to the
  trainer: that test is about training on a vector column, not about NaN, so the
  value was replaced rather than the assertion weakened.
- `OpenAIPrompt` sets `pyInternalWrapper = true`, so codegen emits
  `class _OpenAIPrompt` and a hand-written `OpenAIPrompt.py` supplies the public
  name. Python emitted into that class must use zero-argument `super()`; a
  hardcoded `super(OpenAIPrompt, self)` raises `NameError` because that name does
  not exist inside the generated module. See `OpenAIPromptPythonOverrides.scala`,
  which is on `spark4.1` and reaches `spark4.0` with #2646.
- `PythonInitMerger` makes hand-written `__init__.py` files live package code by
  splicing them *after* the generated imports; before it, codegen overwrote them
  and their contents were inert, so a stale one is now a real bug. Keep the HTTP
  initializer empty — it listed `HTTPFunctions` and `ServingFunctions`, which are
  modules of free functions with no same-named class, and the failed import broke
  `PythonTests core` plus seven website samples. Remove initializers that only
  duplicate generated exports, and do not narrow `import *` by redefining
  `__all__` as a hand-maintained list. Keep the ones that add exports codegen
  does not emit. `test_http_package.py` and `test_package_exports.py` guard this
  where they exist; they are not on live `spark4.0` yet and arrive there with
  #2646, so on that branch the policy is currently unenforced.
- `cyber/utils/spark_utils.py` differs between the branches without either form
  being version-specific: `spark4.0` builds its indexed frame with
  `rdd.toDF(schema)` and `spark4.1` uses `spark.createDataFrame(rdd, schema)`.
  `toDF` was measured working on both 4.0.1 and 4.1.1, so this is a portable
  choice rather than a hazard. Adopting 4.1's form only reduces reliance on the
  monkey-patched RDD API, which does not exist under Spark Connect, and buys
  little on its own while the surrounding `df.rdd.zipWithIndex()` remains an RDD
  call.
- R generation requires ANSI double-quoted identifiers, the validated sparklyr
  1.9.5 pin from the PR snapshots, `SPARK_HOME` connection behavior, and JVM
  loading of nested stages. That pin is not yet everywhere — check
  `environment.yml` on your branch, since a branch still on sparklyr 1.9.3 has
  the failure below as a live concern rather than as history. Where 1.9.5 is
  applied, keep it paired with `r-base=4.4`: 69/69 `RTests` was measured for the
  combination, not for the sparklyr pin alone. Interleaved failures with
  successful tests between them point to selection/proxy behavior, not a dead
  Spark session; read the backtrace. Under sparklyr 1.9.3 with dbplyr 2.6 the
  tell is a frame chain through `dbplyr:::select.tbl_lazy`,
  `sparklyr:::tidyselect_data_proxy.tbl_spark`
  and `simulate_vars_spark`, which surfaces as `invoke_static`/`hive_context`
  being called on `NULL` and reads misleadingly like a dead session.
- `RCodegenSuite` asserts cheap R generation invariants without a full pipeline
  run, but it is not present on every branch — `spark4.1` has it and `spark4.0`
  does not yet. Check for it before relying on it, run it before spending a
  pipeline run on an R failure, and keep its assertions in step when changing
  generated R.
- Nested stages load off the JVM on branches that have adopted it — `spark4.1`
  has, live `spark4.0` has not yet. `PipelineStageWrappable.rLoadLine` emits
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
- `DatabricksCPUStreamingTests` exists only on the Spark 4 branches, and whether
  it is scheduled varies by branch — read `pipeline.yaml` on the branch you are
  working on rather than assuming. It is a separate class because the streaming
  notebook's `server.stop()` cancels concurrent SparkContext jobs, so it needs its
  own cluster instead of a slot on an existing leg, which is why scheduling it
  costs pool capacity. The in-repo comment attributes that behaviour to Spark 4.0
  and it has not been re-confirmed on 4.1. If a sync drops its leg while leaving
  the class defined, that is lost coverage rather than a cleanup: the class is
  still there, so nothing fails to compile and nothing reports the gap.
- Petastorm calls pyarrow APIs the pinned pyarrow no longer ships, so Horovod's
  Spark backend needs a compatibility layer. Only `spark4.1` has one. This is a
  library-version problem, not a Python-version one, so a branch on the same
  pyarrow is not exempt. Deep-learning unit tests will not reveal the gap:
  without a usable Horovod the estimators are stubbed and the Petastorm path
  never runs.
- `/azp run` queues these targets. The ADO pull-request trigger filter allowed
  only `master` until 2026-08-17; it now covers `master`, `spark3.5`,
  `spark4.0` and `spark4.1`, verified by builds recording `reason=pullRequest`
  rather than `reason=manual`. If a comment produces no build, re-read the
  definition's trigger filter before assuming flakiness, and fall back to
  queueing the PR merge ref, never `refs/heads/<branch>`.
- GitHub checks compile/lint but do not replace full Azure, Databricks, native,
  R, or service validation.
- Intermittent ONNX OOM and R package HTTP failures require log evidence and a
  controlled rerun; they are not automatic product regressions or exemptions.

## Before merging a sync

1. Recheck the target's live versions, pins, triggers, and skips.
2. Prove master content survived conflict resolution.
3. Run full Azure validation without a concurrent Spark 4 build.
4. Diff the sibling Spark 4 branch and explain every remaining difference.
