# Shared Spark 4 branch context

Condensed from the branch guides developed in
[#2645](https://github.com/microsoft/SynapseML/pull/2645) and
[#2646](https://github.com/microsoft/SynapseML/pull/2646). Verify every item
against the live target branch.

## Purpose and sync

- Spark 4 branches are maintained ports, not feature branches. Land ordinary
  work on `master`, then merge it into the port branch.
- Resolve conflicts per hunk and compare content with the merge base and
  `master`; blanket `ours`/`theirs` and reachability are insufficient.
- Diff `spark4.0` and `spark4.1` before debugging or merging. Shared fixes often
  already exist on the sibling branch, but version-specific changes must not be
  copied blindly.

## Common deliberate differences from master

- Spark 4 uses Scala 2.13 and Java 17-era tooling. Preserve branch-specific
  dependency comments, Java configuration, and removal of obsolete CMS flags.
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
- Preserve Spark 4 adaptations for SAR encoders/self-joins,
  `Wrappable.safeGetDefault`, and the non-NaN classifier fixture.
- `OpenAIPrompt` is an internal generated wrapper. Generated overrides use
  zero-argument `super()` because the public class name is not in that module.
- `PythonInitMerger` makes hand-written `__init__.py` files live package code.
  Keep the HTTP initializer empty, remove duplicate generated exports, and do
  not narrow `__all__` with hand-maintained class lists.
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

## Runtime and CI

- Spark 4 Databricks builds share scarce GPU capacity. Queue them sequentially
  and use sibling-branch timing/results as a control before blaming capacity.
- `areLibrariesInstalled == false` can mean install timeout rather than a
  failed library. Read statuses and notebook duration before classifying it.
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
