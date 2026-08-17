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
- The `pyarrow` and `mlflow` pins move in lockstep: `mlflow==2.21.3` declares
  `pyarrow<20,>=4.0.0`, so raising `pyarrow` alone breaks the environment solve.
  Trust that bound over the inline comments, which disagree with each other —
  `spark4.1`'s `environment.yml` says `pyarrow<19`, which is wrong, and has no
  comment on the `mlflow` pin at all.
- Scala 2.13 collection boundaries must produce immutable `Seq` values; keep
  the central `asImmutableCollection` conversion rather than per-service fixes.
- Preserve Spark 4 adaptations for SAR encoders/self-joins,
  `Wrappable.safeGetDefault`, and the non-NaN classifier fixture.
- `OpenAIPrompt` is an internal generated wrapper. Generated overrides use
  zero-argument `super()` because the public class name is not in that module.
- `PythonInitMerger` makes hand-written `__init__.py` files live package code.
  Keep the HTTP initializer empty, remove duplicate generated exports, and do
  not narrow `__all__` with hand-maintained class lists.
- R generation requires ANSI double-quoted identifiers, the validated sparklyr
  1.9.5 pin from the PR snapshots, `SPARK_HOME` connection behavior, and JVM
  loading of nested stages. Interleaved failures with successful tests between
  them point to selection/proxy behavior, not a dead Spark session; read the
  backtrace.
- `RCodegenSuite` asserts the cheap R generation invariants on both Spark 4
  branches. Run it before spending a full pipeline run on an R failure, and keep
  its assertions in step when changing generated R.

## Runtime and CI

- Spark 4 Databricks builds share scarce GPU capacity. Queue them sequentially
  and use sibling-branch timing/results as a control before blaming capacity.
- `areLibrariesInstalled == false` can mean install timeout rather than a
  failed library. Read statuses and notebook duration before classifying it.
- `DatabricksCPUStreamingTests` is unscheduled on both Spark 4 branches: the
  class exists in `DatabricksCPUTests.scala`, but `pipeline.yaml` names its test
  legs explicitly and never lists it. It exists as a separate class because the
  streaming notebook's `server.stop()` cancels concurrent SparkContext jobs, so
  it has to run on its own cluster instead of joining an existing leg — which is
  why scheduling it needs pool capacity as well as a notebook fix. The in-repo
  comment is identical on both branches and attributes the behaviour to Spark
  4.0; it has not been re-confirmed on 4.1. Treat it as a known coverage gap to
  verify, not as an omission to accept silently.
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
