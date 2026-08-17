# Shared Spark 4 branch context

Condensed from the branch guides developed in
[#2645](https://github.com/microsoft/SynapseML/pull/2645) and
[#2646](https://github.com/microsoft/SynapseML/pull/2646).

**Read this as describing the branches as of those two sync PRs, not as a
snapshot of the live branches.** Both guides were written on the sync branches,
so they describe the merged result. Until those PRs land, a live branch can lack
things described here — at the time of writing, live `spark4.0` has no
`OpenAIPromptPythonOverrides.scala`, no `test_http_package.py` /
`test_package_exports.py`, no `new_ml_pipeline_stage` in generated R, and its
own GPU pool (`DatabricksUtilities.scala` sets
`GpuPoolName = "synapseml-build-17.3-gpu"` there, not master's
`synapseml-build-14.3-gpu`). Verify every item against the live target branch
with `git show <branch>:<path>` or `git grep <pattern> <branch>`; do not read it
out of a local sync worktree, which is a PR result rather than branch state.
That specific mistake produced several wrong claims in earlier revisions of this
file, and an automated reviewer then made it against this very paragraph —
reporting the GPU pool sentence as stale after reading the sync branch. Quote
the file and value you checked, so the next reader can repeat the check instead
of re-deriving it from whatever tree they happen to have open.

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
  | `pyarrow` | 10.0.1 | 22.0.0 | 18.0.0 |
  | `mlflow` | 2.21.3 | 1.26.1 | 2.21.3 |

  Each branch reached its `pyarrow` by a different route, so do not carry the
  reasoning across: `spark4.0` needs a release with cp312 wheels (older ones
  such as 11.0.0 have none and would build from source), `spark4.1` needs cp313
  wheels under an `mlflow` 2.x `pyarrow<19` bound, and master is held *down* at
  10.0.1 because Petastorm uses legacy Parquet and fsspec APIs removed after
  PyArrow 10. Note also that `spark4.0` carries `mlflow==1.26.1`, a downgrade
  from master's 2.21.3, which is not explained by the Python version and has not
  been validated.
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
  when converting, because it preserves O(1) indexing. Be careful what you
  believe about *where* this is handled: the branch-local notes claimed
  `CognitiveServiceBase.getValueOpt` converted centrally through a helper called
  `asImmutableCollection`, and neither is true — `getValueOpt` returns the row
  or default value with no conversion, and `asImmutableCollection` appears in no
  branch, only in two abandoned commits (`745b342b48`, `6cab133efd`) that are
  contained in no tip. Verify with
  `git grep asImmutableCollection ms/master ms/spark4.0 ms/spark4.1`. If the
  `ClassCastException` resurfaces, one conversion in `CognitiveServiceBase` is
  the right shape of fix, but treat it as a change to make rather than one
  already in place.
- Preserve the Spark 4 adaptations. In `SAR.scala`/`SARModel.scala` the affinity
  pairs use a named `case class` with explicit struct fields because Spark 4
  rejects the old `Seq[Row]` UDF shape with `UnboundRowEncoder`, and the join
  column is qualified (`col("sarUserFactors.flatList")`) because a self-join now
  trips `DetectAmbiguousSelfJoin`. `Wrappable.safeGetDefault` guards
  `getDefault`, which throws on Spark 4 where Spark 3 returned a default.
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
- R generation requires ANSI double-quoted identifiers — `RTestGen.scala` sets
  `spark.sql.ansi.enabled=true` and `spark.sql.ansi.doubleQuotedIdentifiers=true`,
  because sparklyr emits `SELECT 0L AS "class", ...` and without the second flag
  Spark 4 reads `"class"` as a string literal and fails with
  `PARSE_SYNTAX_ERROR`. It also requires the validated sparklyr
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
  working on rather than assuming. As measured on 2026-08-17, only live
  `spark4.0` gives it a leg, and it got one in the original port commit
  `b76c391be4`; `master` and `spark4.1` leave the class defined but unscheduled
  pending pool capacity and a notebook fix. A sync from master therefore drops
  that leg on `spark4.0`, which converges the three branches rather than
  regressing one — but record it as a decision, because nothing reports it. It
  is a separate class because the streaming notebook's `server.stop()` cancels
  concurrent SparkContext jobs, so it needs its own cluster instead of a slot on
  an existing leg, which is why scheduling it costs pool capacity. The in-repo
  comment attributes that behaviour to Spark 4.0 and it has not been
  re-confirmed on 4.1. If a sync drops the leg while leaving the class defined,
  nothing fails to compile and nothing reports the gap, so check deliberately.
- The Databricks GPU suite was split and then deliberately re-merged, and the
  history matters because `spark4.0` still carries the abandoned shape. #2538
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
- Prefer the consolidated form on every branch, and never restore the split
  during a sync. Its indices are hardcoded, so it tests exactly three notebooks
  no matter how many exist: at the time of writing `master` and `spark4.1` have
  four GPU notebooks (`Fine-tune`/`Phi Model` matches) while live `spark4.0` has
  three, so the split covers `spark4.0` today and would silently skip index 3 —
  `Quickstart - End-to-end Local RAG with Phi Model` — the moment a sync brings
  master's fourth notebook in. `DatabricksGPUTests` reads `GPUNotebooks` whole
  and cannot drift that way. #2646 already lands exactly this: the merged
  `DatabricksGPUTests.scala` is byte-identical to master's and the branch picks
  up the fourth notebook, so `spark4.0` needs no separate change.
- Petastorm calls pyarrow APIs the pinned pyarrow no longer ships, so Horovod's
  Spark backend needs a compatibility layer. Only `spark4.1` has one. This is a
  library-version problem, not a Python-version one, so a branch on the same
  pyarrow is not exempt. Deep-learning unit tests will not reveal the gap:
  without a usable Horovod the estimators are stubbed and the Petastorm path
  never runs.
- `/azp run` queues these targets. The ADO pull-request trigger filter allowed
  only `master` until 2026-08-17; it now covers `master`, `spark3.5`,
  `spark4.0` and `spark4.1`, verified by builds recording `reason=pullRequest`
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

  `triggers[].branchFilters` is currently `+master, +spark3.5, +spark4.0,
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
| `.github/workflows/pr-validation.yml` | 11 | **11** | 17 |
| `environment.yml` (`openjdk`) | absent | 17 | 17 |
| `environment.dev.yml` (`openjdk`) | no file | 17 | 17 |
| `templates/java_setup.yml` (`versionSpec`) | no file | 17 | 17 |
| `pipeline.yaml` (`JAVA_VERSION`, ReleaseBranchCompat) | 17 | absent | 17 |
| `tools/docker/*/Dockerfile` (`JAVA_HOME`) | 11 | 17 | 17 |

Two things in that table are not typos. `spark4.0`'s GitHub workflow pins JDK
11 while the rest of the branch is on 17, so do not assume the workflow proves
the branch's Java version; check `environment.yml` or `java_setup.yml` instead.
And `spark4.0` has no `JAVA_VERSION` because it is not yet in the
ReleaseBranchCompat matrix, which is a separate follow-up.

`templates/java_setup.yml` is the pin that CI jobs consume. On `spark4.0` it is
included by the `Style` job; on `spark4.1` the file exists but nothing includes
it yet. Master does **not** have the file at the time of writing: it arrives
with [#2652](https://github.com/microsoft/SynapseML/pull/2652), which sets it to
11 — master's already-effective JDK, measured from a build that echoes
`java -version` — and includes it from the InternalCompat job so that job stops
compiling Spark 4 code on master's JDK. If that PR has not merged yet, expect
the file to be absent on master and the table row above to read "no file";
confirm with `git show ms/master:templates/java_setup.yml`.

**Conflict rule: on the first sync after #2652 merges, `templates/java_setup.yml`
conflicts add/add and git leaves markers. Always keep the branch's own 17.**
Taking master's side is the intuitive resolution and the wrong one: it silently
drops the branch to Java 11 and reintroduces `Class java.lang.Record not found`.
The conflict is one-time — once resolved, the file histories are connected and
later syncs merge it cleanly. Verify with:

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
| `recommendation/`, `dl/`, `hf/`, `cognitive/`, `mmlspark/` | kept | Add exports codegen omits |

`core/.../io/http/__init__.py` listed `HTTPFunctions` and `ServingFunctions`,
which are modules of free functions with no same-named class, so the import
failed and broke `PythonTests core` plus seven website-sample docs. The `vw/`
and `services/openai/` files also redefined `__all__`, which narrowed
`import *` to a hand-maintained list.

Do not add new `__init__.py` files that re-list generated classes. On the Spark 4
branches this is guarded by two tests,
`core/src/test/python/synapsemltest/io/http/test_http_package.py` and
`core/src/test/python/synapsemltest/recommendation/test_package_exports.py`. Note
where they are and are not: both are on `spark4.1`, both reach `spark4.0` through
[#2646](https://github.com/microsoft/SynapseML/pull/2646), and **neither is on
`master`**, which carries `PythonInitMerger` without them. So a change to these
files on `master` is unguarded, and the guards cannot be assumed from the merger's
presence. Verify with
`git ls-tree -r --name-only ms/<branch> | grep -E 'test_http_package|test_package_exports'`.

## Before merging a sync

1. Recheck the target's live versions, pins, triggers, and skips.
2. Prove master content survived conflict resolution.
3. Run full Azure validation without a concurrent Spark 4 build.
4. Diff the sibling Spark 4 branch and explain every remaining difference.
