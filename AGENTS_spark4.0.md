# AGENTS_spark4.0.md

Branch-specific context for `spark4.0`. Read [AGENTS.md](AGENTS.md) first for the
branch model and sync rules that apply everywhere.

## What this branch is

A port of SynapseML to Spark 4.0. It exists so the library can run on runtimes
that have moved past Spark 3.x; it is not a feature branch. Features and fixes
land on `master` and arrive here when `master` is merged in.

| | |
| --- | --- |
| Spark | 4.0.1 |
| Scala | 2.13.16 |
| Java | 17 |
| Python | 3.12 |
| Databricks runtime | `17.3.x-scala2.13`, GPU `17.3.x-gpu-ml-scala2.13` |
| Generated Python | `target/scala-2.13/generated/src/python/` |

## Start here when something breaks

**Check `spark4.1` before debugging from scratch.** That branch is a descendant
of this one's upgrade commit and has been maintained more actively, so it has
usually already hit and solved the same problem. This is the single highest-value
habit for this branch — it has directly supplied the fix for R parsing, the R
Spark connection, nested-stage loading in R, the generated-wrapper `super()`
bug, the stale `__init__.py` shims, the local setup skill, the notebook runtime,
and a failing training test.

```bash
git diff spark4.0 spark4.1 -- <path>
```

Two cautions when porting from `spark4.1`:

- Substitute this branch's versions. `spark4.1` is Spark 4.1.1 / Scala 2.13.17 /
  Python 3.13; blindly copying its version strings breaks the build here.
- Confirm the fix is not 4.1-specific. See "Do not port from spark4.1" below.

## Why things differ from master

### Toolchain and dependencies

`environment.yml` upgrades pins that predate Python 3.12: `pip` (master's 21.3
cannot install for 3.12), `pyarrow` 18.0.0 (older releases have no cp312 wheels
and would build from source; 18 also stays under mlflow's `pyarrow<20` bound),
`torch`/`torchvision` (first releases supporting 3.12), and cp312 wheel URLs for
`pandas` and `horovod`. Each pin carries a comment explaining it — keep those
comments, they are what stops a future sync from "restoring" master's value.

`tools/docker/*/Dockerfile` set `JAVA_HOME` to Java 17.
`.github/workflows/pr-validation.yml` uses JDK 17.

`pipeline.yaml` drops master's `-XX:+UseConcMarkSweepGC
-XX:+CMSClassUnloadingEnabled` from `SBT_OPTS`. CMS was removed in Java 17 and
the JVM refuses to start with those flags.

### Scala 2.13

Scala 2.13 changed how `Seq` is interpreted. Code that produced a
`mutable.ArraySeq` where an `immutable.Seq` is expected throws
`ClassCastException` at runtime, not compile time. `CognitiveServiceBase.getValueOpt`
converts centrally via `asImmutableCollection` (using `toIndexedSeq`, which keeps
O(1) indexing) rather than patching each of the affected services individually.

### Spark 4.0 behaviour changes

- **SAR** (`SAR.scala`, `SARModel.scala`): Spark 4 rejects the previous
  `Seq[Row]` UDF shape with an `UnboundRowEncoder` error, so the affinity pairs
  use a named `case class` with explicit struct fields. Separately, a self-join
  now trips `DetectAmbiguousSelfJoin`, so the join column is qualified
  (`col("sarUserFactors.flatList")`).
- **`Wrappable.safeGetDefault`**: Spark 4's `getDefault` throws where Spark 3
  returned a default, so lookups go through a guarded helper.
- **`VerifyTrainClassifier`**: the vector-column fixture no longer feeds
  `Double.NaN` to the trainer. Spark 4 does not tolerate a NaN feature reaching
  logistic regression the way 3.5 did. The test is about training on a vector
  column, not about NaN, so the value was replaced rather than the test weakened.

### Code generation

`OpenAIPrompt` sets `pyInternalWrapper = true` on this branch, so codegen emits
`class _OpenAIPrompt` and a hand-written `OpenAIPrompt.py` supplies the public
name. Any Python emitted into that class must therefore use **zero-argument
`super()`**; a hardcoded `super(OpenAIPrompt, self)` raises `NameError` because
that name does not exist inside the generated module. See
`OpenAIPromptPythonOverrides.scala`.

### Hand-written `__init__.py` files

`PythonInitMerger` arrived from master and **preserves** hand-written
`__init__.py` content by splicing it *after* the generated imports. Previously
codegen overwrote these files, so their contents were inert. They are now live
code in the shipped package, and a stale one is a real bug.

Current policy on this branch:

| Path | State | Why |
| --- | --- | --- |
| `core/.../io/http/__init__.py` | **must stay empty** | It listed `HTTPFunctions` and `ServingFunctions`, which are modules of free functions with no same-named class. The import failed, breaking `PythonTests core` and seven website-sample docs. |
| `vw/`, `services/openai/` | **removed** | Duplicated what codegen already emits, and redefined `__all__`, narrowing `import *` to a hand-maintained list. |
| `recommendation/`, `dl/`, `hf/`, `cognitive/`, `mmlspark/` | kept | These add exports codegen does not emit. |

Do not add new `__init__.py` files that re-list generated classes.
`test_http_package.py` and `test_package_exports.py` guard this.

### R tests

Several things must all hold or the R suites fail in ways that point at the wrong
culprit. `RCodegenSuite.scala` asserts the cheap ones, so a unit test catches
them instead of a 90-minute pipeline run.

**sparklyr must be 1.9.5, not 1.9.3.** This is the one that actually broke
`RTests core` (21 of 69) and `RTests deep-learning` (3 of 3). Under dbplyr 2.6,
sparklyr 1.9.3's `tidyselect_data_proxy.tbl_spark` yields a proxy carrying no
Spark connection, so operations that go through `dplyr::select` on a `tbl_spark`
lose `sc`. The failure surfaces one or two layers away as `invoke_static` or
`hive_context` applied to `NULL` — which reads as "the session died" even though
the session is alive and other tests keep passing around it. Read the *backtrace*,
not the message: the tell is `dbplyr:::select.tbl_lazy` →
`sparklyr:::tidyselect_data_proxy.tbl_spark` → `simulate_vars_spark`.

Interleaving is the giveaway. A dead session fails everything after a point;
this failed 21 tests scattered among 48 passes, with `sar` passing while
`sar_model` failed. `spark4.1` pins the same `r-base=4.4` with `r-sparklyr=1.9.5`
and passes 69/69.

**ANSI double-quoted identifiers.** `RTestGen.scala` sets
`spark.sql.ansi.enabled=true` and `spark.sql.ansi.doubleQuotedIdentifiers=true`.
sparklyr emits `SELECT 0L AS "class", ...`; without the second flag Spark 4 reads
`"class"` as a string literal and fails with `PARSE_SYNTAX_ERROR`.

**Connect via `SPARK_HOME`.** `RTestGen.scala` generates
`spark_connect(master = "local", spark_home = Sys.getenv("SPARK_HOME"), ...)`,
matching `spark4.1` byte for byte. The pipeline exports `SPARK_HOME` (the
`find ... -name 'spark-*-bin-hadoop*'` line), so `run_r_tests.R` only unsets it
and installs the tarball when it is absent, which is the local-developer path.

To be accurate about what this bought: the previous `version = "4.0"` form also
worked, because `run_r_tests.R` had already installed the tarball and sparklyr
resolves an install it made itself. Measured — the R results were identical
before and after this change. Keep it for the byte-identical alignment with 4.1
and for skipping a redundant install, but do not expect it to fix a failure.
This branch additionally infers `JAVA_HOME` from `PATH` when unset; `spark4.1`
has no such block, and it is inert under CI.

**Load nested stages off the JVM.** `PipelineStageWrappable.rLoadLine` emits
`sparklyr:::new_ml_pipeline_stage(invoke(spark_jobj(x), "getStages")[[1]])`
rather than `ml_stages(x)[[1]]`, matching `spark4.1`. `new_ml_pipeline_stage` is
sparklyr-internal but has an identical signature in every release from v1.8.0 to
v1.9.5. `EstimatorParam`, `ModelParam`, `PipelineStageParam` and
`TransformerParam` all inherit this one implementation — do not reintroduce
per-class overrides, and keep the three `rLoadLine` assertions in
`VerifyModelParam`/`VerifyPipelineStageParams` in step with it.

Equally, be accurate here: this line is *unproven* on this branch. While sparklyr
was 1.9.3 the tests died on the preceding `ml_load` call and never reached it, so
both `ml_stages` and this form produce the same result. It is kept because 4.1
adopted it deliberately and passes with it.

### Databricks

The GPU pool is `synapseml-build-14.3-gpu`, **shared with `master` and
`spark4.1`**. Instance pools are runtime-agnostic, so sharing avoids duplicating
scarce GPU quota — but the pool holds three workers, so two builds running
concurrently can exhaust it. Prefer queueing Spark 4 branch builds sequentially.

The GPU pool is `synapseml-build-14.3-gpu`, **shared with `master` and
`spark4.1`**. Instance pools are runtime-agnostic, so sharing avoids duplicating
scarce GPU quota — but the pool holds three workers, so two builds running
concurrently can exhaust it. Prefer queueing Spark 4 branch builds sequentially.

**Read `areLibrariesInstalled == false` carefully — it is not a capacity verdict.**
The check throws `Library Installation Failure` with the offending statuses if any
library reports `FAILED`. Returning `false` therefore means the opposite: nothing
failed, the libraries simply had not all reached `INSTALLED` before the retry
budget ran out (`60 * 10` attempts at 1s ≈ 10 minutes). It is a *timeout*, and a
slow install reads exactly like a starved pool.

Do not assume capacity. Compare branches instead — the pool is shared, so a
concurrent run on `spark4.1` is a free control. When this last came up,
`Databricks GPU E2E` had failed 3/3 on this branch and succeeded 3/3 on
`spark4.1`, twice at the same moment on the same pool. An outcome that tracks the
branch rather than the timing is a code difference, not contention.

That instance was a stale pin: `torchvision==0.17.0` in `GPULibraries`, which
`spark4.1` had already dropped. torchvision 0.17.0 hard-requires `torch==2.2.0`,
so pip had to *downgrade* the runtime's much newer torch and pull multi-gigabyte
CUDA wheels — slow enough to exhaust the budget, but never `FAILED`. The GPU ML
runtime already ships torch and torchvision, so pinning bought nothing. Prefer
leaving deep-learning packages to the runtime unless a notebook genuinely needs
a specific version.

`DatabricksCPUStreamingTests` is recorded as unscheduled rather than given a CI
leg; it needs both pool capacity and a notebook fix.

### Fabric E2E is disabled

`FabricE2E` is `condition: false`. Fabric's managed runtime has no Spark 4.0
option — Fabric Runtime 2.0 went to general availability on Spark **4.1**, so
there is no runtime to target from this branch. This is real lost coverage, not
a cosmetic skip. It should stay disabled here until a Spark 4.0-capable Fabric
runtime exists, which may never happen; the more likely resolution is that this
branch is superseded by `spark4.1`.

## Do not port from spark4.1

- **`LongOffset` import** — Spark 4.1 moved it to
  `org.apache.spark.sql.execution.streaming.runtime`. On Spark 4.0 it is still in
  `...streaming`, and adopting 4.1's import does not compile.
- **`ImageTransformer.toNDArray` using `np.frombuffer`** — Spark 4.1 returns
  `bytes` for a `BinaryType` column, which `np.asarray` rejects with a
  `ValueError`. Spark 4.0.1 returns `bytearray`, which `np.asarray` handles.
  Measured on both. The change is harmless here but guards a type this branch
  does not produce.
- **petastorm / horovod cloudpickle shims** (`_horovod.py`,
  `_petastorm_compat.py`) — these work around Python 3.13 breakage. This branch
  is on 3.12 and its deep-learning tests pass without them.
- **`numpy` left unpinned** — 4.1 must, because 1.26.4 has no 3.13 wheels. Here
  1.26.4 both has cp312 wheels and stays below the NumPy 2.0 ABI break that
  `pandas` 2.0.3 cannot tolerate, so it is pinned deliberately.
- **Version strings generally** — Spark 4.1.1, Scala 2.13.17, Python 3.13,
  Databricks 18.0.

`cyber/utils/spark_utils.py` is a genuine option rather than a hazard: 4.1 uses
`spark.createDataFrame(rdd, schema)` where this branch uses `rdd.toDF(schema)`.
`toDF` was measured to work on both 4.0.1 and 4.1.1, so nothing is broken here —
adopting 4.1's form only reduces reliance on the monkey-patched RDD API, which
does not exist under Spark Connect.

## Known non-code failures

- `RTests vw` can fail on a conda `HTTP 403` fetching packages. Infrastructure.
- `UnitTests onnx` has intermittently hit `OutOfMemoryError` in
  `ImageFeaturizerSuite`. Re-run before treating it as a regression.
- Databricks library-install timeouts — read the section above before blaming
  the pool; `areLibrariesInstalled == false` is a timeout, not a failure.

## CI

`/azp run` does **not** trigger for this branch. The Azure DevOps definition's
pull-request trigger is defined in the UI with a `+master` branch filter, so the
`pr:` block in `pipeline.yaml` is never consulted. Until that filter is widened,
queue a build directly against the PR merge ref (`refs/pull/<N>/merge`); a manual
queue bypasses trigger filters. `refs/heads/<branch>` does not work — it fails
service-connection authorization.

GitHub Actions checks do run here, but they only compile and lint. They cannot
catch the failures this branch is actually prone to, all of which need the full
Azure DevOps run.

## Before merging a sync from master

1. Confirm no master content was dropped — compare content, not just commit
   reachability (see AGENTS.md).
2. Re-check every item above still holds; a sync can quietly revert a pin or a
   guarded call.
3. Run the full Azure DevOps pipeline, alone rather than alongside another Spark
   4 branch build.
4. Diff against `spark4.1` and account for each difference as intended or
   missing.
