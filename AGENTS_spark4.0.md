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
habit for this branch — it has directly supplied the fix for R parsing, the
generated-wrapper `super()` bug, the stale `__init__.py` shims, the local setup
skill, the notebook runtime, and a failing training test.

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

`RTestGen.scala` sets `spark.sql.ansi.enabled=true` and
`spark.sql.ansi.doubleQuotedIdentifiers=true`. sparklyr emits
`SELECT 0L AS "class", ...`; without the second flag Spark 4 reads `"class"` as
a string literal and fails with `PARSE_SYNTAX_ERROR`.

### Databricks

The GPU pool is `synapseml-build-14.3-gpu`, **shared with `master` and
`spark4.1`**. Instance pools are runtime-agnostic, so sharing avoids duplicating
scarce GPU quota — but the pool holds three workers, so two builds running
concurrently exhaust it and fail with `areLibrariesInstalled == false`. Queue
Spark 4 branch builds **sequentially**; a Databricks failure during overlapping
builds is usually capacity, not code. Confirm by re-running alone before
investigating.

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
- **petastorm / horovod cloudpickle shims** (`_horovod.py`,
  `_petastorm_compat.py`) — these work around Python 3.13 breakage. This branch
  is on 3.12 and its deep-learning tests pass without them.
- **`numpy` left unpinned** — 4.1 must, because 1.26.4 has no 3.13 wheels. Here
  1.26.4 both has cp312 wheels and stays below the NumPy 2.0 ABI break that
  `pandas` 2.0.3 cannot tolerate, so it is pinned deliberately.
- **Version strings generally** — Spark 4.1.1, Scala 2.13.17, Python 3.13,
  Databricks 18.0.

### Unresolved

`spark4.1` replaced `ml_stages(x)[[1]]` with
`sparklyr:::new_ml_pipeline_stage(invoke(spark_jobj(x), "getStages")[[1]])` in
`EstimatorParam.scala`, `PipelineStageParam.scala` and `TransformerParam.scala`,
and removed the now-redundant subclass overrides. This branch still uses
`ml_stages` and pins `r-sparklyr` 1.9.3 against 4.1's 1.9.5.

Whether this branch needs the same change is **not yet established**. Its R tests
previously failed earlier, at the `PARSE_SYNTAX_ERROR` above, so they may never
have exercised this path. If `RTests core` fails on `ml_stages` after that fix,
port the `spark4.1` change — and note that 4.1 also has an `RCodegenSuite.scala`
asserting the generated R, which does not exist here.

## Known non-code failures

- `RTests vw` can fail on a conda `HTTP 403` fetching packages. Infrastructure.
- `UnitTests onnx` has intermittently hit `OutOfMemoryError` in
  `ImageFeaturizerSuite`. Re-run before treating it as a regression.
- Databricks library-install failures during concurrent builds — see above.

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
