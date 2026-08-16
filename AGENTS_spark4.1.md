# AGENTS_spark4.1.md

Branch-specific context for `spark4.1`. Read [AGENTS.md](AGENTS.md) first for the
branch model and sync rules that apply everywhere.

## What this branch is

A port of SynapseML to Spark 4.1. It exists so the library can run on runtimes
that have moved past Spark 3.x; it is not a feature branch. Features and fixes
land on `master` and arrive here when `master` is merged in.

| | |
| --- | --- |
| Spark | 4.1.1 |
| Scala | 2.13.17 |
| Java | 17 |
| Python | 3.13 |
| Databricks runtime | `18.0.x-scala2.13`, GPU `18.0.x-gpu-ml-scala2.13` |
| Generated Python | `target/scala-2.13/generated/src/python/` |

This branch is the more actively maintained of the two Spark 4 branches, and it
descends from `spark4.0`'s upgrade commit. In practice that makes it the
reference: when `spark4.0` hits a problem, the fix usually already exists here.

**So when you fix something here, ask whether `spark4.0` needs it too.** Most
fixes on this branch are Spark-4-generic rather than 4.1-specific, and the
back-port is normally the same patch with version strings substituted. The
exceptions are listed under "Do not port to spark4.0".

```bash
git diff spark4.0 spark4.1 -- <path>
```

## Why things differ from master

### Toolchain and dependencies

`environment.yml` targets Python 3.13, which forces several changes away from
master's pins:

- **`numpy` is intentionally left unpinned.** Master pins `numpy==1.26.4`, which
  has no Python 3.13 wheels. The comment above it says so — keep the comment;
  it is what stops a future sync from "restoring" master's pin.
- `pip`, `pyarrow`, `torch`/`torchvision` and the `pandas`/`horovod` wheel URLs
  are moved forward to releases that publish cp313 artifacts.

Each pin carries a comment explaining it. Preserve those comments through syncs.

`tools/docker/*/Dockerfile` set `JAVA_HOME` to Java 17.
`.github/workflows/pr-validation.yml` uses JDK 17.

`pipeline.yaml` drops master's `-XX:+UseConcMarkSweepGC
-XX:+CMSClassUnloadingEnabled` from `SBT_OPTS`. CMS was removed in Java 17 and
the JVM refuses to start with those flags.

### Python 3.13 runtime shims

`deep-learning/.../dl/_petastorm_compat.py` and the
`_serialize_petastorm_compatibility()` path in `_horovod.py` work around
cloudpickle/petastorm breakage under Python 3.13. These exist **only** because of
the interpreter version — see "Do not port to spark4.0".

### Scala 2.13

Scala 2.13 changed how `Seq` is interpreted. Code that produced a
`mutable.ArraySeq` where an `immutable.Seq` is expected throws
`ClassCastException` at runtime, not compile time. `CognitiveServiceBase.getValueOpt`
converts centrally via `asImmutableCollection` (using `toIndexedSeq`, which keeps
O(1) indexing) rather than patching each affected service individually.

### Spark 4 behaviour changes

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

### Spark 4.1 specifically

`LongOffset` moved to `org.apache.spark.sql.execution.streaming.runtime`.
`HTTPSource.scala` and `DistributedHTTPSource.scala` import it from there. This
is the one import that is genuinely 4.1-only — on Spark 4.0 it is still in
`...streaming` and this import does not compile.

A `BinaryType` column also returns a different Python type. Measured against real
4.0.1 and 4.1.1 installs:

| | Spark 4.0.1 | Spark 4.1.1 |
| --- | --- | --- |
| Python type from a `BinaryType` column | `bytearray` | `bytes` |
| `np.asarray(value, dtype=np.uint8)` | works | `ValueError` |

`np.asarray` accepts `bytearray` because it exposes the buffer protocol as a
sequence of ints, but treats `bytes` as a scalar string. `ImageTransformer.toNDArray`
therefore uses `np.frombuffer`, which handles both. This is required here and
inert on `spark4.0`.

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

Two changes, both required:

- `RTestGen.scala` sets `spark.sql.ansi.enabled=true` and
  `spark.sql.ansi.doubleQuotedIdentifiers=true`. sparklyr emits
  `SELECT 0L AS "class", ...`; without the second flag Spark 4 reads `"class"`
  as a string literal and fails with `PARSE_SYNTAX_ERROR`.
- The `PipelineStageWrappable` trait generates
  `sparklyr:::new_ml_pipeline_stage(invoke(spark_jobj(x), "getStages")[[1]])`
  instead of `ml_stages(x)[[1]]`. The per-type overrides in `EstimatorParam.scala`,
  `PipelineStageParam.scala` and `TransformerParam.scala` became redundant and
  were removed. `r-sparklyr` is pinned to 1.9.5.

`RCodegenSuite.scala` asserts the generated R directly, so a regression in the
above is caught at unit-test time rather than in the much slower `RTests` leg.
This file does not exist on `spark4.0`.

### Databricks

CPU pool `synapseml-build-18.0`; GPU pool `synapseml-build-14.3-gpu`, which is
**shared with `master` and `spark4.0`**. Instance pools are runtime-agnostic, so
sharing avoids duplicating scarce GPU quota — but the pool holds three workers
(`GpuWorkersPerRun` 1 x `GpuConcurrentRuns` 3), so two builds running
concurrently exhaust it and fail with `areLibrariesInstalled == false`.

Queue Spark 4 branch builds **sequentially**. A Databricks failure during
overlapping builds is usually capacity, not code — confirm by re-running alone
before investigating.

`DatabricksCPUStreamingTests` is recorded as unscheduled rather than given a CI
leg; it needs both pool capacity and a notebook fix.

### Fabric E2E is disabled — and the reason is now out of date

`FabricE2E` is `condition: false`, with a comment saying Fabric's managed runtime
"does not yet support Spark 4.1 binaries". **That is no longer true.** Fabric
Runtime 2.0 reached general availability on Apache Spark **4.1** (Scala 2.13,
Python 3.13, Java 21, Delta 4.x). This branch is therefore the one Spark 4 branch
that Fabric can actually host, and the disabled job is real lost coverage.

Re-enabling is not a one-line change. It needs:

1. `core/src/test/scala/.../fabric/FabricOperations.scala` — the workspace
   creation payload hardcodes `'SparkVersion': '3.5'`. It must request `'4.1'`.
   This is hardcoded on all three branches, so master is unaffected by changing
   it here.
2. `pipeline.yaml` — restore
   `condition: and(succeeded(), eq('${{ parameters.testFabricE2E }}', true))`
   and drop the stale comment.
3. A Fabric capacity in the `sempy-integration-region` that can provision
   Runtime 2.0 workspaces.

Step 3 cannot be verified from a development machine — it needs live Fabric
capacity and the `SynapseML Build` service connection. Do this as its own PR
where the pipeline run *is* the test, not as part of a sync PR, so that a Fabric
provisioning failure does not block an unrelated merge.

Note that the equivalent section in `AGENTS_spark4.0.md` reaches the opposite
conclusion, correctly: there is no Fabric runtime on Spark 4.0, so it stays
disabled there.

## Do not port to spark4.0

- **`LongOffset` import** — 4.0 still has it in `...streaming`; 4.1's import does
  not compile there.
- **`ImageTransformer.toNDArray` using `np.frombuffer`** — guards against a
  `bytes` value that Spark 4.0 does not produce; see the table above.
- **petastorm / horovod cloudpickle shims** — Python 3.13 workarounds.
  `spark4.0` is on 3.12 and its deep-learning tests pass without them.
- **`numpy` left unpinned** — on `spark4.0`, `numpy==1.26.4` both has cp312
  wheels and stays below the NumPy 2.0 ABI break that `pandas` 2.0.3 cannot
  tolerate, so it is pinned there deliberately.
- **Version strings generally** — Spark 4.1.1, Scala 2.13.17, Python 3.13,
  Databricks 18.0, sparklyr 1.9.5.

Everything else on this branch is a candidate for back-porting.

One item is worth naming explicitly because it looks 4.1-specific and is not:
`cyber/utils/spark_utils.py` uses `spark.createDataFrame(rdd, schema)` where
`spark4.0` still uses `rdd.toDF(schema)`. `toDF` was measured to work on **both**
4.0.1 and 4.1.1, so this was never a 4.1 necessity — it reduces reliance on the
monkey-patched RDD API, which does not exist under Spark Connect. Back-porting it
is safe but buys little on its own, since the surrounding
`df.rdd.zipWithIndex()` is still an RDD call.

## Known non-code failures

- `UnitTests onnx` intermittently hits `OutOfMemoryError` in
  `ImageFeaturizerSuite`. It has passed on re-run with no code change; re-run
  before treating it as a regression.
- `RTests vw` can fail on a conda `HTTP 403` fetching packages. Infrastructure.
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
4. Diff against `spark4.0` and account for each difference as intended or
   missing — in both directions.
