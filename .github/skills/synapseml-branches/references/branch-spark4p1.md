# `spark4.1`

Read [branch-spark4-common.md](branch-spark4-common.md) first. This is a condensed,
templatized version of the branch context from
[#2645](https://github.com/microsoft/SynapseML/pull/2645).

## Purpose and baseline

- Shared Spark 4.1 port and the more actively maintained Spark 4 reference. At
  the #2645 snapshot it used Spark 4.1.1, Scala 2.13.17, Java 17, Python 3.13,
  and Databricks 18.0; verify live files. The Databricks runtime strings are
  `18.0.x-scala2.13` and GPU `18.0.x-gpu-ml-scala2.13`.
- Ask whether every non-4.1-specific fix should be back-ported to `spark4.0`.

## Core differences

- Python 3.13 requires newer wheels and an intentionally unpinned NumPy.
  Preserve explanatory pin comments through syncs.
- The Petastorm/Horovod compatibility layer is separate from that: it restores
  pyarrow APIs Petastorm still calls, which the pinned pyarrow no longer
  provides. Do not describe it as a Python 3.13 workaround — that framing makes
  `spark4.0` look exempt when it is not. `spark4.0` pins a different, newer
  pyarrow, so those APIs are missing there too. The layer has two halves:
  `_petastorm_compat.py` and the `_serialize_petastorm_compatibility()` path in
  `_horovod.py`. A back-port needs both.
- `LongOffset` moved to `...execution.streaming.runtime`; the 4.0 import does
  not compile here. `HTTPSource.scala` and `DistributedHTTPSource.scala` are the
  files that import it, and they are the whole surface of this difference.
- Spark 4.1 returns Python `bytes` for `BinaryType` where 4.0 returns
  `bytearray`; `np.asarray(value, dtype=np.uint8)` raises `ValueError` on `bytes`
  because it treats them as a scalar string, so `ImageTransformer.toNDArray`
  uses `np.frombuffer`, which accepts both.
- `RCodegenSuite` directly guards generated R behavior, including nested-stage
  loading and the Spark 4 ANSI settings.

## Runtime and CI

- Fabric Runtime 2.0 supports Spark 4.1, so the old "unsupported runtime"
  reason for disabling Fabric E2E is stale. On this branch's `pipeline.yaml` the
  job is switched off with a bare `condition: false`, so it is skipped rather
  than reported. Do not check `master` to confirm that, because `master` has
  the normal `and(succeeded(), eq('${{ parameters.testFabricE2E }}', true))` and
  `spark4.0` a third form, `eq('${{ parameters.testFabricE2E }}', true)` with no
  `succeeded()`. Re-enable only in a dedicated PR, where the pipeline run *is*
  the test, rather than folding it into a sync: a Fabric provisioning failure
  would otherwise block an unrelated merge. That PR should restore `master`'s
  form, drop the stale comment, request Spark 4.1 in workspace creation, and
  validate against real Fabric capacity and service connection. The
  workspace-creation payload lives in the Fabric test package's
  `FabricOperations.scala`, which hardcodes `'SparkVersion': '3.5'` and must
  request `'4.1'`. That value is
  hardcoded identically on `master`, `spark4.0` and `spark4.1`, so changing it
  here does not alter master's behaviour. It also needs a Fabric capacity in the
  `sempy-integration-region` that can provision Runtime 2.0 workspaces, and the
  `SynapseML Build` service connection, which are the prerequisites not
  discoverable from the code.
- Databricks CPU/GPU validation uses 18.x-era runtimes: CPU pool
  `synapseml-build-18.0`, GPU pool `synapseml-build-14.3-gpu`. Run Spark 4 builds
  sequentially because the GPU pool is shared with `master` and `spark4.0`.
- `DatabricksCPUStreamingTests` was unscheduled pending capacity and notebook
  work; verify rather than silently accepting the omission.
- Master compatibility replay commonly applies release-relevant patches here
  and runs `test:compile`; it is not full branch validation.

## Do not port to `spark4.0`

- 4.1 `LongOffset` import, BinaryType workaround, Python 3.13 shims, unpinned
  NumPy, Fabric 4.1 enablement, or version/runtime strings.
- Treat other changes as back-port candidates and validate them on real 4.0.
