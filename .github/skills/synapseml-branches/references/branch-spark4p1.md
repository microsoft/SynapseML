# `spark4.1`

Read [branch-spark4-common.md](branch-spark4-common.md) first. This is a condensed,
templatized version of the branch context from
[#2645](https://github.com/microsoft/SynapseML/pull/2645).

## Purpose and baseline

- Shared Spark 4.1 port and the more actively maintained Spark 4 reference. At
  the #2645 snapshot it used Spark 4.1.1, Scala 2.13.17, Java 17, Python 3.13,
  and Databricks 18.0; verify live files.
- Ask whether every non-4.1-specific fix should be back-ported to `spark4.0`.

## Core differences

- Python 3.13 requires newer wheels and an intentionally unpinned NumPy.
  Preserve explanatory pin comments through syncs.
- The Petastorm/Horovod compatibility layer is separate from that: it restores
  pyarrow APIs Petastorm still calls, which the pinned pyarrow no longer
  provides. Do not describe it as a Python 3.13 workaround, or `spark4.0` looks
  exempt when it pins the same pyarrow version.
- `LongOffset` moved to `...execution.streaming.runtime`; the 4.0 import does
  not compile here.
- Spark 4.1 returns Python `bytes` for `BinaryType`; `ImageTransformer` uses
  `np.frombuffer` because `np.asarray` treats `bytes` as a scalar string.
- `RCodegenSuite` directly guards generated R behavior, including nested-stage
  loading and the Spark 4 ANSI settings.

## Runtime and CI

- Fabric Runtime 2.0 supports Spark 4.1, so the old "unsupported runtime"
  reason for disabling Fabric E2E is stale. Re-enable only in a dedicated PR:
  request Spark 4.1 in workspace creation, restore the pipeline condition, and
  validate with real Fabric capacity/service connection. The workspace-creation
  payload lives in the Fabric test package's `FabricOperations.scala`, which
  hardcodes `'SparkVersion': '3.5'` and must request `'4.1'`. That value is
  hardcoded identically on `master`, `spark4.0` and `spark4.1`, so changing it
  here does not alter master's behaviour. It also needs a Fabric capacity in the
  `sempy-integration-region` that can provision Runtime 2.0 workspaces, which is
  the one prerequisite not discoverable from the code.
- Databricks CPU/GPU validation uses 18.x-era runtimes. Run Spark 4 builds
  sequentially because the GPU pool is shared.
- `DatabricksCPUStreamingTests` was unscheduled pending capacity and notebook
  work; verify rather than silently accepting the omission.
- Master compatibility replay commonly applies release-relevant patches here
  and runs `test:compile`; it is not full branch validation.

## Do not port to `spark4.0`

- 4.1 `LongOffset` import, BinaryType workaround, Python 3.13 shims, unpinned
  NumPy, Fabric 4.1 enablement, or version/runtime strings.
- Treat other changes as back-port candidates and validate them on real 4.0.
