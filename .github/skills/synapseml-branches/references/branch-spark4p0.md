# `spark4.0`

Read [branch-spark4-common.md](branch-spark4-common.md) first. This is a condensed,
templatized version of the branch context from
[#2646](https://github.com/microsoft/SynapseML/pull/2646).

## Purpose and baseline

- Shared Spark 4.0 port. At the #2646 snapshot it used Spark 4.0.1,
  Scala 2.13.16, Java 17, Python 3.12, and Databricks 17.3; verify live files.
  The Databricks runtime strings are `17.3.x-scala2.13` and GPU
  `17.3.x-gpu-ml-scala2.13`. DBR 17.3 LTS ML ships Spark 4.0 and 18.0 ML ships
  Spark 4.1, which is why the runtime version is not a free knob here.
- Check `spark4.1` before debugging from scratch because it is the more actively
  maintained descendant, then prove any candidate fix is not 4.1-specific.
- Live state lags the #2646 description. In `DatabricksUtilities.scala` the live
  branch pairs its own GPU pool with a matching runtime --
  `GpuPoolName = "synapseml-build-17.3-gpu"` with
  `AdbGpuRuntime = "17.3.x-gpu-ml-scala2.13"`, resolved by `getPoolIdByName` --
  and does not yet carry `OpenAIPromptPythonOverrides.scala`, the `__init__.py`
  guard tests, or the `new_ml_pipeline_stage` R loading. #2646 brings those, and
  also moves the GPU pool to master's shared `synapseml-build-14.3-gpu`
  (resolved by `getPoolIdByNameAndNodeType`, which takes a node type and minimum
  capacity) while keeping the 17.3 CPU pool and the 17.3.x GPU runtime. That
  mixed pairing is not an oversight: `spark4.1` already runs master's
  `14.3-gpu` pool against an `18.0.x-gpu-ml-scala2.13` runtime, so the pool name
  identifies a warm node pool rather than a DBR version. Check the live branch
  before assuming any of it.

## Core differences

- NumPy is currently unpinned here, as it is on 4.1. If you reintroduce a pin,
  pin for a reason you can state: Python 3.12 has NumPy 1.26.4 wheels, and
  pandas 2.0.3 is not compatible with the NumPy 2 ABI, so a pin is warranted
  only while something in the resolved set actually requires it.
- This branch has `_horovod.py` but not `_petastorm_compat.py`, so it uses the
  plain Horovod `SparkBackend` rather than 4.1's Petastorm-compatible subclass.
  That gap is real rather than version-driven: the shim restores pyarrow APIs
  Petastorm still calls, and this branch currently resolves to a *newer* pyarrow
  than 4.1 does, so those APIs are at least as absent here. Confirm both pins
  from `environment.yml` on each branch before reasoning about it. See
  [branch-spark4-common.md](branch-spark4-common.md).
- `LongOffset` remains under `...execution.streaming`, not `.runtime`.
- Spark 4.0 returns `bytearray` for Python `BinaryType`; it does not require the
  4.1 `np.frombuffer` workaround.
- Preserve the Spark 4 R fixes shared with 4.1. The branch-local `JAVA_HOME`
  fallback is extra; nested-stage loading was alignment, not proven root cause.
- **sparklyr must be 1.9.5, not 1.9.3** (`r-sparklyr=1.9.5` in `environment.yml`),
  and on the live branch it is still
  1.9.3 — so this is a current failure, not history. Under dbplyr 2.6, sparklyr
  1.9.3's `tidyselect_data_proxy.tbl_spark` returns a proxy carrying no Spark
  connection, so anything routed through `dplyr::select` on a `tbl_spark` loses
  `sc`. It broke `RTests core` (21 of 69) and `RTests deep-learning` (3 of 3),
  and it surfaces one or two layers away as `invoke_static` or `hive_context`
  applied to `NULL`, which reads like a dead Spark session. Interleaving is the
  tell: a dead session fails everything after a point, whereas this failed 21
  tests scattered among 48 passes, with `sar` passing while `sar_model` failed.
  Read the backtrace, not the surface error. `spark4.1` pairs the same
  `r-base=4.4` with 1.9.5 and passes 69/69.
- R connects through `SPARK_HOME`: `RTestGen.scala` generates
  `spark_connect(master = "local", spark_home = Sys.getenv("SPARK_HOME"), ...)`,
  byte-identical to `spark4.1`. The pipeline exports `SPARK_HOME`, so
  `run_r_tests.R` only unsets it and installs the tarball when it is absent,
  which is the local-developer path. Be accurate about what that bought: the
  previous `version = "4.0"` form also worked, because `run_r_tests.R` had
  already installed the tarball and sparklyr resolves an install it made itself.
  Measured R results were identical before and after. It is kept for
  byte-identical alignment with 4.1, not because it fixed anything.

## Runtime and CI

- Fabric E2E remains disabled because Fabric has no managed Spark 4.0 runtime —
  Fabric Runtime 2.0 went GA on Spark 4.1. This is real lost coverage rather
  than a cosmetic skip, and it should stay disabled here until a Spark
  4.0-capable Fabric runtime exists, which may never happen; the more likely
  resolution is that this branch is superseded by `spark4.1`.
- At #2646, two GPU fine-tune notebooks failed because no Horovod wheel matched
  DBR 17.3's PyTorch. The wheel this branch needs is one built against DBR 17.3
  ML's PyTorch, and no such wheel is published — the `synapse-extension` wheel is
  built for 18.0 ML. Producing it is a build-artifact task rather than a code
  change, which is why this is recorded rather than patched around. `spark4.1`
  additionally calls `ensure_petastorm_compatibility()` *before* `import horovod`
  in `_horovod.py`; that is a plausible second contributor but it is unproven, so
  do not quote it as the cause without the notebook's stderr from the Databricks
  run API. Do not switch `AdbGpuRuntime` to DBR 18 merely to turn them green;
  that would test Spark 4.1 instead of this branch and make the suite green by no
  longer testing what it exists to test. Revalidate this known gap.
- Two of four GPU notebooks failing is that gap's expected shape. Check the
  failing count and which notebooks, not the job's red/green, before calling it
  a regression. Note the denominator moves: live `spark4.0` has three GPU
  notebooks and #2646 brings master's fourth
  (`Quickstart - End-to-end Local RAG with Phi Model`), because the sync also
  adopts master's consolidated `DatabricksGPUTests`, which runs the whole
  `GPUNotebooks` set instead of three hardcoded indices. The Horovod wheel is
  the first blocker and it masks the missing Petastorm layer noted above, so
  fixing the wheel alone should not be expected to turn these notebooks green.
  Confirm each step from the notebook's stderr output rather than inferring it.
- Avoid pinning runtime-provided torch/torchvision without a demonstrated need;
  incompatible pins can trigger multi-gigabyte CUDA downgrades and timeouts. The
  recorded instance was `torchvision==0.17.0` in `GPULibraries`, which
  hard-requires `torch==2.2.0`: pip had to *downgrade* the runtime's much newer
  torch and pull large CUDA wheels, slow enough to exhaust the install budget but
  never reporting `FAILED`. The GPU ML runtime already ships both, so the pin
  bought nothing.
- When the fine-tune notebooks were investigated, the notebooks and
  `GPULibraries` were byte-identical to `spark4.1` and still failed here while
  passing there, and swapping in `spark4.1`'s sha256-pinned wheel changed nothing
  measurable (71.6s to 60.6s, 56.2s to 47.0s — the same failure in the same
  window). Treat that swap as alignment with the working branch, not a fix.
- A sub-minute GPU notebook failure occurs during dependency setup, before
  training. Use run timing and stderr rather than attributing it to the model.
- Confirm target-branch automation actually queued rather than assuming the
  comment was enough; see
  [branch-spark4-common.md](branch-spark4-common.md) for how to tell a
  trigger-driven build from a hand-queued one.

## Do not port from `spark4.1`

- 4.1 `LongOffset` import, BinaryType `np.frombuffer` workaround, Python 3.13
  wheels, or version strings.
- Fabric Runtime 2.0 enablement.
- Any runtime/dependency change whose only evidence is a green 4.1 build.
- The Petastorm compatibility layer is not on this list. It is a back-port
  candidate, not a 4.1-only change, but it needs validation on real 4.0 rather
  than adoption on suspicion.
