# `spark4.0`

Read [branch-spark4-common.md](branch-spark4-common.md) first. This is a condensed,
version of the branch context from
[#2646](https://github.com/microsoft/SynapseML/pull/2646), updated against target
`ecec8dd58b` after [#2661](https://github.com/microsoft/SynapseML/pull/2661).

## Purpose and baseline

- Shared Spark 4.0 port. At the #2646 snapshot it used Spark 4.0.1,
  Scala 2.13.16, Java 17, Python 3.12, and Databricks 17.3; verify live files.
  The Databricks runtime strings are `17.3.x-scala2.13` and GPU
  `17.3.x-gpu-ml-scala2.13`. DBR 17.3 LTS ML ships Spark 4.0 and 18.0 ML ships
  Spark 4.1, which is why the runtime version is not a free knob here.
- Check `spark4.1` before debugging from scratch because it is the more actively
  maintained descendant, then prove any candidate fix is not 4.1-specific.
- `DatabricksUtilities.scala` now pairs CPU pool `synapseml-build-17.3` and
  GPU pool `synapseml-build-14.3-gpu` with the 17.3 runtimes. The GPU pool is
  shared, not a runtime selector. The target already contains
  `OpenAIPromptPythonOverrides.scala`, the package-export guard tests,
  `RCodegenSuite`, and `new_ml_pipeline_stage` loading.

## Core differences

- NumPy is pinned to 1.26.4, unlike 4.1. Python 3.12 has compatible wheels,
  and the pin avoids the NumPy 2 ABI break with the branch's pandas package.
- Both `_petastorm_compat.py` and `_horovod.py`'s serialized compatibility path
  are present. Preserve both. `environment.yml` now pairs PyArrow 18.0.0 with
  MLflow 2.21.3, the same versions as 4.1; older notes describing PyArrow 22,
  MLflow 1.26.1, or a missing shim are obsolete.
- `LongOffset` remains under `...execution.streaming`, not `.runtime`.
- Spark 4.0 returns `bytearray` for Python `BinaryType`; it does not require the
  4.1 `np.frombuffer` workaround.
- Preserve the Spark 4 R fixes shared with 4.1. The branch-local `JAVA_HOME`
  fallback is extra; nested-stage loading was alignment, not proven root cause.
- **Keep sparklyr 1.9.5 with r-base 4.4**, as pinned in `environment.yml`.
  The following explains the earlier upgrade, not a current failure. Under dbplyr 2.6, sparklyr
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
  ML's PyTorch. That investigation found only an 18.0 ML wheel, but wheel
  availability must be rechecked rather than treated as a permanent fact.
  Both ports now call `ensure_petastorm_compatibility()` before importing
  Horovod. Its presence does not prove native compatibility. Read the current
  notebook's stderr from the Databricks run API.
  Do not switch `AdbGpuRuntime` to DBR 18 merely to turn them green;
  that would test Spark 4.1 instead of this branch and make the suite green by no
  longer testing what it exists to test. Revalidate this known gap.
- Two of four GPU notebooks failing is that gap's expected shape. Check the
  failing count and which notebooks, not the job's red/green, before calling it
  a regression. The fourth notebook,
  `Quickstart - End-to-end Local RAG with Phi Model`, and the consolidated
  `DatabricksGPUTests` are already on the target. Confirm the selected notebook
  count and each failure from current stderr; the old two-of-four result is not
  an exemption from validating a new head.
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
- The Petastorm compatibility layer is shared and already present. Keep it
  during syncs and validate it on real 4.0 rather than inferring success from 4.1.
