# `spark4.0`

Read [branch-spark4-common.md](branch-spark4-common.md) first. This is a condensed,
templatized version of the branch context from
[#2646](https://github.com/microsoft/SynapseML/pull/2646).

## Purpose and baseline

- Shared Spark 4.0 port. At the #2646 snapshot it used Spark 4.0.1,
  Scala 2.13.16, Java 17, Python 3.12, and Databricks 17.3; verify live files.
- Check `spark4.1` before debugging from scratch because it is the more actively
  maintained descendant, then prove any candidate fix is not 4.1-specific.
- Live state lags the #2646 description. The live branch currently has its own
  GPU pool (`synapseml-build-17.3-gpu`) rather than the shared `14.3-gpu` one,
  and does not yet carry `OpenAIPromptPythonOverrides.scala`, the
  `__init__.py` guard tests, or the `new_ml_pipeline_stage` R loading. #2646
  brings all of those. Check the live branch before assuming any of them.

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

## Runtime and CI

- Fabric E2E remains disabled because Fabric has no managed Spark 4.0 runtime —
  Fabric Runtime 2.0 went GA on Spark 4.1. This is real lost coverage rather
  than a cosmetic skip, and it should stay disabled here until a Spark
  4.0-capable Fabric runtime exists, which may never happen; the more likely
  resolution is that this branch is superseded by `spark4.1`.
- At #2646, two GPU fine-tune notebooks failed because no Horovod wheel matched
  DBR 17.3's PyTorch. Do not switch `AdbGpuRuntime` to DBR 18 merely to turn
  them green; DBR 17.3 LTS ML ships Spark 4.0 and 18.0 ML ships Spark 4.1, so
  bumping it would test Spark 4.1 instead of this branch and make the suite
  green by no longer testing what it exists to test. Revalidate this known gap.
- Two of four GPU notebooks failing is that gap's expected shape. Check the
  failing count and which notebooks, not the job's red/green, before calling it
  a regression. The Horovod wheel is the first blocker and it masks the missing
  Petastorm layer noted above, so fixing the wheel alone should not be expected
  to turn these notebooks green. Confirm each step from the notebook's stderr
  output rather than inferring it.
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
