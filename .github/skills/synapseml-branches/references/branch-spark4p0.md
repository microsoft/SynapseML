# `spark4.0`

Read [branch-spark4-common.md](branch-spark4-common.md) first. This is a condensed,
templatized version of the branch context from
[#2646](https://github.com/microsoft/SynapseML/pull/2646).

## Purpose and baseline

- Shared Spark 4.0 port. At the #2646 snapshot it used Spark 4.0.1,
  Scala 2.13.16, Java 17, Python 3.12, and Databricks 17.3; verify live files.
- Check `spark4.1` before debugging from scratch because it is the more actively
  maintained descendant, then prove any candidate fix is not 4.1-specific.

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

- Fabric E2E remains disabled because Fabric has no managed Spark 4.0 runtime.
- At #2646, two GPU fine-tune notebooks failed because no Horovod wheel matched
  DBR 17.3's PyTorch. Do not switch to DBR 18 merely to turn them green; that
  would test Spark 4.1 instead of this branch. Revalidate this known gap.
- Two of four GPU notebooks failing is that gap's expected shape. Check the
  failing count and which notebooks, not the job's red/green, before calling it
  a regression. The Horovod wheel is the first blocker and it masks the missing
  Petastorm layer noted above, so fixing the wheel alone should not be expected
  to turn these notebooks green. Confirm each step from the notebook's stderr
  output rather than inferring it.
- Avoid pinning runtime-provided torch/torchvision without a demonstrated need;
  incompatible pins can trigger multi-gigabyte CUDA downgrades and timeouts.
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
