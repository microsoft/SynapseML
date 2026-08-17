# `spark4.0`

Read [spark4-common.md](spark4-common.md) first. This is a condensed,
templatized version of the branch context from
[#2646](https://github.com/microsoft/SynapseML/pull/2646).

## Purpose and baseline

- Shared Spark 4.0 port. At the #2646 snapshot it used Spark 4.0.1,
  Scala 2.13.16, Java 17, Python 3.12, and Databricks 17.3; verify live files.
- Check `spark4.1` before debugging from scratch because it is the more actively
  maintained descendant, then prove any candidate fix is not 4.1-specific.

## Core differences

- Keep NumPy 1.26.4 pinned: Python 3.12 has wheels and pandas 2.0.3 is not
  compatible with the NumPy 2 ABI.
- Do not copy Python 3.13 petastorm/cloudpickle shims without branch evidence.
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
- Avoid pinning runtime-provided torch/torchvision without a demonstrated need;
  incompatible pins can trigger multi-gigabyte CUDA downgrades and timeouts.
- A sub-minute GPU notebook failure occurs during dependency setup, before
  training. Use run timing and stderr rather than attributing it to the model.
- Confirm target-branch automation actually queued; this branch historically
  had no PR checks even when `master` contained corrected filters.

## Do not port from `spark4.1`

- 4.1 `LongOffset` import, BinaryType `np.frombuffer` workaround, Python 3.13
  shims, unpinned NumPy, or version strings.
- Fabric Runtime 2.0 enablement.
- Any runtime/dependency change whose only evidence is a green 4.1 build.
