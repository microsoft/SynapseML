# Spark 4.1 branch context

Use the [shared Spark 4 rules](branch-spark4-common.md) and
[live configuration sources](../SKILL.md#sources-of-truth).
Treat non-version-specific fixes as candidates for master and Spark 4.0.

## Preserve these differences

- `LongOffset` is in `...execution.streaming.runtime`. The Spark 4.0 import
  does not compile here.
- Python `BinaryType` values are `bytes`, not Spark 4.0's `bytearray`.
  Preserve `ImageTransformer.toNDArray`'s `np.frombuffer` handling;
  `np.asarray(..., dtype=np.uint8)` treats `bytes` as a scalar string.
- Use wheels compatible with the branch's Python. Preserve the deliberate
  NumPy policy rather than importing Spark 4.0's pin.
- Petastorm's local and worker-side compatibility paths are shared Spark 4
  requirements, not an interpreter-specific workaround.

## Fabric and validation

- A managed Fabric runtime supports Spark 4.1, but the branch's Fabric E2E job
  is deliberately disabled. Runtime availability is not evidence that CI ran.
- Re-enablement is a separate, approved change. Restore the appropriate success,
  parameter, and non-fork guards; check `FabricOperations.scala`'s workspace
  Spark selection as well as the job condition.
- Verify authorized capacity and service-connection support, then exercise the
  actual workspace runtime. Enabling a job alone can still select another Spark
  version.
- Check streaming-suite scheduling and shared GPU capacity as described in the
  common rules. Replay compilation is not full Spark 4.1 validation.
