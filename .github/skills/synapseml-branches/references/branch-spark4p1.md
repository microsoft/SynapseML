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
- SAR uses `Seq[Row]` affinity pairs here. Spark 4.0's `SAR.ItemAffinity`
  encoder workaround is not automatically required on this branch; validate
  schema and recommendation behavior before changing that representation.

## Fabric and validation

- Fabric runtime availability is separate from this branch's support. Check the
  job condition and workspace Spark selection in `FabricOperations.scala`;
  a disabled job or mismatched selection is not Spark 4.1 evidence.
- Re-enablement is a separate, approved change. Restore the appropriate success,
  parameter, and non-fork guards; validate compatible artifacts on the actual
  Spark 4.1 runtime with authorized capacity and service-connection support.
- Check streaming-suite scheduling and shared GPU capacity as described in the
  common rules. Replay compilation is not full Spark 4.1 validation.
