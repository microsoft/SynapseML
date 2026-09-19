# Spark 4.0 branch context

Use the [shared Spark 4 rules](branch-spark4-common.md) and
[live configuration sources](../SKILL.md#sources-of-truth).
Check Spark 4.1 for portable fixes, then prove they work on this branch.

## Preserve these differences

- `LongOffset` is in `...execution.streaming`, not the Spark 4.1
  `...execution.streaming.runtime` package.
- Python `BinaryType` values are `bytearray`; do not assume Spark 4.1's `bytes`
  behavior when changing image conversion.
- Preserve the NumPy 1 ABI choice and its compatible Python/pandas dependencies
  in `environment.yml`. Do not import Spark 4.1's unpinned NumPy or native wheels
  without checking this branch's ABI and interpreter.
- Keep the explicit `SAR.ItemAffinity` structure and named affinity fields.
  Reintroducing an untyped `Seq[Row]` UDF can cause `UnboundRowEncoder`.
- Preserve the branch's local `JAVA_HOME` fallback and the shared R generation,
  connection, and nested-stage loading rules.

## Runtime boundaries

- Both CPU and GPU Databricks profiles must run Spark 4.0. A newer sibling
  runtime is not a workaround for a failing native library.
- Horovod wheels must match this runtime's Python and PyTorch. A wheel that
  works on Spark 4.1 does not establish compatibility here.
- Fabric has no managed Spark 4.0 runtime. Keep Fabric E2E disabled unless a
  matching supported runtime is established; Spark 4.1 Fabric evidence cannot
  validate this port.

Do not automatically copy sibling runtime strings, interpreter-specific wheels,
`LongOffset` imports, NumPy constraints, or Fabric enablement. Carry portable
fixes and regression tests, not the sibling's environment.
