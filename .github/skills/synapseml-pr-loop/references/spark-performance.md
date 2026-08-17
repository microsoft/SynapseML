# Spark and performance review

Apply these checks to every changed execution path.

## Data movement

- Prefer DataFrame/Dataset and Spark SQL expressions.
- Avoid `collect`, `toLocalIterator`, driver-side aggregation, RDD conversion,
  accidental cartesian products, and unbounded arrays/maps.
- Check partition count, repartition/coalesce choices, shuffles, sorts, joins,
  and broadcast size.
- Preserve streaming/lazy execution; do not eagerly scan data merely to
  validate a parameter when schema/metadata can answer it.

## Per-row and per-partition work

- Construct clients, models, sessions, parsers, and native handles once per
  partition or task where safe, not once per row.
- Bound concurrency and queues. Add backpressure, timeouts, cancellation, and
  terminal failure propagation.
- Batch remote/native work when the API supports it.
- Avoid repeated serialization, parsing, schema inference, or metric
  recomputation.

## Memory and resources

- Close streams, sessions, responses, sockets, native handles, and thread pools
  on success and failure.
- Unpersist cached RDD/DataFrame or metric intermediates.
- Use bounded buffers and avoid retaining complete partitions or responses.
- Test cleanup, half-close/cancellation, and retry exhaustion.

## Measurement

Benchmark the old and new head with the same data, cluster/runtime, warm-up, and
iteration count. Report:

- dataset dimensions and partitioning;
- hardware/runtime and dependency/native artifact;
- cold and warm timing;
- throughput or latency distribution;
- memory/spill/shuffle when relevant;
- correctness parity.

Small synthetic tests prove logic, not performance. Use representative scale
and real accelerator/network/service paths for performance claims.
