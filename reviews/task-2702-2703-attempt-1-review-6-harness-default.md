# Round 6: performance, observability, and documentation

Verdict: clean. No high-confidence introduced defects found in the scoped uncommitted changes.

Reviewed production changes, `docs/Explore Algorithms/LightGBM/Overview.md`, and Fabric Python evidence claims.
No source/test edits, builds, tests, or subagents. Round 5 AQE coverage remains with the parent.
Branch context is the master/Spark 3.5 baseline; this is not an overall merge-readiness verdict.

## Findings checked

- `LightGBMBase.scala:675-682` replaces the existing partition-count action, not adds another full-data action.
  Full rows remain available to avoid column pruning changing the adaptive partition topology.
  The prior `ClusterUtil.getNumRowsPerPartition` also consumed full rows.
- Validation checks run in the existing validation-transfer action before the training callback.
  Added per-row checks inspect nullness and declared vector size, without scanning values or adding dense conversions.
  Existing sampling/copy loops remain; no throughput or memory savings have been demonstrated.
- `BasePartitionTask.scala:84-96` derives thread indices, thread count, and row count from the executor-local partition list.
  The count does not filter empty local partitions. This establishes sizing intent, not a measured performance improvement.
- `ReferenceDatasetUtils.scala:117-131` gates the new initialization diagnostic on `verbosity > 1`.
  It reports local partition IDs, rows, and external-thread count once per initialization, not per row.
- `ReferenceDatasetUtils.scala:137-143` adds native deserialization and a feature-count query per supplied/reused reference.
  It uses the existing cleanup-preserving wrapper; deserialization frees temporary native bytes and pointer storage.
  This is additional reference-check overhead, not an extra Spark data scan or a claimed negligible cost.
- `Overview.md:131-150` correctly distinguishes declared sparse size from stored entries and requires stable input across actions.
  Sampling and copy-site size guards are defensive checks, not recovery from arbitrary executor/native failures.
  The documentation does not promise such recovery or equate ingestion thread slots with `numThreads`.

## Evidence limits

Saved Fabric result records confirm all four malformed combinations were accepted on both 2 and 4 executors.
They identify native `lightgbmlib-3.3.510.jar`; `build.sbt` still pins 3.3.510 and has no dependency change.
The Python script distinguishes expected baseline acceptance from fixed rejection and emits timings, not performance conclusions.
Its valid feature vectors have one feature; these results do not establish a fix for #2242's multi-feature sampled-reference failure.
Neither the scoped documentation nor these results establish a fix for #2333 native corruption.
Fixed Fabric execution is pending. No fixed E2E pass, performance savings, or native-corruption resolution is claimed.
Existing compile/style and 35 passing tests are parent-reported evidence, not rerun or evaluated in this round.
