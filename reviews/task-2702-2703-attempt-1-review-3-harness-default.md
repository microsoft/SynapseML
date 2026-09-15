# Round 3: edge cases, concurrency and native ownership

**Verdict: CLEAN.** No new concrete production bug found in the requested scope.
Reviewed the current diff against `master` at `dd220c9`, AGENTS.md, and nearby failure/ownership paths.
Source-only review. No source edits, tests, builds, or Fabric runs performed.

Source paths below are relative to `lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\`.

## Source evidence

- `dataset\DatasetUtils.scala:13-30` rejects nulls and compares declared vector size, not stored-entry count.
  `StreamingPartitionTask.scala:280-330` checks before conversion or feature/metadata writes in both loaders.
  Shorter and longer dense/sparse vectors fail; correctly sized sparse vectors with empty storage remain valid.
  Dense conversion writes all zeroes; CSR records the unchanged element offset for an empty row.
- `dataset\SampledData.scala:43-65` checks dense and sparse dimensions before mutating sample buffers.
  Row-based null samples reject without dereferencing null. `dataset\ReferenceDatasetUtils.scala:24-46`
  places sample insertion inside cleanup, so these rejections release the allocated sample arrays.
- `LightGBMBase.scala:673-683` consumes every full training row, counts with Long arithmetic, and retains
  zero-count partitions in partition-ID order. Its built-in ranker preprocessing only sorts within partitions
  (`LightGBMRanker.scala:88-90`), preserving these counts and feature dimensions.
- `LightGBMBase.scala:562-607` validates validation rows during transfer, before invoking training.
  `ValidationDataServer.scala:346-368,489-524` completes ingestion before returning a server and cleans up
  failed transfers/spools. These failures precede the shared latches in `StreamingPartitionTask.scala:104-124`.
- `BasePartitionTask.scala:84-110` derives thread indices and allocation count from the same local ID list.
  `NetworkManager.scala:492-502,673-691` includes load-only/empty reports and deduplicates partition IDs.
  Noncontiguous IDs and empty local partitions therefore keep indices within the allocated thread slots.
- `LightGBMBase.scala:694-699` checks supplied/reused reference dimensions before distributed ingestion.
  `dataset\ReferenceDatasetUtils.scala:137-153,176-200` uses a temporary one-row Dataset, frees its native
  byte array and pointer container, and closes the Dataset on both match and mismatch.
  `NetworkManagerSocketSupport.scala:145-170` preserves the primary exception and suppresses cleanup failures.
- `LightGBMPerformance.scala:120-132` exposes training-start state without changing elapsed-time semantics.
  The marker is set at task execution, after preflight (`LightGBMBase.scala:794`).

## Scope limits

Inputs must remain stable across Spark actions. No recovery claim covers cancellation, executor loss,
or native/I/O failures after shared ingestion starts; no new regression justifies rewriting that subsystem.
Validation row counts retain checked Int conversion (`ValidationDataServer.scala:680-685`).
Existing unchecked training row-count/offset narrowing (`BasePartitionTask.scala:92-111`) and Int buffer
products (`StreamingPartitionTask.scala:36-47`) remain unchanged. This patch does not establish overflow safety.
Runtime and E2E evidence remain the parent's responsibility.
