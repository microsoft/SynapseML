# Round 4: line-by-line data-flow review

**Verdict: CLEAN.** No high-confidence production defect introduced by the patch found.

Reviewed all seven changed production Scala files against `dd220c9` for #2702/#2703.
Used rounds 1-3 for established architecture and unchanged lifecycle context without repeating those investigations.
Eight read/search calls; no source edits, tests, builds, or subagents.
All seven source blobs still matched the inspected diff at the final read.

Paths below are relative to `lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\`.

## Changed-operation evidence

- `LightGBMBase.scala:678-683`: `foldLeft(0L)` increments by `1L`, not `Iterator.size`.
  Exhausts every validated row and emits one partition-ID/Long-count pair even for empty partitions.
  Sorting pairs before extracting counts retains partition order; the opaque input remains the full Row, with no feature-only projection.
- `LightGBMBase.scala:562-587,694-698`: validation uses its own schema's feature index and the training dimension.
  Its row encoder retains the complete schema; the checked plan reaches validation ingestion before training.
  Bulk mode returns the original validation data. Cached-reference checking precedes returning the same stored bytes.
- `dataset\DatasetUtils.scala:13-30`: null rejection precedes vector access; declared size must equal the expected size.
  The iterator checks each consumed row once, returns that same Row, and does not mutate vectors or metadata.
- `StreamingPartitionTask.scala:279-329`: both loaders check before conversion and feature/metadata writes.
  Accepted dense rows overwrite every column, including zeros. CSR writes the next pointer at `batchRowCount + 1`;
  an empty sparse row preserves the element offset. Existing row increments, exclusive batch bounds, and native arguments remain intact.
- `dataset\SampledData.scala:43-65`: dense and sparse equality checks precede sample-buffer mutation.
  Dense iteration remains `0 until numCols`; sparse iteration uses stored entries only after checking declared dimension.
- `BasePartitionTask.scala:84-111`; `dataset\ReferenceDatasetUtils.scala:124-130`: allocation count and thread index use the same local ID list.
  Noncontiguous global IDs become zero-based local slots; empty IDs remain allocated, so active indices stay below the count.
  The local count occupies the existing external-thread argument; row offsets and the separate OpenMP-thread argument are unchanged.
- `dataset\ReferenceDatasetUtils.scala:117-143,176-199`: verbosity-gated logging sorts a copy, not shared topology.
  Cached-reference checking uses a temporary one-row Dataset and compares incoming size against native feature count.
  Existing cleanup closes it on success or mismatch; deserialization frees temporary bytes and the pointer container.
- `LightGBMPerformance.scala:102-103,120-132`: the new observer reads the existing Long start timestamp without narrowing or mutation.
  It distinguishes started-but-incomplete training without changing duration semantics or adding stored state.
- The diff changes no existing public JVM signatures, parameter definitions, defaults, or serialized parameter shapes.

This is a static, stable-input verdict, not runtime AQE or native-execution proof.
General lifecycle/root-cause claims beyond stable inputs and pre-existing Int row-offset/buffer limits remain outside scope.
Parent validation remains authoritative; no test or build success is inferred here.
# Follow-up: fixture data flow

The shared group-size constant drives both feature-frame grouping and the
alignment check. Only the ranker disables its redundant grouping shuffle;
regressor/classifier parameters, malformed fixtures, persistence checks,
native hashes, and executor membership checks remain unchanged.
