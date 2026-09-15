# Round 1: broad correctness and specification review

**Verdict: needs change.** One P2 finding in the new regression evidence. No high-confidence production correctness defect found in the reviewed diff.

Scope: the uncommitted patch for microsoft/SynapseML#2702 and #2703 against `master` at `dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24`. The checkout HEAD matches that base. Reviewed the checkout's `AGENTS.md`, code-review and Scala skills, master branch guidance, `build.sbt`, and `environment.yml`. This was a direct, static review. No source changes, tests, builds, subagents, staging, or commits.

## Finding

### R1-1, P2: elapsed training time cannot establish that training never started

Location: `lightgbm\src\test\scala\com\microsoft\azure\synapse\ml\lightgbm\split1\StreamingFeaturePreflightSuite.scala:81`.

`assertRejectedBeforeTraining` uses `trainingTime() == 0` to support the requirement that malformed input fails before distributed native preparation. That assertion also passes when a fit enters distributed training and then fails.

The relevant caller and metric implementation are in `lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\`:

- `LightGBMBase.scala:788-793` calls `markTrainingStart()` before launching the partition tasks.
- `LightGBMBase.scala:761-772` calls `markTrainingStop()` only after the training operation returns successfully. An exception from a partition skips that call.
- `LightGBMPerformance.scala:102-103,120-131` initializes the stop timestamp to zero and derives `trainingTime()` from the start and stop timestamps.
- `LightGBMPerformance.scala:81-83` returns zero whenever the stop timestamp is zero, regardless of whether training started.

Consequently, the sequence `markTrainingStart(); fail before markTrainingStop()` produces the same zero duration as genuine preflight rejection. The finished-job check at test lines 76-80 establishes that no job remains active, not that no native-preparation job ran. The timeout and subsequent successful fit remain useful checks, but this duration assertion supplies no independent evidence for the timing requirement.

Fix: observe training entry independently of completion, for example with a read-only training-start indicator or a training-job observer, and assert that it remains unset on preflight rejection. Cover the started-then-failed case so the replacement distinguishes it from never-started training. Preserve the existing elapsed-time API's semantics rather than changing that API merely to satisfy this assertion.

Confidence: high, established directly from the exception control flow and metric implementation. This is a defect in the new test's claimed evidence, not a claim that the production preflight currently runs too late.

## Reviewed changes and correctness evidence

Production Scala paths below are relative to `lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\`. Test suite paths are relative to `lightgbm\src\test\scala\com\microsoft\azure\synapse\ml\lightgbm\split1\`.

| Reviewed file and lines | Assessment |
| --- | --- |
| `BasePartitionTask.scala:81-112` | The external-thread count now comes from the same executor-local ID list that defines `threadIndex`. Noncontiguous global IDs and empty local slots remain represented. Row-count and offset calculations remain unchanged. |
| `LightGBMBase.scala:549-581,661-710` | Training validation consumes the complete iterator with a `Long` count during the existing collection action. The opaque full-Row `mapPartitions` avoids introducing a feature-only projection that could change AQE shuffle sizing. Validation rows are checked before entering `executeTraining`. |
| `StreamingPartitionTask.scala:279-327` | Both dense and CSR copy paths validate declared size and nullness before feature conversion or native-buffer writes. Accepted dense rows write all expected columns, including zeros; sparse vectors retain their declared dimension independently of stored-entry count. |
| `dataset\DatasetUtils.scala:13-31` | Shared guards reject unequal dimensions and null feature values. The iterator wrapper is lazy, consumes each row once when driven by its caller, and returns the original row. |
| `dataset\ReferenceDatasetUtils.scala:103-152,177-201` | Native initialization receives the corrected local count. Cached-reference checking deserializes a temporary one-row Dataset, compares its native feature count with the incoming dimension, and closes it on success or failure while preserving the primary error. The stored byte array and existing ownership-transfer paths are unchanged. Logging is gated by verbosity greater than one and does not include feature values or reference bytes. |
| `dataset\SampledData.scala:42-67` | Dense-array, dense-vector, and sparse-vector sample writes require exact dimensions before modifying sample buffers. A correctly sized sparse vector with no stored entries remains valid. |
| `StreamingLayoutSuite.scala:14-72` | Covers noncontiguous executor-local IDs, empty local slots, offsets, thread-index bounds, and the unchanged one-executor control. These are context-level assertions, not native execution results. |
| `StreamingFeatureSizeSuite.scala:16-131` | Public estimator cases cover short, zero-length, and long vectors in training and validation, cross-format conversion, sample and micro-batch boundaries, plus valid-input copy and model persistence. Sample-buffer checks verify rejection before mutation. |
| `StreamingFeaturePreflightSuite.scala:22-155` | Covers bounded rejection, job cleanup, another fit after rejection, null input, cached-reference dimensional mismatch, and lazy validation. The training-start evidence limitation is R1-1 above. |
| `lightgbm\src\test\python\fabric_streaming_regression.py:21-366` | Reviewed the complete real-runtime script: class-source and native-hash checks, malformed training/validation cases, repeated valid fits, finite predictions, persistence equality, classifier/ranker controls, executor membership checks, and cleanup. Its code is not evidence that the fixed run has passed. |
| `docs\Explore Algorithms\LightGBM\Overview.md:131-150` | Documents strict vector dimensions, cached-reference compatibility, stable input between actions, local thread slots, and the limit on recovery after native work begins. |

Relevant unchanged callers and ownership code were also read directly:

- `NetworkManager.scala:169-209,492-503,673-691` retains both active and load-only partition reports, deduplicates by partition, and groups the complete list by executor. Empty partitions report through the same initialization path in `BasePartitionTask.scala:173-201`.
- `ValidationDataServer.scala:346-367,468-525` completes the ingestion action before returning the validation server. `ValidationDataIngest.scala:23-66` exhausts the checked row iterator, so validation is not merely attached to an unevaluated plan. Failure unwinds existing socket, executor, and spool cleanup before training starts.
- `StreamingPartitionTask.scala:108-138` contains the native-preparation and helper-start boundary. The new driver-side checks precede this boundary for stable input.
- `NetworkManagerSocketSupport.scala:145-170` and `dataset\LightGBMDataset.scala:65-72,213-216` establish temporary-reference cleanup and native feature-count access.
- `LightGBMRanker.scala:88-90` performs partition-local sorting, not an additional repartition in `preprocessData`. `LightGBMBase.scala:153-187,780-795` was checked for preparation and training execution paths.
- `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\ClusterUtil.scala:40-54` and `core\src\test\scala\com\microsoft\azure\synapse\ml\core\utils\VerifyClusterUtil.scala:20-53` explain the existing full-row AQE topology contract. The new LightGBM count path no longer calls that utility, so its existing test alone does not validate the replacement.

The diff retains existing public JVM signatures and parameter definitions. No dependency pins, workflows, generated wrappers, prediction implementations, or serialized parameter shapes change.

## Evidence limits

The supplied baseline reports 11 failing targeted tests and one passing one-executor control; the supplied Fabric baseline accepts four malformed cases while valid controls pass. These are parent-provided observations, not executions performed during this review. Fixed validation remains pending with the parent. No fixed test, Fabric, performance, ABI, or prediction-parity success is inferred.

The full-row counting and ownership assessments above are source-level conclusions, not a substitute for AQE and multi-executor runtime evidence. Generic failure coordination after native preparation remains outside this patch's claim. Stable input between Spark actions remains a prerequisite.

## Resolution of R1-1

Added the package-scoped `InstrumentationMeasures.hasTrainingStarted` observation,
which reads the start timestamp independently of successful completion. The
preflight tests now assert that it is false. A separate regression distinguishes
never-started, started-without-completion, and successfully completed training while
preserving the existing `trainingTime()` semantics. These assertions passed with
all 35 tests in the four selected suites, including bounded public-fit rejection.
