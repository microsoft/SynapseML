# Round 5: test coverage

**Verdict: ISSUES_FOUND. One medium-severity, high-confidence coverage gap.**

Reviewed the uncommitted production changes and all four new test files against
`dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24`, targeting `master`.
Static review only; no code edits, builds, test execution, or delegation.
Below, production paths are relative to `lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\`;
suite paths are relative to `lightgbm\src\test\scala\com\microsoft\azure\synapse\ml\lightgbm\split1\`.

## Finding: the replacement row-count path has no AQE regression test

- **Location:** `LightGBMBase.scala:676-683`, `calculateRowStatistics`.
- The new full-Row Dataset `mapPartitions` replaces `ClusterUtil.getNumRowsPerPartition`.
  `core\src\test\scala\com\microsoft\azure\synapse\ml\core\utils\VerifyClusterUtil.scala:20-52`
  proves that projecting a counting query can change AQE partition topology, but calls only the old helper.
  It cannot detect a regression in this replacement, even if that existing test passes.
- `StreamingLayoutSuite.scala:43-73` supplies synthetic partition counts; it never executes Spark counting.
  `StreamingFeatureSizeSuite.scala:16-24` forces one partition. `StreamingFeaturePreflightSuite.scala:22-33`
  starts from fixed-partition ranges, and its successful follow-up fits coalesce to two partitions.
  `lightgbm\src\test\python\fabric_streaming_regression.py:129-151,301-318` supplies range-based fixtures
  and a ranker smoke fit, but never asserts AQE coalescing or equality of counting/training partition layouts.
- **Impact:** count-array indices determine executor allocation, row offsets, and ingestion stop positions
  in `BasePartitionTask.scala:91-111` and `StreamingPartitionTask.scala:172-181`.
  Matching total rows or obtaining predictions does not prove that per-partition counts remain aligned.
  This is missing regression evidence, not a claim that the current full-Row implementation is broken.
- **Recommendation:** adapt the existing wide-row aggregation AQE fixture to valid feature vectors and exercise
  the actual count-and-validation routine used by `calculateRowStatistics`, via a package-scoped helper if needed.
  Require a narrowed counting projection to produce a different partition layout, then assert the replacement's
  complete ordered count array equals the full training plan's partition census, not just its sum.
  Keep the fixture uncached so materialization cannot hide the difference; include a public streaming fit on it.

## Other claimed coverage checked

- Local/global slots, unsorted noncontiguous IDs, empty local slots, index bounds, and offsets have direct assertions in `StreamingLayoutSuite.scala:43-73`; the local count reaches native initialization at `dataset\ReferenceDatasetUtils.scala:124-130`.
- Dense/sparse training and validation widths 0, 1, and 3 against width 2 are covered in `StreamingFeatureSizeSuite.scala:47-62`; opposite representations and positions 7/8/15/16/17 are also checked.
- `StreamingFeatureSizeSuite.scala:82-104` checks sample counters remain unchanged after rejection. Its boundary-position fits reject during preflight, not inside micro-batch loaders; loader checks precede feature/metadata writes by source inspection, not direct buffer assertions.
- `StreamingFeaturePreflightSuite.scala:119-136` covers null training/validation vectors and an all-valid-width input conflicting only with a reused reference, followed by successful reuse.
- `StreamingFeaturePreflightSuite.scala:56-115` bounds rejection with a 60-second future, checks idle jobs and unchanged validation spools, cancels/joins its worker, and performs valid follow-up fits. The corrected `hasTrainingStarted` assertion and its state test are present; Round 1 is not reopened.
- `StreamingFeatureSizeSuite.scala:107-126` checks estimator/model copy, model UID/schema, finite predictions, and exact save/load equality. The Fabric script additionally checks repeated regression fits, bidirectional prediction equality, executor membership, and native hashes.
- Fabric malformed cases use the regressor; classifier/ranker coverage is valid sparse smoke only. All three share `LightGBMBase`; no separate negative-coverage claim is inferred.
- General native lifecycle failures, unstable inputs, and #2333/#2242 remain outside the claimed fixes and this finding.

## Resolution

Extracted the existing count action unchanged into package-scoped
`DatasetUtils.validatedRowCounts`, used by `calculateRowStatistics`.
Added an uncached wide-vector aggregation fixture with AQE coalescing enabled.
It requires the projected counter to expose fewer partitions, compares every
ordered count against a full-row census, and exercises a public streaming fit.
No RDD API, extra production action, or public signature was added. The AQE
regression passed with all 38 tests in the five selected suites. Scoped core and
LightGBM codegen and generated estimator imports also passed.
