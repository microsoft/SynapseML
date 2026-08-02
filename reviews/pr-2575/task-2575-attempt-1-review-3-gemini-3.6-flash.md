## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: sequential
- **Model**: gemini-3.6-flash
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-3-gemini-3.6-flash.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Inspected join behavior in `mergeWithGroups` (`core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala:569`), confirming standard column join drops rows with `NULL` keys when `collapseGroup = false`.
- [x] Inspected session config lookup in `getSchemaFields` (`core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala:367-368`), verifying that `SparkSession.getActiveSession` is queried directly instead of utilizing `dataset.map(_.sparkSession)`.
- [x] Checked `aggregateField` definition (`core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala:140-141`), verifying that `nullable = dataType != VectorType` marks `VectorType` columns as non-nullable despite potential null results from `Summarizer.mean`.
- [x] Reviewed Python wrapper implementation (`core/src/main/python/synapse/ml/stages/EnsembleByKey.py`) and Python unit tests (`core/src/test/python/synapsemltest/stages/test_ensemble_by_key.py`).

## Issues

### Issue 1: `mergeWithGroups` uses standard inner join on grouping keys, dropping rows with `NULL` keys when `collapseGroup = false`
- **Severity**: High
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala
- **Line(s)**: 569
- **Description**: In `mergeWithGroups`, `left.join(aggregated, resolvedColumns.keyFields.indices.map(keyName))` performs an inner join on the generated key names (`keyName`). In Spark SQL, sequence-based column joins create an `EquiJoin` using standard equality (`=`), where `NULL = NULL` evaluates to `NULL` (false). While Spark's `groupBy` groups `NULL` key values together in `aggregated`, the join in `mergeWithGroups` drops all rows whose grouping keys contain `NULL`.
- **Risk**: High. Silent data loss when processing datasets containing `NULL` values in grouping key columns while `collapseGroup = false`. Rows with `NULL` keys are dropped from the output without error or warning.
- **Suggested Fix**: Use null-safe join conditions (`<=>` or `EqualNullSafe`) when joining `left` with `aggregated` in `mergeWithGroups`, constructing explicit join conditions such as `left(keyName) <=> aggregated(keyName)`.

### Issue 2: `getSchemaFields` queries `SparkSession.getActiveSession` instead of the dataset's `SparkSession`, ignoring session SQL configs when active session is absent or different
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala
- **Line(s)**: 367-368
- **Description**: In `getSchemaFields`, `caseSensitive` is evaluated via `SparkSession.getActiveSession.exists(_.conf.get("spark.sql.caseSensitive", "false").trim.toBoolean)`. When `transform(dataset)` invokes `getSchemaFields(dataset.schema, Some(dataset))`, `dataset` is available, but `getSchemaFields` ignores `dataset.sparkSession`. In multi-threaded environments or PySpark worker threads where `SparkSession.getActiveSession` is `None` (or references a default session), `getActiveSession` evaluates to `None` and falls back to `false`, disregarding `dataset.sparkSession.conf.get("spark.sql.caseSensitive")`.
- **Risk**: Medium. Inconsistent case-sensitivity resolution during `transform` on multi-threaded or PySpark pipelines, leading to unexpected column resolution failures or false ambiguity errors.
- **Suggested Fix**: Use the dataset's SparkSession when available:
  ```scala
  val session = dataset.map(_.sparkSession).orElse(SparkSession.getActiveSession)
  val caseSensitive = session.exists(_.conf.get("spark.sql.caseSensitive", "false").trim.toBoolean)
  ```

### Issue 3: `aggregateField` hardcodes `nullable = false` for `VectorType` outputs, creating schema mismatches and potential runtime errors on all-null vector groups
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala
- **Line(s)**: 140-141
- **Description**: `aggregateField` sets `nullable = dataType != VectorType`, forcing `nullable = false` for `VectorType` aggregate outputs. However, if a group contains exclusively `NULL` vectors (or via outer joins), Spark ML's `Summarizer.mean` evaluates to `null`.
- **Risk**: Low. Schema nullability mismatch between `transformSchema` and actual runtime execution when aggregating vector columns containing null values.
- **Suggested Fix**: Set `nullable = true` for `VectorType` aggregate fields in `aggregateField` to match runtime nullability when vector inputs contain nulls.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Non-collapsed merging now joins each generated grouping key with Spark's
  null-safe equality operator (`<=>`) and projects a single copy of the left keys plus the right
  aggregates before final output selection.
- **Why**: Spark groups null keys together, so the merge must match those groups rather than
  silently dropping their source rows through ordinary SQL equality.
- **How verified**: Added a three-row regression with two null keys and one non-null key. The
  non-collapsed result retains all rows and assigns the null-key group mean to both null-key rows.

### Issue 2
- **Status**: Fixed
- **What changed**: Dataset-aware resolution now reads `spark.sql.caseSensitive` from
  `dataset.sparkSession`; schema-only resolution continues to use the active session when one
  exists and the existing insensitive fallback otherwise.
- **Why**: Runtime DataFrame analysis belongs to the dataset's session, which may differ from the
  thread-local active session in multi-session and PySpark execution.
- **How verified**: Cross-session regressions now assert that a case-sensitive dataset rejects
  case-mismatched references even under an insensitive active session, while an insensitive
  dataset accepts them under a sensitive active session. The no-active-session case also confirms
  runtime follows the dataset session.

### Issue 3
- **Status**: Not applicable
- **What changed**: Vector output nullability remains `false`.
- **Why**: Spark's `Summarizer.mean` expression itself declares a non-nullable vector output, so
  the current `transformSchema` field exactly matches the runtime DataFrame schema. All-null
  vectors do not produce a nullable result; Spark aborts execution with a `MatchError` while
  updating the summarizer. Marking the field nullable would therefore create the schema mismatch.
- **How verified**: Added an all-null vector regression that asserts exact declared/runtime schema
  equality, verifies `mean(features)` is non-nullable, and confirms materialization fails rather
  than returning a null vector. All 44 focused tests and both scalastyle checks pass.

## Round 3 Re-review 1

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: sequential
- **Model**: gemini-3.6-flash
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-3-gemini-3.6-flash.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Verified `mergeWithGroups` null-safe join (`<=>`) in `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala:569` and verified non-collapsed grouping retains null-key rows without duplicate column ambiguity in `EnsembleByKeyResolutionSuite.scala:21-36`.
- [x] Verified runtime `spark.sql.caseSensitive` configuration resolution uses `dataset.sparkSession` in `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala:367-368` and verified multi-session behavior in `EnsembleByKeySuite.scala:210-234`.
- [x] Verified empirical evidence that Spark's `Summarizer.mean` declares vector outputs non-nullable (`nullable = false`) in `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala:140-141` and aborts on all-null vectors rather than returning null in `EnsembleByKeyResolutionSuite.scala:38-52`.
- [x] Ran all 44 Scala tests across `EnsembleByKeySuite` and `EnsembleByKeyResolutionSuite` via sbt (`44 succeeded, 0 failed`) and verified zero scalastyle findings across 211 source files and 151 test files.
- [x] Verified Python implementation in `core/src/main/python/synapse/ml/stages/EnsembleByKey.py` and unit test coverage in `core/src/test/python/synapsemltest/stages/test_ensemble_by_key.py` compile cleanly.
