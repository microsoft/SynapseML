# Code Review — Round 1 of 6 (sequential mode)

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

Scope reviewed: explicit base-to-working-tree diff `8d528f3c9279603a8fde22623ad10a27b0d48ff7` → working tree (head `e49df024cc32366de3ff714addc2be698b23830b`), excluding `reviews/pr-2575`. Confirmed the diff touches exactly two files:

```
core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala      | 49 ++++----
core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala | 108 +++++++++++-
2 files changed, 136 insertions(+), 21 deletions(-)
```

**Bottom line:** the central fix is correct. Every schema claim the new `transformSchema` makes (field order, data types, nullability, metadata, key ordering, both collapse modes) was verified line-by-line against the Apache Spark **v3.5.0** sources that `build.sbt:31` pins (`val sparkVersion = "3.5.0"`). No Critical or High issues. Five lower-severity issues are recorded below, the most notable being an undocumented, untested backward-compatibility regression for case-insensitively-resolved grouping keys (Issue 1).

---

## Evidence Checklist

- [x] **Diff scope confirmed against the authoritative base**, not HEAD-only: `git --no-pager diff --stat 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'` in `C:\Users\singhrana\Documents\SynapseML-pr-2575` returned exactly the two files above; `git status` reports a clean tree; PR commits are `bdc39f93`, `5e6f164d`, `222d99e9`, `f4bc2c9e`, `e49df024`.
- [x] **Read both changed files in full**, not just the hunks: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala` (166 lines) and `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala` (207 lines).
- [x] **Collapse-mode field ordering verified** against Spark 3.5.0 `sql/core/.../RelationalGroupedDataset.scala` `toDF(aggExprs)`: output is `groupingExprs ++ aggExprs` when `spark.sql.retainGroupColumns` is true (default), and `Dataset.groupBy(col1, cols*)` builds `groupingExprs` in the caller's argument order. This matches `EnsembleByKey.scala:154` (`keyFields ++ aggregateFields`) including key ordering taken from `getKeys` rather than schema order.
- [x] **Non-collapse field ordering verified** against Spark 3.5.0 `sql/catalyst/.../analysis/Analyzer.scala:3369` `commonNaturalJoinProcessing`: for `case _ : InnerLike` the project list is `leftKeys ++ lUniqueOutput ++ rUniqueOutput`, with `leftKeys` sourced from the **left** plan in `joinNames` order. This exactly matches `EnsembleByKey.scala:159` (`keyFields ++ inputFields ++ aggregateFields`) where `inputFields` = schema fields minus keys minus output names (`:158`) and the left side is `dataset.drop(needToDrop)` (`:128-129`). Inner join applies no `withNullability(true)` rewrite, so key/passthrough nullability and metadata are preserved — matching `keyNames.map(schema(_))` at `:152`.
- [x] **Float-mean → DoubleType verified** against Spark 3.5.0 `sql/catalyst/.../aggregate/Average.scala`: `resultType` falls through to `case _ => DoubleType` for `FloatType`, and `override def nullable: Boolean = true`. So `case _: FloatType => StructField(outputName, DoubleType)` (`EnsembleByKey.scala:146`, default `nullable = true`) is correct. This is a genuine fix over the previous code, which propagated `f.dataType` (FloatType).
- [x] **Vector-mean nullability verified** against Spark 3.5.0 `mllib/.../ml/stat/Summarizer.scala`: `MetricsAggregate` declares `override def nullable: Boolean = false`, and `structureForMetrics` builds `StructField(name, vectorUDT, nullable = false)`. `Summarizer.mean` returns `c1.getField("mean").as(...)`; `GetStructField.nullable = child.nullable || childSchema(ordinal).nullable = false || false = false`, and `Alias.nullable = child.nullable`. So `StructField(outputName, VectorType, nullable = false)` (`EnsembleByKey.scala:147`) is correct.
- [x] **Vector-mean metadata verified** against Spark 3.5.0 `sql/core/.../Column.scala` `def name(alias: String)` → `Alias(expr, alias)(nonInheritableMetadataKeys = Seq(DATASET_ID_KEY, COL_POS_KEY))` and `sql/catalyst/.../namedExpressions.scala` `Alias.metadata`, whose `case structField: GetStructField => removeNonInheritableMetadata(structField.metadata)` branch yields `Metadata.empty` because `structureForMetrics` attaches no metadata. Confirms the PR's core claim that VectorAssembler `ml_attr` metadata is dropped by the aggregate — so declaring an empty-metadata `StructField` is right, and the old code (which reused the input `StructField`'s metadata implicitly via `f.dataType`/field reuse) was wrong.
- [x] **`VectorType` equality is safe for `StructType ===` comparisons**: `VectorUDT.equals` returns true for any `VectorUDT` instance, so `SQLDataTypes.VectorType` compares equal to the `new VectorUDT` instance created inside `SummaryBuilderImpl`.
- [x] **Default-`colNames` refresh semantics verified**: `setDefaultColNames()` (`EnsembleByKey.scala:86-90`) guards on `isSet(colNames)` (explicit `paramMap` only), while `setDefault` writes `defaultParamMap`. `isSet` therefore stays `false` after an auto-derived default, so the default is recomputed on every call and correctly tracks later `setCols`. This is behaviourally identical to the removed `get(colNames).isEmpty` guard (`Params.get` reads `paramMap`, not defaults) — the refactor is faithful. Covered by the new test at `EnsembleByKeySuite.scala:126`.
- [x] **`cols`/`colNames` length-mismatch consistency checked**: both `transform` (`:114`) and `transformSchema` (`:142`) use `getCols.zip(getColNames)`, and both `transform`'s `needToDrop` (`:128`) and `transformSchema`'s `outputNameSet` (`:157`) use the *full* `getColNames`. Schema and data therefore agree even under mismatched lengths (see Issue 4 for the shared latent behaviour).
- [x] **Missing/invalid column behaviour traced**: `StructType.apply(name)` in Spark 3.5.0 (`sql/api/.../types/StructType.scala:277`) is a case-sensitive `nameToField.getOrElse` that throws plain `IllegalArgumentException("$name does not exist. Available: ...")`. This is what `schema(inputName)` (`:143`) and `keyNames.map(schema(_))` (`:152`) now rely on. See Issues 1 and 5.
- [x] **Blast radius / no downstream Scala consumers**: repo-wide grep for `EnsembleByKey` finds only the transformer, its suite, `core/src/main/scala/.../EnsembleByKey.txt` (website doc blurb, unaffected by this change), and versioned website markdown. No other Scala or Python source depends on `transformSchema`'s previous (incorrect) output.
- [x] **JVM binary + Scala source compatibility**: public surface is unchanged — `def transformSchema(schema: StructType): StructType` keeps its exact signature and visibility; all `keys`/`cols`/`colNames`/`strategy`/`collapseGroup`/`vectorDims` params, getters, setters and overloads are untouched; `object EnsembleByKey extends DefaultParamsReadable[EnsembleByKey]` (readable companion) is preserved; `DefaultParamsWritable`/`copy` unchanged. The only addition is `private def setDefaultColNames(): Unit`, which is not part of the public ABI.
- [x] **Generated-code impact considered**: `Wrappable` codegen derives the Python/R wrappers from `Param` declarations, none of which changed, so no regeneration is required and no generated Python/R behaviour shifts. Confirmed no files under `target/`, `generated/`, or `website/versioned_docs/` were edited by this diff.
- [x] **SynapseML Scala conventions checked**: MIT license header present on both files; `Wrappable` + `DefaultParamsWritable` + `SynapseMLLogging` with `logClass(FeatureNames.Core)` retained (`EnsembleByKey.scala:22-24`); `transform` still wrapped in `logTransform[DataFrame]` (`:93`); DataFrame/Dataset APIs only (no RDD, no `collect` in the transformer). Scalastyle limits satisfied — measured max line length 109 (main) / 115 (test) vs `maxLineLength=120`, file lengths 166 / 207 vs `maxFileLength=800`, no tabs, trailing newline present (verified by script over both files against `scalastyle-config.xml` and `scalastyle-test-config.xml`).
- [x] **Test-quality mapping performed** for all seven stated requirements (see "Requirement → Test Mapping" below), including hand-evaluation of the expected aggregate values in `EnsembleByKeySuite.scala:96-124` against `SummarizerBuffer`'s online-mean algorithm (`realMean(i) = currMean(i) * (currWeightSum(i) / totalWeightSum)` with zero-entry filtering) — all expected values are exactly representable, so the assertions are not float-precision-flaky.
- [x] **Security checklist — NOT APPLICABLE, with reasons**: the diff introduces no serialization/deserialization changes (`DefaultParamsWritable`/`DefaultParamsReadable` untouched), no file/network/process I/O, no authentication/authorization/credential handling, no secrets or tokens, and no logging of user data. The only string interpolation added is `s"$getStrategy($name)"` (`:88`) and the pre-existing exception message (`:148`); neither is used to build SQL text, a shell command, a path, or a URL — column names flow through the typed `Column`/`StructType` API, not a SQL parser. `strategy` is constrained by `allowedStrategies` (`:57-59`), so `$getStrategy` cannot be arbitrary user text. No untrusted-input parsing was added.
- [ ] **Not verified: local compile / test execution.** No JDK, `sbt`, or Spark artifacts are present in this environment (`where.exe java` and `where.exe sbt` both return nothing; `core/target` does not exist; the Coursier cache contains no `spark-mllib` jar). All Spark-behaviour claims above were therefore verified against pinned Spark v3.5.0 upstream sources rather than by execution. Recommend a CI run of `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKeySuite` before merge — the new tests self-verify these assumptions via `assert(transformedSchema === actualSchema)`.

---

## Requirement → Test Mapping

| Requirement | Covered by | Verdict |
| --- | --- | --- |
| `transformSchema` matches actual transform output | `EnsembleByKeySuite.scala:80` (`assert(transformedSchema === actualSchema)`, 4 combinations) and `:107` | Covered |
| Correct collapse and non-collapse shapes and ordering | `:57-94` loops `collapseGroup ∈ {true,false}` and asserts `fieldNames` order; `:108-110` asserts exact non-collapse column order | Covered |
| Grouping keys come from configured grouping columns | `:61` uses `keyNames = Array("group","region")` whose schema positions (2, 1) deliberately differ from `getKeys` order — this is what pins the old `filter(f => colSet(f.name))` bug | Covered, well designed |
| Default and explicit `colNames`, defaults refresh when cols change | `:64` (both name sets) and `:126-136` (refresh after `setCols`) | Covered |
| Float means declared `DoubleType` | `:87` (`StructField(outputNames(1), DoubleType)`) and `:118` (`row.getDouble(1)` on the float column) | Covered |
| Vector mean nullability and metadata match Spark 3.5 | `:88-89` (`nullable = false`), `:111-112` (`metadata === Metadata.empty`, `!nullable`), plus the `:63` guard proving the *input* metadata is non-empty so the assertion is meaningful | Covered, and the `:63` precondition is a genuinely good touch |
| Numeric/vector inputs, multiple keys, overwrite, naming, unsupported types | `:57`, `:96`, `:126`, `:138` | Covered; gaps noted in Issue 5 |

---

## Issues

### Issue 1: Grouping keys are now resolved case-sensitively, breaking previously working transforms
- **Severity**: Medium
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 152 (`val keyFields = keyNames.map(schema(_))`), with contributing context at 143
- **Description**: `transformSchema` now resolves grouping keys through `StructType.apply(name)`, which is a **case-sensitive** `nameToField` map lookup that throws `IllegalArgumentException` on a miss (Spark 3.5.0 `sql/api/.../types/StructType.scala:277`). Spark's own resolution is **case-insensitive by default** (`spark.sql.caseSensitive = false`), and both runtime paths in `transform` resolve keys case-insensitively: `Dataset.groupBy(col1, cols*)` calls `Dataset.resolve`, which uses `sparkSession.sessionState.analyzer.resolver`, and `commonNaturalJoinProcessing` finds `leftKeys` via `left.output.find(attr => resolver(attr.name, keyName))`.

  Before this PR the old `transformSchema` never referenced `getKeys` at all, so a configuration such as `new EnsembleByKey().setKeys("LABEL1").setCol("score1")` against a DataFrame with a `label1` column produced correct data end-to-end. After this PR the same configuration fails at `transform`'s internal `transformSchema(dataset.schema)` call (`:96`) with `IllegalArgumentException: LABEL1 does not exist. Available: label1, ...`.

  Note this is **specific to `keys`**. For `cols` there is no regression: the old `transform` already did `dataset.schema(inColName)` at `:110`, which was equally case-sensitive.
- **Risk**: A silent, undocumented, untested backward-compatibility break. Pipelines that relied on Spark's default case-insensitive column resolution for `keys` now hard-fail with an exception rather than degrading. Because `transform` calls `transformSchema` first, there is no workaround short of changing the caller's key casing. Nothing in the PR description, tests, or `EnsembleByKey.txt` documents the new strictness.
- **Suggested Fix**: Resolve `keys` (and `cols`) against the schema with a case-insensitive fallback that honours `SQLConf.get.caseSensitiveAnalysis`, and — importantly for the PR's own goal — build `keyFields` from the **resolved** `StructField` so the declared output name matches the name `groupBy`/`join` actually emits (which is the *left attribute's* name, not the requested casing). For example:

  ```scala
  private def resolveField(schema: StructType, name: String): StructField =
    schema.fields.find(_.name == name)
      .orElse(if (SQLConf.get.caseSensitiveAnalysis) None
              else schema.fields.find(_.name.equalsIgnoreCase(name)))
      .getOrElse(throw new IllegalArgumentException(
        s"$name does not exist. Available: ${schema.fieldNames.mkString(", ")}"))
  ```

  If strict matching is the deliberate design decision instead, state it explicitly in `EnsembleByKey.txt` and add a test that pins the new exception, so the break is intentional and visible.

### Issue 2: `transformSchema` mutates transformer state, making schema derivation side-effecting and non-thread-safe
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 86-90, 136
- **Description**: `setDefaultColNames()` calls `setDefault(colNames -> ...)`, which writes into `Params.defaultParamMap` — a mutable map. Calling it from `transformSchema` (`:136`) turns a method Spark treats as a pure schema derivation into a state-mutating one. As established in the evidence checklist, the `isSet(colNames)` guard never becomes true for auto-derived defaults, so **every** `transformSchema` call performs this write.

  Two concrete consequences:
  1. **Concurrency.** `Pipeline.fit`, `PipelineModel.transformSchema`, and serving code paths call `transformSchema` on shared stage instances. Sharing a single `EnsembleByKey`/`PipelineModel` across threads (a normal serving pattern) can now race on `defaultParamMap`'s underlying `mutable.Map` from `transformSchema` as well as from `transform`. Concurrent `put` on a `mutable.HashMap` is unsafe.
  2. **Persistence.** `DefaultParamsWriter` serialises `defaultParamMap` alongside `paramMap`, so the on-disk metadata of a saved `EnsembleByKey` now depends on whether `transformSchema` was ever invoked before `save`. (Round-trip behaviour stays correct because the default is recomputed whenever `cols` changes, so this is a cleanliness/determinism concern rather than a correctness one.)

  This is partly pre-existing — the removed code did the same thing inside `transform` — but the PR extends the mutation to a second, read-only-by-contract entry point.
- **Risk**: Low. Non-deterministic map corruption under concurrent shared-instance use; surprising, path-dependent persisted metadata.
- **Suggested Fix**: Compute the effective names without mutating params, and keep `setDefault` (if desired at all) confined to `transform`:

  ```scala
  private def effectiveColNames: Array[String] =
    get(colNames).getOrElse(getCols.map(name => s"$getStrategy($name)"))
  ```

  Then use `effectiveColNames` at `:114` and `:139`. This preserves the exact refresh-on-`setCols` semantics the new test at `EnsembleByKeySuite.scala:126` pins, while making `transformSchema` referentially transparent.

### Issue 3: Residual schema/transform divergence when an output name collides with a grouping key (non-collapse)
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 128-129 (transform) vs 156-159 (transformSchema)
- **Description**: In the non-collapse path, `transform` computes `needToDrop = getColNames.toSet & dataset.columns.toSet` and drops those columns from the **left** side of the join. If a `colNames` entry equals a `keys` entry — e.g. `setKeys("label1").setCol("score1").setColName("label1").setCollapseGroup(false)` — the grouping column is removed from the left side, and `join(aggregated, getKeys)` then fails inside `commonNaturalJoinProcessing`, which throws `QueryCompilationErrors.unresolvedUsingColForJoinError(keyName, ..., "left")` (verified in Spark 3.5.0 `Analyzer.scala:3379-3384`).

  `transformSchema` does not detect this: `keyFields` is built from `schema` (`:152`) and `outputNameSet` only filters `inputFields` (`:157-158`), so it happily returns `[key, ...inputs..., key]`. Given that this PR's stated contract is "`transformSchema` must match actual Spark transform output", this is the one remaining configuration where the two still disagree — `transformSchema` succeeds where `transform` throws.
- **Risk**: Low. The configuration is contrived and the failure is pre-existing, but schema validation reporting success for a configuration that cannot execute defeats the point of the fix, and the resulting `AnalysisException` from deep inside the join is far less actionable than an up-front error.
- **Suggested Fix**: Reject the collision explicitly in `transformSchema`, e.g. before building `fields`:

  ```scala
  if (!getCollapseGroup) {
    val collisions = outputNames.toSet & keyNames.toSet
    require(collisions.isEmpty,
      s"Output column names ${collisions.mkString(", ")} collide with grouping keys " +
        s"${keyNames.mkString(", ")} when collapseGroup is false")
  }
  ```

### Issue 4: Mismatched `cols`/`colNames` lengths silently drop aggregates, and a surplus `colNames` entry silently deletes an input column
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 142 (and 114, 128, 157)
- **Description**: Both `transform` and `transformSchema` use `getCols.zip(getColNames)`, which silently truncates to the shorter array. Two behaviours follow, neither validated:
  - `setCols("a", "b").setColName("x")` produces only the `a` aggregate; `b` is silently ignored with no error.
  - The inverse is worse. `needToDrop` (`:128`) and `outputNameSet` (`:157`) both use the **full** `getColNames`, so `setCols("a").setColNames(Array("x", "b"))` where `b` is an existing DataFrame column causes `b` to be dropped from the non-collapse output with **no replacement aggregate** — a silent column deletion.

  To the PR's credit, `transformSchema` reproduces this faithfully in both directions (I traced both cases through `:142`, `:157-158` against `:114`, `:128-129`), so the schema/data contract still holds. The issue is that this hardening pass is the natural place to reject the ill-formed configuration rather than mirror it.
- **Risk**: Low. Silent data loss / silently missing aggregates under user misconfiguration, with no diagnostic.
- **Suggested Fix**: Add a length guard alongside the existing type validation, e.g. `require(getCols.length == getColNames.length, s"cols (${getCols.length}) and colNames (${getColNames.length}) must have the same length")` at the top of `transformSchema` after `setDefaultColNames()`. Add a test asserting the failure.

### Issue 5: New fail-fast paths in `transformSchema` are untested
- **Severity**: Low
- **File**: `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala`
- **Line(s)**: 138-147 (the only new negative test)
- **Description**: The rewrite changes `transformSchema` from lenient to strict in three ways, only one of which is covered:
  1. A `cols` entry absent from the schema now throws `IllegalArgumentException` from `schema(inputName)` (`:143`). Previously `schema.fields.flatMap { f => if (!colSet(f.name)) None ... }` silently skipped it. **Untested.**
  2. A `keys` entry absent from the schema now throws from `keyNames.map(schema(_))` (`:152`). The old implementation never referenced `getKeys`. **Untested** (and see Issue 1).
  3. `keys` never set now throws `NoSuchElementException` ("Failed to find a default value for keys") from `$(keys)`, where the old implementation returned a schema. **Untested.**

  Only the unsupported-*type* path (`IntegerType`) is covered, at `:138`. Also note that `assert(error.getMessage === "Cannot operate on type IntegerType with strategy mean")` (`:146`) is an exact-string assertion coupled to `DataType.toString`; a looser `assert(error.getMessage.contains("IntegerType"))` would be less brittle across Spark upgrades, though it is correct for Spark 3.5.0 (`IntegerType` is a `case object`, so its default `toString` is `"IntegerType"`).
- **Risk**: Low. The new strictness is a user-visible contract change that nothing pins, so a future refactor could silently revert to lenient behaviour (or tighten it further) without any test failing.
- **Suggested Fix**: Add two short negative tests — one for a `cols` entry missing from the schema, one for a `keys` entry missing from the schema — asserting `intercept[IllegalArgumentException]` and that the message names the offending column.

---

## Notes (no action required)

- The core defect is genuinely fixed. The old code's `val keyFields = schema.fields.filter(f => colSet(f.name))` filtered by **cols**, not keys, and the collapse branch returned `schema.fields ++ newFields` (every input column plus aggregates) — both plainly wrong. The replacement is correct in both modes.
- The `assert(input.schema("features").metadata !== Metadata.empty)` precondition at `EnsembleByKeySuite.scala:63` is a strong test-design choice: it prevents the metadata assertions at `:111` from passing vacuously if `VectorAssembler` ever stops attaching `ml_attr` metadata.
- Expected values in `EnsembleByKeySuite.scala:119-123` are exactly representable in IEEE-754 under `SummarizerBuffer`'s online-mean recurrence (including the zero-value filtering that makes `currWeightSum(0) = 1`, `totalWeightSum = 2` for the `east` group), so the `===` array comparison will not be flaky.
- No `-Xfatal-warnings` / `-Ywarn-unused` in `build.sbt`, and all newly added imports (`SQLDataTypes`, `DoubleType`, `Metadata`, `StructField`) are used, so no lint risk from the import changes.

---

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Added `resolveKeyField` in
  `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`.
  It resolves configured keys case-insensitively, rejects missing or ambiguous
  matches, and passes the resolved input field names to both `groupBy` and
  `join`. Added collapse and non-collapse coverage using uppercase configured
  keys against lowercase input fields.
- **Why**: This preserves Spark's default case-insensitive grouping behavior
  while ensuring `transformSchema` and `transform` use the same actual key
  names and output schema.
- **How verified**: JDK 11 focused run
  `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKeySuite`
  passed all 14 tests, including
  `grouping keys should resolve case-insensitively to input field names`.

### Issue 2
- **Status**: Fixed
- **What changed**: Replaced the mutating `setDefaultColNames` schema path with
  pure effective-name derivation inside `getSchemaFields`. `transformSchema`
  no longer writes `defaultParamMap`; `transform` retains the legacy behavior
  of recording an auto-derived default only after validation.
- **Why**: Schema derivation should be deterministic and safe to call without
  changing persisted parameter state, while preserving established transform
  behavior for callers that inspect defaults after execution.
- **How verified**: The focused 14-test suite passed. The refreshed-default
  test now asserts `getDefault(colNames)` is empty before and after
  `transformSchema`, then verifies updated `cols` produce updated names.

### Issue 3
- **Status**: Fixed
- **What changed**: Added a non-collapse validation that rejects output names
  colliding case-insensitively with resolved grouping keys before Spark builds
  the join.
- **Why**: A grouping key cannot be dropped from the left side and
  simultaneously serve as a join key. Failing early keeps schema validation
  aligned with executable behavior and provides an actionable message.
- **How verified**: The focused 14-test suite passed, including the invalid
  configuration case using output `GROUP` for grouping key `group`.

### Issue 4
- **Status**: Fixed
- **What changed**: Added an equal-length guard for `cols` and effective
  `colNames` before any `zip`, drop, or aggregation logic.
- **Why**: This prevents silent aggregate truncation and surplus-name input
  column deletion instead of reproducing an invalid configuration in both
  schema and data paths.
- **How verified**: The focused 14-test suite passed, including an explicit
  mismatched-length rejection assertion.

### Issue 5
- **Status**: Fixed
- **What changed**: Added table-driven negative coverage for unset keys,
  unset cols, missing key fields, missing aggregate fields, mismatched name
  counts, and key/output collisions. Internal validation now produces
  deliberate `IllegalArgumentException` messages for unset or empty keys and
  cols rather than incidental `NoSuchElementException`/`head` failures.
- **Why**: These are user-visible configuration contracts introduced or
  exposed by schema validation and should remain pinned by focused tests.
- **How verified**: JDK 11 focused run completed with `Tests: succeeded 14,
  failed 0`; `git diff --check` also passed.

---

## Round 1 Re-review 1

## Review Summary
- **Round**: 1 (re-review 1, post-fix)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 2 (both Low; 0 Critical, 0 High, 0 Medium)
- **Verdict**: ISSUES_FOUND

**Original findings status: 5 of 5 confirmed fixed.** Issues 1–5 were each re-verified
independently against the regenerated base-to-working-tree diff and the actual files, not
against the resolution log's claims. No regression was found in the schema/data contract:
every collapse and non-collapse ordering, type, nullability and metadata claim the new
`transformSchema` makes still matches what Spark 3.5.0 actually produces, and the fixes did
not reintroduce divergence anywhere I could construct.

The two new findings are both Low and non-blocking: the case-insensitive key resolver
ignores `spark.sql.caseSensitive` (Issue 6), and the new ambiguous-key error branch added by
the Issue 1 fix is the one new fail-fast path left without test coverage (Issue 7).

---

## Evidence Checklist

- [x] **Reviewed the authoritative regenerated diff, byte-for-byte.** Regenerated
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`
  in `C:\Users\singhrana\Documents\SynapseML-pr-2575` and string-compared it (newline-normalized)
  against the embedded diff in
  `.copilot/session-state/.../pr-2575/prompts/review-round-1.md`: both 13,659 characters,
  comparison returned `IDENTICAL: True`. Scope is exactly two files,
  `EnsembleByKey.scala` (+95) and `EnsembleByKeySuite.scala` (+152), 219 insertions / 28 deletions.
- [x] **Read both changed files in full at their current state**, not just the hunks:
  `core/src/main/scala/.../stages/EnsembleByKey.scala` (198 lines) and
  `core/src/test/scala/.../stages/EnsembleByKeySuite.scala` (251 lines). All line references below
  are to the current working-tree files.
- [x] **Issue 1 (case-sensitive keys) — CONFIRMED FIXED.** `resolveKeyField`
  (`EnsembleByKey.scala:86-96`) matches with `equalsIgnoreCase`, and `keyFields`
  (`:115`) now carries the *resolved schema* `StructField`. `transform` derives
  `keyNames = keyFields.map(_.name)` (`:141`) and feeds those resolved names to both
  `groupBy` (`:170`) and `join` (`:177`), so schema and data agree on the emitted key names.
  Critically, `transformSchema`'s `keyNameSet` (`:188`) is also built from resolved names, so the
  `filterNot` at `:190` removes the correct passthrough fields when the configured key casing
  differs from the schema casing — this is the subtle part and it is right. Pinned by
  `EnsembleByKeySuite.scala:138-153` for both `collapseGroup` values.
- [x] **Issue 2 (impure `transformSchema`) — CONFIRMED FIXED.** `setDefaultColNames` is gone.
  `getSchemaFields` (`:98-136`) performs no `set`/`setDefault`; `transformSchema` (`:183-195`)
  only reads. Grepped the whole file: the sole remaining `setDefault` writes outside construction
  are `:143` inside `transform`, i.e. exactly the pre-PR behavior, so `transformSchema` is now
  referentially transparent and safe on shared stage instances.
- [x] **Default-name refresh semantics preserved across the Issue 2 refactor.**
  `!isSet(colNames)` (`:142`) is equivalent to the removed `get(colNames).isEmpty` guard, because
  Spark's `Params.isSet` reads `paramMap` and `Params.get` reads `paramMap` while `setDefault`
  writes `defaultParamMap`. `getSchemaFields` reads `get(colNames)` (`:109`), which likewise never
  sees an auto-derived default, so names are recomputed from `cols` on every call. Pinned by
  `EnsembleByKeySuite.scala:126-136`, which now asserts `getDefault(colNames).isEmpty` both
  *before and after* a `transformSchema` call and then that changing `cols` changes the names.
- [x] **Issue 3 (key/output collision in non-collapse) — CONFIRMED FIXED, and the guard strictly
  dominates the runtime hazard.** `:116-123` rejects, case-insensitively, any output name that
  matches a resolved key when `collapseGroup` is false. This is the correct comparison, because
  `needToDrop` at `:176` is an exact-case set intersection but `Dataset.drop(colNames: String*)`
  resolves *case-insensitively* via the analyzer resolver — so the only way a grouping key could be
  removed from the left side of the `join` at `:177` is via a case-insensitive match, which the
  guard now blocks first. Correctly skipped for `collapseGroup = true`, where no drop/join happens
  and duplicate names appear identically in both schema and data. Pinned by
  `EnsembleByKeySuite.scala:170-175` (output `GROUP` vs key `group`).
- [x] **Issue 4 (length mismatch) — CONFIRMED FIXED.** `require` at `:111-113` runs before any
  `zip`, drop, or aggregation, closing both the silent-truncation and the surplus-name
  silent-column-deletion paths. The `nonEmpty` guards at `:106-107` additionally protect the
  `keyNames.head` / `newCols.head` calls at `:170-171`. Pinned by
  `EnsembleByKeySuite.scala:161-164`.
- [x] **Issue 5 (untested fail-fast paths) — CONFIRMED FIXED (one residual gap, Issue 7).**
  `EnsembleByKeySuite.scala:155-181` is a table-driven negative test covering unset `keys`, unset
  `cols`, missing key field, missing aggregate field, length mismatch, and key/output collision;
  `:183-192` covers the unsupported type. Verified `.contains` assertions are robust to the
  `"requirement failed: "` prefix that `require` prepends at `:106-107`, `:111`, `:119` while the
  direct `throw`s at `:102`/`:104` have no prefix.
- [x] **Collapse ordering re-verified for the rewritten code path.** `keyFields ++ aggregateFields`
  (`:186`) matches Spark 3.5.0 `RelationalGroupedDataset.toDF`, which emits
  `groupingExprs ++ aggExprs` under the default `spark.sql.retainGroupColumns = true`.
  `Dataset.groupBy(col1, cols*)` maps each name through `resolve`, producing the original
  `AttributeReference` (nullability *and* metadata intact), and `RelationalGroupedDataset.alias`
  passes a `NamedExpression` through unchanged — so key nullability/metadata survive, matching
  `keyFields` taken straight from the input schema.
- [x] **Non-collapse ordering re-verified.** `keyFields ++ inputFields ++ aggregateFields` (`:191`)
  matches Spark 3.5.0 `Analyzer.commonNaturalJoinProcessing`, whose `case _: InnerLike` project list
  is `leftKeys ++ lUniqueOutput ++ rUniqueOutput`, with `leftKeys` taken from the **left** plan in
  `joinNames` order and `rUniqueOutput` = the aggregate columns in `agg` order. `inputFields`
  (`:190`) filters the input schema by exactly the same predicate that `needToDrop` (`:176`) uses
  (output names) plus the keys that the join folds away. `InnerLike` applies no
  `withNullability(true)` rewrite, so passthrough nullability/metadata are preserved on both sides.
- [x] **Output types / nullability / metadata re-verified for the new `aggregateFields`
  (`:125-133`).** `Average.resultType` falls through to `DoubleType` for both `DoubleType` and
  `FloatType` inputs and is nullable, matching `StructField(outputName, DoubleType)` (default
  `nullable = true`, `Metadata.empty`) at `:128-129`. `Summarizer`'s `MetricsAggregate` declares
  `nullable = false` and `structureForMetrics` attaches no metadata, and `Alias.metadata` resolves
  the `GetStructField` child to `Metadata.empty`, matching
  `StructField(outputName, VectorType, nullable = false)` at `:130`. Both are pinned empirically by
  `EnsembleByKeySuite.scala:85-89` and `:111-112`, the latter guarded by the non-vacuity
  precondition at `:63`.
- [x] **Searched for divergence regressions introduced by the fixes; found none.** Hand-traced
  `transformSchema` against `transform` for: key also present in `cols`; output name equal to a
  non-key input column (the overwrite case); output name equal to a key under `collapseGroup = true`
  (duplicate names appear in both, consistently); duplicate output names; and default vs explicit
  `colNames` in both collapse modes. All agree. The only constructible mismatches require a schema
  that already contains two fields differing only in case, which is pre-existing behavior and
  independently rejected for keys by `resolveKeyField`.
- [x] **Independently corroborated the claimed test evidence rather than trusting the log.** Read
  `core/target/test-reports/TEST-com.microsoft.azure.synapse.ml.stages.EnsembleByKeySuite.xml`:
  `tests=14 failures=0 errors=0 skipped=0 time=91.102`, with all 14 cases enumerated. The 14 =
  11 declared in the suite + 3 auto-registered by the fuzzing traits
  (`Fuzzing.scala:450` Experiment Fuzzing, `:543` Serialization Fuzzing, `:609` Getters and
  Setters), so the run covers the entire suite with nothing filtered out. Timestamp ordering proves
  the run postdates the fixes: sources `21:20:24` → main classes `21:27:36` → test classes
  `21:29:42` → report `21:31:49`.
- [x] **Assessed coverage adequacy of that run beyond the raw pass count.** Serialization Fuzzing
  (`Fuzzing.scala:515-541`) wraps the stage in `new Pipeline(...)`, calls `pipe.fit`, and round-trips
  a `PipelineModel`, so `transformSchema` is exercised through the `Pipeline`/`PipelineModel`
  entry points with `collapseGroup = false` — which is what the Issue 2 purity fix most affects —
  and `assertDFEq` compares full transformed DataFrames across the save/load boundary. The four
  new `assert(transformedSchema === actualSchema)` assertions are the real proof obligation and
  they self-verify the Spark-behavior claims above against the pinned Spark 3.5.0
  (`build.sbt`: `val sparkVersion = "3.5.0"`, Scala 2.12.17). **Conclusion: the run is adequate
  evidence for every changed line.** It does not cover: the ambiguous-key branch at `:92-94`
  (Issue 7), `spark.sql.caseSensitive = true` (Issue 6), scalastyle (not run by `testOnly`), and
  the generated Python/R/.NET wrapper tests (produced by the separate codegen task).
- [x] **Blast radius re-checked.** Repo-wide grep for `EnsembleByKey` returns only the transformer,
  its suite, `EnsembleByKey.txt`, and docs markdown — no other Scala/Python consumer. Verified the
  published example in `docs/Quick Examples/transformers/core/_Stages.md`
  (`setKeys(["label1"]).setCols(["score1"])`, default `colNames`, `collapseGroup = true`) still
  satisfies the new length, existence, and collision guards, so the documented snippet does not
  regress.
- [x] **Binary / source compatibility re-confirmed.** Public surface is unchanged:
  `def transformSchema(schema: StructType): StructType` keeps its exact signature; all params,
  getters, setters and overloads are untouched; `object EnsembleByKey extends
  DefaultParamsReadable` and `DefaultParamsWritable`/`copy` are preserved. The two additions
  (`resolveKeyField`, `getSchemaFields`) are `private`. No MiMa configuration exists in `build.sbt`,
  so no binary-compat gate is affected.
- [x] **Generated-code impact re-checked.** No `Param` declaration changed, so `Wrappable` codegen
  for Python/R/.NET wrappers is unaffected and no regeneration is required. The diff touches no
  files under `target/`, `website/versioned_docs/`, or any generated tree.
- [x] **Style/lint gates measured, not assumed.** Max line length 109 (main) and 115 (test) against
  `scalastyle-config.xml` `maxLineLength = 120`; file lengths 198/251 against `maxFileLength = 800`;
  `git --no-pager diff --check` exits 0 (no trailing whitespace or conflict markers). MIT headers,
  `Wrappable`/`DefaultParamsWritable`/`SynapseMLLogging` wiring, and the `logTransform[DataFrame]`
  wrapper (`:139`, `:179`) are all retained.
- [x] **Security checklist — NOT APPLICABLE, with reasons (re-confirmed for the new code).** The
  fixes add no I/O, serialization, credential, or untrusted-input handling. The new string
  interpolations at `:91`, `:94`, `:102`, `:104`, `:113`, `:121-122` build exception messages only;
  none is used to construct SQL text, a shell command, a path, or a URL — column names flow through
  the typed `Column`/`StructType` API. `getStrategy` remains constrained by `allowedStrategies`.
  The error messages echo schema field names, which is standard Spark behavior
  (`StructType.apply` does the same) and not a data-leak vector.
- [ ] **Not verified: local compile / test execution in this session.** This Windows worktree has no
  JDK or sbt on `PATH` (`where.exe java` and `where.exe sbt` return nothing, `JAVA_HOME` is empty).
  All execution evidence therefore comes from the committed
  `core/target/test-reports` XML produced by the JDK 11 run, cross-checked against source and class
  file timestamps as described above; all Spark-behavior claims were verified against pinned Spark
  3.5.0 semantics.
- [ ] **Not verified: behavior under `spark.sql.caseSensitive = true`.** No SynapseML code or test
  sets this conf (grep for `caseSensitive` finds only `TextFeaturizer`'s unrelated
  `caseSensitiveStopWords` param), and no test exercises it. See Issue 6.

---

## Issues

### Issue 6: `resolveKeyField` hard-codes case-insensitive matching, ignoring `spark.sql.caseSensitive`
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 86-96 (`resolveKeyField`), consumed at 115 and, via the collision guard, 116-123
- **Description**: The Issue 1 fix resolves grouping keys with an unconditional
  `equalsIgnoreCase` rather than honoring `SQLConf.get.caseSensitiveAnalysis`. Under the Spark
  default (`spark.sql.caseSensitive = false`) this exactly mirrors Spark's own resolution,
  including the ambiguity error, and is correct. Under `spark.sql.caseSensitive = true` it deviates
  in two directions:
  1. **Newly lenient.** With a schema containing only `group`, `setKeys("GROUP")` now silently
     resolves to `group` and succeeds. Pre-PR, `groupBy("GROUP")` would have failed analysis. A user
     who deliberately opted into case sensitivity gets a typo'd key silently accepted.
  2. **Newly strict (a regression).** With a schema legitimately containing both `group` and
     `GROUP` — legal only when case sensitivity is on — `setKeys("group")` now throws
     `"group is ambiguous. Matches: group, GROUP"` at `:92-94`, and the non-collapse guard at
     `:116-123` likewise rejects the unambiguous pairing `keys = ["group"]`,
     `colNames = ["Group"]`. Both configurations worked before this PR, because the old
     `transformSchema` never referenced `getKeys` at all and Spark's own exact-match resolution
     would have handled them.

  This is confined to a non-default configuration that SynapseML itself never sets, and the
  schema/data contract stays self-consistent in every case (`transformSchema` and `transform` use
  the same resolved names), which is why this is Low rather than Medium.
- **Risk**: Low. Under `spark.sql.caseSensitive = true`: silent acceptance of a mis-cased grouping
  key, and spurious `IllegalArgumentException` rejection of two previously working configurations.
- **Suggested Fix**: Gate the fallback on the session conf, preferring an exact match when one
  exists, e.g.:

  ```scala
  private def resolveKeyField(schema: StructType, name: String): StructField = {
    val candidates =
      if (SQLConf.get.caseSensitiveAnalysis) schema.fields.filter(_.name == name)
      else schema.fields.filter(_.name.equalsIgnoreCase(name))
    ...
  }
  ```

  and use the same predicate for the `equalsIgnoreCase` comparison in the collision guard at
  `:117-118`. Alternatively, if unconditional case-insensitivity is the deliberate contract, state
  it in `core/src/main/scala/.../stages/EnsembleByKey.txt` so the deviation is intentional and
  discoverable.

### Issue 7: The new ambiguous-key error branch has no test coverage
- **Severity**: Low
- **File**: `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala`
- **Line(s)**: 155-181 (the negative-configuration table), for the branch at
  `EnsembleByKey.scala:92-94`
- **Description**: The Issue 1 fix introduced a third fail-fast path — `"$name is ambiguous.
  Matches: ..."` at `EnsembleByKey.scala:92-94` — but the table-driven negative test added for
  Issue 5 covers only the missing-key, missing-col, unset-key, unset-col, length-mismatch and
  collision cases. The ambiguous branch is reachable and cheap to pin: `spark.createDataFrame(...)
  .toDF("group", "GROUP")` produces a schema with two case-variant fields, and
  `new EnsembleByKey().setKey("group").setCol(...).transformSchema(schema)` will hit it. As it
  stands, this is the one new user-visible error contract in the PR that no test would notice being
  removed or reworded — which is precisely the class of gap the original Issue 5 was raised about.
- **Risk**: Low. A future refactor of `resolveKeyField` could silently drop the ambiguity check
  (falling back to "first match wins", which would make `transformSchema` and `transform`
  disagree on which field is the key) with no test failing.
- **Suggested Fix**: Add one row to the existing table at `EnsembleByKeySuite.scala:156-168` using a
  purpose-built duplicate-case schema, asserting the message contains `"is ambiguous"`. While
  there, consider relaxing the exact-string assertion at `:190`
  (`error.getMessage === "Cannot operate on type IntegerType with strategy mean"`) to a `contains`
  check, so the suite is not coupled to `DataType.toString` across Spark upgrades.

---

## Notes (no action required, re-review)

- The `assert(transformedSchema === actualSchema)` assertions added at
  `EnsembleByKeySuite.scala:80`, `:107` and `:150` are the right proof obligation for this PR: they
  make the suite fail if a future Spark upgrade changes aggregate ordering, nullability, or metadata,
  rather than encoding today's Spark behavior as unverifiable constants.
- `transform` at `:142-144` still writes `defaultParamMap`. This is deliberately unchanged pre-PR
  behavior (the Issue 2 fix correctly scoped the purity guarantee to `transformSchema`), and it is
  benign here because `getSchemaFields` reads `get(colNames)`, which never observes a default.
- `transform` no longer routes through `transformSchema`; it calls `getSchemaFields` directly at
  `:140`. Validation is identical, so this is not a behavior change — it only skips the base
  `PipelineStage.transformSchema(schema, logging = true)` debug log.
- The asymmetry where `keys` resolve case-insensitively (`:115`) but `cols` resolve case-sensitively
  (`schema(inputName)` at `:126`, `dataset.schema(inColName)` at `:157`) is **not** a regression —
  the pre-PR `transform` was already case-sensitive for `cols` — and it produces no schema/data
  divergence, because both paths fail fast on the same lookup. Worth a doc line, not a fix.

---

## Resolution Log — Round 1 Re-review 1
_Open; to be updated by the driving agent._

### Issue 6
- **Status**: Fixed
- **What changed**: Added `columnNamesMatch`, which reads the public
  `spark.sql.caseSensitive` setting from the active `SparkSession`. Both key
  resolution and key/output collision checks now use exact matching when
  case-sensitive analysis is enabled and case-insensitive matching otherwise.
- **Why**: This mirrors Spark's configured analyzer semantics without adding a
  dependency on Spark internal `SQLConf` APIs. It preserves exact resolution
  for schemas containing both `group` and `GROUP` in case-sensitive mode and
  rejects mis-cased keys there.
- **How verified**: JDK 11 focused run passed all 15 tests. The new
  `grouping key resolution should honor case-sensitive analysis` test verifies
  exact resolution with duplicate-case fields and rejection of `Group`.

### Issue 7
- **Status**: Fixed
- **What changed**: Added an ambiguous-key assertion using a schema containing
  both `group` and `GROUP` while case-sensitive analysis is disabled.
- **Why**: The ambiguity error is a deliberate user-visible contract in
  Spark's default case-insensitive mode and must not regress to first-match
  behavior.
- **How verified**: The focused suite passed all 15 tests, including the new
  ambiguity branch; `git diff --check` remained clean.

---

## Round 1 Re-review 2

## Review Summary
- **Round**: 1 (re-review 2, post-fix for Issues 6 and 7)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 2 (both Low; 0 Critical, 0 High, 0 Medium)
- **Verdict**: ISSUES_FOUND

**Prior findings status: 7 of 7 confirmed fixed.** Issues 1-7 were each re-verified against the
regenerated base-to-working-tree diff and the current files, not against the resolution log's
claims. The `SparkSession.getActiveSession` + `spark.sql.caseSensitive` approach added for Issue 6
is **correct**, including the no-active-session case; I proved this by execution rather than by
reading (see the "case-sensitivity source" evidence items below), which refuted the plausible
alternative objection that the conf should be read from `dataset.sparkSession` instead.

The two new findings are both Low and both were found by executing adversarial probes against the
**compiled current code**: `transformSchema` and `transform` still disagree when the input schema
contains two fields whose names differ only in case (Issue 8, a real schema/data mismatch), and
ambiguous `cols` entries are not detected the way ambiguous `keys` now are (Issue 9, fail-open
validation). Both are pre-existing behaviours of `transform`, but both fall inside the exact
contract this PR exists to establish, so per this round's "advance only at zero issues" rule the
verdict is ISSUES_FOUND.

---

## Evidence Checklist

- [x] **Regenerated diff is byte-identical to the embedded prompt diff.**
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`
  in `C:\Users\singhrana\Documents\SynapseML-pr-2575`, newline-normalized and trimmed, compared
  with `-ceq` against the fenced block of
  `.copilot/session-state/c2ea157b-.../pr-2575/prompts/review-round-1.md`: both 16,049 characters,
  `IDENTICAL=True`. `git status` shows only the two expected modified files (plus untracked
  `reviews/`); `HEAD` is `e49df024`, base `8d528f3c`. `--stat`: `EnsembleByKey.scala` +103/-, 
  `EnsembleByKeySuite.scala` +189/-, 263 insertions / 29 deletions.
- [x] **Read both changed files in full at their current state** (not just hunks):
  `core/src/main/scala/.../stages/EnsembleByKey.scala` (204 lines) and
  `core/src/test/scala/.../stages/EnsembleByKeySuite.scala` (288 lines). All line references below
  are to the current working tree.
- [x] **Issue 1 (case-sensitive key regression) - CONFIRMED FIXED.** `resolveKeyField` (`:92-102`)
  resolves each configured key against the schema and returns the *schema's* `StructField`;
  `transform` derives `keyNames = keyFields.map(_.name)` (`:147`) and passes those resolved names to
  `groupBy` (`:176`) and `join` (`:183`), and `transformSchema` builds `keyNameSet` from the same
  resolved names (`:194`), so the passthrough filter at `:196` removes the right fields when the
  configured casing differs from schema casing. Pinned by `EnsembleByKeySuite.scala:140-157`.
- [x] **Issue 2 (impure `transformSchema`) - CONFIRMED FIXED.** `getSchemaFields` (`:104-142`) and
  `transformSchema` (`:189-201`) contain no `set`/`setDefault` call; the only `setDefault` outside
  construction is `:149`, inside `transform`, i.e. exactly the pre-PR behaviour. Grepped the whole
  file to confirm. Pinned by `EnsembleByKeySuite.scala:126-138`, which asserts
  `getDefault(colNames).isEmpty` before *and* after a `transformSchema` call.
- [x] **Issue 3 (key/output collision in non-collapse) - CONFIRMED FIXED.** `:122-129` rejects any
  output name matching a resolved key under the configured case semantics, only when
  `collapseGroup` is false. Correctly skipped for `collapseGroup = true`, where no drop/join occurs.
  Pinned by `EnsembleByKeySuite.scala:184-188` (output `GROUP` vs key `group`).
- [x] **Issue 4 (cols/colNames length mismatch) - CONFIRMED FIXED.** `require` at `:117-119` runs
  before any `zip`, drop or aggregation; `nonEmpty` guards at `:112-113` protect `keyNames.head`
  (`:176`) and `newCols.head` (`:177`). Pinned by `EnsembleByKeySuite.scala:180-183`.
- [x] **Issue 5 (untested fail-fast paths) - CONFIRMED FIXED.** `EnsembleByKeySuite.scala:174-206`
  is a table-driven negative test covering unset `keys`, unset `cols`, missing key field, missing
  aggregate field, length mismatch, key/output collision and (new) ambiguity; `:208-217` covers the
  unsupported type. The `.contains` assertions tolerate the `"requirement failed: "` prefix that
  `require` prepends at `:112-113`, `:117`, `:125`, while the direct `throw`s at `:108`/`:110` carry
  no prefix.
- [x] **Issue 6 (`spark.sql.caseSensitive` ignored) - CONFIRMED FIXED.** `columnNamesMatch`
  (`:86-90`) reads the conf from the active session and switches between `==` and
  `equalsIgnoreCase`; it is used by both `resolveKeyField` (`:93`) and the collision guard (`:124`),
  so key resolution and collision detection share one definition of "same column". Pinned by
  `EnsembleByKeySuite.scala:159-172`.
- [x] **Issue 6 follow-up - the *choice* of `SparkSession.getActiveSession` as the conf source is
  correct, verified by execution, not by reading.** I built the real classpath
  (`sbt --error "export core/fullClasspath"`, 17,066 chars) and ran probes in `jshell` (JDK 11,
  Spark 3.5.0, `local[2]`) against the compiled current classes. With a *second* session
  (`spark.newSession()`) whose `spark.sql.caseSensitive = true` while the thread-active session has
  `false`, plain Spark itself resolved case-insensitively: `dfB.groupBy("GROUP")` on the
  case-sensitive session **succeeded** (`PROBE-B-SPARK-GROUPBY:1`) and
  `dfC.groupBy("group")` on a `group`/`GROUP` schema threw
  `[AMBIGUOUS_REFERENCE] Reference 'group' is ambiguous` (`PROBE-C-SPARK-GROUPBY-THREW`). This is
  because Spark's `Analyzer` resolver comes from `SQLConfHelper.conf = SQLConf.get`, whose getter is
  `SparkSession.getActiveSession.map(_.sessionState.conf)` - i.e. the *same* thread-local source the
  PR reads, **not** `dataset.sparkSession.conf`. EnsembleByKey matched Spark in both cases
  (`PROBE-B-EBK-SCHEMA:group,mean(score)`, `PROBE-C-EBK-SCHEMA-THREW:group is ambiguous`).
- [x] **No-active-session behaviour is safe and Spark-aligned, verified by execution.** After
  `SparkSession.clearActiveSession()` (`PROBE-G-ACTIVE-EMPTY:true`), Spark's own
  `dfG.groupBy("GROUP")` against a `caseSensitive = true` session still **succeeded**
  (`PROBE-G-SPARK-GROUPBY-UPPER:1`), because `SQLConf.get` falls back to a defaults-only
  `fallbackConf` (`caseSensitive = false`). `columnNamesMatch`'s `Option.exists` fallback produces
  exactly the same case-insensitive behaviour, and EnsembleByKey returned the identical result
  (`PROBE-G-EBK:group,mean(score)`). The ambiguity path also matched
  (`PROBE-E-EBK-SCHEMA-THREW:group is ambiguous`). Conclusion: the `getOrElse(false)` default is the
  correct fallback, not a latent divergence.
- [x] **Issue 7 (untested ambiguous-key branch) - CONFIRMED FIXED.**
  `EnsembleByKeySuite.scala:198-205` builds a `group`/`GROUP` schema under
  `withCaseSensitiveAnalysis(false)` and asserts `"group is ambiguous"`. I re-derived the same
  message from the compiled code in the probe run (`PROBE-C-EBK-SCHEMA-THREW`), so the branch at
  `:98-100` is genuinely reachable and the test is not vacuous.
- [x] **`withCaseSensitiveAnalysis` helper is leak-free.** `EnsembleByKeySuite.scala:278-287`
  captures `spark.conf.get("spark.sql.caseSensitive")` (which returns the registered default
  `"false"` rather than throwing when unset) and restores it in a `finally`, so a failing assertion
  inside the block cannot leave the shared session case-sensitive for later suites.
  `build.sbt:274` (`Test / parallelExecution := false`) removes cross-suite interleaving risk.
- [x] **Executed the test suite evidence rather than trusting the 15/15 claim.**
  `core/target/test-reports/TEST-com.microsoft.azure.synapse.ml.stages.EnsembleByKeySuite.xml`
  reports `tests=15 failures=0 errors=0 skipped=0 time=72.384`, `timestamp=2026-08-01T04:50:28`,
  `hostname=CPC-singh-M2MZ2`, and enumerates all 15 cases by name, including
  `grouping key resolution should honor case-sensitive analysis` (0.034s) and
  `transformSchema should reject invalid column configurations` (0.024s). 15 = 12 tests declared in
  the suite + 3 auto-registered by `TransformerFuzzing` (Serialization Fuzzing, Experiment Fuzzing,
  Getters and Setters), so nothing was filtered out.
- [x] **Timestamp chain proves the run postdates the Issue 6/7 fixes.** Sources
  `EnsembleByKey.scala` / `EnsembleByKeySuite.scala` last written `21:46:51`; main classes
  `21:50:09`; test classes `21:50:24`; report start `21:50:28` local, report file written
  `21:51:41`. No source edit occurred after compilation.
- [x] **Compilation independently reproduced.** `sbt core/console` in WSL (Ubuntu, OpenJDK
  `11.0.31`, sbt 1.10.11, Scala 2.12.17) recompiled `EnsembleByKey.scala` with
  `[info] done compiling` and no warnings or errors.
- [x] **Scalastyle actually executed, not just measured.**
  `sbt "core/scalastyle" "core/Test/scalastyle"` -> main: `Processed 211 file(s) ... Found 0 errors,
  0 warnings, 0 infos`; test: `Processed 150 file(s) ... Found 0 errors, 0 warnings, 0 infos`. This
  closes the gap flagged in re-review 1 (scalastyle is not run by `testOnly`). Independently
  measured: max line length 109 (main) / 115 (test) vs `maxLineLength=120`; file lengths 204 / 288
  vs `maxFileLength=800`; `transform` 44 lines and `getSchemaFields` 39 lines vs `maxLength=60`;
  ~22 methods vs `maxMethods=50`; no tabs; newline at EOF; `git --no-pager diff --check` exits 0.
- [x] **Collapse-mode ordering / types / nullability / metadata re-verified.**
  `keyFields ++ aggregateFields` (`:192`) matches `RelationalGroupedDataset.toDF`
  (`groupingExprs ++ aggExprs` under the default `spark.sql.retainGroupColumns = true`), with
  grouping expressions in `getKeys` order because `groupBy(col1, cols*)` maps names through
  `resolve` in argument order. `Average.resultType` is `DoubleType` for both `DoubleType` and
  `FloatType` inputs and is nullable, matching `:134-135` (`StructField(outputName, DoubleType)`,
  default `nullable = true`, `Metadata.empty`). `Summarizer`'s `MetricsAggregate` is
  `nullable = false` and `structureForMetrics` attaches no metadata, matching
  `StructField(outputName, VectorType, nullable = false)` (`:136`). All four are pinned empirically
  by `EnsembleByKeySuite.scala:85-90` (`assert(transformedSchema === actualSchema)` across 4
  combinations, plus explicit `StructField` equality including metadata) and `:111-112`
  (`metadata === Metadata.empty`, `!nullable`), the latter guarded by the non-vacuity precondition
  at `:64`.
- [x] **Non-collapse ordering re-verified.** `keyFields ++ inputFields ++ aggregateFields` (`:197`)
  matches `Analyzer.commonNaturalJoinProcessing`, whose `InnerLike` project list is
  `leftKeys ++ lUniqueOutput ++ rUniqueOutput` with `leftKeys` from the left plan in `joinNames`
  order; no `withNullability(true)` rewrite is applied for inner joins, so passthrough
  nullability/metadata survive. Pinned by `EnsembleByKeySuite.scala:109-110`, which asserts the
  exact column order `group, region, id, component1, component2, doubleScore, floatScore, features`.
- [x] **Hand-traced additional schema/transform equivalence cases and found them consistent**: key
  also listed in `cols`; output name equal to a non-key input column (the supported overwrite case);
  output name equal to a key under `collapseGroup = true`; duplicate entries inside `colNames`;
  output name differing in case from an existing column with **no** case-variant twin present
  (consistent, because `needToDrop` and `outputNameSet` are then both no-ops); default vs explicit
  `colNames` in both collapse modes.
- [x] **Ran adversarial duplicate-case probes against the compiled code - these produced Issues 8
  and 9.** `jshell` + `core/fullClasspath`, Spark 3.5.0, `spark.sql.caseSensitive = false`:
  - Schema `id, group, score, features, FEATURES`; `keys=[group]`, `cols=[score]`,
    `colNames=[features]`, `collapseGroup=false`:
    `PROBE-A-SCHEMA:group,id,score,FEATURES,features` vs
    `PROBE-A-ACTUAL:group,id,score,features`, `PROBE-A-MATCH:false`,
    `PROBE-A-ROW:[a,0,1.0,1.0]` (4 columns of data). -> Issue 8.
  - `PROBE-D-DROP:id,group,score` confirms `Dataset.drop("features")` removes **both** `features`
    and `FEATURES` via the analyzer resolver, which is the root cause of Issue 8.
  - Schema `group, score, SCORE`; `keys=[group]`, `cols=[score]`:
    `PROBE-F-SCHEMA:group,mean(score)` succeeded but
    `PROBE-F-TRANSFORM-THREW:[AMBIGUOUS_REFERENCE] Reference 'score' is ambiguous`. -> Issue 9.
- [x] **Blast radius re-checked.** `git grep -l EnsembleByKey` (excluding `reviews/`) returns only
  the transformer, its suite, `EnsembleByKey.txt`, `docs/Quick Examples/transformers/core/
  _Stages.md`, a Zeppelin sample notebook, and versioned website docs. The published example
  (`setKeys(["label1"]).setCols(["score1"])`, default `colNames`, `collapseGroup = true`) satisfies
  every new guard. No Scala, Python, R or .NET consumer depends on the old `transformSchema` output.
- [x] **Binary / source compatibility re-confirmed by differential listing.** Extracting all
  `def` declarations from `git show 8d528f3c:.../EnsembleByKey.scala` yields the identical
  23-member public surface as the working tree (`this`, `getKeys`/`setKeys` x2/`setKey`,
  `getCols`/`setCols` x2/`setCol`, `getColNames`/`setColNames` x2/`setColName`,
  `getStrategy`/`setStrategy`, `getCollapseGroup`/`setCollapseGroup`,
  `getVectorDims`/`setVectorDims` x2, `transform`, `transformSchema`, `copy`). The three additions
  (`columnNamesMatch`, `resolveKeyField`, `getSchemaFields`) are all `private`. `object
  EnsembleByKey extends DefaultParamsReadable` and `DefaultParamsWritable` are unchanged; no MiMa
  gate exists in `build.sbt`.
- [x] **Generated-code impact re-checked.** No `Param` declaration, name, doc string or validator
  changed, so `Wrappable` codegen for the Python/R/.NET wrappers is unaffected and no regeneration
  is required. The diff touches nothing under `target/`, `website/versioned_docs/`, or any generated
  tree.
- [x] **Security checklist - NOT APPLICABLE, with reasons (re-confirmed for the newest code).** The
  Issue 6/7 fixes add no I/O, serialization, credential handling, or untrusted-input parsing. The
  only new external read is `spark.conf.get("spark.sql.caseSensitive", "false")` (`:88`), a
  registered boolean `SQLConf` entry whose value is validated by `SQLConf.setConfString` at set
  time, so `.toBoolean` cannot throw on a session that was constructible. New string interpolations
  (`:97`, `:100`, `:108`, `:110`, `:119`, `:127-128`, `:137`) build exception messages only; none
  reaches a SQL parser, shell, path or URL - column names flow through the typed
  `Column`/`StructType` API. `getStrategy` remains constrained by `allowedStrategies` (`:58`).
  Echoing schema field names in errors matches `StructType.apply`'s own behaviour.
- [ ] **Not verified: full `core` test suite / CI matrix.** Only the focused
  `EnsembleByKeySuite` report, a `core` compile, and `core` + `core/Test` scalastyle were executed.
  Python/R/.NET wrapper tests and the codegen task were not run; they are unaffected by reasoning
  above (no `Param` change) but that reasoning is not backed by an execution here.

---

## Issues

### Issue 8: `transformSchema` over-reports columns when an output name has a case-variant twin in the input schema (non-collapse)
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 182 (`needToDrop`) and 195-196 (`outputNameSet` / `inputFields`)
- **Description**: In the non-collapse path, `transform` computes
  `needToDrop = outputNames.toSet & dataset.columns.toSet` - an **exact-case** set intersection -
  and then calls `dataset.drop(needToDrop.toList: _*)`. `Dataset.drop(colNames: String*)` resolves
  its arguments through the analyzer resolver, so under the default
  `spark.sql.caseSensitive = false` it removes **every case-variant** of each name it is given, not
  just the exact match. `transformSchema` filters passthrough fields with an exact-case
  `outputNameSet(f.name)` test (`:195-196`), so it retains the case-variant that `drop` actually
  deletes.

  Verified by execution against the compiled current code (Spark 3.5.0, JDK 11, `local[2]`,
  `spark.sql.caseSensitive = false`), input schema `id, group, score, features, FEATURES`,
  `setKey("group").setCol("score").setColName("features").setCollapseGroup(false)`:

  ```
  PROBE-D-DROP  : id,group,score                      // drop("features") removed FEATURES too
  PROBE-A-SCHEMA: group,id,score,FEATURES,features    // transformSchema  (5 fields)
  PROBE-A-ACTUAL: group,id,score,features             // transform        (4 fields)
  PROBE-A-MATCH : false
  PROBE-A-ROW   : [a,0,1.0,1.0]
  ```

  Note the trigger requires an *exact* match (to populate `needToDrop`) **plus** a case-variant twin
  (which `drop` also removes). When the output name only differs in case from an existing column,
  `needToDrop` is empty, nothing is dropped, and schema and data agree - so this is narrow, but it
  is a genuine violation of the contract this PR exists to establish. The "overwrite an existing
  column" configuration itself is a first-class supported feature
  (`EnsembleByKeySuite.scala:96` and `:253`).
- **Risk**: Low. `Pipeline`/`PipelineModel` schema propagation and any downstream stage validated
  against `transformSchema` will see a column that does not exist at runtime, turning a static
  schema check into a deferred runtime failure. Confined to schemas containing two fields whose
  names differ only in case, under `collapseGroup = false`.
- **Suggested Fix**: Use the same case-aware predicate already introduced for keys, on both sides:

  ```scala
  // transform (:182)
  val needToDrop = dataset.columns.filter(c => outputNames.exists(columnNamesMatch(c, _)))
  // transformSchema (:196)
  val inputFields = schema.fields.filterNot(f =>
    keyNameSet(f.name) || outputNames.exists(columnNamesMatch(f.name, _)))
  ```

  Add a regression test with a `features`/`FEATURES` schema asserting
  `transformer.transformSchema(df.schema) === transformer.transform(df).schema`.

### Issue 9: Ambiguity detection was added for `keys` but not for `cols`, so `transformSchema` still passes configurations `transform` cannot execute
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 132 (`schema(inputName)`) vs 121 (`resolveKeyField`)
- **Description**: `keys` now go through `resolveKeyField` (`:92-102`), which honours the configured
  case semantics and raises an actionable `"<name> is ambiguous. Matches: ..."` error. `cols` still
  use `schema(inputName)` (`:132`), i.e. `StructType.apply`'s exact-case `nameToField` lookup, while
  the aggregate expression built at `:163-173` (`mean(inColName)` / `Summarizer.mean(col(x))`) is
  resolved by the analyzer under the *configured* case semantics. With a duplicate-case schema this
  makes `transformSchema` report success for a configuration that `transform` cannot run.

  Verified by execution (same environment, `spark.sql.caseSensitive = false`), schema
  `group, score, SCORE`, `setKey("group").setCol("score")`:

  ```
  PROBE-F-SCHEMA       : group,mean(score)
  PROBE-F-TRANSFORM-THREW: [AMBIGUOUS_REFERENCE] Reference `score` is ambiguous, could be:
                           [`score`, `score`].
  ```

  This is pre-existing `transform` behaviour and not a regression - but it is the same failure class
  the PR deliberately closed for `keys`, and this round's brief calls out ambiguous duplicate-case
  fields specifically. The asymmetry also means the two column families now fail with very different
  diagnostics for identical input shapes.
- **Risk**: Low. Fail-open schema validation: a `Pipeline` schema check passes and the job fails
  later with a Spark `AnalysisException` from inside the aggregate rather than an actionable
  configuration error. Only reachable with two schema fields differing solely in case.
- **Suggested Fix**: Route `cols` through the same resolver as `keys` (generalise `resolveKeyField`
  to `resolveField(schema, name)` and use it at `:132`), so ambiguous or mis-cased aggregate inputs
  fail fast with the same message shape. Add one row to the negative-configuration table at
  `EnsembleByKeySuite.scala:175-189` pinning it.

---

## Notes (no action required, re-review 2)

- The `SparkSession.getActiveSession` + `spark.sql.caseSensitive` design is the right call and is
  better than the `SQLConf.get` alternative suggested in Issue 6, because it avoids a Spark-internal
  API while reading the *same* underlying conf: `SQLConf.get`'s getter is
  `SparkSession.getActiveSession.map(_.sessionState.conf).getOrElse(fallbackConf)`. I confirmed by
  execution that Spark's analyzer follows the **active thread-local session**, not
  `dataset.sparkSession` (`PROBE-B-SPARK-GROUPBY`, `PROBE-C-SPARK-GROUPBY-THREW`), and that with no
  active session both Spark and this code fall back to case-insensitive
  (`PROBE-G-SPARK-GROUPBY-UPPER`, `PROBE-G-EBK`). Reading `dataset.sparkSession.conf` in `transform`
  would actually have *introduced* a divergence from Spark.
- `transform` still writes `defaultParamMap` at `:148-150`. This is unchanged pre-PR behaviour, the
  Issue 2 purity guarantee was correctly scoped to `transformSchema`, and it is benign because
  `getSchemaFields` reads `get(colNames)` (`:115`), which never observes a default.
- `transform` validates via `getSchemaFields` (`:146`) instead of calling `transformSchema`.
  Validation is identical; the only lost behaviour is the base
  `PipelineStage.transformSchema(schema, logging = true)` debug log.
- `vectorDims` (`:75-82`) is still accepted but unused by the transformer. That is pre-existing dead
  configuration, untouched by this PR, and out of scope here.
- The exact-string assertion at `EnsembleByKeySuite.scala:216`
  (`error.getMessage === "Cannot operate on type IntegerType with strategy mean"`) is still coupled
  to `DataType.toString`. Correct for Spark 3.5.0; a `contains` check would be more upgrade-proof.
  Not raised as an issue.

---

## Resolution Log - Round 1 Re-review 2
_Open; to be updated by the driving agent._

### Issue 8
- **Status**: Fixed
- **What changed**: Captured the active Spark case-sensitivity setting in a
  private `ResolvedColumns` value. Non-collapse `transform` now identifies all
  resolver-equivalent input fields before `drop`, and `transformSchema` uses
  the same predicate when removing passthrough fields. Added coverage for an
  input containing both `features` and `FEATURES` under both analyzer modes.
- **Why**: Spark's string-based `drop` follows analyzer resolution. Schema
  filtering must use the same resolver semantics or it can retain fields that
  runtime output removes.
- **How verified**: The JDK 11 focused suite passed all 16 tests. The new
  case-variant overwrite test asserts `transformSchema === transform.schema`
  and exact field ordering with case sensitivity both disabled and enabled.

### Issue 9
- **Status**: Fixed
- **What changed**: Generalized key-only lookup to `resolveField` and now
  resolves aggregate input columns through it before deriving types or Spark
  expressions. `transform` uses the resolved input field names, while default
  output aliases continue to derive from the configured `cols` for
  compatibility. Added ambiguous aggregate-column coverage.
- **Why**: Aggregate type validation and Spark expression resolution must
  select the same field or reject the same ambiguity to keep schema validation
  executable.
- **How verified**: The focused suite passed all 16 tests, including a
  `score`/`SCORE` ambiguity assertion; compilation and `git diff --check`
  succeeded.

## Round 1 Re-review 3

## Review Summary
- **Round**: 1 (re-review 3)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 1 (new); Issues 1-9 all CONFIRMED FIXED
- **Verdict**: ISSUES_FOUND

**Prior findings status: 9 of 9 confirmed fixed, independently, by execution rather than by
reading the resolution log.** Issues 8 and 9 are closed: 32 executed schema-vs-transform probes
against the *compiled current code* (Spark 3.5.0, Scala 2.12.17, JDK 11.0.31, `local[2]`) show
`transformSchema(df.schema) == transform(df).schema` for every configuration reachable through the
transformer's own validation, including all four case-variant permutations that produced the Issue 8
mismatch and both duplicate-case-column permutations from Issue 9. The 16/16 claim was reproduced,
not trusted: I re-ran `core/testOnly ...EnsembleByKeySuite` myself.

One new Low issue: the schema contract still breaks under `spark.sql.retainGroupColumns=false`
(pre-existing, verified by execution, cheap to guard or document).

---

## Evidence Checklist

- [x] **Reviewed the authoritative regenerated diff, byte-for-byte.** Extracted the embedded diff
  from `.copilot/session-state/.../pr-2575/prompts/review-round-1.md` and string-compared it
  (newline-normalized) against
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`:
  both **18,098 characters, `IDENTICAL=True`**. Scope is exactly two files
  (`EnsembleByKey.scala` +113/-29 region, `EnsembleByKeySuite.scala` +222), 306 insertions /
  29 deletions. `git status` shows only these two modified files plus untracked `reviews/`.
- [x] **Read both changed files in full at their current state** (`EnsembleByKey.scala`, 214 lines;
  `EnsembleByKeySuite.scala`, 321 lines). All line references below are to the current working tree.
  Also read the **base** version (`git show 8d528f3c:...EnsembleByKey.scala`) to separate regressions
  from pre-existing behaviour.
- [x] **Executed 32 differential probes against the compiled classes**, not against reasoning. Method:
  `sbt --error 'export core/fullClasspath'` → `scala.tools.nsc.MainGenericRunner -usejavacp` REPL, no
  files written to the repo, no source modified. Each probe calls `transformSchema(df.schema)` and
  `transform(df).schema` on the same instance and compares full `StructType`s (name, dataType,
  nullable, metadata) or compares thrown exception types/messages. Results:
  **30/32 MATCH=true; the 2 mismatches are `spark.sql.retainGroupColumns=false` (Issue 10) and two
  pre-existing Spark-resolution gaps recorded in Notes.**
- [x] **Issue 8 (case-variant drop/schema divergence) — CONFIRMED FIXED.** Schema
  `id, key, score, features, FEATURES`, `setKey("key").setCol("score").setColName(...)`,
  `collapseGroup=false`, all four permutations:
  ```
  PROBE|14a-casevariant-ci        |MATCH=true|key,id,score,features
  PROBE|14b-casevariant-cs        |MATCH=true|key,id,score,FEATURES:string,features:double
  PROBE|14c-casevariant-upper-ci  |MATCH=true|key,id,score,FEATURES
  PROBE|14d-casevariant-upper-cs  |MATCH=true|key,id,score,features:string,FEATURES:double
  ```
  This is exactly the `PROBE-A-MATCH: false` case from re-review 2, now true in both analyzer modes
  and in both name directions. The fix is correct on both sides: `transform`'s `needToDrop`
  (`:188-190`) now filters `dataset.columns` through `columnNamesMatch`, which is the same predicate
  `Dataset.drop(colNames: String*)` applies via the analyzer resolver, and `transformSchema`'s
  passthrough filter (`:203-206`) uses the identical predicate. Pinned by
  `EnsembleByKeySuite.scala:123-144`.
- [x] **Issue 9 (ambiguity for `cols`) — CONFIRMED FIXED.** `cols` now go through the same
  `resolveField` as `keys` (`:126`), so the `score`/`SCORE` case fails fast identically in both
  entry points instead of failing open:
  ```
  PROBE|15a-ambiguous-col-ci |MATCH=true|SCHEMA=THREW IllegalArgumentException: score is ambiguous. Matches: score, SCORE
                                        |ACTUAL=THREW IllegalArgumentException: score is ambiguous. Matches: score, SCORE
  PROBE|15b-ambiguous-col-cs |MATCH=true|group,mean(score)     // caseSensitive=true resolves exactly
  PROBE|15c-ambiguous-col-cs-upper|MATCH=true|group,mean(SCORE)
  ```
  Note 15b/15c prove the fix is *not* over-strict: under `spark.sql.caseSensitive=true` the same
  schema resolves exactly and both paths agree. Pinned by `EnsembleByKeySuite.scala:225-233`.
  Verified `logTransform` re-throws these `IllegalArgumentException`s unchanged (probes 09/12/13 show
  byte-identical messages from `transform` and `transformSchema`).
- [x] **Issues 1 + 6 (key case resolution / conf awareness) — CONFIRMED FIXED, no regression.**
  ```
  PROBE|16a-miscased-key-ci|MATCH=true|group,region,id,...,mean(doubleScore)   // setKeys("GROUP","REGION")
  PROBE|16b-miscased-key-cs|MATCH=true|both THREW "GROUP does not exist. Available: id, region, group, ..."
  ```
  Under case-insensitive analysis the emitted key names are the *resolved schema* names, which is
  what `groupBy` (`:182`) and `join` (`:191`) actually emit; under `caseSensitive=true` both entry
  points reject identically. `columnNamesMatch` (`:93-95`) is exactly Spark's
  `caseSensitiveResolution` (`_ == _`) / `caseInsensitiveResolution` (`_.equalsIgnoreCase(_)`).
- [x] **Issue 2 (purity) — CONFIRMED FIXED, measured on a live instance.**
  ```
  PROBE|18-purity-before        |isSet=false|default=None|params=collapseGroup,cols,keys,strategy
  PROBE|18-purity-afterSchema   |isSet=false|default=None
  PROBE|18-purity-afterTransform|isSet=false|default=Some(mean(doubleScore))
  PROBE|31-idempotent           |same=true
  ```
  `transformSchema` performs zero param writes (the only `setDefault` outside construction is
  `:154-156`, inside `transform`, i.e. exactly the pre-PR behaviour), and repeated calls are
  identical. Default-name refresh still tracks `cols`:
  `PROBE|18-purity-afterSetCols|schema=group,mean(floatScore)` after `setCols("floatScore")`.
- [x] **Issue 3 (key/output collision) — CONFIRMED FIXED.**
  `PROBE|09-noncollapse-outname-eq-key|MATCH=true` — both paths throw
  `requirement failed: Output columns group cannot overwrite grouping keys group when collapseGroup is false`.
  The guard (`:128-135`) is correctly scoped to `collapseGroup=false`; with collapse enabled the
  duplicate name is legal and both paths agree
  (`PROBE|08|MATCH=true|group:string,group:double`, and `PROBE|30` for the 2-key permuted variant
  `group,region,region:double,group:double`).
- [x] **Issue 4 (length mismatch) — CONFIRMED FIXED.** `require` at `:120-122` runs before any `zip`,
  drop, or aggregation; `nonEmpty` guards at `:115-116` protect `keyNames.head`/`newCols.head`
  (`:182-183`). Empty-array cases verified end-to-end:
  `PROBE|12-empty-cols|MATCH=true` and `PROBE|13-empty-keys|MATCH=true`, both throwing
  `requirement failed: cols/keys must be set and non-empty` from *both* entry points.
- [x] **Issues 5 + 7 (fail-fast coverage) — CONFIRMED FIXED.** `EnsembleByKeySuite.scala:189-234`
  is a table-driven negative test covering unset `keys`, unset `cols`, missing key, missing
  aggregate col, length mismatch, key/output collision, ambiguous key **and** ambiguous aggregate
  col; `:236-245` covers the unsupported type. All `.contains` assertions are robust to the
  `"requirement failed: "` prefix that `require` prepends, while the direct `throw`s at `:101`/`:104`
  carry no prefix — verified against real messages captured in the probes.
- [x] **Independently re-ran the focused suite and the lint gates** (WSL Ubuntu, JDK 11.0.31,
  sbt 1.10.11, `sbt --batch core/scalastyle core/Test/scalastyle 'core/testOnly ...EnsembleByKeySuite'`):
  ```
  scalastyle (scalastyle-config.xml)      Processed 211 file(s)  Found 0 errors  0 warnings
  scalastyle (scalastyle-test-config.xml) Processed 150 file(s)  Found 0 errors  0 warnings
  [info] Suites: completed 1, aborted 0
  [info] Tests: succeeded 16, failed 0, canceled 0, ignored 0, pending 0
  [success] Total time: 116 s, completed Aug 1, 2026, 6:10:48 AM
  ```
  The **16/16 claim is reproduced, not trusted**. I also audited the pre-existing report
  `core/target/test-reports/TEST-...EnsembleByKeySuite.xml` (`tests=16 errors=0 failures=0
  skipped=0`, all 16 cases enumerated: 13 declared + 3 auto-registered by the fuzzing traits) and
  confirmed it postdates the current sources by timestamp chain: sources `22:37:56` → main classes
  `22:41:21` → test classes `22:41:38` → run start `22:41:42` → report `22:43:02`. Compiled artifacts
  correspond to the current code (`EnsembleByKey$ResolvedColumns.class` exists, a type introduced by
  the Issue 8/9 fixes).
- [x] **Output types / nullability / metadata verified empirically, not just by reading Spark.**
  `PROBE|01` and `PROBE|02` compare full `StructField`s including metadata:
  float-mean → `double:nullable`, double-mean → `double:nullable`, vector-mean →
  `vector:NOT NULL` with empty metadata, while a *passthrough* `features` column keeps its
  VectorAssembler `ml_attr` metadata (`features:vector:n:MD`) in both schema and actual output.
  Nullable inputs behave identically (`PROBE|22-nullable-inputs|MATCH=true` for
  `Option[Double]`/`Option[Float]` columns), and an empty input DataFrame matches
  (`PROBE|23-empty-df|MATCH=true`).
- [x] **Non-collapse drop/join ordering verified by execution for all orderings.**
  `keyFields ++ inputFields ++ aggregateFields` (`:207`) reproduces Spark's
  `commonNaturalJoinProcessing` project list (`leftKeys ++ lUniqueOutput ++ rUniqueOutput`) in every
  probe, including cases where key order differs from schema order (`PROBE|02`,
  `PROBE|05`, `PROBE|11`), where a key is also an aggregate input (`PROBE|10`), and where a key is
  repeated (`PROBE|24-duplicate-keys|MATCH=true`, emitting `group,group,...` on both sides). Row
  count is preserved in non-collapse mode (`PROBE|32|in=3|out=3`) and data is correct under
  case-insensitive key resolution (`PROBE|20`: `foo/…/1.0 ; bar/…/2.0 ; bar/…/2.0`).
- [x] **Duplicate / collision permutations swept.** Duplicate `cols` entries
  (`PROBE|06|MATCH=true`, two identical aggregates), duplicate `colNames`
  (`PROBE|07|MATCH=true`), output name equal to an existing passthrough column
  (`PROBE|11|MATCH=true`), overwrite of numeric + vector inputs (`PROBE|03|MATCH=true`),
  mis-cased aggregate input with derived default alias (`PROBE|04`, `PROBE|05`: alias keeps the
  *configured* casing `mean(DOUBLESCORE)` while the aggregate binds the *resolved* field — both
  paths agree because both consume `resolvedColumns.outputNames`), and an output name that is a
  case-variant of a key under `caseSensitive=true` (`PROBE|21|MATCH=true`, correctly *not* rejected
  because the guard is case-mode aware).
- [x] **Default-alias derivation reviewed for the Issue 9 refactor.** `outputNames` (`:118-119`)
  deliberately derives from the **configured** `cols` casing, not the resolved field names, which
  preserves pre-PR default naming (`s"$getStrategy($name)"`, identical to base `:88`). Because both
  `transform` and `transformSchema` consume the same `ResolvedColumns.outputNames`, there is no
  divergence — confirmed by `PROBE|04`/`PROBE|05`.
- [x] **Serialization / persistence unaffected.** `Serialization Fuzzing` (69.0 s of the 16-test run)
  wraps the stage in a `Pipeline`, fits, round-trips a `PipelineModel` and compares full DataFrames,
  exercising `transformSchema` through the `Pipeline`/`PipelineModel` entry points with
  `collapseGroup=false`. No `Param` declaration, `DefaultParamsWritable`/`DefaultParamsReadable`
  wiring, or `copy` behaviour changed. Because `getSchemaFields` reads `get(colNames)` (`:118`),
  which never observes `defaultParamMap`, a loaded model recomputes names from `cols` exactly as a
  fresh one does.
- [x] **JVM binary + Scala source compatibility confirmed from the compiled class, not the source.**
  `javap -cp core/target/scala-2.12/classes com.microsoft.azure.synapse.ml.stages.EnsembleByKey`
  shows the public surface unchanged: `public StructType transformSchema(StructType)`,
  `public Dataset<Row> transform(Dataset<?>)`, `copy(ParamMap)` (+ bridges), all
  `getKeys/getCols/getColNames/getStrategy/getCollapseGroup/getVectorDims` getters, all
  `setKey(s)/setCol(s)/setColName(s)/setStrategy/setCollapseGroup/setVectorDims` overloads
  (`String`, `String[]`, `Seq[String]`, `java.util.HashMap`), both constructors, and
  `object EnsembleByKey extends DefaultParamsReadable`. Everything added by this PR is `private`
  (`columnNamesMatch`, `resolveField`, `getSchemaFields`, `ResolvedColumns`); the only new public
  symbols are synthetic `$anonfun$…` lambdas, which are not API. No MiMa gate exists in `build.sbt`.
- [x] **Generated-code / wrapper impact re-checked.** No `Param` declaration, description, default,
  or validator changed, so `Wrappable` Python/R/.NET codegen output is byte-identical and no
  regeneration is required. The diff touches nothing under `target/`, `website/versioned_docs/`, or
  any generated tree. Repo-wide `git grep -i EnsembleByKey` outside the two changed files returns
  only `EnsembleByKey.txt` (doc blurb), `docs/Quick Examples/transformers/core/_Stages.md`, versioned
  website copies, and an unrelated 2018 Zeppelin notebook dump — **no Scala, Python, R, or .NET
  consumer**. The published example (`setKeys(["label1"]).setCols(["score1"])`, default `colNames`,
  `collapseGroup=true`) satisfies every new guard.
- [x] **Style/lint gates executed, not estimated.** Both scalastyle configs pass with 0 errors
  (above). Measured max line length 109 (main) / 115 (test) against `maxLineLength=120`; file lengths
  214 / 321 against `maxFileLength=800`; zero tabs; `git --no-pager diff --check` exits 0. MIT
  headers, `Wrappable`/`DefaultParamsWritable`/`SynapseMLLogging` + `logClass(FeatureNames.Core)`
  wiring, and the `logTransform[DataFrame]` wrapper (`:150`, `:193`) are retained.
- [x] **Security checklist — NOT APPLICABLE, with reasons (re-confirmed for the Issue 8/9 code).**
  The new code adds no file/network/process I/O, no serialization format change, no credential or
  secret handling, and no untrusted-input parsing. Every new string interpolation (`:102`, `:105`,
  `:111`, `:113`, `:119`, `:122`, `:133-134`, `:142`) builds an exception message or a column alias;
  none reaches a SQL parser, shell, path, or URL — column names flow through the typed
  `Column`/`StructType` API. `getStrategy` remains constrained by `allowedStrategies` (`:58-60`).
  The `spark.sql.caseSensitive` read at `:124-125` uses `RuntimeConfig.get`, and Spark validates
  boolean conf values at set time (`SQLConf.setConfString` → `entry.valueConverter`), so
  `.toBoolean` cannot be fed an unparseable value; it also parses identically to Spark's own
  `booleanConf` converter. Error messages echo schema field names, which is exactly what
  `StructType.apply` does upstream — not a disclosure vector.
- [x] **`SparkSession.getActiveSession` choice re-validated as the correct conf source.** Spark's
  own `SQLConf.get` getter is
  `SparkSession.getActiveSession.filterNot(_.sparkContext.isStopped).map(_.sessionState.conf).getOrElse(fallbackConf)`,
  and `Dataset.drop(colNames: String*)` resolves through `sessionState.analyzer.resolver`, i.e. the
  same active-session conf. Reading `dataset.sparkSession.conf` instead would have *introduced* a
  divergence, and `transformSchema(schema)` has no dataset to read from at all. Confirmed the
  no-active-session fallback (case-insensitive) matches Spark's `fallbackConf` default.
- [ ] **Not verified: full `core` suite / CI matrix / Python-R-.NET wrapper tests.** Only
  `EnsembleByKeySuite` plus both scalastyle configs were executed here. The wrapper tests are
  unaffected by the reasoning above (no `Param` change) but that reasoning is not backed by an
  execution in this session.
- [ ] **Not verified: multi-session (`spark.newSession()`) conf skew.** Constructing a case where the
  active session's `spark.sql.caseSensitive` differs from the dataset's session was judged out of
  scope: Spark itself mixes the two (analysis runs under `sparkSession.withActive`, while
  `Dataset.drop` resolves under the active session), so any residual skew is upstream behaviour, not
  a property of this diff.

---

## Requirement → Test Mapping (re-review 3 delta)

| Requirement (from Issues 8/9) | Covered by | Verdict |
| --- | --- | --- |
| Non-collapse drop and schema filtering use identical resolver semantics | `EnsembleByKeySuite.scala:123-144` (both analyzer modes, exact field-order assertion) + `PROBE 14a-14d` | Covered |
| Ambiguous aggregate column rejected up front | `EnsembleByKeySuite.scala:225-233` + `PROBE 15a` | Covered |
| Case-sensitive mode still resolves duplicate-case columns exactly (no over-strictness) | `PROBE 15b`, `PROBE 15c` | Not covered by a test (code path shared with keys, which *is* tested at `:180-187`); acceptable |
| Aggregate input resolution does not change default alias derivation | `PROBE 04`, `PROBE 05` | Behaviour verified; no test pins the mis-cased-`cols` alias |

---

## Issues

### Issue 10: `transformSchema` still contradicts `transform` under `spark.sql.retainGroupColumns=false`
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 199-208 (`transformSchema`), 181-191 (`transform`)
- **Description**: `transformSchema` unconditionally prepends `resolvedColumns.keyFields` in both
  branches, but `RelationalGroupedDataset.toDF` only emits the grouping columns when
  `spark.sql.retainGroupColumns` (read from `df.sparkSession.sessionState.conf`) is true. With that
  public conf set to `false`, verified by execution against the current compiled code:

  ```
  PROBE|17a-retainfalse-collapse   |MATCH=false
      |SCHEMA=group:string:n,mean(doubleScore):double:n
      |ACTUAL=mean(doubleScore):double:n
  PROBE|17b-retainfalse-noncollapse|MATCH=false
      |SCHEMA=group:string:n,id:int:!,region:string:n,...,mean(doubleScore):double:n
      |ACTUAL=THREW AnalysisException: [UNRESOLVED_USING_COLUMN_FOR_JOIN] USING column `group`
              cannot be resolved on the right side of the join.
  ```

  Collapse mode silently over-reports the key column; non-collapse mode is fail-open — schema
  validation passes for a configuration `transform` cannot execute, because `aggregated` no longer
  carries the join keys. The diff reads `spark.sql.caseSensitive` (`:124-125`) but not this conf, so
  the asymmetry is visible in the same method.

  This is **not a regression**: the base `transformSchema` returned `schema.fields ++ newFields`
  for collapse mode, which was wrong under either conf value, and the non-collapse join has always
  required the keys on the right side. But it is the one remaining configuration I could construct
  where the contract this PR exists to establish does not hold, and it is reachable through a
  documented Spark SQL conf rather than through a malformed schema.
- **Risk**: Low. `Pipeline`/`PipelineModel` schema propagation reports a column that will not exist
  at runtime (collapse), or validates a configuration that throws deep inside the join
  (non-collapse). Confined to sessions that explicitly set `spark.sql.retainGroupColumns=false`;
  the repo sets it nowhere (`git grep retainGroupColumns` → no hits) and the Spark default is `true`.
- **Suggested Fix**: Either (a) read the conf next to `caseSensitive` at `:124-125` and omit
  `keyFields` from the collapse branch when it is false, plus reject `collapseGroup=false` with an
  actionable message since the join cannot work; or (b) reject the conf outright with a clear
  `require`; or (c) **defer** with a one-line comment stating that `EnsembleByKey` assumes
  `spark.sql.retainGroupColumns=true`. Deferral is a reasonable resolution here: `transformSchema`
  has no `Dataset`, so it cannot read the conf from the same session `RelationalGroupedDataset`
  does (the same accepted limitation already documented for `spark.sql.caseSensitive`), and the
  non-collapse path is independently broken under that conf regardless of what the schema says.

---

## Notes (no action required, re-review 3)

- **Non-orderable grouping keys are still validated fail-open** (pre-existing, unchanged by this
  diff): `PROBE|26-map-key|MATCH=false` — `transformSchema` returns `m:map<string,string>,
  mean(score):double` while `transform` throws `[GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE]`.
  Replicating Spark's `RowOrdering.isOrderable` rules inside the transformer is out of scope; a
  vector key, by contrast, works in both paths (`PROBE|25|MATCH=true`).
- **Column names containing dots are still validated fail-open** (pre-existing, unchanged, and
  shared by essentially every string-column SynapseML stage): `PROBE|27-dotted-col|MATCH=false` —
  `resolveField` matches the literal field `a.b`, but `functions.mean("a.b")` goes through
  `UnresolvedAttribute.quotedString`, which splits on `.`, so `transform` throws
  `[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... \`a\`.\`b\``. Fixing this would require switching to
  backtick-quoted `Column` construction, a behaviour change beyond this PR. Names containing spaces
  are fine (`PROBE|28|MATCH=true`).
- **`getColNames` can report stale names between transforms** (pre-existing, identical in the base
  file): `PROBE|18-purity-afterSetCols|getColNames=mean(doubleScore)` after `setCols("floatScore")`,
  because `transform` writes `defaultParamMap` at `:154-156` and `setCols` does not clear it. The
  transformer's own derivation is unaffected — the very next `transformSchema` correctly returns
  `mean(floatScore)` — because `getSchemaFields` reads `get(colNames)` (`:118`), which never sees a
  default. Only the public getter / `extractParamMap` / persisted metadata observe the stale value.
- **`transform` still mutates `defaultParamMap`** (`:154-156`). Pre-existing; the Issue 2 purity
  guarantee was correctly scoped to `transformSchema`, which I re-verified performs zero writes.
- **Behaviour change worth a release note**: with `collapseGroup=false` under case-insensitive
  analysis, a *case-variant* key/output collision (key `group`, `colName` `GROUP`) used to run in
  the base code — `needToDrop` was an exact-case intersection, so nothing was dropped and the join
  succeeded, producing a DataFrame with two case-colliding columns that no downstream name-based
  reference could resolve. The Issue 8 fix makes `needToDrop` resolver-aware, which would now drop
  the key, so the guard at `:128-135` correctly rejects the configuration up front. Net effect: a
  previously "working" but unusable output is now an actionable error. Same class of intentional,
  user-visible hardening applies to the new length-mismatch, missing-column, and ambiguity errors
  (Issues 4/5/9) — all previously silent misconfigurations.
- **`transform` validates via `getSchemaFields` (`:151`) rather than calling `transformSchema`.**
  Validation is identical (same method); the only lost behaviour is the base
  `PipelineStage.transformSchema(schema, logging = true)` debug log, and any override in a
  hypothetical subclass would be bypassed. `EnsembleByKey` has no subclasses in this repo.
- **`vectorDims` (`:76-83`) remains accepted but unused.** Confirmed against the base file — this is
  pre-existing dead configuration, not something this PR removed.
- **The exact-string assertion at `EnsembleByKeySuite.scala:244`** (`error.getMessage === "Cannot
  operate on type IntegerType with strategy mean"`) is still coupled to `DataType.toString`; correct
  on Spark 3.5.0 (re-confirmed: `PROBE|29-struct-col` produces
  `Cannot operate on type StructType(StructField(inner,DoubleType,false)) with strategy mean`).

---

## Resolution Log - Round 1 Re-review 3
_Open; to be updated by the driving agent._

### Issue 10
- **Status**: Fixed
- **What changed**: `getSchemaFields` now reads
  `spark.sql.retainGroupColumns` from the active `SparkSession` and rejects
  `false` with a clear `IllegalArgumentException` before either schema
  construction or aggregation. Added a test asserting both `transformSchema`
  and `transform` fail with the same contract.
- **Why**: `EnsembleByKey` requires grouping keys in its collapsed output and
  as join columns in non-collapse mode. Spark cannot provide that invariant
  when grouping-column retention is disabled, so an explicit fail-fast
  requirement is safer than publishing an impossible schema.
- **How verified**: The JDK 11 focused suite passed all 17 tests, including the
  disabled-retention case in both entry points; compilation and
  `git diff --check` succeeded.


---

## Round 1 Re-review 4

## Review Summary
- **Round**: 1 (re-review 4)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5 (Slot 1, latest Anthropic Opus — same artifact token as prior rounds)
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 2 (new); Issues 1-10 all CONFIRMED FIXED
- **Verdict**: ISSUES_FOUND

**Prior findings status: 10 of 10 confirmed fixed by execution, not by reading the Resolution Log.**
Issue 10's chosen resolution (fail-fast `require` on `spark.sql.retainGroupColumns`) works for the
single-session case: `transformSchema` and `transform` now throw the *same*
`IllegalArgumentException` instead of publishing an unrealisable schema (`PROBE|B1`, `B2`, `B6`).

Two new Low issues, both located in the four lines the Issue 10 fix added
(`EnsembleByKey.scala:124-131`), both demonstrated by execution and both A/B-compared against the
base commit to separate "new failure mode" from "pre-existing":
1. the guard reads the **thread-active** session while Spark reads group-column retention from the
   **dataset's** session, so the guard is simultaneously fail-open (Issue 10's divergence is still
   reachable) and fail-closed (rejects datasets Spark would have aggregated correctly);
2. the conf strings are parsed with `.toBoolean` instead of Spark's `.trim.toBoolean`, so a
   whitespace-padded value that Spark accepts turns every call into an unrelated-looking crash.

---

## Evidence Checklist

- [x] **Reviewed the authoritative regenerated diff, byte-for-byte.** Extracted the embedded diff from
  `.copilot/session-state/c2ea157b-.../pr-2575/prompts/review-round-1.md` and compared it
  (newline-normalised) with
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`:
  **both 19,157 characters, `IDENTICAL=True`**. Scope is exactly two files
  (`EnsembleByKey.scala` +120/-29 region, `EnsembleByKeySuite.scala` +241), 332 insertions /
  29 deletions. `git status --porcelain` shows only those two modified files plus untracked
  `reviews/`; `git --no-pager diff --check <base>` reports no whitespace errors.
- [x] **Read both changed files in full at their current state** (`EnsembleByKey.scala`, 221 lines;
  `EnsembleByKeySuite.scala`, 340 lines) and the **base** file
  (`git show 8d528f3c:...EnsembleByKey.scala`, 157 lines) to separate regressions from pre-existing
  behaviour. All line references below are to the current working tree.
- [x] **Independently re-ran the focused suite and both lint gates** (WSL Ubuntu, JDK 11.0.31,
  sbt 1.10.11, Spark 3.5.0/Scala 2.12.17):
  `sbt --batch core/scalastyle core/Test/scalastyle 'core/testOnly ...EnsembleByKeySuite'` →
  ```
  scalastyle (scalastyle-config.xml)      Processed 211 file(s)  Found 0 errors  0 warnings
  scalastyle (scalastyle-test-config.xml) Processed 150 file(s)  Found 0 errors  0 warnings
  [info] Suites: completed 1, aborted 0
  [info] Tests: succeeded 17, failed 0, canceled 0, ignored 0, pending 0
  [success] Total time: 125 s, completed Aug 1, 2026, 6:40:09 AM
  ```
  **The 17/17 claim is reproduced, not trusted.** I also audited the *pre-existing* report before
  re-running it: `core/target/test-reports/TEST-...EnsembleByKeySuite.xml` (`tests=17 errors=0
  failures=0 skipped=0`, run timestamp `2026-08-01T06:21:04`) postdates the current sources by an
  intact timestamp chain — sources `23:17:30` → main classes `23:20:45` (incl.
  `EnsembleByKey$ResolvedColumns.class`) → test classes `23:21:00` → run `23:21:04` → report
  `23:22:10`. Test count reconciles: 14 declared `test(` blocks + 3 auto-registered by the fuzzing
  traits (`Serialization Fuzzing`, `Experiment Fuzzing`, `Getters and Setters work as anticipated`),
  and the new retention case appears by name in the XML.
- [x] **Executed differential probes against the compiled current classes** (not against reasoning):
  compiled a throw-away `Probe`/`Probe2`/`Probe3`/`ProbeBase` outside the repository (session-state
  scratch dir, output under `$HOME`) against `sbt 'export core/Test/fullClasspath'`; each probe calls
  `transformSchema(df.schema)` and `transform(df).schema` on equivalent instances and compares full
  `StructType`s (name, dataType, nullable, metadata) or thrown type + message. **No repository file
  was created or modified** (`git status` unchanged before/after).
- [x] **Issue 10 — CONFIRMED FIXED for the single-session case.**
  ```
  PROBE|B1-retainfalse-collapse   |MATCH=true |both THREW IllegalArgumentException:
        requirement failed: EnsembleByKey requires spark.sql.retainGroupColumns=true
  PROBE|B2-retainfalse-noncollapse|MATCH=true |identical message from both entry points
  PROBE|B6-error-identical        |true       (byte-identical messages)
  PROBE|H1-noncollapse-exception  |java.lang.IllegalArgumentException  (logTransform re-throws unchanged)
  PROBE|B7-restored               |true       (no conf leakage from the guard)
  ```
  The previously reported collapse-mode over-report and the non-collapse
  `[UNRESOLVED_USING_COLUMN_FOR_JOIN]` fail-open are both gone in that configuration.
- [x] **New: guard vs. the session Spark actually consults — the residual defect (Issue 11).**
  ```
  PROBE|C0 |activeIsDefault=true|activeRetain=true|datasetRetain=false
  PROBE|C1-datasetsession-retainfalse-collapse   |MATCH=false
        |SCHEMA=group:string:n,mean(score):double:n |ACTUAL=mean(score):double:n
  PROBE|C2-datasetsession-retainfalse-noncollapse|MATCH=false
        |ACTUAL=THREW AnalysisException: [UNRESOLVED_USING_COLUMN_FOR_JOIN] USING column `group`
                cannot be resolved on the right side of the join.
  PROBE|D0-activeDefined=false
  PROBE|D1-noactive-datasetsession-retainfalse   |MATCH=false  (same divergence, guard skipped)
  PROBE|C3-raw-spark-on-good-session|SCHEMA=group:string:n,m:double:n
  PROBE|C4-active-retainfalse-dataset-retaintrue |both THREW "requires spark.sql.retainGroupColumns=true"
  ```
  `PROBE|B3` independently proves Spark honours the *dataset's* session
  (`raw groupBy under retain=false → mean(doubleScore) only`), and `C3` proves the reverse.
- [x] **A/B against the base commit — separates "deliberate hardening" from "new failure mode".**
  Compiled `8d528f3c`'s `EnsembleByKey.scala` into a separate output dir, placed it first on the
  classpath, and ran the identical probe body:
  ```
  BASEPROBE|BASE|1-retainfalse-collapse-transform                 |mean(score):double        <- worked
  BASEPROBE|PR  |1-retainfalse-collapse-transform                 |THREW IllegalArgumentException  <- intended
  BASEPROBE|BASE|2-active-retainfalse-dataset-retaintrue-transform|group:string,mean(score):double <- worked
  BASEPROBE|PR  |2-active-retainfalse-dataset-retaintrue-transform|THREW IllegalArgumentException  <- Issue 11
  BASEPROBE|BASE|3-dataset-retainfalse-transform                  |mean(score):double
  BASEPROBE|PR  |3-dataset-retainfalse-transform                  |mean(score):double  <- guard skipped
  BASEPROBE|BASE|4-whitespace-conf-transform                      |group:string,mean(score):double <- worked
  BASEPROBE|PR  |4-whitespace-conf-transform                      |THREW "For input string: \" false\"" <- Issue 12
  ```
- [x] **Conf-string parsing verified against Spark's own parser (Issue 12).**
  ```
  PROBE2|retain-TRUE-uppercase      |stored=[TRUE]  |spark=SPARK_OK[group,m]|ebk=group,mean(score)   OK
  PROBE2|retain-leading-space       |stored=[ true] |spark=SPARK_OK[group,m]|ebk=EBK_THREW IllegalArgumentException: For input string: " true"
  PROBE2|retain-trailing-space      |stored=[true ] |spark=SPARK_OK[group,m]|ebk=EBK_THREW ... "true "
  PROBE2|casesensitive-leading-space|stored=[ false]|spark=SPARK_OK[group,m]|ebk=EBK_THREW ... " false"
  PROBE2|retain-garbage             |set=SET_THREW  (SQLConf rejects non-boolean at set time)
  ```
  Spark stores the raw string and parses it with `SQLConf.toBoolean` = `s.trim.toBoolean`; the PR
  uses `.toBoolean` without `.trim` (`:126`, `:128`).
- [x] **Issues 1-9 — regression sweep re-executed on the current build, all still fixed.**
  ```
  PROBE|F1-casevariant-ci        |MATCH=true|key,id,score,features                  (Issue 8)
  PROBE|F2-casevariant-cs        |MATCH=true|key,id,score,FEATURES:string,features:double
  PROBE|F3-ambiguous-col-ci      |MATCH=true|both THREW "score is ambiguous. Matches: score, SCORE"  (Issue 9)
  PROBE|F4-miscased-key-ci       |MATCH=true|group,region,mean(doubleScore)         (Issues 1/6)
  PROBE|F5-outname-eq-key-noncollapse|MATCH=true|both THREW "cannot overwrite grouping keys"  (Issue 3)
  PROBE|F6-length-mismatch       |MATCH=true|both THREW "cols (2) and colNames (1) must have the same length"  (Issue 4)
  PROBE|F7-empty-cols/F8-empty-keys|MATCH=true|both THREW "must be set and non-empty"
  PROBE|F9-purity|before=(false,None)|afterSchema=(false,None)|afterTransform=(false,Some(...))   (Issue 2)
  PROBE|E2-transformer-on-casesensitive-session|MATCH=true (both entry points agree)              (Issue 7 coverage)
  ```
  `transformSchema` still performs **zero** param writes; only `transform` writes `defaultParamMap`
  (`:161-163`), exactly as the base did.
- [x] **Core contract re-verified end-to-end** (types, nullability, metadata, ordering, row counts):
  ```
  PROBE|A1-collapse-default-names  |MATCH=true|group,region,mean(doubleScore):double:n,
                                              mean(floatScore):double:n,mean(features):vector:!
  PROBE|A2-noncollapse-default-names|MATCH=true|keys ++ passthrough(features keeps ml_attr :MD) ++ aggregates
  PROBE|A3-noncollapse-overwrite   |MATCH=true|overwritten features:vector:! with metadata dropped
  PROBE|A4-key-is-also-agg-input   |MATCH=true
  PROBE|G1-rowcount|in=3|out=3
  PROBE|G2-values  |[0,1.0,1.0,[1.0,0.1]] ; [1,2.0,2.0,[2.0,-2.5]] ; [2,2.0,2.0,[2.0,-2.5]]
  ```
- [x] **Additional edge sweep for PR-caused divergence (none found beyond Issues 11/12).**
  ```
  PROBE3|1b-duplicate-exact-names   |MATCH=true|both THREW "score is ambiguous. Matches: score, score"
  PROBE3|3a/3b/3c decimal/long/timestamp|MATCH=true|identical "Cannot operate on type X with strategy mean"
  PROBE3|4a-retainfalse-missing-col |MATCH=true (retention error precedes column resolution)
  PROBE3|4c-retainfalse-empty-keys  |MATCH=true (param validation still precedes the conf check)
  PROBE3|6a-empty-df                |MATCH=true|group,mean(score)
  PROBE3|7a-vector-key              |MATCH=true|vec:vector,mean(score)
  ```
- [x] **Binary / source compatibility of the new fail-fast confirmed mechanically.** `javap -p` on the
  compiled `EnsembleByKey.class`: `transform(Dataset)`, `transformSchema(StructType)`, `copy`,
  `write`/`read`/`load`, every `keys`/`cols`/`colNames`/`strategy`/`collapseGroup`/`vectorDims`
  param, getter and setter overload (incl. `setCols(Seq)`/`setCols(String[])` and
  `setVectorDims(java.util.HashMap)`) are present and unchanged; the only additions are the private
  `getSchemaFields`/`resolveField`/`columnNamesMatch`, the private nested `ResolvedColumns`, and
  compiler-synthetic `$anonfun$*` statics. **No public signature added, removed, or narrowed**, so
  the change is source- and binary-compatible; the break is purely behavioural (new
  `IllegalArgumentException` on a previously-executing configuration). The build has **no MiMa
  gate** (`grep -i mima build.sbt project/plugins.sbt` → no hits), so nothing else enforces this.
- [x] **Generated-code impact re-checked**: no `Param` was added, renamed or re-typed, so the
  `Wrappable` Python/R codegen output is unchanged; no files under `target/`, `generated/`, or
  `website/` are touched by the diff. The new failure surfaces to PySpark as the usual
  `Py4JJavaError`-wrapped `IllegalArgumentException`, consistent with the other validation errors.
- [x] **SynapseML Scala conventions re-checked on the current files**: MIT header present on both;
  `Wrappable` + `DefaultParamsWritable` + `SynapseMLLogging` with `logClass(FeatureNames.Core)`
  intact; `transform` still wrapped in `logTransform[DataFrame]`; DataFrame API only; scalastyle
  clean under both configs (run above, 0 errors / 0 warnings).
- [x] **Test-harness safety of the new conf-mutating helper checked**: `withSQLConf`
  (`EnsembleByKeySuite.scala:331-339`) mutates the **shared** `object TestBase` session
  (`TestBase.scala:143`, one session per JVM for all suites) but restores in a `finally`, and
  `build.sbt:274` sets `Test / parallelExecution := false`, so no cross-suite conf race is possible
  in this build. Restoring writes the previous *effective* value (an explicit set where the key was
  previously unset), which is value-identical to the default — harmless.
- [x] **Security checklist — NOT APPLICABLE, with reasons.** The delta since re-review 3 is two
  `RuntimeConfig.get` reads and one `require`. No serialization, I/O, network, credential, secret,
  logging-of-user-data, or untrusted-input parsing is introduced; the conf keys are string literals,
  not user-controlled, and no value is interpolated into SQL text, a path, a URL or a shell command.

---

## Requirement → Test Mapping (re-review 4 delta)

| Requirement (from Issue 10) | Covered by | Verdict |
| --- | --- | --- |
| `spark.sql.retainGroupColumns=false` rejected identically by `transformSchema` **and** `transform` | `EnsembleByKeySuite.scala:197-212` + `PROBE B1/B2/B4-B6` | Covered |
| Rejection is scoped — normal (retain=true) behaviour unchanged | all other suite cases + `PROBE A1-A4`, `PROBE B7` | Covered |
| Guard evaluates the session that actually runs the aggregation | — | **Not covered — Issue 11** (`PROBE C1/C2/C4/D1`) |
| Guard tolerates every conf spelling Spark itself accepts | — | **Not covered — Issue 12** (`PROBE2`) |
| Non-collapse variant of the retention guard | shares `getSchemaFields`; `PROBE B2` only | Covered by probe, not by a test |

---

## Issues

### Issue 11: The `retainGroupColumns` guard reads the thread-active session, but Spark reads group-column retention from the dataset's session
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 124, 127-131 (guard); 156-158 (`transform` call site)
- **Description**: `getSchemaFields` derives the flag from `SparkSession.getActiveSession` — a
  thread-local — via `.forall(...)`. Spark's `RelationalGroupedDataset.toDF` instead reads
  `df.sparkSession.sessionState.conf.dataFrameRetainGroupColumns`, i.e. the **dataset's own**
  session, which I confirmed empirically rather than by reading Spark: with the active session at
  `retain=true` and the dataset's session at `retain=false`, raw Spark still dropped the grouping
  column (`PROBE|C1 ACTUAL=mean(score)`), and with the flags reversed raw Spark kept it
  (`PROBE|C3 SCHEMA=group,m`). The guard therefore fails in both directions:
  - **Fail-open** — the guard passes and Issue 10's exact divergence returns:
    `PROBE|C1 MATCH=false` (schema claims `group,mean(score)`, actual is `mean(score)`) and
    `PROBE|C2 MATCH=false` (schema validates, `transform` throws
    `[UNRESOLVED_USING_COLUMN_FOR_JOIN]`). The same happens whenever the calling thread simply has
    no active session — `.forall` on `None` yields `true` — see `PROBE|D0-activeDefined=false` /
    `PROBE|D1 MATCH=false`.
  - **Fail-closed** — the guard fires for a dataset whose own session retains grouping columns:
    `PROBE|C4` throws `requirement failed: EnsembleByKey requires spark.sql.retainGroupColumns=true`
    where the base build returned a correct `group:string,mean(score):double`
    (`BASEPROBE|BASE|2` vs `BASEPROBE|PR|2`). This is a **new** failure mode introduced by this PR,
    not a pre-existing one.
  `transform` has `dataset` in hand, so unlike `spark.sql.caseSensitive` (correctly kept on the
  active session in re-review 1, because Spark's own `Dataset.drop` resolver reads `SQLConf.get`)
  there is no reason to consult a different session for this particular conf.
- **Risk**: Low, and unchanged in magnitude from Issue 10 — it needs a non-default
  `spark.sql.retainGroupColumns` plus either a second session (`spark.newSession()`, common in
  notebook/Livy/thrift-style multi-tenant drivers) or a caller thread without an inherited active
  session. In those cases `Pipeline` schema propagation still reports a column that will not exist,
  or a valid pipeline is rejected outright.
- **Suggested Fix**: Thread the executing session into the resolver — e.g. give `getSchemaFields` an
  optional `SparkSession` parameter, pass `dataset.sparkSession` from `transform` (`:158`) and keep
  the active-session fallback for the schema-only entry point; reading
  `SparkSession.active` (which falls back to the default session) instead of `getActiveSession`
  would additionally close the "no active session in this thread" hole. Add a probe-equivalent test
  using `spark.newSession()` so the regression is pinned.

### Issue 12: Conf values are parsed with `.toBoolean` instead of Spark's `.trim.toBoolean`, so Spark-accepted values crash the transformer
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 125-128
- **Description**: `SQLConf` stores the raw conf string and parses it with
  `SQLConf.toBoolean` = `s.trim.toBoolean`, so `" true"`, `"true "` and `" false"` are all valid and
  honoured by Spark. `EnsembleByKey` calls `.toBoolean` on the raw string, which throws
  `IllegalArgumentException: For input string: " true"` — from **both** `transformSchema` and
  `transform`, for both `spark.sql.caseSensitive` and `spark.sql.retainGroupColumns`:
  ```
  PROBE2|retain-leading-space       |stored=[ true] |spark=SPARK_OK[group,m]|ebk=EBK_THREW ... " true"
  PROBE2|casesensitive-leading-space|stored=[ false]|spark=SPARK_OK[group,m]|ebk=EBK_THREW ... " false"
  BASEPROBE|BASE|4-whitespace-conf-transform|group:string,mean(score):double   <- base worked
  BASEPROBE|PR  |4-whitespace-conf-transform|THREW "For input string: \" false\""
  ```
  Uppercase (`"TRUE"`) is fine, and genuinely invalid values are impossible because `spark.conf.set`
  validates them (`PROBE2|retain-garbage|set=SET_THREW`), so whitespace padding — which arrives
  easily from `--conf`, cluster-config text boxes and YAML/JSON config generators — is the whole
  exposure. The thrown message names neither `EnsembleByKey` nor the offending conf key, so the
  failure is hard to diagnose.
- **Risk**: Low. A cluster-level conf typo that Spark tolerates now breaks every `EnsembleByKey`
  stage (including schema validation inside a `Pipeline`) with an opaque message. New behaviour
  introduced by this PR — the base read no confs at all.
- **Suggested Fix**: Use `.trim.toBoolean` to match `SQLConf.toBoolean`, or read the typed value
  (`session.sessionState.conf.caseSensitiveAnalysis` /
  `.getConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS)`) so Spark performs the parsing; if the string
  form is kept, wrap the parse so the error names the stage and the conf key.

---

## Notes (no action required, re-review 4)

- **The fail-fast is a deliberate, user-visible behaviour change and deserves a release note.** Under
  `spark.sql.retainGroupColumns=false` with `collapseGroup=true` the base build *worked* — it
  returned a keyless `mean(score):double` (`BASEPROBE|BASE|1`) — and now throws
  (`BASEPROBE|PR|1`). Rejecting is the right call (the transformer's contract is "keys plus
  aggregates", and the non-collapse path could never work), and it was one of the three options
  offered in Issue 10, but it is a compile-compatible / run-incompatible change for anyone who set
  that conf globally.
- **Validation ordering shifted**: the retention `require` (`:129-131`) precedes column resolution,
  so under `retain=false` a missing column or unsupported type now surfaces as the retention error
  (`PROBE3|4a`, `4b`). Param-level checks (`cols`/`keys` set and non-empty, length match) still run
  first (`PROBE3|4c`). Cosmetic only — both entry points agree in every ordering.
- **`spark.sql.caseSensitive` remains correctly sourced from the active session.** Re-review 1's
  analysis still holds and I did not disturb it: `PROBE|E2` shows a dataset from a
  `caseSensitive=true` session evaluated from a `caseSensitive=false` active thread produces the
  *same* result from both entry points (`score is ambiguous`), i.e. no schema-vs-transform
  divergence — only stricter-than-Spark rejection, mirroring Spark's own thread-local
  `SQLConf.get` behaviour in `Dataset.drop`. Issue 11 is specific to `retainGroupColumns`, which
  Spark reads from the dataset instead.
- **Pre-existing, unchanged, out of scope — dotted column names** (`PROBE3|2a`, `2b`, `MATCH=false`):
  `resolveField` matches the literal field `a.b`, but `functions.mean("a.b")` /
  `groupBy("a.b")` go through `UnresolvedAttribute.quotedString`, which splits on `.`
  (`[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... \`a\`.\`b\``). The base build resolved the same way
  (`schema(inputName)` literal lookup followed by the same unquoted `mean(...)` call), so this is a
  repository-wide Spark-API limitation, not a PR defect.
- **Pre-existing, unchanged** — non-orderable (map-typed) grouping keys still validate fail-open, and
  `transform` still writes `defaultParamMap` (`:161-163`) so `getColNames` can report a stale alias
  between calls (`PROBE|F9`, identical to base). `vectorDims` remains accepted but unused.
- **Duplicate exact-name schemas are now rejected earlier and more clearly** than in the base
  (`PROBE3|1b`: `score is ambiguous. Matches: score, score` from both entry points, where the base
  would have proceeded to a Spark `AMBIGUOUS_REFERENCE` at execution time). Improvement, no action.
- **No MiMa/binary-compatibility gate exists in this build**, so the javap-verified API stability
  above is the only check; nothing in the diff requires one.

---

## Resolution Log - Round 1 Re-review 4
_Open; to be updated by the driving agent._

### Issue 11
- **Status**: Fixed
- **What changed**: `getSchemaFields` now accepts an optional execution
  session. `transform` passes `dataset.sparkSession` for
  `spark.sql.retainGroupColumns`, while schema-only calls fall back through
  the active/default session. Case-sensitive name resolution remains sourced
  from the active/default analyzer session to match Spark's resolver behavior.
  Added both fail-open and fail-closed multi-session regression cases.
- **Why**: Spark reads grouping-column retention from the dataset's session,
  not necessarily the thread-active session. Using the same session prevents
  both accepting an impossible aggregation and rejecting a valid dataset.
- **How verified**: The JDK 11 focused suite passed all 19 tests. A dataset
  session with retention disabled is rejected even when the active session
  enables it, and a dataset session with retention enabled transforms
  successfully while the active session disables it.

### Issue 12
- **Status**: Fixed
- **What changed**: Both Spark boolean configuration reads now call
  `.trim.toBoolean`. Added whitespace-padded true/false cases for
  `spark.sql.caseSensitive` and `spark.sql.retainGroupColumns`.
- **Why**: Spark accepts and trims surrounding whitespace in boolean SQL
  configuration values; the transformer must parse the same accepted values.
- **How verified**: The focused 19-test suite passed. Whitespace-padded
  enabled values preserve schema/runtime equality, and whitespace-padded
  disabled retention produces the intended EnsembleByKey requirement error
  instead of a parsing exception.


## Round 1 Re-review 5

## Review Summary
- **Round**: 1 (re-review 5)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5 (Slot 1, latest Anthropic Opus — same artifact token as prior rounds)
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 2 (new); Issues 1-12 all CONFIRMED FIXED
- **Verdict**: ISSUES_FOUND

**Prior findings status: 12 of 12 confirmed fixed by execution, not by reading the Resolution Log.**
Issue 11's fix is correct for the direction it covers — `transform` now sources
`spark.sql.retainGroupColumns` from `dataset.sparkSession` (`:130`, `:161`), which is exactly the
session Spark reads (`PROBE5|A4-rawSparkGroupBy|OK|group,m` with the active session at
`retain=false`). Issue 12's fix is complete: `.trim.toBoolean` now accepts every string Spark's
`SQLConf.toBoolean` accepts (`PROBE5|D-*`, five whitespace/case variants, zero parse failures).

The two new issues are the *residue* of the same session-selection question, in the two places the
fix did not reach:
1. the retention `require` is still evaluated against the **active/default** session on the
   schema-only path, so `transformSchema` (and therefore `Pipeline.fit` / `PipelineModel.transform`)
   rejects a dataset that `transform` aggregates correctly — the fail-closed half of Issue 11,
   still open, and a **regression against the base commit** which ran the same pipeline end-to-end;
2. `caseSensitive` falls back to `SparkSession.getDefaultSession`, but Spark's resolver falls back to
   **default conf values**, not to the default session — so on any thread without an active session
   `transformSchema` declares a column that `transform` drops.

---

## Evidence Checklist

- [x] **Verified the regenerated diff is byte-identical to the working tree.** Extracted the fenced
  diff from `.copilot/session-state/c2ea157b-.../pr-2575/prompts/review-round-1.md` and compared it
  line-by-line (newline-normalised) with
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`:
  **500 lines each, `IDENTICAL=True`**. `git diff --stat` = 2 files, 392 insertions / 30 deletions;
  `git diff --check` clean; `git status --porcelain` shows only the two modified files plus untracked
  `reviews/`, before and after all probes.
- [x] **Read both changed files in full at their current state** (`EnsembleByKey.scala`, 224 lines;
  `EnsembleByKeySuite.scala`, 396 lines) and the base file
  (`git show 8d528f3c:...EnsembleByKey.scala`). All line references are to the current working tree.
- [x] **Independently re-ran the focused suite** (WSL Ubuntu, JDK 11.0.31, sbt, Spark 3.5.0 /
  Scala 2.12.17): `sbt -batch 'core/testOnly ...EnsembleByKeySuite'` →
  `Tests: succeeded 19, failed 0, canceled 0, ignored 0, pending 0` / `[success] Total time: 109 s,
  completed Aug 1, 2026, 7:28:53 AM`. **The 19/19 claim is reproduced, not trusted.** I also audited
  the pre-existing report rather than accepting it: `core/target/test-reports/TEST-...EnsembleByKeySuite.xml`
  (`tests=19 errors=0 failures=0 skipped=0`, timestamp `2026-08-01T06:58:52`) sits on an intact
  timestamp chain — sources `23:55:30` → main classes `23:58:33` (incl. `EnsembleByKey$ResolvedColumns.class`)
  → test classes `23:58:48` → report `00:00:00`. Count reconciles: 16 declared `test(` blocks +
  3 auto-registered by the fuzzing traits (`Serialization Fuzzing`, `Experiment Fuzzing`,
  `Getters and Setters work as anticipated`).
- [x] **Re-ran both lint gates**: `core/scalastyle` → `Processed 211 file(s) / 0 errors / 0 warnings`;
  `core/Test/scalastyle` → `Processed 150 file(s) / 0 errors / 0 warnings`.
- [x] **Executed a 16-case schema/runtime identity matrix against the compiled current classes**
  (`Probe5`, compiled outside the repo against `sbt 'export core/Test/fullClasspath'`), comparing full
  `StructType`s (name, dataType, nullable, metadata) from `transformSchema(df.schema)` against
  `transform(df).schema`: `C1`-`C14`, `C16` **all `MATCH=true`**, `C15` `BOTH_THREW|same=true`.
  Coverage includes collapse/non-collapse defaults, full overwrite, output name = key (collapse),
  duplicate output names, duplicate keys, duplicate cols, key also aggregated, vector-typed key,
  case-variant output names, and the key/output collision guard. Metadata and nullability are exact:
  `features:vector:null=true:md=109` survives the join as a passthrough (`C2`), the aggregate is
  `md=2` (`{}`) and `null=false` (`C3`), a key carrying custom metadata keeps `md=18` in both modes
  (`G1`, `G2`), and an all-null aggregate keeps `null=false` on the key (`G3`).
- [x] **Established empirically which session governs each Spark resolution step**, instead of
  assuming: `RelationalGroupedDataset.toDF` follows the **dataset's** session
  (`PROBE5|A4` keeps `group` while the active session has `retain=false`), whereas `Dataset.drop`
  follows the **thread-active** session (`PROBE5|H1` active=insensitive + dataset=sensitive → drops
  both `features` and `FEATURES`; `PROBE5|H3` active=sensitive + dataset=insensitive → keeps
  `FEATURES`). Both cross-session EBK cases still match (`H2`, `H4` `MATCH=true`), so re-review 1's
  decision to source `caseSensitive` from the active session is correct **whenever an active session
  exists** — see Issue 14 for the case where it does not.
- [x] **A/B-compared PR vs base on the same JVM/classpath** (`Probe6`/`Probe7`, base
  `EnsembleByKey.class` shadowing the PR class) to separate regression from pre-existing defect.
  This is what promotes Issue 13 from "residual" to "regression": `PROBE7|BASE|X3-pipelineFit|OK|group,m`
  and `X4-pipelineCollect|OK|[g,2.0]` versus `PROBE7|PR|X3/X4|THREW ... requires
  spark.sql.retainGroupColumns=true`.
- [x] **Verified conf parsing against Spark's own parse** (`PROBE5|D-*`): `" true "`, `"TRUE"`,
  `"\ttrue\t"`, `" false "`, `"FALSE"` all `set=true`, and EBK's decision matches
  `SQLConf.get.caseSensitiveAnalysis` in every case; padded `retainGroupColumns` behaves identically
  (`" true "` → passes, `" false "` → the intended `EnsembleByKey requires ...` message, not a parse
  crash). Genuinely invalid values remain impossible: `spark.conf.set(..., "notabool")` throws at set
  time (`D-garbageRetain|OK|false`).
- [x] **Checked binary/source compatibility with `javap`** (PR classes vs base classes): every
  difference is a synthetic `$anonfun$...` / `$adapted` lambda-lifted member plus the new
  `EnsembleByKey$ResolvedColumns[$]` nested classes. All declared public API (`setKey(s)`, `setCol(s)`,
  `setColName(s)`, `setStrategy`, `setCollapseGroup`, `setVectorDims`, getters, `transform`,
  `transformSchema`, `copy`, `uid`) is unchanged in signature. No new params → no generated-wrapper
  (Python/R/.NET) surface change; `Wrappable` codegen input is untouched.
- [x] **Confirmed no other code in the repository depends on `EnsembleByKey`'s schema**:
  `git grep -rn EnsembleByKey` outside the two changed files hits only `EnsembleByKey.txt`,
  `docs/**/_Stages.md`, versioned website copies, and an unrelated 2018 Zeppelin notebook blob. No
  Scala/Python production caller, so the (intended) collapse-mode schema change cannot break an
  in-repo consumer.
- [x] **Probed shared-instance concurrency**: 8 parallel `transform` calls on one `EnsembleByKey`
  produced exactly one distinct result (`PROBE5|I1`), i.e. the `setDefault` write in `transform`
  (`:163-165`) did not corrupt output here — it remains the pre-existing smell described in the notes,
  not a new defect.
- [x] **Security review**: no I/O, no reflection, no deserialization, no string-built SQL, no
  credential or network surface in the diff. The new error messages echo user-supplied column names
  and the input schema's field names (`s"$name does not exist. Available: ..."`), which matches
  Spark's own `UNRESOLVED_COLUMN.WITH_SUGGESTION` behaviour and discloses nothing a caller holding
  the DataFrame does not already have. No finding.

## Issues

### Issue 13: The retention guard still rejects on the schema-only path, so `transformSchema`/`Pipeline` fail on datasets `transform` handles correctly (regression vs base)
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 130-134 (guard), 208 (`transformSchema` passes `None`), 161 (`transform` passes the dataset session)
- **Description**: Issue 11's fix threads the executing session into `getSchemaFields` and is correct
  for `transform`. The schema-only entry point still passes `None` (`:208`), so the guard falls back
  to `analysisSession` — the thread-active/default session — for a conf that Spark reads from the
  **dataset's** session. When the dataset's session retains grouping columns but the active session
  does not, the two entry points now contradict each other, which is precisely the property this PR
  exists to establish:
  ```
  PROBE7|PR  |X1-transform        |OK    |group,m
  PROBE7|PR  |X2-transformSchema  |THREW |requirement failed: EnsembleByKey requires spark.sql.retainGroupColumns=true
  PROBE7|PR  |X3-pipelineFit      |THREW |(same)
  PROBE7|PR  |X4-pipelineCollect  |THREW |(same)
  PROBE5|A4-rawSparkGroupBy       |OK    |group,m      <- raw Spark keeps the key column
  ```
  Because `Pipeline.fit` and `PipelineModel.transform` call `transformSchema` before any data flows,
  the working `transform` is unreachable through the normal ML pipeline API. The base build ran the
  identical pipeline end-to-end, so this is a **new hard failure**, not a pre-existing one:
  ```
  PROBE7|BASE|X2-transformSchema  |OK|group,score,m   (inaccurate schema, but no throw)
  PROBE7|BASE|X3-pipelineFit      |OK|group,m
  PROBE7|BASE|X4-pipelineCollect  |OK|[g,2.0]
  ```
  The new test at `EnsembleByKeySuite.scala:213` (`transform should use the dataset session for
  grouping column retention`) builds exactly this state at `:228-234` — dataset session
  `retain=true`, active session `retain=false` — and asserts only `transform`. Adding the
  `transformSchema` assertion that every other test in this suite pairs with `transform` would have
  caught it.
- **Risk**: Low. Requires a non-default `spark.sql.retainGroupColumns` plus a second session
  (`spark.newSession()` — normal in notebook/Livy/thrift-style multi-tenant drivers). In that
  configuration a previously working pipeline now aborts at schema validation with an error about a
  conf the dataset does not actually use.
- **Suggested Fix**: Only enforce retention where the executing session is known, e.g. gate the
  `require` on `executionSession.isDefined`, or have the schema-only path assume Spark's default
  (`true`) instead of consulting an unrelated session. `transform` already fails fast with the correct
  session, so the schema-only path loses nothing. Then extend the test at `:213` with
  `assert(transformer.transformSchema(enabledInput.schema) === transformed.schema)` inside the
  existing `withSQLConf`/`withActiveSession` block so the pairing is pinned.

### Issue 14: `caseSensitive` falls back to the *default session*, but Spark falls back to *default conf values* — schema/runtime divergence on any thread without an active session
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 127-129
- **Description**: `analysisSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)`
  adds a fallback Spark itself does not have. Spark's resolver reads `SQLConf.get`, whose getter is
  `getActiveSession...getOrElse(SQLConf.getFallbackConf)` — a fresh `SQLConf` holding **defaults**,
  never the default session. So on a thread with no active session, EBK reads
  `caseSensitive=true` from the default session while Spark resolves case-**insensitively**, and the
  non-collapse `needToDrop`/declared-schema pair diverges. Reproduced on a thread pool created before
  the `SparkSession` (so it never inherited the `InheritableThreadLocal`) — same session, same
  DataFrame, same transformer, only the executing thread differs:
  ```
  PROBE8|pre-created-pool-thread|activeDefined=false defaultCaseSensitive=Some(true)
      SQLConf.get.caseSensitive=false rawDrop=Success(id,key,score)
      declared=Success(key,id,score,FEATURES,features) actual=Success(key,id,score,features) MATCH=false
  PROBE8|main-thread            |activeDefined=true  ... MATCH=true
  ```
  `PROBE5|B0/B1/B2` reproduce the same divergence via an explicit `SparkSession.clearActiveSession()`:
  `SQLConf.get.caseSensitiveAnalysis=false` while the default session reports `Some(true)`, raw
  `df.drop("features")` removes both case variants, and `transformSchema` still promises `FEATURES`.
  Realistic triggers are a servlet/Akka/Netty request pool or `ExecutionContext` created before the
  session, `SparkSession.clearActiveSession()`, or any framework thread Spark did not spawn. The base
  build was also wrong here (`PROBE6|BASE|B2` `MATCH=false`), so this is an incomplete fix rather than
  a regression — but it is the one remaining single-session-config path where the PR's stated
  invariant does not hold, and it is caused by code this PR added.
- **Risk**: Low. Needs `spark.sql.caseSensitive=true` (non-default) plus a thread without an active
  session. In that combination `Pipeline` schema propagation advertises a column that `transform`
  deletes, and EBK's ambiguity/`does not exist` checks are evaluated under a different resolver than
  the one Spark will use.
- **Suggested Fix**: Drop `.orElse(SparkSession.getDefaultSession)` for the case-sensitivity read (use
  `SparkSession.getActiveSession` alone), or read `SQLConf.get.caseSensitiveAnalysis` directly — that
  is literally the value `Dataset.drop` and `Dataset.resolve` consult, so the two can no longer drift.
  Note this fallback must **not** be removed from the retention read, which is correctly anchored to
  the dataset session (see Issue 13). Add a regression test that wraps the existing case-variant
  assertions in `SparkSession.clearActiveSession()` / restore — the suite already has
  `withActiveSession` (`:384`) and only needs a `None` variant.

---

## Notes (no action required, re-review 5)

- **Every prior finding re-verified by execution, not by reading the log.** Issue 1/6 (case-aware key
  resolution) — `PROBE5|C-matrix` + `H1`-`H4`; Issue 2 (`transformSchema` purity) — the suite's
  `getDefault(transformer.colNames).isEmpty` assertion at `:153-155` plus `PROBE5|E5/E6/E7`
  (`isSet(colNames)=false` after schema derivation, and a later `setCols` is honoured); Issue 3
  (key/output collision) — `PROBE5|C15 BOTH_THREW same=true`; Issue 4 (length mismatch) — suite
  `:262-265`; Issues 5/7/9 (fail-fast coverage and `cols` ambiguity) — suite `:256-298`; Issue 8
  (case-variant twin over-report) — `PROBE5|C3`, `C14`, `H2`, `H4`; Issue 10 (retention divergence)
  — `PROBE5|D-retain[ false ]` and suite `:197`; Issue 11 — `PROBE5|A1` (dataset session governs
  `transform`); Issue 12 — `PROBE5|D-*`.
- **Pre-existing and unchanged — `transform` writes `defaultParamMap` (`:163-165`), so the public
  getter can go stale.** `PROBE5|E3` returns `mean(doubleScore)` after the first `transform`, and
  `E4` still returns `mean(doubleScore)` after `setCols("floatScore")`, while `E5`/`E6` correctly emit
  `mean(floatScore)`. The base behaves identically, and the PR strictly improves matters because
  `transformSchema` no longer consumes that stale default — but `getColNames` can still disagree with
  the emitted column name, and `DefaultParamsWriter` persists `defaultParamMap`, so saved metadata
  depends on whether `transform` ran before `save`.
- **Pre-existing, out of scope — dotted column names.** `resolveField` (`:97`) matches the literal
  field `a.b`, while `functions.mean("a.b")` / `groupBy("a.b")` go through
  `UnresolvedAttribute.quotedString` and split on `.`. Unchanged from base by inspection; not
  re-probed this round.
- **Pre-existing — non-orderable grouping keys validate fail-open.** `PROBE5|C13` shows a
  `VectorType` key producing `MATCH=true` at analysis time; any failure surfaces later from Spark, as
  it did at base. `vectorDims` also remains accepted but unused.
- **The retainGroupColumns fail-fast remains a user-visible behaviour change worth a release note**
  (carried forward from re-review 4): with `spark.sql.retainGroupColumns=false` and
  `collapseGroup=true` the base returned a keyless `mean(score)`; the PR throws. Rejecting is the
  right call, but it is compile-compatible / run-incompatible for anyone who set that conf globally.
- **Public API is stable and no MiMa gate exists in this build**, so the `javap` comparison above is
  the only compatibility check; nothing in the diff requires one.
- **Test suite quality is otherwise high**: the `assert(input.schema("features").metadata !== Metadata.empty)`
  precondition (`:64`) still prevents vacuous metadata assertions, every positive test pairs
  `transformSchema` with `transform`, and `Test / parallelExecution := false` (`build.sbt:274`) means
  the new `spark.conf` mutations in `withSQLConf` (`:374-382`) cannot leak into a concurrently running
  suite; the `try/finally` restore handles the sequential case.

---

## Resolution Log - Round 1 Re-review 5
_Open; to be updated by the driving agent._

### Issue 13
- **Status**: Fixed
- **What changed**: Removed the schema-time retention rejection. `transform`
  now reads `spark.sql.retainGroupColumns` from `dataset.sparkSession`; when
  Spark disables automatic key retention, the transformer explicitly includes
  the resolved grouping columns in the aggregation expression list. Added
  collapse/non-collapse, multi-session, and Pipeline coverage.
- **Why**: Explicitly projecting grouped attributes makes the runtime output
  independent of this Spark SQL setting, so `transformSchema` can always
  declare the transformer contract without guessing which dataset session a
  future call will use.
- **How verified**: The JDK 11 focused suite passed all 20 tests. Both
  retention settings now produce identical declared/actual schemas, and a
  Pipeline built from a dataset session whose setting differs from the active
  session runs successfully.

### Issue 14
- **Status**: Fixed
- **What changed**: Removed the default-session fallback from case-sensitivity
  resolution. `getSchemaFields` now uses only `SparkSession.getActiveSession`,
  falling back to Spark's default case-insensitive semantics when none exists.
  Added a no-active-session case-variant overwrite test.
- **Why**: Spark's `SQLConf.get` resolver falls back to default configuration
  values, not the default `SparkSession`; matching that behavior prevents
  schema/drop divergence on uninherited worker threads.
- **How verified**: The focused 20-test suite passed. With a default session
  configured case-sensitive but no active session, `transformSchema` and
  `transform` both follow Spark's case-insensitive fallback and emit the same
  exact fields.


## Round 1 Re-review 6

## Review Summary
- **Round**: 1 (re-review 6)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5 (Slot 1, latest Anthropic Opus — same artifact token as prior rounds)
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 2 (new); Issues 1-14 all CONFIRMED FIXED
- **Verdict**: ISSUES_FOUND

**Issues 13 and 14 are genuinely fixed, and the new explicit grouping-column projection is correct.**
I re-derived every claim by execution rather than reading the Resolution Log. The
`keyColumns ++ newCols` projection under `spark.sql.retainGroupColumns=false` produces a plan that
is *structurally identical* to the one Spark builds when it prepends grouping columns itself
(only `exprId` counters differ), identical row data, and identical schemas — including key
metadata, key nullability, duplicate keys, multiple keys, vector keys, keys that are also
aggregated, and the non-collapse `USING` join. `transformSchema` now never consults
`retainGroupColumns` at all, so the schema-only path can no longer contradict `transform`
(Issue 13), and `caseSensitive` now tracks `SQLConf.get` semantics exactly, including on a thread
that never inherited an active session (Issue 14).

The two new issues are unrelated to retention. They are the residue of `resolveField` (`:97-106`,
added by this PR) matching **literal top-level field names**, while `transform` resolves the same
strings through Spark's attribute-name *parser* (`dataset.col` → `UnresolvedAttribute.parseAttributeName`,
`functions.mean(String)` → `UnresolvedAttribute.quotedString`), where `.` means nesting and
backticks quote. That mismatch produces one functional regression against the base commit and one
residual violation of the exact schema/transform invariant this PR exists to establish.

---

## Evidence Checklist

- [x] **Confirmed the regenerated diff is byte-identical to the working tree.** Extracted the fenced
  diff from `.copilot/session-state/c2ea157b-.../pr-2575/prompts/review-round-1.md` (lines 82-611)
  and compared it newline-normalised against
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`:
  **530 lines each, `IDENTICAL=True`**. `git diff --stat` = 2 files / 420 insertions / 31 deletions;
  `git diff --check` clean; `git status --porcelain` shows only the two modified files plus untracked
  `reviews/`. Read both changed files in full at their current state (`EnsembleByKey.scala` 214 lines,
  `EnsembleByKeySuite.scala` 428 lines); all line references below are to the current tree.
- [x] **Independently reproduced the 20/20 claim** (WSL Ubuntu, OpenJDK 11.0.31, sbt, Spark 3.5.0 /
  Scala 2.12.17): `sbt -batch 'core/testOnly ...EnsembleByKeySuite'` →
  `Tests: succeeded 20, failed 0, canceled 0, ignored 0, pending 0` /
  `[success] Total time: 108 s, completed Aug 1, 2026, 7:55:58 AM`. All 17 declared `test(` blocks
  plus the 3 fuzzing-trait tests are listed individually in the run output. **Not trusted — re-run.**
- [x] **Re-ran both lint gates**: `core/scalastyle` → `Processed 211 file(s) / 0 errors / 0 warnings`;
  `core/Test/scalastyle` → `Processed 150 file(s) / 0 errors / 0 warnings`.
- [x] **Executed a 40-case schema-identity matrix over the retention axis** (`Probe9`, compiled
  outside the repo against `sbt 'export core/Test/fullClasspath'`), comparing full `StructType`s
  (name, dataType, nullable, metadata) from `transformSchema(df.schema)` against `transform(df).schema`
  for `retain ∈ {true,false}` × `collapse ∈ {true,false}` × 10 shapes: two keys, one key,
  **duplicate keys** (`A3` → `group,group,mean(doubleScore)`), **key carrying custom metadata**
  (`A4`), **nullable keys** (`A5`), full overwrite (`A6`), **key that is also an aggregate input**
  (`A7`), **vector-typed key** (`A8`), output-name-equals-key (`A9`), duplicate output names (`A10`).
  **Every case `MATCH=true`**, except `A9` with `collapse=false`, where both paths throw the same
  guard message (`BOTH_THREW|sameMsg=true`). The retain=false column ordering is exactly
  `keys ++ aggregates` (collapse) and `keys ++ passthrough ++ aggregates` (non-collapse), i.e.
  identical to retain=true.
- [x] **Verified retain=false is data-identical, not just schema-identical**: `PROBE9|B-collapse=true|DATA_EQ=true`
  and `PROBE9|B-collapse=false|DATA_EQ=true` (full `collect()` compare, sorted).
- [x] **Compared analyzed execution plans across the retention setting** (`PROBE9|C`). The trees are
  structurally identical; the only textual difference is `exprId` counters
  (`... AS mean(doubleScore)#1808` vs `#1829`). Collapse mode yields the same single
  `Aggregate [group#16, region#15], [group#16, region#15, avg(...) AS ...]` node in both settings —
  i.e. the explicit projection reproduces Spark's own prepend exactly rather than adding a
  `Project`/extra shuffle. Non-collapse yields the same `Project → Join Inner → (left, Aggregate)`
  shape in both settings.
- [x] **Tested the dataset-id metadata hazard introduced by switching `groupBy(String*)` →
  `groupBy(Column*)`.** `Dataset.col` tags `AttributeReference`s with `__dataset_id`/`__col_position`
  for `DetectAmbiguousSelfJoin`, and that rule *returns early without stripping* when
  `spark.sql.analyzer.failAmbiguousSelfJoin=false`
  (`spark v3.5.0 .../execution/analysis/DetectAmbiguousSelfJoin.scala:78`). Probed all 8 combinations
  of `failAmbiguousSelfJoin` × `retainGroupColumns` × `collapseGroup`: `PROBE9|D-*` → **`leak=false`
  and `MATCH=true` in every case** (`group:string:null=true:md={}`). No finding — the hypothesis is
  disproved, and the non-collapse self-join does not trip the ambiguity detector either.
- [x] **Re-checked no-active-session behaviour two independent ways.** (a) `PROBE9|E1/E2`:
  `SparkSession.clearActiveSession()` × retain × collapse → all `MATCH=true` with correct rows
  (`[g,2.0]`, `[g,1.0,2.0];[g,3.0,2.0]`). (b) `PROBE11|P2` on a **thread created after
  `clearActiveSession`** (never inherited the `InheritableThreadLocal`):
  `activeDefined=false defaultDefined=true SQLConf.get.caseSensitiveAnalysis=false` while the default
  session has `spark.sql.caseSensitive=true` — the exact Issue 14 repro — and
  `PROBE11|P3-noActive-caseVariantOverwrite|MATCH=true|names=key,id,score,features`. EBK now follows
  Spark's default-conf fallback instead of the default session. **Issue 14 fixed.**
- [x] **Re-checked the Issue 13 repro end-to-end.** `PROBE9|F1` (dataset session retain=false, active
  session retain=true) and `F2` (dataset retain=true, active retain=false) → both `MATCH=true`, both
  collapse modes; `PROBE9|F3` builds `Pipeline(...).fit(dfRetainTrue).transform(...)` while the active
  session says retain=false → `group,mean(score)` / `group,score,mean(score)` with **no throw**. The
  `require(... spark.sql.retainGroupColumns=true)` guard is gone from the source entirely
  (`grep` finds no `require` mentioning retention). **Issue 13 fixed.**
- [x] **A/B-compared PR vs base on the same JVM/classpath** (`Probe10`, base `EnsembleByKey.class`
  shadowing the PR class). Under default confs the two builds are byte-for-byte equal in columns and
  rows for both collapse modes (`AB1`, `AB2`), so the runtime path is not regressed. Under
  `retainGroupColumns=false` the intended change is visible: `BASE|AB3-collapse=true|cols=mean(doubleScore)`
  (keys silently dropped) vs `PR|...|cols=group,region,mean(doubleScore)`, and
  `BASE|AB3-collapse=false|THREW AnalysisException [UNRESOLVED_USING_COLUMN_FOR_JOIN]` vs PR success.
- [x] **Verified typed `Dataset[T]` inputs** (`Transformer.transform(Dataset[_])` is the public
  signature, and the PR now calls `dataset.col` on the raw `Dataset` before `toDF()`):
  `PROBE11|P1-typedDataset-*` → `MATCH=true` for all four retain × collapse combinations on a
  `Dataset[Rec]`.
- [x] **Re-verified `transformSchema` purity and the `transform`-time default write**:
  `PROBE11|P5-purity|before=None|after=None|isSet=false` (schema derivation writes nothing) and
  `P6-afterTransform|default=Some(mean(score))|isSet=false` (pre-existing `defaultParamMap` write,
  unchanged from base). `PROBE9|H1/H2/H3` show a single reused instance producing correct output
  across a retention flip, and `PROBE9|I1` shows 8 parallel `transform` calls yielding exactly one
  distinct result.
- [x] **Checked source/binary compatibility with `javap`**: public members of
  `com.microsoft.azure.synapse.ml.stages.EnsembleByKey` (PR vs base, synthetic `$anonfun`/`$adapted`
  filtered) → `PUBLIC_API_IDENTICAL`. Only additions are the private nested
  `EnsembleByKey$ResolvedColumns[$].class`. No new/renamed/removed params → no `Wrappable`
  Python/R/.NET generated-wrapper surface change; `testObjects()` is unchanged so generated fuzzing
  tests are unchanged.
- [x] **Confirmed no in-repo consumer depends on the changed schema**: `git grep -l EnsembleByKey`
  outside the two changed files hits only `EnsembleByKey.txt` (prose), `docs/**/_Stages.md` +
  versioned website copies (default-conf example, unaffected — verified the snippet uses
  `setKeys(["label1"]).setCols(["score1"])` with no schema assertion), and an unrelated Zeppelin
  notebook blob.
- [x] **Probed column-name resolution semantics** (`Probe10|N1-N7`) against both builds — this is
  where Issues 15 and 16 come from. Also confirmed `spark.sql.parser.quotedRegexColumnNames=true`
  changes nothing for ordinary names (`PROBE9|K1-*|MATCH=true`) and that names containing spaces work
  (`PROBE10|PR|N5-spacedNames|MATCH=true`).
- [x] **Security review**: the diff adds no I/O, reflection, deserialization, string-built SQL,
  credential handling, or network surface. Error messages echo caller-supplied column names and the
  input schema's field names (`:102`, `:105`), matching Spark's own
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` disclosure for a caller who already holds the DataFrame. No
  finding.

## Issues

### Issue 15: Nested-struct and backtick-quoted key names that worked at base are now rejected by `resolveField`
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 97-106 (`resolveField`), 128 (`keyFields`), 155 (`dataset.col`)
- **Description**: `resolveField` compares the user-supplied string against `schema.fields.name`
  literally. `Dataset.col`/`groupBy(String*)` instead run the string through
  `UnresolvedAttribute.parseAttributeName`, where `.` denotes nested-field access and backticks quote
  a literal name. Base called `groupBy(getKeys.head, getKeys.tail: _*)` → `Dataset.resolve` → the
  parser, so both spellings worked. The PR pre-validates with the literal matcher and throws before
  Spark ever sees the name. Measured on the same JVM with the base class shadowing the PR class:
  ```
  # key is a nested struct field: df has columns [s: struct<k:string>, score]
  PROBE10|BASE|N7-nestedStructKey|ONLY_DECLARED_THREW|...|actual=k,mean(score)      <- transform WORKED
  PROBE10|PR  |N7-nestedStructKey|BOTH_THREW|IllegalArgumentException: s.k does not exist. Available: s, score

  # key is a column literally named `a.b`, addressed with backticks (the Spark-correct spelling)
  PROBE10|BASE|N2-dottedKey-backtick|ONLY_DECLARED_THREW|...|actual=a.b,mean(score) <- transform WORKED
  PROBE10|PR  |N2-dottedKey-backtick|BOTH_THREW|IllegalArgumentException: `a.b` does not exist. Available: a.b, score
  ```
  Note the base failures in those lines are `NoSuchElementException: Failed to find a default value
  for colNames` on the *schema-only* path (a defect this PR fixes); base `transform` — the primary
  API and the one PySpark/Scala users call directly — succeeded in both cases and now throws.
- **Risk**: Low. A previously working `EnsembleByKey.setKey("outer.inner")` (grouping by a nested
  struct field, a normal Spark idiom for nested data) or `setKey("`odd.name`")` now aborts with
  `IllegalArgumentException: ... does not exist`, and the message lists the available names in a form
  that looks like the user's input (`Available: a.b`), which makes the failure hard to diagnose.
  Fail-fast may be the intended trade (base's schema was wrong for these cases anyway), but it is
  currently silent, untested, and undocumented.
- **Suggested Fix**: Resolve names the way Spark does instead of literally — e.g. run the key/col
  strings through `UnresolvedAttribute.parseAttributeName(name)` and match on the parsed head part
  (falling back to nested resolution via `schema.findNestedField`), or accept that nesting is
  unsupported and say so explicitly in the error message plus a test that pins the new behaviour.
  Whatever is chosen, add regression tests for a nested key and a backtick-quoted key so the
  decision is deliberate rather than incidental.

### Issue 16: A literal dotted column name passes `transformSchema` but fails in `transform`, breaking the PR's core invariant
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 97-106 (`resolveField`), 155 (`dataset.col`), 165-176 (`mean(inColName)` /
  `Summarizer.mean(col(inColName))`), 202-213 (`transformSchema`)
- **Description**: The mirror image of Issue 15. When a schema really does contain a top-level field
  named `a.b`, `resolveField` matches it literally and `transformSchema` happily declares it, but
  `transform` hands the same unparsed string to `dataset.col(...)` / `functions.mean(String)`, which
  parse it as `a`.`b` and fail analysis. This is precisely the declared-vs-actual divergence the PR
  exists to eliminate, and for the default (`colNames` unset) configuration it is *newly* reachable,
  because base's `transformSchema` threw before it could lie:
  ```
  PROBE10|PR  |N1-dottedKey-plain|ONLY_ACTUAL_THREW|AnalysisException:[UNRESOLVED_COLUMN.WITH_SUGGESTION]
        A column or function parameter with name `a`.`b` cannot be resolved.
        Did you mean one of the following? [`a`.`b`, `score`].|declared=a.b,mean(score)
  PROBE10|BASE|N1-dottedKey-plain|BOTH_THREW|NoSuchElementException: Failed to find a default value for colNames

  PROBE10|PR  |N3-dottedCol-plain|ONLY_ACTUAL_THREW|ExtendedAnalysisException:[UNRESOLVED_COLUMN.WITH_SUGGESTION]
        ... name `s`.`c` cannot be resolved ...|declared=group,mean(s.c)
  PROBE10|BASE|N3-dottedCol-plain|BOTH_THREW|NoSuchElementException: Failed to find a default value for colNames
  ```
  It affects both `keys` (`:155`) and `cols` (`:171-176`), and both collapse modes; retention setting
  is irrelevant (`PROBE10|PR|N6-*` behaves the same under `retainGroupColumns=false`).
- **Risk**: Low. Requires a `.` in a column name — uncommon, but produced routinely by CSV/Parquet
  ingestion of external data and by flattening tools. In a `Pipeline`, schema propagation succeeds and
  the failure surfaces only when the stage actually runs, which is the failure mode this PR set out
  to remove.
- **Suggested Fix**: Same root fix as Issue 15 — make `resolveField` and the aggregate-expression
  construction agree on one resolution strategy. Concretely, build the aggregate columns from the
  resolved `Column`s (e.g. `mean(dataset.col(name))` / `Summarizer.mean(dataset.col(name))` using the
  already-resolved attribute) instead of re-resolving from the raw string, and quote names when
  handing them to string-based APIs. Add a test with a column named `a.b` asserting
  `transformSchema === transform(...).schema`.

---

## Notes (no action required, re-review 6)

- **All fourteen prior findings re-verified by execution, not by reading the Resolution Log.**
  Issue 1/6 (case-aware key resolution) — `PROBE9|G1/G2` under both retention settings; Issue 2
  (`transformSchema` purity) — `PROBE11|P5`; Issue 3 (key/output collision) — `PROBE9|A9-collapse=false
  BOTH_THREW sameMsg=true`; Issue 4 (length mismatch) — suite `:293-296`; Issues 5/7/9 (fail-fast
  coverage, `cols` ambiguity) — suite `:277-329`; Issue 8 (case-variant twin over-report) —
  `PROBE9|G2` → `key,id,score,FEATURES,features` `MATCH=true`; Issue 10 (retention divergence) — the
  whole `PROBE9|A*-retain=false` block; Issue 11 (dataset session governs retention) — `PROBE9|F1/F2`;
  Issue 12 (`.trim.toBoolean`) — `:126`, `:184`, suite `:239-251`; Issue 13 — `PROBE9|F3` +
  guard removed from source; Issue 14 — `PROBE11|P2/P3` on a non-inheriting thread.
- **The retain=false output change is a deliberate, user-visible behaviour change worth a release
  note.** With `spark.sql.retainGroupColumns=false` and `collapseGroup=true`, base returned a keyless
  `mean(score)` (`PROBE10|BASE|AB3-collapse=true|cols=mean(doubleScore)`) and the PR now returns
  `group,region,mean(doubleScore)`. With `collapseGroup=false` base threw
  `UNRESOLVED_USING_COLUMN_FOR_JOIN` and the PR now succeeds. Both are improvements, but the first is
  run-incompatible for anyone who set that conf globally and indexes result columns positionally.
- **Pre-existing and unchanged — `transform` writes `defaultParamMap` (`:156-158`).** `PROBE11|P6`
  confirms `getColNames` starts reporting derived names only after the first `transform`, and
  `DefaultParamsWriter` persists `defaultParamMap`, so saved metadata still depends on whether
  `transform` ran before `save`. The PR strictly improves this (schema derivation no longer consumes
  the stale default) but does not remove it.
- **Pre-existing and unchanged — non-collapse mode drops rows whose keys are NULL**, because the
  merge is an inner `USING` join. `PROBE9|J2/J4` show a 3-row input with two NULL-containing key rows
  producing a single output row, identically under both retention settings, and identically to base.
  The declared schema keeps `nullable=true` on the keys in both paths, so schema/runtime still agree.
- **Pre-existing — non-orderable grouping keys validate fail-open.** `PROBE9|A8` shows a `VectorType`
  key producing `MATCH=true` at analysis time in both collapse modes; any failure surfaces later from
  Spark, as at base. `vectorDims` also remains accepted but unused.
- **Test-suite quality remains high.** Every positive test pairs `transformSchema` with `transform`;
  the `assert(input.schema("features").metadata !== Metadata.empty)` precondition (`:64`) prevents a
  vacuous metadata assertion; `Test / parallelExecution := false` (`build.sbt:274`) plus the
  `try/finally` restores in `withSQLConf`/`withActiveSession`/`withoutActiveSession` (`:401-428`)
  keep the global conf and active-session mutations from leaking. Minor gap: the retention tests
  assert schemas and column names only — no test collects rows under `retainGroupColumns=false` to
  pin that the projected key *values* are correct (verified externally by `PROBE9|B`, `DATA_EQ=true`).
- **Public API is stable and there is no MiMa gate in this build**, so the `javap` comparison above
  is the compatibility check; nothing in the diff requires more.

---

## Resolution Log - Round 1 Re-review 6
_Open; to be updated by the driving agent._

### Issue 15
- **Status**: Fixed
- **What changed**: Added Spark multipart-name parsing through
  `UnresolvedAttribute.parseAttributeName` and recursive StructType resolution.
  Resolved fields retain both the configured Spark reference and the final
  `StructField`. Runtime grouping uses the configured parsed reference while
  aliasing the key back to the resolved field name and metadata. Non-collapse
  mode materializes nested key leaves for the join. Added nested-struct and
  backtick-quoted key coverage.
- **Why**: Spark supports nested references and backtick-quoted literal names;
  schema validation must accept the same syntax and declare the key field name
  Spark emits.
- **How verified**: The JDK 11 focused suite passed all 22 tests. Nested keys
  work in collapse and non-collapse modes with correct values, and a literal
  dotted key addressed with backticks produces an identical declared/actual
  schema.

### Issue 16
- **Status**: Fixed
- **What changed**: Aggregate expressions now consume `Column`s built from the
  same parsed references used by schema validation rather than re-resolving
  resolved field names as raw strings. Plain dotted names are interpreted as
  nested paths and fail early when that path does not exist; backtick-quoted
  literal dotted aggregate fields are supported.
- **Why**: A single Spark parsing strategy prevents schema validation from
  accepting a top-level dotted name that runtime interprets as a nested path.
- **How verified**: The focused 22-test suite passed. A quoted `s.c` aggregate
  has matching schemas and a plain `s.c` against a literal top-level field
  raises the same `IllegalArgumentException` from both entry points.


## Round 1 Re-review 7

## Review Summary
- **Round**: 1 (re-review 7)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5 (Slot 1, latest Anthropic Opus — same artifact token as prior rounds)
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 6 (new); Issues 1-16 all CONFIRMED FIXED
- **Verdict**: ISSUES_FOUND

**Issues 15 and 16 are genuinely fixed.** `UnresolvedAttribute.parseAttributeName` + recursive `StructType`
descent now matches `Dataset.col` for the common cases: nested struct keys work in both collapse modes
(`P13|PR|L1/L2 MATCH=true` for a 3-level path `a.b.c`), backtick-quoted literal dotted names work as keys
(`H0/H0b MATCH=true`) and as aggregates, and a plain `s.c` against a literal top-level field now fails
identically from both entry points. I also disproved a hypothesis I had going in: `caseSensitive` reading
`SparkSession.getActiveSession` (`:152-153`) while `retainGroupColumns` reads `dataset.sparkSession`
(`:214-215`) looks asymmetric but is **exactly right** — `Dataset.col`/`drop`/`withColumn`/`join` go through
`analyzer.resolver`, and `Analyzer` mixes in `SQLConfHelper` (`def conf = SQLConf.get` → active session),
whereas `RelationalGroupedDataset.toDF` reads `df.sparkSession.sessionState.conf.dataFrameRetainGroupColumns`
directly. Probed both directions across two sessions with divergent confs: `P12|I1` throws exactly when
Spark's own `groupBy` throws `AMBIGUOUS_REFERENCE`, and `P12|I2 MATCH=true` where Spark's `groupBy` succeeds.

The six new issues are: one **CI-breaking lint regression** introduced by the Issue 15/16 fix, three
correctness defects in the new nested-key handling (mis-declared nullability, silent column
destruction/row loss, and a `transformSchema`-passes/`transform`-throws case), and two residual
resolution-fidelity gaps between `resolveField` and Spark.

---

## Evidence Checklist

- [x] **Confirmed the regenerated prompt diff is byte-identical to the working tree.** Extracted the fenced
  diff from `.copilot/session-state/c2ea157b-.../pr-2575/prompts/review-round-1.md` (lines 82-713) and
  compared newline-normalised against
  `git --no-pager diff 8d528f3c9279603a8fde22623ad10a27b0d48ff7 -- . ':(exclude)reviews/pr-2575'`:
  **632 lines each, `IDENTICAL=True`**. `--stat` = 2 files / 511 insertions / 44 deletions; `--check`
  clean; `status --porcelain` = the two modified files + untracked `reviews/`. Read both changed files in
  full at their current state (`EnsembleByKey.scala` 254 lines, `EnsembleByKeySuite.scala` 437 lines);
  all line references below are to the current tree.
- [x] **Independently reproduced the 22/22 claim** (WSL Ubuntu, OpenJDK 11.0.31, sbt, Spark 3.5.0 /
  Scala 2.12.17): `sbt -batch 'core/testOnly ...EnsembleByKeySuite'` →
  `Tests: succeeded 22, failed 0, canceled 0, ignored 0, pending 0` /
  `[success] Total time: 118 s, completed Aug 1, 2026, 8:37:36 AM`. All 19 declared `test(` blocks plus
  the 3 fuzzing-trait tests are listed individually in the run output. **Not trusted — re-run.**
- [x] **Re-ran both lint gates and found a NEW failure (Issue 17).** `sbt -batch 'core/scalastyle'` →
  `[error] .../EnsembleByKey.scala:179:15: Cyclomatic complexity of 12 exceeds max of 10` /
  `Processed 211 file(s) / Found 1 errors`. `core/Test/scalastyle` → `Processed 150 file(s) / 0 errors`.
  Root `sbt scalastyle` (the literal CI command at `pipeline.yaml:111`) → `Found 1 errors` /
  `[error] Total time: 11 s`. **A/B-proved it is this PR's regression** by running the standalone
  `org.scalastyle.Main` 1.0.0 CLI with the repo's own `scalastyle-config.xml` against the base file and
  the PR file in isolation: `base → Found 0 errors`, `pr → error ... line=179 column=15 / Found 1 errors`.
- [x] **Built an execution harness outside the repo** (`Probe12/13/14`, compiled with scalac 2.12.17
  against `sbt 'export core/Test/fullClasspath'`, 201 entries) and ran every case on **both builds** —
  the PR classes, and the base `EnsembleByKey` (`git show 8d528f3c:...`) compiled to a separate
  directory placed first on the classpath so it shadows the PR class. Every claim below is a measured
  line of output, not a reading of the source.
- [x] **Nested-key nullability (Issue 18)**: `P13|PR|A1-nestedKey-nullableOuter-collapse|MATCH=false`
  (`DECL key:string:null=false` vs `ACT key:string:null=true`) and the same for
  `A2-...-noncollapse`. Controls pass: `R1` (nullable leaf) `MATCH=true`, `T1/T2` (non-nullable
  top-level key) `MATCH=true`, `L1/L2` (deep nested, all non-nullable) `MATCH=true`.
- [x] **Nested-key materialization collisions (Issue 19)**:
  `P13|PR|C1-nestedLeafVsCaseVariantTop-noncollapse|MATCH=false` (declared carries `KEY`, actual does
  not, and `DATA` shows the `X`/`Y` values gone); `P14|D2-topLevelIdDestroyed-noncollapse|MATCH=true`
  with input `[row-1,[g],1.0];[row-2,[g],3.0]` → output `[g,[g],1.0,2.0];[g,[g],3.0,2.0]` (the
  top-level `id` values are silently replaced); `P14|S1-materializationShadowsStruct-noncollapse|ONLY_ACTUAL_THREW|AnalysisException:
  [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "k" ...|declared=k,x,a,score,mean(score)`.
  Base threw `UNRESOLVED_USING_COLUMN_FOR_JOIN` for all three, so these are newly reachable.
- [x] **Duplicate nested leaf names (Issue 20)**:
  `P14|E2-dupNestedLeaf-noncollapse|MATCH=false|inRows=2|outRows=0` with declared 6 fields vs actual
  7 (an extra trailing `k`) and `DATA:` empty. Collapse mode is fine: `P14|E1|MATCH=true|outRows=1|DATA=[p,q,2.0]`.
- [x] **`quotedRegexColumnNames` (Issue 21)**: `P13|PR|H1-backtickDottedKey-regexConf|ONLY_ACTUAL_THREW|AnalysisException:
  [INVALID_USAGE_OF_STAR_OR_REGEX] Invalid usage of regular expression 'a.b' in expression \`alias\`.`
  vs `P13|BASE|H1|...|actual=a.b,mean(score)|DATA=[a,2.0]` — base's `transform` worked. Same for
  `H2` on the aggregate side. `H3-plainNames-regexConf|MATCH=true` confirms ordinary names are unaffected.
- [x] **Resolution-fidelity gaps (Issue 22)**: `P13|PR|F1-qualifiedKey|BOTH_THREW` vs
  `P13|BASE|F1|...|DATA=[g,2.0]`; `G1-arrayNestedKey` PR throws vs `BASE ... actual=f:array<string>|DATA=[WrappedArray(x),2.0]`;
  `M1-mapNestedKey` PR throws vs `BASE ... DATA=[v,2.0]`. Cross-checked that Spark itself resolves all
  three today: `SPARKREF|F0-sparkGroupByQualified|OK|group,avg(score)`,
  `SPARKREF|G0-sparkGroupByArrayNested|OK|f,avg(score)`, `SPARKREF|F0b-datasetColQualified|OK|group`.
- [x] **All sixteen prior findings re-verified by execution, not by reading the Resolution Log.**
  Issue 1/6 — suite `:165,:184`; Issue 2 (purity) — `P14|PURITY|beforeDefault=None ... afterTransformSchema=None|isSet=false`,
  and only `afterTransform` writes `Some(...)` (pre-existing base behaviour); Issue 3 — suite `:355-359`;
  Issue 4 — `:344-347`; Issues 5/7/9 — `:321-363`; Issue 8 — suite `:128` + `P12|C2 MATCH=true`;
  Issue 10 — `P12|J1/J2 MATCH=true` (retain=false with nested keys) + suite `:199`; Issue 11 — suite `:215`;
  Issue 12 — `:153`, `:215`, suite `:242`; Issue 13 — `grep` over `EnsembleByKey.scala` finds no `require`
  mentioning retention (only `:143`, `:144`, `:161`), and `transformSchema` never reads
  `spark.sql.retainGroupColumns`; Issue 14 — suite `:259` + `P12|I1/I2` two-session probe;
  Issue 15 — `P13|L1/L2/N1` + `P12|H0/H0b` all `MATCH=true`; Issue 16 — suite `:303-319` and
  `P12|K2/K3` (nested aggregate inputs) `MATCH=true`.
- [x] **Binary/source compatibility**: `javap` on `com.microsoft.azure.synapse.ml.stages.EnsembleByKey`
  (PR vs base, `$anonfun`/`$adapted`/`$lessinit` filtered, sorted) → `PUBLIC_API_IDENTICAL`. Only
  additions on disk are the private nested `EnsembleByKey$ResolvedField[$].class` and
  `EnsembleByKey$ResolvedColumns[$].class`. No new/renamed/removed `Param`s → no `Wrappable`
  Python/R/.NET generated-wrapper surface change; `testObjects()` unchanged → generated fuzzing tests
  unchanged.
- [x] **No in-repo consumer depends on the changed schema**: `git grep -l EnsembleByKey` outside the two
  changed files hits only `EnsembleByKey.txt` (prose), `docs/**/_Stages.md` + 20 versioned website
  copies (default-conf example, no schema assertion), and an unrelated Zeppelin notebook blob.
- [x] **Security review**: the diff adds no I/O, reflection, deserialization, string-built SQL,
  credential handling, or network surface. `UnresolvedAttribute.parseAttributeName` is a pure parser
  over a caller-supplied param. Error messages echo caller-supplied names and the input schema's field
  names (`:108`, `:111`, `:129`), matching Spark's own `UNRESOLVED_COLUMN.WITH_SUGGESTION` disclosure
  for a caller who already holds the DataFrame. No finding.

---

## Issues

### Issue 17: `sbt scalastyle` now fails — this PR breaks the CI Style job
- **Severity**: High
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 179 (`override def transform`)
- **Description**: The Issue 15/16 fix pushed `transform` over the repository's cyclomatic-complexity
  gate. `scalastyle-config.xml:58-59` declares
  `<check level="error" class="...CyclomaticComplexityChecker"><parameter name="maximum">10</parameter>`,
  and `pipeline.yaml:111` runs `sbt scalastyle test:scalastyle` as the `Style` job's first step.
  ```
  $ sbt -batch 'core/scalastyle'
  [error] .../stages/EnsembleByKey.scala:179:15: Cyclomatic complexity of 12 exceeds max of 10
  [info] scalastyle Processed 211 file(s)
  [info] scalastyle Found 1 errors
  [error] Total time: 11 s
  $ sbt -batch 'scalastyle'          # the literal CI command
  [info] scalastyle Found 1 errors
  [error] Total time: 11 s
  ```
  A/B against the base commit, running the standalone `org.scalastyle.Main` 1.0.0 CLI with this repo's
  `scalastyle-config.xml` over each file in isolation:
  ```
  --- base ---  Found 0 errors / Found 0 warnings
  --- pr   ---  error file=.../EnsembleByKey.scala message=Cyclomatic complexity of 12 exceeds max of 10 line=179 column=15
                Found 1 errors
  ```
  The count comes from the branches now packed into one method: the two `Map` lambdas, the four-way
  `dataType` match in `newCols` (`:198-209`), the `retainGroupColumns` ternary (`:216`), the
  `getCollapseGroup` branch (`:221`), the `path.length == 1` branch inside the fold (`:225-226`), and
  the two nested predicate lambdas in `needToDrop` (`:228-230`).
- **Risk**: High for merge. The `Style` job fails, so merge validation is red regardless of test
  results. It is also the one gate a reviewer is least likely to re-run locally, and prior re-review
  rounds recorded `0 errors`, so the failure is easy to mistake for pre-existing.
- **Suggested Fix**: Extract the non-collapse merge into a private helper (e.g.
  `private def mergeWithGroups(dataset: DataFrame, aggregated: DataFrame, resolved: ResolvedColumns): DataFrame`)
  and/or move the aggregate-expression construction out of `transform` into a private
  `aggregateColumns(dataset, resolved)`. Both extractions are behaviour-preserving and each drops
  `transform` below the threshold. Re-run `sbt scalastyle test:scalastyle` as the gate.

### Issue 18: Nested grouping keys declare the leaf's nullability, but Spark widens it with the enclosing struct's
- **Severity**: Medium
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 134 (`ResolvedField(reference, path, resolvePath(...))`), 183-184 (`keyColumns`),
  240 and 247 (`resolvedColumns.keyFields.map(_.field)`)
- **Description**: `resolveField` returns the nested `StructField` verbatim, and `transformSchema`
  publishes it unchanged. At runtime the key is `Alias(GetStructField(outer, ord), leafName)`, and
  `GetStructField.nullable = child.nullable || childSchema(ordinal).nullable`. So whenever the
  enclosing struct is nullable and the leaf is not, the declared and actual nullability disagree —
  in **both** collapse modes:
  ```
  input: nested: struct<key: string NOT NULL> (nullable), score: double

  P13|PR|A1-nestedKey-nullableOuter-collapse|MATCH=false
     DECL: key:string:null=false | mean(score):double:null=true
     ACT : key:string:null=true  | mean(score):double:null=true
  P13|PR|A2-nestedKey-nullableOuter-noncollapse|MATCH=false
     DECL: key:string:null=false | nested:...:null=true | score:...:null=true | mean(score):...
     ACT : key:string:null=true  | nested:...:null=true | score:...:null=true | mean(score):...
  ```
  Controls confirm the scope: `R1` (nullable leaf in nullable struct) `MATCH=true`, `T1/T2`
  (non-nullable **top-level** key) `MATCH=true`, `L1/L2` (3-level path, no nullable ancestor)
  `MATCH=true`. Metadata is *not* affected — `Alias.metadata` falls through to `GetStructField.metadata`,
  verified by `P12|B1/B2/B3 MATCH=true`.
- **Risk**: This is a direct violation of the invariant the PR exists to establish, on the very feature
  the Issue 15 fix added. The shape is ordinary — an optional struct with required leaves is what you
  get from `Dataset[Outer]` where `Outer(nested: Option[Inner], ...)` and `Inner(key: Int)`, from Avro
  and protobuf sources, and from any `spark.read.schema(...)` with an explicit schema. Downstream
  `PipelineModel.transformSchema` consumers, schema-compat checks against a Delta/Parquet target, and
  code-generated writers act on the declared `nullable=false` and then receive a nullable column.
- **Suggested Fix**: Track nullability while descending in `resolvePath` and return a widened
  `StructField` — carry an `ancestorNullable` accumulator and finish with
  `field.copy(nullable = field.nullable || ancestorNullable)` (single-part paths keep today's value).
  Add a test with `spark.createDataFrame(rows, explicitSchema)` where the outer struct is nullable and
  the leaf is not, asserting `transformSchema(df.schema) === transform(df).schema` for both collapse
  modes.

### Issue 19: Non-collapse mode materializes nested keys under their bare leaf name, silently destroying same-named top-level columns
- **Severity**: Medium
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 224-227 (`datasetWithKeys` fold / `df.withColumn(resolvedKey.field.name, ...)`),
  242 (`keyNameSet`), 243-247 (`inputFields` filter)
- **Description**: The fold writes the resolved leaf into the **top-level** namespace under its bare
  name. `Dataset.withColumn` replaces any existing column the analyzer's resolver matches, so the new
  key silently consumes an unrelated top-level column. Three distinct measured failures:

  **(a) Case-variant collision → declared/actual schema divergence + column loss** (default,
  case-insensitive analysis). `transformSchema` excludes key names with an *exact* `Set` lookup
  (`keyNameSet(field.name)`, `:244`) while `withColumn` matches case-insensitively:
  ```
  input: nested: struct<key>, KEY: string, score: double ; key = "nested.key", collapse=false
  P13|PR|C1|MATCH=false
     DECL: key | nested | KEY | score | mean(score)      <- 5 fields
     ACT : key | nested |     | score | mean(score)      <- 4 fields; KEY is gone
     DATA: [a,[a],1.0,2.0];[a,[a],3.0,2.0]               <- the "X"/"Y" values are unrecoverable
  ```
  Under `spark.sql.caseSensitive=true` the same input is `MATCH=true` (`P12|C2`), which is what makes
  this easy to miss.

  **(b) Exact collision → silent data corruption that the schema check cannot catch.** Schemas agree,
  so nothing fails; the values are simply overwritten:
  ```
  input: id: string, meta: struct<id: string>, score: double ; key = "meta.id", collapse=false
  P14|D2-INPUT| [row-1,[g],1.0];[row-2,[g],3.0]
  P14|D2|MATCH=true|inRows=2|outRows=2
     DATA: [g,[g],1.0,2.0];[g,[g],3.0,2.0]               <- id "row-1"/"row-2" replaced by "g"
  ```

  **(c) Materialized key shadows a struct a later key still needs → `transformSchema` passes,
  `transform` throws.** The fold resolves each subsequent reference against the *accumulated* frame:
  ```
  input: a: struct<k>, k: struct<x>, score ; keys = ["a.k", "k.x"], collapse=false
  P14|S1|ONLY_ACTUAL_THREW|AnalysisException: [INVALID_EXTRACT_BASE_FIELD_TYPE]
        Can't extract a value from "k". Need a complex type [STRUCT, ARRAY, MAP] but got "STRING".
        |declared=k | x | a | score | mean(score)
  ```
  All three are newly reachable: base threw `UNRESOLVED_USING_COLUMN_FOR_JOIN` for every one
  (`P13|BASE|C1`, `D1`, and the `E2`/`L2` lines), so no existing user is relying on them, but nothing
  in the suite covers them either — `:279` only exercises a nested key on a 2-column frame with no
  name collisions.
- **Risk**: (b) is the serious one: a correct-looking schema with corrupted values, and `meta.id` /
  `payload.id` / `header.key` alongside a top-level `id`/`key` is exactly how flattened event data
  looks. (a) breaks the PR's own invariant and loses a column. (c) is the pre-`Pipeline`-run failure
  mode this PR set out to remove.
- **Suggested Fix**: Materialize into a collision-free name and rename after the join — resolve every
  key `Column` against the *original* `dataset` up front (as `keyColumns` at `:183-184` already does),
  project them under generated aliases that cannot clash (e.g. `s"__ebk_key_${'$'}i"`), join on those,
  then `withColumnRenamed` to the leaf names as the final projection. At minimum, if a nested leaf name
  collides with an existing top-level column, fail fast with a clear message the way `:161-164` does
  for key/output collisions. Add tests for all three shapes above.

### Issue 20: Two nested keys with the same leaf name emit an extra column and silently return zero rows
- **Severity**: Medium
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 182 (`keyNames`), 224-227 (fold), 231 (`join(aggregated, keyNames)`)
- **Description**: When two keys resolve to the same leaf name, the fold's second `withColumn`
  overwrites the first, so the left side of the join carries only the *last* key's value, while
  `aggregated` carries both. `keyNames` then contains the duplicate `"k"`, and the `USING` join is
  built on it twice:
  ```
  input: a: struct<k>, b: struct<k>, score ; keys = ["a.k","b.k"], collapse=false
  rows:  [[p],[q],1.0], [[p],[q],3.0]

  P14|E2|MATCH=false|inRows=2|outRows=0
     DECL: k | k | a | b | score | mean(score)            <- 6 fields
     ACT : k | k | a | b | score | k | mean(score)        <- 7 fields
     DATA: (empty)
  ```
  The join predicate becomes `left.k = "p" AND left.k = "q"` against a left side where `k` only ever
  holds `"q"`, so every row is dropped. Collapse mode is correct
  (`P14|E1|MATCH=true|outRows=1|DATA=[p,q,2.0]`), which means the defect is confined to the merge path.
  Base rejected this configuration outright (`P13|BASE|E2|...UNRESOLVED_USING_COLUMN_FOR_JOIN`).
- **Risk**: Silent total row loss with no exception and no warning — an `EnsembleByKey` stage in the
  middle of a pipeline turns the frame empty. Grouping by two same-named leaves of different structs
  (`left.id`, `right.id`) is a normal shape for joined/nested data.
- **Suggested Fix**: Same generated-alias fix as Issue 19 (unique join columns, rename afterwards). If
  duplicate leaf names are not going to be supported, detect them in `getSchemaFields` — where
  `keyFields` is built at `:156` — and `require` a distinct set of resolved key names when
  `collapseGroup` is false, with a test pinning the error.

### Issue 21: `spark.sql.parser.quotedRegexColumnNames=true` turns every backtick-quoted reference into a regex, so `transformSchema` succeeds and `transform` throws
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 184 and 200 (`dataset.col(...)`), 118 (`parseAttributeName`)
- **Description**: The PR routes both keys and aggregates through `Dataset.col`. That method checks
  `conf.supportQuotedRegexColumnName` **first** and, when the conf is on, delegates to `colRegex`,
  which treats a backtick-quoted string as a regular expression rather than a literal identifier.
  `resolveField` has no such branch, so the schema path resolves the literal field and the runtime
  path builds an `UnresolvedRegex`:
  ```
  spark.sql.parser.quotedRegexColumnNames = true ; df has a column literally named "a.b"
  P13|PR|H1-backtickDottedKey-regexConf|ONLY_ACTUAL_THREW|AnalysisException:
        [INVALID_USAGE_OF_STAR_OR_REGEX] Invalid usage of regular expression 'a.b' in expression `alias`.
        |declared=a.b:string:null=true | mean(score):double:null=true
  P13|PR|H2-backtickDottedCol-regexConf|ONLY_ACTUAL_THREW|AnalysisException:
        [INVALID_USAGE_OF_STAR_OR_REGEX] Invalid usage of regular expression 's.c' in expression `avg`.
        |declared=group | mean(`s.c`)
  ```
  `H1` is also a **regression**: base reached the same column through
  `Dataset.groupBy(String*)` → `Dataset.resolve` → `resolveQuoted`, which bypasses the regex branch
  entirely, so base's `transform` succeeded (`P13|BASE|H1|...|actual=a.b,mean(score)|DATA=[a,2.0]`).
  Ordinary unquoted names are unaffected (`P12|H3-plainNames-regexConf|MATCH=true`), so this only bites
  the backtick syntax the Issue 15/16 fix just made a supported input — and `:279-301` and `:303-319`
  test exactly that syntax under the default conf only.
- **Risk**: Low — needs a non-default conf. But the conf is a session-level setting some platforms
  enable globally, and the failure is precisely the declared-vs-actual divergence this PR exists to
  eliminate: a `Pipeline` propagates the schema fine and then blows up at run time.
- **Suggested Fix**: Build the `Column` from the already-parsed path instead of re-parsing the raw
  string — e.g. `new Column(UnresolvedAttribute(resolved.path.toSeq))` (or
  `dataset.col(resolved.path.map(p => s"`${'$'}{p.replace("`", "``")}`").mkString("."))`) so the regex
  branch is never reached and the runtime path is guaranteed to use the same parts `resolveField`
  resolved. Add a test that wraps the existing backtick cases in `withSQLConf("spark.sql.parser.quotedRegexColumnNames", "true")`.

### Issue 22: Qualifier-prefixed, `array<struct>` and `map` references that base resolved (and that Spark still resolves) are now rejected
- **Severity**: Low
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 120-132 (`resolvePath`)
- **Description**: `resolvePath` descends only through `StructType` and matches only against
  `schema.fields`, so it is strictly narrower than `LogicalPlan.resolve`, which also honours plan
  qualifiers and extracts through `ArrayType(StructType)` and `MapType` via `ExtractValue`. Measured on
  the same JVM, base class shadowing the PR class:
  ```
  # df.as("t"); key = "t.group"
  SPARKREF|F0-sparkGroupByQualified|OK|group,avg(score)          <- Spark resolves it today
  P13|BASE|F1|ONLY_DECLARED_THREW|...|actual=group,mean(score)|DATA=[g,2.0]   <- base transform WORKED
  P13|PR  |F1|BOTH_THREW|IllegalArgumentException: t.group does not exist. Available: group, score

  # arr: array<struct<f:string>>; key = "arr.f"
  SPARKREF|G0-sparkGroupByArrayNested|OK|f,avg(score)
  P13|BASE|G1|...|actual=f:array<string>:null=true|DATA=[WrappedArray(x),2.0]  <- base transform WORKED
  P13|PR  |G1|BOTH_THREW|IllegalArgumentException: arr.f does not exist. arr has type ArrayType(...)

  # m: map<string,string>; key = "m.k"
  P13|BASE|M1|...|actual=k:string|DATA=[v,2.0]                                 <- base transform WORKED
  P13|PR  |M1|BOTH_THREW|IllegalArgumentException: m.k does not exist. m has type MapType(...)
  ```
  Both entry points throw the same message, so the PR's invariant is intact — this is a pure
  capability regression, and the direct successor of Issue 15 (which fixed the struct and backtick
  half of the same mismatch).
- **Risk**: Low. `array`/`map` grouping keys are unusual, and a qualified reference requires an aliased
  input. But `EnsembleByKey().setKey("t.group")` on `df.as("t")` and `setKey("items.sku")` on an array
  column are both things that worked in the last release and now abort with
  `IllegalArgumentException: ... does not exist`, whose "Available:" list looks like it is denying a
  name the user can plainly see.
- **Suggested Fix**: Either (a) extend `resolvePath` to mirror `ExtractValue` — descend into
  `ArrayType(StructType)` yielding `ArrayType(fieldType)` and into `MapType` yielding the value type,
  both of which are fully derivable from the input schema so `transformSchema` stays honest; or
  (b) accept the narrower contract deliberately and say so in the message
  (`"... nested access is supported only through struct fields"`) plus tests pinning each rejection.
  Qualifiers cannot be supported from a bare `StructType` and are worth an explicit note in the
  parameter documentation for `keys`/`cols`.

---

## Notes (no action required, re-review 7)

- **The `caseSensitive`/`retainGroupColumns` source asymmetry is correct, not a bug.** I went in
  expecting Issue 11's defect to have been reintroduced for `caseSensitive` at `:152-153`, and probed
  it across two sessions with divergent confs. `Analyzer` mixes in `SQLConfHelper`
  (`def conf: SQLConf = SQLConf.get` → active session, fallback defaults), so `Dataset.col`/`drop`/
  `withColumn`/`join` all follow the **active** session; `RelationalGroupedDataset.toDF` reads
  `df.sparkSession.sessionState.conf.dataFrameRetainGroupColumns` → the **dataset's** session. The code
  matches both. `P12|I1` (dataset sensitive, active insensitive) throws exactly where Spark's own
  `groupBy` throws `AMBIGUOUS_REFERENCE`; `P12|I2` (dataset insensitive, active sensitive) is
  `MATCH=true` where Spark succeeds. **No finding.**
- **Nested-key metadata is preserved in both modes** — `Alias.metadata` falls through to
  `GetStructField.metadata`, so the materialized column and the aliased grouping column both carry the
  nested field's metadata (`P12|B1/B2 MATCH=true`, control `B3 MATCH=true`). Only nullability diverges
  (Issue 18).
- **`retainGroupColumns=false` continues to hold with nested keys** — `P12|J1/J2 MATCH=true`, so the
  Issue 10/13 fix composes with the Issue 15 fix.
- **Vector aggregate nullability is safe** — `Summarizer.mean` is non-nullable regardless of input:
  `P12|K1-nullableVectorAggregate|MATCH=true` and `K2-nestedVectorAggregate|MATCH=true` with a
  nullable vector inside a nullable struct, matching the `nullable = false` declared at `:171`.
- **`transformSchema` remains pure** — `P14|PURITY|beforeDefault=None|isSet=false` →
  `afterTransformSchema=None|isSet=false` → `afterTransform=Some(...)|isSet=false`. The
  `transform`-time `defaultParamMap` write at `:185-187` is pre-existing base behaviour; the PR
  narrows it usefully (`!isSet` instead of `get(...).isEmpty`, so a stale default no longer survives a
  `setCols` change — suite `:151`).
- **Public API and generated wrappers are unchanged** — `javap` PR vs base is `PUBLIC_API_IDENTICAL`;
  the only new classes are the private `ResolvedField`/`ResolvedColumns` case classes. No `Param`
  additions, so the Python/R/.NET codegen surface and `testObjects()` fuzzing set are untouched. There
  is no MiMa gate in this build, so the `javap` comparison is the compatibility check.
- **Test-suite quality remains high**, with a coverage gap that maps 1:1 to the new findings. Every
  positive test pairs `transformSchema` with `transform`; `assert(input.schema("features").metadata !==
  Metadata.empty)` (`:66`) keeps the metadata assertion from being vacuous; `Test / parallelExecution
  := false` (`build.sbt:274`) plus the `try/finally` restores in
  `withSQLConf`/`withActiveSession`/`withoutActiveSession` (`:410-437`) prevent conf and
  active-session leakage. Gaps: the nested-key tests (`:279-301`) use only a 2-column frame with a
  non-nullable `struct(...)` wrapper and no name collisions, so Issues 18, 19 and 20 are all invisible
  to them; and no test still collects rows under `retainGroupColumns=false` (verified externally in
  re-review 6).
- **The retain=false output change remains a user-visible behaviour change worth a release note** — see
  re-review 6; unchanged in this iteration.
- **Pre-existing and unchanged**: non-collapse mode drops rows with NULL keys (inner `USING` join);
  `vectorDims` is accepted but unused.

---

## Resolution Log - Round 1 Re-review 7
_Updated after the implementation and focused validation for Issues 17-22._

### Issue 17
- **Status**: Fixed
- **What changed**: Extracted aggregation construction, retention-independent group aggregation,
  non-collapse nested-key validation, and merge logic into private helpers. `transform` now
  coordinates those helpers instead of carrying all branches inline.
- **Why**: This preserves the runtime behavior while bringing every method below the repository's
  cyclomatic-complexity limit.
- **How verified**: `core/scalastyle` and `core/Test/scalastyle` both reported `Found 0 errors`,
  `Found 0 warnings`, and `Found 0 infos`. The focused suite also passed 26/26.

### Issue 18
- **Status**: Fixed
- **What changed**: `resolvePath` now carries ancestor nullability while descending through
  `StructType` and widens the resolved leaf field when any enclosing struct is nullable.
- **Why**: Spark's nested `GetStructField` output is nullable when either the leaf or an ancestor is
  nullable; `transformSchema` must declare that same field.
- **How verified**: Added an explicit-schema regression with a nullable outer struct and
  non-nullable leaf for both collapse modes. Both declared schemas equal the runtime schemas, and
  the resulting key is nullable. The focused suite passed 26/26.

### Issue 19
- **Status**: Fixed
- **What changed**: Non-collapse mode now validates every nested key leaf against all top-level
  fields using Spark's active resolver before materializing keys. Nested keys are materialized in
  one projection from the original dataset rather than through sequential `withColumn` mutation.
- **Why**: Failing fast prevents unrelated top-level data from being overwritten and prevents an
  earlier materialized key from invalidating a later nested path.
- **How verified**: Added exact leaf/top-level collision checks against both `transformSchema` and
  `transform`; each now raises the same actionable error. The focused suite passed 26/26.

### Issue 20
- **Status**: Fixed
- **What changed**: Non-collapse validation now requires resolved key leaf names to be distinct
  under Spark's configured case semantics.
- **Why**: The merge path cannot safely expose or join two top-level grouping columns with the same
  name; explicit rejection prevents the prior extra-column and zero-row behavior.
- **How verified**: Added two nested keys with the same leaf name and asserted matching fail-fast
  errors from `transformSchema` and `transform`. The focused suite passed 26/26.

### Issue 21
- **Status**: Fixed
- **What changed**: Runtime expressions are now built from each reference's already-parsed path via
  `UnresolvedAttribute` instead of routing raw strings through `Dataset.col`.
- **Why**: This bypasses `quotedRegexColumnNames` reinterpretation and guarantees runtime resolution
  uses the same path parts as schema resolution.
- **How verified**: Added literal dotted key and aggregate references under
  `spark.sql.parser.quotedRegexColumnNames=true`; declared and runtime schemas match in both cases.
  The focused suite passed 26/26.

### Issue 22
- **Status**: Fixed by explicit contract
- **What changed**: Kept schema resolution deliberately limited to top-level and nested struct
  fields, improved collection-path errors to state that limitation, documented that dataset
  qualifiers and array/map extraction are unsupported, and added consistency tests for each case.
- **Why**: A bare `StructType` cannot represent dataset qualifiers, and supporting collection
  extraction would expand the PR beyond its grouping-column schema goal. Rejecting these forms from
  both entry points preserves an honest schema/runtime contract.
- **How verified**: Added qualified, `array<struct>`, and map reference regressions; each now fails
  consistently from `transformSchema` and `transform` with the documented limitation. The focused
  suite passed 26/26.

## Round 1 Re-review 8

## Review Summary
- **Round**: 1 (re-review 8)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Base / Head**: `b4a5983c86c756e102941d02c8cdc2a21d0ef99c` /
  `3098665582880a49f33b6145a4b1a3f5ec5427d8`
- **Issues Found**: 2 (1 Medium, 1 Low)
- **Verdict**: ISSUES_FOUND

### Issue 23: Identical duplicate grouping keys escape distinctness validation
- **Severity**: Medium
- **Description**: Non-collapse validation skipped pairs whose configured reference strings were
  identical. Repeating `group` or `nested.key` therefore produced duplicate aggregate aliases and
  an extra runtime join column not declared by `transformSchema`.
- **Suggested Fix**: Reject every pair that resolves to the same output field name, regardless of
  whether the original reference strings are identical.

### Issue 24: Nested-key materialization fails on duplicate top-level column names
- **Severity**: Low
- **Description**: Materialization projected every input field through a name-based `col(...)`
  lookup. An unrelated duplicated top-level name was therefore ambiguous at runtime even though
  `transformSchema` succeeded.
- **Suggested Fix**: Preserve the input through star expansion and append only the materialized
  nested key columns.

## Resolution Log - Round 1 Re-review 8

### Issue 23
- **Status**: Fixed
- **What changed**: Removed the original-reference inequality condition from duplicate-key
  validation, so identical and differently quoted references resolving to the same key name are
  rejected consistently in non-collapse mode.
- **Why**: The join path requires distinct exposed key names; accepting identical references caused
  runtime output to be wider than the declared schema.
- **How verified**: Added duplicate top-level key tests under both
  `spark.sql.retainGroupColumns` settings and a collapse-mode compatibility control.

### Issue 24
- **Status**: Fixed
- **What changed**: Nested-key materialization now selects `col("*")` and appends materialized key
  columns instead of resolving every top-level input field by name.
- **Why**: Star expansion preserves duplicate input attributes without ambiguous name lookup.
- **How verified**: Added a non-collapse nested-key regression with an unrelated duplicated
  top-level column and exact schema equality/order assertions.

## Round 1 Re-review 9

## Review Summary
- **Round**: 1 (re-review 9)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Issues Found**: 1 (1 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 25: Column references are re-resolved under a different Spark session
- **Severity**: Medium
- **Description**: Schema resolution followed the thread-active session, but runtime unresolved
  Columns could be analyzed with the dataset session. Opposing case-sensitivity settings therefore
  allowed `transformSchema` to pass while `transform` failed or became ambiguous.
- **Suggested Fix**: Bind runtime Columns to the already-resolved analyzed attributes and avoid
  name-based drop and USING-join resolution in the non-collapse merge.

## Resolution Log - Round 1 Re-review 9

### Issue 25
- **Status**: Fixed
- **What changed**: Resolved fields now retain top-level and nested ordinals. Runtime grouping and
  aggregate Columns are built from the dataset's analyzed attributes plus explicit
  `GetStructField` expressions, preserving the schema-time resolution decision. The non-collapse
  merge now uses bound equality conditions and an explicit bound output projection instead of
  string-based `drop` and USING-join resolution.
- **Why**: This removes secondary session-dependent name resolution while preserving the active
  session semantics used by `transformSchema`.
- **How verified**: Added opposing active/dataset case-sensitivity tests in both directions and a
  no-active-session case, each asserting exact declared/runtime schema equality.

## Round 1 Re-review 10

## Review Summary
- **Round**: 1 (re-review 10)
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: claude-opus-5
- **Base / Head**: `b4a5983c86c756e102941d02c8cdc2a21d0ef99c` /
  `3098665582880a49f33b6145a4b1a3f5ec5427d8`
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Focused `EnsembleByKeySuite` passed 29/29.
- [x] `core/scalastyle` and `core/Test/scalastyle` reported zero errors and warnings.
- [x] Issues 1-25 were independently rechecked against the current implementation.
- [x] Bound Catalyst attribute and exprId behavior was exercised across joined, self-joined,
  typed, nested, duplicate-name, empty, and null-key inputs.
- [x] Case sensitivity, multi-session behavior, both grouping-retention modes, metadata,
  nullability, serialization, Pipeline chaining, and physical equi-join plans were verified.

No actionable findings remain in Round 1.
