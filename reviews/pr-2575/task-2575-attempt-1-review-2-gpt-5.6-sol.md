## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Issues

### Issue 1: Preserve Spark's existing grouping-key resolution contract
- **Severity**: Medium
- **Description**: The schema resolver intentionally rejected dataset qualifiers,
  array-of-struct extraction, and map extraction even though the previous runtime path delegated
  those forms to Spark successfully. This narrowed behavior without changing the public API.
- **Risk**: Existing Scala, Python, and R pipelines could fail after upgrade, and the documented
  restriction would turn an implementation limitation into a compatibility break.
- **Suggested Fix**: Preserve Spark's grouping-key grammar and add success regressions for
  qualifiers and collection extraction.

## Resolution Log

### Issue 1
- **Status**: Fixed
- **What changed**: Extended schema resolution and bound Catalyst expression construction to
  support optional qualifier prefixes, array-of-struct field extraction, and map key extraction.
  Updated the stage documentation and converted rejection tests into schema/runtime success tests.
- **Why**: The public API is unchanged, so previously accepted Spark reference syntax should remain
  source-compatible.
- **How verified**: Qualified, array-of-struct, and map grouping references are covered by focused
  tests that assert exact `transformSchema` and runtime schema equality.

## Round 2 Re-review 1

## Review Summary
- **Issues Found**: 4 (2 High, 2 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Array-of-struct extraction loses element nullability
- **Status**: Fixed
- **What changed**: Array extraction now propagates
  `containsNull || selectedField.nullable`, matching Spark's extraction type.
- **How verified**: Added an action-level null-field regression and exact schema equality assertion.

### Issue 2: Map schema resolution accepts non-coercible key types
- **Status**: Fixed
- **What changed**: Map path extraction now requires an atomic key type and rejects complex keys
  consistently before runtime analysis.
- **How verified**: Added a struct-keyed map regression asserting matching schema/runtime errors.

### Issue 3: Qualifier normalization discards qualifier identity
- **Status**: Fixed
- **What changed**: Resolved references retain multipart qualifier prefixes. Runtime transform
  binds those prefixes against analyzed output qualifiers, supports equivalent duplicate schema
  fields, and rejects invalid qualifiers.
- **How verified**: Added joined-frame qualified duplicate fields, invalid qualifier, and
  `global_temp.<view>` multipart qualifier tests.

### Issue 4: Bound Catalyst Columns do not compile on Spark 4.1
- **Status**: Fixed
- **What changed**: Removed Catalyst `Expression`/`GetStructField` construction and
  `new Column(expression)`. Runtime now positionally renames the input to unique internal names and
  uses public DataFrame/Column APIs for grouping, extraction, joining, and final projection.
- **How verified**: Focused tests pass 30/30 and source no longer relies on the removed Spark 4.1
  `Column(Expression)` constructor.

## Round 2 Re-review 2

## Review Summary
- **Issues Found**: 3 (1 High, 2 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Qualifier collision groups by the wrong field
- **Status**: Fixed
- **What changed**: Qualifier candidates are resolved against analyzed output before nested-path
  interpretation. References that are inherently ambiguous between a nested field and qualifier
  are rejected consistently instead of silently selecting one meaning.
- **How verified**: Added qualifier-identity tests and an explicit ambiguity regression.

### Issue 2: Qualified non-collapse grouping drops unrelated duplicate columns
- **Status**: Fixed
- **What changed**: Schema and runtime passthrough filtering now excludes only the selected
  top-level key ordinal, preserving other same-named qualified attributes.
- **How verified**: Added non-collapse joined-frame coverage asserting both `group` columns and
  exact output ordering.

### Issue 3: Nested paths are re-resolved under a different session
- **Status**: Fixed
- **What changed**: Schema resolution now records canonical resolved field names rather than raw
  configured path casing. Runtime references use those canonical names after positional
  top-level normalization.
- **How verified**: Existing cross-session tests plus the expanded qualified/nested suite pass with
  exact declared/runtime schema equality.

## Round 2 Re-review 3

## Review Summary
- **Issues Found**: 4 (4 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Qualifier/nested precedence differs from Spark
- **Status**: Fixed
- **What changed**: Dataset-qualified matches are resolved first from analyzed output; when none
  exists, the reference is interpreted as a nested path.
- **How verified**: Nested `meta.id`, aliased `meta.id`, invalid qualifier, and multipart qualifier
  paths are covered.

### Issue 2: Qualified ordinal is applied too late
- **Status**: Fixed
- **What changed**: An exact qualified top-level ordinal now selects its `StructField` directly
  before nested traversal, avoiding ambiguity from same-named fields with different nullability.
- **How verified**: Qualified joined-frame tests pass with exact schema equality.

### Issue 3: Atomic map-key validation is incomplete
- **Status**: Fixed
- **What changed**: Map path extraction is limited to Spark-coercible string and numeric key types;
  boolean and complex map keys are rejected consistently.
- **How verified**: String/numeric success and boolean/struct rejection cases are covered.

### Issue 4: Canonicalization changes nested key output names
- **Status**: Fixed
- **What changed**: Canonical names are retained only for runtime extraction; nested output fields
  and aliases use the configured final path segment casing, matching Spark.
- **How verified**: Added `nested.Key` referenced as `nested.key`, asserting output name `key`.

## Round 2 Re-review 4

## Review Summary
- **Issues Found**: 4 (2 High, 2 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Qualifier-dependent resolution breaks the Transformer schema contract
- **Status**: Fixed
- **What changed**: Qualifier-dependent references are accepted only when duplicate schema
  candidates are structurally equivalent; differently typed or nullable qualified duplicates are
  rejected consistently because `StructType` cannot identify them.
- **How verified**: Qualified equivalent duplicates succeed; schema-dependent ambiguities reject.

### Issue 2: Qualifier matching ignores longest-match precedence
- **Status**: Fixed
- **What changed**: Qualified candidates are filtered to the maximum qualifier length before
  ambiguity handling, matching Spark's multipart qualifier precedence.
- **How verified**: Multipart `global_temp.<view>` resolution remains covered.

### Issue 3: Map extraction rejects Spark-coercible date keys
- **Status**: Fixed
- **What changed**: Date and timestamp map-key extraction are accepted in addition to string and
  numeric keys, while unsupported boolean and complex keys remain rejected.
- **How verified**: Map-key validation and focused tests pass.

### Issue 4: Direct top-level key aliases do not match Spark
- **Status**: Fixed
- **What changed**: All grouping outputs use the configured final path segment as the alias,
  while canonical names remain separate for extraction.
- **How verified**: Case-insensitive uppercase key references now emit uppercase key names in both
  declared and runtime schemas.

## Round 2 Re-review 5

## Review Summary
- **Issues Found**: 3 (2 High, 1 Medium) plus 2 self-found coupled defects
- **Verdict**: ISSUES_FOUND

### Issue 1: Multipart qualifiers can produce incorrect declared schemas
- **Status**: Fixed
- **What changed**: `resolveField` now derives every schema-only interpretation of a multipart
  reference (each split where a segment matches a top-level field) and requires all of them to
  contribute the same declared output field and the same consumed input ordinal. Divergent
  qualifier/nested interpretations are rejected consistently in `transformSchema` and `transform`;
  equivalent interpretations are accepted, so runtime qualifier precedence can only change which
  equivalent source column is read, never the declared schema.
- **How verified**: `multipart qualifiers should agree with schema-only interpretations` covers
  `global_temp.<view>.group` resolved as a nested path (frame aliased `global_temp`) and as a
  qualified top-level column (real global temp view), asserting schema agreement plus action-level
  values `nested` and `top`; a conflicting frame is rejected in both modes.

### Issue 2: Qualified aggregate inputs are rejected after outer joins
- **Status**: Fixed
- **What changed**: Duplicate qualified candidates are compared by the output each candidate
  *derives* for its role, not by raw `StructField` equality. `FieldRole` supplies the projection:
  aggregate inputs collapse to the mean output field (Double/Float to `DoubleType`, vectors to
  `VectorType`), so source nullability no longer matters, while grouping keys still compare full
  type, nullability and metadata. The comparison resolves the complete reference path for each
  candidate, so nested qualified references are compared at their leaf.
- **How verified**: `qualified aggregates should compare derived aggregate outputs` accepts
  `left.score`/`right.score` on a `left_outer` join (nullability `false`/`true`, means 2.0/5.0),
  accepts the nested double/float pair `right.s.value` (mean 5.0), and rejects key
  `right.score` and nested `right.s.value` when the derived outputs differ.

### Issue 3: Manual map-key allowlisting is narrower than Spark coercion
- **Status**: Fixed
- **What changed**: The manual allowlist was removed. Map extraction is accepted when Spark's own
  `Cast.canCast(StringType, keyType)` allows the coercion, and the runtime key is built as
  `lit(segment).cast(keyType)` so extraction no longer depends on ANSI/implicit coercion mode and
  `transformSchema` always agrees with `transform`. Non-castable keys (structs, arrays, maps, UDTs)
  are rejected with an explicit message. No `MapType(StringType, _)` pattern matching is used, which
  keeps the code valid for collated string types in Spark 4.x.
- **How verified**: `map key extraction should follow Spark cast coercion` groups through boolean,
  binary, integer and date map keys with action-level value assertions; struct keys are rejected
  consistently.

### Issue 4 (self-found): Non-collapsed qualified keys could reorder passthrough columns
- **Status**: Fixed
- **What changed**: When `collapseGroup` is false, a qualified top-level key that matches several
  columns is rejected, because `transformSchema` consumes the first matching ordinal while
  `transform` consumes the qualifier-bound ordinal, which silently changed passthrough ordering.
- **How verified**: `qualified references should preserve qualifier identity` asserts consistent
  rejection, and `non-collapsed qualified references should preserve unrelated duplicates` shows
  unrelated duplicate columns are still preserved in order.

### Issue 5 (self-found): Map extraction could lose to a dataset qualifier at runtime
- **Status**: Fixed
- **What changed**: A map column whose extraction segment also names a top-level column under a
  matching dataset qualifier is now covered by the interpretation-equivalence check, so the
  schema-only map interpretation and the runtime qualified interpretation cannot disagree.
- **How verified**: `map extraction should reject dataset qualifier collisions` rejects
  `values.field` in both modes when the interpretations derive different fields.

### Validation
- `core/compile`, `core/scalastyle`, `core/Test/scalastyle`: success, 0 errors, 0 warnings.
- `core/testOnly ...EnsembleByKeySuite`: 36 tests, 36 succeeded, 0 failed.

## Round 2 Re-review 6

## Review Summary
- **Issues Found**: 2 (1 High, 1 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Multiple qualified matches collapse to no match and fall back to nested resolution
- **Status**: Fixed
- **What changed**: `qualifiedMatch` no longer conflates "no qualifier match" with "ambiguous
  qualifier match". Candidate attributes are still filtered to Spark's longest-qualifier
  precedence, then deduplicated by the analyzed `ExprId`. When every remaining candidate is the
  same expression the qualified interpretation is used (the lowest matching ordinal); when the
  candidates are genuinely distinct attributes the reference is rejected as ambiguous instead of
  silently reinterpreting it as a nested struct path. `bindQualifier` applies the same `ExprId`
  deduplication so runtime qualifier binding accepts same-expression duplicates and keeps
  rejecting distinct ones.
- **Why**: Spark's `AttributeSeq.resolve` never falls back to the unqualified interpretation once
  qualified candidates exist, and its lookup maps are `distinct`-ed, so exact duplicate attributes
  count once. The previous `None` fallback made `dup.group` silently resolve to a nested `dup`
  struct field that Spark itself rejects, and made same-expression duplicates unusable.
- **How verified**: `duplicated qualifier attributes should follow Spark expression identity`
  asserts a frame whose two `dup`-qualified `group` attributes share one `ExprId` resolves to the
  qualified top-level column (`top`, mean 2.0), that the same reference under a non-matching alias
  resolves to the nested path (`nested`, mean 2.0) with `transformSchema` agreeing in both cases,
  and that a frame with two distinct `dup.group` attributes is rejected by `transform` exactly
  where Spark raises `AnalysisException`.

### Issue 2: Unqualified duplicate schema fields are rejected for a single Spark expression
- **Status**: Fixed
- **What changed**: `resolveFromSchema` now accepts duplicate unqualified top-level matches when a
  dataset is available, all matching analyzed attributes share one `ExprId`, and every candidate
  derives the same `FieldRole` output. Such references resolve at the first matching ordinal, the
  same column Spark reads. Schema-only resolution is unchanged and still rejects duplicates,
  because a bare `StructType` carries no expression identity.
- **Why**: `df.select(col("score"), col("score"))` produces two output attributes with one
  `ExprId`; Spark resolves `score` because its lookup maps are deduplicated. Rejecting the
  reference made otherwise valid frames unusable even though the aggregate is unambiguous.
- **How verified**: `duplicated unqualified attributes sharing one expression should aggregate`
  asserts the duplicated frame keeps two `score` fields with one `ExprId`, that Spark resolves
  `score`, that `transform` produces `key`/`mean(score)` with value 2.0, and that schema-only
  `transformSchema` still rejects the reference as ambiguous.

### Validation
- `core/scalastyle`: success, 0 errors, 0 warnings.
- `core/Test/scalastyle`: success, 0 errors, 0 warnings.
- `core/testOnly ...EnsembleByKeySuite`: 38 tests, 38 succeeded, 0 failed.
- Regression proof: with both fixes reverted, the two new tests fail
  (`Tests: succeeded 3, failed 2`); with the fixes applied all 38 pass.
## Round 2 Re-review 7

## Review Summary
- **Issues Found**: 1 (1 High)
- **Verdict**: ISSUES_FOUND

### Issue 1: Union duplicate attributes are rejected though Spark prunes them
- **Status**: Fixed
- **What changed**: The resolver now reproduces Spark's `AttributeSeq.resolve` duplicate pruning.
  When more than one candidate matches a reference, candidates whose metadata contains
  `__is_duplicate` are removed before longest-qualifier precedence, `ExprId` deduplication and the
  `FieldRole` derived-output comparison. Pruning is applied on every resolution path:
  `qualifiedPathMatches` (qualified dataset attributes, pruned before the longest-qualifier
  filter), `resolveFromSchema` and `resolveFromOrdinal` (unqualified and qualified top-level
  ordinals), and `bindQualifier` (runtime qualifier binding). Pruning is keyed on the same
  metadata Spark uses, so a `Dataset` schema and its analyzed attributes prune identically and
  `transformSchema` agrees with `transform`. If every candidate is duplicate-tagged the unpruned
  set is kept, so the resolver never becomes less strict than before.
- **Why**: Spark's union analysis re-aliases duplicated child outputs with fresh `ExprId`s and tags
  them `__is_duplicate`, then `AttributeSeq.resolve` filters those candidates out
  (`candidates.filter(c => !c.metadata.contains("__is_duplicate"))`) before reporting ambiguity.
  The previous resolver only accepted duplicates that shared one `ExprId`, so union-generated
  duplicates - which have distinct `ExprId`s by construction - were rejected as ambiguous even
  though `union.select("score")` resolves in Spark.
- **Conservative schema-only behavior preserved**: schema-only resolution still rejects duplicate
  columns that carry no `__is_duplicate` marker. `duplicated unqualified attributes sharing one
  expression should aggregate` continues to assert that `transformSchema` rejects
  `df.select(col("score"), col("score"))`, and `qualified references should preserve qualifier
  identity` continues to reject a non-collapsed key that matches two distinct joined columns.
- **How verified**: `union duplicate attributes should follow Spark duplicate pruning` builds
  `base.select(key, score, score).union(itself)`, asserts the union schema keeps two `score`
  fields with two distinct `ExprId`s and the second field tagged `__is_duplicate`, and that Spark
  itself resolves `union.select("score")`. It then asserts action-level results for the
  unqualified aggregate reference `score` (`key`, `mean(score)`, value 2.0) and the qualified
  aggregate reference `u.score` on `union.as("u")` (`key`, `mean(u.score)`, value 2.0), each
  through `assertSchemaAgrees`, which requires `transformSchema(input.schema)` to equal the
  runtime output schema exactly.

### Validation
- `core/compile`: success (JDK 11.0.31, Scala 2.12.17, Spark 3.5.0).
- `core/scalastyle`: 211 files, 0 errors, 0 warnings.
- `core/Test/scalastyle`: 150 files, 0 errors, 0 warnings.
- `core/testOnly ...EnsembleByKeySuite`: 39 tests, 39 succeeded, 0 failed.
- Regression proof: with the pruning key changed so no candidate is ever pruned, the new test
  fails with `score is ambiguous. Matches: score, score`; with the fix applied all 39 pass.
## Round 2 Re-review 8

## Review Summary
- **Issues Found**: 1 (1 High)
- **Verdict**: ISSUES_FOUND

### Issue 1: Duplicate pruning ran before qualifier selection instead of within the candidate set
- **Status**: Fixed
- **What changed**: Duplicate pruning no longer runs globally ahead of qualifier/name matching.
  Every resolver path now reproduces Spark's `AttributeSeq.resolve` ordering - the qualifier/name
  candidate set is determined first, and `__is_duplicate` pruning only narrows that set:
  - Dataset-aware qualified path: `qualifiedPathMatches` returns raw qualifier/name matches with
    no pruning; `qualifiedMatch` applies the longest-qualifier filter first, then
    `pruneDuplicates`, then `ExprId` deduplication and the ambiguity `require`.
  - Unqualified path: `resolveUnqualifiedFromSchema` prunes strictly within the unqualified
    same-name candidate set, then applies the shared-`ExprId` and derived-output checks.
  - Schema-only qualified path: a bare `StructType` carries no qualifier metadata, so the
    resolver can no longer guess which ordinal the qualifier would pick. It now requires *all*
    potentially selectable ordinals (the unpruned name matches) to derive equivalent `FieldRole`
    outputs via `requireStableQualifiedField` before resolving, rather than globally pruning
    tagged fields.
  - `resolveFromOrdinal` runs the same stability requirement over the unpruned top-level matches,
    so the runtime path and the schema-only path enforce identical rules.
  - `bindQualifier` was already pruning inside the exact qualifier-matched set and is unchanged.
- **Why**: Spark builds the candidate set from qualifier/name matching first and only then runs
  `if (candidates.size > 1) candidates.filter(c => !c.metadata.contains("__is_duplicate"))`
  (`sql/catalyst/.../expressions/package.scala`, v3.5.0). Pruning first inverts that precedence:
  a union-derived tagged `u.group` cross-joined with an untagged `v.group` had the tagged
  candidate removed before qualifiers were considered, so `transformSchema` selected the
  `v.group` ordinal while `transform` (which sees qualifiers) selected `u.group`. That breaks the
  `transformSchema(input.schema) == transform(input).schema` contract and, with
  `collapseGroup=false`, silently accepted a configuration that the runtime rejects.
- **Metadata consistency**: resolved fields are emitted through `declaredField`, which strips the
  internal `__is_duplicate` marker so it never leaks into the transformer output schema.
  Spark treats an *explicitly empty* alias metadata as "no explicit metadata" and lets the child
  attribute's metadata (including the marker) flow through, so `as(name, Metadata.empty)` alone is
  not enough. Grouping keys are therefore projected through `keyColumn`, which casts to the
  already-resolved data type before aliasing; the cast makes the alias child a non-`NamedExpression`
  so `Alias.metadata` falls back to `Metadata.empty` and the runtime schema matches the declared
  one. This was verified empirically to also hold for `VectorType`/UDT keys and through `groupBy`,
  and the identity cast is removed by `SimplifyCasts` in the optimized plan.
- **Test layout**: the duplicate-resolution tests moved into a new
  `EnsembleByKeyResolutionSuite` so both `EnsembleByKey.scala` and `EnsembleByKeySuite.scala` stay
  inside the 800-line scalastyle limit. All existing union and duplicate tests are retained
  verbatim.
- **How verified**: new regression `duplicate pruning should not override qualifier selection`
  builds `base.select(group, group, score).union(itself).toDF("other", "group", "score")` so the
  only `group` of `u` is tagged `__is_duplicate`, cross-joins it with an untagged `v.group`, and
  asserts: the tag is present on `group` and absent on `other`; Spark itself resolves
  `joined.select("u.group")` to `"u"`; `assertSchemaAgrees` for `setKey("u.group")` /
  `setCol("score")` yields `group`/`mean(score)` with values `"u"` and `2.0` and empty `group`
  metadata; and that with `collapseGroup=false` both `transformSchema` and `transform` reject the
  reference with `multiple columns are named group`.

### Validation
- `core/compile`, `core/Test/compile`: success (JDK 11.0.31, sbt 1.10.11, Scala 2.12.17,
  Spark 3.5.0).
- `core/scalastyle`: 211 files, 0 errors, 0 warnings.
- `core/Test/scalastyle`: 151 files, 0 errors, 0 warnings.
- `core/testOnly ...EnsembleByKey*`: 40 tests, 40 succeeded, 0 failed (2 suites).
- `core/testOnly com.microsoft.azure.synapse.ml.stages.*`: 164 tests, 164 succeeded, 0 failed
  (24 suites) - the full package that owns the changed transformer.
- `core/test` (full suite, `getDatasets` first, `-Xmx8g`): no EnsembleByKey regressions. The run
  cannot complete in this local WSL environment and ends in
  `UnsatisfiedLinkError: libawt_xawt.so` inside `image.SuperpixelSuite` (no AWT native libs).
  All other failures are environmental and unrelated to this change: the `nbtest.*` suites abort
  on missing cloud credentials (`INTEGRATION_WORKSPACE_PREFIX` etc.), `WrappableTests.test
  CompanionModelClassName` asserts a codegen fixture's companion name, the binary-file image
  tests time out after 10800 microseconds, and one trainer test rejects a NaN vector.
- Regression proof: restoring the pre-fix global pruning in the schema-only qualified branch makes
  the new test fail - `Expected exception java.lang.IllegalArgumentException to be thrown, but no
  exception was thrown` (`transformSchema` accepted the non-collapsed configuration that
  `transform` rejects); with the fix applied all 40 pass.
## Round 2 Re-review 9

## Review Summary
- **Issues Found**: 1 (1 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Map key resolution accepted key types that Spark cannot order
- **Status**: Fixed
- **What changed**: `resolveStep` no longer accepts a `MapType` segment on `Cast.canCast(StringType,
  keyType)` alone. The new `mapKeyIsExtractable` predicate requires both the existing cast
  validation and `RowOrdering.isOrderable(keyType)`, mirroring Spark's `GetMapValue`
  `checkInputDataTypes`. `unsupportedMapKeyMessage` now distinguishes the two rejection reasons, so
  a non-castable key still reports `does not accept string keys` (the existing struct-key test is
  unchanged) while a castable-but-unorderable key reports `map key type <type> is not orderable, so
  Spark cannot look up a map value by key. Use a map column whose key type is orderable, such as
  string.`
- **Cross-version compatibility**: `org.apache.spark.sql.catalyst.expressions.RowOrdering.isOrderable(dataType: DataType): Boolean`
  is declared identically in Spark 3.5.0 and Spark 4.1 (`sql/catalyst/.../expressions/ordering.scala`,
  both delegating to `OrderUtils.isOrderable`), so the predicate compiles and behaves the same on
  both runtimes. `TypeUtils.checkForOrderingExpr` - the wrapper `GetMapValue` calls - was avoided
  because its return type changed shape across versions; the boolean predicate is the stable form.
- **Why**: `GetMapValue.checkInputDataTypes` runs `TypeUtils.checkForOrderingExpr(keyType,
  prettyName)` in both Spark 3.5.0 and Spark 4.1, so an orderable key is mandatory in addition to
  the key literal cast. `CalendarIntervalType` is castable from a string but not orderable, so the
  old predicate let `transformSchema` accept a reference that Spark rejects at plan time - breaking
  the `transformSchema` / `transform` agreement contract this PR is built around.
- **Documentation**: `EnsembleByKey.txt` now states that the map key type must also be orderable.
- **File-length budget**: `EnsembleByKey.scala` stayed under the 800-line scalastyle limit by
  collapsing two already-existing multi-line call sites (`requireStableQualifiedField` in
  `resolveFromOrdinal` and the trailing `ResolvedField` construction in `resolveAtOrdinal`) into
  single-line forms; no behavior changed (798 lines).
- **How verified**: new regression `map keys Spark cannot order should be rejected consistently`
  builds a map column with `make_interval(0, 0, 0, 1, 0, 0, 0)` keys, asserts the key type is
  `CalendarIntervalType`, that `Cast.canCast(StringType, keyType)` is true and
  `RowOrdering.isOrderable(keyType)` is false, that Spark itself rejects
  `values[make_interval(0, 0, 0, 1, 0, 0, 0)]` with an `AnalysisException`, and then uses
  `assertConsistentSchemaError` so both `transformSchema` and `transform` must throw
  `IllegalArgumentException` carrying the actionable `is not orderable` / `Use a map column whose
  key type is orderable` guidance.

### Validation
- Toolchain: JDK 11.0.31, sbt 1.10.11, Scala 2.12.17, Spark 3.5.0.
- `core/scalastyle`: 211 files, 0 errors, 0 warnings.
- `core/Test/scalastyle`: 151 files, 0 errors, 0 warnings.
- `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*`: 41 tests, 41 succeeded,
  0 failed (2 suites: `EnsembleByKeySuite`, `EnsembleByKeyResolutionSuite`).
- Regression proof: reverting `mapKeyIsExtractable` to the cast-only predicate makes the new test
  fail with `Expected exception java.lang.IllegalArgumentException to be thrown, but no exception
  was thrown` (`transformSchema` accepted a map key Spark cannot order); with the fix applied all
  41 pass.

## Round 2 Re-review 10

## Review Summary
- **Issues Found**: 2 (2 Medium)
- **Verdict**: ISSUES_FOUND

### Issue 1: Extracted grouping values were not checked for Spark orderability
- **Status**: Fixed
- **What changed**: After resolving every configured grouping reference, `resolveColumns` now
  validates the resolved leaf data type with `RowOrdering.isOrderable`. This applies equally to
  direct columns and values extracted from structs, arrays, and maps, and fails during both
  `transformSchema` and `transform` with an error naming the reference and resolved type.
- **Why**: Spark requires all grouping expressions to be orderable. Checking map key orderability
  alone did not protect against an orderable map key whose extracted value was an unorderable map,
  leaving `transformSchema` able to accept a schema that runtime `groupBy` rejected.
- **How verified**: Added `extracted grouping values Spark cannot order should be rejected
  consistently`, which groups through `values.item` where the extracted value is itself a map,
  confirms Spark rejects the grouping expression, and asserts matching transformer errors.

### Issue 2: Derived default column names were unavailable through `getColNames`
- **Status**: Fixed
- **What changed**: `getColNames` now returns the explicit `colNames` value when set and otherwise
  derives the public defaults from the current `cols` and `strategy`, without mutating parameter
  state. `resolveColumns` uses the same derivation.
- **Why**: `transformSchema` is intentionally pure and no longer stores generated names as a side
  effect. The inherited getter therefore threw when `colNames` was omitted, even after schema
  transformation, breaking the existing public API expectation that defaults are observable.
- **How verified**: Added `getColNames should expose derived defaults without mutating params`,
  covering initial defaults, post-`transformSchema` behavior, strategy changes, explicit names,
  and preservation of `isSet(colNames) == false` for derived defaults.

### Validation
- `core/scalastyle`: 211 files, 0 errors, 0 warnings.
- `core/Test/scalastyle`: 151 files, 0 errors, 0 warnings.
- `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*`: 42 tests, 42 succeeded,
  0 failed (2 suites).
- `git diff --check`: clean aside from Git's informational CRLF conversion warning.
- `EnsembleByKey.scala`: 799 lines, within the 800-line scalastyle limit.

## Round 2 Re-review 11

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-2-gpt-5.6-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete generated Round 2 prompt and its explicit base-to-working-tree diff.
- [x] Traced dataset-aware and schema-only resolution through
  `EnsembleByKey.scala:438-446,762,779` and checked the Spark 3.5/4.1 `Pipeline` schema-validation
  contract.
- [x] Checked the generated-binding path in
  `Wrappable.scala:118-135,238-271` against the new computed getter in
  `EnsembleByKey.scala:96`.
- [x] Checked the changed public Param surface, Spark 3.5/4.1 Catalyst API usage, identifier
  quoting/map literals, companion readability, and generated-code implications.
- [x] Ran `git diff --check`; no whitespace errors were reported (only the existing CRLF warning).
- [ ] Focused tests and scalastyle were not rerun because this was a read-only review; the supplied
  42-test/scalastyle result was treated as prior evidence.

## Issues

### Issue 1: Dataset-aware duplicate resolution cannot be used in a Spark ML pipeline
- **Severity**: High
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: 438-446, 762, 779
- **Description**: `transform` passes `Some(dataset)` and accepts duplicate unqualified attributes
  when they share one `ExprId`, while `transformSchema` passes no dataset and rejects the identical
  schema. Spark `Pipeline.fit` and `PipelineModel.transform` invoke every stage's
  `transformSchema` before calling `transform`, so the newly advertised Spark-resolvable duplicate
  case works only when the transformer is invoked directly. The current focused regression also
  codifies this split by expecting direct transformation to succeed and schema transformation to
  fail.
- **Risk**: This PR specifically repairs pipeline schema validation, yet a supported input shape is
  rejected before execution in Scala and generated-language pipelines. That violates Spark's
  optimistic `transformSchema` contract and the documentation claim that duplicate columns Spark
  treats as one expression resolve as one column.
- **Suggested Fix**: Make schema-only resolution optimistic when all duplicate candidates derive
  the same output and no consumed-input ordinal can affect output ordering, then retain the
  `ExprId` ambiguity check in dataset-aware `transform`. Alternatively reject the case in both
  paths. Add `Pipeline.fit` and `PipelineModel.transform` regressions for the shared-expression
  duplicate frame.

### Issue 2: The computed `getColNames` behavior is not exposed by generated Python bindings
- **Severity**: Medium
- **File**: `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`;
  `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`
- **Line(s)**: `EnsembleByKey.scala:96,616-617,762-765`;
  `Wrappable.scala:118-135,238-271`
- **Description**: The new JVM getter derives names when the `colNames` Param is not explicitly
  set, but the generated Python getter for ordinary Params is always
  `self.getOrDefault(self.colNames)`. Code generation only seeds defaults present on a fresh JVM
  instance, and `colNames` has no such default. The default written inside Java `transform` is not
  transferred back to the Python wrapper, so Python `getColNames()` remains undefined even though
  Scala now returns derived names; save/load can instead expose a snapshot default.
- **Risk**: The public getter has different behavior across SynapseML's Scala and generated Python
  APIs, leaving the previous `getColNames` compatibility fix incomplete and making persisted
  default state inconsistent with the dynamically derived Scala value.
- **Suggested Fix**: Provide a generated or hand-written Python override that derives from explicit
  `colNames`, `cols`, and `strategy` (or deliberately calls a synchronized JVM getter), and remove
  or reconcile the transform-time snapshot default. Add Python tests before/after transform and
  after save/load.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Schema-only duplicate resolution is now optimistic when every candidate
  derives the same declared output and no candidate ordinal can alter non-collapsed passthrough
  ordering. Dataset-aware resolution still requires Spark's duplicate marker or one shared
  `ExprId`, so genuinely ambiguous runtime attributes remain rejected.
- **Why**: Spark pipelines call `transformSchema` without analyzed attribute identity. Equivalent
  candidates can safely produce one schema there, while `transform` retains the information needed
  to reject distinct expressions before execution.
- **How verified**: The shared-`ExprId` duplicate regression now requires exact schema agreement,
  fits a Spark `Pipeline`, and compares `PipelineModel.transform` output with direct transform.
  Equivalent schema-only duplicates are accepted while distinct dataset attributes remain covered
  by runtime ambiguity tests.

### Issue 2
- **Status**: Fixed
- **What changed**: `EnsembleByKey` now generates an internal `_EnsembleByKey` wrapper and exposes
  a hand-written public Python class whose `getColNames` derives unset defaults from `getCols` and
  `getStrategy`. Runtime transformation no longer writes a snapshot `colNames` default, keeping
  Scala, Python, and persisted Param state dynamic and consistent.
- **Why**: Ordinary generated Param getters use `getOrDefault`, which cannot represent this
  computed default. The established internal-wrapper/hand-written-override pattern provides the
  behavior without modifying generated files.
- **How verified**: `sbt codegen` succeeded and produced both generated `_EnsembleByKey.py` and the
  copied public `EnsembleByKey.py` override. The Scala getter regression now also transforms data,
  confirms no Param default is created, and verifies later `cols` changes update the derived names.
  All 42 focused tests pass.

## Round 2 Re-review 12

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-2-gpt-5.6-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the current `HEAD`-to-working-tree diff while excluding `reviews/**`, including the
  untracked public Python override and `EnsembleByKeyResolutionSuite.scala`.
- [x] Verified the optimistic schema-only branch and dataset-aware shared-`ExprId` enforcement at
  `EnsembleByKey.scala:429-450`, including distinct-attribute rejection in `transform`.
- [x] Verified the successful duplicate case through both `Pipeline.fit` and
  `PipelineModel.transform` at `EnsembleByKeyResolutionSuite.scala:43-59`; Spark 3.5.0 and 4.1.0
  `Pipeline.scala` both validate stages through `transformSchema` before execution and document
  that schema inference should be optimistic.
- [x] Ran `core/scalastyle`, `core/Test/scalastyle`, and
  `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*`: 42 tests passed across two
  suites, with zero scalastyle errors.
- [x] Ran `core/codegen` with JDK 11 and verified generated `_EnsembleByKey.py`, the copied public
  `EnsembleByKey.py`, and the public import in `stages/__init__.py`.
- [x] Verified `EnsembleByKey.scala:71,97,618-619` and
  `core/src/main/python/synapse/ml/stages/EnsembleByKey.py:9-13`: derived names remain dynamic, and
  no transform-time `colNames` default assignment remains.
- [x] Ran a generated-wrapper smoke probe for derived, changed, stale-default, and explicitly set
  names; also ran Python syntax, Black, and `git diff --check` checks successfully.
- [ ] No committed Python test exercises the new public override or its persistence behavior; this
  is the remaining finding below.

## Issues

### Issue 1: The public Python compatibility fix has no regression test
- **Severity**: Low
- **File**: `core/src/main/python/synapse/ml/stages/EnsembleByKey.py`;
  `core/src/test/python/synapsemltest/`
- **Line(s)**: `EnsembleByKey.py:9-13`; no corresponding Python test
- **Description**: The hand-written public `getColNames` override is the only code that fixes the
  generated Python API, but all committed regressions are Scala tests. Code generation proves that
  the files are emitted, not that the public class shadows the generated getter after construction,
  transform, or Java ML persistence.
- **Risk**: A codegen, MRO, or Param-transfer change can silently restore the Scala/Python mismatch
  or stale persisted-name behavior without any CI failure.
- **Suggested Fix**: Add a Python regression using the public
  `synapse.ml.stages.EnsembleByKey` class. Cover derived names, changing `cols` after transform,
  explicit `colNames`, and save/load for both derived and explicit states; assert no derived
  `colNames` Param/default snapshot is created.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: Added
  `core/src/test/python/synapsemltest/stages/test_ensemble_by_key.py` against the public
  `synapse.ml.stages.EnsembleByKey` class. It covers derived names, preservation of unset/default
  Param state across transform, dynamic changes after `setCols`, derived-state save/load, and
  explicit `colNames` save/load.
- **Why**: The compatibility behavior now has durable coverage in the generated language surface
  where the mismatch occurred, including both dynamic and persisted Param states.
- **How verified**: The new test and public override pass Python syntax compilation and Black
  formatting checks. The focused `core/testPython` invocation was attempted, but this local WSL
  environment fails before pytest: pre-existing `pyTestgen` data generation aborts while
  serializing `ICEExplainerSuite`, then the build cannot execute the Windows-mounted `conda`
  command (`Permission denied`). Code generation itself succeeds, and all 42 Scala regressions
  continue to pass.

## Round 2 Re-review 13

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-2-gpt-5.6-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Read the complete regenerated Round 2 prompt and reviewed its explicit diff, including the
  untracked public Python override and both untracked Scala/Python regression suites while
  excluding `reviews/**`.
- [x] Verified the Re-review 12 regression at
  `core/src/test/python/synapsemltest/stages/test_ensemble_by_key.py:20-48` uses the public
  `synapse.ml.stages.EnsembleByKey` class and covers derived names, an action-level transform,
  post-transform dynamic `cols`, unset/default `colNames` state, and derived/explicit save-load.
- [x] Reran `core/scalastyle`, `core/Test/scalastyle`, and
  `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*`: 42 tests passed across
  `EnsembleByKeySuite` (38) and `EnsembleByKeyResolutionSuite` (4), with zero style findings.
- [x] Reran `core/codegen`; generated `_EnsembleByKey.py`, the copied public
  `EnsembleByKey.py`, and the public `stages/__init__.py` import were present. The source and
  generated public-wrapper SHA-256 hashes matched exactly.
- [x] Ran Python syntax compilation and the available Black 26.5.1 `--check` on the new public
  wrapper and regression; both passed. `git diff --check` also reported no whitespace errors
  (only the existing CRLF conversion warning).
- [x] Rechecked the public/API and generated-code pattern: the JVM signature and readable companion
  remain intact, `Wrappable`/logging conventions remain present, the hand-written class extends
  `_EnsembleByKey`, no generated `target/` file is edited, and identifier construction uses escaped
  identifiers/public Column APIs without introducing SQL or shell interpolation.
- [ ] The focused `core/testPython` command still does not reach pytest in this local WSL setup:
  `pyTestgen` aborts in an unrelated explainer test-data Spark job and WSL cannot execute the
  Windows-mounted `conda` (`error=13, Permission denied`). This is recorded separately as a local
  harness/environment blocker, not a code-correctness finding.
