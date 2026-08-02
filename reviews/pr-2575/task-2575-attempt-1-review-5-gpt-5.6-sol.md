## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-5-gpt-5.6-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the complete Round 5 prompt and reviewed the explicit working-tree diff plus all untracked source/test files; `git status --short --untracked-files=all` identified the Scala implementation/doc, the main Scala suite, the new resolution suite, and the new public Python wrapper/package/test.
- [x] Ran `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*` through the JDK 11 SynapseML wrapper: exit 0; the refreshed XML reports contain 38 passing `EnsembleByKeySuite` tests and 7 passing `EnsembleByKeyResolutionSuite` tests, with 0 failures/errors/skips.
- [x] Traced every schema-producing path in `EnsembleByKey.scala:609-792` against the Scala assertions, including collapse modes, scalar/vector nullability, output overwrite ordering, duplicate attributes, qualifiers, nested struct/array/map extraction, null keys, and `spark.sql.retainGroupColumns`.
- [x] Verified the no-active-session assertion at `EnsembleByKeySuite.scala:251-264` deliberately expects different schemas, while `EnsembleByKey.scala:621-622` chooses the dataset session only for `transform`; Spark 3.5 and 4.1 `Pipeline` both invoke `transformSchema(dataset.schema)` before stage transforms.
- [x] Checked generated-language coverage: `core/codegen` output contains public `EnsembleByKey.py`, internal `_EnsembleByKey.py`, re-export from `stages/__init__.py`, and `_from_java` routing to the public class; both new Python files parse successfully with `ast.parse`.
- [x] Checked Spark 3.5/4.1 source compatibility for the new Catalyst calls: `Dataset.queryExecution`, `Cast.canCast(DataType, DataType)`, and `RowOrdering.isOrderable(DataType)` exist in both reviewed Spark versions; the public JVM method signatures remain unchanged, and user references are parsed/literal-bound rather than passed to SQL-expression string APIs.

## Issues

### Issue 1: The no-active-session test codifies a schema/runtime contract violation
- **Severity**: Medium
- **File**: `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala`; `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: `EnsembleByKeySuite.scala:251-264`; `EnsembleByKey.scala:621-622`
- **Description**: With a case-sensitive input session and no active session, `transformSchema` falls back to case-insensitive resolution, while `transform` reads `dataset.sparkSession`. The test explicitly asserts the resulting disagreement: schema-only output is `key,id,score,features`, but runtime output is `key,id,score,FEATURES,features`. This is not merely an untested edge case; the assertion treats a known violation of the transformer's declared-schema contract as expected behavior.
- **Risk**: Spark 3.5 and 4.1 `Pipeline.fit`/`PipelineModel.transform` call `transformSchema(dataset.schema)` before executing stages. On a thread without the matching active session, a downstream stage can be rejected because the declared schema removed a case-distinct column that runtime would preserve, or can be validated against a shape runtime will not produce.
- **Suggested Fix**: Make case-resolution policy available to both schema-only and dataset-aware paths (for example, an explicit/persisted resolution setting, or another design that does not infer different policies). Change this regression to require schema equality and add a two-stage pipeline test under `withoutActiveSession`. If the asymmetry is intentionally unavoidable, document it beside the qualifier limitation and test the exact pipeline failure mode rather than presenting the two schemas as equivalent coverage.

### Issue 2: Invalid-configuration coverage misses explicit empty arrays and checks only `transformSchema`
- **Severity**: Low
- **File**: `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala`; `core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala`
- **Line(s)**: `EnsembleByKeySuite.scala:621-639`; `EnsembleByKey.scala:609-614`
- **Description**: The implementation has separate branches for an unset Param (`get(...).getOrElse`) and a set-but-empty array (`require(...nonEmpty)`), but the invalid-configuration table covers only the unset cases. It also invokes only `transformSchema`, despite the PR's schema/runtime-consistency objective and the existing `assertConsistentSchemaError` helper. Public Scala setters and the generated Python constructor can both supply `keys=[]` or `cols=[]`.
- **Risk**: The explicit-empty branches can regress into later `.head`/aggregation failures or produce a different exception from runtime without any focused test detecting it. Current tests would still pass if only the unset-Param path remained correct.
- **Suggested Fix**: Add `setKeys(Array.empty[String])` and `setCols(Array.empty[String])` cases with the opposite required Param populated, and run every invalid configuration through `assertConsistentSchemaError`. Add the corresponding Python empty-list check if Python validation behavior is part of the public contract.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed by documenting and testing the unavoidable limitation
- **What changed**: `EnsembleByKey.txt` now explains that schema-only resolution uses the active
  session while runtime uses the dataset session, and instructs callers to keep the dataset session
  active during pipeline construction/validation. The regression is renamed as a limitation test
  and now asserts the exact two-stage `Pipeline.fit` failure caused by the divergent schema.
- **Why**: A `StructType` contains neither a SparkSession nor its case-sensitivity policy, so
  `transformSchema(schema)` cannot recover the dataset session. Runtime must still honor the
  dataset's analyzer semantics; silently reverting to an unrelated/default active policy would
  make direct transformation disagree with Spark column resolution.
- **How verified**: Under no active session, the test asserts both declared/runtime shapes and
  proves a downstream `VectorAssembler` requiring the runtime-preserved `FEATURES` column is
  rejected during pipeline validation. Normal same-session paths continue to require exact schema
  equality.

### Issue 2
- **Status**: Fixed
- **What changed**: Added explicit empty `keys` and empty `cols` configurations to the invalid
  table and switched every table entry to `assertConsistentSchemaError`, covering both
  `transformSchema` and `transform`. The public Python regression also checks `keys=[]` and
  `cols=[]`.
- **Why**: Set-but-empty Params now have durable coverage distinct from unset Params on both JVM
  execution paths and the generated Python surface.
- **How verified**: All 45 focused Scala tests and both scalastyle checks pass. The Python files
  pass syntax compilation and Black formatting.

## Round 5 Re-review 1

## Review Summary
- **Round**: 5 (re-review 1)
- **Theme**: Testing & coverage
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-5-gpt-5.6-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the complete 1,872-line Round 5 prompt and the current tracked/untracked diff, including
  the 795-line Scala implementation, both Scala suites, documentation, public Python override,
  Python regression, and regenerated Python bindings.
- [x] Verified explicit empty arrays at `EnsembleByKeySuite.scala:627-644`: empty `keys` and empty
  `cols` both use `assertConsistentSchemaError`, which invokes `transformSchema` and `transform`.
  Verified the public Python regression at `test_ensemble_by_key.py:20-23` covers `keys=[]` and
  `cols=[]` through `transform`.
- [x] Verified the case-policy limitation is documented at `EnsembleByKey.txt:21-25`, and normal
  active-session case-sensitive and case-insensitive paths require exact schema equality at
  `EnsembleByKeySuite.scala:126-142`.
- [x] Ran `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*` with JDK 11:
  38 `EnsembleByKeySuite` tests plus 7 `EnsembleByKeyResolutionSuite` tests passed with zero
  failures, errors, or skips. Repository `scalastyle` and `test:scalastyle` also passed.
- [x] Ran `core/codegen` and verified generated `_EnsembleByKey.py`, the copied public
  `EnsembleByKey.py`, package re-export, and `_from_java` routing. Generated/source Python parsed
  successfully and the two changed Python files passed `black --check`.
- [x] Reviewed current Spark 3.5 schema/runtime paths, public JVM/Python compatibility, identifier
  parsing/literal map-key binding, serialization, and security. No additional actionable gap was
  found beyond the downstream-pipeline regression below.
- [ ] The Python pytest itself was not reached locally because `core/pyTestgen` aborted while
  generating unrelated `ICEExplainerSuite` fuzz data; this review independently verified the
  hand-written test and generated binding path instead.

## Issues

### Issue 1: The downstream pipeline test does not isolate the documented case-policy failure
- **Severity**: Low
- **File**: `core/src/test/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKeySuite.scala`
- **Line(s)**: 251-269
- **Description**: The no-active-session regression feeds `VectorAssembler` the preserved
  `FEATURES` column, but that column is `StringType`. Its assertion only requires the exception
  message to contain `FEATURES`. A standalone Spark 3.5 probe against this build produced
  `FEATURES does not exist...` with no active session, but with the matching case-sensitive
  session active the same pipeline still failed with
  `Data type string of column FEATURES is not supported.` Both failures satisfy the current
  assertion, so the test does not prove that the downstream pipeline is otherwise valid or guard
  the exact missing-column failure caused by schema/runtime divergence.
- **Risk**: The direct field-name assertions still protect the core schema mismatch, but the new
  `Pipeline.fit` regression can remain green when the downstream failure is caused by an unrelated
  unsupported input type. That weakens the intended regression for the documented pipeline
  limitation.
- **Suggested Fix**: Make the case-variant passthrough columns numeric (or another
  `VectorAssembler`-supported type), assert the missing-column diagnostic such as
  `FEATURES does not exist`, and preferably assert that the same two-stage pipeline fits when the
  dataset session is active.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: The case-variant passthrough columns are now numeric, so `VectorAssembler` can
  consume `FEATURES` when it is present. The failure assertion requires the exact
  `FEATURES does not exist` diagnostic, and the same two-stage pipeline is required to fit once the
  matching case-sensitive session is active.
- **Why**: The regression now isolates the documented schema/runtime case-policy mismatch rather
  than allowing an unrelated unsupported-string-type failure to satisfy the assertion.
- **How verified**: Both focused suites pass (45 tests), and test scalastyle remains clean.

## Round 5 Re-review 2

## Review Summary
- **Round**: 5 (re-review 2)
- **Theme**: Testing & coverage
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: /c/Users/singhrana/Documents/SynapseML-pr-2575/reviews/pr-2575/task-2575-attempt-1-review-5-gpt-5.6-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Read the complete 1,872-line DIRECT prompt and re-reviewed the current tracked and untracked
  Scala, Python, generated-language, documentation, schema/runtime, compatibility, and security
  paths. `git diff --check b4a5983c86` reported no whitespace errors.
- [x] Verified the strengthened regression at `EnsembleByKeySuite.scala:251-270`: both case-variant
  passthrough columns are `DoubleType` inputs (`2.0`, `3.0`), the no-active-session branch requires
  `FEATURES does not exist`, and after `withoutActiveSession` restores the matching case-sensitive
  session the identical two-stage pipeline must fit successfully.
- [x] Ran `core/testOnly com.microsoft.azure.synapse.ml.stages.EnsembleByKey*`, `scalastyle`, and
  `test:scalastyle` through the JDK 11 wrapper. The refreshed reports contain 38 passing
  `EnsembleByKeySuite` tests and 7 passing `EnsembleByKeyResolutionSuite` tests, with zero
  failures/errors/skips; both scalastyle result files contain no violations.
- [x] Ran `core/codegen` successfully and verified generated public/internal Python wrappers,
  package re-export, `_from_java` routing to the public class, and the generated R wrapper. Source
  and generated Python files parse successfully, and both changed source Python files pass
  `black==22.3.0`.
- [x] Re-traced case policy, duplicate/qualified/nested/map resolution, collapse modes, null keys,
  output overwrite ordering, scalar/vector schemas, invalid configurations, and pipeline
  validation. Spark 3.5 compiled and executed the focused suites; Spark 4.1 source retains the
  Catalyst APIs used here (`queryExecution`, `parseAttributeName`, `Cast.canCast`, and
  `RowOrdering.isOrderable`).
- [x] Rechecked compatibility and security: existing public JVM signatures and readable companion
  behavior are preserved; user references are parsed then bound by ordinal, nested names use
  `getField`, map keys use typed literals, internal identifiers are backtick-escaped, and no new
  deserialization, I/O, network, credential, SQL-string, or shell-execution path is introduced.
- [ ] The Python pytest was not executed locally because no configured `synapseml` conda
  environment or PySpark installation is available; code generation, routing, AST parsing, pinned
  Black, and the hand-written regression were independently verified.
