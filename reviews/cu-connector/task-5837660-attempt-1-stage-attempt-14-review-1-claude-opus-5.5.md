---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 14
round: 1
timestamp: "2026-11-04T22:11:42Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ee04a0c997da78f0baae46aa4f98de742149a593466dad3f91955c7bf470017e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-14-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Submission recovery** (`cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`):
  - 92–99 and 527–575: each submission is one `exchange` with no POST replay. Unsent requests become `Rejected` (`RequestNotSent` when the budget ran out). 401/403/429 without a location become `Rejected`. Transport failures, 408, 5xx, unreadable 2xx responses and unsafe or missing Operation-Location become `Unknown`.
  - 514–525: `checkSharedFailure` skips `Unknown` responses and accepted handles. A 404 trips the stop only when `sharedAnalyzer` is set and the status is definitive.
- [x] **Partition stop** (`rowHandler`, Protocol 670–690): the `AtomicBoolean` is per task closure. A stopped partition returns `Rejected` with `SharedConfigurationFailure` for non-poll rows, and poll is never stopped. Polling (601–634) passes `acceptedHandle = true` and keeps previous state on retryable or non-terminal errors.
- [x] **Credential classification**: `rowUsesColumnAuth` (ContentUnderstanding.scala 147) and `FailureClassifier` (Protocol 481–512) follow the resolve precedence, compare SHA-256 header fingerprints, and use the row-auth marker. The marker is stripped from user headers in `addHeaders` (CognitiveServiceBase.scala 549–563), and its value is not `"true"`, so it cannot trigger Fabric auth.
- [x] **Transport** (Protocol 372–430):
  - Timeouts are capped at the remaining budget.
  - The deadline abort and the 4-thread daemon executor are best-effort.
  - The not-sent decision uses `hasStartedWriting` (`ContentUnderstandingTransport.scala` 19–27).
  - Clients are created per operation, with retries and redirects disabled.
- [x] **DNS** (`ContentUnderstandingDnsResolver.scala`): permits come from a fair semaphore acquired within the remaining budget. They are released in the worker's `finally` or on `RejectedExecution`, and are never released when the caller times out, which matches the spec.
- [x] **URL and header safety**:
  - Protocol 200–278: `parseUri` wraps `URISyntaxException`. HTTP is allowed only for literal loopback. Operation-Location must be same-origin, a single safe path segment, and carry exactly one `api-version`.
  - `ContentUnderstandingHeaderValidation.scala` 16–22: error messages contain only parameter names, never values.
  - ContentUnderstanding.scala 77–85: auth params are included in `activeParams`, so they are validated on the driver (scalar) or per row (column).
- [x] **Session and resolver handling** (ContentUnderstanding.scala): `transform` (59) uses the Dataset resolver and schema-only paths use `SQLConf.get`. `buildInternalTransformer` (353–398) checks output collisions and generates case-insensitively unique internal names. `rowError` (276) turns invalid column values into row-level `InvalidInput`. Management (401–440) is driver-only and scalar-only, never retries the PUT, and requires `ready`.
- [x] **Shared changes in `CognitiveServiceBase.scala`**: `addContentType` dedup (358–361, 431). `addHeaders` defaults `addContentType = true` (551), so other services keep their behavior. New `HasCustomHeaders` getter and column setter at 226–230.
- [x] **Python and codegen**:
  - `Wrappable.scala` 83–89, 155–164, 182–213, 290–295 and 559. All `pyAdditionalMethods` overrides chain to `super`.
  - `Utils.py` 62–214.
  - CU mixin in `ContentUnderstandingParams.scala` 155–270: skips `None`, keeps bytes as bytes instead of passing them to `_java2py`, and redacts auth in `explainParam`.
  - `TextAnalytics.scala` 52 and 137–141 (JVM column default).
  - `test_ContentUnderstanding.py` 339–372 and 470–491.
- [x] **Docs and CI**: `docs\Explore Algorithms\AI Services\Content Understanding.md` 96–110; `docs\Use with MLFlow\Autologging.md` 9–18; `environment.yml` 36 (`mlflow==2.21.3`); `pipeline.yaml` 821–822 (approved entry); `ContentUnderstandingConcurrencySuite.scala` 277–326.
- [ ] No tests, builds or commands were run (read-only assignment). The validation evidence in the prompt is taken as reported, not reproduced.
- [ ] MLflow source is not in the repository. The `_get_param_map` idiom (`{p.name: inst.getOrDefault(p) for p in inst.params if inst.isDefined(p)}`) and safe_patch's warn-and-continue behavior come from my knowledge of MLflow 2.x, not from verification here.
- [ ] No external CI or live Content Understanding evidence exists. Per the plan, this is not a defect in itself.

## Issues

### Issue 1: JVM-backed `isSet`/`hasDefault` breaks PySpark's `isDefined ⇒ getOrDefault` contract in every generated cognitive wrapper
- **Severity**: Medium
- **File**: `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`; `core\src\main\python\synapse\ml\core\schema\Utils.py`
- **Line(s)**:
  - `Wrappable.scala` 83–89 (`isSet` override, emitted for every wrapper at 559); 157–158 (no Python defaults for ServiceParams); 205–206 and 211–212 (setters pop `_paramMap`).
  - `Utils.py` 62–68 (`hasDefault` falls back to the JVM); 94–102 (`getOrDefault` raises `KeyError` at 100); 104–119 (`extractParamMap`).
  - Related: `cognitive\...\services\text\TextAnalytics.scala` 52 and 137–141; CU doc 107–110; `test_ContentUnderstanding.py` 362–365 and 489–491.
- **Description**:
  - **Contract break.** Python now answers `isSet`/`hasDefault` for ServiceParams from the JVM, but `getOrDefault` raises `KeyError` when the JVM value is a column binding. So `isDefined(p)` is true while `getOrDefault(p)` raises. In PySpark, `getOrDefault` raises only when a param is neither set nor defaulted. The tests (362–365 and 489–491) and the CU doc (108–110) build this behavior in.
  - **Not limited to CU.** `TextAnalyticsInputParams` declares `setDefault(text -> Right("text"))` (`TextAnalytics.scala` 52). TextSentiment, KeyPhraseExtractor, NER, PII, LanguageDetector, EntityDetector, AnalyzeHealthText and TextAnalyze all inherit it. A freshly built `TextSentiment()` now reports `text` as defined but raises on `getOrDefault`, with no user action.
  - **Why this is new.** Codegen emits no Python defaults for ServiceParams, and setters never filled `_paramMap`. Before this change, generic enumeration couldn't see these params at all.
  - **Credentials become enumerable.** The same change exposes scalar credentials held on the JVM (`subscriptionKey`, `AADToken`, `CustomAuthHeader`, `customHeaders`) to generic enumerators. `explainParam` redaction does not cover that path.
- **Risk**:
  - **Lost autolog runs.** `Autologging.md` says SynapseML supports `mlflow.pyspark.ml.autolog()` for every model. Autolog gathers each Pipeline stage's params using the `isDefined`/`getOrDefault` idiom. Any `Pipeline.fit` that contains a TextAnalytics stage, or any column-bound cognitive/CU stage, now hits `KeyError` during param collection. MLflow normally downgrades this to a warning, so the fit still succeeds, but the run's params and model logging are lost (MLflow test mode raises instead).
  - **Crashing user code.** User code that uses the same idiom now crashes.
  - **Credentials in MLflow.** When collection succeeds, scalar credentials are written as immutable MLflow run params that anyone with experiment access can read. This is a broader, implicit exposure than the accepted explicit-getter/map policy, and it is undocumented.
- **Suggested Fix**:
  - **Consistency.** Count a JVM ServiceParam as set or defaulted only when its value `isLeft`. This matches `extractParamMap`, which already omits column bindings. Keep column inspection available through `explainParam`, the `get*Col` getters, and JVM copy/save/load. Update CU doc 107–110 and tests 362–368.
  - **Credentials.** Make an explicit policy decision. Either exclude auth ServiceParams from the JVM fallback (explicit getters keep returning values), or document the autologging exposure in the CU and Autologging docs.
  - **Test.** Add a Python test that runs the enumeration idiom over a default `TextSentiment()`, a column-bound CU stage, and a scalar-credential stage.

### Issue 2: Input bindings are validated by exact name but selected with Spark's resolver
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala`
- **Line(s)**: 236 (`schema.fields.find(_.name == name)`); 368 (`col(snapshot.quoted(name)).alias(name)`); 375 (`withColumn(inputColumn, struct(inputs: _*))`). Test gap: `ContentUnderstandingConcurrencySuite.scala` 310–326.
- **Description**:
  - Validation accepts a binding if a field with exactly that name exists. Execution then selects the column by name, and Spark resolves that name with the session resolver.
  - This breaks when the input has case-variant duplicates (for example `doc` and `DOC` after a join in a case-insensitive session) or exact duplicate names. Validation passes, then the first Lambda throws `AnalysisException [AMBIGUOUS_REFERENCE]`.
  - The existing test only uses a single-column frame.
- **Risk**: The failure happens before any request is sent, so nothing is submitted. But the documented "exact top-level field names" contract is not what actually runs, and users get a Spark analysis error instead of the connector's `IllegalArgumentException`.
- **Suggested Fix**: Pass the resolver into `validateParamSchema` and require exactly one exact match and no other field matching under the resolver, with a clear "ambiguous" error. Alternatively, bind columns by position. Add tests for `doc`/`DOC` in a case-insensitive session and for exact duplicate names.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting the driving agent's decision on Python scalar-view consistency and credential enumeration policy
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: awaiting the driving agent
- **How verified**: pending

## Driver resolution update: attempt 14

The original review and verdict are retained. Collection confirmed unchanged
source fingerprints, patch hash, HEAD, and path set. All 156 Opus tool calls
were read/search calls, with no prior/sibling feedback queries.

- **Issue 1 - implemented, awaiting final validation:** generated `isSet` and
  shared Python `hasDefault` expose a consistent scalar view. Effective column
  bindings are omitted from `isSet`/`hasDefault`/`isDefined`/`extractParamMap`,
  including bindings overriding a scalar default. Explicit scalar access to a
  column binding still raises `KeyError`; JVM copy/save/load and `explainParam`
  preserve binding access. Actual Python pending scalar values retain
  precedence, and parameter transfer inspects real Python map membership.
- **Regression evidence:** `cu-attempt14-python-red.log` reproduced failures
  using the `isDefined`/`getOrDefault` enumeration idiom on default
  `TextSentiment`, column-bound CU, and credential-bearing `AnalyzeDocument`.
  Regressions also cover default column -> scalar -> clear, scalar default ->
  column -> clear, copy/load, map conversion, raw media, and binding
  load/copy/resave.
- **Credential policy:** explicit scalar getters/maps and normal SynapseML
  persistence remain available. CU and Autologging documentation now explain
  that MLflow/generic enumeration can log scalar credentials, headers, and
  signed URLs, and that description redaction or `log_models=False` does not
  prevent parameter logging. Pinned MLflow v2.21.3 canonical source was
  independently inspected: `_get_param_map` at 327-332, recursive stage
  enumeration at 348-391, and pretraining collection/logging at 989/1016.
  An actual MLflow autologging runtime was not executed.
- **Issue 2 - implemented, awaiting final validation:** CU keeps exact-name
  input binding but additionally requires uniqueness under the Dataset's
  resolver. Duplicate exact names and case-variant ambiguity now fail clearly
  before HTTP dispatch. The red run reproduced the prior Spark analysis
  exception, and the intermediate run passed the connector-validation test.
  The shared schema/session matrix and further negative cases are described in
  the companion GPT resolution update.

Final Scala/codegen/publication and generated-wrapper Python validation were
pending when this update was written. These changes do not constitute an
independent clean re-review.

### Completed local validation

Both findings are locally resolved, pending independent re-review. Core and
cognitive main/test style and affected compilation passed, as did 1/1
generated-wrapper and 95/95 CU Scala tests. Codegen/publication passed.
`cu-attempt14-python-fixed3.log` records 20/20 offline Python tests, with actual
loaded JAR hashes matching build/publication and generated Utils.py identical
to source. Full Black and the subsequently edited test's Black check passed.

Two new test-fixture assumptions were corrected without broadening production
changes: TextAnalytics scalar text retains its existing Scala-sequence getter
representation (verified against HEAD Wrappable.scala), and TextSentiment model
loading is blocked by its pre-existing legacy-UDF deserialization protection.
That default-column transition regression therefore covers fresh/copy behavior;
CU/AnalyzeDocument regressions cover load/copy/resave. No unsafe-deserialization
setting was enabled. The two intermediate failing Python logs are retained.
MLflow's enumeration idiom was exercised, not an actual MLflow autologging run.