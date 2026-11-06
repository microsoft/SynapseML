---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 1
timestamp: "2026-11-03T17:48:45Z"
outcome: "issues_found"
reviewer_model: "gpt-6-astra"
input_sha256: "4553e193de40ce29b60e9d0caa4cfca4c12f568302dc72735497b0ea419b3c2d"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6-astra
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-1-gpt-6-astra.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed all supplied changes, including the three Content Understanding implementation files, Scala/Python suites and fixtures, documentation, sidebar registration, and telemetry changes.
- [x] Traced request preparation and column handling through `ContentUnderstanding.scala`, `CognitiveServiceBase.scala`, `SimpleHTTPTransformer.scala`, and `DatasetExtensions.scala`.
- [x] Checked Python parameter synchronization and generation against `ContentUnderstandingParams.scala`, `core\src\main\python\synapse\ml\core\schema\Utils.py`, and `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`.
- [x] Compared submission classification, management recovery, URL validation, and response bounds with `ContentUnderstandingSuite.scala`, `ContentUnderstandingRecoverySuite.scala`, and `docs\Explore Algorithms\AI Services\Content Understanding.md`.
- [ ] No tests or build commands executed in this read-only review. Passing validation is caller-supplied evidence; live CU tests were not run.

## Issues

### Issue 1: Changing a copied or loaded Python stage's endpoint is silently undone
- **Severity**: High
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala`
- **Line(s)**: 188-207
- **Description**: `_transfer_params_from_java` stores the existing `url` in Python's `_paramMap`. However, the inherited Python `setEndpoint` only updates the JVM object (`CognitiveServiceBase.scala:283-285`). Consequently, `loaded.setEndpoint(newEndpoint)` or `original.copy({}).setEndpoint(newEndpoint)` leaves the previous URL in `_paramMap`. The next transform, management call, or save invokes the superclass parameter transfer and restores that previous URL.
- **Risk**: Requests can send documents and newly configured credentials to the previous resource. Analyzer-management calls can also target the wrong resource despite an explicit endpoint change.
- **Suggested Fix**: Synchronize endpoint setters with the Python `url` parameter using the normalized JVM URL, rather than leaving a stale Python value. Add two-endpoint regression cases covering copy/load followed by endpoint replacement and execution.

### Issue 2: Generated Python instances share supposedly unique default output names
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala`
- **Line(s)**: 155-158
- **Description**: This wrapper inherits generic default generation unchanged. `Wrappable.scala:140-158` renders the code-generation instance's JVM defaults as literal Python values. Thus, every Python `ContentUnderstanding()` receives the same default `outputCol` and `errorCol`; `ComplexParamsMixin._transfer_params_to_java` subsequently overwrites the JVM's genuinely instance-specific defaults with those literals. This contradicts the documented unique defaults.
- **Risk**: Two independently constructed CU stages cannot be chained with default column names: the second encounters the first stage's output/error columns and rejects the pipeline.
- **Suggested Fix**: Initialize these defaults from each runtime JVM instance, and preserve the appropriate defaults during loading. Add a generated-wrapper test asserting distinct defaults for two new stages and successful pipeline composition without explicit output/error names.

### Issue 3: Case-sensitive temporary-column allocation can silently remove input columns
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala`
- **Line(s)**: 288-308, 323
- **Description**: Collision checks and `findUnusedColumnName` use exact string equality, whereas Spark resolves columns case-insensitively by default. An input column named `CONTENTUNDERSTANDINGINPUT` therefore does not reserve `contentUnderstandingInput`: the first `withColumn` overwrites it, and the final `DropColumns` removes it. Case-only collisions between public output/error names also escape validation.
- **Risk**: Original columns and their values can disappear silently, violating the input-preservation contract. Other case-only collisions produce ambiguous-column failures.
- **Suggested Fix**: Use Spark's configured column resolver for collision validation and temporary-name allocation, including temporary columns introduced by the inner HTTP pipeline. Add case-insensitive preservation tests and reject resolver-equivalent public output/error names before dispatch.

### Issue 4: Lazy request preparation reads mutable parameters while the handler uses a frozen configuration
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala`
- **Line(s)**: 218-220, 270-271, 301-315
- **Description**: The validation and input-parser closures capture the mutable transformer and read its parameters during execution. Conversely, `rowHandler(settings, getOperationMode)` captures its configuration during `transform`. For example, constructing a submit DataFrame, changing the same stage to poll mode with a scalar handle, and then collecting the original DataFrame makes its input parser produce a GET while its handler still operates in submit mode.
- **Risk**: Already-constructed DataFrames can analyze the wrong document/analyzer or perform a different operation after subsequent setter calls. Request preparation and protocol handling can disagree within one execution.
- **Suggested Fix**: Capture one consistent parameter snapshot for validation, request preparation, and handling when constructing the transformation. Add a regression that creates a DataFrame, mutates the stage before the action, and verifies that the original plan retains its original request configuration.

### Issue 5: Accepted analyzer-creation handles are discarded when the response body cannot be read
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 538-542, 576-582
- **Description**: `exchange` retains `Operation-Location` even when reading the response produces `ResponseTooLarge`, a transport error, or a deadline failure. However, `createAnalyzer` calls `analyzerBody(raw)` before processing that location. Its exception is constructed with `decode(raw, None)`, discarding the available management handle. An oversized accepted response is additionally represented as `Failed` even though creation may still be running.
- **Risk**: A successful PUT admission becomes an unresumable-looking failure, contrary to the exception's handle-retention contract. Callers lose the operation evidence needed to distinguish ongoing provisioning from a failed creation.
- **Suggested Fix**: Validate and retain the management location before rejecting an unreadable body. Preserve an uncertain/nonterminal outcome and either poll within the remaining budget or return the validated handle in the exception. Add accepted-PUT cases with oversized and interrupted bodies, without replaying the PUT.

### Issue 6: An invalid operation handle can be mislabeled as a retry-safe rejection
- **Severity**: Medium
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`
- **Line(s)**: 416-442
- **Description**: An unsafe or malformed `Operation-Location` is discarded before `decodeSubmission` checks whether a handle was returned. For HTTP 429, this produces `Rejected`; the subsequent location-error branch preserves that status because the HTTP response was unsuccessful. The connector therefore treats “a handle was returned but could not be trusted” as “no operation handle was returned.”
- **Risk**: The documented retry recipe selects `Rejected` rows as safe to resubmit. An ambiguous response carrying a rejected handle can consequently cause duplicate accepted work and charges.
- **Suggested Fix**: Distinguish an absent header from an invalid header. An invalid returned operation location must not produce the retry-safe `Rejected` state; retain `Unknown` with `InvalidOperationLocation` and do not follow the URL. Add a 429-with-invalid-location regression.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: `setEndpoint` now synchronizes the normalized JVM endpoint into Python's `url` parameter map.
- **Why**: Prevent stale Python state from silently overriding an explicitly changed endpoint after copy or load.
- **How verified**: Added copy/load endpoint-replacement regressions in `test_ContentUnderstanding.py`; offline CU Python tests passed (7/7), as did Scala compile and CU suites.

### Issue 2
- **Status**: Fixed
- **What changed**: Python output/error defaults now come from the runtime JVM instance rather than code-generation-time literals.
- **Why**: Preserve per-stage unique names so default-configured Python stages can be composed.
- **How verified**: Added distinct-default and pipeline-composition regressions in `test_ContentUnderstanding.py`; offline CU Python tests passed (7/7), as did Scala compile and CU suites.

### Issue 3
- **Status**: Fixed
- **What changed**: Public collision validation and temporary-column allocation now use Spark's configured resolver.
- **Why**: Match Spark's case-insensitive name resolution and preserve inputs with case-only name collisions.
- **How verified**: Added case-insensitive allocation and preservation coverage; core schema tests passed (7/7) and CU Scala suites passed (50/50).

### Issue 4
- **Status**: Fixed
- **What changed**: The internal transformation plan now captures a parameter snapshot.
- **Why**: Keep request preparation and row handling consistent when the mutable estimator is changed after `transform`.
- **How verified**: Added a mutate-after-plan regression; the CU Scala suites passed (50/50).

### Issue 5
- **Status**: Fixed
- **What changed**: Analyzer creation validates and retains an accepted operation handle before reporting an unreadable response body.
- **Why**: Avoid losing recovery evidence or treating an accepted asynchronous PUT as a definitive failure.
- **How verified**: Added oversized accepted-response coverage; the CU Scala suites passed (50/50).

### Issue 6
- **Status**: Fixed
- **What changed**: Invalid returned operation locations now produce `Unknown` instead of retry-safe `Rejected`.
- **Why**: Do not invite duplicate submissions when the service returned a handle that cannot be trusted.
- **How verified**: Added a 429-with-invalid-location regression; the CU Scala suites passed (50/50).