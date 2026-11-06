---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 5
round: 1
timestamp: "2026-11-04T00:49:10Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "e7287e6a39d27de39ba0f73b478ff7b9c400deee1ab2d67272309f274d0d4f02"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: direct
attempt: 1
stage_attempt: 5
round: 1
timestamp: 2026-11-04T18:00:00Z
outcome: issues_found
reviewer_model: grok-4.7
input_sha256: 4830C0C528F04855528AFEF1209FCD702B300624D87427888D8502F728AC547E
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-5-review-1-grok-4.7.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala` (`uncertainSubmission` 504-509, `checkSharedFailure` 491-501, `execute` 608-624, `rowHandler` 635-636, `createAnalyzer` 672-715) and `ContentUnderstandingException` at 40-45.
- [x] Read `ContentUnderstanding.scala` internal pipeline and `ContentUnderstandingOutputParser` (`pyInternalWrapper`/`rInternalWrapper` at 398-399). Compared provisioning tests in `ContentUnderstandingSuite.scala` (explicit 201/202 location, 400 without location, unsafe location).
- [x] Read `ContentUnderstandingParams.scala` generated `clear` (246-249) and `_transfer_params_from_java` (210-224); `Wrappable.scala` ServiceParam setter pop (202, 208); Python test `loaded.clear` at `test_ContentUnderstanding.py:322`. Pinned runtime is `pyspark==3.5.0` in `environment.yml`.
- [x] Read `Fuzzing.scala` `makePyTestFile` import (193), `rTestInstantiateModel` public `ml_` name (272), and `makeRTestFile` source path (378). Confirmed underscore modules are omitted from package exports in `PyCodegen.scala` and internal names in `Wrappable.scala` (85-89, 626-631).
- [x] Read failure-classification and task-retry wording in `docs/Explore Algorithms/AI Services/Content Understanding.md` (about 188-189 and 241-265). Caller-supplied pass counts were not treated as proof.
- [ ] Tests were not executed in this review.

## Issues

### Issue 1: Non-success responses with Operation-Location are treated as successful operations
- **Severity**: High
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 504-509, 616-618, 672-691
- **Description**: `createAnalyzer` follows any same-origin `Operation-Location` without checking the PUT status. A 400/401/409/500 that still carries a management operation URL is polled. If that operation is already `Succeeded`, the method GETs the analyzer and returns it as success, hiding the failed create or replace. The same pattern exists for row analysis: `uncertainSubmission` marks an unsuccessful response with a location as `Unknown`, and analyze mode then polls it. A 4xx whose location points at a different already-succeeded operation can become `Succeeded` for this row. Tests cover 201/202 with a location, 400 without a location, and unsafe locations. They do not cover a non-success status plus a valid location.
- **Risk**: Provisioning can report success when the service rejected the definition. Callers may analyze with a stale analyzer. Analyze mode can attach another operation's result to the row and hide the submission failure.
- **Suggested Fix**: Follow `Operation-Location` only for HTTP 200, 201, or 202. For every other PUT/POST status, throw `ContentUnderstandingException` with that response and do not request the location. Add tests for 400 and 409 with a same-origin succeeded operation URL; both must fail without a follow-up GET.

### Issue 2: Shared failures thrown from executors are retried as new submissions
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 40-45, 491-501, 635-636
- **Description**: Shared 401/403 and shared `AnalyzerNotFound` / deployment failures are raised as `ContentUnderstandingException` inside the row handler. That aborts the Spark task before the output row, including any accepted operation location, is produced. Spark retries ordinary task failures up to `spark.task.maxFailures` (default 4). Each retry runs analyze/submit again, so a deterministic configuration failure repeats the POST. The connector does not itself replay POSTs, but this exception is the fail-the-action signal, and the docs' poll-resume path cannot be used because the handle was discarded. The documented warning about task retries does not make this deterministic path safe.
- **Risk**: A bad analyzer, deployment, or shared credential multiplies billable submissions per partition and can create operations the user can no longer address.
- **Suggested Fix**: Do not throw from the executor for a classified shared failure. Return the completed response in the row and record the failure in an accumulator, then fail the action on the driver after the successful tasks finish. If a handle was already accepted, keep it in that row so the user can poll instead of resubmitting.

### Issue 3: Python clear() raises KeyError after ServiceParam values are popped
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala
- **Line(s)**: 210-224, 246-249
- **Description**: Generated ServiceParam setters in `Wrappable.scala` (202, 208) pop the param from `_paramMap` after updating the JVM. CU's `_transfer_params_from_java` also pops every `ServiceParam:` entry on load. `clear` then clears the JVM param and calls `super().clear(param)`. PySpark 3.5 `Params.clear` raises `KeyError` when the param is not in `_paramMap`. After `setDocumentBytes`, constructor transfer, or `ContentUnderstanding.load`, `clear(documentBytes)` therefore fails. The JVM value is already cleared before the exception, so a caught `KeyError` leaves Python and JVM disagreeing about whether a later `super().clear` is still required. `test_ContentUnderstanding.py:322` calls `loaded.clear(loaded.documentBytes)` on this path. This review did not execute that test; the caller reported it passed, which conflicts with the PySpark 3.5 contract and should be re-run against the generated wrapper.
- **Risk**: The public `clear` API fails after the documented save/load and setter paths. Callers cannot switch a scalar document to a column, which is the sequence the Python test expects. The same pop makes `Params.clear` raise for other generated ServiceParam setters once a value has been popped.
- **Suggested Fix**: Clear the JVM param, then call `super().clear(param)` only when `self.isSet(param)`. Add a generated-wrapper test that loads a scalar `documentBytes` value and clears it without `KeyError` before `setDocumentBytesCol`.

### Issue 4: Internal parser fuzzing suite generates Python and R tests that cannot import it
- **Severity**: Medium
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingOutputParserFuzzingSuite.scala
- **Line(s)**: 9-11
- **Description**: The parser sets `pyInternalWrapper` and `rInternalWrapper` (`ContentUnderstanding.scala` 398-399), so codegen emits `_ContentUnderstandingOutputParser` and `_ml_content_understanding_output_parser`, and `PyCodegen.scala` does not export underscore modules. The suite extends `TransformerFuzzing`, which is a `PyTestFuzzing` and `RTestFuzzing` service. `PyTestGen` will emit `from synapse.ml.services.contentunderstanding import ContentUnderstandingOutputParser` (`Fuzzing.scala:193`). R generation calls `ml_content_understanding_output_parser` (`Fuzzing.scala:272`) and sources `ml_content_understanding_output_parser.R` (`Fuzzing.scala:378`), not the underscore wrapper. Scala `FuzzingTest` does not exercise those generated imports.
- **Risk**: Python and R codegen test generation for this suite fails at import or `source`, breaking the language-test gate added with this connector.
- **Suggested Fix**: Do not register an internal-only parser with `TransformerFuzzing`. Keep a Scala-only parser test, or override Python and R generation to no-op. Do not export the parser just to satisfy the generator.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed.
- **What changed**: Analysis and analyzer-management operations follow `Operation-Location` only for accepted 200/201/202 responses; ambiguous non-success responses are not polled.
- **Why**: A failed submission must not be converted into a successful operation result.
- **How verified**: Regression tests cover 400/409 responses with operation locations and management PUT responses; all 70 CU Scala tests passed.

### Issue 2
- **Status**: Fixed according to the user's selected policy.
- **What changed**: Shared failures before acceptance fail fast. After a valid operation handle is accepted, failures return a row result retaining that handle.
- **Why**: Preserve fail-fast behavior before acceptance while preventing accepted operations from being lost or resubmitted.
- **How verified**: `ContentUnderstandingRecoverySuite` covers pre-acceptance failure and post-acceptance handle retention; all 70 CU Scala tests passed.

### Issue 3
- **Status**: Not reproduced.
- **What changed**: Added a regression assertion that clears a scalar value immediately after `setDocumentBytes`; retained the existing clear-after-load coverage.
- **Why**: The reported `clear()` failure does not occur for either the setter or load path with the repository's pinned PySpark 3.5.0 runtime.
- **How verified**: All 11 offline Content Understanding Python tests passed, including immediate clear-after-setter and clear-after-load.

### Issue 4
- **Status**: Fixed; full test-generation verification is environment-blocked.
- **What changed**: The internal parser fuzzing suite now skips Python and R test generation, and its R wrapper generation is disabled.
- **Why**: Generic language test generation assumes public wrappers and names that the internal parser intentionally does not expose.
- **How verified**: Python and R wrapper generation passed; the generated parser wrapper is internal and absent from the public Python package and R output. Full Python/R test generation stopped in an unrelated AIFoundry suite because the `az` executable is unavailable.