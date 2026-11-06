---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 5
timestamp: "2026-11-05T20:59:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ae4990672ff3fb07e87bbb9abe9e444c119d33aed3cb60cfc18e5b9a7128f60e"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-5-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the whole supplied diff, covering Scala main code, the six CU suites, the stub and fixtures, core `SimpleHTTPTransformer`/`DatasetExtensions`/`Wrappable`, Python `Utils.py`, and the CU Python tests and fixtures.
- [x] Checked that poll mode does not require document columns. `ContentUnderstanding.scala` lines 83-96 filter `getVectorParamMap` through `activeParams(poll)`, so there is no gap.
- [x] Checked the Python end-to-end helper. `content_understanding_fixtures.py` line 17 defines `synthetic_pdf(page_count=4)`, so the call `synthetic_pdf(1)` is valid.
- [x] Checked that the new default `isSet` stub in `Wrappable` reaches every wrapper. Every `pyAdditionalMethods` override calls `super` (`CognitiveServiceBase.scala` 284/636/679, `ContentUnderstandingParams.scala` 214, `OpenAIPrompt.scala` 168, `AzureMapsTraits.scala` 23).
- [x] Checked that the shared Content-Type de-duplication is tested. `ContentUnderstandingRecoverySuite.scala` lines 593-611 test the casings `Content-Type`, `content-type` and `CONTENT-TYPE`.
- [x] Checked Retry-After coverage. `RecoverySuite:331` tests the 19-digit saturation case and `ConcurrencySuite:159-170` tests the 200-digit fallback. The guard is at `ContentUnderstandingResponseParsing.scala:88-90`.
- [x] Confirmed these areas have tests:
  - shared-stop isolation per partition and per action
  - the HTTP 203/206 unaccepted-handle matrix
  - creation status mapping
  - DNS capacity and deadline handling
  - the explicit-versus-legacy ComplexParams writer
  - the Dataset resolver (`VerifyDatasetExtensions`, CU suites)
- [ ] I did not run any tests (read-only review). Validation status comes from the logs supplied with the review request:
  - 144/144 Scala tests at baseline, plus 7 new tests and 1 strengthened test.
  - The Python 29/29 result belongs to snapshot25.
  - No combined 151-test run is claimed.

## Issues

### Issue 1: Deep-error test accepts either decode outcome, so it cannot catch a regression in either one
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingConcurrencySuite.scala
- **Line(s)**: 124-139
- **Description**: The test was strengthened in this attempt, but it still accepts two outcomes:
  - status `Failed` or `Unknown`;
  - error containing `InvalidResponse`, or `Failed` with `InvalidContent`.

  Which outcome you get depends on stack depth. Whether recursive decoding finishes or the scoped stack-overflow/nonfatal handler runs depends on the executor thread's stack size, so the test never pins down which path ran.
- **Risk**: Two regressions could pass unnoticed:
  - The stack-overflow handling in `ContentUnderstandingResponseParsing` could be removed or broken (for example, by catching too broadly or leaking an `InvalidContent` lookalike), and the test would still pass on a large-stack JVM.
  - The successful deep-decode path could break, and the test would still pass on a small-stack JVM.
- **Suggested Fix**: Split it into two deterministic tests.
  - Run the decode on a `Thread` with a small stack (for example, 256 KiB) and assert exactly `Unknown`/`InvalidResponse` with the handle and raw body kept.
  - Use nesting within the 1000-level limit on the default thread and assert exactly `Failed` with `InvalidContent`.

### Issue 2: Retry-After boundary values (20 digits, non-ASCII digits) are not tested
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingResponseParsing.scala (tests: ContentUnderstandingRecoverySuite.scala:331, ContentUnderstandingConcurrencySuite.scala:159)
- **Line(s)**: 15, 88-90
- **Description**: The contract allows "at most 19 ASCII digits with saturation; unsupported inputs fall back." The tests cover 19 nines (saturation) and 200 nines (fallback). Two cases are missing:
  - The exact boundary of 20 digits.
  - Non-ASCII Unicode digits such as `"٣"`. The old `toLong` path accepted these; the new `c >= '0' && c <= '9'` guard deliberately rejects them, and nothing tests that.
- **Risk**: Two regressions could go unnoticed:
  - Changing `<=` to `<` in the length check, or switching to `Character.isDigit` / `toLong`, would not fail any test.
  - A Unicode-digit regression could make polling wait an unintended delay instead of falling back.
- **Suggested Fix**: Add a short table test on `retrySeconds`, or one stub row per case, that asserts `"9" * 20` → fallback, `"٣"` → fallback, `"+5"`/`" 5 "` → the documented behavior, and an HTTP-date → still supported.

### Issue 3: Shared `SimpleHTTPTransformer` planning change has no core-level test
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/SimpleHTTPTransformer.scala
- **Line(s)**: `makePipeline` (temporary column naming / `colsToAvoid` including `getErrorCol`; case-insensitive internal resolver)
- **Description**: This change affects every HTTP-based service, not just CU. It does two things:
  - adds the error column to the names that internal temporary columns must avoid;
  - plans those temporary names with a case-insensitive resolver.

  The only test is indirect, through the CU Recovery-suite resolver test (`PARSEDINPUT`/`UNPARSEDOUTPUT`). No core suite runs `SimpleHTTPTransformer` directly with an error column that collides with an internal name (`parsedInput`/`unparsedOutput`) or with input columns that differ only in case.
- **Risk**: CU overrides much of the transformer path, so a regression in the base `SimpleHTTPTransformer` behavior (used by non-CU services) would not be caught by core tests, and overwriting an input column could return.
- **Suggested Fix**: Add one small core test that builds a `SimpleHTTPTransformer` with a stub handler. Set `errorCol = "parsedInput"` and add an input column `UNPARSEDOUTPUT`, then assert that input columns stay intact and output/error land in their configured columns, under both case-sensitive and case-insensitive sessions.

### Issue 4: Deadline/abort-saturation test does not check the diagnostic for a dispatched POST
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingConcurrencySuite.scala
- **Line(s)**: 577-612
- **Description**: The test checks timing (<4000 ms), `Unknown` status, no handle, and one request. It does not check `response.error`. For a POST that was dispatched and then timed out, the contract requires a "may have been accepted" diagnostic of a specific kind, distinct from an unsent expiry or a genuine DNS/transport failure. That branch of `ContentUnderstandingTransport.failure` remains unpinned.
- **Risk**: Two kinds of change would still pass the test:
  - Reclassifying a dispatched-POST timeout as an unsent `OperationTimedOut`, which would wrongly suggest it is safe to resubmit.
  - Dropping the possibly-accepted wording.

  Either would break the no-replay and ambiguity guarantees.
- **Suggested Fix**: Assert the exact error kind and that the message says the submission may have been accepted, matching the wording already used in the connection-refusal and oversized-header tests.

### Issue 5: No test for a column-bound `modelDeployments` with a scalar analyzer and API version
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala (`settings`: `sharedModels` vs `sharedAnalyzer`); tests in ContentUnderstandingRecoverySuite.scala / ContentUnderstandingSubmissionSuite.scala
- **Line(s)**: ContentUnderstanding.scala `settings` (sharedAnalyzer/sharedModels derivation); RecoverySuite 284-300
- **Description**: `sharedModels` additionally requires `isScalar(modelDeployments)`, separately from `sharedAnalyzer`. The existing tests leave the mixed case untested:
  - "per-row API versions…" makes both flags false through `apiVersionCol`.
  - "shared deployment errors stop submissions…" keeps both flags true.
  - `setModelDeploymentsCol` is only used in schema/copy tests (`ContentUnderstandingSuite.scala` 97, 508).

  No test covers `sharedAnalyzer = true` with `sharedModels = false`, where a `DeploymentNotFound` should stay a row-level failure while `AnalyzerNotFound` still stops new submissions.
- **Risk**: If `isScalar(modelDeployments)` were dropped from `sharedModels`, one row's bad per-row deployment mapping could stop the whole partition, against the "column bindings stay row-level" contract. No test would fail.
- **Suggested Fix**: Add a stub test with a scalar analyzer and API version and a `modelDeployments` column over two rows. Reply `DeploymentNotFound` for row 1 and success for row 2, then assert two POSTs, row 1 `Failed`/`DeploymentNotFound`, and row 2 `Succeeded`. Optionally add the contrasting case where `AnalyzerNotFound` still produces `SharedConfigurationFailure` for later rows.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Unresolved; repeated deterministic-fallback recommendation.
- **What changed**: Retain the strengthened service/fallback diagnostic assertion without forcing one stack-dependent outcome.
- **Why**: Thread stack-size hints are platform-dependent. Deterministically isolating parser and compaction fallbacks deserves separate test design, not a production parser change merely to satisfy the test.
- **How verified**: Existing scoped StackOverflowError handling remains intact; the strengthened test passed. No forced-small-stack experiment or exact-depth-1000 acceptance test was added.

### Issue 2
- **Status**: Addressed during stage-6 unit-name cleanup.
- **What changed**: Added a focused table for zero, ordinary milliseconds, 19-digit saturation, 20-digit rejection, non-ASCII digits, plus signs, surrounding whitespace and empty input; assert both the renamed internal delta helper and the public protocol fallback.
- **Why**: Pin units and boundaries while renaming the misleading internal helper. Conversion behavior remains unchanged. Changing <= to < was already covered by the earlier 19-digit case.
- **How verified**: The new case passes in `cu-stage6-polish-green.log` with all 12 focused tests. No runtime parsing defect was claimed.

### Issue 3
- **Status**: Unresolved; duplicate of GPT issue 2.
- **What changed**: No new shared core regression test.
- **Why**: Direct shared HTTP column-planning coverage is useful; retain rather than silently count CU override coverage as equivalent.
- **How verified**: Driver inspected the production reservation and the core suite under `io/split1`; the requested case is absent there.

### Issue 4
- **Status**: Unresolved diagnostic-assertion recommendation, with contract qualification.
- **What changed**: No timeout behavior or assertion was changed.
- **Why**: A sent-timeout diagnostic assertion would strengthen coverage, but the proposed wording is not the current implementation contract. `failure(... timedOut = true)` returns OperationTimedOut; definitely unsent requests use RequestNotSent. OperationTimedOut is not itself an unsent classification.
- **How verified**: Driver inspected `ContentUnderstandingTransport.failure` and `ContentUnderstandingProtocol.timeoutError`/`decodeSubmission`. Keep the coverage suggestion separate from an unproven wording defect.

### Issue 5
- **Status**: Unresolved coverage recommendation.
- **What changed**: No additional mixed-binding matrix at the second-attempt gate.
- **Why**: Pinning sharedAnalyzer=true/sharedModels=false would strengthen the current row-level deployment behavior without requiring a new feature.
- **How verified**: `ContentUnderstanding.settings` retains the separate isScalar(modelDeployments) guard. No runtime failure was reproduced; preserve this as missing regression coverage.