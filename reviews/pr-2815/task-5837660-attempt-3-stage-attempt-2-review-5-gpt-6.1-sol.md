---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 5
timestamp: "2026-11-06T19:59:28Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "39f8e9494a9ddc60ee513dae27d634d1217ee681b00f5448ef0216e2410cf0d1"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-5-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied contribution against its Scala/Python tests, including the frozen `ContentUnderstandingParams.scala` setter correction.
- [x] Compared protocol, DNS, transport and budget paths with admission, concurrency, recovery and submission suites; checked request-count, retained-handle, diagnostic and row-association assertions.
- [x] Reviewed generated-wrapper lifecycle coverage and existing ServiceParam bridge regressions against `Utils.py` and `Wrappable.scala`.
- [x] Reviewed shared schema/persistence changes and corresponding core tests; inspected the dedicated CU selector in `pipeline.yaml`.
- [x] Considered supplied validation evidence without executing commands. The 57 offline Python results concern the frozen tree; earlier broader Scala results were not treated as freshly rerun.
- [ ] Live CU, packaged-runtime and Azure CI validation remain outstanding, as disclosed by the caller.

## Issues

### Issue 1: Interruption tests never interrupt an operation already waiting
- **Severity**: Medium
- **File**: `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingSuite.scala`
- **Line(s)**: 424–438; also `ContentUnderstandingSubmissionSuite.scala:96–127`
- **Description**: Both interruption tests set the interrupt flag before entering management execution. They therefore exercise the entry-time `checkInterrupted`, not interruption during `ContentUnderstandingBudget.pause` or either blocking DNS wait. The new flag-restoration and `InterruptedIOException` propagation paths in `ContentUnderstandingDnsResolver.scala:34–40,60–79` and `ContentUnderstandingProtocol.scala:400` remain untested. Deadline-expiry tests do not exercise these paths.
- **Risk**: Losing the interrupt flag or treating cancellation as an ordinary retryable transport failure could leave callers waiting or permit additional GETs without failing the current suite.
- **Suggested Fix**: Add latch-coordinated tests that interrupt an operation during its polling delay and interrupt DNS callers during capacity acquisition and resolution waiting. Assert prompt termination, preserved interruption, no subsequent request/retry, and correct capacity recovery after releasing the resolver. Include a public management-call path for the polling-delay case.

### Issue 2: Shared HTTP error-column collision fix lacks a regression test
- **Severity**: Low
- **File**: `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\SimpleHTTPTransformer.scala`
- **Line(s)**: 117–122
- **Description**: Adding `getErrorCol` to reserved names fixes a distinct collision path, but neither the existing `SimpleHTTPTransformerSuite` nor the added CU tests configures an error column named `parsedInput` or `unparsedOutput`. CU supplies its own generated internal error-column name, so its public-column tests do not exercise this shared fix. Removing `getErrorCol` from the reserved set would leave these tests unaffected.
- **Risk**: A regression could overwrite an intermediate HTTP response or drop the requested error column for existing non-CU consumers without detection.
- **Suggested Fix**: Add focused cases to `SimpleHTTPTransformerSuite` using both reserved error-column names and a case variant. Execute successful and failed responses, asserting preserved input, output/error values, final column names and agreement with `transformSchema`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Blocking interruption paths lack direct coverage.
- **How verified**: Static source/test inspection only.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: The shared error-column reservation change lacks a targeted regression.
- **How verified**: Static source/test inspection only.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s5t2-gpt-6.1-sol-i1: Interrupt already-blocking operations

- Disposition: **test-improvement**.
- Evidence: Existing tests pre-interrupt before entry; they do not prove blocking pause/DNS/HTTP interruption or owning Spark-task cancellation. Pair targeted cases with cancellation correction.
- Consolidated topic: `coverage-interruption`.

### s5t2-gpt-6.1-sol-i2: Shared HTTP temporary/error-column collisions

- Disposition: **test-improvement**.
- Evidence: Existing CU tests cover public names and case variants, not errorCol equal to parsedInput/unparsedOutput in core helper. Add direct regression alongside schema-helper correction.
- Consolidated topic: `coverage-http-columns`.
