---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 6
timestamp: "2026-11-05T21:11:22Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ca8b168613732a859182408c74fa2cfa2a9c1bc22d9c25eae24c7eb8f42bdefc"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the supplied diff for the CU transformer, protocol, response parsing, the shared `CognitiveServiceBase` auth-header change, the Python mixins and the docs.
- [x] Searched `cognitive\src` and `core\src` (`*.scala`, `*.py`) for `writeToTable|writeToPath|ContentUnderstandingWriter|requestFingerprint|submitOne|pollOne`. The only matches are the negative assertions in `ContentUnderstandingSuite.scala`, so no references to the deferred writer or journal remain in production code.
- [x] Checked logging precedent. `core\src\main\scala\...\io\http\Parsers.scala` (lines 38, 95, 157, 212, 233) and `HTTPTransformer.scala:101` use `logClass(FeatureNames.Core)` for internal HTTP stages. `ContentUnderstanding.scala:475` uses the CU feature name for its internal parser.
- [x] Checked `ContentUnderstandingResponseParsing.scala:88-95` (`retrySeconds` returns milliseconds) and `ContentUnderstandingProtocol.scala:300-305` (`retryDelay` binds the value to `seconds`).
- [x] Checked `ContentUnderstandingProtocol.scala:44-48`. The `ContentUnderstandingException` message deliberately leaves out all diagnostic codes.
- [x] Checked `pipeline.yaml:821-822`. `PACKAGE: "services.contentunderstanding"` resolves through `TEST_SPEC` (line 970) to `com.microsoft.azure.synapse.ml.services.contentunderstanding.**`, which matches the new test package.
- [x] Checked the docs against code. `concurrentTimeout` is rejected (`validateConfiguration`), Retry-After accepts at most 19 digits with fallback, the durable writer is documented as deferred, and transform never provisions analyzers.
- [x] Checked `ContentUnderstanding.scala:396-422` (`buildInternalTransformer`). It creates a new snapshot, `settings(resolver)` and output parser on every call; `handlingFunc` fails closed.
- [ ] No tests, builds or codegen were run; this review was read-only. Validation claims come from the supplied logs and were not re-run.

## Issues

### Issue 1: `retrySeconds` returns milliseconds, but its name and the local variable say seconds
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingResponseParsing.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: ResponseParsing 88-95; Protocol 300-305
- **Description**: `retrySeconds(raw)` returns `BigInt(value) * MillisecondsPerSecond`, saturated to `Long.MaxValue`, so the result is in milliseconds. `retryDelay` stores it in `val seconds` and combines it with `date` (also milliseconds) and `fallback` (milliseconds). The arithmetic is correct, but the names say seconds.
- **Risk**: A later maintainer could "fix" the apparent mismatch by multiplying by 1000 again or by changing the saturation. That would quietly change the Retry-After behavior the contract requires (at most 19 digits, saturation, long delays not shortened). Unit confusion in retry and deadline code is easy to miss in review.
- **Suggested Fix**: Rename the function to `retryAfterMillis` (or `parseRetryAfterDeltaMillis`) and the local variable to `deltaMillis`. Update the callers and tests only; no behavior changes.

### Issue 2: The internal CU output parser logs under the public CU feature name, so each transform emits extra CU telemetry
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 424-425, 472-490
- **Description**: `getInternalOutputParser` creates a new `ContentUnderstandingOutputParser` on every `buildInternalTransformer` call. Its constructor calls `logClass(FeatureNames.AiServices.ContentUnderstanding)`, and its `transform` wraps the work in `logTransform`. A single user `ContentUnderstanding.transform` therefore records two CU constructor events and two CU transform events: the outer stage (lines 52 and 68) plus the internal parser. Schema-only `transformSchema` calls add more. Other internal HTTP stages use `FeatureNames.Core` (Parsers.scala, HTTPTransformer.scala).
- **Risk**: CU adoption and usage telemetry is inflated by about 2x and harder to interpret. This is the observability metric the new `FeatureNames.AiServices.ContentUnderstanding` constant was added to measure.
- **Suggested Fix**: Use `logClass(FeatureNames.Core)` in `ContentUnderstandingOutputParser`, matching the sibling internal parsers. Alternatively, drop the parser's `logClass`/`logTransform`, since the outer CU transform already logs the operation.

### Issue 3: The exception message leaves out even connector-generated diagnostic codes, so the actionable cause is hidden
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 44-48
- **Description**: `ContentUnderstandingException` always reads "returned <status> (HTTP <n>). Check the configured authentication, analyzer, deployments, and operation budget…". Errors created by the connector itself, such as OperationTimedOut, MissingOperationLocation, InvalidOperationLocation, ResponseTooLarge, AnalyzerNotReady and AnalyzerRetrievalFailed, contain no service text. They could appear in the message without breaking the no-service-message-echo rule, but they are left out. Users of the driver-side `createAnalyzer`/`getAnalyzer` (especially in Python, through `Py4JJavaError`) must call `e.java_exception.response().error()` to see the cause. The generic hint also suggests checking auth and deployments when the real cause is a response size limit or an unsafe handle.
- **Risk**: Harder triage of management failures in notebooks and logs, and misleading remediation advice in the hint.
- **Suggested Fix**: Add `Diagnostic code: <code>` to the message only when the top-level `error.code` is in a fixed allowlist of connector-generated codes. Keep service codes and messages out. Optionally, tailor the hint for OperationTimedOut and ResponseTooLarge (operation budget, `maxResponseBytes`).

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed.
- **What changed**: Renamed the internal helper to `retryAfterDeltaMillis` and its caller variable to `deltaMillis`; conversion, saturation, fallback and date behavior are unchanged.
- **Why**: Make the returned unit explicit without redesigning the retry policy.
- **How verified**: Main/test style and compilation pass. `cu-stage6-polish-green.log` passes 12 focused tests, including direct and public-helper checks for milliseconds, 19-digit saturation, exact 20-digit rejection, non-ASCII rejection, signs, whitespace and fallback.

### Issue 2
- **Status**: Fixed and locally verified.
- **What changed**: The internal output parser logs its constructor under `FeatureNames.Core`, matching internal HTTP parser precedent. The public CU stage's feature logging and ordinary transform logs are unchanged.
- **Why**: Avoid extra public-CU certified constructor events from internal pipeline construction. Driver inspection confirms that `logTransform` passes no certified feature name; the report's exact 2x usage/billing interpretation was not established.
- **How verified**: `cu-stage6-telemetry-red.log` failed before the change, capturing a public-CU constructor event while parsing succeeded. The regression captures `logBase` locally (no telemetry delivery), then passes with Core constructor and ordinary transform events. All 12 focused tests, including parser serialization/experiment fuzzing, pass in `cu-stage6-polish-green.log`.

### Issue 3
- **Status**: Deferred optional diagnostic-UX recommendation.
- **What changed**: No exception-message change.
- **Why**: Preserve the current generic message plus structured-response contract during polish. A fixed allowlist can constrain displayed strings but does not establish that a service-supplied lookalike code originated in the connector. No new provenance mechanism or message-format promise was introduced.
- **How verified**: Driver inspected the existing exception and structured error path. No evidence that the retained behavior violates the agreed connectivity scope; the enhancement remains unimplemented, not fixed.

## Driver: Stage 6 attempt 1 collection and final attempt plan

- GPT clean (105.29s); Opus three Low findings (130.442s); Grok one Low duplicate of the parser telemetry issue (176.662s). All artifacts persisted, input hashes matched, and source/known-document tool arguments were audited without prior/sibling feedback exposure. Recoverable missing/malformed-path lookups remain in the transports.
- Two scoped items are implemented: explicit millisecond naming and internal-parser feature attribution. Red/green telemetry evidence and 12 passing focused tests are retained. Whitespace, cognitive main/test style and compilation pass.
- The new Retry-After boundary table also resolves stage-5 Opus issue 2. Other earlier coverage/architecture recommendations and the creation-status compatibility question remain retained; a later clean stage must not erase them.
- Prior evidence stays qualified: 144-test baseline before subsequent changes; seven added tests plus one strengthened test at stage 5; no new combined full-suite claim. Snapshot25 Python/codegen/publication hashes are not the latest build.
- Attempt 2 uses immutable `cu-stage6-attempt2.patch`: 37 paths, 465636 bytes, SHA256 `b42d07e10ed2cd016f6950f32ea261022bc028019ad99e1d61789d3f0908cb81`. Source frozen until collection.
- Same human-selected High/direct policy and current-catalog reviewers: `gpt-6.1-sol/high`, `grok-4.7/low`, `claude-opus-5.5/high`; primary `gpt-6-astra`. Broad contribution and substantial concurrency/state/lifecycle complexity justify the three reviewers. No third polish attempt; retain unresolved second-attempt findings. No new bounds or permissions.
- Reviewers receive identical contract and qualified validation, explicit source-search guidance, and no prior/sibling reports. No commit, push, ADO edit, live CU call, target integration or CI claim.