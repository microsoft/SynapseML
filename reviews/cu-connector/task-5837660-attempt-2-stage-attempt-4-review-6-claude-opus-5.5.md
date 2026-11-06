---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 4
round: 6
timestamp: "2026-11-05T23:32:43Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6fd98681658a4c397b60db19036213efcafa0bed66a9771fcca62b8df5ce1a25"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-4-review-6-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the whole supplied frozen diff for the 39 paths, covering the Scala transformer, protocol, transport, DNS resolver, exception, logging, response parsing, params and Python codegen, the shared `CognitiveServiceBase`/`SimpleHTTPTransformer`/`ComplexParamsWriter`/`Wrappable` changes, the docs and the tests.
- [x] Confirmed that the new default generated Python `isSet` in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` reaches every override. Every `pyAdditionalMethods` override calls `super.pyAdditionalMethods`: `OpenAIPrompt.scala:168`, `AzureMapsTraits.scala:23`, `ContentUnderstandingParams.scala:216`, `CognitiveServiceBase.scala:284/636/679`, and `PyCodegenSuite`.
- [x] Confirmed that management logging writes only outcome, timing and exception class. `SynapseMLLogging.scala:106-117` (`getPayload`) and `:135-147` (`logBase` with `featureName = None`) carry no service body or message. `ContentUnderstandingLogging.safely` re-interrupts on `InterruptedException`, and `logManagement` handles `InterruptedException` separately because `NonFatal` excludes it.
- [x] Checked the Python lifecycle in `ContentUnderstandingParams.scala:157-252`:
  - `_transfer_params_from_java` pops ServiceParams and only copies non-service params where JVM `isSet` is true.
  - `outputCol` and `errorCol` defaults are refreshed after a UID change.
  - The dedicated `setDocumentBytes`/`setDocumentBytesCol` setters remove stale pending entries.
  - `setEndpoint`/`setCustomServiceName` re-sync `url`.
  - `clear` clears both the JVM and Python sides.
  - `createAnalyzer`/`getAnalyzer` transfer pending values first.
- [x] Checked the bounds and concurrency code against the documentation ("Bounds and concurrency" in `docs/Explore Algorithms/AI Services/Content Understanding.md`):
  - DNS: `BudgetedDnsResolver` uses a fair semaphore of 32 that is released in the worker's `finally`, with 32 threads and a queue of 32. Permits are held until the lookup completes, and stuck workers are not replaced.
  - Deadline: `DeadlineControl` uses a daemon scheduled pool with remove-on-cancel, and `exchangeBeforeDeadline` caps the timeouts at the remaining budget.
- [x] Checked `ContentUnderstandingProtocol.scala:295-301` and `ContentUnderstandingResponseParsing.scala:88-95`. The Retry-After bound is at most 19 ASCII digits, otherwise an RFC 1123 date, otherwise the fallback. Delays are not shortened to fit the budget.
- [x] Checked exception privacy:
  - `ContentUnderstandingException` keeps its message neutral, using allowlisted codes only and inspecting the error text only when it is 4096 characters or fewer.
  - In `rowHandler`, `innererror` is allowlisted to TransportError, OperationTimedOut, ResponseTooLarge and ResponseBodyReadFailed, and is used only when there is no raw response.
  - The shared-stop log fires once per partition behind `compareAndSet`.
- [x] Checked the `pipeline.yaml` `$(PACKAGE)` selector usage at lines 674-676, 742 and 970. It is unchanged in shape and no expansion is needed.
- [ ] I did not run tests or builds; this was a read-only review. I rely on the driver's evidence: scalastyle, compile, 165 CU tests, codegen and Black.

## Issues

### Issue 1: Retry-After always attempts HTTP-date parsing, even after a valid delay-seconds value
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: 295-301 (related: ContentUnderstandingResponseParsing.scala 88-95)
- **Description**: `retryDelay` computes the RFC 1123 `date` value even when `deltaMillis` has already parsed, because `orElse` receives a value that was evaluated earlier, not a lazy expression. As a result, every digit-only `Retry-After` causes `ZonedDateTime.parse`, which throws a `DateTimeParseException` that `Try` then swallows. The date path also skips the `trim` that `retryAfterDeltaMillis` applies, so the two formats treat whitespace inconsistently.
- **Risk**: This affects performance and clarity only; the chosen delay is still correct. It adds an exception to every poll or retry that carries a numeric Retry-After. Across many rows and polls, this is avoidable CPU and garbage-collection work in a hot path that runs on every poll. Whitespace handling now depends on which format the service sends. HttpClient normally trims header values, so this is unlikely to show up in practice.
- **Suggested Fix**: Parse the date only when needed, using a trimmed value. For example: `value.flatMap(v => ContentUnderstandingResponseParsing.retryAfterDeltaMillis(v).orElse(Try(ZonedDateTime.parse(v.trim, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant.toEpochMilli).toOption.map(t => math.max(0L, t - System.currentTimeMillis())))).getOrElse(fallback.toLong)`. Optionally, add one test that asserts a padded HTTP-date value is accepted.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed locally; awaiting independent rerun.
- **What changed**: Moved the date-parsing expression into Option.orElse's by-name argument and trim the date input. Valid numeric values no longer evaluate the date path.
- **Why**: Avoid the unnecessary exception-producing parse and make date/numeric whitespace handling consistent. This eager pattern was present in committed HEAD.
- **How verified**: `cu-retry-after-lazy-red.log` records the new padded-date assertion returning fallback7 before the fix. `cu-retry-after-lazy-green.log` records all5 Retry-After tests across4 suites passing, plus main/test style/compile and codegen. Black205 unchanged; whitespace passed. This is functional/static evidence, not a production performance benchmark.
- **Snapshot**: `cu-stage6-attempt7.patch`, SHA256 `2eecde9cd40cc27b0b8955c4f3d5abb93a7b4bb9613011c7c6b0246200e5ba76`,495826 bytes,39 paths. Earlier165/165 full-CU evidence is snapshot6, not a full rerun on snapshot7. Original Low/ISSUES_FOUND remains intact.