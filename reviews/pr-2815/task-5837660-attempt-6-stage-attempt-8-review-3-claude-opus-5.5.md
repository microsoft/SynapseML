---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 8
round: 3
timestamp: "2026-11-11T02:46:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "d6caf1224cefaaba258cdc47e0601583fa216d3861ff1fe03553cd37816e7b7b"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-8-review-3-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Which 429s are retried (requirement 1).** `P\ContentUnderstandingTransport.scala` 21-37:
  - `throttled(method)` is true only for a 429 on a GET, or a 429 on a POST with no `Operation-Location` header at all.
  - An empty header arrives as `Some("")`. That response is not throttled, so it decodes as `Unknown` and is never replayed.
- [x] **Retry is analysis-only and opt-in.** `P\ContentUnderstandingProtocol.scala` `exchange` 317-339 sets `retryEnabled = analysis && settings.retryThrottledRequests`.
  - Analysis polling passes `analysis = analyzer.isEmpty` (607-608).
  - The management PUT, creation polling and `getAnalyzer` pass `analysis = false`, so their behavior is unchanged.
  - `HasRetryThrottledRequests` (`P\ContentUnderstandingParams.scala` 153-163) is a separate trait: a `BooleanParam`, not a ServiceParam, defaulting to false.
- [x] **Backoff (requirement 2).** `P\ContentUnderstandingThrottling.scala` 27-44:
  - `base = max(1000, pollingDelay)` and `cap = max(60000, base)`; the delay doubles each time.
  - Jitter is drawn from `nextLong(max(base, cur - cur/10), cur + 1)`. The lower bound never exceeds `cur`, so the call can't throw `IllegalArgumentException`. The first fallback equals `base` exactly, and a `base` of at least 60000 stays constant. No overflow is possible.
  - A valid header resets the next delay to `base`.
  - Each `retry` call creates its own `Backoff`, so state resets per exchange.
  - The `pollingDelay` and `maxPollAttempts` docs (Params 107-122) match this behavior.
- [x] **Retry-After parsing.** `P\ContentUnderstandingResponseParsing.scala` 90-103:
  - Delta-seconds values are clamped through BigInt to `Long.MaxValue`.
  - RFC 1123 dates become `max(0, t - now)`, so zero and past values are honored exactly with no upper cap.
  - `retryDelay` (Protocol 261-262) uses the same parser on non-throttled paths and gives the same result as the removed baseline.
- [x] **Budget refunds.** `P\ContentUnderstandingBudget.scala` 20-28:
  - `mark`/`refundSince` refunds separate, non-overlapping intervals, so elapsed time never goes negative and remaining time never exceeds `durationMs`.
  - The final non-throttled attempt is charged.
  - Throttled GETs loop inside `exchange`, so they don't use up `maxPollAttempts`.
  - Each `withClient` call gets its own budget, used by one thread only.
- [x] **Shared stop.**
  - `Context.submissionStopped` applies to POST only (Throttling 17-23), so accepted GETs and acceptance already in flight continue.
  - A wait tick throws `SubmissionStopped`, a ControlThrowable that `pause` doesn't catch (it only catches `InterruptedException`). `retry` catches it, refunds the wait and returns the last 429.
  - The wrapper at Protocol 336-338 applies only to throttled POSTs with retry on and the stop already set.
  - `decode`/`decodeSubmission` (Protocol 417-451, ≈512-526) then produce `Rejected` with HTTP 429, the original body as `rawResponse`, and any read or size error nested as `innererror`.
  - `checkSharedFailure` (≈485-500) can't fire again: a 429 isn't 401/403/404, and the error codes are internal only.
  - With retry off, the single attempt and its diagnostics are unchanged. Rows reaching `rowHandler` (661-682) after a stop are still rejected before dispatch.
- [x] **Cancellation and concurrency.**
  - `checkInterrupted` (Protocol 252-259) uses `AsyncClient.isTaskCancelled`. Its TaskContext is carried to async row threads through a ThreadLocal (`C\..\io\http\Clients.scala` 51-57). It runs at each attempt and at every 50 ms wait slice, including a zero-length wait.
  - An interrupt during a wait sets the interrupt flag again and propagates. The reporter's `safely` also re-sets the flag, so the next tick throws.
  - The `blocking` wrappers in `exchange` and `pause` are not nested inside each other.
  - `Context` uses an AtomicBoolean and is Serializable, and so is the synchronized `ThrottleReporter`. The handler closure is deserialized per task, so state is per partition.
- [x] **Throttle logging.** `P\ContentUnderstandingLogging.scala` 15-58: logs the first retry, then at most once per 60 s, including during waits. Fields are method, retryCount, waitMs and the stage, partition and task-attempt IDs only, with no URL, body or key. Logging failures are swallowed except for interrupts.
- [x] **`allowInputTruncation` (requirement 3).**
  - `HasAllowInputTruncation` (Params 142-151) is a `ServiceParam[Boolean]` with no default.
  - In `P\ContentUnderstanding.scala`, `expectedType`/`validateValue` and `analyzeUrl` use `getValueOpt`: unset or null values are omitted and false is sent. Poll-mode `activeParams` exclude it.
  - There is no API-version allowlist; the version check is a regex only.
  - `T\ContentUnderstandingOptionsSuite.scala` read in full.
- [x] **Python inspection (requirement 4).**
  - The `Utils.py` overrides were removed. A grep for `def (isSet|hasDefault|getOrDefault|extractParamMap)\(` across `core\src\main` and `cognitive\src\main` finds nothing.
  - So removing the `isSet` stub exclusion in `pyStubAdditionalMethods` (`C\codegen\Wrappable.scala` 469-488) changes nothing for these modules.
  - Reviewed for consistency: `test_ServiceParamPythonBridge.py`, the `test_OpenAIResponseSchema.py` hunk, the PyCodegenSuite and ContentUnderstandingSuite source assertions, and the `Autologging.md` caveats.
- [x] **Notebook and guide (requirement 5).** `Y\test_ContentUnderstanding.py` 507-965 checks that:
  - Repeating an `errorifexists` save raises `AnalysisException` and sends no new requests, and results are reloaded before display.
  - The endpoint, origin, run ID and model-deployment mapping are validated before any service call.
  - Wrong-resource recovery writes nothing and sends nothing.
  - `modelDeployments` appears only in analysis requests, is omitted when empty, and the creation request has none.
  - The analyzer is created eagerly, and the field, confidence, source and supporting-text projections are covered.
- [x] **Unattended notebook exclusions (requirement 6).**
  - `generatedNotebookName` (`R\nbtest\SharedNotebookE2ETestUtilities.scala` 89-91) is an exact extraction of the removed baseline transform.
  - `SynapseTests.ExcludedNotebooks`/`includeNotebook` (48-70) is used only at the sites grep found.
  - Databricks `CPUNotebooks` excludes the notebook (`DatabricksUtilities.scala` 256-258), as tested in `DatabricksUtilitiesSuite.scala` 20-32. The Fabric allowlist also excludes it.
- [x] **Test suites read.** ThrottlingSuite, ThrottlingPolicySuite and OptionsSuite were read in full, plus the modified hunks in the Admission, Concurrency (create × retry), Fuzzing, Recovery, Submission, ContentUnderstandingSuite and PyCodegenSuite suites.
- [x] **Considered and rejected as defects.**
  - A repeated `Retry-After: 0` produces a tight loop, but the requirements mandate exact zero handling and the loop is still cancellable on every iteration.
  - A slow-trickling 429 body is still treated as throttled and retried; this is by design and cancellable.
  - `retryCount` in the log is cumulative per partition. This is diagnostic only.
- [ ] **No commands or tests were run** (read-only mandate). The driver's validation evidence (compile, style, codegen, Scala, Python and notebook test counts, Black, whitespace) is unverified.
- [ ] **Generated stubs and wrappers were not inspected.** `target/` and modules outside `core`/`cognitive` were out of scope, so the byte-identity claim rests on the driver's evidence.
- [ ] **Not every range was re-read in the final pass.** Most indexed ranges, including the docs, the notebook, Python tests outside 500-965 and the Fabric tests, were read before context compaction in this same review. The final pass re-read the retry, budget, transport, logging and decode paths, the Params, Python test lines 500-965 and the notebook-harness files.
- [ ] **No live Content Understanding validation,** which the plan excludes.