---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 7
round: 4
timestamp: "2026-11-11T02:01:16Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ce4d2c68f9fdc3c77267c1775e86d4f44fdd15266fecae8babfd4f55652902f4"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-7-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Backoff arithmetic** (`P\ContentUnderstandingThrottling.scala` 27-44):
  - `base=max(1000,pollingDelay)`.
  - `cap=max(60000,base)`.
  - `next=min(cap,2*current)` cannot overflow, because `current ≤ cap ≤ Int.MaxValue`.
  - `nextLong(max(base,current-current/10), current+1)` draws from the closed range [lower,current], and `lower ≤ current` always holds.
  - The first fallback equals `base` exactly. When `base ≥ 60000`, the fallback is constant.
  - A valid header resets `next=base`.
  - A new Backoff is created for each `retry` call (line 52), so state resets per exchange.
- [x] **Retry loop** (Throttling 46-76):
  - With retry off, `attempt` is evaluated once (49-50).
  - `mark` is taken before each attempt (55).
  - A stopped POST skips the wait (57). During a wait, the tick throws the `SubmissionStopped` ControlThrowable, which is caught by an object match (62-66).
  - `refundSince(mark)` refunds only the throttled attempt and its wait (68).
  - Interrupt and cancellation exceptions propagate without a refund. GET is never stopped (line 22).
- [x] **Budget** (`P\ContentUnderstandingBudget.scala` 15-55):
  - `started += now-mark` keeps `started ≤ now`, so `remainingMs ≤ durationMs`.
  - `pause` checks for interrupts and ticks before each 50 ms chunk.
  - `delay - elapsed` cannot overflow even with a `Long.MaxValue` Retry-After, so waits stay cancellable.
- [x] **Response parsing** (`P\ContentUnderstandingResponseParsing.scala` 28-45, ~113-129):
  - `throttled` means 429 and either GET, or POST with `location.isEmpty`. Any Operation-Location, including an empty one, prevents replay, and `uncertainSubmission` (Protocol ~502-510) marks the row Unknown.
  - Retry-After of up to 19 digits is multiplied by 1000 and clamped to `Long.MaxValue`. An RFC 1123 date gives `max(0, t-now)`, so zero or past values retry immediately.
  - `retryDelay` (Protocol 261-262) keeps the old semantics (`Try` → `safely`, both NonFatal).
- [x] **`exchange`** (Protocol 317-339):
  - `retryEnabled = analysis && retryThrottledRequests`.
  - `managementGet = !analysis && GET` matches the old mapping at each call site: poll, execute POST, getAnalyzerResponse and createAnalyzer PUT.
  - A suppressed stopped POST 429 keeps its 429 status and raw body, and gets SharedConfigurationFailure with nested `innererror`.
  - `decodeSubmission` returns Rejected. `checkSharedFailure` (485-500) finds no category because 429 is not 401/403 and the nested connector codes are not analyzer or deployment codes, so nothing is re-thrown.
- [x] **Polling and management** (Protocol 583-621, 697-786):
  - `remaining` decrements once per completed `exchange`, so opted-in GET 429 retries do not use up `maxPollAttempts`.
  - Ordinary poll pauses are not refunded.
  - Management exchanges pass `analysis=false`.
  - `createAnalyzer` reuses one Context across PUT, poll and finishCreation. `getAnalyzer` creates a new one.
- [x] **Partition scope:** `rowHandler` (Protocol 660-683) creates one Context and captures it in the handler closure. `HTTPTransformer.scala` 107-116 and `SharedVariable.scala` 17-23 (`@transient lazy`) show a per-task client built from the deserialized handler, so the stop and reporter are partition-scoped. Poll mode ignores the stop.
- [x] **Throttle logging** (`P\ContentUnderstandingLogging.scala` 15-40):
  - Synchronized. Logs the first report, then at most one per 60000 ms, with waits included through the `waiting` tick.
  - Includes method, retryCount, waitMs and task IDs from the existing `AsyncClient.currentTaskContext` (`Clients.scala` 50-73). No URL, body or key.
  - `T\ContentUnderstandingSubmissionSuite.scala` 327-347 asserts task IDs at concurrency 1 and 2.
- [x] **`allowInputTruncation`** (`ContentUnderstanding.scala` 84-92, 169-241, 270-283, 318-325; Params 142-165):
  - Included only in non-poll mode, checked against an exact BooleanType schema, and the boxed Boolean passes the no-op value checks.
  - Generic `getValueOpt[T]` (`CognitiveServiceBase.scala` 102-107) erases T, so a null column value becomes None and the option is omitted.
  - An explicit false is sent as "false" through the default `toString` (`JsonEncodableParam.scala` 90).
  - There is no version gate. The retry flag lives in its own trait and defaults to false.
- [x] **Python inspection:**
  - The `isSet`/`hasDefault`/`getOrDefault`/`extractParamMap` ServiceParam overrides were removed from `U\core\schema\Utils.py`.
  - Explain redaction and the transfer-to/from-JVM pop/apply logic remain.
  - A grep of hand-written Python in cognitive and core main found no caller relying on the removed behavior.
  - The test updates in `Y\test_ContentUnderstanding.py`, `Y\test_ServiceParamPythonBridge.py` and `Y\openai\test_OpenAIResponseSchema.py` 74-91 match the new behavior.
- [x] **Notebook and loopback test** (`Y\test_ContentUnderstanding.py` 662-879):
  - Locks the cell contract.
  - Configuration and model-deployment validation happen before any request.
  - Results are written with `errorifexists`.
  - A wrong-resource recovery raises before any write or request.
  - Re-polling already settled results sends no requests.
  - `modelDeployments` appears only in the analysis POST and is omitted when empty.
- [x] **nbtest exclusions:**
  - `generatedNotebookName` (SharedNotebookE2ETestUtilities 89-91) applies the same transform as the removed inline code, and its output matches the SynapseTests token (48-69).
  - The Databricks CPU filter (256-258) excludes the notebook, and it does not match the GPU or Rapids filters.
  - Fabric uses an include list, so the notebook is excluded there too.
- [x] **Docs:** `D\Content Understanding.md` lines 195 and 210-226 (allowInputTruncation), 366-410 (throttling), 495-533 (params and logging) and 640-710 (`errorifexists` writes followed by reload) match the code.
- [x] **Optional tastes, not defects:**
  - `pause` ticks twice before its first sleep; this is harmless because logging is rate-limited.
  - The sleep loop uses `System.nanoTime` directly, while budget accounting uses the injected clock; this looks intentional.
  - The `RegularNotebook.py` check in DatabricksUtilitiesSuite depends on the working-directory path, a limitation it inherits from the existing `getAbsolutePath.contains` check.
  - The new `blocking {}` around the exchange is bounded by concurrency.
- [ ] **Not executed:** with read-only tools I ran no build, test or codegen. The validation evidence in the plan was accepted as supplied, not reproduced.