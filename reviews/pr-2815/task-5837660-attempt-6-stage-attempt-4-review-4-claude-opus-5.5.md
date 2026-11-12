---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 4
round: 4
timestamp: "2026-11-10T21:49:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "a4092cf9679ae0168e99d70283272629ac616cc4bae510d9eeff2add1e334d76"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-4-review-4-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Backoff arithmetic** (`ContentUnderstandingThrottling.scala` 24–41): `base = max(1000, pollingDelay)` and `cap = max(60000, base)`. The jitter lower bound `max(base, current - current/10)` is always ≤ `current`, so `nextLong(lower, current + 1)` cannot throw and cannot overflow.
- [x] **Backoff sequence**: with the default `pollingDelay`, the delays are 1000 (exact), then [1800,2000], …, [28800,32000], then [54000,60000] for every delay at the cap. Only `pollingDelay ≥ 60000` gives a constant fallback. A valid `Retry-After` resets growth and is returned exactly, including 0.
- [x] **Retry loop** (`ContentUnderstandingThrottling.scala` 43–72): the order is mark → attempt → throttled and not stopped → delay → `retrying` → cancellable pause → `refundSince(mark)` → stop check. `SubmissionStopped` is a ControlThrowable. `pause` catches only `InterruptedException` (`ContentUnderstandingBudget.scala` 46–50), so the stop reaches `retry`'s catch. GET never stops, because `stopSubmission` is POST-only (lines 16–20).
- [x] **Budget** (`ContentUnderstandingBudget.scala` 14–52): `refundSince` adds back only the time since the mark, so earlier work stays charged. Each operation runs on one thread, so the `@volatile var` read-modify-write is safe. Calling `onTick` twice at pause start is harmless.
- [x] **Protocol data flow** (`ContentUnderstandingProtocol.scala`): traced `exchange` 314–335, `exchangeBeforeDeadline` 337–392, `decode` 413–446, `checkSharedFailure` 481–495, `decodeSubmission` 507–523, `safeSubmission` 525–541, `pollWithClient` 583–616 (`analysis = analyzer.isEmpty` at 603–604), `execute` 618–645 and `rowHandler` 653–678.
  - A 429 whose body read fails or exceeds `maxResponseBytes` keeps its code, location and Retry-After, so it is still retried.
  - Once the budget has expired, the request returns status 0 and is never retried.
- [x] **429 classification** (`ContentUnderstandingResponseParsing.scala` 28–29, 40–43, 112–126): `admissionRejected`, `throttled` and `retryable` behave as intended. Retry-After allows at most 19 digits, saturates at `Long.MaxValue`, and clamps past HTTP dates to 0.
- [x] **`allowInputTruncation` data flow** (`ContentUnderstanding.scala`):
  - It is active only outside poll mode (84–92) and checked as BooleanType (168–178, 189). A boxed `java.lang.Boolean` matches `_: Boolean`.
  - Column type is checked by `matchesType` (243–255).
  - Null column values are skipped in `validateRow` and omitted in `analyzeUrl` (317–324). An explicit `false` sends `"false"`.
  - `buildUrl` sorts the query (311–315), giving `allowInputTruncation` before `api-version`.
- [x] **Settings and params**: `retryThrottledRequests` is plumbed into `Settings` (`ContentUnderstanding.scala` 147) and defaults to false (`ContentUnderstandingParams.scala` 150–161). Management calls never pass `analysis = true` (Protocol 701, 749). `ContentUnderstandingConcurrencySuite.scala` 193–220 checks that management behavior is the same with the flag on or off.
- [x] **Logging**: the `ThrottleReporter` is created once per task's deserialized `Context` (Protocol 659). It is synchronized, logs the first retry, then at most once every 60 s, and the `retrying` + `waiting` pair cannot log twice. `taskDescription` uses `AsyncClient.currentTaskContext`, which falls back to `TaskContext.get()` (`core\...\io\http\Clients.scala` 50–71).
- [x] **Tests traced**:
  - `ContentUnderstandingThrottlingSuite.scala`, including the backoff waits (219–238), the sibling stop with retry=true (241–260) and the GET-continues test (262–273).
  - `ContentUnderstandingConcurrencySuite.scala` 437–457.
  - `ContentUnderstandingSubmissionSuite.scala` 409–428.
- [x] **Docs**: checked `docs\Explore Algorithms\AI Services\Content Understanding.md` lines 283–312, 375–405, 412–459 and 491–528 against the code.
- [x] **Python**:
  - `core\src\main\python\synapse\ml\core\schema\Utils.py` no longer has the generic inspection overrides.
  - The generated ServiceParam getter (`Wrappable.scala` 305–326) still goes through `_paramMap` and then the typed JVM getter.
  - The CU Python truncation, retry, persistence and MLflow tests (172–310) are consistent with this.
- [x] **Notebook**: in the semantic quickstart's recovery cell, the `resumable` / `~resumable` split cannot drop rows, because `analysis.status` is non-null whenever `analysis` is non-null. Results are written once and the saved Parquet is read before display.
- [ ] No builds, tests or codegen were run; this is a read-only review. The live service name of the `allowInputTruncation` query parameter cannot be verified offline and was explicitly approved.

## Issues

### Issue 1: Shared-stop relabel of in-flight POST 429s also runs in the default (retry off) path
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 332–334 (shown with 321–331, 631, 659–670)
- **Description**: `exchange` relabels with `if (analysis && raw.throttled(method) && context.stopSubmission(method)) raw.copy(diagnostic = Some(SharedStopError))`. This check does not look at `settings.retryThrottledRequests`, so it also runs when the opt-in is off.
  - In the default mode, take a sibling POST that was already sent and gets a definitive 429 after another row has set the partition's `stopped` flag.
  - Its service error is replaced by "SharedConfigurationFailure … stopped new submissions". `decode` prefers `raw.diagnostic`; status stays `Rejected` and `httpStatus` stays 429.
  - No retry was skipped, because none exists in the default mode. The new label depends only on whether the 429 arrived before or after the sibling's failure.
  - This conflicts with the docs: "Already-started sibling requests finish and their results are collected" (Content Understanding.md 305–307) and "The default is `False`, preserving the behavior described above" (378–379).
  - The only test that covers the relabel uses `setRetryThrottledRequests(true)` (ThrottlingSuite 241–260). No default-mode test exists.
- **Risk**: The same service response gets a different diagnostic depending on timing. A throttled row can be reported as a configuration failure, sending users to "fix the configuration" instead of "reduce concurrency" (docs 434–438). This changes default behavior that the opt-in was meant to leave alone.
  - Impact is limited to diagnostics: `rawResponse` keeps the body, and the retry-selection SQL still picks the row because it is `Rejected`.
- **Suggested Fix**: Add `settings.retryThrottledRequests` to the condition. Better still, have `ContentUnderstandingThrottling.retry` report that the stop cut a retry short, and relabel only in that case. Add a default-mode test with concurrency 2, where a latch delays the first 429 until the sibling 404 has set the stop. Assert that the 429 row keeps its service error code and does not contain `SharedConfigurationFailure`.

### Issue 2: The `pollingDelay` param doc wrongly says capped fallback delays are exact
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 118
- **Description**: The doc says "Downward jitter applies above the starting delay; the first fallback and delays of at least 60000 are exact."
  - With the default `pollingDelay = 1000`, the backoff (Throttling.scala 24–41) reaches the 60000 cap and then draws from [54000, 60000]. Fallback delays at 60000 are therefore jittered, not exact.
  - The constant case applies only when `pollingDelay` itself is at least 60000, because then `base == cap` and `lower == current`.
  - The Markdown docs state this correctly (Content Understanding.md 396–399, and table row 498). The Scala param doc is the one that feeds generated Python docstrings and `explainParams`.
- **Risk**: Users reading the generated API docs may expect fixed 60 s retries at the cap and misjudge retry timing or rate planning.
- **Suggested Fix**: Reword to: "Downward jitter applies above the starting delay; the first fallback is exact, and a pollingDelay of at least 60000 gives a constant, unjittered fallback." Then regenerate the cognitive codegen.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Resolution Update (supersedes the pending entries above)

- Issue 1: implemented an explicit retry-opt-in guard on shared-stop relabeling
  and nested original read diagnostics. Deterministic stopped-context cases
  cover both modes and readable/truncated/oversized POST 429 responses.
- Issue 2: corrected the public pollingDelay text to describe a constant
  fallback only when the configured base is at least 60000; capped samples at a
  smaller base remain jittered. Regeneration is included in validation.
- Validation and stage collection are recorded in the lens-1 driver section.
  Original findings and verdict are preserved for the next independent review.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, freshly resolved from the current task catalog",
    "stageBehavior": "Six concurrent independent lenses; human explicitly requested a fix/review loop until clean",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head; most volume is tests/docs. Localized CU options/throttling/budget changes plus shared Python parameter-inspection changes.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, partition shared-stop, fallback backoff and refund accounting interact with existing asynchronous state machines.",
  "rationale": "Six complementary lenses cover behavioral, architectural, concurrency, correctness, testing and documentation concerns. Supported CLI update and serialization probe passed; each owning runner must still validate transport and identity. Failed or contaminated results cannot count as clean.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: get into a fix review loop until everything is clean and give me a report when you are done. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 4,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The synthetic CLI transport probe does not establish full review transport success. Offline checks do not establish live CU compatibility or approval of matching packaged artifacts."
}
