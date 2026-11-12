---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 3
round: 4
timestamp: "2026-11-10T21:01:56Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "79a46995235288b9dfb6e8d88811ed9a2ab602baf0d94b90d3f85a85b33c9636"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 42269 (char 42268)
Invalid CLI event on line 334: Expecting ',' delimiter: line 1 column 6934 (char 6933)

## Original Response

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-3-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Retry loop.** I traced `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingThrottling.scala` lines 33–58 one line at a time.
  - When `enabled = false`, `attempt` runs exactly once and nothing is refunded.
  - Each pass runs `mark` → attempt → `shouldRetry` → `delay` → log when `(count & (count - 1)) == 0` (so retries 1, 2, 4, 8, …) → `pause` → `refundSince(mark)`.
  - The final response that is not a 429 is not refunded, so it stays charged.
  - If `pause` throws during cancellation, the exception propagates and no refund happens.
  - `@tailrec` recursion means the retry count cannot overflow the stack.
- [x] **Backoff math** (same file, lines 14–31).
  - `base = max(1000, pollingDelay)` and `cap = max(60000, base)`.
  - `current * 2` cannot overflow, because `current ≤ cap ≤ Int.MaxValue`.
  - `nextLong(lower, current + 1)` always has a valid range, because `lower ≤ current` always holds.
  - A valid header resets `next`, and a new `Backoff` is created for each exchange.
  - The first fallback delay is exactly `base`. When `pollingDelay ≥ 60000`, every fallback delay is constant. The jitter range is empty in these cases because the floor is `base`. This is intended: the docs say "up to 10%" jitter, and the policy suite pins it. Not a defect.
- [x] **Budget refunds and cancellation.** `ContentUnderstandingBudget.scala` lines 15–49.
  - `started += nanoTime() - mark` excludes only the throttled interval. Elapsed time was already charged, so remaining time cannot go negative.
  - `pause` checks for interruption first and sleeps in `CancellationPollMs` slices, so a huge Retry-After stays cancellable without overflow.
  - `checkInterrupted` (Protocol lines 269–276) covers both Spark task cancellation and thread interruption.
  - The nested `scala.concurrent.blocking` wrappers do nothing extra under an existing blocking context and are harmless.
- [x] **Retry eligibility and wiring.** `ContentUnderstandingProtocol.scala` line 107 (`Settings` default false), lines 329–346 (`exchange`), about 612–613 (poll `analysisRetry = analyzer.isEmpty`) and about 638 (submit `analysisRetry = true`).
  - A GET 429 is retried.
  - A POST 429 is retried only when `admissionRejected` holds, meaning complete headers and no `Operation-Location`.
  - A POST 429 with a handle, and responses that failed before dispatch with status 0, are not retried.
  - Management PUT and GET keep `analysisRetry = false`.
  - Every attempt rebuilds the request from `HTTPRequestData`, so the request body is replayable.
- [x] **Retry-After parsing.** `ContentUnderstandingResponseParsing.scala` lines 91–105.
  - Up to 19 ASCII digits are accepted; the value is converted to milliseconds with BigInt and capped at `Long.MaxValue`.
  - RFC 1123 dates give `max(0, t − now)`, so zero, past dates and very large values are honored exactly.
  - `retryDelay` (Protocol lines 278–279) behaves the same as before for the ordinary paths.
- [x] **`allowInputTruncation` data flow.**
  - `ContentUnderstandingParams.scala` lines 139–148 define the trait, which is mixed in only by `ContentUnderstandingParams`.
  - In `ContentUnderstanding.scala` it appears in `activeParams` for submit/analyze only (lines 84–92), so poll mode neither validates it nor reads its column.
  - It is typed as `BooleanType` (lines 168–178), and the `_: Boolean` pattern matches boxed `java.lang.Boolean` (line 189).
  - Row validation skips null column values (lines 272–278).
  - `CognitiveServiceBase.scala` lines 102–107 return `Option(row.getAs[T])` with the type parameter erased, so a null becomes `None` rather than an unboxed `false`.
  - `analyzeUrl` (lines 317–323) sends `value.toString`, so an explicit false sends `false`.
  - `buildUrl` sorts the query keys (lines 311–315), putting `allowInputTruncation` before `api-version`.
- [x] **Python inspection** (`core/src/main/python/synapse/ml/core/schema/Utils.py` lines 156–240). With the generic overrides removed:
  - Service parameters that live only on the JVM are no longer in `_paramMap`, so `isDefined` and `extractParamMap` skip them, and the MLflow parameter map does not include them.
  - The typed getters still read from the JVM.
  - `copy` uses the JVM copy plus `_transfer_params_to_java`, which routes pending service values through setters.
  - Loading skips service parameters set on the JVM.
  - `explainParam` still redacts credentials.
  - `retryThrottledRequests` is an ordinary `BooleanParam` with a false default (`ContentUnderstandingParams.scala` lines 151–157 and 175), so it remains visible.
- [x] **Notebook exclusion.**
  - `SharedNotebookE2ETestUtilities.scala` lines 100–103 strip spaces and hyphens from names, which yields `SemanticExtractionwithContentUnderstanding`.
  - That name matches `SynapseTests.ExcludedNotebooks` (lines 48–69).
  - The `DatabricksUtilities.CPUNotebooks` filter (lines 256–258) runs before the sorted modulo partitioning (lines 274–278).
  - `DatabricksUtilitiesSuite` lines 19–28 assert both exclusions.
- [x] **Docs match the code.** `docs/Explore Algorithms/AI Services/Content Understanding.md`:
  - Lines 375–399 cover opt-in scope, POST eligibility, budget exclusion and Retry-After or fallback semantics.
  - Lines 466–500 cover the bounds table.
  - Lines 510–513 cover the 19-digit rule.
  - Lines 551–556 say the per-request timeout is capped by the remaining budget, which matches `exchangeBeforeDeadline`.
- [x] **Earlier checks in this review.** I also checked the test assertions against the code and traced the arithmetic. For example, the policy suite's refund case leaves 600 ms remaining when enabled.
  - Also checked: the notebook's PDF offsets and the origin pre-check before writing a replacement snapshot.
  - Also checked: the guide test, where `errorifexists` fails during planning before any POST.
- [ ] **Not run by this reviewer.** I did not build or test anything, and no live CU calls were made. This review comes only from reading the code. The validation results come from the driver; I did not reproduce them.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, resolved from the current task model catalog",
    "stageBehavior": "Six concurrent independent lenses; human requested review after the fixes",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head. Most volume is tests and documentation; production changes are localized to CU request options, throttling and time accounting, and shared Python parameter inspection.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, fallback-only backoff, elapsed-budget refunds, credential inspection and durable notebook recovery interact with existing state machines.",
  "rationale": "Six complementary lenses cover the localized production changes and cross-connector inspection implications. The user explicitly requested another review after fixing the recovered findings. Retain all earlier failed evidence; a transport failure cannot count as signoff.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: when the fixes are in please start review after this. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 3,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The CLI JSON transport failure may recur. Offline evidence does not establish live CU compatibility or approval of matching packaged Python/JVM artifacts."
}

## Driver resolution update before stage attempt 4

The recovered response contained no findings, but failed transport still blocks
signoff. No original outcome was changed. A supported CLI update and synthetic
serialization probe succeeded; a fresh complete batch must validate the changed
source and actual review transport. See stage-attempt-3 lens 1's driver update
for the fix and validation record.
