---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 9
round: 4
timestamp: "2026-11-11T03:22:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "2e8ed87c94f22b56f14a7c07fdf08f87694fd77b49282b29cff1691d4eae17e2"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-9-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage.** I read every indexed current hunk range in all 31 files: P, T, Y, C, U, R, D, `docs\Use with MLFlow\Autologging.md`, `website\sidebars.js`, the new Throttling, Options and ThrottlingPolicy files, and the notebook. I also checked them against the removed-baseline lines. A second line-by-line pass re-read `ContentUnderstandingThrottling.scala` 1–91, `ContentUnderstandingBudget.scala` 1–54, `ContentUnderstandingLogging.scala` 1–65, `ContentUnderstandingTransport.scala` 1–50, `ContentUnderstandingResponseParsing.scala` 60–112 and `ContentUnderstandingProtocol.scala` 300–800. I read no review or run directories.
- [x] **Requirement 1, retry gating.**
  - `WireResponse.throttled` (Transport 35–36) is true only for HTTP 429 where the method is GET, or where `admissionRejected` holds: POST with no `Operation-Location` at all (21–22).
  - The location comes from `Option(getFirstHeader)` (Protocol 361), so an empty header is still `Some`. That response becomes `Unknown` through `uncertainSubmission` (502–510) and is never replayed.
  - `retryEnabled = analysis && settings.retryThrottledRequests` (Protocol 325).
  - Call-site analysis flags:
    - execute POST: `true` (635)
    - analysis poll: `analyzer.isEmpty` (607–608)
    - management GET: `false` (703)
    - management PUT: `false` (754)
  - The old `managementGet` values match `!analysis && method == "GET"` at all four sites.
  - When retry is disabled, the path is `attempt` alone (Throttling 63–64) with no post-processing (Protocol 336–338).
- [x] **Requirement 2, Retry-After.**
  - `normalizeDeltaSeconds` (Throttling 33–44) trims, strips leading zeros, maps all-zero to `"0"`, and maps more than 19 significant digits to `Long.MaxValue`.
  - `retryAfterDeltaMillis` (ResponseParsing 90–97) saturates through `BigInt.min`, so a long value never falls back early.
  - HTTP dates in the past clamp to 0 (102). The 4-digit RFC 1123 year rules out overflow.
  - The legacy `retryDelay` (Protocol 261–262) keeps the 19-digit limit.
- [x] **Requirement 2, fallback backoff** (Throttling 46–57).
  - `base = max(1000, pollingDelay)`, `cap = max(60000, base)`, and `next = min(cap, 2*current)`. This cannot overflow because the input is an Int.
  - Jitter is drawn from `nextLong(max(base, current - current/10), current + 1)`. Since `origin < bound` always holds, the first delay is exactly `base`, and the delay is constant when `base >= 60000`.
  - A valid header resets `next`. A new `Backoff` is created per exchange (66).
- [x] **Requirement 2, budget and cancellation.**
  - `refundSince` (Budget 22–24) freezes elapsed time at the mark. The loop refunds each throttled attempt and its wait, and charges the final attempt (Throttling 69–83).
  - Poll attempts are decremented once per exchange (Protocol 617).
  - `pause` (32–54) is not capped by the budget. It checks `checkInterrupted` (a `CancellationException` or `InterruptedException`) and `onTick` on every 50 ms slice.
  - `SubmissionStopped` is a `ControlThrowable`. It is caught only at Throttling 79–80, and neither `NonFatal` nor the pause's `InterruptedException` catch intercepts it.
- [x] **Requirement 2, shared stop.**
  - `submissionStopped` applies only to POST (Throttling 22), and `stopped` never resets, so the checks at 71, 83 and Protocol 336 agree.
  - When a throttled POST is suppressed, the diagnostic becomes `SharedConfigurationFailure` with any read error nested as `innererror` (310–315). The status and body are kept.
  - `decodeSubmission` (521–522) then gives `Rejected`, and `checkSharedFailure` (485–500) does not misclassify it.
  - GET polling and in-flight accepted submissions continue. `rowHandler` (661–682) creates one Context per deserialized task closure, the same pattern as the baseline `AtomicBoolean`.
- [x] **Requirement 2, logging.** `ThrottleReporter` (Logging 15–36) is synchronized. It logs the first retry, then at most once per 60 s, and wait ticks are included. Messages contain the method, retry count, wait time and task IDs only. Logging failures are swallowed by `safely`, while the tick's stop signal still propagates.
- [x] **Requirement 3.**
  - `HasAllowInputTruncation` is a URL `ServiceParam[Boolean]`, separate from the `HasRetryThrottledRequests` BooleanParam that defaults to false (Params 139–163).
  - Poll mode excludes it (ContentUnderstanding 83–91). In `analyzeUrl` (318–325), null is omitted because `getValueOpt` wraps the value in `Option`, and false is sent through `toValueString`.
  - There is no API-version allowlist.
- [x] **Requirement 4.**
  - The Utils.py `isSet`, `hasDefault`, `getOrDefault` and `extractParamMap` overrides are removed. `explainParam` redaction, `clear` and the transfer functions remain (Utils 147–237).
  - The Wrappable `isSet` exclusion is removed, and PyCodegenSuite 188–191 and ContentUnderstandingSuite 639–642 assert that no `def isSet(` is generated. No stage defines its own `isSet`.
  - The Autologging caveat is accurate.
- [x] **Requirements 5 and 6.**
  - The notebook validates deployments before eager `createAnalyzer`. The dict is serialized by the wrapper at Params 287–291, and an empty mapping is omitted.
  - It submits with `errorifexists` and reloads before display. Recovery checks handle origins before polling or writing, makes no mapping or creation calls, and makes no exactly-once claim.
  - Guide lines 375–434 and 513–529 match the code.
  - Unattended notebook exclusions and sidebars were verified (DatabricksUtilities 254–261, SynapseTests 45–80, SharedNotebookE2ETestUtilities 86–105).
- [x] **Rejected as non-defects or optional preferences.**
  - Notebook rows with a null status but a handle are excluded by `~resumable`; the connector always sets a status.
  - `Retry-After: 0` gives tight but cancellable retries, which the requirement explicitly allows.
  - The race between the stop check and dispatch is the same as in the baseline.
  - Bookkeeping time between retries is charged but negligible.
  - Two `onTick` calls on pause entry are idempotent.
- [ ] **Not run by me:** builds and tests. This is a read-only review, so I relied on the supplied evidence: compile, test-compile and style; 203 CU Scala tests; 3 Python regressions; Black; and the Retry-After regression (failed before the fix, passed after).
- [ ] **Not exercised:** live Content Understanding, packaging, and the Databricks, Synapse and Fabric runners. No live evidence was supplied.