---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 4
round: 2
timestamp: "2026-11-10T22:00:17Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "8d0047a3a4a1aa6c90f7dd97bbf5ab5650d9560ba8fe9747b0f55b1552ef8295"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-4-review-2-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Conventions.** I read the repository AGENTS.md guidance and `.github\skills\scala-code\SKILL.md`. The stage contract is intact in `ContentUnderstanding.scala`:
  - ComplexParamsReadable companion at :34, `logClass` at :52, `defaultCopy` at :62, `logTransform` at :66–71.
  - The new params are additive and have defaults (`ContentUnderstandingParams.scala:140–161`).
  - Only package-private internals changed shape (`Settings`, `exchange`, `ContentUnderstandingBudget`). No public JVM signature or serialized parameter shape breaks.
- [x] **`allowInputTruncation` follows the existing URL-param pattern.**
  - It uses `isURLParam = true` like `stringEncoding` and `processingLocation` (`ContentUnderstandingParams.scala:77, 85, 143`).
  - `toValueString` matches the base `prepareUrl` (`CognitiveServiceBase.scala:482–489`). The CU `prepareUrl` override is `analyzeUrl` (`ContentUnderstanding.scala:317–326`).
  - It is kept out of the JSON body (`requestBody` :299–309). Its Boolean type is validated at :171–172 and :189.
  - Tests: `ContentUnderstandingOptionsSuite.scala:24–68`, `ContentUnderstandingFuzzingSuite.scala:20`, `test_ContentUnderstanding.py:174–224`.
  - The retry opt-in is its own `HasRetryThrottledRequests` trait with default false (:151–161), separate from `ContentUnderstandingLimits`.
- [x] **Protocol traced end to end** in `ContentUnderstandingProtocol.scala`: `exchange` :314–335, `exchangeBeforeDeadline` :337–391, `decode` :413–447, `checkSharedFailure` :481–497, `decodeSubmission` :508–524, `pollWithClient` :582–618, `execute` :620–649, `rowHandler` :657–682.
  - Management `exchange` calls (:701, :749) keep retry disabled.
  - This matches `ContentUnderstandingConcurrencySuite.scala:193–222` and `ContentUnderstandingThrottlingPolicySuite.scala:69–99`.
- [x] **Throttling policy checked against the human requirements.**
  - `ContentUnderstandingThrottling.scala` `Backoff` :24–41 covers exact Retry-After, base and cap, and the jitter floor at base.
  - `retry` :43–73 refunds only the throttled attempt and its wait (`ContentUnderstandingBudget.scala:18–23`). The partition stop halts POST retries, including their waits.
  - Tests: `ContentUnderstandingThrottlingPolicySuite.scala:16–67, 102–119` and `ContentUnderstandingThrottlingSuite.scala:241–272`.
  - `pause` uses the real clock while the rest of Budget uses the injected one. This is intentional and not flagged.
- [x] **Partition scope and logging.**
  - A `Context` is created per deserialized handler closure (`rowHandler` :658–659; core `HTTPTransformer.scala:24–37, 107–118`, `SharedVariable.scala:17–23`).
  - Owning task IDs reuse core `AsyncClient.currentTaskContext` (`Clients.scala:50–92`) via `ContentUnderstandingLogging.scala:38–40`.
  - Warning text (:55–67) contains no URL, header, credential or body.
  - `concurrentTimeout` is still rejected (`ContentUnderstanding.scala:114–115`), so unlimited retries cannot race a shared async wait.
- [x] **Searches limited to `cognitive\src\main\scala` and `cognitive\src\test\scala`.**
  - `TooManyRequests`: Protocol :50 has no references; ResponseParsing :22 is a private copy; the test fixture is `ContentUnderstandingStub.scala:48`.
  - `stopSubmission` and `.stopped` users: Throttling :19, :54, :59, :66; Protocol :332, :660; ThrottlingSuite :266.
  - Direct test calls: `exchange` (RecoverySuite :776), `retry` (PolicySuite :57), `pollWithClient` (AdmissionSuite :567).
  - Nested `blocking` checked against core `HTTPClients.scala:460–462, 468–470`. Scalastyle limits checked at `scalastyle-config.xml:3–4, 52–59`.
- [x] **Docs and Python consistency.**
  - `docs\Explore Algorithms\AI Services\Content Understanding.md`: shared failures :282–290 and :412–457, opt-in :375–404, setter table :492–502, logging :504–520, MLflow :146–160. Also `docs\Use with MLFlow\Autologging.md:12–22`.
  - There is no hand-written CU Python. The generated wrapper hooks (`ContentUnderstandingParams.scala:184+`) and helpers (`Wrappable.scala:324`, `Utils.py:130–178`) are still used. Generic-inspection test: `test_ContentUnderstanding.py:1049–1078`.
  - Notebook exclusions and the docs link are consistent: `DatabricksUtilities.scala:257–258`, `SynapseTests.scala:49–69`, `SharedNotebookE2ETestUtilities.scala:100–103`, `website\docusaurus.config.js:17`.
- [x] **Independence disclosure.** I did not read `reviews\pr-2815`, run directories, or other reviewers' output. One early `glob` for `scalastyle*.xml` was accidentally run from the checkout root. It returned only scalastyle config and result XML paths. I saw no review artifacts or feedback.
- [ ] **Build and test execution not performed.** This is a read-only lens and I ran no commands. The review relies on source inspection and the driver-supplied offline validation evidence. I do not claim any tests were run.

## Issues

### Issue 1: Throttled POSTs get the shared-stop label even when retry is off
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 322–334 (relabel at 332–334); related 311–312, 446, 659–670
- **Description**:
  - **The mismatch.** Retry is enabled only when `analysis && settings.retryThrottledRequests` (322). The relabel after it, `if (analysis && raw.throttled(method) && context.stopSubmission(method)) raw.copy(diagnostic = Some(SharedStopError))` (332–334), leaves out the opt-in check.
  - **When it fires in default mode.** Retry is off and `setConcurrency > 1`. A sibling row's `ContentUnderstandingException` sets the partition flag (`rowHandler` :670) while this row's POST is in flight. When this row then gets a definitive 429, its diagnostic is replaced.
  - **What the row ends up with.** `decode` prefers `raw.diagnostic` (446), so the service's `TooManyRequests` error (or `HttpError`) becomes `SharedConfigurationFailure`, whose message says the failure "stopped new submissions in this partition". In default mode there was no retry to stop; the row was sent and throttled.
  - **Deterministic path (by inspection).** Set `Context.stopped` before calling `ContentUnderstandingProtocol.execute(settings /* retryThrottledRequests = false */, post, "submit", context = c)` against a stub that returns 429 `{"error":{"code":"TooManyRequests"}}`.
  - **Root cause.** `exchange` re-derives "was the retry stopped?" from shared mutable state after the fact. `ContentUnderstandingThrottling.retry` is the only component that actually knows.
  - **Test gap.** Only the opt-in variant is tested (`ContentUnderstandingThrottlingSuite.scala:241–260`). The default-mode 429 tests run without a stopped context: `ContentUnderstandingSuite.scala:256–266, 355–366` and `ContentUnderstandingConcurrencySuite.scala:354–378`.
- **Risk**:
  - The docs say the default `False` preserves prior behavior (Content Understanding.md:377–379).
  - The docs define `SharedConfigurationFailure` only for the triggering row and for unsent rows with HTTP 0 (:286–290). This change adds a race-dependent third case in default mode.
  - It hides the throttling signal. The docs give different advice for throttling (:434, reduce concurrency) and for configuration failures (:438, fix configuration).
  - Impact is limited to diagnostic accuracy. Status `Rejected`, HTTP 429 and `rawResponse` are kept, and the retry-selection SQL (:450–457) still selects the row.
- **Suggested Fix**:
  - Gate the relabel on the opt-in: `analysis && settings.retryThrottledRequests && ...`.
  - Better, have `retry` report that it suppressed a retry, for example by returning a stopped flag or taking an `onStopped` hook, so the relabel happens only then.
  - Add a default-mode test with a pre-stopped context that asserts the `TooManyRequests` diagnostic is kept.
  - In the opt-in docs (:388–390), state that a suppressed retry reports `SharedConfigurationFailure` while keeping HTTP 429 and its raw response.

### Issue 2: Implicit `analysis`/`context` defaults and a leaky partition-state abstraction
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala (also ContentUnderstandingThrottling.scala, ContentUnderstandingThrottlingSuite.scala, ContentUnderstandingRecoverySuite.scala)
- **Line(s)**: Protocol 318–320, 329, 590–591, 603–604, 625–626, 631, 659–670, 701, 749; Throttling 16–20, 43–44; ThrottlingSuite 265–266; RecoverySuite 776–777
- **Description**:
  - **One flag, two jobs.** `analysis: Boolean = false` controls throttle-retry eligibility (322) and management-GET transport wording (`!analysis && method == "GET"`, 329). Its default selects management behavior.
  - **Callers must remember it.** Management call sites rely on the default (701, 749). Analysis call sites must pass `analysis = true` explicitly (604 derives it from `analyzer.isEmpty`; 631 sets it directly).
  - **`context` defaults at four layers.** `exchange` (319–320), `pollWithClient` (590–591), `execute` (625–626) and `retry` (Throttling :44) all default to a fresh `Context`. A caller inside a partition that forgets to pass it silently loses the partition stop and the reporter rate limit, with no compile error.
  - **Existing example.** RecoverySuite:776 already calls `exchange` with defaults on an analysis-result URL (`locationPath`), so it runs with management behavior. This is harmless for its 200-only assertions but shows the hazard.
  - **Leaky state.** `Context` exposes a raw `AtomicBoolean`. `rowHandler` reads it and compare-and-sets it (660, 663, 670), and tests set it directly (ThrottlingSuite:266).
  - **Placement.** The partition-wide submission stop, which `rowHandler` uses for non-throttling reasons, now lives in `ContentUnderstandingThrottling`.
  - **Minor.** `val sharedError = SharedStopError` (661) is a redundant alias.
- **Risk**: There is no current functional defect. Future analysis call paths could silently lose 429 recovery, use the wrong transport-failure wording, or get an independent stop and reporting scope. The type that owns the stop flag does not enforce its invariants.
- **Suggested Fix** (optional, scoped):
  - Replace the boolean with a small sealed kind (Analysis/Management), or make it required at the internal `exchange`/`pollWithClient` boundary.
  - Keep a default only on test-facing `execute` if independent scopes should remain documented (Content Understanding.md:515–516).
  - Wrap the flag as `stop(): Boolean` / `isStopped` on `Context` (or a partition-state type), and update ThrottlingSuite:266.
  - Pass an explicit kind in RecoverySuite:776.
  - Drop the alias at 661.

### Issue 3: Dead and duplicated 429 constant; status policy moved into a parsing object
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala; ContentUnderstandingResponseParsing.scala
- **Line(s)**: Protocol 49–50; ResponseParsing 19, 22–45
- **Description**:
  - **Dead constant.** Moving `TransientStatuses`/`AdmissionRejections` out of Protocol left `val TooManyRequests = 429` (Protocol:50) with no references in main or test code. Its only users were the removed sets, and tests use `ContentUnderstandingFixtures.TooManyRequests` (Stub.scala:48).
  - **Duplicates.** ResponseParsing:22 adds a private copy of the 429 constant. `MillisecondsPerSecond` is also duplicated (Protocol:49 public, ResponseParsing:19 private).
  - **Cohesion.** `WireResponse`, a transport result, and the HTTP status policy now sit in `ContentUnderstandingResponseParsing`. The moved policy is `TransientStatuses`, `AdmissionRejections`, `admissionRejected`, `retryable` and `throttled`. The object's other job is bounded JSON and header parsing.
  - **Existing alternative home.** `ContentUnderstandingTransport` already owns transport-failure classification (`failure`, `hasStartedWriting`).
  - **Likely motivation.** The move seems driven by the scalastyle `maxFileLength` limit of 800 (`scalastyle-config.xml:3–4`; Protocol is about 783 lines), not by a module boundary.
- **Risk**: There are two sources of truth for the same status code, which can drift. Admission and retry policy is harder to find in a parsing utility.
- **Suggested Fix**:
  - Delete Protocol:50, or keep a single shared constant.
  - Optionally move `WireResponse` and the status sets and predicates to `ContentUnderstandingTransport` or a small dedicated wire object. Keep ResponseParsing for parsing only, which still satisfies the file-length cap.

### Issue 4: `pause` callback is also used to abort the wait; redundant `blocking`
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingBudget.scala (also ContentUnderstandingThrottling.scala, ContentUnderstandingProtocol.scala)
- **Line(s)**: Budget 28–53 (`blocking` at 30, `onTick` at 32 and 39); Throttling 22, 57–64; Protocol 329
- **Description**:
  - **Hidden abort contract.** The generic budget primitive `pause(delay, onTick: () => Unit)` has an undocumented contract. Throttling passes a callback that both logs (`reporter.waiting`) and aborts the wait by throwing a private `ControlThrowable` (`SubmissionStopped`), which the caller then catches (57–64).
  - **What correctness relies on.** `pause` must catch only `InterruptedException`, and `ControlThrowable` must keep bypassing `NonFatal` and `blocking`.
  - **Redundant `blocking`.** The new wrappers (Budget:30, Protocol:329) sit inside the `blocking` already applied by `AsyncHTTPClient.handle` and `SingleThreadedHTTPClient.handle` (core `HTTPClients.scala:460–462, 468–470`). Scala 2.12 nested `blocking` runs under the default BlockContext, and non-pool callers get the default context too, so in current call paths the wrappers do nothing.
- **Risk**: There is no current defect. A future `try`/`finally`, `NonFatal` handler or `safely` wrapper in `pause` could swallow or reorder the abort. The extra `blocking` implies a scheduling guarantee it does not provide.
- **Suggested Fix**:
  - Give `pause` an explicit stop predicate, for example `pauseUntil(delay, shouldStop: () => Boolean): Boolean`, which returns whether the full wait elapsed.
  - Report from the loop or predicate instead of throwing.
  - Remove the inner `blocking` wrappers, or document why they are needed.

### Issue 5: `pollingDelay` param doc is ambiguous about when fallback delays are exact
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 117–118 (compare Content Understanding.md:396–400 and :499, and ContentUnderstandingThrottling.scala:35–38); optionally ContentUnderstanding.scala:114–115
- **Description**:
  - **Ambiguous wording.** The generated public doc says "Downward jitter applies above the starting delay; the first fallback and delays of at least 60000 are exact."
  - **Why the natural reading is wrong.** With the default `pollingDelay` of 1000, fallback grows to the 60000 ms cap. The capped delay is drawn from [54000, 60000] (`lower = max(base, current - current / 10)`, Throttling:37–38). So reading it as "fallback delays of at least 60000 are exact" is wrong.
  - **What it means to say.** The intended rule is that a `pollingDelay` of at least 60000 gives a constant, unjittered fallback. The website prose (:396–400) and setter table (:499) already say this precisely.
  - **Related.** The `concurrentTimeout` rejection message, "maxOperationDuration bounds each operation" (ContentUnderstanding.scala:115), is no longer fully true when the opt-in excludes throttled waits from that budget.
- **Risk**: The generated Scala/Python API docs disagree with the website on a policy the human asked to make explicit. Users may expect unjittered 60 s waits at the cap.
- **Suggested Fix**: Reword to match the website: "The first fallback is exact; a pollingDelay of at least 60000 gives a constant, unjittered fallback." Optionally qualify the `concurrentTimeout` message for the opt-in, then regenerate codegen.

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

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Resolution Update (supersedes the pending entries above)

- Issue 1: implemented the retry-opt-in guard and diagnostic nesting; added
  stopped-context tests for readable, truncated and oversized POST 429s.
- Issue 2: selected the suggested minimal boundary fix: internal `exchange`,
  `pollWithClient` and `retry` require explicit context, and `exchange` requires
  the analysis flag. Test-facing `execute` retains its independent default.
  `Context` now owns a private atomic behind `stop()`/`isStopped`; the predicate
  is named `submissionStopped`. Removed the redundant shared-error alias.
- Issue 3: deleted the unused Protocol 429 constant. Retained the existing
  WireResponse/parsing placement and local time-conversion constants: moving
  them again supplies no behavioral correction and would expand this fix.
- Issue 4: documented callback exception propagation and added a deterministic
  test that fails if an in-wait stop is swallowed or deferred until after the
  wait. Kept the callback shape rather than adding a second waiting abstraction.
  Kept explicit `blocking` boundaries for direct callers outside the HTTP
  handlers; nesting is not claimed to add another scheduling guarantee.
- Issue 5: clarified fallback, transient-failure attempt accounting, and the
  opted-in duration exception in the concurrentTimeout rejection message.
- Optional broader architecture changes above are explicitly declined as
  non-defect refactors, not silently reported as implemented. Validation and
  stage collection are recorded in the lens-1 driver section.

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
