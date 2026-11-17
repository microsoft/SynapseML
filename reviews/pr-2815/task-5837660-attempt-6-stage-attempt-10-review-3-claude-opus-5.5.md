---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 10
round: 3
timestamp: "2026-11-11T03:54:50Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "26583353a80ee86a50054fd6752d619a9da4296208143b06c3ec77eea77ec628"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-10-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Coverage:** I read every indexed current range for files 1–31, including all of new files 27–31, and compared them with the removed baseline. No range is unread.
- [x] **Backoff** (`P\ContentUnderstandingThrottling.scala` 67–102):
  - The fallback uses base = max(1000, pollingDelay) and cap = max(60000, base), doubles each time, and jitters with `nextLong(max(base, current - current/10), current + 1)`.
  - The first fallback is exactly base, and a base of 60000 or more is deterministic.
  - A valid header resets `next`, and each `retry` call creates its own Backoff.
  - Tests: ThrottlingPolicySuite 19, 66–67, 95–122 and 128–129.
- [x] **Retry loop** (Throttling 104–134):
  - With retry off, the attempt runs once, as before.
  - `refundSince(mark)` runs only on the retry branch.
  - `SubmissionStopped` is swallowed only when a POST is stopped. Other tick exceptions (cancellation, interrupt via Budget 32–54) propagate.
- [x] **Protocol `exchange`** (317–339):
  - Retry is enabled only for analysis calls with the opt-in set, so management calls are unchanged.
  - Retry applies only to GET 429, or POST 429 without Operation-Location. Line 361 reads the location with `getFirstHeader`, so an empty or repeated header still counts as present and nothing is replayed.
  - The shared-stop diagnostic is added only to an opted-in, stopped, throttled request. It nests the original diagnostic as `innererror`, and the response keeps its status and body.
- [x] **`rowHandler`** (Protocol 661–682):
  - Each deserialized task gets its own Context.
  - Once stopped, new non-poll rows are rejected while polls continue.
  - Only `ContentUnderstandingException` is caught (670–677). I grepped the file's `catch`/`NonFatal` sites (80, 167, 285, 368, 738, 763) and none wraps `exchange`.
- [x] **Delta seconds, budget and logging:**
  - `retryAfterDeltaMillis` (ResponseParsing 90–97) saturates, and `normalizeDeltaSeconds` (Throttling 73–84) handles zero padding.
  - The legacy `retryAfterMillis` (ResponseParsing 99–104) is unchanged and keeps its conversion inside `safely`.
  - Budget `refundSince`/`pause` is safe with `Long.MaxValue` and polls for cancellation every 50 ms.
  - ThrottleReporter (Logging +3,42 and +52,17) is synchronized and logs the first event, then at most once a minute including waits. It includes task IDs and no URLs, bodies or secrets.
- [x] **Requirement 3** (`Y\test_ContentUnderstanding.py` +169,149, about lines 172–232; Params +139,29 and +171,8):
  - A scalar `false` is sent; unset or null values are omitted.
  - Column values are forwarded, and poll mode ignores the setting.
  - The retry option is a separate trait, and the docs state there is no version allowlist.
- [x] **Requirement 4:**
  - The `Utils.py` overrides are removed (+147,12), along with the `Wrappable.scala` +468,8 and `PyCodegenSuite` +188,7 counterparts.
  - Getters, copy and persistence are tested in `test_ServiceParamPythonBridge.py` and `test_ContentUnderstanding.py` 1129–1525.
  - The `Autologging.md` 9–32 caveat is accurate: `LangchainTransform.py` 198 uses pure-Python Params.
- [x] **Requirement 5** (notebook 1–472 and tests in +489,446):
  - The origin, run ID and deployments are validated before any PUT, GET or write.
  - The analyzer is created eagerly with `allowReplace=False` and a ready check.
  - Results are written once with `errorifexists` and reloaded before display.
  - `modelDeployments` is sent only with analysis and omitted when empty.
  - Spans are sliced by code point, and missing confidence or grounding stays null.
  - There is no exactly-once claim.
- [x] **Requirement 6:**
  - The notebook is excluded in DatabricksUtilities about 254–262 and in SynapseTests 48–71, where the `SynapseTests` companion object has no class-initialization side effects.
  - SharedNotebookE2ETestUtilities 86–105 and DatabricksUtilitiesSuite 20–31 support the exclusion, and `sidebars.js` +36,7 adds the notebook.
- [x] **Test-gap grep** of ThrottlingPolicySuite (`GMT|invalid|malformed|asctime`): the malformed-date cases (116–122) cover only an invalid day, a PST zone and trailing text. No test covers year width or overflow.
- [x] **Optional notes, not counted as defects:**
  - `Retry-After: 0` repeated retries at network speed, which the requirements allow.
  - The notebook's `created.get("status","").lower()` raises on a null status before any submission.
  - A stop can race the next POST after a pause; this is allowed in-flight acceptance.
  - RFC 1123 leniency on offsets and weekday is shared with the legacy parser.
  - The positive `includeNotebook(new File("RegularNotebook.py"))` assertion depends on the working directory containing no excluded token.
- [ ] **Not executed:** I ran no commands or tests. The supplied validation (206 CU Scala tests, notebook tests, Black/whitespace, mutation runs, earlier codegen and Python counts) was not reproduced. Live CU, package builds and Databricks/Synapse runs were not exercised.

## Issues

### Issue 1: Opt-in HTTP-date Retry-After accepts out-of-grammar years, and extreme years throw `ArithmeticException` instead of using the fallback
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingThrottling.scala
- **Line(s)**:
  - Throttling: 23–24, 37–49, and 50–53 (`toEpochMilli` at 52). The code is reached from `Backoff.delay` (86–90) and `retry` (116).
  - The exception escapes through Protocol 317–339 and 661–682, where the catch at 670–677 does not cover it.

- **Description**: `httpDateMillis` wraps only the parsing in `Try`. The subtraction `instant.toEpochMilli - nowMillis` on line 52 runs outside it, and two parsers accept years that RFC 9110 forbids:
  - **RFC 1123 parser:** `RFC_1123_DATE_TIME` parses leniently with an optional weekday. Its year field (`appendValue(YEAR, 4)`) is not fixed-width in context, so it accepts 1–9 digit years, for example `1 Jan 999999999 00:00:00 GMT`, `01 Jan 10000 00:00:00 GMT` and `06 Nov 94 08:49:37 GMT`.
  - **Asctime parser:** `AsctimeDate` uses `uuuu` (EXCEEDS_PAD, width 4–19), so it accepts `+10000`…`+999999999` and `-0001`…`-999999999` when the weekday matches.

  Consequences:
  - `Instant.toEpochMilli` uses `Math.multiplyExact` and throws `ArithmeticException` when the year's magnitude exceeds about 292,278,994. The exception travels through `retry` and `exchange`, neither of which catches it, then `execute`. `rowHandler` catches only `ContentUnderstandingException`.
  - Years that do fit are treated as valid:
    - 5–9 digit years cause waits of millennia. The wait is refunded from the budget, and the shared stop does not cancel it for GETs.
    - Short or negative years cause an immediate 0 ms retry and reset the fallback.
    - Close to the negative overflow edge, the subtraction wraps to a huge positive wait.

  Requirement 2 and the docs ("Missing or malformed values use the adaptive fallback", Content Understanding.md about 522–534) both require these values to use the fallback. The legacy parser keeps its conversion inside `safely`, so this regression exists only on the opt-in path.
- **Risk**: One malformed header on an opted-in analysis GET 429, or on a POST 429 without Operation-Location, fails the Spark task. The failure is a bare exception with no CU diagnostic. Spark's task retry then replays the partition and resubmits POSTs that were already accepted and billed, before the job fails. Large but representable years stall the partition until the job is cancelled. Short or negative years remove the backoff. The likelihood is low, but the impact is high.
- **Suggested Fix**:
  1. Require the RFC 9110 shapes on the opt-in path only: exactly four unsigned year digits for IMF-fixdate and asctime, using anchored regexes or `appendValue(YEAR, 4)` instead of `uuuu`. Leave the shared legacy parser unchanged.
  2. Move the conversion and subtraction inside the `Try`, or use exact arithmetic that maps failure to `None`, so the opt-in parser can never throw.
  3. Add ThrottlingPolicySuite cases expecting `1000L` and then a jittered fallback for each of these:
     - `1 Jan 999999999 00:00:00 GMT`
     - `01 Jan 10000 00:00:00 GMT`
     - `06 Nov 94 08:49:37 GMT`
     - asctime `+10000` and `-0001`, with matching weekdays generated by the test
  4. Add one end-to-end ThrottlingSuite case showing the row completes instead of the task failing.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved locally; awaiting stage-attempt 11 independent review.
- **What changed**: Both opt-in IMF/RFC1123 and asctime parsers now require unsigned four-digit years. Date conversion remains in `Try`; past-date comparison precedes arithmetic, and positive exact-subtraction overflow saturates. Legacy parsing was not changed.
- **Why**: Short, signed and extended years are malformed HTTP dates and must use the adaptive fallback. They must not reset it, throw, or wrap into very long waits.
- **How verified**: The new policy tests reproduced an epoch overflow and a subtraction wrap before the fix. All seven malformed-year cases now use first/follow-up fallback; extreme clocks and actual submit/poll exchanges are covered. Aggregate compile/test-compile/style and 208 CU Scala tests passed (`cu-loop-s11-scala.log`). The three affected Python files passed 81 tests and 213 subtests; Black/whitespace passed. Logs are retained in the session workspace.