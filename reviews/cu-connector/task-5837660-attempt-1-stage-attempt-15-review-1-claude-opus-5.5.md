---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 15
round: 1
timestamp: "2026-11-04T23:25:07Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ac7f0d8184666af564cf9e154f90bc225b6459d86d5d79efadda8a4e3236caa8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-15-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Submission recovery classification** checked against the spec in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`. Code read: the `exchangeBeforeDeadline` catch arms (398-413), `decodeSubmission` (538-556), `safeSubmission` (558-572), and `checkSharedFailure` / `uncertainSubmission` (514-535). Confirmed behavior:
  - HTTP 401/403/429 with no location stays `Rejected`, even when `transportFailure` is set.
  - A contradictory location makes the outcome `Unknown`.
  - Unsent DNS, connection and deadline failures are `Rejected`; deadline cases report `RequestNotSent`.
  - `raw.diagnostic.orElse(...)` keeps transport and response-size diagnostics.
  - A 404 whose body was cut off is `Unknown`, so it does not trigger the shared stop.
- [x] **Partition stop** in `rowHandler` (669-694):
  - It uses one `AtomicBoolean` per task, and poll mode is exempt.
  - It nests only `TransportError`, `OperationTimedOut` or `ResponseTooLarge`, and only when `rawResponse` is empty, so service messages cannot leak.
  - `FailureClassifier` (481-512) limits the shared auth stop to 401/403 responses that sent a scalar-credential fingerprint and carry no row-auth marker.
- [x] **Auth precedence**: `ServiceAuthHeaders.resolve` (`CognitiveServiceBase.scala` 376-436) sends exactly one auth header. CU's `rowUsesColumnAuth` and the scalar fingerprints in `settings` follow the same precedence. `addContentType` (358-361) replaces existing values case-insensitively.
- [x] **Session and resolver handling** in `ContentUnderstanding.scala`:
  - `transform` uses `resolverFor(dataset)`.
  - `getInternalTransformer` (352-353) and the inherited `transformSchema` use `SQLConf.get.resolver`.
  - `buildInternalTransformer` (387-413) works on a snapshot copy, checks output/error collisions with the resolver, and picks internal column names case-insensitively.
  - `validateParamSchema` (231-243) requires an exact name plus exactly one resolver match.
- [x] **Row validation**: `validateValue` (174-189) rejects null map values through its type check before any `toString` call. `parseUri` (Protocol 200-207) turns `URISyntaxException` into `IllegalArgumentException`, so `rowError` reports `InvalidInput` instead of failing the task.
- [x] **Python scalar view** files read:
  - `core\src\main\python\synapse\ml\core\schema\Utils.py` 61-215.
  - `Wrappable.scala` setters 185-218, getter 293-298 and `setParams` 490-505.
  - CU overrides in `ContentUnderstandingParams.scala` 156-262.
  - `test_ContentUnderstanding.py` 266-304.
- [x] **Security**:
  - The operation location must be same-origin and HTTPS (HTTP is allowed only for loopback); see `validateOperationLocation`, approximately lines 270-288.
  - Redirects and automatic retries are disabled.
  - `ContentUnderstandingHeaderValidation.scala` rejects control characters, non-Latin-1 values and connector-managed headers, without echoing values.
  - Row-auth markers are stripped before sending.
- [x] **DNS and deadlines**: `ContentUnderstandingDnsResolver.scala` holds each permit until the actual lookup finishes and uses a bounded worker pool. Deadline aborts are best-effort. A set `concurrentTimeout` is rejected in `validateConfiguration`.
- [x] **Docs and CI**:
  - `docs\Explore Algorithms\AI Services\Content Understanding.md` 224-250, 284-346 and 350-385.
  - `docs\Use with MLFlow\Autologging.md` 12-20 contains the credential and autologging warning.
  - The `pipeline.yaml` 821-822 entry was approved by the human.
- [x] **Tests read** (not executed): `ContentUnderstandingRecoverySuite.scala` 147-159 and `ContentUnderstandingConcurrencySuite.scala` 174-197.
- [ ] **Tests, builds and codegen were not executed.** Only read-only tools were available, and the validation results in the plan are reported by the caller.
- [ ] **External evidence is unavailable**: no external Azure CI, no live CU calls, and no fresh check against the remote base. This blocks a merge-ready claim but is not counted as a defect.
- [ ] **Unverified inputs**: the snapshot SHA256 (`de064985…92ec2`) came from the caller and was not hashed here. Git history was not checked, so whether Issue 3 predates this change is unknown.

## Issues

### Issue 1: A transient last poll hides the documented `OperationTimedOut` / `PollAttemptsExhausted` diagnostics
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`; `docs\Explore Algorithms\AI Services\Content Understanding.md`; `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingRecoverySuite.scala`
- **Line(s)**: Protocol 586-598, 614-617 and 623-624; docs 361-366; test 147-159
- **Description**: The code and docs disagree when the last poll was transient.
  - `retainPollState` copies a transient poll error into the retained state. Examples are a 503 or 429 body, or a polling `TransportError`.
  - When the budget has expired, `pollWithClient` returns `retained` unchanged if it already has an error.
  - On the final attempt, it adds `PollAttemptsExhausted` only when `current.error.isEmpty`.
  - So when the last poll was transient (for example, 503 with Retry-After), the row ends `Running`, `NotStarted` or `Unknown` with the transient code. It never gets `OperationTimedOut` or `PollAttemptsExhausted`.
  - The docs promise those codes unconditionally, while the recovery test asserts the opposite for budget expiry.
- **Risk**: Users or automation that pick resumable rows by those codes will miss operations that are still running and have saved handles. They may then resubmit instead of polling, which repeats billable work. The impact is limited because the status and handle are retained.
- **Suggested Fix**: Choose one of these:
  - Update docs 361-366 to say the last poll error takes precedence. Recommend selecting rows with `status IN ('NotStarted','Running','Unknown') AND operationLocation IS NOT NULL`.
  - Or always report the budget or attempt condition and nest the last poll error under `innererror`, then update the test.

  Either way, add a test for running out of attempts after a transient last poll.

### Issue 2: A definitive 401/403/429 rejection with an unreadable body says the request "may have been accepted"
- **Severity**: Low
- **File**: `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`; `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingTransport.scala`
- **Line(s)**: Protocol 407-412, 546-548 and 684-689; Transport 27-41 (message at line 37); `ContentUnderstandingConcurrencySuite.scala` 189
- **Description**: The row's status and its only diagnostic contradict each other.
  - When the status and headers arrive but reading the body throws `IOException`, `failure(..., notSent = false)` creates a POST diagnostic. It reads: `TransportError: "...HTTP transport failed. A submitted request may have been accepted; it was not resubmitted."`
  - `decodeSubmission` then correctly marks the row `Rejected`.
  - For a scalar-credential 401/403, `rowHandler` also nests this `TransportError` under `SharedConfigurationFailure`.
  - The result is `status=Rejected` with a message saying the request may have been accepted. Truly ambiguous `Unknown` rows use the same `TransportError` code.
- **Risk**: Operators, or automation that keys on `TransportError`, may treat definite rejections as possibly accepted. They could leave these rows out of retries or look for operations that do not exist. They could also learn to ignore the warning on rows that really are ambiguous.
- **Suggested Fix**: When a status line was received (`code != 0`) and the response is a location-less admission rejection, emit a separate retained diagnostic, for example `ResponseBodyReadFailed`: "the response status was received but the body could not be read". Then:
  - add that code to the `rowHandler` nesting allowlist;
  - update concurrency test line 189;
  - update docs 305-309.

### Issue 3: Generated ServiceParam getters ignore pending Python scalar values
- **Severity**: Low
- **File**: `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`; `core\src\main\python\synapse\ml\core\schema\Utils.py`
- **Line(s)**: Wrappable 293-298 and 490-505 (`self._set(**kwargs)` at 503); Utils.py 98-106; `CognitiveServiceBase.scala` 39-41; `test_ContentUnderstanding.py` 272-279
- **Description**: `get<Name>()` reads only from the JVM, while other Python APIs give pending values precedence.
  - Scalars set through the generic generated `setParams` or `Params.set`/`_set` stay only in `_paramMap` until they are transferred to the JVM.
  - `isSet`, `getOrDefault` and `extractParamMap` give these pending values precedence, but the generated getter calls `self._java_obj.get<Name>()` directly.
  - As a result, `AnalyzeDocument().setParams(subscriptionKey="k").getSubscriptionKey()` raises a Py4J error, because JVM `getScalarParam` does `.left.get` on an unset value.
  - If the JVM already holds an older value, the getter returns that stale value while `getOrDefault` returns `"k"`.
  - The test calls the getter only after `_transfer_params_to_java()` (lines 278-279).
  - CU's own `setParams` avoids this by calling the JVM setters, so the gap affects other generated wrappers and direct `set` calls.
- **Risk**: This breaks the spec rule "Pending Python scalar values retain precedence." Callers can crash or see stale values, including stale credentials, that disagree with `isSet` and `getOrDefault`.
- **Suggested Fix**: In the generated ServiceParam getter, return the pending `_paramMap` value first, otherwise read the JVM value. Add tests that check the getter before transfer and that a pending value overrides an older JVM scalar.

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

## Driver resolution update: attempt 15

Original findings and verdict are retained. Collection verified unchanged
source fingerprints, patch hash, HEAD and path set. All 125 Opus calls used
read/search tools with no prior/sibling feedback queries.

- **Issue 1 - documentation corrected, behavior preserved.** The last poll
  diagnostic continues to take precedence over timeout/attempt-limit codes.
  Documentation now qualifies both limit-code promises and selects saved
  resumable rows using a non-null operation handle plus
  `NotStarted`/`Running`/`Unknown`, not diagnostic code. A new regression covers
  the last attempt ending in 503, 429 or a truncated GET response in analyze and
  poll modes, verifying retained state, handle, original diagnostic and request
  counts. It passed against the pre-fix implementation, confirming this is a
  documentation correction. Existing budget-expiry coverage is retained.
- **Issue 2 - reproduced and implemented; final validation pending.**
  A location-less POST admission rejection whose body raises I/O now reports
  `ResponseBodyReadFailed`, with the received status and exception class but no
  possibly-accepted warning. Ambiguous submissions and GET transport failures
  keep their existing diagnostics. Shared authentication failures can nest the
  new connector diagnostic; service-provided lookalike codes still cannot leak
  service messages into the generic shared error. The rejection matrix covers
  401/403/429, scalar/column authentication, truncated/oversized bodies and
  subsequent rows, and now rejects the misleading wording. The updated
  regression failed before the fix. Oversized responses retain
  `ResponseTooLarge`. A named admission-rejection predicate is shared by
  diagnosis and classification; no lint suppression was introduced.
- **Issue 3 - reproduced and implemented; final validation pending.**
  Generated ServiceParam getters now return a pending Python value before
  consulting the JVM. Existing JVM getter/conversion behavior remains the
  fallback. Added coverage checks a fresh generic `setParams` value before
  transfer, pending values overriding old JVM scalars in CU and AnalyzeDocument,
  unchanged JVM state, and dictionary-valued headers. The red Python run
  reproduced the unset-JVM getter error and both stale-value failures.
  HEAD Wrappable.scala confirms direct JVM getters predate this change; the
  minimal correction is tightly coupled to the pending-value precedence
  contract and new shared scalar-inspection behavior.

Evidence: `cu-attempt15-scala-red.log` (three expected failures plus the preserved
polling behavior passing) and `cu-attempt15-python-red.log` (six failing subcases
and one error). Final Scala/codegen/publication and Python validation are pending.

### Completed local validation

All three findings are locally resolved, pending independent re-review.
`cu-attempt15-fixes-validation2.log` records core/cognitive main/test style and
affected compilation, 1/1 generated-wrapper and 98/98 CU tests, codegen and
publication passing. `cu-attempt15-python-fixed.log` records 22/22 Python tests
with current generated code and verified loaded JAR fingerprints. Full Black
passed. Polling behavior remains unchanged; its documentation and last-attempt
coverage now agree. No security setting or lint suppression was enabled.
No external CI, live CU request, commit, push or ADO edit ran.