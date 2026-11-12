---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 7
round: 5
timestamp: "2026-11-11T02:05:46Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a042860ad3031fd17aa644b4d9462c55c296c27bfc340b7dca743ea455a0d347"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-7-review-5-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Scope.** I read every current hunk range in the index. The index was cut off after the `D\Content Understanding.md` hunk at +647, so I found and read the remaining files myself:
  - `ContentUnderstandingThrottling.scala`
  - `ContentUnderstandingThrottlingSuite.scala`
  - `ContentUnderstandingThrottlingPolicySuite.scala`
  - `ContentUnderstandingOptionsSuite.scala`
  - the quickstart notebook
  - `docs\Use with MLFlow\Autologging.md`
  - `website\sidebars.js`

  I did not read `reviews\pr-2815` or any review run directory.

- [x] **Requirement 1: retries only for throttled analysis requests.**
  - Code:
    - `P\ContentUnderstandingProtocol.scala` 317–340 retries only when the call is an analysis call and `retryThrottledRequests` is on.
    - `P\ContentUnderstandingResponseParsing.scala` 28–29 and 42–43 treat a response as throttled only for a GET 429, or a POST 429 with no `Operation-Location`.
    - `P\ContentUnderstandingParams.scala` 153–162 is a separate trait, `HasRetryThrottledRequests`, with default false.
  - Tests:
    - `T\ContentUnderstandingSuite.scala` 196–211 and 355–372: with the default, a POST 429 sends one request and returns `Rejected` with the 429 body kept; a GET 429 uses up a poll attempt.
    - `T\ContentUnderstandingThrottlingSuite.scala` 29 and 47: when opted in, a POST keeps retrying past a 1000 ms budget; two GET 429s with `maxPollAttempts=1` produce three GETs.
    - `T\ContentUnderstandingThrottlingSuite.scala` 111: a valid or invalid `Operation-Location`, a 500, a 408 and a disconnect are never replayed.
    - `T\ContentUnderstandingThrottlingSuite.scala` 196: 401, 403 and 503 behave the same with retries on or off.
    - `T\ContentUnderstandingThrottlingPolicySuite.scala` 102–133: the management PUT, the creation poll and the analyzer GET are never retried.
    - `T\ContentUnderstandingConcurrencySuite.scala` 195–206 runs each case with retries off and on.

- [x] **Requirement 2: Retry-After handling and fallback backoff.** The code is in `ContentUnderstandingThrottling.scala` 27–44.
  - `ContentUnderstandingThrottlingPolicySuite.scala` 16–34 covers:
    - the 1000 ms floor;
    - the second delay falling in [1800, 2000];
    - the 60000 ms cap;
    - a 30000 ms polling delay giving 30000 first, then values in [54000, 60000];
    - fixed delays when the polling delay is 60000 or 120000.
  - `ContentUnderstandingThrottlingPolicySuite.scala` 60–74 covers:
    - exact, uncapped header values of 0, 120000 ms and `Long.MaxValue`;
    - a past date giving 0, and a future date within the expected bounds;
    - the fallback resetting after a valid header.
  - `ContentUnderstandingThrottlingSuite.scala` 62 and 218 cover the reset for each new request: waits of 1000/1000/0/2000, and a POST followed by a GET giving 1000, [1800, 2000], 0, 1000.
  - `T\ContentUnderstandingSubmissionSuite.scala` 682–695 shows the refactored `retryDelay` behaves as before for:
    - 19- and 20-digit values;
    - a non-ASCII digit, `+5`, extra whitespace and an empty value;
    - a whitespace-padded HTTP date.

- [x] **Budget, refunds, cancellation and ticks.** The code is in `P\ContentUnderstandingBudget.scala` 16–55.
  - `ContentUnderstandingThrottlingPolicySuite.scala`:
    - 36–58: a tick fires on entry and during waits; an exception thrown at tick 3 reaches the caller unchanged.
    - 76–100: the throttled attempt and its wait are refunded and the final attempt is charged, leaving 600 ms with retries on or off.
    - 154–176: a shared stop ends the current wait, and only throttled work is refunded, leaving 700 ms.
  - `ContentUnderstandingThrottlingSuite.scala`:
    - 140–153: the normal `maxPollAttempts` and `maxOperationDuration` limits still apply.
    - 155: an interrupt works during both submit and poll waits.
  - `Y\test_ContentUnderstanding.py` 311–440: Python cancellation works during throttled waits with `Retry-After: 60`.

- [x] **Shared stop, diagnostics and logging.**
  - `ContentUnderstandingThrottlingSuite.scala`:
    - 241: a 404 on another row ends a 60 s wait, and both rows report `SharedConfigurationFailure`.
    - 261: a real wait stops at tick 3, with one POST and one warning.
    - 293: GETs for accepted work continue after a stop.
    - 306: one log event per partition.
    - 326: the shared-stop label is applied only when opted in. It keeps the 429 status, the raw body and `TooManyRequests`, keeps `ResponseBodyReadFailed` and `ResponseTooLarge` under `innererror`, and sends one request.
    - 90: no key, endpoint or body is logged.
  - `ContentUnderstandingThrottlingPolicySuite.scala` 135–152 and 178–200: one event first, then at most one per `ThrottleReportIntervalMs`, including during one long wait.
  - `ContentUnderstandingSubmissionSuite.scala` 328–352 now requires `stageId`, `partitionId` and `taskAttemptId` at concurrency 1 and 2. This is stricter than the removed check, which accepted `taskContext=unavailable`.

- [x] **Requirement 3: `allowInputTruncation`.**
  - Code: `P\ContentUnderstanding.scala` 84–92, 172–173 and 320–324, plus a separate trait in `ContentUnderstandingParams.scala` 142–150.
  - `T\ContentUnderstandingOptionsSuite.scala`:
    - 24: on GA and preview API versions, true and false are sent and a cleared value is omitted; the option goes in the query string, not the body.
    - 44: a column value of true or false is sent, null is omitted, and a non-boolean column is rejected.
    - 62: poll mode ignores a missing column.
  - `Y\test_ContentUnderstanding.py` 172–222 covers the Python getters, copy, save, load and clear, and checks the exact query strings.
  - `T\ContentUnderstandingFuzzingSuite.scala` 20 adds both new parameters to the serialization fixture.
  - There is no API-version allowlist.

- [x] **Requirement 4: Python parameter inspection.**
  - Code: `U\core\schema\Utils.py` 147–166 removes the `isSet`, `hasDefault`, `getOrDefault` and `extractParamMap` overrides. The `_java_service_value` helper is now used only by `explainParam`.
  - `Y\test_ContentUnderstanding.py` 1163–1531 covers:
    - no JVM lookups during inspection;
    - `KeyError` for values set only on the JVM;
    - scalar values set in Python but not yet sent to the JVM;
    - getters, copy, save and load;
    - `explainParam` redaction and column bindings.
  - `Y\test_ServiceParamPythonBridge.py` 126–243 covers the shared Python bridge cases.
  - `Y\openai\test_OpenAIResponseSchema.py` 87–88 now checks both sides: unset in Python, set on the JVM.
  - `T\ContentUnderstandingSuite.scala` 641 and `R\codegen\PyCodegenSuite.scala` 190–191 guard the generated Python code.
  - `C\codegen\Wrappable.scala` 471 changes only a comment.

- [x] **Requirement 5: guide and notebook.**
  - `Y\test_ContentUnderstanding.py` 492–660 runs the Python blocks in the guide:
    - a second `errorifexists` write raises `AnalysisException` and sends no new requests;
    - the analyzer is created eagerly.
  - `Y\test_ContentUnderstanding.py` 662–918 runs the notebook cells:
    - the cell IDs match the expected set;
    - invalid configuration is rejected before any request, and a creation that is not ready raises an error;
    - one POST is sent;
    - recovery from the wrong origin is rejected before any call or write;
    - results are reloaded before display;
    - the result fields are read without the SDK;
    - `modelDeployments` is sent only with analysis and omitted when empty.
  - The notebook JSON at lines 255–370 uses `errorifexists` and then reloads.
  - The `custom = ` block in the guide contains no mapping or re-creation logic.
  - `Autologging.md` 11–30 explains the difference between JVM-backed and pure-Python stages.

- [x] **Requirement 6: notebooks excluded from unattended runs.**
  - `R\nbtest\DatabricksUtilitiesSuite.scala` 20–32 checks the CPU notebook list, every partition, the generated Synapse name, and a notebook that should still run.
  - It tests against `R\nbtest\SynapseTests.scala` 44–82, `R\nbtest\DatabricksUtilities.scala` 257–258 and `R\nbtest\SharedNotebookE2ETestUtilities.scala` 89–91; the test reuses `generatedNotebookName` from that last file.

- [x] **Removed test code.**
  - `T\ContentUnderstandingRecoverySuite.scala` 773–779 now calls `exchange` directly, which it can because `exchange` is `private[contentunderstanding]`, instead of through reflection. It keeps the status, body, diagnostic, `ResponseTooLarge` and close-log assertions.
  - `T\ContentUnderstandingAdmissionSuite.scala` 565 only follows the new method signature.
  - The removed Python assertions tested the deleted overrides; each was replaced by an assertion that the value is no longer exposed in Python, or by a check on the JVM side.

  No behavior that still exists lost coverage.

- [x] **Optional notes, not defects.**
  - A blank `Operation-Location` on a POST 429 goes through the same presence check as the tested `"invalid"` value.
  - `ContentUnderstandingThrottlingSuite.scala` 90 checks only `partitionId`; `ContentUnderstandingSubmissionSuite.scala` 328–352 covers all the task IDs.
  - The `concurrentTimeout` message text is not asserted.
  - Poll mode with a scalar `allowInputTruncation` is untested, but poll URLs are built from the operation handle, so the option cannot be sent.
  - The notebook test does not re-run `submit-and-save` against an existing path; the guide tests cover that pattern.

- [x] **Validation evidence supplied by the caller; I did not run it.**
  - Compile, test-compile, style and codegen passed.
  - 201 Content Understanding and 24 notebook Scala tests passed.
  - 99 Python tests with 250 subtests passed; 7 live tests were skipped, with 2 upstream warnings.
  - Black and whitespace checks passed.
  - These runs used the local classpath and generated Python, not release packages or the live Content Understanding service.

- [ ] **Truncated diff index.** The index stops after `D\Content Understanding.md` +647. I found and read the remaining files myself but cannot confirm they match the hidden index entries one for one; the driver's hash check covers that.
- [ ] **Partial notebook read.** I read notebook JSON lines 255–370 and used grep for the rest; the remaining cells are exercised by the Python notebook tests listed above.
- [ ] **Nothing executed.** As a read-only reviewer, I ran no commands, builds or tests.