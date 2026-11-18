---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 5
timestamp: "2026-11-17T22:29:10Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5a99f1e7c25d0d8ef80f0bd2c1b6620cb29d5cc4d83ad659bba2ec6d93b4c222"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-15-stage-attempt-1-review-5-decisions-paths-opus.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Production code read.** I read the changed paths:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`: `messages` (lines 362-388) and the lazy Decisions branch in `prepareRequests` (lines 474-478).
  - `AIRequestAdapter.scala`: `validateCapabilities` (247-265), `validateMessageConstruction` (275-281), `validateDecisionsInputColumn` (about 325-332), `nativeEnvelope`, and both `request` overloads.
  - `NativeQuestions.scala`: `decisions()`, `prepareDecisionsRequest`, `validateDecisionsImageCount`, `questionImageCount`, `prepareParts`, `hostedImageUrl` and `validatePart`.
  - `services/openai/OpenAIAttachmentUtils.scala`: `inferMimeType` (295-308), `mayBeImagePath` (315-318), `dataUrlMetadata` and `dataUrlFileType`.
  - `services/openai/OpenAIPrompt.scala`: `createMessagesForRow` and `validateImageAttachment`/`wrapFileToMessagesList`.
- [x] **Checks that run before image IO have tests.** All of these cover both stages, both `inputCol` and `promptTemplate`, and assert that the image server receives no requests:
  - `AIPromptMediaInputSuite.scala` 272-301: null or empty text gives `missing_input`; whitespace-only text gives `invalid_request`.
  - 376-412: questions, model, version and options are checked before IO.
  - 473-501: `.pdf` is rejected before IO and before size checks, with bounded and unbounded limits.
  - 503-540: the combined path-plus-question limit. The accepted row performs exactly one read, and a blank second path is not counted.
- [x] **Payloads and persistence have tests.** In `AIPromptMediaInputSuite.scala`:
  - 165-205: null `file_id`/`detail` in question images are stripped, other null extensions are kept, and copy/save/load and `questionsCol` work.
  - 207-270: local, HTTP, extensionless and inline images (PNG, JPG/JPEG, GIF) send exactly one user message with `input_text` followed by `input_image`. Read counts per transform are checked.
  - 503-540: attachments follow schema order, not map order.
- [x] **MIME handling has tests.** In `AIPromptMediaInputSuite.scala`:
  - 303-334: WebP works through local, HTTP and data URLs in Decisions, Chat and Responses. Removing the RIFF/WEBP check would fail this test.
  - 336-374: WAV and an 11-byte RIFF file stay audio.
  - 414-470: XBM content in a `.png` is rejected after the read, missing paths are not leaked, and the size limit is checked at the exact byte boundary.
- [x] **Hosted URL tests.** `NativeQuestionsSuite.scala` 101-118 and 131-157 cover accepted and rejected URLs: underscore, IDN, IPv6, ports, signed queries, file/ftp/relative URLs, and no leakage of supplied values. `AIPromptMediaInputSuite.scala` 123-163 shows the stages forward hosted URLs unchanged.
- [x] **Construction and capability tests.**
  - `AIPromptPreflightSuite.scala` 59-97 and `AIPromptRecoveryPreflightSuite.scala` 78-120: the `messagesCol` matrix, the exact `fileSizeLimitMB` message, and acceptance of an empty map.
  - `AIPromptMediaInputSuite.scala` 561-615: settings Decisions does not use, input-mode conflicts, and System One `columnTypes`.
  - `AIPromptRecoveryPreflightSuite.scala` 31-55: the case-insensitive `inputCol`/path overlap is rejected, durable and non-durable, before HTTP or storage.
- [x] **Durable output and cancellation have tests.** `AIPromptRecoveryAttachmentSuite.scala` 26-108 covers inline resume and rejection of hosted or file inputs. In `AIPromptAttachmentSuite.scala`, the cancellation matrix (about lines 357-381) now includes `decisions`.
- [x] **Live suite is gated correctly.** `OpenAIDecisionsLiveSuite.scala` 22-69 skips through `assume(...)` when credentials are missing and embeds no secrets.
- [x] **Docs checked against tests.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 259-260 (request-wide limit), 302-323 (path rules, `.ashx`), 736-739 (capability table) and 869-872 (resolver).
- [x] **Searches for missing coverage.** I searched `cognitive/src/test` for:
  - question arrays with more than one image-bearing question;
  - `postProcessing` rejection on native APIs;
  - Chat/Responses tests that combine row bindings with path columns;
  - a case-sensitive Decisions overlap test.

  None were found.
- [x] **Nothing executed.** I ran no tests or commands. The driver's validation evidence (284 passed, 3 live tests canceled) was not reproduced.
- [ ] **Not verified: Python at runtime.** Python `setQuestions` with nested instruction arrays could not be checked in a read-only review. The supplied evidence covers static parsing only. The Scala side's nested `AIJson.fromAny` conversion is exercised in `AIJsonSuite.scala` 555-582.

## Issues

### Issue 1: The 128-image limit is never tested across more than one question
- **Severity**: Medium
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestionsSuite.scala`, `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` (production: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`)
- **Line(s)**: NativeQuestionsSuite 120-129, 152-156; AIPromptMediaInputSuite 503-540, 617-641; NativeQuestions.scala `decisions()` (about 118) and `prepareDecisionsRequest`/`validateDecisionsImageCount`/`questionImageCount` (about 151-172)
- **Description**: The docs (lines 259-260) say the limit counts "all question instructions, not 128 images per question." This PR adds that sum in `questionImageCount`, which is used in three places:
  - the constant-question preflight in `decisions()`;
  - the count before IO, which includes path images;
  - the recount after preparation in `prepareDecisionsRequest`.

  Every limit test uses a single question. The only multi-question image test (NativeQuestionsSuite 120-129) has 6 images in total. Nothing separates a sum across questions from a per-question maximum or a first-question-only count. Stage tests also never send a question array that mixes string and part-array instructions. Null `file_id`/`detail` stripping is only checked on the first question.
- **Risk**: A regression to per-question counting would pass every test. Requests over the limit would then be sent after the file/HTTP reads and fail at the provider, instead of failing early with `invalid_request`. The documented guarantee would quietly stop being true.
- **Suggested Fix**:
  - In `NativeQuestionsSuite`, test two image questions with 64+64 images (accepted) and 64+65 (rejected) through `NativeQuestions.validate("decisions", ...)`.
  - Also there, call `validateDecisionsImageCount(1, ...)` with 64+63 (accepted) and 64+64 (rejected).
  - Add one stage-level case for both stages with `questionsCol`, one path column and two image questions whose total crosses 128. Assert `invalid_request` and that the image server receives no requests.
  - In that stage case, include a string-instruction question and give the second image question null `file_id`/`detail`, so normalization of every question is checked.

### Issue 2: No test proves unrecognized extensions are rejected without being read
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` (production: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, `.../openai/OpenAIPrompt.scala`)
- **Line(s)**: AIPromptMediaInputSuite 414-471, 473-501; OpenAIAttachmentUtils.scala 315-318; docs lines 320-323
- **Description**: The docs say filenames with any other extension, "including unrecognized ones such as `.ashx`, are rejected without reading the file." The no-IO test only uses `.pdf`, which is a known non-image extension.
  - `.tiff` and `.bmp` appear only as existing local files whose content is also not an image.
  - If `mayBeImagePath` were narrowed to reject only known non-image extensions, those files would be read and still fail the MIME check after the read, with the same message.
  - So no test fails if unrecognized extensions start being read.
- **Risk**: A plausible change, such as letting `.ashx`/CDN handler URLs through for content sniffing, would contradict the docs without any test failing. Decisions would then perform HTTP/file reads and size checks before rejecting. A missing file would report `input_io_error` instead of the format message.
- **Suggested Fix**: Extend "Decisions known non-image paths reject before IO and size checks" with two cases, for both stages and with bounded and unbounded limits:
  - an HTTP `/image.ashx` (or `/photo.tiff`) URL whose server returns valid PNG bytes;
  - a missing local `missing.bmp`.

  Assert `invalid_request`, the exact JPEG/PNG/GIF/WebP message, and `imageServer.requests.isEmpty`.

### Issue 3: The rewritten capability check for non-generative APIs is only partly tested
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` (tests: `AIPromptMediaInputSuite.scala`, `AIPromptSuite.scala`)
- **Line(s)**: AIRequestAdapter.scala 247-260; AIPromptMediaInputSuite 561-615; AIPromptSuite 466-487; docs 736-739
- **Description**: The condition changed from `p == messagesCol && api == "decisions"` to `api == "decisions" && decisionsSettings(p)`, which now also allows `columnTypes` and `fileSizeLimitMB`.
  - **Covered:** Decisions with `systemPrompt`, with `dropPrompt`, and with `fileSizeLimitMB` but no path column; System One with `columnTypes`; Embeddings with `systemPrompt`.
  - **Not covered:** Decisions with `postProcessing`/`postProcessingOptions`; System One or Embeddings with `fileSizeLimitMB`, `dropPrompt` or `postProcessing*`; Embeddings with `columnTypes`. The docs table lists all of these as unsupported.

  The later "requires a path column" check for `fileSizeLimitMB` applies only to Decisions. If the `api == "decisions"` part of the condition were lost, System One and Embeddings would silently accept `fileSizeLimitMB` and ignore it.
- **Risk**: Settings could be accepted and silently ignored, so users would believe a size limit or post-processing applies when it does not.
- **Suggested Fix**: Add one table-driven preflight test.
  - Cover System One, Embeddings, and both Decisions stages against `systemPrompt`, `columnTypes`, `fileSizeLimitMB`, `postProcessing`, `postProcessingOptions` and `dropPrompt`.
  - Configure each stage so it is valid apart from the setting under test.
  - For every unsupported combination, assert that `transformSchema` and `transform` fail with `<param> is not supported by apiType='<api>'` before HTTP.
  - Assert that the three settings Decisions allows reach their own specific checks.

### Issue 4: The case-sensitive side of the `inputCol`/path overlap check is untested
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptRecoveryPreflightSuite.scala` (production: `AIRequestAdapter.validateDecisionsInputColumn`)
- **Line(s)**: AIPromptRecoveryPreflightSuite 31-55; AIRequestAdapter.scala about 325-332
- **Description**: The only overlap test sets `spark.sql.caseSensitive=false` and checks that `asset`/`ASSET` is rejected. Nothing checks that with `caseSensitive=true`, two distinct columns are accepted: `ASSET` as the text `inputCol` and `asset` as a path column. An implementation using `equalsIgnoreCase` would pass today's test but wrongly reject that valid setup. That contradicts the requirement that Spark's configured column resolver is respected (see also docs line 872).
- **Risk**: In case-sensitive sessions, valid schemas could be rejected with a misleading message, and no test would catch it.
- **Suggested Fix**: Add a case-sensitive branch for both stages, through both `transformSchema` and `transform`. Assert success, that the input text comes from `ASSET`, and exactly one image read from `asset`. Restore the configuration in `finally`.

### Issue 5: No test pins Chat/Responses to eager message preparation
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` (no corresponding test)
- **Line(s)**: AIPrompt.scala 474-478
- **Description**: Decisions now builds messages lazily, after the metadata checks. Chat/Responses stay eager only because the `lazy val` is passed by value to the three-argument `request` overload, and the plan requires them to keep that behavior. No Chat/Responses test has a row with both an invalid row-level binding (null `modelCol`/`apiVersionCol` or malformed `extraBodyCol`) and a path attachment.
- **Risk**: Sending all APIs through the lazy overload is a natural cleanup, and no test would fail. It would change Chat/Responses error precedence (for example `input_io_error` becoming `missing_input`) and skip their reads, contrary to the stated scope.
- **Suggested Fix**: Add a small Chat/Responses test mirroring "Decisions invalid request metadata rejects path rows before image IO". Use one row with a null model binding and a missing local image, which should report `input_io_error`. Use another with a valid HTTP image, which should cause exactly one image-server read. Any future move to lazy evaluation would then be a deliberate, visible change.

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

## Driver triage before the final lens

- **Issue 1 - accepted coverage gap.** Add a focused boundary across multiple
  image-bearing questions, including the path-plus-question sum and pre-IO
  rejection through both public stages.
- **Issue 2 - accepted small table extension.** Add an unrecognized extension
  to the existing bounded/unbounded no-IO rejection cases.
- **Issue 3 - optional matrix expansion, deferred.** Existing assertions cover
  the changed Decisions allowlist and representative rejected native APIs.
  The remaining proposed combinations exercise unchanged generic parameter
  rejection, not a demonstrated new defect.
- **Issue 4 - accepted positive regression.** Pin the case-sensitive distinct
  `ASSET` text / `asset` path configuration, complementing the new overlap
  rejection and checking the actual wire input.
- **Issue 5 - optional precedence contract, deferred.** The source retains the
  by-value overload for Chat/Responses, and existing attachment/native tests
  verify their behavior. A priority contract for rows with several independent
  errors is not introduced solely to constrain a hypothetical future refactor.

Collect lens 6 before editing. The intended test changes will invalidate this
pass; do not call it a qualifying review of the future patch. Original
findings and verdict remain unchanged.

## Final driver dispositions

- **Issue 1 - addressed.** `NativeQuestionsSuite` now distinguishes 64+64 from
  64+65 images across two questions, and 1+64+63 from 1+64+64 with a shared path.
  The existing public-stage count test now uses two image questions plus a
  string-instruction question. Both stages check the total before additional
  image IO, and exact payload assertions verify null-option removal from the
  second question as well as preservation of the text-only question.
- **Issue 2 - addressed.** The existing no-IO matrix now includes an `.ashx`
  endpoint serving PNG bytes and a missing `.bmp`, through both stages with
  finite and unset file limits. It asserts the format error and zero image or
  inference requests.
- **Issue 3 - optional additional combinations, deferred.** The current
  allowlist and representative rejected APIs are covered. No new behavior
  failure is demonstrated by the proposed exhaustive cross-product.
- **Issue 4 - addressed.** Both stages accept distinct case-sensitive `ASSET`
  text and `asset` inline-image columns. Schema/transform success and the exact
  one-text/one-image request are asserted, and session configuration is restored
  in `finally`. The case-insensitive overlap rejection remains covered.
- **Issue 5 - optional multi-error precedence assertion, deferred.** Direct
  source inspection confirms the retained eager overload. No additional public
  priority contract for simultaneously invalid fields is introduced.

Verification: all 58 tests in the three changed suites passed, together with
root ScalaStyle/test ScalaStyle, compile/test compile, codegen, wrapper parsing
and six documentation-example parses. Blob comparisons show only these three
test files changed from the preceding full 284-test run: all production/docs and
the other 228 deterministic tests are unchanged. This provides applicable
evidence for 286 unique tests, not a single full 286-test run. Three live tests
canceled in the preceding run; no provider E2E is claimed.

Validated patch SHA-256:
`05a6a91f7184b4b33639b785368163082b8e4e00272a6f2d34e6beb7c7924dee`.
Pass 15 remains invalidated by the test update; a new complete pass is required.