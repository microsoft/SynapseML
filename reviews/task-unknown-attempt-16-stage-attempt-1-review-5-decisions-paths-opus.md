---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 5
timestamp: "2026-11-18T00:35:49Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "a076c396e25452b78e1fed519b92442b1bca785fd6fba77cf251a4f9c9ee954f"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-16-stage-attempt-1-review-5-decisions-paths-opus.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Production paths read directly.** I read the changed code paths in these files:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`: the Decisions branch of `messages` (377-385) and the lazy/eager split in `prepareRequests` (474-478).
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`: `validateCapabilities` (247-266), `validateDecisionsInputColumn` (325-332), `nativeEnvelope` (582-598) and the `request` overloads (610-633).
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`: 86-119, 151-184 and 203-246.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`: 137-171, 295-318 and 431-442.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`: 600-656.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala`: 143-195.
- [x] **Prompt-text validation is pinned.**
  - `AIPromptMediaInputSuite.scala` 272-301 asserts the exact code and message for null, empty and whitespace-only text. It covers `inputCol` and template forms on both stages, with zero image or inference requests.
  - `NativeQuestionsSuite.scala` 172-198 covers the non-path text and JSON split.
- [x] **Pre-IO ordering is pinned.**
  - `AIPromptMediaInputSuite.scala` 376-412 covers null and `{}` questions, null model, null version, `[]` options and a combined failure. Each case asserts the code, a message fragment and that no image server was called.
  - Lines 504-553 pin the image-count sum across questions with one and two paths. When `pathCount=1`, the blank second path is not counted. Rejected rows download nothing. Exact wire assertions pin mixed string/array instructions and null-field stripping.
- [x] **Preflight checks are pinned.**
  - `AIPromptMediaInputSuite.scala` 574-600 pins the exact `fileSizeLimitMB` message on both stages, for `transformSchema` and `transform`.
  - Lines 602-628 pin the JSON `inputMode` conflict, the `messagesCol` conflict and System One's `columnTypes` rejection.
  - Lines 555-572 show that empty or text-only mappings keep native text input.
  - `AIPromptRecoveryPreflightSuite.scala` covers the case-insensitive `inputCol`/path overlap rejection (before IO or storage) and acceptance under case-sensitive resolution.
- [x] **Format and IO behavior is pinned.**
  - `AIPromptMediaInputSuite.scala` 414-471 checks the exact Decisions diagnostic for non-image data URLs and local files. A missing file returns `input_io_error` without leaking its path. A limit one byte below the file size fails, and the exact limit passes.
  - Lines 473-502 cover pre-IO extension rejection for `.pdf`, `.ashx` serving PNG bytes and a missing `.bmp`, with finite and unset limits and no image server calls.
  - Lines 207-270 load PNG, JPEG and GIF from local, HTTP, extensionless HTTP and inline sources. They run through copies and save/load, with exact wire content and exactly two downloads per transform.
- [x] **The WebP fix is pinned for named inputs.**
  - Lines 303-334 would fail without the RIFF/WEBP check: the JDK returns `audio/x-wav`, so a `.webp` name would be classified as `unsupported`.
  - Lines 336-374 keep extensionless WAV and the 11-byte RIFF file as audio, with an exact message for Decisions and Responses and a fixed message prefix for Chat. This also shows the Decisions-only predicate does not leak into Chat/Responses.
- [x] **Hosted URLs are covered.**
  - `NativeQuestionsSuite.scala` 101-117 accepts 12 forms, including underscore, IDN, IPv6, userinfo, empty-port, uppercase-scheme and signed-query URLs. Lines 222-252 cover the rejected forms.
  - `AIPromptMediaInputSuite.scala` 123-163 forwards hosted and inline forms unchanged for `messagesCol` and JSON input on both stages. Lines 165-205 pin question images with null stripping through bindings, copies and save/load.
- [x] **Durable mode is covered.**
  - `AIPromptRecoveryAttachmentSuite.scala` 26-55: inline path images resume. File and hosted paths raise `SparkException` and create no results directory.
  - Lines 57-106 reject hosted question images in `transformSchema`, `transform` and per-row mode. Inline question images resume with exact wire questions.
  - `AIDurableInput.scala` 172 classifies inline inputs from `columnTypes` only, so `inputCol` versus template does not affect durable path validation.
- [x] **Test doubles are adequate.**
  - `AIRecordingServer` is a real local HTTP server, and the `encodeResponse` image servers count reads.
  - Missing-file variants turn "no local IO" into a distinguishable error code.
  - `path()` returns a unique UUID path (`AIPromptTestBase.scala` 9), so the extensionless fixtures don't overwrite each other.
- [x] **No product defect was demonstrated.** All four issues below are test gaps, and fixing them needs no production change.
  - The lazy predicates have no observable effect on Chat/Responses: `pathFilename` is wrapped in `Try`, and `dataUrlFileType` raises the same `IllegalArgumentException` messages as the later reader.
  - One incidental change: Decisions `messagesCol` rows now report row metadata errors before message-struct errors. This matches JSON mode and the brief, so I did not raise it.
- [x] **Driver validation evidence was reviewed, not re-run.** It reports 58 passing tests in the three changed suites plus 228 unchanged tests from the earlier full run. Three credential-gated live tests were canceled, and no Python runtime or provider end-to-end test was run. I executed no tests.
- [ ] **Hunk-level attribution is limited.** The supplied diff was truncated after the first `OpenAIAttachmentUtils.scala` hunk. I reviewed the post-change source and tests directly, so I cannot attribute every line in those files to this patch versus the target.
- [ ] **Not pinned, but acceptable:**
  - Header validation after attachment loading is documented (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 306-307) but has no test.
  - Python wrappers only delegate to the JVM and gained no new Params. The Python docs examples are only checked for parsing.

## Issues

### Issue 1: The new Decisions path route has no tests for malformed data-URL metadata or for parts that only native validation rejects
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`. Related code: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`.
- **Line(s)**:
  - `AIPromptMediaInputSuite.scala` 25, 77-82, 130-133 and 423-436
  - `OpenAIPrompt.scala` 633-635
  - `OpenAIAttachmentUtils.scala` 159-171 and 431-442
  - `AIRequestAdapter.scala` 590-596
- **Description**:
  - The new Decisions-only predicate is the only caller of `dataUrlFileType`, and no test reaches its error branches: a missing comma (`take(0)`) or a missing `base64` marker.
  - Every Decisions path data URL at 423-436 has both a comma and a `base64` marker.
  - The `malformedImages` fixture (`data:image/png`, `data:image/png,AQID`) runs only for Chat/Responses at 77-82, and those APIs never evaluate the predicate.
  - Every Decisions path rejection fails in the reader or in the format predicate. No path case passes the reader and is then rejected by the native header validation in `prepareDecisionsRequest`. One such input is `data:image/png;name;base64,AQID`: the reader classifies it as PNG, but the explicit-part test at 130-133 rejects it.
- **Risk**: Two regressions would pass the current tests:
  - A slicing change in `dataUrlFileType` that throws something other than `IllegalArgumentException` would fail the whole job instead of returning a row error.
  - A refactor that trusts generated path messages and skips native validation would return a provider `http_error` instead of a local `invalid_request`.

  Both break the "controlled errors" invariant on the new route.
- **Suggested Fix**:
  - Add a Decisions case with the existing helper: `verifyMedia("decisions", mediaInput, Seq("missing_input", "invalid_request", "invalid_request"), sends = 1) { _.setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setQuestions(NativeRequestFixtures.DecisionQuestions) }`.
  - Add `data:image/png;name;base64,AQID` to the Decisions path test. Expect `invalid_request` and no inference requests.

### Issue 2: No test covers extensionless WebP, the case the RIFF/WEBP fix most affects
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`. Related code: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`.
- **Line(s)**:
  - `AIPromptMediaInputSuite.scala` 303-334 and 336-374
  - `OpenAIAttachmentUtils.scala` 295-308
  - `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 314-317
- **Description**:
  - The WebP test uses only `.webp`-named local and HTTP inputs plus a declared `data:image/webp` URL.
  - The extensionless test covers only the negative side: WAV and the 11-byte RIFF file stay audio.
  - A named input could be classified as an image through its name. Only an extensionless input proves that content sniffing returns `image/webp` instead of the JDK's `audio/x-wav`.
  - The docs promise that extensionless images work when the runtime recognizes the header.
- **Risk**: A refactor could gate the RIFF/WEBP check on the `.webp` extension, or prefer name-based image MIME types before content sniffing. The named test would still pass, but extensionless WebP would again be classified as audio and rejected on all four stages: audio errors on Chat/Responses and the format error on Decisions. That is the misidentification this PR fixes.
- **Suggested Fix**:
  - In the WebP test, add an extensionless local file (`Files.write(Paths.get(path()), bytes)`) and an extensionless HTTP URL (`imageServer.url + "/photo"`) that serves the existing WebP fixture.
  - Run both on all four stages. Assert no row error and that each inference body contains `data:image/webp;base64,<encoded>`.

### Issue 3: No test pins eager message preparation for Chat/Responses, although the brief requires it
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`. Tests: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` and `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptAttachmentSuite.scala`.
- **Line(s)**:
  - `AIPrompt.scala` 377-385 and 474-478
  - `AIRequestAdapter.scala` 610-611
  - `AIPromptMediaInputSuite.scala` 376-412
  - `AIPromptAttachmentSuite.scala` 336-348
- **Description**:
  - Both branches share the lazy `inputMessages`. Only the by-value `else adapter.request(row, prompt, inputMessages)` call keeps Chat/Responses eager.
  - No Chat/Responses test combines a path column with a row-level model, version or options failure. Among the `services/ai` tests, `setModelCol`, `setApiVersionCol` and `setExtraBodyCol` appear with path columns only in the Decisions test at 393-394.
  - The Chat/Responses IO test at `AIPromptAttachmentSuite.scala` 336-348 uses a constant model.
  - The Chat/Responses message on the edited missing-text expression ("prompt is null", `AIPrompt.scala` 381-382) is not asserted anywhere under `cognitive/src/test`.
- **Risk**: Routing every API through the `() => inputMessages` overload would be a natural simplification, and it would make Chat/Responses lazy. Missing model, version or options errors would then take precedence over attachment `input_io_error` and `invalid_request`, and failing rows would skip attachment reads. Every current test would still pass, even though the brief requires that other APIs keep eager message preparation.
- **Suggested Fix**:
  - Add a Chat/Responses counterpart to the Decisions metadata test. Use `setModelCol("model")` with a null model and a path to a missing local file.
  - For Chat/Responses, assert `input_io_error` (eager). For Decisions, the same row should give `missing_input` with no reads.
  - Optionally add a row that asserts the exact Chat/Responses missing-prompt message.

### Issue 4: No test asserts the changed hosted-or-inline image message, and hosted-URL rejections are checked only by exception type
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestionsSuite.scala` and `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`. Related code: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`.
- **Line(s)**:
  - `NativeQuestions.scala` 228-230 and 240
  - `NativeQuestionsSuite.scala` 222-252
  - `AIPromptMediaInputSuite.scala` 130-133 and 151-154
- **Description**:
  - This PR changes the user-facing row error to "Decisions images must use HTTP(S) URLs or inline base64 image data URLs" and adds the hosted-URL branch.
  - That text does not appear under `cognitive/src/test` or `docs`.
  - The URL rejections at `NativeQuestionsSuite.scala` 224-237 assert only `intercept[IllegalArgumentException]` (line 252).
  - The end-to-end `rejected` list at `AIPromptMediaInputSuite.scala` 130-133 contains only data URLs, and it checks only that the error contains `invalid_request`.
- **Risk**: These regressions would still pass the type-only assertions:
  - Restoring the old "must be inline base64" message, which now contradicts hosted-URL support.
  - Rejecting hosted URLs through an earlier, unrelated `require`.
  - Misrouting hosted-URL errors at the row level.
- **Suggested Fix**:
  - For the URL cases at `NativeQuestionsSuite.scala` 224-237, assert that `getMessage == "requirement failed: Decisions images must use HTTP(S) URLs or inline base64 image data URLs"`.
  - Add one malformed hosted URL, such as `https:///image.png`, to the end-to-end `rejected` list and assert the parsed error `message`.

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

## Driver resolution on the next frozen patch

All four coverage findings are addressed. The original review and its
`ISSUES_FOUND` verdict remain unchanged; these test changes invalidate pass 16,
so a new complete pass is required.

1. **Malformed path metadata and native validation:** the existing Decisions
   path-error matrix now includes missing comma, missing base64 marker, empty
   base64 data, and `data:image/png;name;base64,AQID`. Both public stages must
   produce local `invalid_request` rows with no inference. The last header
   deliberately passes the reader but is rejected by native validation.
2. **Extensionless WebP:** the existing four-stage WebP test now includes an
   extensionless local file and HTTP path. All five inputs must preserve the
   exact `data:image/webp;base64,...` payload. Two HTTP reads per stage are
   asserted; WAV and truncated RIFF regressions remain intact.
3. **Chat/Responses eager preparation:** two new tests in
   `AIPromptAttachmentSuite` combine a missing local path with a null row model.
   Both must report sanitized `input_io_error` without sending inference.
   Existing Decisions tests prove the complementary metadata-before-IO ordering.
   The test belongs in this suite to preserve the 800-line media-suite limit.
4. **Hosted-URL diagnostics:** the existing public request matrix now includes
   `https:///image.png`, through messages and JSON input on both stages. It
   checks the parsed error code and exact hosted-or-inline diagnostic, including
   the existing `requirement failed:` prefix. Helper-level assertions were not
   duplicated because this public-path assertion covers routing and wording.

**Verification:** 57 tests passed across `AIPromptMediaInputSuite` (26) and
`AIPromptAttachmentSuite` (31), with zero failures or cancellations. Root
ScalaStyle, test ScalaStyle, compilation, test compilation and codegen passed.
Generated wrappers and six Python documentation examples parse.
Production files are byte-identical to the prior snapshot. The other 231
deterministic tests are unchanged and retain their prior passing evidence:
288 unique applicable passes across three runs, not one full run.

An initial style failure (the media file exceeded 800 lines) was corrected by
placing the new ordering tests in the existing attachment suite. An initial
assertion omitted the existing error prefix; the expected string was corrected,
not the production behavior. Both failures remain in local validation evidence.

Next product patch:
`5f939e3f515e6f44844ce05af6b10327dd65b1a020e282378b72bff27834d3d7`.
No live-provider E2E pass is claimed.