---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 6
timestamp: "2026-11-18T03:28:29Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "76f70327d31538796d55bcafd67a33af51a5319d34c44c7e84453bc678857d19"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-17-stage-attempt-1-review-6-decisions-paths-opus.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff coverage:** I read all 13 file sections of `.review-input-decisions-paths.diff` (file headers at diff lines 1, 35, 141, 289, 349, 422, 498, 1072, 1125, 1228, 1346, 1470, 1540). The `OpenAIAttachmentUtils.scala` and `OpenAIPrompt.scala` hunks were cut off in the prompt, so I read them from the file at diff lines 289-421.
- [x] **Image reads happen after validation:** `AIPrompt.scala:473-478` builds Decisions messages lazily and passes a thunk.
  - `AIRequestAdapter.nativeEnvelope` (~582-598) validates the questions and calls `validateDecisionsImageCount(pathImageCount, …)` before `messages()`.
  - `prepareDecisionsRequest` counts the images again after they are read.
  - Chat and Responses use the by-value `request` overload, so their messages are still built eagerly.
  - `retainGeneratedMessages` is false for Decisions, so nothing forces the lazy value early.
- [x] **Prompt-text error codes:** `AIPrompt.scala:377-385` rejects null or empty text with `AIMissingInputException("input is null or empty")` and whitespace-only text with an IllegalArgumentException from `validateDecisionsInput`. The catch block (`AIPrompt.scala:482-494`) maps these to `missing_input` and `invalid_request`. Both happen before `generatedMessages` reads any image.
- [x] **Capability and schema checks** (`AIRequestAdapter.scala` ~247-263, ~325-332):
  - The `messagesCol` conflict is reported before the rule that `fileSizeLimitMB` needs a path column.
  - Path columns require `inputMode='text'`, which is the default (`AIPromptParams.scala:105`).
  - The inputCol/path conflict check uses `AIColumnPlan.matches`, so it follows Spark's column resolver.
- [x] **Format checks** (`OpenAIPrompt.scala:628-649`):
  - The extension check runs before `prepareFile` reads the file.
  - The data-URL check reads only the metadata (`OpenAIAttachmentUtils.scala:437-438`).
  - `categorizeFileType` (line 334) needs an `image/*` type plus an allowed extension, so `data:image/bmp` is rejected today.
  - The predicates are by-name and are skipped unless the API is Decisions, so Chat and Responses never run them.
- [x] **WebP fix** (`OpenAIAttachmentUtils.inferMimeType`, diff lines 296-306): a file of at least 12 bytes with `RIFF` and `WEBP` tags is detected before the JDK guess. WAV and short RIFF files still get the JDK's audio type.
- [x] **Hosted URLs** (`NativeQuestions.scala:203-213, 227-241`): validation only parses the URL; there is no fetch or DNS lookup. Ports must be ASCII digits and empty hosts are rejected. Error messages never include the URL or path, so signed query strings stay out of the error column.
- [x] **Durable output:** the unchanged `AIDurableInput.scala` still rejects any `image_url` that is not a data URL, which matches the docs' inline-only rule.
- [x] **Docs match the code** (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 245-300, 435-439, and the capability table at ~739-741):
  - Template placeholders for path columns become filenames (`AIPrompt.scala:425-429`, `OpenAIPrompt.scala:318`).
  - Each read is capped at the size limit plus one byte.
  - The live-test environment variable and HTTPS `assume` match `OpenAIDecisionsLiveSuite.scala:22-33`.
  - No hand-written Python and nothing in `AIPromptPython.scala` contradicts the docs.
- [x] **Test assertions inspected:**
  - `AIPromptMediaInputSuite.scala:370,466,506` assert the exact format message.
  - `AIPromptMediaInputSuite.scala:547,661` and `NativeQuestionsSuite.scala:138,141` use `contains("at most 128 images")`.
  - The CancellationException test now also covers `decisions` (`AIPromptAttachmentSuite` diff hunk at +372).
- [x] **Severity:** I found no Critical, High or Medium defects. All four findings are Low maintainability, observability or performance items, not demonstrated functional defects.
- [x] **Independence:** I did not open, search or list `reviews/` or any other reviewer's output.
- [ ] **Test and build runs:** not performed, because this review is read-only. I inspected the driver's results (57 fresh passes, 288 unique applicable passes across three runs, ScalaStyle, compile and codegen) but did not reproduce them. No provider end-to-end runs were checked.

## Issues

### Issue 1: Decisions path-image format rule is defined in four places, two of them shared with Chat/Responses
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 313, 628-629, 634-635, 645, 649
- **Description**: The documented Decisions rule (JPEG, PNG, GIF and WebP only; an unsupported extension is rejected before IO) is encoded in four places:
  1. the pre-read check, `mayBeImagePath(..., imageExtensions)`;
  2. the data-URL check, `dataUrlFileType(..., imageExtensions, …) == "image"`;
  3. a hard-coded MIME set checked after the read;
  4. the literal error message.

  Checks 1 and 2 use the shared `imageExtensions` table, which controls Chat/Responses attachments. Checks 3 and 4 are Decisions-only. All four agree today, so this is not a current defect. Also, the name `validateImageAttachment` sounds general, but the helper does nothing for any API except Decisions.
- **Risk**: If Chat/Responses later adds a type such as `heic` or `avif` to `imageExtensions`, Decisions behavior changes without any Decisions code change:
  - A file with that extension passes the documented pre-IO gate, is read and size-checked, and only then fails. The docs promise it is rejected before the read.
  - A data URL of that type has no post-read check, and `NativeQuestions.inlineImageHeader` accepts any `image/<token>`. It would go to the provider without the documented local error.
- **Suggested Fix**: Define one Decisions-owned set of MIME types and derive its extensions from it. Use it for all three checks; compare the data URL's declared MIME type directly against the set. Build the error message from the same set. Rename the helper to show its scope, for example `requireDecisionsImage`. Keep `imageExtensions` for Chat/Responses only.

### Issue 2: The post-read format error does not say what type was detected
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628-629, 649
- **Description**: When the content check after the read fails, the user gets the same message as for the pre-read extension check: "Decisions path columns only support JPEG, PNG, GIF, and WebP images". The post-read check only fails when the filename looked acceptable but the content was detected as something else. Two examples:
  - a file named `.png` that actually contains RIFF/WAVE audio, detected as audio;
  - a real JPEG with no extension whose first marker is `FF D8 FF DB` or an ICC `FF D8 FF E2`. The JDK does not recognize these headers, so the file becomes `application/octet-stream`.

  In both cases the message says PNG or JPEG is supported, about a file that is named or actually is PNG or JPEG.
- **Risk**: Users cannot tell whether to rename, re-encode or replace the file. The documented workaround (add an extension) cannot be found from the row error, which slows diagnosis and adds support load.
- **Suggested Fix**: For the post-read check only, append the detected type, for example `(detected application/octet-stream)`. The MIME type is not sensitive and does not reveal the path. Keep the pre-read message unchanged. Update any exact-message assertions that cover post-read cases; check `AIPromptMediaInputSuite.scala:370,466,506`.

### Issue 3: optional — Every path-loaded image is copied and rescanned in full
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 227-238 (also `OpenAIAttachmentUtils.scala:207-212`)
- **Description**: With this patch, every Decisions path image goes through `validatePart`, including bytes SynapseML has just base64-encoded itself. `validatePart` copies the whole base64 body with `url.substring(comma + 1)` (line 231), then scans it with a boxed iterator in `validateBase64Content`. Data URLs in path cells were already validated by the shared reader, so they are scanned twice. The patch's own `dataUrlFileType` was written specifically to avoid this ("Classify only the metadata without copying the potentially large base64 body").
- **Risk**: Each image adds one temporary copy of its payload plus extra CPU on executors. With large files and up to 128 images per request, this increases GC pressure on top of the copies serialization already needs. This is a performance improvement, not a correctness defect, and the cost is small compared with HTTP latency.
- **Suggested Fix**: Validate the base64 in place. Add an overload that takes a start index and loops with `charAt`, and replace `encoded.nonEmpty` with `comma + 1 < url.length`. Messages and behavior stay identical.

### Issue 4: The 128-image limit has two different error messages and a hard-coded number
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 136, 166-167
- **Description**: The same request-wide limit is checked in three places:
  - lines 118 and 167 say "Decisions accepts at most 128 images per request";
  - line 136, the input-only count, still says "Decisions accepts at most 128 images". `prepareDecisionsRequest` reaches this check first.

  All three repeat the number `128` even though `MaxImages` exists. The docs (lines 259-260) describe one limit covering both input and question instructions, but the input-only message can be read as a per-input limit.
- **Risk**: The same rule produces different messages depending on where the images came from. A future limit change could update `MaxImages` and leave the old number in the messages.
- **Suggested Fix**: Use one message value, `s"Decisions accepts at most $MaxImages images per request"`, at all three places. The existing tests match on `contains("at most 128 images")`, so none need to change.

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

## Driver disposition

The four Low recommendations are closed without implementation for this patch.
Original findings and the `ISSUES_FOUND` verdict remain unchanged. No current
functional defect was reported; these dispositions do not claim new behavior
or performance measurements.

1. **Format-policy consolidation:** duplicate of this pass's rounds 1 and 2.
   The current extension, metadata, detected-MIME and diagnostic rules agree.
   Shared future format expansion would require its own Decisions compatibility
   assessment and tests. No such expansion is introduced here. A new
   Decisions-owned policy abstraction is not needed to correct current behavior.
2. **Detected-MIME diagnostics:** the existing message identifies the supported
   formats without exposing caller-supplied data. Extensionless sniffing and the
   supported-extension workaround are documented, and misnamed unsupported
   content has public-stage rejection coverage. Appending detected MIME would
   be useful additional detail, not a missing correctness guarantee. Retain the
   tested diagnostic and shared reader's documented limitations.
3. **In-place base64 scanning:** optional optimization. Inspection of
   `NativeQuestions.scala:227-238` and
   `OpenAIAttachmentUtils.scala:207-218` confirms the repeated validation and
   temporary substring; no correctness problem or measured regression was
   demonstrated. Keep the final native validator complete for explicit input
   as well as generated messages. A shared parser API/performance change is
   outside this focused fix and would need separate measurement.
4. **Image-limit messages:** duplicate of round 2. All checks use `MaxImages`
   for acceptance and accurately report the current 128 limit. Tests cover
   shared-input, question and combined 128/129 boundaries. Unifying wording
   and interpolation is optional maintenance, not a fix to a wrong limit.

Source and test evidence cited by this lens and the driver agree. Production
remains unchanged. The accepted round-5 durable-coverage additions invalidate
this pass for the next test snapshot; this artifact is preserved as historical
evidence, not signoff for that subsequent patch.