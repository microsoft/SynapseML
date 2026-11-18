---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 6
timestamp: "2026-11-18T05:46:59Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3b8dfd6ac65551492d62b2ca4b24a7586ccbfc56527cd6ea04010bb4381442fc"
---

I found five Low findings (three marked optional) and no correctness defects; the Decisions doc claims I checked match the code. I didn't run tests, and I couldn't check whether the artifact already exists because listing `reviews/` is off-limits.

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-18-stage-attempt-1-review-6-decisions-paths-opus.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff complete:** `.review-input-decisions-paths.diff` has 13 `diff --git` headers, matching the supplied patch. I read every production, test and doc hunk; none were truncated.
- [x] **Decisions pre-IO order:** checks run in this order: `selectedModel`, `requestVersion`, `mergedOptions`, questions null/validate, then `validateDecisionsImageCount` (`AIRequestAdapter.scala` 589). Only then does `messages()` (590) trigger attachment IO, followed by `prepareDecisionsRequest` (595).
  - `AIPrompt.scala` 474–478 keeps `inputMessages` lazy for Decisions only.
  - The catch block (482–494) never forces it.
  - Chat/Responses use the eager overload (`AIRequestAdapter.scala` 610–611).
- [x] **Prompt-text error parity:** `AIPrompt.scala` 377–384 matches the non-path flow (`AIRequestAdapter.scala` 535–542, `NativeQuestions.scala` 32–35). Null or empty gives `missing_input` "input is null or empty"; whitespace-only gives `invalid_request`.
- [x] **Rows with only blank path cells:** they get `missing_input` via `AIPrompt.scala` 353–361.
- [x] **Attachment gates:** inspected `OpenAIPrompt.scala` 605–654 and `OpenAIAttachmentUtils.scala` 173–188, 295–318 and 431–442.
  - The extension check lowercases and uses only the last URI path segment.
  - Data-URL classification reads only the metadata.
  - `imageExtensions` is the private shared set at `OpenAIPrompt.scala` 313.
- [x] **Base64 cost:** inspected `NativeQuestions.scala` 140–163 and 215–244, and `OpenAIAttachmentUtils.scala` 207–219, 221–239, 444–466 and 490–512.
- [x] **Hosted URL check:** `NativeQuestions.scala` 203–213 never echoes the URL and returns a generic message on failure. The test pin is at `AIPromptMediaInputSuite.scala` 154–157.
- [x] **Documentation accuracy:** I compared the new sections of `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` with the code. This covered `fileSizeLimitMB` semantics, `inputMode`, the `messagesCol` conflict (`AIRequestAdapter.scala` 276–280), durable inline-only images, the capability rows at 740–741 and the live-suite note at 435–440. All match.
  - Template filenames come from `AIPrompt.scala` 425 and `OpenAIPrompt.scala` 318.
  - `docs/Explore Algorithms/OpenAI/AIPrompt.ipynb` (391–395, 862–890) has no stale Decisions claims.
  - The Param descriptions at `OpenAIPrompt.scala` 241–270 don't depend on the API.
- [x] **Live suite:** the new `OpenAIDecisionsLiveSuite.scala` test skips through `assume` when credentials or `SYNAPSEML_DECISIONS_IMAGE_URL` are missing, and its failure message hides the error contents.
- [x] **Tests pinning diagnostics:**
  - `AIPromptMediaInputSuite.scala` 370, 439/466 and 491/506 assert exact messages.
  - The 128-image checks (`AIPromptMediaInputSuite.scala` 547 and 661, `NativeQuestionsSuite.scala` 138–141) use `contains`.
- [ ] **Tests not run:** this review was read-only. The only validation evidence is what the driver supplied.
- [ ] **Artifact existence not checked:** listing or searching `reviews/` is prohibited by the independence rules.

## Issues

### Issue 1: Decisions image rejection messages don't say which rule failed
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`
- **Line(s)**: OpenAIPrompt.scala 628–629, 634–635, 645, 649; NativeQuestions.scala 229–230, 240
- **Description**: Three separate gates throw the same message, "Decisions path columns only support JPEG, PNG, GIF, and WebP images":
  - the data-URL media-type check (634–635);
  - the filename-extension preflight (645);
  - the MIME check after the file is read (649).

  The tests pin this message for content that is actually a supported format: a `data:image/jpg` JPEG (`AIPromptMediaInputSuite.scala` 439/466) and a hosted `/image.ashx` that serves PNG bytes (491/506). The user is told only JPEG/PNG are supported, with no hint that the extension or declared media type is the problem. Row errors deliberately omit the path, so the message is the only clue. Likewise, `https:///image.png` or an HTTPS URL with illegal characters gets "Decisions images must use HTTP(S) URLs or inline base64 image data URLs" (154–157), even though the scheme is HTTPS.
- **Risk**: Users may misdiagnose the failure and support load goes up. Users may re-encode valid images instead of renaming the file, fixing the media type, or using an explicit `input_image` part. This affects diagnostics only.
- **Suggested Fix**: Keep the leading sentence that names the four formats, and append a short hint for each cause that doesn't include the path:
  - Extension: the filename extension must be .jpg, .jpeg, .png, .gif or .webp, or omitted; use an explicit `input_image` part for other URLs.
  - Data URL: the media type must be image/jpeg, image/png, image/gif or image/webp.
  - Content: the detected content is not a supported image.
  - Hosted URLs: "must be absolute HTTP(S) URLs with a host, or inline base64 image data URLs".

  Update the exact-match assertions at `AIPromptMediaInputSuite.scala` 156, 370, 466 and 506.

### Issue 2: Generated path images are copied and validated a second time on every row
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`
- **Line(s)**: 141, 152, 231–238 (called from `AIRequestAdapter.scala` 595; payloads produced at `OpenAIAttachmentUtils.scala` 228–232 and 498–501)
- **Description**: Every Decisions path image goes through `prepareDecisionsRequest`, then `validateDecisionsInput`, then `validatePart`. That step copies the whole base64 body (`url.substring(comma + 1)`) and scans every character through a generic iterator (`OpenAIAttachmentUtils.scala` 207–219). The scan adds nothing in either case:
  - For file/HTTP paths, `Base64.getEncoder` just produced the data URL (498–501), so it is valid by construction.
  - For data-URL path cells, `prepareDataUrlAttachment` already validated the same payload (232).

  The PR avoided copying the body in `dataUrlFileType` (`OpenAIAttachmentUtils.scala` 437), but this later step brings the copy back for every image.
- **Risk**: Each image adds about 4/3 of its size in temporary allocation, plus an O(n) scan, per row. With `fileSizeLimitMB` unset, reads are unbounded, and a row may carry up to 128 images, so heavy image workloads see more executor heap and GC pressure. Correctness is unaffected, and I haven't measured the cost.
- **Suggested Fix**: Validate the payload in place:
  - Add an offset-aware `validateBase64Content(value: CharSequence, start: Int)` that loops over indices with `charAt`.
  - Call it from `validatePart` with `comma + 1`, and replace `encoded.nonEmpty` with `url.length > comma + 1`.

  Alternatively, check generated parts structurally (and count them) without re-scanning the base64.

### Issue 3: optional - The Decisions-only image check has a generic name and an inline, duplicated allowlist
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
- **Line(s)**: OpenAIPrompt.scala 313, 628–629, 634–635, 645, 649; OpenAIAttachmentUtils.scala 315–318
- **Description**: `validateImageAttachment` sounds like an image check for every API, but it does nothing unless `apiType` is `decisions`. Its by-name parameter is what keeps the extra classification work out of Chat/Responses. The supported-format rule is spread across three places:
  - an inline `Set(...)` literal at 649, rebuilt for every Decisions file;
  - the message text at 629;
  - the shared `imageExtensions` at 313.

  `mayBeImagePath` also doesn't say that names without an extension pass.
- **Risk**: The pieces can drift apart. For example, adding `bmp` to `imageExtensions` for Chat would make Decisions read `.bmp` files before rejecting them. That would quietly break the documented promise that unsupported extensions are rejected without reading the file.
- **Suggested Fix**:
  - Rename the method to `requireDecisionsImage(isSupported: => Boolean)` and add a note that the argument must not be evaluated for other APIs.
  - Move the format list into named constants (`DecisionsImageMimeTypes`, and optionally a Decisions-owned extension set) that both the check and the message use.
  - Rename `mayBeImagePath` to something like `hasImageOrNoExtension`.

### Issue 4: optional - New limit messages and the RIFF check use bare numbers next to named constants
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
- **Line(s)**: NativeQuestions.scala 15, 118, 136, 166–167; OpenAIAttachmentUtils.scala 122–123, 297–299
- **Description**: The new limit messages hard-code "128" even though the `MaxImages` constant exists. One limit now has two wordings: "at most 128 images" (136) and "at most 128 images per request" (118, 167). In `inferMimeType`, the tag length is a bare `4` used twice, and `WebpHeaderLength = 12` silently equals `RiffFormatOffset + 4`.
- **Risk**: A future change could update the limit but not the message, and users already see two phrasings of one limit.
- **Suggested Fix**:
  - Add `ImageLimitMessage = s"Decisions accepts at most $MaxImages images per request"` and use it at 118, 136 and 167. The tests use `contains`, so they still pass.
  - Add `RiffTagLength = 4` and derive `WebpHeaderLength` from it.

### Issue 5: optional - The deferred-IO function shares the `messages` name, and the pre-IO ordering is undocumented in code
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`
- **Line(s)**: 249, 582–597, 610–628
- **Description**: Both `request` overloads and `nativeEnvelope` name their parameter `messages`, whether it is an eager `Option[Seq[Row]]` or a `() => Option[Seq[Row]]` that, for Decisions path columns, performs file/HTTP reads. The documented guarantee that cheap checks run before IO depends only on statement order: `messages()` at 590 must stay after line 589, and no comment says so. Separately, the name `decisionsSettings` (249) doesn't convey that it lists the only Chat/Responses-only settings Decisions may also use.
- **Risk**: A refactor could move `messages()` earlier and quietly put IO back ahead of validation. The tests would catch it, but the intent isn't visible where the code would be edited.
- **Suggested Fix**: Rename the function parameter to `loadMessages` and add a comment at 590: "May read path attachments; keep after all row-level pre-IO checks". Rename `decisionsSettings` to something like `decisionsAllowedGenerativeParams`.

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

## Driver disposition

All five Low recommendations are closed without implementation for this focused
fix. The original findings and `ISSUES_FOUND` verdict remain unchanged. No
functional defect was reported; no proposed optimization or diagnostic change
is represented as completed.

1. **More specific image errors:** duplicate diagnostic refinement. Current
   errors accurately reject unsupported path representations or malformed
   absolute URLs without disclosing caller values. Recognized extensions,
   standard JPEG MIME naming, extensionless sniffing and explicit URL
   alternatives are documented. Cause-specific hints could help, but are not
   required for correct rejection, routing or sanitization.
2. **Base64 copies/scans:** duplicate optional performance work. The native
   validator remains complete for generated and caller-supplied content.
   The extra substring/scan is acknowledged; no measured regression or
   correctness problem was demonstrated. Changing the shared validator API
   or trusting generated payloads needs a separate measured optimization.
3. **Names and format constants:** duplicate optional architecture suggestions.
   The current extension/MIME checks agree, and by-name gating preserves the
   other APIs. No future format expansion is included in this patch.
4. **Numeric constants:** the actual image limit and all current messages agree
   at 128; boundaries are covered. RIFF identifiers are fixed four-byte format
   tags, with the tested WEBP offset/header-length check. Additional constants
   or interpolated wording are maintenance alternatives, not current fixes.
5. **Thunk naming/comments:** optional naming change. The function type, explicit
   invocation position, public pre-IO tests and documented ordering expose the
   deferred behavior. Renaming internal parameters and adding commentary does
   not close a demonstrated behavioral gap.

The driver inspected the cited native/attachment and request-validation paths.
All six independent lenses read the same 13-file frozen patch. Round 1 was
clean; rounds 2-6 retain their original findings with explicit no-change scope
dispositions. This is completion with driver dispositions, not six reviewer
CLEAN verdicts and not a final CI-qualified pass.

Local evidence remains 290 unique deterministic passes across four runs,
including the latest 12-test durable suite and required Scala gates. Provider
E2E is unverified. Product content did not change during this pass:
`1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22`.