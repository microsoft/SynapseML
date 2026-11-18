---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 6
timestamp: "2026-11-17T20:05:49Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f0a1dfc9f60f55087123befca8c1649982b6f03c8644e464a4713b51115eca0f"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-14-stage-attempt-1-review-6-decisions-paths-opus.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`, including `hostedImageUrl` (203–213), `validatePart` (215–245), `prepareDecisionsRequest`, `validateDecisionsImageCount`, `questionImageCount`, `imageCount` and `prepareParts`.
  - The port comment correctly describes `java.net.URL`, which accepts signed and Unicode-digit ports.
  - Constant questions are validated once. Per row, the only added work is `prepareParts` plus two linear image counts.
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`: `messages()` (361–386), `prepareRequests` (453–500), `useBoundedAttachments` (218) and `retainedMessagesName` (293).
  - For Decisions, the lazy `inputMessages` is forced only inside `adapter.request`.
  - Retained messages apply only to the generative APIs, so Decisions does not trigger IO outside the adapter.
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`: `validateCapabilities` (247–264), `input()` (530), `nativeEnvelope` (571–587) and both `request` overloads (599–620).
  - Validation order is model, then version, then options, then questions, then image count, then prompt text, then IO. This matches the docs at lines 303–308.
  - Text-only and path-column Decisions rows both report missing text as `input is null or empty`.
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`: `pathFilename`/`attachmentFilename` (173–188), RIFF/WEBP detection (about 295–308), `fileExtension`/`mayBeImagePath` (310–318), `readBoundedFile`, `dataUrlFileType`, and the Chat/Responses message builders.
  - The WebP constants and comment are accurate.
  - The extension comes from the last URI path segment, so dots in directory names or hosts do not cause a rejection before IO.
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` at 225–330 and 540–700: `createMessagesForRow` (602–618), `validateImageAttachment` (628–629) and `wrapFileToMessagesList` (631–660). The by-name predicate keeps the Decisions checks out of Chat/Responses.
- [x] Read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala`. The durable inline-only rule is enforced for path columns and for question images (both scalar `questions` and `questionsCol`), as documented.
- [x] Checked `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (95–440 and 700–760) against the code:
  - An unset or infinite `fileSizeLimitMB` reads the whole file; a finite limit reads limit + 1 bytes.
  - The documented `missing_input` and `invalid_request` behavior and the pre-IO checks match the code.
  - The `columnTypes`, `fileSizeLimitMB` and `systemPrompt`/`dropPrompt` table rows (735–737) match `validateCapabilities`.
- [x] Performance: no material regression found.
  - Lazy messages avoid image IO when a row fails an earlier check.
  - The remaining extra work is one more base64 validation pass over SynapseML-generated data URLs in `validatePart`, plus two linear image counts. I did not flag it.
- [x] `scalastyle-config.xml` sets `maxFileLength` 800, method length 60 and line length 120. `OpenAIPrompt.scala` is exactly 800 lines. The others are `AIPrompt.scala` 666, `AIRequestAdapter.scala` 624, `OpenAIAttachmentUtils.scala` 530 and `NativeQuestions.scala` 246.
- [x] Searched `cognitive/src/main/python` and `docs/Explore Algorithms/OpenAI` for outdated Decisions statements ("hosted image", "inline-only", `fileSizeLimitMB`, `columnTypes`, WebP). None found.
- [x] Checked how tests assert these messages:
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptPreflightSuite.scala` (61–95) checks unused settings only with `.contains(name)`.
  - `AIPromptMediaInputSuite` compares the generic Decisions format message exactly, so Issue 1's fix must update it.
  - The 128-image tests use `.contains("at most 128 images")`.
- [x] Independence: did not open `reviews/**`, `.git/**`, session directories or other reviewers' output.
- [ ] No tests, builds or commands were run in this read-only lens. I relied on the validation evidence the driver supplied and did not reproduce it. External OpenAI documentation links were not checked because no network access was available.

## Issues

### Issue 1: Decisions format errors omit the detected type and which check failed
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628–629 (message); call sites 634–635, 645, 649
- **Description**: Three different failures produce the same message, `Decisions path columns only support JPEG, PNG, GIF, and WebP images`:
  - A filename extension rejected before IO.
  - A data URL whose declared MIME type is not a standard image type.
  - File content whose detected MIME type is outside the allowlist.

  The message sometimes contradicts the input. A JPEG named `.jfif`, or `data:image/jpg;base64,...`, is rejected with text saying JPEG is supported. The same happens for the misnamed `.png` (X-bitmap) and extensionless WAV fixtures. The Chat/Responses errors from the same reader include the MIME type. With several path columns, the per-row error does not show which value failed or why.
- **Risk**: Users cannot diagnose failures at scale. A user may conclude the service rejects JPEG/PNG, when the real cause is the extension, a non-standard MIME type such as `image/jpg`, or content that does not match the extension.
- **Suggested Fix**: Pass a short detail into the validator and append it to the message, for example `; extension '.jfif' is not accepted`, `; declared MIME type 'image/jpg'` or `; detected MIME type 'audio/x-wav'`. Never include the path or URL, which may carry tokens. Update the exact-match assertions in `AIPromptMediaInputSuite`.

### Issue 2: `fileSizeLimitMB` error says "not supported" for Decisions when it is only conditionally supported
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: 248–255
- **Description**: For Decisions with no `path` entry in `columnTypes` (including text-only mappings) or with `messagesCol`, setting `fileSizeLimitMB` fails with `fileSizeLimitMB is not supported by apiType='decisions'`. The docs table (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, line 737) says Decisions supports it with image path columns. The actual cause is the missing path column or the use of `messagesCol`, not the API type.
- **Risk**: The message contradicts the docs. Users may drop the limit instead of adding a path mapping, which leaves reads uncapped.
- **Suggested Fix**: Special-case this message for Decisions, for example `fileSizeLimitMB requires inputCol/promptTemplate with a columnTypes 'path' column for apiType='decisions'`. Keep the generic message for the other settings. The current `.contains(name)` assertions still pass; add one exact-message assertion.

### Issue 3: Two unclear sentences in the new Decisions docs
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Unified AI prompting.md
- **Line(s)**: 307–308, 320
- **Description**:
  - "Header validation still happens after attachment loading" sits between the list of pre-IO checks and a paragraph about image-header recognition. It refers to HTTP request header values bound from row columns (validated in `buildRequest`), not image file headers, but readers can easily take it the other way.
  - "Known unsupported filename extensions are rejected without reading the file" understates the rule. `mayBeImagePath` (`OpenAIAttachmentUtils.scala` 315–318) rejects any non-empty final-segment extension other than jpg/jpeg/png/gif/webp, including unknown ones such as `.ashx`, `.php` or `.v2`.
- **Risk**: Users with dynamic image URLs (for example `image.ashx`) may expect the content check to accept them. They need to know such URLs fail before any read and should be sent as explicit `input_image` parts.
- **Suggested Fix**: Reword as "Row-level HTTP header values (for example credentials bound from columns) are still validated after attachment loading." and "Filenames with any other extension, including unrecognized ones such as `.ashx`, are rejected without reading the file; send such URLs as explicit `input_image` parts."

### Issue 4: Decisions-only image rules live in the legacy base class under generic names, in a file at the line limit
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: OpenAIPrompt.scala 611–616, 628–629, 649; AIRequestAdapter.scala 248–250
- **Description**:
  - `validateImageAttachment` sounds general, but it enforces only for Decisions and does nothing for every other API.
  - The rule "JPEG, PNG, GIF and WebP" is written in three places that can drift apart:
    - the inline MIME set at line 649;
    - `imageExtensions`, used through `mayBeImagePath` and `dataUrlFileType`;
    - the hard-coded message.

    For example, adding an extension to `imageExtensions` for Chat would widen the Decisions extension and data-URL checks but not the file-content allowlist.
  - `OpenAIPrompt.scala` is exactly at ScalaStyle's 800-line limit. The patch removed the comments explaining the `null` returned at 611–612, which `AIPrompt.generatedMessages` (`AIPrompt.scala`:361) turns into `attachment input is missing`.
  - In `AIRequestAdapter`, `decisionsPaths` is a Boolean but reads like a collection. `decisionsOptions` is a set of allowed parameters, but elsewhere in that file "options" means request-body options (`bodyOptions`, `allowedOptions`, `mergedOptions`).
- **Risk**: The rules can drift apart, maintainers can misread the code, and any later fix in `OpenAIPrompt.scala` must delete code or comments to pass ScalaStyle.
- **Suggested Fix**:
  - Minimal fix:
    - Rename the helper to `requireDecisionsImage`.
    - Add a named Decisions MIME constant in `OpenAIAttachmentUtils` (530 lines) and build the message from it.
    - Rename the flags to `hasDecisionsPathColumns` and `decisionsSupportedSettings`.
    - Restore a one-line comment on the `null`.
  - Optional alternative, not a defect: a protected hook in `AIPrompt`, like `useBoundedAttachments`, to free up lines in `OpenAIPrompt.scala`.

### Issue 5: The same 128-image limit has two different messages
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 118, 136, 167
- **Description**: The existing input-only check (136) says `Decisions accepts at most 128 images`. The new question check (118) and the combined check (167) say `... per request`. All three hard-code `128` instead of using `MaxImages`.
- **Risk**: Readers may think there are two separate limits, log searches and alerts must match both strings, and the text can go stale if `MaxImages` changes.
- **Suggested Fix**: Define `private val TooManyImages = s"Decisions accepts at most $MaxImages images per request"` and use it at all three places. The existing `.contains("at most 128 images")` assertions still pass.

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

## Final driver dispositions

These updates supersede the pending entries. Preserve the original
`ISSUES_FOUND` verdict; pass 14 does not qualify the corrected snapshot.

- **Issue 1 - optional diagnostic detail, deferred.** The existing diagnostic
  names the accepted image formats without echoing user-controlled paths,
  URLs or MIME strings. The adjacent documentation explicitly covers supported
  extensions, JPEG aliases, MIME spelling and content inspection. More granular
  detection-phase diagnostics can be added independently; serialization and
  rejection behavior are already pinned.
- **Issue 2 - fixed.** A missing path mapping now receives the specific
  `fileSizeLimitMB` prerequisite; `messagesCol` receives its construction
  conflict. Four baseline failures demonstrated the overlap/diagnostic gaps.
  Exact-message tests pass for both public stages and durable preflight.
- **Issue 3 - fixed.** Documentation now says row-level HTTP header values, and
  explicitly rejects every other filename extension, including `.ashx`, before
  IO. It points dynamic URLs to explicit image parts.
- **Issue 4 - naming clarified; remaining refactor deferred.** The adapter uses
  `hasDecisionsPathColumns` and `decisionsSettings`. The shared reader's
  Decisions-only predicate remains guarded and tested; broader extraction of
  attachment policies is not required for this bounded compatibility fix.
  ScalaStyle passes without suppressions.
- **Issue 5 - optional wording consolidation, deferred.** Both existing messages
  describe the same verified request limit. Count-boundary tests pin behavior;
  retaining the pre-existing input-only wording is not a second limit.

The corrected 13-file patch passed 284 deterministic tests across 12 suites,
root ScalaStyle/test ScalaStyle, compile/test compile, codegen, wrapper parsing
and six documentation examples. Three live tests canceled without credentials;
no provider E2E is claimed. Validated patch SHA-256:
`e37ef3864d28d028a4ab89f9425fe4000fbd2e0da319fb3087d70b94145b7b19`.