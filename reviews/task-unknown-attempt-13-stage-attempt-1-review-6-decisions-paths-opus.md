---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 6
timestamp: "2026-11-17T17:36:24Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bd60b0547106cb7fc803c52c5368d3fd8847ec01f156536c1061f791a0c98581"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-13-stage-attempt-1-review-6-decisions-paths-opus.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied ten-file diff. Re-read the frozen sources for each invariant in this lens:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`: 241–275, 305–320, 596–658
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`: 121–153, 173–238, 290–529
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`: 1–245
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`: 240–290, 560–625
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`: 352–386, 453–499
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala`: 84–91, 172
- [x] Checked validation ordering and repeated work:
  - In `AIPrompt.prepareRequests`, `inputMessages` is lazy. Decisions forces it only through the `nativeEnvelope` thunk, after model/version/options/questions checks and `validateDecisionsImageCount`. Other APIs still force it before `request`.
  - `retainGeneratedMessages` is false for Decisions, so images are never loaded twice.
  - The pre-IO `pathImageCount` uses the same null/trim rule as `createMessagesForRow` (OpenAIPrompt.scala:608–613).
- [x] Confirmed `prepareFile` (OpenAIAttachmentUtils.scala:398–426) returns unsupported types without throwing. The Decisions-only gate (OpenAIPrompt.scala:628–629, 645, 649) therefore produces the message users see.
- [x] Confirmed the pre-read extension guard does not misread signed URLs. `pathFilename` (OpenAIAttachmentUtils.scala:173–179) drops query strings via `URI.getPath`, so signed URLs are classified by path extension and query values never appear in errors.
- [x] Confirmed the WebP/RIFF fix (OpenAIAttachmentUtils.scala:121–123, 295–308):
  - It is an O(1) 12-byte header check, and its comment is accurate.
  - RIFF/WAVE still reaches the JDK classifier, so WAV stays audio; `AIPromptMediaInputSuite.scala:326–358` covers this for all four stages.
- [x] Compared the docs (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`: 244–325, 717–731) with the code. Accurate: hosted URLs forwarded without fetching, the Hadoop query/percent limitation, `missing_input`/`invalid_request` ordering, blank path cells, durable inline-only rules and the capability table.
- [x] Grepped `cognitive/src/main` and `docs/Explore Algorithms/OpenAI` for stale "inline-only Decisions" wording. Only durable-mode statements remain, and they are correct.
- [x] Checked which assertions would change if messages change:
  - Exact matches: AIPromptMediaInputSuite.scala:348, 438–439, 477–478.
  - Substring matches (`contains`): AIPromptMediaInputSuite.scala:507, 581, 530–531.
  - NativeQuestionsSuite.scala:202–235 intercepts only the exception type and also asserts that errors do not disclose supplied values.
- [x] Observability: row failures still map to errorCol codes (`AttachmentReadException`→`input_io_error`, `AIMissingInputException`→`missing_input`, `IllegalArgumentException`→`invalid_request`). `logTransform` is unchanged and no new logging is needed.
- [x] Independence: I did not read `reviews/**`, `.git/**` or any sibling or prior review. Content searches stayed within `cognitive/src` and `docs/Explore Algorithms/OpenAI`.
- [ ] I did not run tests, ScalaStyle, compile or codegen; my tools are read-only. Their pass status comes only from the driver's evidence.
- [ ] I did not check the external OpenAI guide anchors (Unified AI prompting.md:347–348); I have no network access.
- [ ] I did not check whether the assigned artifact already exists, because listing or viewing `reviews/` is prohibited in this pass.

## Issues

### Issue 1: The Decisions format error does not say which extension or type was rejected
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628–629, 634–635, 645, 649
- **Description**: All three Decisions format gates raise the same fixed text: "Decisions path columns only support JPEG, PNG, GIF, and WebP images". The gates are the data-URL classification, the pre-read extension guard and the post-read MIME allowlist. None of them names the extension or MIME type that caused the rejection.
  - Chat and Responses use the same reader but do report the cause. For the same extensionless WAV, Chat reports "File type 'audio' with MIME type 'audio/x-wav'" and Responses reports that audio is unsupported; Decisions only lists the allowed formats (AIPromptMediaInputSuite.scala:346–350).
  - The most confusing case is one the docs already anticipate (Unified AI prompting.md:313–315). A real JPEG named `.jfif`/`.jpe`, or an extensionless JPEG whose header the runtime does not recognize (`application/octet-stream`), is told that only JPEG is supported.
- **Risk**: Row-level `invalid_request` errors are hard to triage at scale. Users cannot tell a wrong extension from an unrecognized header or a non-image file without re-reading the docs or opening the file.
- **Suggested Fix**: Add the cause to the message without exposing caller data:
  - Pre-read guard: the lower-cased extension, e.g. "...; found extension '.jfif'".
  - Post-read gate: the detected MIME type. File/HTTP detection only yields values from fixed JDK/registry sets.
  - Data URLs: the declared type, as Responses already does with `Unsupported file type: $mimeType`.

  Never include the path or URL; the non-disclosure check at AIPromptMediaInputSuite.scala:441 must keep passing. Update the exact-message assertions at AIPromptMediaInputSuite.scala:348, 438–439 and 477–478. Optional extras:
  - Reword Unified AI prompting.md:318 to "Any other filename extension is rejected without reading the file"; "Known unsupported" reads like a denylist.
  - Rename the WAV test's `imageServer` (line 330), since it serves audio.

### Issue 2: `fileSizeLimitMB` without a path column is reported as unsupported by Decisions
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: 248–255
- **Description**: This PR makes `fileSizeLimitMB` valid for Decisions when `columnTypes` has a `"path"` column. The docs table says so (Unified AI prompting.md:729), and the doc example sets it (line 279). Without a path column, `validateCapabilities` still throws the old generic "fileSizeLimitMB is not supported by apiType='decisions'". That was true before this PR; it now contradicts the docs.
- **Risk**: The likely cause is a forgotten `columnTypes` entry, or an image column left as `"text"`. The message suggests removing `fileSizeLimitMB`. Once removed, the image column's path or URL is silently sent as prompt text and no image is attached.
- **Suggested Fix**: Give Decisions a message that names the requirement, e.g. "fileSizeLimitMB applies to apiType='decisions' only when generated messages include a columnTypes 'path' column". The same wording also covers the `messagesCol` case, so users don't hit a second error after fixing the first. The tests only assert `.contains("fileSizeLimitMB")` (AIPromptMediaInputSuite.scala:519–520, 530–531), so they still pass. Optionally rename the Boolean `decisionsPaths` to `hasDecisionsPathColumns`.

### Issue 3: Malformed hosted image URLs get an error about the URL scheme
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 203–213, 239–241
- **Description**: `hostedImageUrl` turns every parse failure into `false` through `Try(...).getOrElse(false)`. That covers unescaped spaces, invalid percent escapes, signed or non-ASCII ports and empty hosts. `validatePart` then reports "Decisions images must use HTTP(S) URLs or inline base64 image data URLs". For inputs such as `https://example.invalid/image with spaces.png` or `https://example.invalid:-1/image.png` (rejected by design, NativeQuestionsSuite.scala:204–214), the caller did use HTTP(S).
- **Risk**: Blob URLs pasted with unencoded spaces are common. Every affected row then gets an `invalid_request` telling the user to change the scheme instead of escaping the URL, which slows triage on large jobs.
- **Suggested Fix**: If the URL has an `http`/`https` scheme (case-insensitive) but fails validation, raise a separate message that does not echo the URL. For example: "Decisions HTTP(S) image URLs must be valid absolute URIs with a nonempty host and numeric port; percent-encode spaces and reserved characters". Keep the current message for other schemes. The tests check only the exception type and non-disclosure, so no assertion changes are needed. Optionally rename the predicate to `isValidHostedImageUrl`.

### Issue 4: The 128-image limit has two message variants, hard-coded literals and no counts
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 136, 165–167
- **Description**: The docs describe one request-wide limit (Unified AI prompting.md:259–260), but the code reports it in two ways:
  - The input-only check in `validateDecisionsInput` (136) says "Decisions accepts at most 128 images".
  - The new question-only check (118) and combined check (167) say "...per request".

  All three repeat the literal `128` instead of using `MaxImages`, and none reports how many images were found or where they came from. With path columns, the pre-IO combined check can fail when input images alone are under 128 but question images push the total over. Nothing in the message says that question images count toward the limit.
- **Risk**: One limit gets inconsistent diagnostics, and the combined case is hard to understand without counts. If `MaxImages` changes, the messages will be wrong.
- **Suggested Fix**: Build one message from `MaxImages` that includes counts, e.g. `s"Decisions accepts at most $MaxImages images per request across input and question instructions; found $inputImages input and $questionImages question images"`. Use it in all three checks; the input-only check reports only its input count. Counts are not caller content. The `.contains("at most 128 images")` assertions (AIPromptMediaInputSuite.scala:507, 581) still pass.

### Issue 5: The Decisions image allowlist is spread across three places behind a generic helper name
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 313, 628–629, 634–635, 649
- **Description**: The shared `wrapFileToMessagesList` calls `validateImageAttachment(...)` on every Chat, Responses and Decisions attachment. The name suggests every attachment must be an image, but the helper does nothing unless the apiType is Decisions. The allowlist itself appears in three forms:
  - An unnamed inline `Set("image/jpeg", "image/png", "image/gif", "image/webp")` (649).
  - For data URLs, `dataUrlFileType(...) == "image"` (634–635), which derives the list from the shared Chat/Responses tables `imageExtensions` (313) and `MimeTypeExtensions` (OpenAIAttachmentUtils.scala:137–153).
  - A hand-written list in the error message (629).
- **Risk**: If Chat/Responses later add an image type to those shared tables, Decisions data URLs would start accepting it. That would contradict the error message and docs, while the file branch still rejects the same type. Tests cover only TIFF, BMP and X-bitmap (AIPromptMediaInputSuite.scala:416–418). The helper's name also misleads readers of the shared method about its scope.
- **Suggested Fix**:
  - Rename the helper to `requireDecisionsImage`.
  - Add a `private val DecisionsImageMimeTypes` constant and use it for both the post-read check and data URLs. For data URLs, compare the lower-cased declared MIME type from a header-only helper; `dataUrlFileType` already parses the header without copying the body.
  - Build the message's format list from the same constant.
  - Keep the by-name predicate so Chat/Responses never evaluate it.

  Behavior is unchanged.

### Issue 6: Each path-loaded image is copied and base64-scanned again while preparing the request
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 227–238 (reached via 151–157 from `AIRequestAdapter.nativeEnvelope`)
- **Description**: Every image from a Decisions path column now goes through `prepareDecisionsRequest` → `validateDecisionsInput` → `validatePart`. That code copies the whole base64 body with `url.substring(comma + 1)` (231) and scans it again with `validateBase64Content` (233). For path images this work is redundant:
  - File/HTTP paths: the payload was just produced by `Base64.getEncoder` (OpenAIAttachmentUtils.scala:500).
  - Inline path data URLs: `prepareDataUrlAttachment` already validated it (OpenAIAttachmentUtils.scala:232).

  The copy is also avoidable for explicit image parts. Elsewhere the codebase avoids this kind of copy; `dataUrlFileType` notes it classifies "without copying the potentially large base64 body" (OpenAIAttachmentUtils.scala:437).
- **Risk**: Each image costs one extra payload-sized temporary string plus another O(n) pass per request. A request can carry up to 128 images of up to `fileSizeLimitMB` each, so this adds executor heap and GC pressure on top of the existing encode and serialize copies. This is not a correctness defect.
- **Suggested Fix** (optional): Add an index-range overload, e.g. `validateBase64Content(value: String, start: Int)`, that loops over characters without `substring`. Use it in `validatePart`, with `comma + 1 < url.length` as the non-empty check. Acceptance stays identical, and the existing test "Decisions inline image syntax matches the basic base64 decoder without exposing content" still checks equivalence. Validation still covers the same inputs.

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

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver dispositions

The original findings and `ISSUES_FOUND` verdict above are preserved. No
production changes are made for these six non-blocking suggestions.

1. **Not adopted: richer format diagnostics.** The current diagnostic names
   every accepted format and intentionally never echoes an untrusted extension,
   MIME declaration, path, or URL. The documentation explains filename and
   standard MIME requirements. A fixed, safe diagnostic is appropriate for
   this compatibility change; richer diagnostics are optional.
2. **Not adopted: conditional capability wording.** The existing capability
   validator rejects an ineffective setting in the supplied configuration and
   identifies `fileSizeLimitMB`. Documentation and the example explicitly require
   path columns, and public-stage negative tests verify rejection rather than
   ignoring the setting. The suggested wording would improve guidance, not
   correct validation or request construction.
3. **Not adopted: URL-error specialization.** The message states accepted URL
   forms without reflecting the rejected value. Parsing errors and unsupported
   schemes both remain controlled `invalid_request` failures. A more detailed
   malformed-URI diagnostic is useful but not required for acceptance, unchanged
   forwarding, or non-disclosure.
4. **Not adopted: count-message consolidation.** All enforcement already uses
   `MaxImages`; both messages accurately state the same current limit.
   Question-only, input-only and combined checks protect separate entry points.
   This duplicates round 2's non-blocking cleanup finding. The batched tests add
   two-path boundary coverage without changing diagnostic compatibility.
5. **Not adopted: allowlist redesign.** This duplicates round 2's extensibility
   observation. Current sets agree; existing negative tests reject BMP, TIFF,
   X-bitmap and documents, and the batched additions cover JPEG/GIF positive
   paths and the nonstandard `image/jpg` negative case. A future shared-format
   extension must preserve those Decisions tests. No current policy drift is
   demonstrated.
6. **Deferred: range-based base64 scanning.** The copy/scan cost is real, but
   this is explicitly optional performance work without a demonstrated
   regression threshold. Keeping final request validation also protects
   externally supplied parts. Changing a shared parser or adding trusted-input
   shortcuts is outside this focused image-support patch; callers should set
   finite file limits and bound explicit payloads, as documented.

Verification: driver checked the named guards, message strings, shared image
tables, and public-stage assertions against the frozen pass-13 source.
These dispositions are not a substituted clean reviewer verdict or a claim
that the later documentation/test patch has already been reviewed.