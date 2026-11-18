---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 4
timestamp: "2026-11-17T16:57:34Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ca6d92ddef2704a7dab199ed3b29a9533652d4bddb7c17715e3b599e5938683d"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-13-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Empty or missing prompt text fails before image IO.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`, lines 364-386 (`messages`):
  - When Decisions has path columns, null or `""` raises `AIMissingInputException("input is null or empty")`. That is the same code and text that `AIRequestAdapter.input` uses for Decisions without paths (`AIRequestAdapter.scala` 524-532).
  - Whitespace-only text reaches `NativeQuestions.text` and returns invalid_request ("Native question or input text must be nonempty").
  - Both errors are raised before `generatedMessages` (353-362) reads any attachment.
  - The new `.filter` does nothing for other APIs, so Chat/Responses still report "prompt is null".
- [x] **Lazy message building is limited to Decisions and runs at most once.** In `AIPrompt.scala` 474-478, the `lazy val` plus `() => inputMessages` thunk is used only when `apiType == "decisions"`.
  - Other APIs call the three-argument overload (`AIRequestAdapter.scala` 599-600). Passing the lazy value forces it before `request` runs, so preparation stays eager as before.
  - `retainedMessagesName` (`AIPrompt.scala` 295-300) applies only to generative APIs. The thunk therefore runs once, at `nativeEnvelope` line 579, and never runs if an earlier check throws.
  - A grep of `cognitive/src` shows the only production caller of `request` is `AIPrompt.scala:477-478`. The two overloads differ in argument count, so they cannot be ambiguous.
- [x] **Pre-IO image count matches the post-IO count, with no off-by-one.**
  - `AIPrompt.scala` 476 counts `Option(v).exists(_.trim.nonEmpty)`. That is the same blank rule as `OpenAIPrompt.createMessagesForRow` (`OpenAIPrompt.scala` 608-610).
  - Each non-blank path produces exactly one image part: `wrapFileToMessagesList` (631-658) returns `Seq(fileMessage)` or throws for non-images. The pre-IO count therefore equals the later `imageCount`.
  - All three limit checks use `<= 128` (`NativeQuestions.scala` 118, 136 and 165-167), so 128 images pass and 129 fail in each.
  - `indices` includes every `columnTypes` key, and path columns must be `StringType` (`AIRequestAdapter.scala` 311-313). The new `row.getString` calls are therefore type-safe.
- [x] **Adapter check order is correct.** In `AIRequestAdapter.scala` 571-587 and 602-623, the order is: model, version, options, questions (null, parse, validate), the combined path-plus-question image limit, `messages()` IO, then `prepareDecisionsRequest`. This meets the pre-IO requirements.
  - One side effect: Decisions `messagesCol` rows now report message errors after binding and question errors. That matches where Decisions `inputCol` rows already validate their input, and both codes stay local (missing_input or invalid_request). This is not a defect.
- [x] **Option gating is correct.** `AIRequestAdapter.scala` 247-264 and 274-280:
  - Decisions always accepts `columnTypes`, and accepts `fileSizeLimitMB` only when a path column exists.
  - `systemPrompt`, `dropPrompt` and `postProcessing*` are still rejected.
  - System One and embeddings reject the same parameters as before, because `decisionsOptions(p)` is guarded by `api == "decisions"`.
  - Path columns require `inputMode="text"`. Short-circuiting means other APIs never evaluate `getInputMode`.
  - `messagesCol` combined with non-empty `columnTypes`, `fileSizeLimitMB` or `inputMode` is still rejected.
- [x] **Question instruction parts are handled correctly.** `NativeQuestions.scala` 86-119 and 151-184:
  - Instructions accept either non-blank text or a non-empty part array checked by `validatePart`.
  - `questionImageCount` equals the sum of `validatePart` return values.
  - The direct lookups `fields("instructions")`, `fields("type")` and `fields("content")` run only after validation. Constant questions are validated at `AIRequestAdapter.scala` 282-290 and row questions at line 577.
  - Text instructions are emitted unchanged. `omitNulls` and `updated` go through `AIJson`'s ordered map, so key order is preserved and earlier request shapes are byte-identical.
- [x] **Hosted-URL validation (`NativeQuestions.scala` 203-213) behaves correctly for every traced case:**
  - Accepted:
    - plain hosts, `:443` and empty ports;
    - `[::1]` and `[::1]:8080`;
    - `user:pass@host`;
    - IDN and underscore hosts, which are forwarded unchanged.
  - Rejected:
    - signed `+443` and Unicode-digit ports;
    - `:99999999999`;
    - `https:///x`, `https://:443` and `https://user@/x`;
    - authorities with more than one `@`;
    - the opaque URL `http:example.com`;
    - spaces and backslashes;
    - `ftp:`, `file:` and `data:text/...`.
  - The check `portSeparator <= lastIndexOf(']')` handles bracketed IPv6 without an off-by-one.
  - In the inline branch (227-241), `&&` short-circuiting prevents `substring(-1)`.
  - Validation never rewrites the URL.
- [x] **Format prefilter and MIME allowlist are consistent and Decisions-only.**
  - `imageExtensions` is exactly jpg, jpeg, png, gif and webp (`OpenAIPrompt.scala` 313). `mayBeImagePath` (`OpenAIAttachmentUtils.scala` 315-318) uses the same `attachmentFilename` and `fileExtension` as `prepareFile` (395-426).
  - As a result, .pdf, .bmp, .tiff, .wav and .txt names fail before both the IO and `validateFileSize`, while extensionless names are still read.
  - The post-read allowlist (`OpenAIPrompt.scala` 649) compares against the MIME type that is lowercased at line 422.
  - For data-URL path cells, `dataUrlFileType` (431-442) treats only the four image MIME types in `MimeTypeExtensions` (137-154) as images. `image/jpg`, `image/bmp` and `image/x-bitmap` are therefore rejected.
  - A data URL without a comma raises `IllegalArgumentException` in `dataUrlMetadata` (159-171), which becomes invalid_request.
  - `validateImageAttachment` (628-629) takes a by-name argument, so none of these three checks runs for Chat/Responses.
- [x] **WebP detection has correct bounds.** In `OpenAIAttachmentUtils.scala` 295-308:
  - The check requires at least 12 bytes and reads bytes 0-3 and 8-11, so it cannot index out of bounds.
  - Decoding as US-ASCII cannot falsely match non-ASCII bytes.
  - RIFF/WAVE files still go to the JDK and come back as `audio/x-wav`, so WAV stays audio and the Decisions allowlist rejects it.
  - A WebP file named `.wav` becomes "unsupported" instead of mislabeled audio, which is within the requested correction.
- [x] **Template filenames and output schema are unaffected.** `AIPrompt.scala` 419-438 and 576-650:
  - Path columns are replaced by their filenames in prompt templates for every API. Only durable template validation sets `interpolateFilenames = false`.
  - Null or blank path cells render as `""`, so they do not turn the concatenated prompt into null.
  - The Decisions output schema is unchanged because Decisions never retains generated messages.
- [x] **Durable mode stays inline-only.** `AIDurableInput.scala:172` registers path columns as inline. In `AIPromptInitializationSuite.scala` 232-294:
  - The durable Decisions JSON and `messagesCol` cases check that forwarded data URLs equal the originals.
  - Malformed headers (for example `;base64;name=…`, `name=bad%xy` or an empty payload) stay terminal invalid_request errors with no requests sent.
- [x] **Docs and tests agree with the code.** The Decisions image sections and parameter table in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` match the code on:
  - check order (extension guard before IO; IO and size checks before classification);
  - `image/jpg` rejection;
  - the Hadoop query-string and percent-escape limitation;
  - inline-only durable media.

  The assertions in `AIPromptMediaInputSuite.scala`, `NativeQuestionsSuite.scala`, `AIPromptPreflightSuite.scala` and `AIPromptRecoveryAttachmentSuite.scala` agree with the traced flow and the limit arithmetic.
- [x] **Observations checked and judged not to be defects:**
  - Hosted-URL ports are not range-checked, so `:99999` is accepted. RFC 3986 allows any digit sequence, the goal limits the check to ASCII digits, and the provider rejects such a port when it fetches the URL.
  - The existing input-only message ("at most 128 images") differs from the new combined "… per request" message. Both are accurate.
  - Row-bound header and auth validation in `buildRequest` (`AIPrompt.scala` 388-401; `requestHeaders` 79-93) still runs after attachment IO. That ordering is unchanged for all APIs and is not in the goal's list of pre-IO checks.
- [ ] **Tests were not run.** This lens is read-only, so I relied on the driver-supplied evidence for the 128 tests, style checks, compile and codegen. The diff in the prompt was cut off after `OpenAIAttachmentUtils.inferMimeType`. I covered the remaining hunks by reading the current source, docs and test files directly; the driver attests these match the frozen blobs. I did not recompute the supplied diff SHA-256.