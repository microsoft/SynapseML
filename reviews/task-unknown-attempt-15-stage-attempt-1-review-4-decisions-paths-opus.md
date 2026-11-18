---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 4
timestamp: "2026-11-17T22:11:24Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "cec2179d7f70633107bc6c8c69b579cd5ad3ee7dfa4a7bff32391f20339c344a"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-15-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Prompt text checks in `AIPrompt.messages`** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 362–387):
  - Null or empty Decisions prompt text gives `AIMissingInputException("input is null or empty")`, reported as `missing_input`.
  - Whitespace-only text reaches `NativeQuestions.validateDecisionsInput(JsString(text))` and fails as `invalid_request`.
  - Both checks run before `generatedMessages`, so before any attachment IO.
  - For Chat/Responses the new `.filter` does nothing and the message stays `prompt is null`.
- [x] **Lazy messages and image count in `prepareRequests`** (`AIPrompt.scala` 453–495):
  - Decisions passes `() => inputMessages` (a lazy val) and a pre-IO `imageCount`.
  - That count uses the same `trim.nonEmpty` test as `createMessagesForRow` (`OpenAIPrompt.scala` 608–616).
  - `wrapFileToMessagesList` returns exactly one part per non-blank path, or throws. So the pre-IO count always equals the post-IO count.
  - Other APIs call the by-value overload, so they still prepare messages eagerly.
  - `retainedMessagesName` (295–296) is generative-only, so Decisions never forces the lazy value a second time.
  - The catch order (cancelled read, read error, missing input, `IllegalArgumentException`) is correct for the subclass relationships.
- [x] **Decisions request order** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` 582–633):
  - The order is: model, version, `mergedOptions`, parse/validate questions, `validateDecisionsImageCount`, then `messages()` (the first image IO), then `prepareDecisionsRequest`.
  - Header validation in `buildRequest` stays after IO, as documented.
  - The envelope key order (questions, input/state, model) and System One's content selection are unchanged.
  - Overload resolution between the 3-argument and 4-argument `request` is unambiguous.
- [x] **Capability checks** (`AIRequestAdapter.scala` 247–265, 275–281, 293–332):
  - The non-generative allowlist uses Spark `Param` equality (parent plus name), so System One and Embeddings still reject `columnTypes` and `fileSizeLimitMB`.
  - `getInputMode` defaults to `"text"` (`AIPromptParams.scala` 105) and `getColumnTypes` defaults to `Map.empty` (`OpenAIPrompt.scala` 293), so the new getter calls cannot throw.
  - For `messagesCol` with `fileSizeLimitMB`, the stage now reports the exact construction conflict, as the test suites assert (`AIPromptPreflightSuite` 86, `AIPromptRecoveryPreflightSuite` 107).
- [x] **`inputCol`/path overlap check** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIColumnPlan.scala` 37, 51–53):
  - `resolve` and `matches` use the same `SQLConf` resolver on top-level names, so `matches(inputCol, key)` holds exactly when both resolve to the same field.
  - The `asset`/`ASSET` test (`AIPromptRecoveryPreflightSuite` 31–56) covers both `transformSchema` and `transform`, with and without durable storage.
- [x] **Question validation and image limits** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` 86–184):
  - Instructions accept a non-blank string or a non-empty array of parts.
  - The question-only and combined limits allow up to 128 images; 129 is rejected.
  - Every unguarded `Map.apply` (`fields("content")`, `fields("instructions")`, `fields("type")`) runs only after structural validation.
  - `prepareParts` removes only null `file_id`/`detail`, and `updated` keeps question key positions.
- [x] **Image URL checks** (`NativeQuestions.scala` `hostedImageUrl` 203–213, `validatePart` 215–245):
  - All `substring` indices stay in bounds.
  - Rejected: empty hosts, opaque, relative and malformed URIs, non-HTTP(S) schemes, signed or Unicode-digit ports, and leading whitespace.
  - IPv6 brackets and userinfo `@` are handled correctly.
  - Underscore and IDN hosts pass through `URI`'s registry-authority fallback and `URL.getHost`.
  - The original URL string is forwarded unchanged; there is no client fetch.
- [x] **Format checks stay Decisions-only and agree** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` 313, 628–649; `OpenAIAttachmentUtils.scala` 160–171, 315–337, 395–442):
  - The `validateImageAttachment` predicate is by-name and short-circuits, so Chat/Responses never evaluate the Decisions checks.
  - `mayBeImagePath` uses the same `attachmentFilename` as `prepareFile`.
  - A data URL is classified as `image` only for `image/jpeg|png|gif|webp`, because `extensionForMimeType` maps every other `image/*` to `bin`. That matches the post-read MIME set, and the post-read set guarantees `fileType == "image"`.
  - The three definitions are aligned today. Regression rows for data-URL `bmp`/`tiff`/`x-bitmap`/`jpg` and local `.tiff`/`.bmp`/`.wav` files pin the diagnostic (`AIPromptMediaInputSuite` 413–500). I therefore treat the separate definitions as an optional design choice, not a defect.
- [x] **WebP fix** (`OpenAIAttachmentUtils.scala` 295–308):
  - The `length >= 12` guard covers offset 8 plus 4 bytes, and the check uses US_ASCII.
  - Only `RIFF....WEBP` changes classification. WAV, AVI and truncated RIFF files still get the JDK's `audio/x-wav`.
- [x] **Durable mode stays inline-only** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala` 84–104, 143–196):
  - Path columns are validated as `inline`.
  - Images in question instructions (constant or `questionsCol`) and in JSON input or messages go through `externalBindings` → `content` → `inline`, so hosted URLs are rejected.
- [x] **Size boundary test arithmetic** (`AIPromptMediaInputSuite` 455–462): scaling by 2^20 is exact in floating point, so `len - 1` is rejected and `len` is accepted, for both the bounded read and the data-URL size estimate.
- [x] **Considered and not reported:**
  - `data:image/png;base64;name=x` (parameter after `base64`, which RFC 2397 does not allow) fails with a controlled `invalid_request`.
  - Ports from 65536 to `Int.MaxValue` in hosted image URLs are forwarded under existing `URL` semantics; the provider decides.
  - With an invalid kind such as `PATH` plus `fileSizeLimitMB`, the `fileSizeLimitMB` message appears first. It is still accurate.
- [x] **Docs** (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 114–126, 240–330, 464–476, 732–760): every claim checked matches the code. The underscore-host restriction at line 471 applies only to endpoint URLs, not image URLs.
- [x] **Tests reviewed:** `OpenAIDecisionsLiveSuite` 21–69 skips via `assume` when credentials or an HTTPS image URL are missing, which matches the three supplied canceled results. The driver's 284-pass, style, compile and codegen results are supplied evidence only; this reviewer did not run any tests or commands.
- [ ] **Runtime evidence not verified:** this was a read-only review, so I could not confirm that the four regressions failed before the fix or reproduce the runtime results.