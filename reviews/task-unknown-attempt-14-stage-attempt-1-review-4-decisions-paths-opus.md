---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 4
timestamp: "2026-11-17T19:23:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "30bf1f870d3b7825530a6a384bb00f3c4f95c53b667bf6d91b00ac91bc21ce99"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-14-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Prompt-text check.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` L377–385, a null or empty Decisions prompt raises `AIMissingInputException("input is null or empty")`, which becomes `missing_input`. A whitespace-only prompt fails `NativeQuestions.validateDecisionsInput(JsString)` with "Native question or input text must be nonempty", which becomes `invalid_request`. The no-path route produces the same codes and messages through `AIRequestAdapter.input` (L520–528) and then `prepareDecisionsInput`. For Chat and Responses the new filter always passes, and the message stays "prompt is null".
- [x] **Order of checks before image IO.** Traced `AIPrompt.scala` L474–478 and `AIRequestAdapter.scala` L571–622.
  - Decisions runs these steps in order: `selectedModel`, `requestVersion` (null version), `mergedOptions`, questions parse/validate, the combined image count, then `messages()`. `messages()` does the text check and only then reads attachments.
  - `AIJson.parse` (AIJson.scala L87–104) turns malformed row JSON into `IllegalArgumentException`, so it becomes `invalid_request` before any IO.
  - The 3-argument `request` overload forces `inputMessages` at the call site, so Chat, Responses, System One and embeddings still prepare messages eagerly.
  - The two overloads differ in arity, so the call cannot be ambiguous.
  - Decisions never retains generated messages, so the lazy value is evaluated only once.
- [x] **Pre-IO and post-IO image counts agree.** `AIPrompt.scala` L476 counts path cells with `Option(...).exists(_.trim.nonEmpty)`. `createMessagesForRow` uses the same rule to skip cells (`OpenAIPrompt.scala` ~L598–617).
  - Each nonblank Decisions path produces exactly one `input_image` part or throws, so the pre-IO count equals `inputImages` in `prepareDecisionsRequest` (`NativeQuestions.scala` L151–163).
  - The `<= 128` boundary accepts 2 path images + 126 question images and rejects 2 + 127. That matches the supplied validation evidence.
- [x] **Question validation and wire shape.** Traced `NativeQuestions.scala` L86–119 and L151–184.
  - String instructions go through `text`; array instructions validate each part with `validatePart`; other types are rejected. The question image total is also checked.
  - `prepareParts` only removes null `file_id` and `detail` from `input_image` parts.
  - `fields.updated` on `AIJson.OrderedMap` (AIJson.scala L23–44) replaces the value in place, so the bytes and field order of text-only questions are unchanged.
  - The image counters only run on JSON that has already been validated, so they cannot hit missing keys.
  - `questionsCol` must be a `StringType` column (`AIRequestAdapter.scala` L344–361), so struct null fields cannot leak into parts.
- [x] **Hosted image URL check** (`hostedImageUrl`, `NativeQuestions.scala` L203–213). I traced each case:
  - User info: when there is no `@`, `lastIndexOf('@') + 1` is 0, so the whole authority is kept.
  - IPv6: comparing the last `:` with the last `]` handles addresses with no port, with a port, and with an empty port.
  - Ports must be ASCII digits only, so `+80`, `-1` and full-width digits are rejected.
  - Scheme matching ignores case.
  - Empty hosts are rejected through `URL.getHost`: `https://:443`, `https:///x` and `https://user@/x`.
  - Underscore and IDN hosts (registry authority) are accepted.
  - Multiple colons fail because `URL` throws `MalformedURLException`.
  - Malformed escapes and spaces fail in `URI`.
  - The URL is never fetched or rewritten.
- [x] **Inline data URLs.** Traced `NativeQuestions.scala` L191–201 and L215–245.
  - The `data:image/` prefix check ignores case.
  - `comma > 0` fails before `substring(comma + 1)` can run on a missing comma.
  - The header must end with `base64` and any parameters must be URL tokens.
  - Base64 is validated without decoding. Any nonempty valid base64 decodes to at least one byte.
  - Data URLs generated from path columns (`OpenAIAttachmentUtils.scala` L498–501) always satisfy this grammar for the four allowed MIME types. An empty image file fails as `invalid_request` with "Decisions image data must be nonempty".
- [x] **Format gating** (`OpenAIPrompt.scala` ~L626–658; `OpenAIAttachmentUtils.scala` L137–157, L295–337, L423–433).
  - `validateImageAttachment` takes a by-name argument and only evaluates it for Decisions. Chat and Responses do no extra work and get no new exceptions.
  - Data URLs: only `image/jpeg`, `image/png`, `image/gif` and `image/webp` map to image extensions. Every other MIME type maps to `bin` and is classified unsupported.
  - Filesystem and HTTP paths: `mayBeImagePath` (no extension, or an image extension) agrees with `categorizeFileType`. A nonempty non-image extension can never be classified as `image`, so no previously accepted image is now rejected before IO.
  - The check after reading rejects `image/x-bitmap` and similar types even when the file has an image extension. Any MIME type that passes this check is always classified as `image`.
- [x] **WebP fix** (`OpenAIAttachmentUtils.scala` L122–123, L295–308).
  - The length ≥ 12 check runs before the read at offset 8.
  - RIFF + WEBP is detected as `image/webp`.
  - An 11-byte RIFF header and RIFF/WAVE files still fall through to the JDK and are detected as `audio/x-wav`, so WAV stays audio.
  - For Chat and Responses, `.webp` files change from unsupported to image, which is the intended fix.
- [x] **Which settings each API accepts** (`AIRequestAdapter.scala` L247–280 and L296–322).
  - System One and embeddings still reject every parameter in the list.
  - Decisions accepts `messagesCol` and `columnTypes`, and accepts `fileSizeLimitMB` only when a path column exists. `systemPrompt`, `dropPrompt` and `postProcessing` are rejected.
  - Path columns require `inputMode='text'` and a string column type.
  - `messagesCol` combined with non-empty `columnTypes`, `fileSizeLimitMB` or `inputMode` is still rejected.
  - `Set[Param[_]]` membership depends on Spark `Param.equals`, which compares parent and name.
- [x] **File size limits** (`OpenAIAttachmentUtils.scala` L350–377 and L391–418).
  - AIPrompt uses bounded reads and `fileSizeLimitMB` has no default, so an unset limit reads the whole file.
  - An infinite limit converts to `Long.MaxValue` and also reads the whole file.
  - A finite limit reads at most the limit plus one byte, and `validateFileSize` uses the same conversion.
  - NaN and values ≤ 0 are rejected.
  - This matches the documentation.
- [x] **Wire shape of generated messages** (`OpenAIResponses.scala` L448–458 and L604–620; `AIRequestAdapter.scala` ~L538–568).
  - Generated messages encode to `{role:"user", content:[input_text, input_image...]}` with no `type`, `name` or `detail` fields.
  - Decisions gets no system message.
  - A null `name` passes the allowed-field check.
- [x] **Durable output.** `AIDurableInput.scala` L172 validates path columns as inline-only, and question image parts go through the same inline checks. `AIPromptOutput.scala` L179–184 (`validateSettings`) adds no Decisions-specific constraint that conflicts.
- [x] **Documentation.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` L110–112, L245–343 and L737 match the code on:
  - the order of checks before image IO;
  - skipping null or blank path cells;
  - the extension check before reading;
  - header checks after loading;
  - the Hadoop query-string and percent-escape limitation;
  - durable mode accepting only inline images;
  - the table of supported parameters.
- [x] **Live test.** `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisionsLiveSuite.scala` L22–69 is skipped cleanly through `assume` when credentials or the image URL are missing. The answer fields it reads match the schema in `AIResponseCodec.scala` L27–43.
- [x] **Considered, not defects** (outside the stated contract or shared existing behavior):
  - Row-level header validation and the unpaired-surrogate check on a row `apiVersion` still run after image IO.
  - For Decisions, a null `messagesCol` cell is now reported after question errors instead of before them.
  - When `inputCol` names a path column, the raw path is sent as text as well as loaded as an image. Chat and Responses already behave this way.
  - Ports above 65535 are forwarded unchanged.
- [x] **Coverage limitation.** The supplied diff was cut off after the `OpenAIAttachmentUtils` header. For the remaining files I read the staged worktree sources directly, which the driver verified match the frozen blobs: `OpenAIAttachmentUtils.scala`, `OpenAIPrompt.scala`, the test suites and the docs.
- [ ] **Tests and builds not run.** As a read-only reviewer I did not reproduce the driver-supplied validation (139 deterministic tests, ScalaStyle, compile and codegen).