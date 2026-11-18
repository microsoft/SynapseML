---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 4
timestamp: "2026-11-18T00:15:29Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "4f82569a940e4bf7a3a852a2f8e7015ae04427291a6e9f7d05435e353d957ab2"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-16-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Prompt text checks in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` (lines 353-386).**
  - For Decisions, null or `""` text gives `missing_input` "input is null or empty". This is the same text as `AIRequestAdapter.input` (`AIRequestAdapter.scala` lines 535-543).
  - Whitespace-only text gives `invalid_request` through `NativeQuestions.text`.
  - Both checks run before `generatedMessages`, so before any attachment IO.
  - For Chat/Responses the new filter does nothing, so they still get "prompt is null".
  - Exactly one input binding is enforced (`AIRequestAdapter.scala` lines 293-298), so the `orElse` fallback to `inputCol` is never needed.
- [x] **Lazy message preparation in `AIPrompt.scala` (lines 453-495).** `adapter.request` has only two callers (lines 477-478).
  - Non-Decisions APIs use the by-value overload, so `inputMessages` is still built before `request` runs.
  - Decisions builds messages inside `nativeEnvelope`, in this order: model → version → options → parse and check questions → `validateDecisionsImageCount` → `messages()`.
  - Errors thrown while building the lazy value reach the same catch block, so error codes are unchanged.
  - SystemOne gets `pathImageCount = 0` and no messages.
  - `retainedMessagesName` is never set for Decisions.
- [x] **Image count before IO matches the count after IO.**
  - The early count, `paths.count(Option(..).exists(_.trim.nonEmpty))`, uses the same rule as `orderedAttachments` in `OpenAIPrompt.createMessagesForRow` (`OpenAIPrompt.scala` lines 607-618).
  - `wrapFileToMessagesList` (lines 631-660) creates exactly one part per path, and Decisions rejects every non-image, so each counted path becomes exactly one image.
  - `attachmentColumns` returns the configured key names (`AIPrompt.scala` lines 349-351), so the `indices(name)` lookups are safe.
  - Keys that the resolver treats as duplicates are rejected (`AIRequestAdapter.scala` line 310).
  - `messagesCol` cannot be combined with path columns (lines 279-280), so the count is never too high.
- [x] **128-image limits in `NativeQuestions.scala` (lines 86-184).** The `<= 128` checks give the intended boundaries:
  - 64+64 accepted, 64+65 rejected.
  - With one path, 1+64+63 accepted and 1+64+64 rejected.
  - The input-only cap in `validateDecisionsInput` is kept.
  - `questionImageCount` only runs after instructions are validated, and `prepareDecisionsInput` validates before reading `fields("content")`, so neither can hit a missing key.
  - `prepareParts` removes only null `file_id`/`detail` from `input_image` parts.
  - `OrderedMap.+`/`-` keep entry positions (`AIJson.scala` lines 23-44), so field order on the wire is preserved.
- [x] **`hostedImageUrl` (`NativeQuestions.scala` lines 203-213), traced by hand.**
  - The userinfo split (`lastIndexOf('@') + 1`) works when there is no userinfo.
  - Comparing the port colon against `]` handles IPv6 literals and an empty port.
  - Ports like `+443`, `-1` and non-ASCII digits are rejected, even though `URL`'s `Integer.parseInt` would accept them.
  - Underscore and IDN hosts fall back to a registry authority and are accepted, with the URL forwarded unchanged.
  - Rejected: empty hosts, multiple `@`, non-HTTP(S) schemes, malformed escapes and spaces.
  - Inline images: the prefix check ignores case, then `inlineImageHeader`, base64 and non-empty checks apply.
- [x] **`validateCapabilities` and `validateDecisionsInputColumn` (`AIRequestAdapter.scala` lines 247-264 and 325-332).**
  - The `messagesCol` allowance behaves exactly as before.
  - The new allowance covers only `columnTypes` and `fileSizeLimitMB`, and only for Decisions. SystemOne and embeddings still reject both.
  - The `messagesCol` conflict messages are reported before the fileSizeLimitMB-requires-path rule.
  - The `inputCol` overlap check uses `AIColumnPlan.matches`, which respects Spark's case-sensitivity setting.
- [x] **Attachment reading in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` (lines 120-187, 221-239, 296-320 and 395-466).**
  - The WEBP check (at least 12 bytes, `RIFF` at offset 0, `WEBP` at offset 8) is safe on short arrays. Real `WAVE` files and truncated RIFF files stay audio.
  - `mayBeImagePath` uses the same lowercased `attachmentFilename`/`fileExtension` as `prepareFile`. Extensionless names are still read.
  - In `dataUrlFileType`, a URL with no comma becomes `take(0)` and raises a controlled error.
  - `image/jpg` maps to `bin` and is rejected, as documented.
  - The by-name `validateImageAttachment` (`OpenAIPrompt.scala` lines 628-629) means Chat/Responses never run the Decisions checks.
- [x] **Tests checked against the code.** In `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/`:
  - `AIPromptMediaInputSuite.scala`: lines 303-334 (WEBP is an exact data-URL match on all four stages) and 414-470 (each unsupported kind gets the format message; the size limits divide by powers of two, so they are exact).
  - `NativeQuestionsSuite.scala`: lines 120-146 (the summed limit across questions plus the path).
  - `AIPromptRecoveryPreflightSuite.scala`: lines 31-150.
  - `AIPromptRecoveryAttachmentSuite.scala`: lines 26-104 (durable output is inline-only and resumes correctly; hosted and `file://` media are rejected before storage or HTTP).
- [x] **Docs (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, lines about 217-350) match the code.** This covers the headers being checked after attachments load, only missing model/version bindings being checked before IO, `.ashx` and other non-image extensions being rejected before IO, the Hadoop `?`/`%` limitation, and the 128-image request total. No hand-written Python under `cognitive/src/main/python` touches these settings.
- [x] **Behavior changes that are not defects:**
  - The image count for constant questions is recomputed on every row (negligible cost).
  - For rows with more than one problem, error precedence changed for Decisions with `messagesCol`.
  - A WEBP file named `.wav` now reports `image/webp` in Responses. This follows from the requested WEBP fix.
  - Surrogate checks on prompt text still run after IO, which the stated before-IO contract does not cover.
- [ ] **Tests, builds and live suites not executed.** The tools were read-only. I relied on the driver's evidence (58 tests fresh plus 228 unchanged), and the credential-gated `OpenAIDecisionsLiveSuite.scala` (lines 21-69) was not run.
- [ ] **Truncated diff.** The supplied diff was cut off inside the `OpenAIAttachmentUtils` hunk. I reviewed the rest from the current source files, but could not compare those hunks to the patch byte for byte.