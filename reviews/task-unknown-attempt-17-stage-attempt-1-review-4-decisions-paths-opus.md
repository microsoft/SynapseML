---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 4
timestamp: "2026-11-18T02:49:42Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "f5b1d3b85686567fb71ee127bb999906ad30a5ce8f38c572352d7076fac73e50"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-17-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage.** `.review-input-decisions-paths.diff` has exactly 13 `diff --git` headers, at lines 1, 35, 141, 289, 349, 422, 498, 1072, 1125, 1228, 1346, 1470 and 1540. I read every hunk, including the ones cut off in the prompt: OpenAIAttachmentUtils, OpenAIPrompt, all six test suites and the docs. `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisions.scala` is unchanged. It gets the new behavior by extending `AIPrompt` with `defaultApiType = "decisions"`.
- [x] **Decisions request order** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 474-481; `AIRequestAdapter.scala` 582-598, 610-634).
  - Order: `selectedModel` → `requestVersion` → `mergedOptions` → questions parse and validation → `validateDecisionsImageCount(pathCount)` → `messages()` (image IO) → `prepareDecisionsRequest` → `buildRequest` headers.
  - `messages()` is called on every Decisions row, so no row skips validation. Laziness only changes which error is reported when a row has several faults.
  - Chat/Responses still evaluate `inputMessages` before `selectedModel` because it is passed by value, so their eager order is unchanged.
  - `retainedMessagesName` (295-300) only applies to Chat/Responses, so the Decisions thunk is never evaluated a second time.
  - Overload resolution between `Option[Seq[Row]]` and `() => Option[Seq[Row]], Int` is unambiguous.
- [x] **Prompt-text checks** (AIPrompt.scala 377-384, compared with AIRequestAdapter.scala 535-543).
  - Null or empty text throws `AIMissingInputException("input is null or empty")`, giving missing_input.
  - Whitespace-only text fails `NativeQuestions.text` with "Native question or input text must be nonempty", giving invalid_request.
  - Both match the existing native-text path. The new filter does nothing for Chat/Responses, which still report "prompt is null".
  - When every path cell is blank, `createMessagesForRow` returns null (OpenAIPrompt.scala 606-611), which becomes missing_input "attachment input is missing".
- [x] **Image-count arithmetic.**
  - The pre-IO count `paths.count(... _.trim.nonEmpty)` uses the same trimming rule as `createMessagesForRow`.
  - Each non-blank cell produces exactly one `input_image` or an error, so the pre-check cannot reject a row the full check would accept.
  - All three checks use `<= MaxImages`, so 128 passes and 129 fails: `decisions()`, `validateDecisionsInput` and `validateDecisionsImageCount` (NativeQuestions.scala 118, 136, 165-167).
  - The test at `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` 515-564 covers 128 and 129 with one and two path images. It checks that images follow schema order (gif before png) and its fetch counts show the failing row does no IO.
- [x] **Hosted URL checks** (NativeQuestions.scala 203-245). I traced each case:
  - **Accepted:** underscore and IDN hosts (Java's `URI` treats them as a registry authority, and `URL.getHost` is non-empty) and IPv6 hosts with or without a port (the `]` guard).
  - **Rejected:** empty, `user@` and `:443` hosts; signed and Unicode-digit ports; multi-colon authorities (`URL` throws `NumberFormatException`, so `Try` returns false); illegal spaces and bad escapes (`URISyntaxException`); and non-HTTP schemes, including non-image `data:` URLs, which get the HTTP(S)-or-inline message.
  - URLs are forwarded unchanged. `prepareParts` only removes null `file_id`/`detail`.
- [x] **Decisions format checks** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` 313, 628-658; `OpenAIAttachmentUtils.scala` 137-171, 181-188, 221-238, 310-338, 431-442).
  - The `isImage` argument is by-name and short-circuits, so Chat/Responses never run these checks.
  - `dataUrlFileType` classifies with the same `extensionForMimeType` and `categorizeFileType` as `prepareDataUrlAttachment`, so the precheck agrees with the actual route.
  - `mayBeImagePath` relies on `pathFilename`, which never throws.
  - The exact post-read MIME check blocks types the extension allows but the content sniff rejects, such as `image/x-bitmap` inside a `.png`.
  - When the MIME type is in the set and the extension passes, `fileType` is always "image", so `responsesMessage` always emits `input_image`.
- [x] **RIFF/WEBP fix** (OpenAIAttachmentUtils.scala 295-307). The check requires at least 12 bytes, "RIFF" at offset 0 and "WEBP" at offset 8, which are the correct RIFF header offsets. Short RIFF and WAVE files fall through to the JDK result (audio/x-wav), so WAV stays audio. Only the old WebP-as-WAV misclassification changes.
- [x] **Construction preflight** (AIRequestAdapter.scala 247-266, 276-291, 293-332).
  - Of the settings Decisions previously rejected, only `messagesCol`, `columnTypes` and `fileSizeLimitMB` are now allowed. SystemOne and embeddings rejection messages are unchanged.
  - Path columns require inputMode "text", which is the default.
  - `validateMessageConstruction` runs before the Decisions `fileSizeLimitMB` rule, so `messagesCol` reports its own conflict.
  - The `inputCol`/path conflict check uses `AIColumnPlan.matches` (Spark's resolver). `Param` equality (parent plus name) makes the `decisionsSettings` lookup correct.
- [x] **Durable inline-only rule** (AIDurableInput.scala 20-33, 45-58, 84-101, 145-147, 172; unchanged by this diff). `externalBindings` calls `content` on every nested JSON object, so `image_url` values in question instructions must be data URLs. Path columns are registered as "inline". Hosted URLs therefore stop durable runs before inference.
- [x] **Native message shape.** For Decisions, `createMessagesForRow` emits a single user message with `input_text` parts (OpenAIPrompt.scala 577-583, 602-618). `protocolMessages` accepts the null `name` field. `OpenAIResponses.requestMessages` (604-618) drops nulls, and `validateInputImagePart` never rewrites URLs. The wire shape is `{"role":"user","content":[...]}`.
- [x] **Tests.** The assertions in these files match the traced code paths:
  - AIPromptMediaInputSuite.scala: 277-306, 308-381, 383-513, 566-695.
  - AIPromptAttachmentSuite.scala: 336-353, 375-399.
  - The AIPromptPreflightSuite hunk: Decisions now shares the general settings list, and the `fileSizeLimitMB` message must match exactly.
  - OpenAIDecisionsLiveSuite: skipped via `assume` without credentials, and creates no resources.
  - `path()` returns a UUID-unique path (AIPromptTestBase.scala 9), so file fixtures never overwrite each other.
  - The size-limit arithmetic `(n / 2^20) * 2^20` is exact in floating point, so the boundary assertions are not flaky.
- [x] **Docs** (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 242-345). These statements all match the code: per-file column ordering, the inline-only durable rule, the Hadoop query-string and percent-escape limitation, the `.jfif`/`.jpe`/`image/jpg` guidance, row-level headers being checked after IO, and the six Python examples. The phrase "Missing model/version" is accurate: a non-null but malformed `apiVersion` is only checked later in `endpoint()`, and the docs do not claim otherwise.
- [x] **Candidates considered and dismissed** (not defects):
  - Decisions `messagesCol` rows now report model/question errors before "messages is null". This follows from the requirement that only other APIs keep eager message preparation, and no row skips validation.
  - Ports from 65536 to 99999 are accepted; the provider rejects them.
  - The input-only limit message omits "per request". That wording predates this change.
  - `Map.updated` can reorder JSON keys, which has no semantic effect.
- [ ] **Test execution: not done.** This lens only allows reading files and running no commands. I relied on the driver's reported results (57 media/attachment tests, compile, ScalaStyle, codegen) without re-running them.