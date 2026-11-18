---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 2
timestamp: "2026-11-17T18:34:54Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3c4ddb6fa41e8f2af544e33709778b620f4338b0f0b9edee305523b329a8a025"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-14-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the staged production sources at the cited ranges:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`: 196-218, 350-386 and 466-497
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`: 245-284, 520-536 and 571-624
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`: the whole file
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`: 308-318, 570-660 and 792-800
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`: 118-160, 173-188, 290-340 and 428-445
- [x] The repository already lets subclasses change behavior through protected hooks. AIPrompt overrides `supportedApiTypes` (208), `hasExplicitPostProcessing` (216) and `useBoundedAttachments` (218; hook defined at OpenAIPrompt 626). A grep of OpenAIPrompt finds the patch's new `"decisions"` branches at 578, 615, 628, 637, 649 and 651.
- [x] OpenAIPrompt.scala ends at line 800 with `// scalastyle:on number.of.methods`. `scalastyle-config.xml` lines 3-4 set `maxFileLength` to 800, and `MethodLengthChecker` and `PublicMethodsHaveTypeChecker` also apply (52-53, 60).
- [x] I traced the Decisions request order:
  - AIPrompt 474-478 calls `AIRequestAdapter.request` (599-619), then `nativeEnvelope` (571-586).
  - `nativeEnvelope` validates questions, then the pre-IO image count, then forces the messages thunk, then calls `prepareDecisionsRequest`.
  - Only AIPrompt 477-478 call `request`. AIPrompt 486-494 maps failures to `input_io_error`, `missing_input` and `invalid_request`.
- [x] The new pre-IO guard cannot raise uncontrolled exceptions. `mayBeImagePath` calls `attachmentFilename`, which calls `pathFilename`, and `pathFilename` is guarded by `Try` (OpenAIAttachmentUtils 173-188, 315-318).
- [x] `hostedImageUrl` (NativeQuestions 203-213) intentionally differs from endpoint validation. It accepts only ASCII-digit ports, keeps IDN and underscore hosts, and never fetches. This matches the goal, so it is not a finding.
- [x] The WebP fix (`inferMimeType`, OpenAIAttachmentUtils 295-307) uses named constants like the existing `MimeSniffPrefixLength`. It only intercepts a 12-byte RIFF/WEBP header, so shorter RIFF data still goes through the JDK path and WAV stays audio.
- [x] The patch follows the AGENTS.md rules I checked:
  - It uses no RDD APIs; preparation uses `mapPartitions` on a Dataset.
  - No public JVM signature or serialized Param shape changes. `AIRequestAdapter` is `private[ai]`, `NativeQuestions` is `private[services]`, and `mayBeImagePath` is `private[openai]`.
  - Durable inline-only enforcement reuses `AIDurableInput`.
  - No hand-written Python references `columnTypes` or `fileSizeLimitMB`.
- [x] Tests follow repository conventions. They use `AIRecordingServer` and `NativeRequestFixtures`, compare against `transformSchema`, and cover copy and save/load. The live suite skips through `assume` when credentials are missing.
- [x] I compared the docs (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 116-122, 286-300 and 737) with the capability checks in AIRequestAdapter 248-279.
- [x] The suggested fixes keep existing tests valid. AIPromptMediaInputSuite asserts `contains("at most 128 images")` at 529 and 627 and `contains(name)` at 561-582. It pins the exact format diagnostic at 363, 455 and 494, and inline TIFF/BMP/x-bitmap cases at 432-434.
- [x] The OpenAIPrompt before-state (dropped explicit return type and removed `createMessagesForRow` comments) comes from the supplied patch. `.git/**` is out of scope, so I checked only the after-state against the staged file.
- [ ] Tests, scalastyle, compile and codegen were not run, because this lens is read-only. I relied on the driver's validation (139 deterministic tests).
- [ ] I did not check whether the assigned artifact already exists, because the independence rules exclude `reviews/`.

## Issues

### Issue 1: Decisions-only logic is hard-coded in the OpenAIPrompt base class instead of using subclass hooks
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 578, 609-621, 628-629, 637, 649, 651 (file ends at 800)
- **Description**: `OpenAIPrompt.supportedApiTypes` covers only `chat_completions` and `responses`. Even so, the patch adds Decisions-only behavior to this public base stage by checking the apiType string:
  - the input-text wrapper (578);
  - user-only generated messages (615);
  - a Decisions format validator and its error message (628-629);
  - Decisions branches in both wrap matches (637, 651);
  - a Decisions MIME allowlist (649).

  Elsewhere, AIPrompt changes base behavior by overriding a protected hook that has a neutral default, for example `useBoundedAttachments` (OpenAIPrompt 626, AIPrompt 218), `supportedApiTypes` (208) and `hasExplicitPostProcessing` (216).

  The file is now exactly 800 lines, the scalastyle `FileLengthChecker` maximum. `number.of.methods` is already suppressed. To fit, the diff shrinks `buildContentParts` to one line with an inferred result type (620-621). It also deletes the explanatory comments in `createMessagesForRow`, including the note on the `null` return (611). `AIPrompt.generatedMessages` depends on that `null` return (AIPrompt 358-361).

  Classification: maintainability, not a demonstrated defect.
- **Risk**: Any Decisions-only policy change requires editing and re-validating the Chat/Responses stage. The file has no remaining line budget, so the next change here must delete more code or comments to pass scalastyle. The `null`-return contract is now undocumented and easy to break during that kind of trimming.
- **Suggested Fix**: Add small no-op protected hooks to OpenAIPrompt and override them in AIPrompt for apiType `decisions`. For example:
  - a path check that runs before IO;
  - a MIME check after reading or inline classification;
  - `messagesForParts(parts)`, defaulting to `getPromptsForMessage(Left(parts))`.

  Keep the allowlist and its error message in the `ai` package. Widen `mayBeImagePath` to `private[services]` if AIPrompt needs to call it. Use the freed lines to restore the explicit `Seq[Map[String, String]]` return type and the `null`-return comment. If the refactor is deferred, restore at least the comment.

### Issue 2: The supported Decisions image formats are defined in four places, and inline images follow the Chat/Responses tables
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: OpenAIPrompt 313, 628-634, 646-649; OpenAIAttachmentUtils 137-155, 315-318, 431-442
- **Description**: The JPEG/PNG/GIF/WebP rule is written four separate times:
  - **Inline data URLs** pass when `dataUrlFileType(...) == "image"` (OpenAIPrompt 633-634). That result comes from the shared `imageExtensions` set (313) combined with `MimeTypeExtensions` (OpenAIAttachmentUtils 137-155).
  - **The pre-IO file/HTTP check** uses `mayBeImagePath(path, imageExtensions)` (646).
  - **The post-read check** is a hard-coded MIME set (649).
  - **The error message** (629) and the docs repeat the list.

  Only the post-read set and the error message belong to Decisions. The inline and pre-IO checks accept whatever Chat/Responses supports. All four agree today only because the shared tables happen to contain exactly these four formats.

  Classification: maintainability, not a demonstrated defect.
- **Risk**: Suppose someone adds AVIF or HEIC to the shared tables for Chat/Responses. Decisions would then silently send that inline data URL to the provider instead of returning the documented local error. `.avif` paths would also pass the pre-IO check, so they would be fully read (file/HTTP IO and size checks) before the post-read check rejects them. That breaks the documented rule that known unsupported extensions are rejected before IO. Current tests (AIPromptMediaInputSuite 432-434) cover only TIFF, BMP and x-bitmap.
- **Suggested Fix**: Define one Decisions-owned constant that maps each MIME type to its extensions. Derive the inline MIME check, the pre-IO extension check, the post-read check and the error message from it. Optionally, add a format that is not in the constant to the existing negative cases, so widening the shared tables fails a test.

### Issue 3: The caller picks lazy or eager message preparation by choosing between two undocumented adapter overloads
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: AIPrompt 474-478; AIRequestAdapter 571-579, 599-603; OpenAIPrompt 606-608
- **Description**: Decisions must validate model, version, options, questions and image count before any image IO. `AIPrompt.prepareRequests` does this by checking `getApiType == "decisions"` and calling an overload that takes a thunk and a `pathImageCount`. Every other API calls a by-value overload, which builds the messages before model, version and options are validated.
  - The ordering rule lives in the caller. The adapter owns per-API sequencing and already holds `stage` and `schema`, but it cannot enforce the rule, and neither overload documents it.
  - `pathImageCount` is a Decisions-only concept added to the shared signature.
  - Its nonblank check (`Option(...).exists(_.trim.nonEmpty)`) re-implements the filter in `createMessagesForRow` (OpenAIPrompt 606-608). The pre-IO count is correct only while both checks agree and each nonblank path produces exactly one `input_image`.

  Classification: maintainability, not a demonstrated defect.
- **Risk**: If a future caller uses the by-value overload for Decisions, the pre-IO image-limit check silently disappears, because `pathImageCount` defaults to 0. Image IO would also run before binding and question validation. Using the thunk overload for Chat/Responses would instead change which error they report first. Neither mistake is visible at the call site.
- **Suggested Fix**: Prefer a single adapter entry point that takes the thunk. The adapter would build messages up front for non-Decisions APIs and receive the ordered path values, so it can compute the Decisions count itself. At minimum, document the ordering rule on both overloads and share one nonblank-path helper between `createMessagesForRow` and the count.

### Issue 4: Decisions prompt validation is mixed into the generative branch and repeats `AIRequestAdapter.input`
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: AIPrompt 377-385; AIRequestAdapter 524-531
- **Description**: The generative branch of `AIPrompt.messages` now also handles Decisions with path columns, using three separate `getApiType == "decisions"` checks: the empty filter, the error text and the `validateDecisionsInput` call.
  - The missing/empty rule and its message `input is null or empty` repeat `AIRequestAdapter.input`, which handles Decisions without path columns.
  - Whitespace-only text is rejected by `validateDecisionsInput` in one flow and by `prepareDecisionsRequest` in the other.

  The goal requires both flows to keep the same `missing_input` and `invalid_request` codes and messages. Only tests (NativeQuestionsSuite 177 and AIPromptMediaInputSuite) keep them aligned.

  Classification: maintainability, not a demonstrated defect.
- **Risk**: A future change to Decisions input rules, such as new wording, trimming or JSON input, could be applied to one flow and not the other. Mixing in Decisions checks also makes it harder to confirm that Chat/Responses behavior is unchanged.
- **Suggested Fix**: Add a separate `else if (getApiType == "decisions" && paths.nonEmpty)` branch before the generative branch, and restore the generative branch to its earlier form. Have both this branch and `AIRequestAdapter.input` call one shared helper that throws `AIMissingInputException("input is null or empty")`, followed by `NativeQuestions.validateDecisionsInput`.

### Issue 5: The image limit uses two counting functions, counts input images twice and has two error messages
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 121-136, 151-157, 165-173, 215-245
- **Description**: Checking the 128-image limit at question preflight (118), before IO (165-167, called from AIRequestAdapter 578) and after preparation (157) is reasonable. The implementation is inconsistent, though:
  - It uses two counting functions: `validatePart`, which returns 1 per image and is summed at 128-135, and `imageCount`, which counts `type == "input_image"` (170-173).
  - `prepareDecisionsRequest` counts input images again right after `validateDecisionsInput` computed and discarded the same total.
  - Exceeding the limit with input images alone reports "Decisions accepts at most 128 images" (136). Exceeding it with question images, or the combined total, reports "...at most 128 images per request" (118, 167).
  - All three messages hard-code 128 next to `MaxImages` (15).

  Classification: maintainability and message consistency, not a demonstrated defect.
- **Risk**: The same request-wide limit produces different messages depending on which check trips first. Changing `MaxImages` or what counts as an image requires the same edit in several places.
- **Suggested Fix**: Have `validateDecisionsInput` return its image count, and use that value in `prepareDecisionsRequest` instead of counting again. Route every limit check through `validateDecisionsImageCount`, and build one message from `MaxImages`. Existing assertions use `contains("at most 128 images")` (AIPromptMediaInputSuite 529 and 627), so they still pass.

### Issue 6: The `fileSizeLimitMB` error says Decisions does not support it, though Decisions now does with path columns
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala; `docs/Explore Algorithms/OpenAI/Unified AI prompting.md`
- **Line(s)**: AIRequestAdapter 248-255, 274-279; docs 118-122, 737
- **Description**: Decisions now supports `fileSizeLimitMB` when an image path column is mapped (docs 737).
  - Without a path column, the generic loop still reports "fileSizeLimitMB is not supported by apiType='decisions'". That message was accurate only before this patch.
  - With `messagesCol`, docs 118-120 describe the conflict as "`messagesCol` cannot be combined with ... `fileSizeLimitMB`". Chat/Responses report it that way through `validateMessageConstruction` (274-279). Decisions instead reports the generic "not supported" message, because the generic loop fails first.

  Classification: misleading error message; the rejection itself is correct.
- **Risk**: A user whose `columnTypes` path mapping is missing or misspelled is told that Decisions cannot use size limits at all, which steers them away from the documented fix. The same documented conflict also produces different messages depending on the API.
- **Suggested Fix**: For Decisions, skip `fileSizeLimitMB` in the generic loop and add a targeted check, for example `require(!stage.isSet(stage.fileSizeLimitMB) || decisionsPaths || stage.isSet(stage.messagesCol), "fileSizeLimitMB requires a Decisions image path column (columnTypes value 'path')")`. Let `validateMessageConstruction` report the `messagesCol` conflict. Existing tests assert only `contains(name)` (AIPromptMediaInputSuite 561-582), so they remain valid.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 5
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 6
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver dispositions on the unchanged snapshot

These six observations repeat already assessed design/diagnostic suggestions;
none identifies a new incorrect request, acceptance failure, uncontrolled error,
or missing required invariant. The original findings and `ISSUES_FOUND` verdict
are preserved. No reviewed content is changed.

1. **Not adopted; optional architectural refactor.** The legacy stage still
   rejects Decisions while the facade opts in. Shared message building avoids
   a second attachment pipeline; protected extension points would enlarge the
   maintenance surface without correcting behavior. The null result is handled
   explicitly by `Option(createMessagesForRow(...))` and covered by missing-path
   tests. File length and method rules pass without adding a suppression.
2. **Not adopted; hypothetical future format expansion.** All current tables
   agree. Positive JPEG/PNG/GIF/WebP and negative BMP/TIFF/X-bitmap/document
   cases now pin both public Decisions stages. A future shared-format change
   needs its own API-specific compatibility review; no unsupported current
   format is accepted by this patch. Duplicating policy solely for hypothetical
   AVIF/HEIC support is not necessary to ship the requested compatibility fix.
3. **Not adopted; retain tested caller sequencing.** There is one production
   caller, which explicitly selects Decisions' thunk overload. The by-value
   overload preserves other APIs' established error ordering. Both the
   pre-IO count and loader apply the same null/trim/nonempty rule. Current
   tests now verify two-image counts, blank exclusion, schema ordering, and
   zero downloads for the over-limit row. Moving ordered path values into
   the adapter would introduce a wider request-building change.
4. **Not adopted; retain distinct existing input entry points.** Generated
   attachments need early prompt checks, while ordinary native input is
   validated in the adapter. Both use `NativeQuestions` for whitespace
   semantics and preserve the same missing-input diagnostic. Tests exercise
   null/empty/whitespace text in both flows and unchanged empty/text-only
   mappings. A helper extraction is optional cleanup, not a missing behavior.
5. **Not adopted; optional count/message consolidation.** Every enforcement
   comparison uses `MaxImages`. Repeated checks protect independently callable
   validators, pre-IO rejection, and serialized output. The bounded second
   count and two accurate message variants do not change the supported limit.
   Existing and expanded 128/129 boundary tests pass. No return-signature or
   diagnostic changes are needed for this feature.
6. **Not adopted; non-blocking diagnostic refinement.** Rejection is correct
   for the submitted configuration and identifies the ineffective parameter.
   The documented capability table, example and public-stage tests describe
   the path-column condition; `messagesCol` combinations are rejected rather
   than silently ignoring a limit. More contextual wording would be useful,
   but changing validator precedence is not needed to fix image handling.

Verification: all five production blobs match the prior independently reviewed
snapshot; the current full manifest is unchanged. The final validation includes
139 passing deterministic tests, root ScalaStyle/test ScalaStyle, compilation,
test compilation, codegen, generated-wrapper checks and six parsed examples.
Three live cases were canceled without credentials and remain unqualified.

Continue the remaining independent lenses on patch
`008f86591bfcb8609c7699e9c7f0f25d633629fe26c417c83762cee9088eb8c8`.
These dispositions do not relabel this review as `CLEAN`.