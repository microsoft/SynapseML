---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 3
timestamp: "2026-11-18T02:28:01Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5d1376a968c0fe793123ad669433785c156deca96f7ebe7367d6d118fcefc659"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-17-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Whole patch read.** I read all of `.review-input-decisions-paths.diff`, lines 1–1707. It touches 13 files: 5 production, 7 test and 1 docs file. For each changed area I also read the post-change source. These are `AIPrompt.scala`, `AIRequestAdapter.scala`, `NativeQuestions.scala`, `OpenAIAttachmentUtils.scala`, `OpenAIPrompt.scala`, the unchanged `AIDurableInput.scala`, and `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 250–350.
- [x] **Decisions checks happen before any image is read.**
  - In `AIPrompt.scala` 474–478, `inputMessages` is a `lazy val`. Decisions gets the deferred (thunk) overload. Its path count uses the same trimmed, non-blank filter as `OpenAIPrompt.scala` 608–610.
  - In `AIRequestAdapter.scala` 610–636, the order is `selectedModel`, then `requestVersion`, then `mergedOptions`, then `nativeEnvelope` (582–598).
  - Inside `nativeEnvelope`, questions are parsed and validated and the combined image count is checked. Only then is `messages()` called.
  - In `AIPrompt.scala` 377–384, null or empty prompt text gives `missing_input` and whitespace-only text gives `invalid_request`, both before `generatedMessages`.
  - Chat/Responses use the eager overload (478, 610–611), so attachment IO still comes before missing-model errors. System One gets `() => None` and builds the same request as before.
- [x] **Attachment gates** (`OpenAIPrompt.scala` 628–656).
  - `validateImageAttachment` takes its check by name and only evaluates it for Decisions. Chat/Responses never run the Decisions-only `dataUrlFileType` or `mayBeImagePath` checks.
  - For data URLs, the MIME type is classified from the metadata before the size or base64 checks.
  - For files, the extension gate (`OpenAIAttachmentUtils.scala` 315–318) runs before `prepareFile` reads anything. The MIME allowlist is applied after the read.
  - A file that passes both gates is always categorized as `image`.
  - `wrapFileToMessagesList` can only be reached for chat/responses/decisions (`GenerativeApis` at line 22; `validateCapabilities` 247–265), so there is no MatchError path.
- [x] **WebP detection** (`OpenAIAttachmentUtils.inferMimeType`).
  - A file is treated as WebP only if it has at least 12 bytes, `RIFF` at offset 0 and `WEBP` at offset 8. This check runs before the JDK guess.
  - Reading the tag as ASCII cannot match by accident. WAV files and RIFF files too short to hold the tag stay audio.
  - A WebP file named `.wav` still fails on Chat/Responses; only the error message changes.
  - Tests in diff lines 714–786 cover named and extensionless files, both local and over HTTP.
- [x] **Hosted image URLs** (`NativeQuestions.scala` 203–213, 225–238).
  - The check is wrapped in `Try` and does no network fetch or DNS lookup. A null scheme does not throw.
  - Rejected: empty hosts (via `toURL.getHost`), signed or Unicode-digit ports, opaque URIs and non-HTTP(S) schemes. IPv6 brackets are handled.
  - Inline data URLs are detected case-insensitively (`regionMatches`) and forwarded unchanged.
  - Error messages never echo the URL (NativeQuestionsSuite, diff 1406–1430).
- [x] **128-image limit.**
  - Each non-blank path cell yields exactly one `input_image` part or throws, so the early count equals the final count.
  - `prepareDecisionsRequest` (151–163) recounts after loading.
  - Line 118 caps images in questions. Malformed questions are validated before `questionImageCount` runs. `questionsCol` must be a string column (`validateServiceColumn` 355–371).
  - Test at diff 921–970: an over-limit row makes no image-server requests.
- [x] **Configuration checks** (`AIRequestAdapter.scala` 247–281, 325–332).
  - Path columns require `inputMode="text"`.
  - When `messagesCol` is set, its own conflict message wins over the Decisions-specific `fileSizeLimitMB` message.
  - System One and embeddings still reject `columnTypes` and `fileSizeLimitMB`.
  - The inputCol/path conflict check uses `AIColumnPlan.matches`, the same matching as `resolve` and the `indices` lookup (`AIPrompt.scala` 458–460).
- [x] **Durable mode** (`AIDurableInput.scala` 20–33, 84–101, 165–180). Path cells are checked as `inline`. The check recurses into question `instructions[].image_url` and into JSON-input and `messagesCol` parts. Tests at diff 1142–1227 cover this together with resume.
- [x] **Errors and concurrency.**
  - The catch order in `AIPrompt.scala` 482–494 maps a cancelled read to `CancellationException`. A failed read becomes `input_io_error` (caught before the generic IllegalArgumentException), and a size failure becomes `invalid_request`.
  - Bounded reads produce size errors without filenames.
  - On the Decisions path the local lazy value is forced once. No shared mutable state is added, and regex matchers are created per call.
- [x] **Docs match the code**: reads capped at limit+1 bytes, Hadoop query/percent limitations, the list of pre-IO checks, header validation after IO, processing in column order, blank cells skipped, and all-blank rows giving `missing_input` (`OpenAIPrompt.scala` 611–612; `AIPrompt.scala` 360–361).
- [x] **Considered and not reported as issues:**
  - Deferred message loading changes which error a Decisions `messagesCol`/JSON row reports first. Both errors are controlled and the order is not promised.
  - Invalid (rather than missing) model/version values and unpaired surrogates are caught after IO; this is not promised.
  - Constant questions with 128 images plus a path column fail every row before IO, rather than at schema time.
  - A data URL the reader accepts but the native header check rejects gets the "HTTP(S) or inline" message. This is deliberate and tested.
- [ ] **Tests were not run.** My tools are read-only. I relied on the driver's validation evidence and inspected the test sources. The live suite needs credentials.

## Issues

### Issue 1: Bad parts in question instructions get an error that points at the input
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 91, 215–243 (fallback message at 243)
- **Description**: This PR sends question `instructions` arrays through `validatePart` (line 91). That method's fallback error, "Unsupported Decisions input part", was written for the shared `input`. Question instructions can now reach it, for example with an `input_file` part (which the docs explicitly say is unsupported) or a misspelled `type`. The resulting row error or schema-time exception points users at the input. The row error gives no other hint that the questions caused it. The new NativeQuestionsSuite case only checks that nothing sensitive leaks; it does not check this message.
- **Risk**: Diagnostics only. The request is still rejected correctly, before IO and inference, with a controlled `invalid_request`. Users may waste time checking prompt, path or `messagesCol` data instead of `questions`/`questionsCol`.
- **Suggested Fix**: Make the fallback neutral, for example "Unsupported Decisions content part; use input_text or input_image". Alternatively, pass a context label so question parts say "question instruction part". Add one NativeQuestionsSuite assertion for the instruction case. Optionally, align the input-only message "at most 128 images" (line 136) with the new "per request" wording.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver disposition

Issue 1 is closed without a product change as an optional diagnostic-clarity
recommendation. The original Low finding and `ISSUES_FOUND` verdict remain
unchanged; this does not claim that a more specific message was implemented.

`NativeQuestions.scala:89-91,215-243` deliberately validates the same native
`input_text` / `input_image` content-part types in shared input and question
instructions. The existing message identifies an unsupported input-part type;
it does not name a DataFrame column or assert that the top-level `input` field
is the failing field. It is less contextual than the suggested alternative,
but is not an incorrect acceptance, routing, or error-classification result.
Context-qualified errors throughout nested native question validation are not
part of this change's contract.

`NativeQuestionsSuite.scala:144-169` covers unsupported document parts and
malformed instruction content and checks that supplied sensitive values are
not disclosed. The independently reviewed request ordering rejects these
questions before loading path images or sending inference. The current native
suite's passing validation remains applicable because its source and production
validators are unchanged. No new test run or message improvement is claimed.

Keep the established shared validator and its existing diagnostic for this
focused image-support fix. More precise nested-field diagnostics remain a
possible follow-up. All 13 files were reviewed from the complete frozen diff;
continue pass 17 without changing patch
`5f939e3f515e6f44844ce05af6b10327dd65b1a020e282378b72bff27834d3d7`.