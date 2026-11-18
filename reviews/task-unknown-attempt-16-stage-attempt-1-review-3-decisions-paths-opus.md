---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 3
timestamp: "2026-11-17T23:46:38Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "8d8d980478a76439f785f402e54e86547a9c3f77704000f246de14fd881d1668"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-16-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` (full file): instruction arrays go through `array()`, which rejects empty arrays, and then through `validatePart`. Question images are capped at 128, and so is the input-plus-question total (`validateDecisionsImageCount`, `prepareDecisionsRequest`). `hostedImageUrl` is wrapped in `Try`. Its scheme test short-circuits before `toURL`. It accepts IPv6 and empty ports, and rejects empty hosts and signed or non-ASCII-digit ports. The `data:image/` inline branch keeps the strict header and base64 checks. `prepareParts` strips null `file_id`/`detail` only on `input_image` parts.
- [x] `AIRequestAdapter.scala` `request` (lines 610–635) and `nativeEnvelope` (582–598): checks run in this order: model (`selectedModel` 415–420 also rejects blank), version, options, question parse/validate, path-plus-question count, then lazy `messages()` (IO). The 3-argument overload passes `pathImageCount = 0` for other APIs. Grep shows `AIPrompt.prepareRequests` (477–478) is the only caller, so durable and non-durable runs share the path.
- [x] `AIPrompt.scala` `messages` (364–386) versus `AIRequestAdapter.input` (535–543): null or empty text gives `missing_input` "input is null or empty", and whitespace gives `invalid_request`, on both the path and non-path routes. In `prepareRequests` (466–494), the local `lazy val` is evaluated at most once per row. Every new failure is an `IllegalArgumentException` or `AIMissingInputException` and is caught. `retainedMessagesName` (295–301) is generative-only, so Decisions never forces messages a second time.
- [x] `AIRequestAdapter.scala` `validateCapabilities` (247–265), `validateMessageConstruction` (275–281), `validateInputs` (293–323) and `validateDecisionsInputColumn` (325–332): exactly one input binding is required. The `messagesCol` conflicts fire before the `fileSizeLimitMB` check. System One and embeddings still reject `columnTypes`/`fileSizeLimitMB`. The `inputCol` overlap check uses Spark's resolver.
- [x] `OpenAIPrompt.scala` (577–650): `validateImageAttachment` takes its argument by name, so Chat/Responses never evaluate the Decisions prechecks. The `_.trim` attachment filter matches the image-count precheck. Decisions produces one `user` message with no system prompt. A row with no usable path gives `missing_input`, as documented.
- [x] `OpenAIAttachmentUtils.scala` `inferMimeType` (295–308): the WebP check requires at least 12 bytes, so short RIFF data and real WAV stay audio. `categorizeFileType` (320–338) and `MimeTypeExtensions` mean only JPEG, PNG, GIF and WebP MIME types count as images. `mayBeImagePath` (315–318) lowercases the extension and does not throw.
- [x] Durable inline-only media enforcement in `AIDurableInput.scala` covers path columns and question images. `AIJson` ordered maps keep key order on `updated` and `-`, so the wire shape is unchanged.
- [x] `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (291–330) matches the code on pre-IO checks, header validation after loading, Hadoop query/percent limits, rejection of non-image extensions, and durable inline-only media.
- [x] Tests inspected: `AIPromptMediaInputSuite.scala` (272–520), `AIPromptAttachmentSuite.scala` (290–390). A grep of the `ai` test directory finds no multi-path Decisions test with an unsupported extension in a later column.
- [ ] I did not run any tests (read-only review). Runtime results come only from the driver's evidence.

## Issues

### Issue 1: Multi-path Decisions rows read earlier attachments before rejecting a later column's unsupported format
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 608–621, 631–649
- **Description**: The Decisions format precheck runs inside `wrapFileToMessagesList`. That is `mayBeImagePath` for file/HTTP paths and `dataUrlFileType == "image"` for data URLs. `buildContentParts` calls it one attachment at a time through `paths.flatMap(...)`, over a strict `Vector`. In a row with several path columns, attachment N's extension is checked only after attachments 1..N-1 have been read through `prepareFile` and size-checked. The combined image count is different: `AIPrompt.prepareRequests` (474–477) checks it across all path cells before any IO.
  - Example 1: `first=https://host/photo.png` and `second=.../scan.bmp`. The client fetches `photo.png` over HTTP, then the row fails with `invalid_request`.
  - Example 2: `first=missing.png` and `second=scan.bmp`. The row reports `input_io_error` instead of the deterministic `invalid_request`. Changing the schema column order flips the code.
  - The existing test at `AIPromptMediaInputSuite.scala` 473–505 uses only single-path rows, so no test covers this.
- **Risk**: Rows that are certain to fail still read files and make HTTP requests. With `fileSizeLimitMB` unset, that means whole-file reads into executor memory, repeated on every row when one column is misconfigured. The error code depends on column order, and an IO code can look transient and invite retries. No incorrect request is sent to the provider.
- **Suggested Fix**: When `getApiType == "decisions"`, add a pre-pass in `buildContentParts`/`createMessagesForRow` over `orderedAttachments` before any `wrapFileToMessagesList` call:
  - data URLs: `validateImageAttachment(dataUrlFileType(...) == "image")`;
  - other paths: `validateImageAttachment(mayBeImagePath(...))`.

  Keep the post-read MIME check, and keep Chat/Responses unaffected through the API guard. For each stage, add a two-path test: column 1 is a recording image server's `/photo.png` (or a missing `.png`), column 2 is `missing.bmp`. Assert `invalid_request` with the JPEG/PNG/GIF/WebP message and `imageServer.requests.isEmpty`. If per-file semantics are intended instead, say in the docs that earlier path columns may be read first.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver disposition

**Issue 1: Closed without changes as a required fix; retained as an optional
row-wide IO optimization.** The observation is correct, and the original
`ISSUES_FOUND` verdict is unchanged. Earlier supported attachments may be read
before a later unsupported path is reached. This is not a violation of the
current per-file contract, and no invalid request is sent to the provider.

- **What/why:** Keep schema-ordered attachment processing and first-failure
  diagnostics. The requested feature is to interpret marked columns as images,
  not to introduce row-wide format-error precedence over earlier IO/size errors.
  A preliminary scan could save IO for some invalid rows, but would deliberately
  change that precedence and still could not identify misleading filenames or
  unsupported extensionless contents without reading them.
- **Evidence:** `OpenAIPrompt.scala:608-621,631-651` processes each trimmed,
  non-blank path in order and rejects its unsupported extension before reading
  that file. `Unified AI prompting.md:306-324` separately lists the request-wide
  pre-IO checks, states that unsupported filenames are rejected "without reading
  the file", and explains that supported/extensionless names require inspection
  and can fail IO or size checks first. It does not promise a row-wide format scan.
- **Verification:** Source inspection confirms the reported ordering.
  `AIPromptMediaInputSuite.scala:473-503` proves the per-file no-IO guarantee
  under bounded/unset limits for both public stages; `:505-553` proves the
  distinct request-wide count guarantee with multiple paths and questions.
  These tests passed in the latest focused run. No claim is made that they
  exercise or implement the proposed row-wide optimization.
- **Prior disposition:** This duplicates pass 15 round 3's retained performance
  opportunity; it is not a newly introduced regression. Its prior deferred
  suggestion remains visible in that artifact. For this release it is
  explicitly not adopted, rather than left as an unaddressed required fix.
- **Communication:** The external PR-description draft now says that earlier
  path columns may already have been read when a later unsupported path fails.
  This clarifies existing behavior without changing repository documentation,
  source, tests, or the frozen review snapshot.

The pass continues on product patch
`05a6a91f7184b4b33639b785368163082b8e4e00272a6f2d34e6beb7c7924dee`.
This is a source-backed driver disposition, not a clean reviewer verdict or a
claim that the optimization was implemented.