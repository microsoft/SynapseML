---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 3
timestamp: "2026-11-17T16:24:52Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "155452cf11ecc7bbac9756087aac3f3fe8718333efb7360f7e5976b954430171"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-13-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope.** I reviewed the full supplied ten-file patch. I also read these parts of the current source:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`: `retainedMessagesName` 295-301, `messages` 364-386, `buildRequest` 388-401, `preparePrompt` 419-438, `prepareRequests` 453-500, `infer` 576-600.
  - `AIRequestAdapter.scala`: `validateCapabilities` 247-264, `validateMessageConstruction` 273-280, `validateInputs` 302-326, `validateServiceColumn` 344-360, `endpoint`/`versionedEndpoint` 494-518, `input` 524-532, `nativeEnvelope`/`request` 571-623.
  - `NativeQuestions.scala` (all of it), `AIJson.scala` 150-240, `services/openai/OpenAIDecisions.scala`.
  - `OpenAIPrompt.scala` 241-296 and 577-659; `OpenAIAttachmentUtils.scala` 24-30, 100-200 and 290-530.
  - `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 112-128, 240-360 and 724-755.
- [x] **Pre-IO ordering holds.** `AIRequestAdapter.request` (602-622) runs `selectedModel`, then `requestVersion`, then `mergedOptions`, then `nativeEnvelope`. Inside `nativeEnvelope` (571-587), questions are parsed and validated, then `validateDecisionsImageCount(pathImageCount, …)` runs, and only then `messages()` at 579. That call runs the prompt-text checks and then `generatedMessages`. So missing model/version bindings, malformed options/questions and path-plus-question overflow all fail before any attachment IO, as documented at docs 299-303.
- [x] **Lazy messages are safe.**
  - Non-Decisions APIs still evaluate `inputMessages` eagerly through the three-argument overload. Retained messages reuse the cached lazy value, so there is no double read.
  - Decisions never retains messages, because `retainedMessagesName` covers only `GenerativeApis`. The thunk is called once.
  - Neither `nativeEnvelope` nor `request` has an inner `try`. Cancellation, read and size exceptions from the thunk therefore reach the same `prepareRequests` catch (483-495). `TaskKilledException`/`InterruptedException` still propagate.
- [x] **The pre-IO image count is exact.**
  - `pathImageCount` (AIPrompt.scala:476) uses the same `trim.nonEmpty` predicate as `createMessagesForRow` (OpenAIPrompt.scala:608-610).
  - Each remaining path yields exactly one `input_image` or throws.
  - `prepareDecisionsRequest` re-checks the total after preparation.
  - Path columns must be strings (AIRequestAdapter.scala:313), so `row.getString` cannot throw `ClassCastException`.
- [x] **Prompt-text errors stay distinct.** Null or empty text raises `AIMissingInputException("input is null or empty")`, the same message as `input()` at AIRequestAdapter.scala:530. Whitespace-only text raises an `IllegalArgumentException` from `validateDecisionsInput(JsString)`. Both happen before `generatedMessages`. A row where every path is blank still gives `missing_input` "attachment input is missing".
- [x] **The format guard stays out of Chat/Responses.**
  - `validateImageAttachment(isImage: => Boolean)` short-circuits on `apiType`, so Chat/Responses never evaluate `dataUrlFileType`, `mayBeImagePath` or the MIME allowlist.
  - When `mayBeImagePath` is false (a non-empty, non-image extension), `categorizeFileType` cannot return "image". The pre-IO rejection therefore never refuses something the shared classifier would accept.
  - Data URLs with no comma, `data:,` or no base64 marker fail with controlled `IllegalArgumentException`s, not index errors.
- [x] **The RIFF/WEBP fix is safe.** The 12-byte length guard prevents out-of-range offsets, and non-WEBP RIFF files (WAV, AVI) still use JDK sniffing. I traced a text file starting `RIFF....WEBP` named `.md`/`.csv`: it is still categorized as text, and `.txt`, which was already unsupported, stays unsupported. A WebP named `.wav` changes only which error message Chat/Responses report.
- [x] **`hostedImageUrl` is robust** (NativeQuestions.scala 203-213). Everything is inside `Try`.
  - Rejected: signed, non-ASCII-digit and overflowing ports; `host:80:80`; empty hosts; opaque `http:foo`; leading whitespace; non-image `data:` URLs.
  - Accepted and forwarded unchanged: IDN, underscore, IPv6, user-info and empty-port URLs.
  - Diagnostics never echo the URL.
- [x] **Capability checks behave as described.**
  - System One still rejects `columnTypes`.
  - Decisions allows `fileSizeLimitMB` only when there is a path entry.
  - Path columns require `inputMode="text"`. `inputMode` defaults to "text" (AIPromptParams.scala:105) and `columnTypes` defaults to an empty map (OpenAIPrompt.scala:293), so neither getter can throw.
  - `messagesCol` still rejects a non-empty `columnTypes` and `fileSizeLimitMB`.
  - `OpenAIDecisions` inherits `AIPrompt` unchanged.
- [x] **JSON shape is preserved.** `prepareParts`/`updated` keep key positions in AIJson's ordered map. `questionsCol` must be a string column (AIRequestAdapter.scala:358), so struct null members cannot reach the request. Only `file_id`/`detail` nulls on `input_image` parts are stripped.
- [x] **Durable mode does no IO.** `AIDurableInput` requires inline path values and inline question images, and resume semantics are unchanged.
- [x] **Accepted behavior change.** For Decisions with `messagesCol`, the messages column is now read after question/model validation. This only changes which error a row with several faults reports, and involves no IO.
- [x] **Supplied validation used as evidence only.** I reviewed the driver's results (128 tests across five suites, plus style, compile and codegen) but did not re-run them.
- [ ] **Not verified:** live-provider Decisions behavior and actual test execution. This lens is read-only and cannot run anything.

## Issues

### Issue 1: Per-row header validation still runs after Decisions image IO
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala (also cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala)
- **Line(s)**: AIPrompt.scala 388-399 and 474-479; AIRequestAdapter.scala 579 and 622
- **Description**: The new thunk delays path reads until after the model, version, options, questions and image-count checks. But `buildRequest` only runs after `adapter.request` has returned, which is after the thunk has read every path image. That is where two per-row header checks happen:
  - `AIPrompt.validateHeader` checks values from column-bound `subscriptionKey`, `AADToken` and `CustomAuthHeader`.
  - `validateCallerHeaders` checks map names from `customHeadersCol` and `telemHeadersCol`.

  For example, a key column whose values end in `\n`, or a `customHeadersCol` entry named `Content-Length`, makes every row read or download all of its images before failing with `invalid_request` ("Invalid HTTP header value" or the framing-header message). `endpoint(modelName, version)` at AIRequestAdapter.scala:622 also runs after IO, though its per-row failures are rarely reachable for Decisions.
- **Risk**: Rows that are certain to fail still use executor time, storage/HTTP egress and memory for each image. When `fileSizeLimitMB` is unset, each file is buffered whole, so a large file can cause an out-of-memory failure before the header error is reported. The final error code is correct, and this case is outside the pre-IO guarantees you listed. Treat it as optional hardening, not a contract violation.
- **Suggested Fix**: For Decisions, check the row-bound header names and values (`customHeaders`/`telemHeaders` maps and explicit credential columns) inside the thunk, just before attachment reads. These checks don't depend on the resolved address, and the existing order of the other checks stays the same. Add a regression test that combines an invalid column-bound header with a path column served by the recording server, and assert that no image requests are made. Alternatively, document that header validation happens after attachment loading.

### Issue 2: Decisions docs call the path reader "bounded", but reads are unbounded unless `fileSizeLimitMB` is set
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Unified AI prompting.md
- **Line(s)**: 291-294 (compare 743-749); code references: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala 269-296, OpenAIAttachmentUtils.scala 100-112 and 350-351
- **Description**: The new paragraph says Spark loads path inputs "using the existing bounded attachment reader and per-file `fileSizeLimitMB`". However, `fileSizeLimitMB` has no default, so `attachmentSizeLimit` is `None`. In that case `prepareFile` calls `readFile`, which reads the whole stream with `IOUtils.toByteArray`. The only caveat ("Unset or positive-infinite limits remain unlimited") is about 450 lines later, in a generic section. Decisions path columns now accept arbitrary HTTP(S) URLs and filesystem paths from row data, so a reader of the Decisions section alone would assume reads are capped by default.
- **Risk**: The example sets a limit, but users who adapt it may drop `setFileSizeLimitMB`. If a path column then points at a very large object or a never-ending HTTP body, and the name is supported or extensionless, the whole body is buffered before the content-type check. `readAttachment` catches only non-fatal exceptions, so an `OutOfMemoryError` fails the task or executor instead of producing a row-level `input_io_error`/`invalid_request`.
- **Suggested Fix**: Clarify the sentence. For example: "…sends their bytes as image data URLs. Set `fileSizeLimitMB` to cap each read; AIPrompt then reads at most the limit plus one byte. When unset, each file is read fully into executor memory." Optionally recommend always setting it for hosted or untrusted paths. No code change is needed.

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

## Driver triage before collecting the remaining lenses

- Issue 1 is optional hardening, not a defect in the advertised pre-IO contract.
  `buildRequest` still validates headers before inference, and the documentation
  lists the specific checks that precede attachment loading. Reordering
  credential/header assembly is not required for this image-support change.
  The current order is retained; clarify that distinction alongside the size
  limit wording. No claim is made that every invalid row fails before IO.
- Issue 2 is confirmed: `fileSizeLimitMB` has no default, so describing the reader
  as unconditionally bounded is inaccurate. A documentation correction is
  queued until the other lenses finish on this unchanged snapshot.
- Source/index content remains frozen. This triage does not resolve Issue 2,
  convert the original `ISSUES_FOUND` verdict, or qualify this pass for
  publication. After collected findings are addressed, a fresh full pass is
  required.

## Final driver resolution

- **Issue 1: resolved by clarification, not reordered header processing.**
  The Decisions section now explicitly says header validation follows
  attachment loading. The existing pre-IO guarantees remain precisely scoped.
- **Issue 2: fixed.** The same section now states that finite `fileSizeLimitMB`
  caps reads at the limit plus one byte, whereas unset or positive-infinite
  limits read the whole file into executor memory.
- Verified the documentation against the unchanged reader implementation and
  parsed all six Python examples. Root style, compilation, test compilation,
  codegen, and 139 deterministic tests passed on the corrected snapshot.
  Three live tests canceled without credentials; they are not provider proof.
- Corrected full-patch SHA-256:
  `008f86591bfcb8609c7699e9c7f0f25d633629fe26c417c83762cee9088eb8c8`.
  This source change invalidates pass 13. Its original findings remain intact;
  pass 14 must review all six lenses independently.