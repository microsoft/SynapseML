---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 5
timestamp: "2026-11-15T09:47:59Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "7c297ec2089c570d6a932018d960f5cade11a562442fb79942b0d892f4f37445"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-11-stage-attempt-1-review-5-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Mapped each new production branch in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` to at least one test that checks it:
  - the Office table and reverse MIME map (126-152);
  - the Office lookup in `inferMimeType` and `fileExtension` (293-305);
  - the Office pair check in `categorizeFileType` (320);
  - the new Office diagnostic in `responsesMessage` (477-479).
- [x] Reasoned through mutations without running them. Each change below would make named tests fail:
  - Dropping the reverse map breaks the data-URL cases (`OpenAIPromptMultimodalRequestSuite.scala` 191-236).
  - Moving the Office lookup ahead of content sniffing breaks the HTML cases (238-261) and the markup/RTF cases (`OpenAIPromptOfficePathSuite.scala` 144-161).
  - Moving it after the name lookup breaks the container-override cases (111-142).
  - Removing `Locale.ROOT` lowercasing breaks the uppercase temp-file cases.
  - Removing or ungating the diagnostic breaks the exact-message assertions at `OpenAIPromptOfficePathSuite.scala` 52, 81 and 156.
- [x] Verified the global-state claim:
  - `URLConnection.setFileNameMap` is restored in `finally` (`OpenAIPromptOfficePathSuite.scala` 83, 138).
  - `build.sbt` 282 sets `Test / parallelExecution := false` in the shared `settings`, and `cognitive` uses them (371-373).
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/core/test/base/TestBase.scala` 152 mixes in no `ParallelTestExecution`.
- [x] Verified row-local error capture:
  - `OpenAIPrompt.scala` 499-514 wraps `createMessagesForRow` in `Try` and sends every failure except `TaskKilledException` to the error column.
  - Existing transformer tests (`OpenAIPromptMultimodalRequestSuite.scala` 331-353, 369-382, 626-639) cover argument, read and size errors.
  - So the Office rejections tested through `createMessagesForRow` get the same capture.
- [x] HTTP test (`OpenAIPromptOfficePathSuite.scala` 18-57) checks:
  - the exact `input_file` map;
  - the exact octet-stream error;
  - the exact URIs the server received;
  - that the exchange and server are closed in `finally`.
- [x] Query-string limitation: this is existing transport behavior that the contract keeps on purpose. The existing test at 369-382 already shows a URL with a query string failing row-locally with a sanitized message, so I did not raise it.
- [x] Notebook:
  - Only markdown cells change.
  - The example cell sets `.setFileSizeLimitMB(10.0)` (notebook line 152).
  - `validateFileSize` uses MiB (`limit * 1024 * 1024`).
  - `OpenAIPrompt.setDefault` sets no default `fileSizeLimitMB`.
- [x] Fixtures (binaries not decoded, per instructions):
  - `cognitive/src/test/resources/office/README.md` documents where they came from.
  - `.gitattributes` (`* text=auto eol=lf`) plus the binary patches means line endings won't be rewritten.
  - Loading via `getResource(...).toURI` matches existing practice (`SpeechHlsFixture.scala` 35).
- [x] Searched cognitive/core Scala tests and cognitive Python tests for older Office MIME or `attachment.bin` expectations. None exist, so no existing assertion was silently reversed.
- [x] Traced `AIPrompt`:
  - It `extends OpenAIPrompt` (AIPrompt.scala 170), calls the shared `createMessagesForRow` (319) and sets `useBoundedAttachments = true` (188).
  - It has no attachment-type allowlist of its own (searched `services/ai`).
- [ ] Test execution was not performed because this review is read-only. The supplied result (129 passed, 0 skipped) was not reproduced.

## Issues

### Issue 1: New Office assertions are partial and would miss extra parts or diagnostic regressions
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala`
- **Line(s)**: MultimodalRequestSuite 212-216, 224, 230; OfficePathSuite 44-48, 100-104, 132-136
- **Description**: This is a weakness in test assertions, not a production bug.
  - Every positive Office case compares only `parts(1)` / `content(1)`. None checks the full content array or the part count.
  - The Chat rejection check is only `contains("use apiType='responses'")`. Every non-text, non-image Chat error contains that phrase, whether the file was classed as `file` or `unsupported` and whatever MIME is reported.
  - The size check is only `contains("exceeds limit")`, so it can't tell the path message from the data-URL message.
  - The suite already checks the exact Chat message for PDF (296-306) and the exact part types for images (62-74).
- **Risk**:
  - An extra or duplicated part would pass, for example a text fallback added next to the `input_file`.
  - A Chat error that reports the wrong type or Office MIME would also pass.
  - Classification regressions are still caught by the Responses assertions in the same loop, so the impact is limited to message wording and part count.
- **Suggested Fix**:
  - Assert the full part list, e.g. `parts.map(_.asJsObject.fields("type")) == Seq(JsString("input_text"), JsString("input_file"))`, and `messages(1).content.size == 2` in the path suite.
  - Compare the Chat message exactly: `s"File type 'file' with MIME type '$mimeType' is not supported for Chat Completions. Only text and image attachments are supported; use apiType='responses' for other file inputs."`
  - Check the size-error prefix for each case: `Attachment '<name>' size ` for paths, `Data URL attachment size ` for data URLs.
  - Optionally split the Chat and size checks into separately named tests.

### Issue 2: Office support silently reaches `AIPrompt` (bounded reader, `invalid_request` errors) with no test
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` (tests belong in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptAttachmentSuite.scala`)
- **Line(s)**: 170, 188, 319, 435-446
- **Description**: This is a coverage and scope gap, not a production defect.
  - `AIPrompt` extends `OpenAIPrompt` and reuses `createMessagesForRow` with bounded reads.
  - On `responses` with `path` columns it now accepts all six Office formats. It reads them through `readBoundedFile` and the session's Hadoop configuration.
  - It reports the new alias diagnostic and Chat rejection as `invalid_request` errors (435-446).
  - `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (632-639) presents `AIPrompt` as the main entry point and `OpenAIPrompt` as legacy.
  - The new tests and the PR docs cover only `OpenAIPrompt`. In the AIPrompt test suites, `input_file` appears only as raw `messagesCol` parts (`AIPromptMediaInputSuite.scala` 241-247).
- **Risk**:
  - The recommended stage gains a capability that no test or doc mentions.
  - A later AIPrompt-only change (bounded reader, error format, `{asset}` filename placeholders such as `attachment.docx`) could break Office handling without a failing test.
  - If AIPrompt support was not intended, nothing flags it.
  - The shared helpers don't depend on format, so a present defect is unlikely.
- **Suggested Fix**:
  - Add one `responses` case with `setColumnTypes(Map("asset" -> "path"))`, `setFileSizeLimitMB(1.0)` (to force the bounded reader) and a temp copy of `/office/invoice.docx`. Assert the exact `input_file` part in the recorded request.
  - Add one alias or Chat case asserting `invalid_request` with the exact message.
  - Alternatively, state in the PR and AIPrompt docs whether AIPrompt is in scope.

### Issue 3: Documented data-URL behavior is not tested for Office labels on non-Office bytes
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
- **Line(s)**: 191-236, 238-261, 263-277 (production: `OpenAIAttachmentUtils.scala` 222-238, 418-440; docs: notebook line 22)
- **Description**: This is optional hardening of behavior the contract keeps on purpose; it is not a defect.
  - The docs say data URLs "use their declared MIME type without content detection, so the label must match the bytes". Paths behave differently: HTML in a `.doc` file is sent as `input_text`, and a test (238-261) checks that.
  - The Office data-URL tests that expect acceptance use only real Office bytes, which content sniffing would not reclassify anyway.
  - The rejection tests use ZIP header bytes.
  - So no test shows that an Office-labeled data URL containing HTML or CSV bytes is still forwarded unchanged as `input_file`.
- **Risk**: If someone later adds sniffing to data URLs (for example, to match paths), this documented behavior would change silently and the docs would become wrong.
- **Suggested Fix**: Add a `responses` case using `data:application/msword;base64,<base64 of the HTML string>`. Assert the part equals `{"type":"input_file","filename":"attachment.doc","file_data":<original data URL>}`, and optionally that Chat rejects it.

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

## Driver rebuttal

These are retained coverage-hardening suggestions, not required corrections to
the feature's regression coverage. No reviewed content changed.

1. **Assertions cover the changed contract.** Each new accepted Office case
   compares the entire attachment object, including type, exact MIME, filename,
   and bytes. `requestPayload` also requires exactly one outgoing request and
   the cases require no row error. Unchanged `wrapFileToMessagesList` returns
   exactly `Seq(fileMessage)`; it has no Office-specific assembly branch.
   Existing image tests assert the common two-part sequence. Negative tests
   assert rejection and the relevant limit/API guidance without unnecessarily
   fixing every unchanged diagnostic word. New alias diagnostics have exact
   assertions. More assertions are possible, but no changed branch lacks a
   regression that would fail on the old implementation.
2. **Inherited support is intentional.** `AIPrompt` uses the same classifier and
   serializer for generated Responses requests. Its bounded read occurs before
   classification and has no format-dependent path; its error mapping also has
   no Office-specific branch. The PR now explicitly states this scope.
   Independently ran the existing `AIPromptAttachmentSuite` on the frozen patch:
   **28 passed, zero failed/canceled/ignored/pending**, on JDK 11, Spark 3.5.0,
   Scala 2.12.17. These tests cover bounded reads, session Hadoop configuration,
   default/explicit limits, cancellation, and `invalid_request` mapping.
   They complement the six-format classifier/request regressions; they are not
   described as dedicated Office or live-provider AIPrompt tests.
3. **Declared-MIME trust is unchanged.** Data-URL parsing and serialization
   already trust MIME independently of bytes. This patch adds six reverse-map
   entries and tests each with exact original-URL preservation. It does not
   introduce a content-sniffing branch on that path. An HTML-in-Office-data-URL
   test would be optional additional protection for the existing generic
   contract, rather than a missing new-format admission test.

All 53 reviewer tool calls were audited: source/test/build/documentation reads
only, with no access to earlier feedback. Original findings and verdict above
are preserved. The additional validation did not alter the frozen source.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- This is part of the six-round pre-commit review on that product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
