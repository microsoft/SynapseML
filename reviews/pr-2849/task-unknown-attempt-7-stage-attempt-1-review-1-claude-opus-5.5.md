---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 1
timestamp: "2026-11-15T06:07:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "be55890adda56449f5beb0c5320a128da163169ed6793004906f210273b06284"
---

I found one low-severity documentation gap. The production change, tests and other docs check out.

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-7-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **MIME lookup order** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, lines 126–152 and 293–305):
  - The order is: content sniffing of the bytes, then the JSON/XML check, then the Office extension, then the JVM filename map, then `application/octet-stream`. Existing sniffing still runs first.
  - `OfficeMimeTypes` is defined before `MimeTypeExtensions`, which reads it, so the object initialises safely.
- [x] **File classification** (same file, lines 307–325, 382–413 and 464–486):
  - A file is only classed as an Office `file` when its extension and MIME type are an approved pair (line 320).
  - The size check (line 406) still runs before classification.
  - The new message naming both the MIME type and the extension only fires for unsupported files whose MIME type is one of the six Office types (lines 477–479). Chat messages and data-URL messages are unchanged.
- [x] **JDK content sniffing** (`URLConnection.guessContentTypeFromStream`): it never detects ZIP-based OOXML files. For OLE files it only reports FlashPix, which it identifies by class ID. Genuine Office files therefore always reach the extension step. The test inputs behave as the tests expect:
  - HTML becomes `text/html`, sent as text.
  - `<table>` becomes `application/xml`, unsupported.
  - `{\rtf1` becomes `application/json`, unsupported.
- [x] **Data URLs** (`prepareDataUrlAttachment`): the extension comes from the reverse MIME map, so only the six exact Office MIME types are admitted. ZIP, OLE, macro-enabled and template MIME types map to `bin` and keep the existing "Unsupported file type" message.
- [x] **Callers and row-local errors:**
  - `OpenAIPrompt.wrapFileToMessagesList` (lines 631–655) routes HTTP(S) URLs through the same file-path branch as local paths.
  - Errors stay per-row through the `Try` in `createMessagesUDF` (lines 498–516).
  - `AIPrompt` inherits the change through `createMessagesForRow`, but only for `chat_completions` and `responses` (`AIRequestAdapter.scala` line 22). There, an `IllegalArgumentException` becomes an `invalid_request` row error (`AIPrompt.scala` around line 442).
- [x] **Chat restriction kept:** both `chatCompletionsMessage` and `chatCompletionsDataUrlMessage` still reject `file` attachments. `OpenAIChatCompletion.collapseContentPartsToText` (lines 313–326) joins text parts with `\n`, which matches the HTML Chat test assertion.
- [x] **New tests** (`OpenAIPromptMultimodalRequestSuite.scala` lines 191–273 and `OpenAIPromptOfficePathSuite.scala`): the assertions compare request bytes, filenames and MIME types exactly.
- [x] **Global MIME-map changes:** the tests restore `URLConnection.setFileNameMap` in `finally`. `build.sbt` sets `Test / parallelExecution := false` (line 282) in the shared settings that `cognitive` uses (lines 371–373), so the global change cannot leak into suites running alongside.
- [x] **Style:** both scalastyle configs allow 120 characters per line. The longest new line is about 116.
- [x] **No stale docs or pinned behaviour:** `columnTypes`, `Unified AI prompting.md` and `AIPrompt.ipynb` don't list attachment types. There is no Python copy of the classifier. No existing test expected `attachment.bin` or Office rejection.
- [x] **Notebook:** only markdown cells changed. The example still calls `.setFileSizeLimitMB(10.0)`. `OpenAIPrompt` sets no default for `fileSizeLimitMB`, and the limit is computed in MiB (`limit * 1024 * 1024`), matching the text.
- [ ] Not verified: the provider statements in the notebook (50 MB limits, 1,000 spreadsheet rows, text-only parsing of non-PDF files). The provider's page can't be reached with read-only repository tools.
- [ ] Not verified: the binary fixtures' contents and metadata, and the claimed test, build and live-run results. Per instructions the fixtures were not decoded, and this read-only review executed nothing.

## Issues

### Issue 1: Quickstart Office guidance does not say how HTTP(S) URLs are routed
- **Severity**: Low
- **File**: `docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb`
- **Line(s)**: 18–22 (the "Office documents" markdown cell)
- **Description**: The notebook intro lists three input forms: file paths, HTTP(S) URLs and data URLs. The new Office section only says "Use a file path or a data URL…" and gives rules for those two.
  - In the code, HTTP(S) URLs take the file-path branch (`wrapFileToMessagesList` → `prepareFile`).
  - Office routing therefore depends on the extension at the end of the URL path (`pathFilename` → `fileExtension`).
  - The server's `Content-Type` header is never read. `OpenAIPromptHttpTestBase` serves its extensionless JSON/XML URLs as `application/octet-stream`, and they are recognised only by content sniffing.
  - As a result, an Office link without an extension in its path (share or download links such as `…/download?id=…`) fails with `Unsupported file type: application/octet-stream.`, even when the server labels the file correctly. Data URLs behave the opposite way: their declared label is trusted.
- **Risk**: Users who follow the quickstart's HTTP(S) examples with Office links may expect the server's declared type to count, as it does for data URLs, and get confusing row errors. There is no security or data-integrity impact.
- **Suggested Fix**: Add one sentence to the Office section, for example: "HTTP(S) URLs follow the file-path rules: the URL path must end in a supported Office extension. Response `Content-Type` headers are not used, so extensionless download links are rejected; download the file or use a correctly typed data URL instead." Optionally, add an offline test using `withEchoServer`: one URL ending in `.docx` (accepted) and one extensionless Office URL (rejected).

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary estimate",
    "themes": "repository six-round policy",
    "execution": "repository sequential override",
    "model": "installed capability-first complementary-family rule"
  },
  "changeSize": "Bounded production MIME classification plus fixtures, request regressions and documentation.",
  "cyclomaticComplexity": "Low added branching; moderate API, platform MIME and package-integration interaction.",
  "rationale": "Six independent complementary-reviewer lenses cover the bounded patch. Publication and dependency sequencing are tracked separately.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "contextTier": "long_context",
  "capabilitySource": "https://platform.claude.com/docs/en/models/overview",
  "capabilityEvidence": "Anthropic recommends Opus 5.5 for long-running agentic coding. It is the strongest eligible Anthropic coding option in the current task model catalog.",
  "catalogEvidence": "Current task schema includes claude-opus-5.5 with max reasoning and long_context. Actual runtime identity must match.",
  "overrideSource": "SynapseML repository sequential six-round PR-loop policy",
  "stageGate": "All six lenses on one frozen patch; source fixes consume the pass.",
  "bounds": "Existing per-PR checkpoint counters remain in force; 20-minute per-review process-tree deadline.",
  "artifactPolicy": "Public-safe session drafts before a PR exists; numbered PR review directory after publication."
}

## Driver disposition and bounded stop

Issue 1 is verified and remains open. `pathFilename` takes the URI path and
`prepareFile` reads attachment bytes without consulting HTTP response headers.
The quickstart should explicitly document the required Office URL-path extension
and the alternative of a correctly typed data URL or a downloaded local file.

The configured five fix/integration cycles and six executed pre-commit passes
are exhausted. No further source edit, reviewer dispatch, commit, push or PR
creation was performed. A renewed budget is required to make the documentation
change and complete a fresh six-round pass before publication.

The final staged snapshot is unchanged: manifest SHA-256
`9dca9cc3503d35f0678e799aaf4df2c7fb8b00cec343df788d5a304274be40bc`.
All 128 request/path tests, compile, style and packaging checks passed for this
snapshot. The 39 tool calls in this review were audited with no access to earlier
feedback or checkpoints. This is a blocked result, not a clean signoff.

## Follow-up resolution after authorized continuation

Issue 1 is addressed in the Responses quickstart: HTTP(S) attachments follow
path routing, the URL path needs a supported Office extension, and response
Content-Type headers are not used. Extensionless links can instead be downloaded
or supplied as correctly typed data URLs. This documents the existing behavior;
it does not change routing.

Notebook JSON and list-of-strings cell sources validate. Executable notebook
cells and every source, test and fixture blob are unchanged from the successful
128-test build. The new frozen manifest SHA-256 is
`85a8ae4dfa381d8d9079a85b6bcd5a62c9c6bbbe94aa72fcf57c845b67f28d7f`.
Independent binary-diff replay reproduced that manifest. The prior review remains
an issues-found result; a fresh complete pass is required on the corrected patch.


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
