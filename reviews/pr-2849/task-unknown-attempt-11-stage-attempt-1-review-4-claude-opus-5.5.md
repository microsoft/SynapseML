---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 4
timestamp: "2026-11-15T09:31:59Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "d44c9ac9701ebf8a1e14b0f4e0385d55389166e86bdab2300a0b3876b7055598"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-11-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **MIME lookup order** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, `inferMimeType` lines 293–299): content sniffing runs first, then structured-text detection, then the Office extension, then the JVM name map, then `application/octet-stream`.
  - Genuine Office files are not identified by the earlier steps. OLE2 files (`D0 CF 11 E0`) only match the JDK's FlashPix check (`image/vnd.fpx`) when they carry the FlashPix class ID, and ZIP files (`PK`) don't start with `{`, `[` or `<`. So genuine files reach the Office lookup.
  - Detected HTML still comes back as `text/html`. Office-named `<table>` content maps to `application/xml` and RTF `{` maps to `application/json`, both of which stay unsupported. This matches the contract.
- [x] **Extension/MIME pair check** (`categorizeFileType`, line 320): both call sites pass a lowercase extension (`fileExtension` or `extensionForMimeType`) and a lowercase MIME. The check therefore admits only the six canonical IANA pairs.
  - `.docm`, `.xlsm`, `.pptm`, `.dot`, `.zip` and `.bin` fall through to `unsupported` even when the name map returns an Office MIME.
  - No Office MIME starts with `text/`, `image/` or `audio/`, so no later branch can admit one.
- [x] **Data URL flow**: `prepareDataUrlAttachment` takes the extension from the declared MIME using the extended `MimeTypeExtensions`. That map is defined after `OfficeMimeTypes` (line 152), so initialization order is safe.
  - `file_data` is the original URL, unchanged.
  - ZIP, OLE, macro-enabled and template MIME types map to `bin` and produce the generic unsupported message. `OpenAIPromptMultimodalRequestSuite` asserts this exact message.
- [x] **`fileExtension` refactor** (lines 301–304): behaves the same as the inline code it replaces (`lastIndexOf('.')`, `substring(idx + 1)`, `Locale.ROOT`).
  - Filenames come from `pathFilename` (`HPath.getName`), so dots in directory names can't affect the extension.
  - The new Office-specific unsupported message (lines 477–479) is only reachable for non-approved extensions that the name map labels with an Office MIME. Its guard compares lowercase values.
- [x] **Callers** (`OpenAIPrompt.scala`, `wrapFileToMessagesList` lines 631–653): data URLs go to the data-URL helpers and paths go to `prepareFile`.
  - Size checks run before categorization in both flows.
  - Chat rejects the `file` type with the `use apiType='responses'` message.
  - Errors stay row-local through the `Try` in `createMessagesUDF` and through `AIPrompt.prepareRequests`, which maps `IllegalArgumentException` to `invalid_request`.
  - `AIPrompt` inherits the same admission rules, and only for Chat and Responses requests (`GenerativeApis`).
  - `OpenAIResponses.validateInputFilePart` (lines 507–521) does not restrict MIME types.
- [x] **Test assertions**:
  - The `input_file` checks compare the complete object: filename, canonical-MIME data URL and bytes.
  - The Chat HTML assertion relies on text parts being joined with `mkString("\n")` (`OpenAIChatCompletion.scala:325`), which they are.
  - Global `FileNameMap` changes are restored in `finally`, and `build.sbt:282` sets `Test / parallelExecution := false`.
  - The HTTP test closes each exchange and the server in `finally`. Its queue-to-`Seq` comparison is element-wise.
  - Test names are unique, and the new suite is covered by the `pipeline.yaml` `openai` leg (`PACKAGE: "services.openai"`).
  - No line exceeds the 120-character scalastyle limit.
- [x] **Notebook claims checked against code**:
  - Content detection takes precedence, and HTTP routing uses the URL path extension, not `Content-Type`.
  - Hadoop's `Path(String)` treats `?` as part of the path, and the multi-argument `URI` constructor percent-encodes it in the request path.
  - Data URLs are trusted by their declared MIME.
  - `fileSizeLimitMB` has no default and applies only when set (`attachmentSizeLimit`, line 627). The check uses MiB arithmetic.
  - The example cell sets `.setFileSizeLimitMB(10.0)`.
  - The H1→H3 heading jump follows the existing `AIPrompt.ipynb` pattern, so it is house style rather than a defect.
  - Only markdown cells changed.
- [x] **Fixtures**: the README was reviewed instead of decoding the binaries. `.gitattributes` uses `text=auto`, and the diff stores all six files as `GIT binary patch`, so they are stored as binary.
- [ ] **Not executed**: this review was read-only, so I ran no builds or tests. The 129-test count and the live extraction results come from the author and were not checked here.
- [x] **Non-blocking observations** (not defects; no action needed):
  - Data URLs labelled `application/vnd.ms-excel` that actually contain CSV (a common browser label) will now reach the provider instead of failing locally. This follows the "data URLs trust their declared MIME" contract. Such URLs were already rejected before this patch, so existing text handling is unchanged.
  - Chat's existing message tells users to try `apiType='responses'` even for types Responses also rejects. This predates the patch.
  - `invoice.ppt` is about 646 KiB, compared with at most 37 KiB for the other fixtures. Optional: make it smaller.
  - Optional: add explicit `binary` attributes for the Office fixtures so their storage doesn't depend on Git's auto-detection.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- This is part of the six-round pre-commit review on that product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
