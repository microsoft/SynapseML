---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 1
timestamp: "2026-11-15T07:29:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "80612f90b4328cacaa4040dc3280e1db4ed814611566104229ce307a722d33ab"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-10-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **MIME detection order (`OpenAIAttachmentUtils.scala`, `inferMimeType`, lines 293-300).** The order is: JDK content sniffing, then `inferStructuredTextMimeType`, then the Office extension lookup (line 297), then the JDK file-name lookup.
  - Neither sniffer matches ZIP (`PK\x03\x04`) or OLE (`D0 CF 11 E0`) headers. OLE only becomes `image/vnd.fpx` when the FlashPix ID is present. Real Office files therefore reach the extension lookup.
  - HTML, XML and JSON-looking content still wins over the extension.
  - Generic ZIP, OLE and octet-stream file-name mappings can no longer override the six approved extensions.
- [x] **Allowlist check (`categorizeFileType`, line 320).** A file is admitted only when the MIME exactly matches the one listed for its extension.
  - For paths, this needs an approved extension and no sniffed type.
  - For data URLs, the extension comes from `extensionForMimeType`. Its new reverse entries (line 152) cover only the six standard MIME types.
  - ZIP, OLE, macro-enabled, template and alias MIME types still map to `bin` and are rejected as unsupported.
  - `.docm`, `.xlsm`, `.pptm`, `.dot`, `.zip` and `.bin` files that the file-name map labels with an Office MIME reach the new extension-specific error (line 479).
  - `OfficeMimeTypes` (line 126) is declared before `MimeTypeExtensions`, so the object initializes safely.
  - `fileExtension` (line 302) reproduces the removed inline logic, including `Locale.ROOT` lowercasing.
- [x] **How requests are built (`OpenAIPrompt.scala`).**
  - `wrapFileToMessagesList` (around lines 630-652) sends data URLs to `responsesDataUrlMessage`, which forwards the original URL as `attachment.<ext>`.
  - Paths go to `prepareFile` and `responsesMessage`, which base64-encode the bytes that were read and keep the path's filename.
  - The Chat helpers still reject `file` and `unsupported` attachments.
  - `createMessagesUDF` (lines 499-513) keeps attachment errors on the affected row.
  - `validateResponsesApiCompatibility` blocks `responses` with AI Foundry chat endpoints.
  - In `OpenAIResponses.scala`, `validateInputFilePart` (lines 506-521) accepts `type`, `filename` and `file_data` with no MIME allowlist.
  - Size checks and their MiB arithmetic are unchanged and run before classification.
- [x] **Other affected code.**
  - `AIPrompt` inherits the change through `generatedMessages` (lines 314-322), only for the chat and responses APIs. It still turns errors into `invalid_request` rows.
  - Its template-filename step now produces `attachment.docx` (and similar) instead of `attachment.bin` for Office data URLs, which matches the request payload.
  - `NativeQuestions` only uses `validateBase64Content`.
  - Nothing in core, deep-learning, lightgbm, opencv or vw calls these helpers.
  - No existing test expects Office files to be rejected or named `attachment.bin`.
- [x] **Tests checked against the code.** I traced each new case in `OpenAIPromptMultimodalRequestSuite` and `OpenAIPromptOfficePathSuite` to the branch it covers:
  - HTML starting with `<html` is sent as text for both APIs.
  - `<table>` is detected as `application/xml` and `{\rtf1` as `application/json`; both are rejected.
  - File-name-map aliases produce the extension-specific error.
  - Generic ZIP/OLE and octet-stream mappings are overridden.
  - The HTTP test asserts the exact request URIs.
  - Chat and size rejections are checked, including uppercase extensions.

  Test names are unique, and the global `FileNameMap` is restored in `finally`. `Test / parallelExecution := false` is in the shared settings applied to `cognitive` (build.sbt lines 282 and 371-373). Only the ZIP/OLE mapping cases depend on lookup order, which matches the reported mutation result. I did not re-run any tests; the supplied evidence reports 129 passing.
- [x] **Docs.**
  - The edited notebook cell is valid JSON, cell IDs are unchanged and executable cells are untouched.
  - The text matches the code: content sniffing runs first and HTTP URLs are routed by the extension in their path.
  - Hadoop's `Path` percent-encodes `?` into the path, so query strings are not forwarded, as documented.
  - HTML is sent as text, RTF is reported as `application/json`, and the size limit uses MiB.
  - The example sets `setFileSizeLimitMB(10.0)`, as the text says.
  - The provider link uses the same domain as existing repo docs.
  - No other doc lists supported attachment types.
- [x] **Fixtures.**
  - The README describes synthetic Contoso/Fabrikam invoices and how they were generated.
  - `.gitattributes` (`* text=auto eol=lf`) leaves files containing NUL bytes unconverted, which covers all six fixtures.
  - Fixtures load through the `getResource(...).toURI` pattern already used in this module.
- [ ] **Fixture contents and metadata were not decoded.** As instructed, I relied on the README and the byte-exact assertions instead.
- [x] **Non-blocking observations, not counted as issues:**
  - The Chat error still suggests `apiType='responses'` for file-name aliases that Responses also rejects. This wording predates the patch and is kept by the contract; improving it would be optional.
  - A custom `FileNameMap` that gives an extensionless name an Office MIME would produce the message `for extension '.'`. This is cosmetic and cannot happen with the default JDK tables.
  - `invoice.ppt` is 661,504 bytes, about 20× the size of the PPTX. Trimming it would be optional and only affects repo and test-JAR size.

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


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
