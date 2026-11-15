---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 1
timestamp: "2026-11-15T06:51:14Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "7a78d5c380037c6bcb19f537637658e624c6ea01d6d04e630bab31fc2ba4ace8"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-8-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Production logic in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`:**
  - `OfficeMimeTypes` (lines 126-133) is set up before the reverse merge into `MimeTypeExtensions` (line 152), so no initialization-order NPE is possible.
  - `inferMimeType` (lines 293-300) runs stream sniffing and `inferStructuredTextMimeType` first. The Office extension lookup comes next, before `guessContentTypeFromName`.
  - `categorizeFileType` (line 320) accepts only an exact approved extension/MIME pair.
  - The new `responsesMessage` guard (lines 477-479) only fires when an Office MIME arrives through a non-approved path extension. Data-URL and Chat messages are unchanged.
  - The object is `private[services]`, so no public signatures change.
- [x] **Case matrix, checked by reading the code (not run):**
  - Genuine OOXML (`PK\x03\x04`) and OLE (`D0 CF 11 E0`) headers don't match the JDK stream sniffer (whose only OLE check is FlashPix) or the JSON/XML prefix check, so they fall through to extension routing.
  - `<html` becomes `text/html` and is sent as text.
  - `<table>`, `<?xml` and `{\rtf1` become `application/xml` or `application/json` and are rejected as unsupported.
  - `.docm`, `.dot`, `.zip` and `.bin` aliases are rejected with both MIME and extension in the message.
  - ZIP, OLE and macro-enabled data URLs map to `bin` and are rejected.
  - Uppercase extensions are lowercased with `Locale.ROOT`.
- [x] **Callers and downstream code:**
  - `OpenAIPrompt.scala`: `wrapFileToMessagesList` (line 631) routes data URLs and paths separately. `createMessagesUDF` (line 499) keeps failures row-local through `Try`. `attachmentSizeLimit` (line 626) applies the MiB check only when the limit is set. `validateResponsesApiCompatibility` (line 450) still blocks Responses on AI Foundry chat endpoints.
  - `services/ai/AIPrompt.scala` picks up the change through `createMessagesForRow` (line 319) and `attachmentFilename` (line 382). The only filename change (`attachment.bin` to `attachment.<ext>`) is for Office data URLs, which were rejected before.
  - `OpenAIResponses.validateInputFilePart` (lines 506-522) has no MIME allowlist.
  - There is no Python-side MIME handling.
- [x] **Tests:**
  - 37 new, uniquely named cases (15 in `OpenAIPromptMultimodalRequestSuite`, 22 in `OpenAIPromptOfficePathSuite`) assert exact payloads and messages.
  - The global `FileNameMap` is saved and restored in `finally`, and `build.sbt` line 282 sets `Test / parallelExecution := false`.
  - No existing test asserts `attachment.bin` or Office rejection.
  - The multimodal suite is 772 lines (the `FileLengthChecker` limit is 800), and no new line exceeds 120 characters.
  - The new suite is in the `services.openai` package, which the `pipeline.yaml` `openai` job covers.
- [x] **Fixtures (checked through the README and raw-byte grep; patches not decoded):**
  - The OOXML packages contain `[Content_Types].xml` and the expected main parts.
  - No `vbaProject`, `_VBA_PROJECT`, `externalLink`, `oleObject`, `file:///` or home/user-path strings were found in ASCII. UTF-16 OLE directory names were not scanned.
  - `.gitattributes` uses `text=auto`, and the fixtures appear in the diff as GIT binary patches, so line endings are not normalized.
- [x] **Quickstart vs. code:** the descriptions of sniffing precedence, HTML-as-text, RTF reported as `application/json`, declared-MIME data URLs and MiB arithmetic all match the code. The example sets `setFileSizeLimitMB(10.0)`, and the executable cells are unchanged. External provider limits (50 MB, 1,000 rows) were not independently checked.
- [ ] **Builds and tests were not run.** This is a read-only lens; findings rest on source inspection plus the supplied validation evidence.

## Issues

### Issue 1: The workaround for extensionless links suggests a local path that executors may not see
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 22
- **Description**: The new text says to "download the file to a correctly named local path." Attachments are read inside a Spark UDF (`createMessagesUDF` calls `prepareFile`, which calls Hadoop `FileSystem.open`), so the read happens on executors. On a multi-node cluster, a file saved to the driver's local disk isn't visible there. The same notebook elsewhere recommends Hadoop-compatible paths such as `abfss://`.
- **Risk**: Users who follow this advice on a cluster get `Unable to read attachment` row errors and may conclude Office support is broken.
- **Suggested Fix**: Change the wording to: "download the file to a correctly named, Spark-accessible path (for example, a Lakehouse or ADLS `abfss://` path that executors can read) or use a correctly typed data URL."

### Issue 2: No regression test covers the newly documented rules for Office files over HTTP(S)
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala; cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala
- **Line(s)**: OpenAIPromptOfficePathSuite.scala (new suite); OpenAIPromptHttpTestBase.scala 82-100
- **Description**: The quickstart now says HTTP(S) URLs need an Office extension in the URL path and that the response `Content-Type` is ignored. The new Office tests only use `file:` URIs and data URLs. The echo server only serves image, JSON and XML routes, so no test downloads an Office file over HTTP or rejects an extensionless Office link.
- **Risk**: The documented behavior could regress without CI noticing, for example if filename derivation changes or header-based typing is added later. The live-validation evidence doesn't say HTTP(S) inputs were tested.
- **Suggested Fix**: Add one focused test to `OpenAIPromptOfficePathSuite` (the multimodal suite is close to the 800-line limit). Use a local `HttpServer` with two routes serving a fixture:
  - `/files/invoice.docx` with `Content-Type: application/zip`: assert the exact `input_file` part (filename `invoice.docx`, canonical MIME).
  - An extensionless route serving the same bytes: assert `Unsupported file type: application/octet-stream.`

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

## Driver resolution

Both findings are addressed. The quickstart now recommends a correctly named
Spark-accessible path that executors can read, with ADLS as an example. The path
suite adds a local HTTP regression using genuine DOCX bytes. A `.docx` URL with
a ZIP response header produces the exact canonical Office input_file part;
an extensionless URL with the correct Office header is rejected. Query strings
also demonstrate that the path, not a query filename, determines routing.

All 129 tests in both request/path suites pass, with zero skipped. Compile,
test compile and both cognitive Scala style gates pass. Notebook JSON and
list-of-strings cell sources validate; executable cells are unchanged.
All 59 reviewer tool calls were audited: they stayed within source, test,
documentation and configuration context and did not access other feedback.

The corrected manifest SHA-256 is
`1eae5c0cf866823708a73074fac29f71eb744b9a428f079864c505d26d2332b9`;
independent binary-diff replay matched it. This issues-found pass is consumed.
A new complete six-round review is required on the corrected patch.


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
