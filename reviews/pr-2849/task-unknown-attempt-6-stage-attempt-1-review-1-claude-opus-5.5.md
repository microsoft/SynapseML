---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 1
timestamp: "2026-11-15T05:45:13Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "53490290805459c76afd051d119ff5c0502f4f1661806f880ab38464b61ba4ad"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-6-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` in full.
  - The lookup order (lines 293–299) is: content sniff → structured-text sniff → `OfficeMimeTypes` by extension → `guessContentTypeFromName` → `application/octet-stream`.
  - Line 320 marks an attachment as `file` only when the extension and MIME type are an exact approved pair.
  - `OfficeMimeTypes` (line 126) is declared before `MimeTypeExtensions` (line 135), so building the reverse map with `++` cannot hit an uninitialized value.
- [x] Traced data URLs. `prepareDataUrlAttachment` (lines 219–237) gets the extension from the declared MIME type.
  - The six Office MIME types become `file` with the filename `attachment.<ext>`.
  - `application/zip`, `application/x-ole-storage` and the macro-enabled MIME types map to `bin`, become `unsupported`, and keep the existing message.
  - The new alias error branch (line 477) can only be reached from name-based guesses for file paths.
- [x] Traced file paths.
  - `fileExtension` lowercases with `Locale.ROOT`, so uppercase extensions work.
  - Real OOXML files start with `PK` and real OLE2 files with `D0 CF`, so neither content check ever reads them as HTML, JSON or XML.
  - With an Office extension, only content detected as HTML becomes text; JSON, XML and images stay `unsupported`.
  - Text, image, PDF and extensionless paths behave as before, because the Office lookup returns `None` for them.
- [x] Checked that errors stay per-row. `OpenAIPrompt.createMessagesUDF` (`OpenAIPrompt.scala` about lines 497–514) wraps `createMessagesForRow` in `Try` and writes failures to the error column. The size checks and both Chat Completions message builders are unchanged.
- [x] `OpenAIResponses.validateInputFilePart` (`OpenAIResponses.scala` lines 506–521) has no MIME allow-list, so Office `input_file` parts pass client-side validation.
- [x] Checked test isolation for the global `URLConnection` file-name map.
  - `Test / parallelExecution := false` is in the shared `settings` (`build.sbt` line 282), which `cognitive` uses (lines 371–373).
  - `OpenAIPromptOfficePathSuite` restores the map in `finally`.
  - No other production code under core or cognitive calls `guessContentTypeFrom*`.
- [x] Looked for regressions and inconsistencies.
  - No existing tests reference `attachment.bin` or Office MIME types.
  - There is no hand-written Python MIME classifier (searched `*.py` under the core and cognitive sources).
  - The `columnTypes` parameter description and the other docs do not list attachment kinds.
  - The new test names are unique.
- [x] Checked the notebook.
  - Its JSON structure is intact, and only markdown cells changed.
  - The example uses `.setFileSizeLimitMB(10.0)`, as the new text says.
  - The MiB claim matches `limit * 1024 * 1024`.
  - "Applies only when set" is true for `OpenAIPrompt` (`useBoundedAttachments = false`); `AIPrompt` overrides this.
- [x] Git treats all six fixtures as binary (the diff shows them as binary patches), so the `.gitattributes` rule `* text=auto eol=lf` does not rewrite them. `office/README.md` documents that the fixtures are synthetic.
- [x] Office-alias paths in Chat Completions mode get the existing generic Chat rejection, which does not name the extension. I accepted this as consistent with "preserve existing Chat restrictions" and "other rejection messages stay unchanged", so it is not raised as an issue.
- [ ] Not checked: the binary fixture contents (metadata, absence of macros). Per instructions I did not decode them and relied on `office/README.md` and the test assertions.
- [ ] Not run by this reviewer: tests, builds and live extractions. The 115-pass and live-run claims come from the supplied evidence. I could not verify the provider limits or the external guide URL offline.

## Issues

### Issue 1: No regression test checks that Office routing beats generic ZIP/OLE MIME types from file-name lookup
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala (ordering under test in cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala)
- **Line(s)**: OpenAIPromptOfficePathSuite.scala 16–44; OpenAIAttachmentUtils.scala 293–299
- **Description**: The requirement is to keep Office MIME types correct "despite platform ZIP/OLE/octet-stream name-based MIME inference", and the comment at line 296 says the same.
  - Every file-name-map override in the patch maps an unapproved extension (`.docm`, `.xlsm`, `.pptm`, `.dot`, `.zip`, `.bin`) to an Office MIME type.
  - All tests with approved extensions (the six fixture tests, the HTML tests and the delegated-content tests) use the default JVM table. That table does not map `.doc`, `.docx`, `.xls`, `.xlsx`, `.ppt` or `.pptx` to `application/zip` or an OLE type.
  - If `OfficeMimeTypes.get(fileExtension(fileName))` were moved after `guessContentTypeFromName` (line 298), every test in the patch would still pass.
- **Risk**: A later refactor could silently break the exact platform behavior this change fixes, with CI staying green. For example, `.docx` on a platform that maps it to `application/zip` would fail with "Unsupported file type: application/zip."
- **Suggested Fix**: Reuse the override-and-restore pattern already in `OpenAIPromptOfficePathSuite`.
  - Map approved extensions to generic types: `.docx`, `.xlsx` and `.pptx` to `application/zip`; `.doc`, `.xls` and `.ppt` to `application/x-ole-storage`.
  - Using the real fixtures, assert that the Responses part is `input_file` with the original filename and `data:<canonical Office MIME>;base64,<unchanged bytes>`.

### Issue 2: The quickstart implies data URLs are content-checked before Office routing
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb (behavior in cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala)
- **Line(s)**: notebook 22; OpenAIAttachmentUtils.scala 219–237, 418–440
- **Description**: The notebook says "Office routing uses the file extension or declared data-URL MIME type when content detection does not identify another type." Data URLs are never content-checked.
  - `prepareDataUrlAttachment` classifies them from the declared MIME type alone (line 231, then `categorizeFileType`).
  - `responsesDataUrlMessage` forwards the original URL as `file_data`.
  - So a data URL labeled with any of the six Office MIME types is uploaded as `input_file`, even if its bytes are HTML, CSV or JSON.
  - The "HTML is sent as text" rule in the first paragraph applies only to file paths.
- **Risk**: Readers may assume mislabeled data URLs are caught on the client. A realistic case: Windows machines with Excel installed often label `.csv` files as `application/vnd.ms-excel`.
  - Before this patch, data URLs built from those labels were rejected on the client.
  - Now they are uploaded as XLS and left to the provider to accept or reject. That costs a provider request per row and can produce provider errors or wrong-format handling.
- **Suggested Fix**: Split the sentence into two rules:
  - For file paths, Office routing uses the extension when content detection finds no other type. Content detected as HTML is sent as text; other detected types are rejected.
  - Data URLs are routed by their declared MIME type with no content detection, so the label must match the bytes.

### Issue 3: RTF saved with a `.doc` extension is rejected as `application/json`
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala (user-facing claim in the quickstart notebook)
- **Line(s)**: OpenAIAttachmentUtils.scala 276–282, 294–297, 320; notebook 20
- **Description**: `inferStructuredTextMimeType` labels any content whose first non-whitespace character is `{` as `application/json`.
  - RTF files start with `{\rtf1`. Saving RTF under a `.doc` name is a long-standing practice, since Word opens such files without complaint. It is the counterpart of the HTML-as-`.doc` case this patch handles and tests.
  - Content checks run before the new Office lookup, so such a file is classed `unsupported` at line 320.
  - Responses rows fail with "Unsupported file type: application/json."; Chat rows put the JSON MIME type in the generic message.
- **Risk**: Now that `.doc` is advertised as supported (notebook line 20), users get an error naming JSON for a Word-named file. During bulk extraction, that hides the real cause when sorting out failed rows.
- **Suggested Fix**: The spec keeps existing content-check precedence and leaves other rejection messages unchanged, so the compliant fix is a quickstart note: RTF saved with a `.doc` extension is not a Word binary, is rejected, and may be reported as `application/json`; convert it to `.docx` or PDF.
  - If the spec owner accepts a message change, detect a leading `{\rtf` only for Office-named paths and report `application/rtf`.
  - Keep the current JSON/text behavior for extensionless and `.json` inputs, and add a regression test.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

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

## Driver dispositions after round 1

The original review above is preserved. All three findings are resolved:

- Issue 1: Added twelve real-fixture request-builder cases covering each approved
  extension with both its generic ZIP/OLE mapping and an octet-stream mapping.
  A controlled lookup-order mutation made all six ZIP/OLE cases fail while the
  six octet-stream controls passed. Restoring the original production ordering
  made all twelve pass, proving the new coverage detects that regression.
- Issue 2: The quickstart now separates path content detection from data-URL
  classification using only the declared MIME type. No routing logic changed.
- Issue 3: Documented that RTF renamed to `.doc` is not binary Word and remains
  unsupported, including the existing JSON diagnostic and conversion advice.
  A public request-builder regression verifies that behavior without modifying
  the existing text classifier.

Verification: 128 tests passed, zero skipped; compile, test compile, both Scala
style checks, and core/cognitive packaging passed. Production source was restored
byte-for-byte after the controlled mutation. All 31 reviewer tool calls stayed
within source/documentation scope and did not read earlier feedback. This pass
is consumed by the test/documentation changes, not a clean pre-commit signoff.


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
