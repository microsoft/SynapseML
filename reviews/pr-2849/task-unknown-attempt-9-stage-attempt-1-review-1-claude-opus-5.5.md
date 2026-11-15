---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 9
stage_attempt: 1
round: 1
timestamp: "2026-11-15T07:10:09Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "cd6419669e19fad541e32f05ef664f0b6a09d558a6005eeb2d23782e507e4340"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-9-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced the production flow in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`:
  - `OfficeMimeTypes` is initialized before the `MimeTypeExtensions` merge that uses it (lines 126–152), so there is no null during object initialization.
  - `inferMimeType` and `fileExtension` (293–305): content sniffing runs first, then the structured-text sniffer, then the Office-extension lookup, and only then the JVM name map.
  - The exact extension/MIME pair check is at line 320.
  - Also read `prepareFile` (382–413), `responsesDataUrlMessage` (418–440) and the new alias diagnostic in `responsesMessage` (464–486).
- [x] Checked sniffer behavior on genuine Office bytes. JDK `guessContentTypeFromStream` does not detect ZIP, and for OLE it only reports FlashPix. The UTF-8 structured-text sniffer finds no leading `{`, `[` or `<`. So genuine OLE/ZIP Office files fall through to the extension lookup.
- [x] Checked the alias cases. Detected HTML becomes `text`, and XML or JSON (including RTF) in Office-named files stays `unsupported`. Aliases on `.docm`, `.dot`, `.zip` and `.bin` fail the pair check and produce the new MIME-plus-extension message.
- [x] Checked data URLs. Canonical Office MIME types map back to the matching extension and are classified as `file`. ZIP, OLE and macro-enabled MIME types fall to `bin`, giving the unchanged `Unsupported file type: <mime>.` message. Chat still rejects through `chatCompletionsDataUrlMessage` and `chatCompletionsMessage`.
- [x] Checked callers:
  - `OpenAIPrompt.scala` 498–514: errors stay row-local through `Try` in `createMessagesUDF`.
  - `OpenAIPrompt.scala` 626–653: `wrapFileToMessagesList` routing.
  - `OpenAIPrompt.scala` 450–456: Responses with an AI Foundry model is rejected up front.
  - `ai/AIPrompt.scala` 188, 319 and 430–446: bounded reads and the `invalid_request` mapping.
  - No hand-written Python lists file types.
- [x] Checked docs claims against code:
  - `fileSizeLimitMB` has no `OpenAIPrompt` default (`OpenAIPrompt.scala` 269–278, 626–629).
  - `validateFileSize` uses MiB.
  - The notebook example sets `10.0`.
  - Notebook JSON structure is intact and only markdown cells changed.
- [x] Checked test infrastructure:
  - `build.sbt:282` sets `Test / parallelExecution := false`.
  - `URLConnection` file-name-map changes are restored in `finally`.
  - `.gitattributes` uses `* text=auto eol=lf`, and the OLE/ZIP fixtures contain NUL bytes, so Git stores them as binary (the diff shows binary patches).
  - All six fixtures and the README exist.
  - No duplicate test names.
  - The longest new lines are at most 117 characters.
- [x] Checked HTTP fetch semantics. `prepareFile` builds `new HPath(filePathStr)` (line 398) and reads it with `path.getFileSystem(conf).open(path)` (line 338). Neither the cognitive nor the core sources register a custom `fs.http(s).impl`.
- [ ] Did not run builds or tests (read-only review). I relied on the supplied validation evidence.
- [ ] Did not verify provider-side claims (50 MB limits, text-only processing of non-PDF files, 1,000 rows per sheet, the guide URL); they are outside what repository tools can check.

## Issues

### Issue 1: The HTTP regression's query strings never reach the server as queries, and prefix-matched contexts hide this
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala
- **Line(s)**: 23–34, 39–40, 46–47. Related: `OpenAIAttachmentUtils.scala` 338 and 398; quickstart notebook line 22.
- **Description**: Attachment URLs are read through Hadoop: `new HPath(filePathStr)`, then the filesystem's `open`.
  - Hadoop's `Path(String)` does not parse a query, so `?download=1` stays in the path. When the URI is rebuilt, `?` is encoded as `%3F`.
  - Hadoop's `HttpFileSystem` therefore requests `/files/invoice.docx%3Fdownload=1` with no query component.
  - The JDK `HttpServer` picks the context by prefix match on the decoded path, so the `/files/invoice.docx` and `/download` contexts still serve the bytes.
  - The test does prove that the filename comes from `java.net.URI#getPath`. It does not prove that query-bearing URLs are fetched as written, although the stated rationale ("Query strings are included in both requests") suggests it does.
- **Risk**: The new quickstart text says HTTP(S) Office URLs only need a supported path extension, and it offers alternatives only for extensionless links. Users with signed or tokenized links (for example `.../invoice.docx?sv=...&sig=...`) will get `Unable to read attachment 'invoice.docx'` with no guidance. This fetch behavior predates the patch, but the new test and docs make it look covered.
- **Suggested Fix**:
  - In the handlers, record `exchange.getRequestURI.getRawPath` and `getRawQuery`, and assert what the server actually received (or reject unexpected paths).
  - Then either fix query propagation separately, or document that query strings are not forwarded and recommend an `abfss://` path or a typed data URL for signed or query-dependent links.
  - Alternatively, remove the query strings from the test and its rationale.

### Issue 2: The macro disclosure implies only legacy formats carry macros to the provider
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 24. Related: `OpenAIAttachmentUtils.scala` 293–300 and 320; `OpenAIPromptOfficePathSuite.scala` 85–104.
- **Description**: The quickstart says bytes are forwarded "including hidden content and any macros in legacy documents" and that macro-enabled OOXML extensions are unsupported. In the code, admission depends only on the path extension, or on the declared MIME type for data URLs. OOXML package contents are never inspected. So a macro-enabled package named `.docx`/`.xlsx`/`.pptx`, or a data URL labeled with a canonical OOXML MIME type, is uploaded byte-for-byte, including its `vbaProject.bin`. The delegated-content test confirms that content is not validated.
- **Risk**: Readers applying data-handling or DLP policies may treat the `.docm`/`.xlsm`/`.pptm` rejection as a macro control and assume OOXML macro projects never reach the provider. The actual guarantee is narrower than the wording suggests.
- **Suggested Fix**: State that macro blocking is based only on the extension or MIME label. Any embedded macro project, whether in a legacy document or in a macro-enabled package named or labeled as `.docx`/`.xlsx`/`.pptx`, is forwarded unchanged and is neither executed nor stripped. Inspecting package contents would be a separate decision.

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

Issue 1 was independently reproduced by recording the actual server request
URIs and asserting the intended query-bearing URIs. The assertion failed:
Hadoop sent `%3F` in each path, not a query. The Office regression now uses
query-free URLs and checks the exact server-request URIs as well as payloads
and errors. The quickstart documents this existing reader limitation and
provides storage/data-URL alternatives. URL transport behavior is unchanged.

Issue 2 is addressed by stating that all Office bytes are forwarded unchanged,
including macros in packages named or labeled as a supported format. The docs
explicitly distinguish extension/MIME checks from macro filtering or package
inspection. No document parser or macro-processing behavior was added.

All 129 tests pass, zero skipped; cognitive compile, test compile and both style
gates pass. Notebook JSON/source arrays validate and executable cells are
unchanged. All 42 reviewer tool calls stayed within repository code, tests,
documentation and configuration; none accessed earlier feedback or checkpoints.
The corrected manifest SHA-256 is
`9a7ba8aa2c5ed7d1d3520a7d3627f9fb3135c5598260ce1f96eb4e70c7939156`;
independent binary-diff replay matched it. This pass is consumed, not a clean
signoff; a complete fresh pass is required on the corrected snapshot.


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
