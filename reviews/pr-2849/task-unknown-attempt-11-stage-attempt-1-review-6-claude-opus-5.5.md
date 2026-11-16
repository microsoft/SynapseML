---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 6
timestamp: "2026-11-15T10:06:01Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "5db2af23ddd2aab721952d5371239ef9a6d79b6264df06259d36abf7a83a5c9e"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-11-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced how Office files are admitted in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`:
  - `OfficeMimeTypes` (126-133) and its reverse mapping (152).
  - The lookup order in `inferMimeType` (293-299): content sniffing, then structured text, then the Office extension, then the JVM name map.
  - The exact extension/MIME check (320) and the new diagnostic (477-479).
  - Callers: `OpenAIPrompt.scala` `wrapFileToMessagesList` (631-653); `AIPrompt` inherits the behavior (`services/ai/AIPrompt.scala:188`, `:382`).
- [x] Performance: no new cost worth noting.
  - Both maps are built once as immutable object-level values. Each attachment adds one map lookup and one `fileExtension` call.
  - `valuesIterator.contains` scans six entries and only runs on the error path.
  - The Office lookup returns before the JDK name-map lookup is reached.
  - Responses data URLs pass `originalUrl` through without decoding and re-encoding. File paths reuse the existing PDF base64 path.
  - Whole-file buffering with no default limit in legacy `OpenAIPrompt` is pre-existing and preserved by the contract (not counted).
- [x] Checked the docs' claims against the code:
  - MiB arithmetic (`limit * 1024 * 1024` at 190 and 341).
  - `.setFileSizeLimitMB(10.0)` is present in the notebook's configure cell.
  - HTML is detected as `text/html` and sent as text. `<table>` is detected as `application/xml` and `{\rtf1` as `application/json`, so both are rejected; this matches `OpenAIPromptOfficePathSuite.scala:148-156`.
  - Response Content-Type is ignored: the filename comes from `URI.getPath` (173) and the read uses `new HPath` (398).
- [x] Test hygiene:
  - `Test / parallelExecution := false` (`build.sbt:282`) makes the JVM-wide `URLConnection.setFileNameMap` changes in `OpenAIPromptOfficePathSuite.scala` (73-83, 126-138) safe.
  - The original map is restored in `finally`, the HTTP server is stopped in `finally`, and the exact request URIs are asserted (53).
- [x] Checked related docs for out-of-date format lists: `docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, `AIPrompt.ipynb` and the `columnTypes` param doc (`OpenAIPrompt.scala:241-243`) don't list attachment formats, so nothing is out of date.
  - Data-URL filename placeholders change from `attachment.bin` to `attachment.<ext>`. This only affects rows that were previously rejected, and no doc describes that mapping.
- [x] Fixtures:
  - `.gitattributes` is `* text=auto eol=lf`, and the diff shows all six files as Git binary patches, so line endings won't be rewritten.
  - Reviewed `cognitive/src/test/resources/office/README.md`. Optional, not counted: record the generator versions and commands so the fixtures can be regenerated.
- [x] Pre-existing items noted but not counted:
  - The `fileSizeLimitMB` param description says "megabytes" (`OpenAIPrompt.scala:269-271`), while the code and the new notebook text use MiB.
  - Chat's generic unsupported-type message suggests `apiType='responses'` even for types that Responses also rejects.
- [ ] Builds and tests not run: this is a read-only review. I relied on the supplied validation evidence.
- [ ] Not independently verified: the external provider guide link and limits (text-only processing for non-PDF files, 1,000 rows per sheet, 50 MB). This review had no network access.
- [ ] Binary fixture contents not decoded, per the review instructions. I relied on the README and the test assertions.

## Issues

### Issue 1: Office docs never list the accepted extension/MIME pairs
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 20, 22, 24
- **Description**: Office admission is an exact allowlist of extension/MIME pairs (`OpenAIAttachmentUtils.scala:126-133`, enforced at 320).
  - The notebook tells users to use "the correct Office MIME type" and "a correctly typed data URL". It recommends the typed data URL as the workaround for signed or extensionless links. It never states the six accepted MIME strings.
  - Common real-world labels are rejected, for example `application/vnd.ms-office`, `application/x-msexcel` and `application/excel`.
  - Some mistakes are accepted but mislabelled. For example, XLSX bytes labelled `application/vnd.ms-excel` are sent to the provider as `attachment.xls`.
- **Risk**: Users get `Unsupported file type: <mime>.` row errors and have to guess the right string, or they send mislabelled files that the provider rejects.
- **Suggested Fix**: Add a short table mapping each extension to its accepted MIME type. State that matching is case-insensitive, that other aliases are rejected, and that data URLs get the filename `attachment.<ext>`.

### Issue 2: HTTP/path guidance omits Hadoop's re-encoding of percent-escapes
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 22 (code context: `OpenAIAttachmentUtils.scala:173`, `:398`)
- **Description**: The docs explain that `?` is encoded into the request path. They don't mention that the same Hadoop `Path(String)` parsing (line 398) treats input as unencoded and encodes `%` again.
  - As a result, `My%20Invoice.docx` is requested as `My%2520Invoice.docx`.
  - The supplied evidence records this failure: encoded `file:` URIs failed while raw paths passed.
  - `pathFilename` decodes `URI.getPath` (173), so the row error reads `Unable to read attachment 'My Invoice.docx'` and gives no hint that encoding was the cause.
  - Office filenames often contain spaces, and browsers and storage explorers copy percent-encoded URLs.
- **Risk**: Users who follow "Remove optional query parameters" still hit unexplained read failures on percent-encoded HTTP(S) or `abfss://` paths.
- **Suggested Fix**: Doc-only, since transport behavior is preserved by the contract. Add a sentence saying paths and URLs are read as unencoded text, so escapes such as `%20` are encoded again. Tell users to pass the decoded path (for example, `My Invoice.docx`) or copy the file to a Spark-accessible path.

### Issue 3: Office section heading level, placement and change-relative wording
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 18-26
- **Description**:
  - `### Office documents` is an H3 placed directly under the H1 title, before `## Prerequisites`. Every other section in the notebook is H2, so the heading skips a level.
  - About 400 words of edge cases (query strings, HTML detection, macros, RTF, provider limits) come before the Quickstart's prerequisites and setup.
  - Phrases such as "existing content detection", "the existing text sniffer" and "still rejects" describe the change rather than the product, and mean nothing to someone reading the final docs.
  - The second paragraph packs nine sentences covering local paths, HTTP, query strings, signed links, HTML and data URLs.
- **Risk**: The site's table of contents nests the section under nothing. Readers have to wade through caveats before setup and can miss the key rules.
- **Suggested Fix**:
  - Change the heading to `## Office documents` and move the section after the configuration section, leaving a one-line pointer in the intro.
  - Reword the change-relative phrases, for example: "SynapseML first inspects the file's leading bytes…".
  - Split the path, HTTP and data-URL rules into bullets. No code cells need to change.

### Issue 4: New Office-alias diagnostic is ambiguous and narrowly scoped
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 477-480 (tests: `OpenAIPromptOfficePathSuite.scala:81`, `:156`)
- **Description**: The new branch produces `Unsupported file type: application/msword for extension '.dot'.`
  - It names a supported MIME type as the unsupported "file type" and doesn't list the accepted extensions.
  - A name with no extension renders as `for extension '.'`. This is only reachable through a custom name map.
  - It only covers the rare alias case. RTF or markup saved as `.doc`/`.xls`, a common legacy export, is reported as `Unsupported file type: application/json.` or `application/xml.` with no mention of the Office extension. That gap is why the notebook has to explain the `application/json` result.
- **Risk**: The error column is the user's main signal, so unclear messages lead to trial and error (renaming files, switching apiType).
- **Suggested Fix**:
  - Reword the message, for example: `Unsupported Office attachment extension '.dot' (MIME type application/msword); supported Office extensions are .doc, .docx, .xls, .xlsx, .ppt, .pptx.`
  - Drop the extension clause when the extension is empty.
  - Optional, if the contract allows changing pre-existing text: when the extension is an Office extension but content detection finds another unsupported type, add that context to the message.
  - Update the assertion at `:81` (and `:156` if the optional change is made). Keep the generic message for non-Office cases.

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

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver rebuttal

The four low-priority suggestions are retained; none requires changing the
frozen patch.

1. **MIME reference is available.** Independently checked the notebook's linked
   provider guide on 2026-11-15. Its "Full list of accepted file types" includes
   all six canonical Office MIME strings used here, and its usage section
   confirms under 50 MB per file and 50 MB combined per request. The notebook
   names this client's narrower six-extension allowlist and excludes aliases
   such as generic ZIP/OLE and macro-enabled/template labels. Repeating the
   standard MIME table locally is a useful optional convenience, not missing
   or incorrect format support. The PR description also lists the exact pairs.
2. **Duplicate inherited path-reader limitation.** This is the same optional
   documentation expansion recorded in round 3. The reader and URI parsing are
   unchanged. Raw paths with spaces are tested, and the PR handoff explicitly
   discloses percent-encoding limitations. No new transport guarantee is made.
3. **Editorial preference, not broken content.** The H1-to-H3 introduction
   pattern also exists in the neighboring `AIPrompt.ipynb`; Markdown does not
   require an H2 for this content to render. The paragraph grouping and words
   "existing"/"still" can be edited in a documentation-focused change, but
   the rules remain understandable and accurate. No failed rendering, link,
   or executable example was demonstrated.
4. **Diagnostic deliberately identifies the rejected combination.** The message
   says which MIME was rejected *for the supplied extension*, avoiding the
   misleading MIME-only failure this branch replaces. It does not claim that
   the MIME is rejected for every extension. The empty-extension custom-map
   example is an optional presentation improvement, not incorrect admission.
   General RTF/XML and Chat diagnostics predate this change and are intentionally
   preserved. Their limitations are documented rather than expanded here.

All 32 reviewer calls were read-only and limited to repository source, tests,
build, and docs; no earlier feedback was accessed. The provider verification was
performed by the driver after this independent review. Original findings and
verdict above are preserved.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- This is part of the six-round pre-commit review on that product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
