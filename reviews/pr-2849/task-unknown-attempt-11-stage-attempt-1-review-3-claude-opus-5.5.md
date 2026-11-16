---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 3
timestamp: "2026-11-15T09:16:56Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "536dffd28f579b24e31bd2d83f678d186f69f5390368a9703f79cef8052271d5"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-11-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Path routing.** Traced in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`. `inferMimeType` (L293–300) still runs stream sniffing and JSON/XML sniffing before the Office map. `categorizeFileType` (L320) returns `file` only when `OfficeMimeTypes.get(extension)` equals the inferred MIME. I walked each case against the new assertions in `OpenAIPromptOfficePathSuite.scala` and `OpenAIPromptMultimodalRequestSuite.scala`, and all match:
  - `.docm`, `.dot`, `.zip` and `.bin` mapped to Office MIME types are rejected.
  - `.docx` and other Office names mapped to ZIP, OLE or octet-stream get the canonical MIME.
  - HTML goes to `text`.
  - `<table>` becomes `application/xml`, which is rejected.
  - RTF becomes `application/json`, which is rejected.
- [x] **Data URLs.** `prepareDataUrlAttachment` gets the extension from the lowercased declared MIME through the merged `MimeTypeExtensions` (L135–152). Only the six canonical MIME types reach `input_file` (named `attachment.<ext>`). ZIP, OLE and macro-enabled labels fall back to `bin` and are rejected. The original URL is forwarded unchanged, as PDF data URLs are today. Office data URLs can't reach the new Office-specific `unsupported` branch.
- [x] **Initialization order.** `OfficeMimeTypes` (L126) is defined before `MimeTypeExtensions` merges it (L152), so the merge can't see an uninitialized `val`.
- [x] **Content-first ordering is safe for genuine files.** OOXML starts with `PK\x03\x04` and OLE with `D0 CF 11 E0`. Neither matches `URLConnection.guessContentTypeFromStream`: OLE is only claimed by the FlashPix CLSID check, which returns false on malformed headers. Neither matches the `{`, `[` or `<` prefixes in `inferStructuredTextMimeType`.
- [x] **Row-local errors.**
  - OpenAIPrompt wraps `createMessagesForRow` in `Try` and rethrows only `TaskKilledException` (`OpenAIPrompt.scala` L499–513).
  - AIPrompt uses the same message builder (`services/ai/AIPrompt.scala` L319) and maps read and argument errors to `input_io_error` and `invalid_request` (L435–447).
  - The new messages expose only the MIME type and extension, never paths.
- [x] **Global test state.**
  - `OpenAIPromptOfficePathSuite` restores `URLConnection.setFileNameMap` in `finally`.
  - `build.sbt` sets `Test / parallelExecution := false` (L282) in the shared settings used by `cognitive` (L371–373).
  - `TestBase` is a plain `AnyFunSuite`, so tests within a suite don't run in parallel.
  - No production code under `core/src/main` or `cognitive/src/main` references `FileNameMap` or `content.types.user.table`.
- [x] **Size handling.**
  - Main sources set no default for `fileSizeLimitMB` (`OpenAIPrompt.scala` L269–278).
  - The notebook example sets `.setFileSizeLimitMB(10.0)` (notebook source L152).
  - Bounded reads apply only to AIPrompt (`AIPrompt.scala` L188; `OpenAIAttachmentUtils.scala` L399). OpenAIPrompt checks size after the full read (L406).
- [x] **Fixture hygiene.**
  - `.gitattributes` uses `* text=auto eol=lf`. The six fixtures contain NUL bytes and show as `GIT binary patch`, so Git won't normalize their line endings.
  - Loading resources with `new File(getClass.getResource(...).toURI)` follows an existing pattern (`services/speech/SpeechHlsFixture.scala`).
  - Temp files and the local `HttpServer` are cleaned up in `finally`.
- [x] **No stale expectations.** No other Scala or Python test under `cognitive/src/test` or `core/src/test` asserts `attachment.bin` or Office extensions or MIME types. Hand-written Python under `core/src/main` and `cognitive/src/main` has no file-type allowlist.
- [x] **Considered and not raised** (contract or pre-existing behavior):
  - HTML served at an Office-named URL, such as a login page, is routed as text.
  - Empty, corrupt or encrypted Office bytes reach the provider.
  - On Linux, `pathFilename` cuts local names at `#` or `?`.
  - The Chat error for unsupported types still suggests `apiType='responses'`.
  - Office-named HTML is decoded as UTF-8 like other text.
- [ ] **Tests not run by me.** This lens is read-only. I relied on the supplied results: 129 tests passed, compile, style and packaging gates passed, and the supplied lookup-order mutation evidence.
- [ ] **Not verifiable offline.** Provider behavior (50 MB limits, acceptance of uppercase `.DOCX` in `filename`, live extractions) and the fixture README's metadata claims. I did not decode the binary patches, as instructed.

## Issues

### Issue 1: HTTP and storage guidance omits that percent-encoded URLs and `#` fragments fail in the Hadoop reader
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 22 (Office documents cell, HTTP(S) paragraph)
- **Description**: The notebook explains that `?` gets encoded into the request path. The same mechanism also breaks URLs that are already percent-encoded:
  - `org.apache.hadoop.fs.Path(String)` treats `?` and `#` as ordinary path characters. It builds its URI with the multi-argument `java.net.URI` constructor, which always escapes `%`.
  - So `https://host/files/Q3%20Invoice.docx` is requested as `/files/Q3%2520Invoice.docx`.
  - An `abfss://…/Q3%20Invoice.docx` path names a blob whose name literally contains `%20`.
  - Meanwhile `pathFilename` (`OpenAIAttachmentUtils.scala` L171–177) decodes the URL with the single-argument `URI` constructor. Routing and the error message therefore show a believable name, `Q3 Invoice.docx`, while the read fails with `Unable to read attachment 'Q3 Invoice.docx'`.
  - The supplied validation already shows this for encoded `file:` URIs: all six formats fail with encoded URIs and pass with raw paths. The notebook doesn't mention it.
- **Risk**: Office file names often contain spaces, and URLs copied from browsers, portals or storage explorers are percent-encoded. Users who follow the notebook's own alternatives (an HTTP path with an Office extension, or an `abfss://` path) get row-level read errors that look like auth or network failures. This affects diagnosis only; no data is corrupted.
- **Suggested Fix**: Change the docs only, since transport parsing is out of scope for this patch. Next to the query-string note, add something like: "Pass paths and URLs unencoded. Literal spaces work, but percent-encoded sequences such as `%20` are encoded again, and `#` fragments are sent as part of the path." Optionally, add a local `HttpServer` case that requests `/files/Q3 Invoice.docx` with a raw space and asserts the server receives `/files/Q3%20Invoice.docx`, to back the recommendation with a test.

### Issue 2: Size guidance doesn't say that oversized Office files are buffered and encoded on executors when `fileSizeLimitMB` is unset
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb (code context: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala)
- **Line(s)**: notebook 26; `OpenAIAttachmentUtils.scala` L399, L406, L484
- **Description**: The notebook says "the service enforces both limits" and presents `fileSizeLimitMB` as optional.
  - With the limit unset (the default), an accepted Office file of any size is read in full (`readFile`), copied into a base64 `file_data` string (L484), and serialized into the request body. All of this happens in executor memory before the provider can reject the file.
  - Before this patch these formats were rejected right after the read. The extra memory use (several base64 copies, each about 4/3 of the file size) is new for Office, though it matches the existing PDF path.
  - An `OutOfMemoryError` is fatal. It skips OpenAIPrompt's `Try` and AIPrompt's catch blocks, so the task or job fails instead of the row getting an error.
  - Even with a limit, OpenAIPrompt checks size only after the full read. The limit prevents encoding and sending, not the initial read.
- **Risk**: Large workbooks and decks with embedded media can easily exceed the provider's 50 MB. A few such files in a mixed dataset can exhaust executor memory. Reading "the service enforces both limits", users may reasonably leave `fileSizeLimitMB` unset.
- **Suggested Fix**: Keep the defaults, as the contract requires; change the docs only. State that the provider's limits apply only after SynapseML has read and base64-encoded the file on executors. Recommend setting `fileSizeLimitMB` whenever inputs may include large decks or workbooks. Defaults and size arithmetic stay unchanged.

### Issue 3: The extension-aware Office error never fires for the near-miss formats the notebook names
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 293–300, 477–479
- **Description**: The new `case "unsupported" if OfficeMimeTypes.valuesIterator.contains(mimeType)` branch only fires when a name table maps an unapproved extension to one of the six canonical Office MIME types.
  - SynapseML installs no such mapping. No production code uses `FileNameMap` or `content.types.user.table`, and the tests fake one with a custom `FileNameMap`.
  - A correct MIME table never maps `.docm`, `.xlsm`, `.pptm`, `.xlsb` or `.dotx` to a canonical type. These are the near-misses the notebook calls out.
  - Those files typically get `Unsupported file type: application/octet-stream.`, or their own macro or template MIME if the runtime knows it. The message doesn't include the extension.
  - In Chat, the existing message adds `use apiType='responses' for other file inputs`, but Responses rejects these files as well.
- **Risk**: Error messages only; which files are admitted is correct. Users with macro-enabled or template files get an unhelpful error and may rename the file, which the docs warn against.
- **Suggested Fix**: Optional and non-blocking.
  - Base the extension-aware message on the path extension for a small set of Office near-miss formats, for example `docm`, `dotx`, `dotm`, `dot`, `xlsm`, `xlsb`, `xltx`, `xltm`, `xlt`, `pptm`, `potx`, `potm`, `pot`, `ppsx`, `pps`. Leave admission and all other messages unchanged.
  - Add one test for `.docm` that uses the real name table, without a custom `FileNameMap`.
  - Alternatively, note in the Resolution Log that the branch is intentionally only a safeguard against custom MIME tables.

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

All three findings are retained. They do not establish an incorrect admission
rule or require a change to the frozen patch.

1. **Existing path escaping; optional documentation expansion.** The patch does
   not change `HPath(filePathStr)`, `pathFilename`, or the transport reader. The
   recommendation is to use a correctly named Spark-accessible path, not an
   encoded browser URL. Raw local paths with spaces are covered for all six
   formats. The proposed broad assertion that raw spaces work for every HTTP
   and cloud-storage implementation was not exercised by this review and is
   not added as a new compatibility promise. Percent-encoded paths and fragment
   handling remain inherited limitations, disclosed in the PR handoff rather
   than expanding this format-admission change into a path-transport contract.
2. **Resource caveat retained; recommendation already present.** The notebook
   explicitly recommends a conservative `fileSizeLimitMB`, with `10.0` as its
   example, and the executable example sets it. It accurately identifies the
   check as optional and per-file, and the 50 MB constraints as provider limits.
   It does not claim that provider rejection protects executor memory or that
   the parameter bounds the initial read. The existing read-then-check and
   base64 serialization implementations are unchanged; supported Office inputs
   intentionally use the same materialized `input_file` contract as PDFs.
   Buffering and concurrency remain deployment considerations, not a new
   unbounded-reader implementation or a reason to alter defaults in this patch.
3. **Intentional diagnostic scope.** The extension-aware branch specifically
   explains rejection when an unapproved path extension nevertheless resolves
   to an allowed Office MIME type. This is the cross-platform/custom MIME-table
   scenario that the tests control deterministically. It is not intended as a
   complete list of all unsupported Office variants. Standard unsupported types
   retain their previous diagnostics; expanding those messages is optional.

Verification: inspected the complete production diff against the target,
the unchanged read/size/serialization paths, and the notebook's actual size
recommendation. All 39 reviewer tool calls stayed within repository source,
tests, build, and documentation; no prior review feedback was accessed.
No reviewed content changed. Original findings and verdict above are preserved.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- This is part of the six-round pre-commit review on that product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
