---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 3
timestamp: "2026-11-15T08:02:09Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bf4de5ed521c68dd7e75cdcae3abf2c9f4b8ee1769e5527e2f01443146870f74"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-10-stage-attempt-1-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` and traced the `inferMimeType` order (L293-300): stream sniff, then structured-text sniff, then the Office extension, then the name registry. The Office step returns only the MIME paired with its own extension, so `categorizeFileType` (L320) admits exact pairs only. For data URLs, MIME-to-extension resolution goes through `MimeTypeExtensions`. Aliases, ZIP, OLE, macro-enabled and template MIMEs resolve to `bin` and end up `unsupported`.
- [x] Checked initialization order and thread safety. `OfficeMimeTypes` (L126) is defined before `MimeTypeExtensions` (L135), so there is no forward-reference null. Both maps are immutable, and the new helpers cannot throw.
- [x] The Office-specific diagnostic (L477-479) is reachable only when a name registry maps an extension outside the allowlist to a canonical Office MIME. `prepareFile` lowercases `mimeType` (L409) before that comparison.
- [x] Traced the callers: `OpenAIPrompt.wrapFileToMessagesList` (`OpenAIPrompt.scala` L630) and `AIPrompt` through `createMessagesForRow` (`AIPrompt.scala` L319). New failures are `IllegalArgumentException`. They stay row-local through the `createMessagesUDF` `Try` (`OpenAIPrompt.scala` L499-514) and AIPrompt's `invalid_request` mapping (`AIPrompt.scala` L436-446).
- [x] The new negative Office cases (Chat, size, aliases, markup, extensionless HTTP) assert through `createMessagesForRow`, not `transform`. Generic transformer-level row-local capture is already covered by "invalid and null data image URLs stay row-local and skip HTTP" (`OpenAIPromptMultimodalRequestSuite.scala` L331). The two compose soundly, so I did not raise this as a defect. However, the validation summary's "public-transformer ... Chat/size rejection" wording describes helper-level checks for those cases.
- [x] Checked downstream gates. `OpenAIResponses.validateInputFilePart` (L506-522) has no MIME allowlist, and `cognitive/src/main` has no other PDF-only gate. I found no duplicated MIME logic in the Python sources.
- [x] Checked test isolation. Every `URLConnection.setFileNameMap` mutation is restored in `finally` (`OpenAIPromptOfficePathSuite.scala` L70-84, L124-139). `build.sbt` sets `Test / parallelExecution := false` (L282). `TestBase` does not mix in `ParallelTestExecution`. Only `OpenAIAttachmentUtils` calls `guessContentTypeFromName` in the core, cognitive, deep-learning and opencv main sources.
- [x] Checked the fixtures. `.gitattributes` uses `* text=auto eol=lf`, and the diff shows `GIT binary patch`, so EOL normalization will not alter the fixtures. Loading them with `new File(getResource(...).toURI)` follows an existing convention (`SpeechHlsFixture.scala` L35). I inspected the fixture README and test assertions, not the binary contents.
- [x] Checked the docs. The notebook's JSON cell structure is valid. `fileSizeLimitMB` has no default (`OpenAIPrompt.scala` L269-278, and no `setDefault` under `services/ai`), and the example sets `10.0` (notebook L152).
- [x] Considered these cases and did not raise them, because they are pre-existing or explicitly kept by the contract:
  - A `200` HTML error page served at an Office-named URL is sent as text, as already happens for PDFs.
  - Hadoop `Path` percent-quotes `%`, so percent-encoded HTTP paths fail to read (transport behavior, out of scope).
  - Zero-byte, mislabelled or encrypted Office bytes are forwarded to the provider.
  - Data-URL `file_data` is forwarded verbatim, including MIME casing and parameters.
  - There is no default size limit.
- [x] I ran no tests or builds; this is a static review, and the supplied validation evidence was not re-run.

## Issues

### Issue 1: Office routing filename is cut at `#`/`?` for non-HTTP paths, so validly named documents are rejected
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 171-177 (`pathFilename`), 396-400 and 408-411 (`prepareFile`), 293-305 (`inferMimeType`, `fileExtension`); also line 22 of `docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb`
- **Description**: Path admission for Office files depends only on the extension that `attachmentFilename` → `pathFilename` derives.
  - **Mismatch**: `pathFilename` parses the path with `java.net.URI`, which treats `#` as a fragment and `?` as a query. The file itself is read with `new HPath(filePathStr)` (L398), and Hadoop's `Path(String)` does not treat `#` or `?` as delimiters.
  - **Example**: For URI-legal, non-HTTP paths such as `/lakehouse/default/Files/Invoice#1042.docx` or `abfss://c@acct.dfs.core.windows.net/inv/PO#77.xlsx`, the full file is read, but the routing name becomes `Invoice` or `PO`, with no extension.
  - **Result**: The JDK stream sniffer does not recognize ZIP or OLE bytes. `inferStructuredTextMimeType` returns `None`, and the name registry has nothing for an extensionless name. The row therefore fails with `Unsupported file type: application/octet-stream.`
  - **Inconsistency**: A path that also contains a space escapes the problem, because `new URI` throws and the fallback uses `HPath.getName`.
  - **Origin**: This is pre-existing behavior in `pathFilename`, and it affects PDFs and CSV files the same way; this diff did not introduce it. Office routing inherits it, though, and the new docs advise users to download to "a correctly named, Spark-accessible path" — guidance that fails for such names. `#` is common in invoice and PO filenames.
- **Risk**: Valid Office documents get row-local false rejections with an unhelpful octet-stream message. The impact is limited to those rows: there is no task failure and no data exposure.
- **Suggested Fix**: Choose one of these:
  - **(a)** For non-`http(s)` schemes, including scheme-less paths, derive the routing name with `new HPath(path).getName` so it matches what Hadoop reads. Keep `java.net.URI` parsing for HTTP(S), where it strips queries and keeps the sanitized filename that `OpenAIPromptMultimodalRequestSuite` L369-381 expects. Add a regression test with a raw local path containing `#`.
  - **(b)** Defer the code change, and add one sentence to the notebook saying that `#` and `?` in path file names are not supported for type routing.

  Option (a) also changes `filename` and template values for PDF and text paths, so a follow-up PR may be preferable.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver rebuttal

The filename limitation is real, but it is an inherited path-reader restriction,
not a defect introduced by the Office MIME-admission change. It is rejected as
a mandatory fix for this patch, not represented as corrected.

The driver compared `pathFilename` with target
`45578f08e6bacf8475de0e92f1717e761d40f795`: the complete function is identical.
The same URI parsing precedes filename classification for existing text/PDF
inputs. Neither the file-path read nor this name derivation changes here.
The reviewer likewise explicitly identifies the origin as pre-existing.

Changing name derivation across non-HTTP schemes would change filename/template
behavior for existing attachment types and needs a separately scoped path-reader
fix. This patch's contract preserves existing path handling while adding six
format/MIME pairs; it does not claim support for every possible filename.
The PR description explicitly retains this limitation and recommends simple
filenames or typed data URLs. No user-visible guarantee is being silently removed.

Validation scope clarification: positive Office serialization is tested through
`transform` for all six formats. Office-specific Chat/size rejection assertions
use `createMessagesForRow`; existing generic transformer tests cover row-local
error capture. These are complementary checks, not six live negative API tests.

All 48 reviewer tool calls stayed within source, test, documentation and
configuration context; none accessed prior/sibling feedback or checkpoints.
No reviewed content changed. The original ISSUES_FOUND verdict and the known
limitation are preserved; driver triage does not treat the unrelated path-reader
repair as an Office-format merge prerequisite.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
