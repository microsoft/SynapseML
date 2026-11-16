---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 4
timestamp: "2026-11-15T08:21:39Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "7da0e66e1f921ce8205988b0fb0892bdaef2547b8c0df6b87a93f57527d1b5ae"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-10-stage-attempt-1-review-4-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced `inferMimeType` in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` (L293-300). The lookup order is: JDK content sniff, then `inferStructuredTextMimeType`, then the Office extension (L297), then the JVM name map. Genuine OLE headers (`D0 CF 11 E0 …`) match neither sniffer; the JDK only returns a type for OLE when it finds a FlashPix CLSID. Genuine ZIP headers (`PK\x03\x04`) also match neither. So sniffer precedence can't misroute real Office bytes. HTML (`<html`, `<!`) still wins, and XML, JSON and RTF-like text fall to `unsupported`.
- [x] Confirmed that admission requires an exact extension/MIME pair in `categorizeFileType` (L320). A path can only get an Office MIME from its own extension (L297) or from the JVM name map. Aliases such as `.dot`, `.docm`, `.zip` and `.bin` therefore end up `unsupported`. Data URLs derive `extension` from the declared MIME through the reversed `MimeTypeExtensions` (L135-152, L231-235), so only the six canonical MIME types are admitted. `fileExtension` (L302-305) is identical to the old inline logic (lowercased with `Locale.ROOT`).
- [x] Checked object initialization order. `OfficeMimeTypes` (L126) is defined before `MimeTypeExtensions` (L135), which uses it at L152, so it is never null during `object` init. The guarded `case "unsupported"` (L477) comes before the generic case (L480). `prepareFile` lowercases `mimeType` (L409) before any comparison.
- [x] Confirmed that existing behavior is preserved:
  - The Chat helpers still reject every type other than text and image.
  - Size checks run before categorization (`prepareFile` L406, `prepareDataUrlAttachment` L229).
  - `createMessagesUDF` wraps `createMessagesForRow` in `Try` (`OpenAIPrompt.scala` L499-512), and `AIPrompt` turns `IllegalArgumentException` into a row-level `invalid_request`.
  - `validateInputFilePart` (`OpenAIResponses.scala` L506-522) has no MIME allowlist.
  - `AIRequestAdapter.GenerativeApis` contains exactly the two API types matched in `wrapFileToMessagesList`.
- [x] Checked knock-on effects. Office data URLs now render as `attachment.<ext>` instead of `attachment.bin` in prompt-template filename substitution (`OpenAIPrompt.extractFilename`, `AIPrompt.preparePrompt`). This matches the `input_file` filename, and no existing test asserted `attachment.bin`.
- [x] Checked test isolation. `URLConnection.setFileNameMap` is restored in `finally`. `build.sbt` L282 sets `Test / parallelExecution := false`. Across the core, cognitive, deep-learning and opencv sources, only `OpenAIAttachmentUtils` reads the JVM name map, so other modules can't be affected.
- [x] Checked docs and fixtures:
  - The notebook example calls `.setFileSizeLimitMB(10.0)`.
  - `validateFileSize` uses MiB.
  - `attachmentSizeLimit` (`OpenAIPrompt.scala` L626-627) only applies to `OpenAIPrompt` when the limit is set.
  - With `.gitattributes` set to `* text=auto eol=lf`, all six fixtures are classified as binary (each appears as a GIT binary patch), so line endings won't be normalized.
- [x] Considered but not raised as defects:
  - For Office-alias paths, Chat still shows its generic "use apiType='responses'" hint. This is existing behavior that the contract keeps.
  - The new Office diagnostic could show `'.'` as the extension only if a custom name map returned an Office MIME for a file with no extension. The default JDK tables can't produce that.
- [ ] I did not run any tests, builds or live calls; this is a read-only review that relies on the supplied validation evidence. I did not decode the binary fixtures; as instructed, I checked `cognitive/src/test/resources/office/README.md` and the test assertions instead.

## Issues

### Issue 1: The notebook says only "query-dependent" links fail, but any URL with a query string fails
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 22
- **Description**: The new text says the Hadoop-based reader "does not forward URL query strings, so do not use query-dependent links such as signed download URLs." The code handles the URL in two different ways:
  - **Routing and filename:** these come from `pathFilename` (`OpenAIAttachmentUtils.scala` L171-177), which uses `java.net.URI#getPath` and drops the query. For example, `…/invoice.docx?v=2` becomes `invoice.docx` and is routed as Office.
  - **Reading the bytes:** this uses `new HPath(filePathStr)` (L398). Hadoop's `Path(String)` treats everything after the authority as an unescaped path, so `?v=2` is percent-encoded into the path. The GET then targets `/…/invoice.docx%3Fv=2`, which is the behavior the supplied validation reproduced.

  So every URL that contains a query string requests a different resource, not just links whose content depends on the query. The user then sees a row-level `Unable to read attachment 'invoice.docx'` that names a valid-looking file. The transport behavior is pre-existing and the contract keeps it; this finding is only about the accuracy of the new sentence.
- **Risk**: Users with optional parameters (cache busters, `?raw=true`, tracking parameters) will reasonably read "does not forward" as "the query is dropped and the link still works", and then get confusing read errors. Failures stay row-local, and no data is corrupted.
- **Suggested Fix**: Describe the actual behavior, for example: "HTTP(S) URLs that contain a query string are not supported. The Hadoop-based reader encodes `?` into the request path instead of sending a query, so even optional parameters change the requested resource. Remove optional parameters. For signed, query-dependent or extensionless links, download the file to a correctly named, Spark-accessible path, or use a correctly typed data URL." No code change is needed.

### Issue 2: New path tests pass percent-encoded `file:` URIs, but Hadoop expects unescaped path strings
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala; cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala
- **Line(s)**: OpenAIPromptOfficePathSuite.scala 79, 99, 131, 154; OpenAIPromptMultimodalRequestSuite.scala 206, 219, 244
- **Description**: These tests pass `tempFile.toUri.toString` or `fixture.toUri.toString`. `Path.toUri` percent-encodes spaces, non-ASCII characters and `%`.
  - Production reads the value with `new HPath(filePathStr)` (`OpenAIAttachmentUtils.scala` L398).
  - Hadoop documents `Path(String)` as accepting URIs "with unescaped elements" and rebuilds the path with the multi-argument `java.net.URI` constructor, which always escapes `%`.
  - A directory such as `First Last` therefore becomes `First%2520Last`. `LocalFileSystem` then looks for a literal `First%20Last` directory, and `readAttachment` throws `AttachmentReadException`.

  This affects the 12 container-mapping tests that use `fixture.toUri` whenever the checkout path contains such characters, and the temp-file tests whenever `java.io.tmpdir` does. They then fail with read errors that have nothing to do with Office routing. The existing attachment tests in these suites pass raw paths (`tempFile.toString` at `OpenAIPromptMultimodalRequestSuite.scala` L178 and L284, and `OpenAIPromptSuite.scala` L66), which matches Hadoop's contract. Typical CI workspace paths don't contain these characters, which fits the green validation. This is a test-only problem; production is not affected.
- **Risk**: On developer machines, for example Windows profiles or checkout directories with spaces or non-ASCII names, tests fail depending on the environment. These failures look like Office-routing regressions and can hide real failures.
- **Suggested Fix**: Follow the existing convention and pass raw local paths (`tempFile.toString`, `fixture.toString`); the expected filenames (`getFileName.toString`) and error messages stay the same. If a `file:` scheme case is specifically wanted, keep one such case and build it unescaped, for example with `new org.apache.hadoop.fs.Path(tempFile.toUri).toString`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver resolution

Both findings are addressed. The quickstart now explicitly says query-bearing
HTTP(S) URLs are unsupported because Hadoop encodes `?` into the path, and
recommends removing optional parameters or using storage/data-URL alternatives.
This changes documentation, not URL transport.

For the test-portability finding, the driver first added spaces to the temporary
Office filenames without changing their encoded URI inputs. All six Office
format cases failed with attachment-read errors; the existing data-file control
passed. Every new local Office path now uses raw `Path.toString`, following the
existing tests and Hadoop's contract. The spaces remain in the six format cases
so reintroducing encoded URI inputs is caught by normal CI.

All 129 tests pass, zero skipped, along with cognitive compile, test compile and
both style gates. Notebook JSON/source arrays validate; executable cells are
unchanged. All 49 reviewer tool calls stayed within source, tests, documentation
and configuration without reading prior/sibling feedback or checkpoints.

The corrected manifest SHA-256 is
`02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`;
independent binary-diff replay matched it. This pass is consumed by the test and
documentation correction. Earlier rounds are preserved but cannot be combined
with a new patch's review results.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
