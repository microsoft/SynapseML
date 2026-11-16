---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 2
timestamp: "2026-11-15T08:56:26Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1f0c340d15fbe4a18d12de389ae847c80ca9bbb2fe0abdf88360ec0629f095d5"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-11-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Production design.** I read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`. One private allowlist, `OfficeMimeTypes` (L126-133), drives four things:
  - path inference, which runs after content sniffing and before the JVM name map (L293-300);
  - the exact extension/MIME pair rule next to PDF in `categorizeFileType` (L320);
  - data-URL filenames, through the derived reverse map (L152);
  - the alias diagnostic, which only path inputs reach (L477-479).

  `fileExtension` (L302-305) replaces the inline copy in `prepareFile`. Every new member is private inside the `private[services]` object (L22). Public JVM signatures, params, persisted shapes and generated wrappers are therefore unchanged.
- [x] **Consistency across call paths.** `OpenAIPrompt.wrapFileToMessagesList` (`OpenAIPrompt.scala` L631-656) sends data URLs and paths to the matching Responses or Chat builders. `AIPrompt.generatedMessages` (`ai/AIPrompt.scala` L314-323) reuses `createMessagesForRow` for `GenerativeApis = {chat_completions, responses}` (`ai/AIRequestAdapter.scala` L22). Both facades get the same admission rules and Chat restrictions, and the adapter needs no change. Office `input_file` parts use the same keys as existing PDF parts.
- [x] **Row-local errors.** `createMessagesUDF` (`OpenAIPrompt.scala` L499-515) wraps `createMessagesForRow` in `Try` and writes any failure other than a task kill to the error column. Office rejections are plain `IllegalArgumentException`s on that path.
- [x] **JVM-global test state.** `build.sbt` L282 sets `Test / parallelExecution := false`. Both `URLConnection.setFileNameMap` overrides are restored in `finally` (`OpenAIPromptOfficePathSuite.scala` L82-84 and L137-139). This confirms the claim that module tests run serially.
- [x] **Conventions.**
  - License headers are present and the new suite extends `TestBase`.
  - Resource loading through `new File(getClass.getResource(...).toURI)` matches `services/speech/SpeechHlsFixture.scala` L35.
  - Checked-in binary fixtures follow the existing audio resources in `cognitive/src/test/resources`.
  - The scalastyle limits are 800 lines and 120 characters (`scalastyle-test-config.xml` L3-7). `OpenAIPromptMultimodalRequestSuite.scala` is now 773 lines.
- [x] **Docs.**
  - Only markdown cells in the quickstart notebook changed, and the example still calls `setFileSizeLimitMB(10.0)` (notebook L152).
  - The documented behavior matches the code: sniffing takes precedence, routing uses the URL path extension, response Content-Type is ignored, data URLs trust their declared MIME, and the size limit is unset by default.
  - Putting `### Office documents` under the H1 intro follows `AIPrompt.ipynb`, which has an H3 at L25 before its first H2 at L47.
  - `Unified AI prompting.md` and `AIPrompt.ipynb` don't list attachment types, so they need no update. The frozen `website/versioned_docs` snapshots are correctly left unchanged.
- [x] **Regression scan.** No other test or Python source under `cognitive/src/test` or `cognitive/src/main/python` checks Office MIME types or the generic `Unsupported file type` wording.
- [x] **Pre-existing behavior, not counted.** Three behaviors are either out of scope or explicitly preserved by the contract:
  - Chat's generic advice to "use apiType='responses'" for any unsupported type, including Office-MIME aliases such as `.dot`;
  - RTF being sniffed as `application/json`;
  - Hadoop encoding `?` into the request path.
- [ ] **Tests executed:** not applicable. This lens is read-only, so I relied on the supplied validation evidence (129 passing tests, compile and style gates).

## Issues

### Issue 1: Office test fixtures and global MIME-map overrides don't use the package's shared-fixture pattern
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala`
- **Line(s)**: OfficePathSuite 18-57, 68-84, 111-139; MultimodalRequestSuite 44, 191-200
- **Description**: Office test coverage is split across two suites, and the setup code is duplicated:
  - The six canonical extension/MIME pairs are written out twice (MultimodalRequestSuite L191-198 and OfficePathSuite L111-118).
  - The resource-loading expression appears four times (MultimodalRequestSuite L200; OfficePathSuite L19, L68, L122).
  - The process-global `URLConnection.setFileNameMap` capture, override and restore is written inline twice (OfficePathSuite L70-84 and L124-139).
  - The HTTP test starts its own `HttpServer` (L18-57) instead of extending `OpenAIPromptHttpTestBase.withEchoServer`.

  This package already keeps shared test tables in a `private[openai]` fixture object (`AttachmentReadFailures`, `AttachmentReadFailureFileSystem.scala` L16). It scopes shared or global state through loan helpers (`withAttachmentFailureFileSystem` at MultimodalRequestSuite L31-42, and `withEchoServer`). The transformer-level Office cases live in the multimodal suite, where `requestPayload` (L44) is private. That brings the file to 773 of its 800-line scalastyle limit.
- **Risk**: This affects maintainability only; behavior and coverage are correct.
  - Adding or changing a format means editing two suites in step.
  - Today, only `Test / parallelExecution := false` keeps these global overrides from affecting other suites. A future copy of the inline override that sets the map before its `try`, or forgets to restore it, would leak into later suites.
  - The next test added to the multimodal suite will probably hit the file-length gate and force an unplanned split.
- **Suggested Fix**: This is a test-only refactor with no production change.
  1. Add a test-scope `private[openai] object OfficeAttachmentFixtures` containing:
     - the six pairs, kept independent of production's `OfficeMimeTypes` so assertions don't just repeat the code under test;
     - a `bytes(extension)` loader;
     - a `withFileNameMap(extension, mimeType)(body)` helper that owns capture and restore and documents the serial-execution dependency.
  2. In `OpenAIPromptHttpTestBase`, add either `requestPayload` or an extra-attachment-context parameter for `withEchoServer`.
  3. Move all the Office cases into one suite.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver rebuttal

Issue 1 is retained as a low-priority maintainability suggestion, not a defect
requiring changes to this patch. No reviewed content changed.

- The independent six-format expectation tables are intentional test data.
  Their small duplication does not create inconsistent behavior; all six
  mappings are asserted through both public-transformer and path-level tests.
- Both global MIME-map mutations occur inside `try` and restore the original
  map in `finally`. `build.sbt` explicitly disables parallel test execution.
  A possible future test omitting restoration is not a current failure.
- `OpenAIPromptHttpTestBase.withEchoServer` has fixed attachment routes and
  records `getPath`, whereas this regression needs configurable conflicting
  headers and exact `getRequestURI` capture. Extending a shared helper and moving
  existing transformer tests would increase this format-admission patch's
  scope without improving its current correctness.
- The multimodal suite remains below the enforced 800-line limit. Its 773
  lines are not a current style violation; restructuring for hypothetical
  future tests is deferred.

Verification: inspected both affected suites, the HTTP base helper, and
`build.sbt` at the frozen patch. Existing evidence remains 129 passing tests,
zero skips, and passing compile/test-compile/style gates. All 50 reviewer tool
calls were read-only and remained within repository source, test, build,
documentation, and guidance paths; none accessed prior review feedback.
Original reviewer findings and verdict above are preserved.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- This is part of the six-round pre-commit review on that product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
