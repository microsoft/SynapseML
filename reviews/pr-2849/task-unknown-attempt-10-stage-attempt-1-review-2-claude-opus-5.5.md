---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 2
timestamp: "2026-11-15T07:43:38Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "7c941c799ca835e6125096c8f80a6ad5f36bff4f447c986400a8409cc8b5c288"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-10-stage-attempt-1-review-2-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the production change in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` (lines 126-133, 152, 293-305, 320, 382-413 and 418-500). I traced how each input type is handled:
  - Office binary and OOXML fixtures don't match the stream or structured sniffers, so they reach the Office table.
  - HTML is still sent as text.
  - `{\rtf…` is detected as `application/json` and `<table>` as `application/xml`; both stay unsupported.
  - Data URLs get their extension from the declared MIME. The pair rule therefore admits exactly the six canonical Office MIME types and rejects ZIP, OLE and macro-enabled labels.
- [x] Traced everything that calls into this code:
  - `OpenAIPrompt.scala` lines 312-319 and 631-653.
  - `ai/AIPrompt.scala` lines 170, 188 and 380-384. It inherits the same path, with bounded reads.
  - `OpenAIResponses.scala` lines 506-522. It puts no MIME restriction on `input_file`.
  - `ai/NativeQuestions.scala`. It only handles `data:image/` and base64 validation.

  There is no public param, signature or codegen change. A grep of `core/src/main/scala` found no shared MIME registry that the new table duplicates.
- [x] Checked that the tests' changes to the JVM-global `URLConnection` file-name map (`OpenAIPromptOfficePathSuite.scala` lines 70-84 and 124-139) can't leak into other tests:
  - `OpenAIAttachmentUtils.scala:298` is the only caller of `guessContentTypeFromName` in the core, cognitive, deep-learning and opencv sources.
  - `build.sbt:282` sets `Test / parallelExecution := false`.
  - Both overrides are restored in `finally`.

  I did not raise this as a finding.
- [x] Compared the test structure with existing conventions:
  - `OpenAIPromptHttpTestBase.scala` lines 63-111.
  - `OpenAIPromptOfficePathSuite.scala` lines 16-56.
  - `OpenAIPromptMultimodalRequestSuite.scala` lines 191-277; the file is now 772 lines.
  - `scalastyle-test-config.xml`: `maxFileLength` 800, `maxLineLength` 120.
  - `.github/skills/scala-code/SKILL.md`.
- [x] Checked the docs:
  - The notebook's Office section (lines 16-26) matches the code paths above.
  - The `setFileSizeLimitMB(10.0)` example it refers to exists (line 152).
  - The H3 heading inside the intro matches the sibling `AIPrompt.ipynb` (line 25).
  - The "Images and files" section of `AIPrompt.ipynb` (lines 391-396) doesn't list file types, so it isn't out of date.
  - The module already checks in binary audio fixtures, so adding binary Office fixtures follows precedent. `build.sbt:283` publishes test artifacts.
- [x] A grep across cognitive main, cognitive test and docs found no existing assertion or doc that depends on the old Office `Unsupported file type: <mime>.` wording.
- [ ] I did not run tests, scalastyle or notebook validation; this was a read-only review. I only checked the validation evidence you supplied for consistency with the source.

## Issues

### Issue 1: Office rules are split across MIME inference, categorization and one message builder
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: 126-133, 152, 297, 320, 477-479 (compare 488-500)
- **Description**: The Office rules appear in four places, each with a different check:
  - `inferMimeType` maps an extension to its MIME type (line 297).
  - `categorizeFileType` admits only the exact `OfficeMimeTypes.get(extension)` pair (line 320).
  - `responsesMessage` works out why a path was marked `"unsupported"`. It checks `OfficeMimeTypes.valuesIterator.contains(mimeType)` and re-derives `fileExtension(fileName)` (lines 477-479).
  - `MimeTypeExtensions` inverts the same table to pick filenames for data URLs (line 152).

  `categorizeFileType` already knows the reason a path is rejected (an approved Office MIME paired with an unapproved extension). It returns only the string `"unsupported"`, so `responsesMessage`, which just builds the request payload, now contains admission logic.

  This has two consequences:
  - Only the Responses path gets the extension-specific error. Take a `.dot` path that the name map resolves to `application/msword`. `chatCompletionsMessage` (lines 488-500) tells the user to "use apiType='responses'", and Responses then rejects the same input. The generic Chat wording existed before this patch; the mismatch between the two APIs is new.
  - The inverted map assumes each MIME type has exactly one extension. If a later entry maps two extensions to one MIME type, the data-URL filename will depend on HashMap iteration order rather than the order the entries are declared.
- **Risk**: Nothing is broken today. But templates, macro-enabled variants and aliases are already explicit policy decisions, so future allowlist changes will need matching edits in three or four places. The Chat and Responses error messages can also drift further apart.
- **Suggested Fix**: Put the Office rules in one private helper, for example an `OfficeFormats` object. It would expose `mimeFor(extension)`, `admits(extension, mimeType)` and `isOfficeMime(mimeType)`, plus an explicit MIME-to-extension map instead of an inverted one. Then compute the rejection detail where categorization happens. One option is a shared `unsupportedDetail(fileName, mimeType): Option[String]` that both `responsesMessage` and `chatCompletionsMessage` call. At minimum, add a `require` that each MIME type in `OfficeMimeTypes` maps to only one extension. The current message text and assertions can stay as they are.

### Issue 2: The new Office suite doesn't reuse the shared HTTP test base, and Office tests are split across suites near the file-length limit
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptOfficePathSuite.scala; cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala; cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptHttpTestBase.scala
- **Line(s)**: OpenAIPromptOfficePathSuite.scala 16-56; OpenAIPromptMultimodalRequestSuite.scala 45-60, 191-277; OpenAIPromptHttpTestBase.scala 63-111
- **Description**: `OpenAIPromptHttpTestBase.withEchoServer` is the existing fixture for OpenAIPrompt HTTP tests. It provides the mock endpoint, attachment routes and request recording. Its `addAttachmentContext` is a local function with fixed routes, though.
  - Instead of making those routes configurable, the new suite extends `TestBase` and builds its own attachment server (lines 22-37).
  - Because it doesn't have the base's endpoint mock, its only HTTP Office test calls the `createMessagesForRow` helper directly. The public `transform` path for Office URLs is covered only indirectly, through the shared categorizer.
  - The transformer-level Office tests went into the multimodal suite instead (+88 lines). That took the file from about 684 to 772 lines, against the 800-line `FileLengthChecker` limit in `scalastyle-test-config.xml`.
  - The split between the two suites isn't obvious. Helper-level data-URL rejection tests (lines 263-277) are in the multimodal suite, while helper-level path tests are in the new suite.

  This goes against the Scala skill's guidance to reuse existing helpers and keep tests aligned with module conventions.
- **Risk**: There is duplicated fixture code to maintain. The next unrelated test added to the multimodal suite will probably hit the scalastyle limit and force an unplanned split. Office coverage is also harder to audit. Correctness is not affected.
- **Suggested Fix**:
  1. Let `withEchoServer` accept extra attachment routes (path, content type, bytes) and record full request URIs.
  2. Move `requestPayload` into the base.
  3. Make the Office suite extend `OpenAIPromptHttpTestBase`, and move the Office tests from lines 191-277 of the multimodal suite into it. Consider renaming it `OpenAIPromptOfficeAttachmentSuite`.
  4. Keep the cases that change the file-name map in that same suite, which runs serially.
  5. Optionally, add one HTTP Office case that goes through `transform`.

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

## Driver rebuttal

Both observations are retained as optional refactoring suggestions, not defects
requiring a change to this patch. The reviewer explicitly states that nothing
is broken today and correctness is not affected. No source was changed.

**Issue 1:** `OfficeMimeTypes` is already the single source of truth
(`OpenAIAttachmentUtils.scala:126-133`). All four consumers derive their behavior
from that table; adding a distinct supported pair would not require synchronizing
four separate allowlists. The six current MIME values are distinct. All six
data-URL filename/MIME pairs are independently asserted through `transform`
(`OpenAIPromptMultimodalRequestSuite.scala:191-235`), so the suggested future
duplicate-entry scenario is not an existing ambiguous mapping. A second explicit
reverse table would duplicate policy rather than improve this invariant.

The `responsesMessage` guard at lines 477-479 only selects an error message
after categorization has already rejected the input; it does not admit a file.
Both APIs rejected `.dot` aliases before this change. Chat's generic suggestion
already pointed those callers toward Responses, where they were rejected; this
patch only makes the Responses explanation more specific. Changing Chat error
text, introducing an OfficeFormats abstraction or expanding the classifier's
return type is unnecessary and would broaden the explicit compatibility scope.

**Issue 2:** The shared HTTP base has no configurable Office attachment routes:
its route helper is local and its endpoint behavior is fixed
(`OpenAIPromptHttpTestBase.scala:63-111`). Reusing it for this focused
attachment-reader test would first require a generalization of shared
infrastructure used by unrelated suites. The new server is a small local fixture
with deterministic bytes, exact request-URI assertions and finally-based cleanup
(`OpenAIPromptOfficePathSuite.scala:16-60`).

Public transformer serialization is already exercised for every Office format
through `requestPayload`, which calls `transform`
(`OpenAIPromptMultimodalRequestSuite.scala:45-60,191-235`); the additional suite
isolates path-reader and JVM MIME-map edge cases. A future hypothetical file
length violation is not a current style failure: 772 lines is below the enforced
800-line limit and both style gates pass. Moving existing tests and generalizing
the base can be considered separately, but are not prerequisites for this fix.

Verification: current source and the actual transformer assertions were read;
all 129 tests and both cognitive style gates pass on the frozen patch. All 46
reviewer tool calls were audited with no access to prior/sibling feedback or
checkpoints. The original ISSUES_FOUND verdict is preserved. Driver triage
rejects these as mandatory findings; this is not a newly clean reviewer verdict
or a waiver of any remaining independent review round.

## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
