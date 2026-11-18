---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 2
timestamp: "2026-11-17T21:14:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b49916c3b526f8ceb5ec4ac9f35f8843c95f0b405f1ffdd049a727617cee5922"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-15-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read every changed production hunk in context:
  - `AIPrompt.scala` lines 349-386 and 458-500
  - `AIRequestAdapter.scala` lines 247-283, 313-332, 535-543 and 582-633
  - `NativeQuestions.scala` lines 86-245
  - `OpenAIPrompt.scala` lines 186-204, 308-318 and 570-662
  - `OpenAIAttachmentUtils.scala` lines 119-142, 295-338 and 431-442
- [x] I checked the change against the repository's conventions: `AGENTS.md` ("Follow nearby code before introducing a new pattern", plus the rules on public signatures, RDDs and codegen), `.github/skills/scala-code/SKILL.md`, `.github/skills/code-review/SKILL.md`, and `scalastyle-config.xml` lines 3-4 (`maxFileLength` 800).
- [x] Hook precedent and reachability:
  - `OpenAIPrompt.scala` lines 192-199 only allow `chat_completions` and `responses`.
  - `AIPrompt.scala` overrides `supportedApiTypes` (line 208), `hasExplicitPostProcessing` (line 216) and `useBoundedAttachments` (line 218).
  - So the new `"decisions"` branches in `OpenAIPrompt.scala` (lines 578, 615, 628, 637 and 651) can only be reached from the `ai` subclasses.
- [x] File-length headroom: `OpenAIPrompt.scala` turns `number.of.methods` off at line 33 and ends at line 800 with `// scalastyle:on number.of.methods`. That is exactly the configured limit.
- [x] Public surface and codegen are unchanged:
  - `AIRequestAdapter` is `private[ai]`.
  - `NativeQuestions`, `OpenAIAttachmentUtils` and `createMessagesForRow` are `private[services]`.
  - No new Params were added, so generated wrappers and serialized parameter shapes do not change.
- [x] No RDD use: request preparation stays on `Dataset.mapPartitions` (`AIPrompt.scala` line 468).
- [x] Column resolution is consistent:
  - `validateDecisionsInputColumn` uses `AIColumnPlan.matches`, which uses the session resolver.
  - `attachmentColumns` uses `AIColumnPlan.resolve` (`AIPrompt.scala` lines 349-351).
- [x] Field order is preserved: `AIJson.OrderedMap.+` replaces existing keys in place (`AIJson.scala` lines 23-44). So `fields.updated("instructions" | "content", ...)` in `NativeQuestions.prepareDecisionsRequest`/`prepareDecisionsInput` keeps the native request field order.
- [x] The lazy/eager split is correct:
  - Chat, Responses and System One still call the strict three-argument `request` overload (`AIPrompt.scala` line 478).
  - `validateImageAttachment` takes its check by name and short-circuits for APIs other than Decisions (`OpenAIPrompt.scala` line 628).
- [x] Durable mode stays inline-only: the unchanged `AIDurableInput.content`/`inline` covers both message and question images. The docs (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, Decisions image section and parameter table) match the `validateCapabilities` messages and the `fileSizeLimitMB` behaviour.
- [x] Test conventions are followed:
  - New tests use `TestBase.tmpDir` through `AIPromptTestBase.path()`.
  - The live test is gated by `assume(...)` (`OpenAIDecisionsLiveSuite.scala` lines 72-77).
  - `AIPromptMediaInputSuite.scala` is 764 lines, under the 800-line test limit.
- [ ] Tests not run: this reviewer has read-only tools. I relied on the driver's evidence (284 deterministic tests, ScalaStyle, compile, test compile and codegen).

## Issues

### Issue 1: Decisions rules are hard-coded into the legacy `OpenAIPrompt` base class instead of using protected hooks
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 578, 615-616, 620-621, 628-629, 634-635, 637, 645, 649, 651, 800 (validator at 192-199)
- **Description**: `OpenAIPrompt`'s `apiType` validator only allows `chat_completions` and `responses` (lines 192-199). Even so, the patch adds `"decisions"` literals to this class for:
  - the input part type (line 578);
  - leaving out the system message (line 615);
  - the image allowlist and its error message (lines 628-629, called at 634, 645 and 649);
  - payload routing (lines 637 and 651).

  Only the `ai`-package subclasses (`AIPrompt` and `OpenAIDecisions`) can reach these branches. The `openai` base class now holds rules for an API type it cannot select itself, so the dependency runs the wrong way. The codebase already has a pattern for this: protected hooks that `AIPrompt` overrides (`supportedApiTypes` line 208, `hasExplicitPostProcessing` line 216, `useBoundedAttachments` line 218). The patch skips that pattern.

  To fit the new code, the patch also removes comments, a blank line and the explicit return type of `buildContentParts`. That leaves the file at exactly the 800-line `maxFileLength` limit. This is a maintainability problem, not a demonstrated functional defect.
- **Risk**:
  - Any future line added to `OpenAIPrompt.scala` fails ScalaStyle, including unrelated Chat/Responses fixes. That pushes people to strip more comments and types.
  - Decisions format rules now live in two packages: `NativeQuestions` for explicit parts and `OpenAIPrompt` for path parts. Changing a Decisions rule means editing the legacy public stage.
- **Suggested Fix**: Replace the literals with a few protected hooks that have neutral defaults, for example:
  - `usesResponsesParts`
  - `includeSystemMessage`
  - one attachment-validation hook, called at the three existing check points

  Override them in `AIPrompt` for `getApiType == "decisions"`, and keep the allowlist and its message next to `NativeQuestions`. Use the freed lines to restore the removed comments and the return type.

### Issue 2: The Decisions image allowlist is written three different ways
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 313, 628-629, 634-635, 645, 649
- **Description**: The JPEG/PNG/GIF/WebP rule appears as:
  - `dataUrlFileType(...) == "image"`, derived from `MimeTypeExtensions` and the shared `imageExtensions` (line 313);
  - `mayBeImagePath(path, imageExtensions)` before IO;
  - a literal `Set("image/jpeg", "image/png", "image/gif", "image/webp")` after the read;
  - a hard-coded error message, "JPEG, PNG, GIF, and WebP".

  They agree today only because the shared `imageExtensions` set happens to match the Decisions allowlist.
- **Risk**: Chat and Responses use the same `imageExtensions` set. If it grows (for example avif or heic), the Decisions data-URL and pre-IO checks would accept the new format, but the post-read MIME check and the error message would not. Inline data URLs and file paths would then accept different formats. Some failures that are now rejected before IO would only fail after IO.
- **Suggested Fix**: Define one Decisions constant, for example MIME types plus derived extensions and a display label. Use it for all three checks and the error message, separate from the Chat/Responses `imageExtensions`.

### Issue 3: Decisions prompt-text validation is duplicated between the stage and the adapter
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: AIPrompt.scala 377-383; AIRequestAdapter.scala 535-543
- **Description**: `AIPrompt.messages` adds three `getApiType == "decisions"` checks to the generative branch. Together they re-implement the adapter's `input()` rule: null or empty text throws `AIMissingInputException("input is null or empty")`. The method then calls `validateDecisionsInput` to reject whitespace-only text. Rows without paths still go through `input()`. So the documented missing_input versus invalid_request contract has two implementations, and which one runs depends on whether the row has a path column.
- **Risk**: If one copy changes (wording, how whitespace is classified, or which text source wins), rows with and without paths will report different error codes or messages for the same prompt text.
- **Suggested Fix**: Give the adapter sole ownership. Two options:
  - For Decisions, `nativeEnvelope` validates `input(row, preparedText)` and calls `validateDecisionsInput` before it calls `messages()`. `AIPrompt.messages` can then keep the generative branch unchanged. Question validation still runs first, so error order is preserved.
  - Alternatively, add one adapter helper that returns the validated text, and call it from both places.

### Issue 4: The pre-IO image count leaks into the shared adapter API and duplicates the attachment check
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: AIPrompt.scala 474-478; AIRequestAdapter.scala 582-598, 610-614; NativeQuestions.scala 118, 136, 165-167; OpenAIPrompt.scala 609
- **Description**:
  - `prepareRequests` branches on Decisions and counts usable paths with `Option(...).exists(_.trim.nonEmpty)`. That is a second copy of `createMessagesForRow`'s `Option(v).map(_.trim).filter(_.nonEmpty)`.
  - The count is passed through a second `request` overload. Its `pathImageCount` parameter is always 0 for every other API.
  - The "no image IO before the limit check" guarantee depends on statement order inside `nativeEnvelope`.
  - The 128-image limit is checked in four places with two different messages: "at most 128 images" (line 136) and "... per request" (lines 118 and 167).
- **Risk**: If the two checks drift apart, the pre-IO count is wrong. Counting too many falsely rejects rows before IO. Counting too few lets IO run before the limit is enforced. The two wordings also make diagnostics and text-matching tests inconsistent.
- **Suggested Fix**: Share one helper that decides whether an attachment value is usable. Consider letting the adapter compute the count from the row, since it already resolves columns, so `prepareRequests` makes a single `request` call. Use one message constant for the limit.

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

## Driver dispositions on the unchanged snapshot

The original findings, severities and `ISSUES_FOUND` verdict remain intact.
All four concern alternative factoring or future drift, not a demonstrated
behavioral failure. The following scope decisions supersede the pending entries;
no source change or reviewer verdict rewrite is made.

- **Issue 1 - considered, architectural extraction deferred.** The base Param
  already uses the virtual `supportedApiTypes` hook in its validator
  (`OpenAIPrompt.scala:192-199`), so the Decisions value is valid on the subclass.
  The shared serializer does not import the `ai` package or call its classes:
  the package dependency has not reversed. Grouping Decisions with Responses
  reuses their identical text/image part representation and reader rather than
  adding three new protected extension points for one caller. The file's
  800-line size is genuine maintenance debt, but is within the existing enforced
  limit and is not a functional blocker. A broader base-class extraction is
  deferred rather than expanding this compatibility fix.
- **Issue 2 - considered, future-policy consolidation deferred.** The current
  extension and MIME classifiers agree for the four supported formats; no
  mismatch exists in the reviewed patch. The post-read MIME check intentionally
  remains separate from the metadata-only precheck, which cannot know content.
  Real-image and misleading-content tests pin both boundaries. Broadening the
  shared format set would be a later behavior change requiring coordinated
  Decisions tests, not evidence that the present configuration is wrong.
- **Issue 3 - considered, helper extraction deferred.** The stage must reject
  invalid prompt text before attachment loading, whereas the adapter also
  supports text-only native inputs and System One. The small checks currently
  agree on text-source precedence and missing/whitespace error semantics.
  `AIPromptMediaInputSuite` pins null, empty and whitespace behavior through
  templates and input columns on both Decisions stages with no image IO.
  Moving ownership across the lazy boundary is an alternative design, not
  needed to restore an invariant or fix a current defect.
- **Issue 4 - considered, private adapter design retained.** `AIRequestAdapter`
  is package-private, so the overload adds no public signature or serialization
  contract. The by-value overload deliberately preserves eager behavior for the
  other APIs; the lazy overload permits the pre-IO count for Decisions.
  Both count and reader use the same null/trim/nonempty rule and each usable path
  produces one image part. The one/two-path, blank-path and combined-count tests
  verify the boundary and download counts. The post-read count also protects
  explicit parts and question images. Shared helper/message consolidation is
  optional; no incompatible limit or divergent result was found.

Verification: direct reads of the cited stage, adapter and attachment-reader
code, plus 284 passing deterministic tests on this exact snapshot, including
native JSON/order, System One and durable preflight. Root style/build/codegen
gates pass. No provider E2E is claimed. Continue the remaining independent
lenses on patch
`e37ef3864d28d028a4ab89f9425fe4000fbd2e0da319fb3087d70b94145b7b19`.