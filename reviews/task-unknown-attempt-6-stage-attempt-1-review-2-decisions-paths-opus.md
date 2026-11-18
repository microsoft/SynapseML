---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 2
timestamp: "2026-11-17T11:36:19Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "0feddeefdce8462adc0ac7ddce66a67939b44231f6face9fa9810e94115bc094"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-6-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`. That covered `decisions()` (87-120), `validateDecisionsInput` (122-139), `prepareDecisionsInput` (141-150), `prepareDecisionsRequest` and the count helpers (152-188), and the hosted/inline branches in `validatePart` (207-243). Hosted URLs are only syntax-checked and forwarded unchanged; they are never fetched.
- [x] In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` I read:
  - the constant hoisting (206-216) and `validateCapabilities` (247-262)
  - the constant-question validation (288) and `input` (524-531)
  - `nativeEnvelope` (571-587) and both `request` overloads (599-621)

  System One is unchanged: the allowlist still rejects `columnTypes`/`fileSizeLimitMB` for it, and its envelope still comes from `input()`. The adapter is `private[ai] final`, and the original `request` signature is kept.
- [x] In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` I read the `supportedApiTypes`/`useBoundedAttachments` overrides (208-218), `generatedMessages`/`messages` (353-386), `prepareRequests` (453-500), and the `infer` orchestration. The lazy thunk is only used for `decisions`; Chat and Responses still call the eager overload. Retained messages stay limited to Chat and Responses, so the output schema is unchanged.
- [x] In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` I read:
  - the base `supportedApiTypes` (192)
  - the `columnTypes`/`fileSizeLimitMB` Param docs (242-271), which don't name any API, so generated wrapper docs need no change
  - `imageExtensions` (313) and the message construction code (577-658)
- [x] In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` (100-500), I confirmed that the RIFF/WEBP check runs before the JDK content guess, so RIFF/WAVE files still go through the JDK and stay audio. The Responses part builders are reused as-is, and the data-URL and file classification paths are as described in Issue 4.
- [x] I read all of `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala`. Durable mode still rejects hosted URLs and filesystem paths in path columns and question instructions, through the unchanged `inline`/`content`/`externalBindings` walkers.
- [x] I read `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIJson.scala` 23-41. `OrderedMap` keeps a key's position on `+` and `-`, which matters for the idiom point in Issue 3.
- [x] I checked `OpenAIDecisions.scala` and the `AISystemOne` overrides, and grepped `cognitive/src/main/python` for `decisions` and `columnTypes`: no matches, so there is no hand-written Python gating. Against the AGENTS.md checklist: no new stages or params, no RDD code, no `target/` edits, and no changes to persisted param shapes.
- [x] I reviewed the new tests in `AIPromptMediaInputSuite`, `AIPromptRecoveryAttachmentSuite` and `NativeQuestionsSuite` against suite conventions: `Using.resource` recording servers, `transformSchema` equality, and copy/save/load coverage for both public stages. I also compared them with the existing classifier tests in `OpenAIPromptMultimodalRequestSuite.scala` (141-300).
- [x] I checked `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 205-384 against the code paths and against the doc's own Foundry and public-OpenAI binding conventions.
- [ ] Not done: I didn't run any builds or tests. This is a read-only review, so I relied on the validation evidence the driver supplied.

## Issues

### Issue 1: The base `OpenAIPrompt` hard-codes `decisions`, a mode only `AIPrompt` supports, instead of using a subclass hook
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 578, 616-617, 630-631, 636-638, 649-651 (context: 192, 628; `AIPrompt.scala` 208-218)
- **Description**:
  - `OpenAIPrompt.supportedApiTypes` contains only `chat_completions` and `responses` (192). `decisions` can only be reached through `AIPrompt` (208-209) and `OpenAIDecisions`.
  - Even so, the diff puts the Decisions content rules in the base class. It adds five `getApiType == "decisions"` branches:
    - in `stringMessageWrapper` (578)
    - in `createMessagesForRow` (616-617)
    - in the new `validateImageAttachment` (630-631)
    - in both `wrapFileToMessagesList` match arms (638, 651)
  - The codebase already handles `AIPrompt`-specific attachment behavior with a protected hook: `useBoundedAttachments` (628), overridden at `AIPrompt.scala` 218.
  - The rest of the Decisions rules live in `AIRequestAdapter` and `NativeQuestions`. They are now split between the legacy class and the `AIPrompt` layer.
- **Risk**: The base methods now have to list API types that only a subclass supports, inside string matches with no default case. Any future `AIPrompt` API that reuses generated messages would mean editing the legacy stage again. Readers of `OpenAIPrompt` also see branches that never run for that class. There's no functional defect today.
- **Suggested Fix**: Add protected hooks whose base defaults keep today's behavior, and override them in `AIPrompt` when `getApiType == "decisions"`:
  - `protected def attachmentWireApi: String = getApiType`. `AIPrompt` would map `decisions` to `responses` for part shapes and the `input_text` wrapper.
  - `protected def wrapGeneratedContent(parts): Seq[OpenAICompositeMessage] = getPromptsForMessage(Left(parts))`
  - `protected def validateAttachmentType(fileType: String): Unit = ()`

  The existing Chat, Responses and Decisions suites already cover this refactor.

### Issue 2: Checks that must run before image loading are split between `AIPrompt` and `AIRequestAdapter`
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala
- **Line(s)**: 377-383, 474-478 (related: `AIRequestAdapter.scala` 524-531, 571-587, 599-600; `OpenAIPrompt.scala` 609)
- **Description**:
  - **(a) Prompt-text rules are copied.** `AIPrompt.messages` re-implements the adapter's Decisions prompt-text rules:
    - It throws the same `"input is null or empty"` `AIMissingInputException` (380-382), duplicating `AIRequestAdapter.scala` 530.
    - It rejects whitespace-only text by calling `NativeQuestions.validateDecisionsInput` directly (383).
    - This happens inside a general-purpose method that now has three `getApiType == "decisions"` checks.
  - **(b) The path-filter logic is duplicated.** `prepareRequests` computes `pathImageCount` (476) with a copy of `createMessagesForRow`'s blank-path filter (`OpenAIPrompt.scala` 609). It also assumes that each non-blank path becomes exactly one `input_image`. That holds only because `validateImageAttachment` rejects other types and `wrapFileToMessagesList` returns a single part.
  - **(c) The check ordering is implicit.** The guarantee that row metadata is checked before any image IO comes only from a `lazy val` (474), the new thunk overload, and the statement order inside `request`/`nativeEnvelope`. The eager overload passes a hard-coded `0` image count (599-600).
- **Risk**: Decisions prompt-text rules and attachment expansion now each have two owners. A change on one side (message wording, trimming, attachments that produce several parts) could quietly make path and non-path rows behave differently. It could also make the early image estimate disagree with the final count, which would reject valid rows early. Current tests pin today's behavior, so drift would likely be caught, but the coupling remains.
- **Suggested Fix**: Make the adapter the single owner.
  - For Decisions, have `nativeEnvelope` resolve and validate the text with the existing `input(row, preparedText)` plus `NativeQuestions.validateDecisionsInput`.
  - Then call a builder of type `String => Option[Seq[Row]]` that does only the attachment IO.
  - Compute the early estimate from one shared helper (for example `usableAttachments(map, order)`) that `createMessagesForRow` also uses. Add a comment linking it to the one-image-per-path rule.
  - Add a short comment at `request` that states the required check order.

### Issue 3: The image-limit checks and question preparation are fragmented, and constant questions are reprocessed on every row
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 13, 92, 119, 133-137, 146, 152-179 (related: `AIRequestAdapter.scala` 206-216, 288, 578, 584)
- **Description**:
  - **The 128-image limit is checked in several places, with different counters and messages:**
    - `validateDecisionsInput` adds up the counts returned by `validatePart` (133) and fails with "Decisions accepts at most 128 images" (137).
    - `decisions()` throws away the count from `validatePart` (92) and recounts with the new `questionImageCount`/`imageCount` (119, 173-179). Its message ends in "per request".
    - `prepareDecisionsRequest` validates the input again, recounts it, and recounts the questions through `validateDecisionsImageCount` (152-171).
    - `nativeEnvelope` counts question images once at `AIRequestAdapter.scala` 578, and line 584 counts them again.
  - **User-visible effect:** JSON input with 129 images gets one message, while 128 input images plus one question image gets the other, for the same documented limit.
  - **Implicit precondition:** the count helpers read `obj(question)("instructions")` directly. They depend on earlier validation; on unvalidated JSON, a `NoSuchElementException` would escape the row-level error handling.
  - **Constant questions are reprocessed per row:** they are validated once (288), like the adapter's other precomputed constants (206-216), but rebuilt and recounted for every row.
  - **Inconsistent idiom:** `prepareDecisionsRequest` rebuilds objects with `ListMap` (161, a new import). The sibling `prepareDecisionsInput` uses `fields.updated` (146), which already keeps key order through `AIJson.OrderedMap`.
  - **Stale doc comment:** the object's doc comment (13) still says it does not rewrite provider JSON.
- **Risk**: The counters can drift apart, and users see different errors for the same limit. Work that never changes between rows is repeated per row, at a cost proportional to the size of the questions, including any inline question images. The helpers' preconditions are not stated anywhere.
- **Suggested Fix**:
  - Have validation return the counts: question images from `decisions()` and input images from `validateDecisionsInput`.
  - Send every check through one `checkImageLimit(total)` with one message.
  - Prepare constant questions, and their image count, once in a `lazy val` next to `constantQuestions`.
  - Use `fields.updated("instructions", prepareParts(fields("instructions")))` instead of the `ListMap` rebuild.
  - Update the object's doc comment.

### Issue 4: The Decisions image-only check classifies data URLs and files differently
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 630-631, 636, 649 (context: 313; `OpenAIAttachmentUtils.scala` `categorizeFileType` and `responsesDataUrlMessage`)
- **Description**:
  - For data URLs, the check only looks at the raw `data:image/` prefix (636). For files, it uses the shared classifier's result, `fileType == "image"` (649).
  - So image formats outside `imageExtensions` (313: jpg, jpeg, png, gif, webp), such as TIFF or BMP, get different errors depending on the source:
    - A file fails with "Decisions path columns only support image attachments".
    - A data URL passes the check, then fails later in `responsesDataUrlMessage` with "Unsupported file type: image/tiff."
  - The hard-coded `"data:image/"` string also duplicates `NativeQuestions.InlineImagePrefix`.
- **Risk**: The same format produces a different error depending on where it came from. For a real but unsupported image, the file message is misleading. This weakens the "one clear error" goal for rejected attachments, and leaves two classification rules to maintain.
- **Suggested Fix**:
  - Base the check on the shared classifier's `fileType` for both sources. Either expose a data-URL classification helper from `OpenAIAttachmentUtils`, or pass a set of allowed types into the shared Responses builders.
  - Use one error message that names the supported formats (JPEG, PNG, GIF, WebP).
  - Add a TIFF or BMP case, as both a file and a data URL, to the existing invalid-path test.

### Issue 5: The new Decisions image docs contradict the documented Foundry binding and modify an unrelated example stage
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Unified AI prompting.md
- **Line(s)**: 271-276, 320-329, 346 (compare 217-231 and the Scala example at 378-382)
- **Description**:
  - **Binding contradicts the guidance.** The Foundry image example sets `DECISIONS_DEPLOYMENT` with `setModel` (275). The existing Foundry Decisions example uses `setDeploymentName` (220), and the text says "Use the **deployment name**" (231). The doc reserves `setModel` for public OpenAI; the Scala example uses `setModel(sys.env("DECISIONS_MODEL"))`.
  - **The question-image snippet modifies the wrong stage.** It calls `decisions.setQuestions(...)` (320) on the text-ticket stage (`setInputCol("ticket")`, 222). That stage's shared input contains no reference image, yet the instruction asks whether the candidate "match[es] the shared reference image" (325).
  - **The narrative is broken.** The new block also replaces the three-question set and now sits between the Python three-question example and the Scala lead-in, "The same three-question workflows are available directly from Scala" (346).
- **Risk**: Readers may copy a binding that contradicts the guidance right next to it, and the per-question example's premise doesn't hold. This only affects clarity: `setModel` still works, because both setters fill the `model` field and Decisions routing and auth don't depend on which one is used.
- **Suggested Fix**:
  - Use `.setDeploymentName(os.environ["DECISIONS_DEPLOYMENT"])` in the Foundry example.
  - Base the question-image snippet on `image_decisions` (whose shared input is the path image) or on a new stage.
  - Move the image subsection after the Scala example, or reword the Scala lead-in.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not adopted; optional architectural alternative, not a correctness defect.
- **What changed**: Retained the existing shared message/attachment builder.
- **Why**: The supported subclass explicitly opts into Decisions; the legacy stage still rejects it. Adding three new protected extension hooks solely for this one supported subclass expands the abstraction and JVM surface without correcting observed behavior. The API-specific branches intentionally reuse the existing bounded reader and wire serializers.
- **How verified**: Inspected `AIPrompt.supportedApiTypes`, `AIRequestAdapter.validateCapabilities`, and `OpenAIPrompt.createMessagesForRow`. The public-stage tests cover both Decisions classes and preserve Chat/Responses behavior. This disposition does not claim the suggested design is invalid in a broader refactor.

### Issue 2
- **Status**: Not adopted; retain tested ordering and compatibility.
- **What changed**: No additional request-builder refactor.
- **Why**: The two entry paths have distinct responsibilities: ordinary native text goes through `input`, while generated path messages validate their source prompt before attachment IO. Both use `NativeQuestions.validateDecisionsInput` for whitespace semantics. The early count uses exactly the loader's non-null, trimmed-nonempty predicate; each supported path produces one image. The final count independently validates the actual serialized request. A future multi-part attachment API would require revisiting this invariant, but is explicitly outside this image-only change.
- **How verified**: Traced `AIPrompt.messages` and `prepareRequests` through both adapter `request` overloads and `nativeEnvelope`. Model/version/options/questions and the known combined image count precede the lazy builder; the eager overload preserves existing APIs. Regressions exercise null/empty/whitespace text, row metadata errors, skipped empty paths and the 128-image boundary before IO.

### Issue 3
- **Status**: Clarity and idiom fixes applied; broader counting/cache redesign not adopted.
- **What changed**: Corrected the object comment to acknowledge optional-null normalization and used the existing `fields.updated` idiom for question instructions.
- **Why**: All production callers validate questions before the count helpers: constant questions during adapter validation, row questions in `nativeEnvelope`, and standalone validation before its final question count. The input-only error retains its existing public text; combined input/question failures identify the request-wide bound. Rechecking the actual serialized request is intentional defense beyond the pre-IO estimate. Preparing constant questions per row does not decode or duplicate their base64 strings; adding a cached prepared-question representation is an optional performance change with no demonstrated regression.
- **How verified**: Inspected all call sites in `AIRequestAdapter` and `NativeQuestions`. Existing ordering/null-normalization and combined-count tests are included in the fresh nine-suite validation; results are recorded below after completion.

### Issue 4
- **Status**: Fixed and locally validated.
- **What changed**: Both inline and file attachments now use the shared file-type classifier. Rejection names JPEG, PNG, GIF and WebP explicitly. Added TIFF and BMP file/data-URL cases to the public-stage invalid-path regression.
- **Why**: The same unsupported format must have a consistent, actionable error regardless of input source. Metadata-only inline classification avoids copying the base64 body before size validation.
- **How verified**: Before the fix, the new regression failed with `Unsupported file type: image/tiff.` instead of the common Decisions error. All 232 tests across nine suites now pass on the corrected patch, including both Decisions stages and file/data-URL TIFF/BMP cases.

### Issue 5
- **Status**: Fixed and locally validated.
- **What changed**: The Azure image example uses `setDeploymentName`; per-question image instructions modify `image_decisions`; the Scala lead-in explicitly refers to the earlier text-only three-question workflows. Documented supported path-image formats.
- **Why**: The example now has the shared reference image it describes, follows the documented Azure deployment-binding convention, and no longer implies the intervening example still has three questions.
- **How verified**: Read the surrounding examples and public setter signatures. All six documented Python examples and both regenerated wrappers parse; the wrappers expose the shared path and question setters.

### Post-fix validation

The nine-file product patch has SHA-256
`ec1be9462ad345440d69e75b54cd7e0a881c7c209b880e37f693c0c453f488ea`
against target `d17f57e314517e4b413cda3cdcd86e7ad15ddb04`.
All 232 tests across nine affected suites passed with zero failures, errors or
skips. Root ScalaStyle, test ScalaStyle, compile, test compile and codegen passed.
This includes ordered-map/null-normalization coverage for Issue 3.
The fixes invalidate this pass; all six lenses will run afresh on the frozen
corrected patch. Local recording-server coverage is not live-provider E2E.