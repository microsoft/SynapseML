---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 2
timestamp: "2026-11-18T01:47:19Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2cac2a8367ab632ae88c1b1174f26b00d4f646f26a6312085e8dba806499fd83"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-17-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the complete diff in `.review-input-decisions-paths.diff`, which has 13 `diff --git` headers at lines 1, 35, 141, 289, 349, 422, 498, 1072, 1125, 1228, 1346, 1470 and 1540. I used the raw `OpenAIPrompt.scala` hunk (diff lines 349–421) to attribute changes, so no hunks were missing.
- [x] Checked repository conventions:
  - `AGENTS.md`: Scala-first, keep public JVM signatures, no RDDs.
  - `.github/skills/scala-code/SKILL.md`: follow existing patterns, reuse helpers.
  - `scalastyle-config.xml` lines 3–4: `FileLengthChecker` `maxFileLength` is 800 at error level. `OpenAIPrompt.scala` is now exactly 800 lines.
- [x] Checked the base-class extension pattern:
  - `OpenAIPrompt.scala` exposes protected hooks at 114, 192, 195 and 626.
  - `AIPrompt.scala` 208–218 and `OpenAIDecisions.scala` 20–21 override them.
  - All five `"decisions"` references in `OpenAIPrompt.scala` (578, 615, 628, 637, 651) are new in this diff. No `openai`-package file referenced `AIPrompt`-only API types before.
- [x] Checked eager versus lazy message preparation:
  - `AIPrompt.scala` 474–478 and `AIRequestAdapter.scala` 582–598 and 610–630.
  - Chat/Responses still force messages at the call site through the by-value overload, before `selectedModel`. That keeps attachment IO errors ahead of missing-model errors.
  - Decisions resolves the model and version, validates options and questions, and checks the pre-IO image count (589) before calling `messages()` (590).
  - `CancelledAttachmentReadException` still propagates (`AIPrompt.scala` 482–494).
- [x] Checked that error classes match on the path and non-path Decisions routes:
  - `AIPrompt.scala` 380–383 uses the same `"input is null or empty"` text as `AIRequestAdapter.input()` (535–543).
  - Whitespace-only text goes to `NativeQuestions.text` (32–35) and becomes `invalid_request` on both routes.
- [x] Checked capability and preflight rules:
  - `AIRequestAdapter.scala` 247–265: `decisionsSettings`, the `inputMode='text'` rule, and `fileSizeLimitMB` requiring a path column after `validateMessageConstruction`, so `messagesCol` reports its own conflict.
  - 325–332: the inputCol/path conflict check uses `AIColumnPlan.matches`, which respects Spark's resolver.
  - System One still rejects `columnTypes` (`AIPromptMediaInputSuite.scala` 632–637).
- [x] Checked the image-format gates:
  - `OpenAIPrompt.scala` 628–650.
  - `OpenAIAttachmentUtils.scala` 137–141 (`MimeTypeExtensions`), 156–157, 315–318 (`mayBeImagePath`), 320–338 (`categorizeFileType`), 431–442 (`dataUrlFileType`, metadata only).
  - The by-name predicate keeps these checks out of Chat/Responses.
  - WebP sniffing is shared (122–123, 295–307).
- [x] Checked hosted-URL validation in `NativeQuestions.scala` 203–213 and 215–245. The only other URL helper is `AIRequestAdapter.endpointUri`, which serves service endpoints and has different semantics, so a local helper is justified.
- [x] Checked the image-limit sites and their tests:
  - `NativeQuestions.scala` 118, 136, 151–167 and 169–175.
  - `NativeQuestionsSuite.scala` 138–141.
  - `AIPromptMediaInputSuite.scala` 520–560: pre-IO path plus question limit, a whitespace-only path cell, and no image fetch for the over-limit row.
  - `AIPromptMediaInputSuite.scala` 640–664: shared plus question totals.
  - Every test matches the substring `"at most 128 images"`.
- [x] Checked durable mode and public surface:
  - `AIDurableInput.scala` is unchanged and still requires inline images for path columns and question images.
  - `AIRequestAdapter` is `private[ai]`; `NativeQuestions` and `OpenAIAttachmentUtils` are `private[services]`.
  - No hand-written Python under `cognitive/src/main/python` mentions `columnTypes`, `fileSizeLimitMB` or Decisions, so generated wrappers can't diverge.
- [x] Checked the docs: `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 245–336 and 741 match the code. They cover the inputCol/path conflict, `inputMode='text'`, usable-path and missing_input rules, per-file ordering, pre-IO extension rejection, inline-only durable images and standard MIME naming.
- [x] Stayed independent: I didn't read `reviews/**`, `.git/**` or any session or sibling artifacts. Searches were limited to `cognitive/src`, the docs file, `scalastyle-config.xml` and the permitted diff file.
- [ ] I didn't run tests, ScalaStyle or codegen because my tools are read-only. I relied on the driver's validation evidence for those.

## Issues

### Issue 1: Decisions attachment rules are hard-coded in the legacy `OpenAIPrompt` base class instead of using its subclass hooks
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 578, 615–616, 620–621, 628–629, 634–635, 637, 645, 649, 651. Related: `OpenAIAttachmentUtils.scala` 137–141, 315–318, 320–338, 431–442; `scalastyle-config.xml` 3–4.
- **Description**:
  - `OpenAIPrompt.supportedApiTypes` (192) never includes `"decisions"`. That value only comes from the `ai`-package subclasses `AIPrompt` and `OpenAIDecisions`.
  - This PR adds the base class's first knowledge of an `AIPrompt`-only API: `input_text` part typing, a single user message with no system prompt, a Decisions-specific error, and two `"responses" | "decisions"` match arms.
  - This file's existing way to customize subclasses is protected hooks: `hasExplicitPostProcessing`, `supportedApiTypes`, `defaultApiType` and `useBoundedAttachments`. `AIPrompt` and `OpenAIDecisions` override those.
  - The PR also creates two definitions of a supported Decisions image:
    - Data-URL cells and pre-read filenames are checked through the shared `imageExtensions` (313) and `MimeTypeExtensions`.
    - Files that have been read are checked against an inline literal, `Set("image/jpeg","image/png","image/gif","image/webp")` (649).
    - They agree today only because `MimeTypeExtensions` happens to map exactly those four image types.
  - The file is now exactly 800 lines, the error-level ScalaStyle cap. To fit, the hunk deleted two explanatory comments in `createMessagesForRow` and shortened `buildContentParts` by renaming its parameters and dropping its explicit return type.
- **Risk**: This is not a demonstrated defect; the documented behavior holds and is tested. The maintainability risks are:
  - Decisions format rules now live in two packages: `NativeQuestions` for explicit parts and the base class for path columns.
  - Widening `imageExtensions` for Chat/Responses (for example, adding `bmp`) would cause one of two problems:
    - If `MimeTypeExtensions` is also updated, BMP data-URL path cells would pass for Decisions while BMP files would still be rejected after being read.
    - If not, `.bmp` files would no longer be rejected before IO, which silently breaks the documented "rejected without reading" promise.
  - Any edit that adds net lines to `OpenAIPrompt.scala` now fails root ScalaStyle.
- **Suggested Fix**:
  - Keep the base behavior unchanged and add narrow protected hooks whose defaults match today's behavior. For example: a text-part type, a generated-message envelope that defaults to `getPromptsForMessage(Left(parts))`, and an attachment validator that defaults to a no-op.
  - Override those hooks in `AIPrompt` when `apiType == "decisions"`.
  - Define the Decisions image extensions and MIME allowlist once, in `NativeQuestions`. Use that one definition for the data-URL, pre-read and post-read gates, widening `mayBeImagePath` to `private[services]` if needed.
  - This changes no API, schema or wire format, and it frees room under the 800-line cap to restore the deleted comments and return type.

### Issue 2: `AIPrompt.prepareRequests` has to know about Decisions' lazy preparation, and it duplicates the usable-path rule
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala (also `ai/AIRequestAdapter.scala` and `openai/OpenAIPrompt.scala`)
- **Line(s)**: `AIPrompt.scala` 377–383, 474–478; `AIRequestAdapter.scala` 589, 610–614; `OpenAIPrompt.scala` 608–610.
- **Description**:
  - The adapter already decides request handling per API (its `api match`). However, the choice between eager messages for Chat/Responses and lazy messages for Decisions is now split.
  - `prepareRequests` checks `getApiType == "decisions"` itself to pick a `request` overload.
  - The thunk overload takes `pathImageCount`. Only Decisions uses it, and the eager overload passes `0`.
  - The pre-IO count at line 476, `Option(row.getString(...)).exists(_.trim.nonEmpty)`, reimplements the filter in `createMessagesForRow`, `Option(v).map(_.trim).filter(_.nonEmpty)` (609), in another package. It also assumes each usable path produces exactly one image part.
  - `messages()` sends Decisions through the branch guarded by `GenerativeApis` and then checks `getApiType == "decisions"` three more times inside that branch.
- **Risk**: There's no current defect: the two rules agree, and `AIPromptMediaInputSuite.scala` 520–560 covers the whitespace-only cell and the no-fetch behavior. But this count rejects rows before any IO. If the filter or the one-image-per-path assumption changes on only one side, the count could come out too high and valid rows would fail with "at most 128 images". The post-IO check in `prepareDecisionsRequest` only catches counts that are too low.
- **Suggested Fix**:
  - Extract one `private[services]` helper for usable attachments and call it from both `createMessagesForRow` and the pre-IO count.
  - Optionally, replace the two overloads with a single thunk-based `request(...)`. The adapter would force messages first for non-Decisions APIs and count Decisions paths itself, using column indices passed when it's constructed.
  - Give Decisions its own `else if` branch in `messages()`.

### Issue 3: The 128-image limit is checked in three places with two counters and two messages
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 118, 136, 151–158, 165–175, 215–245
- **Description**:
  - The limit is checked in three places:
    - `decisions()` checks questions only (118) and says "...per request".
    - The pre-existing `validateDecisionsInput` checks input only (136) and says "Decisions accepts at most 128 images".
    - The new `validateDecisionsImageCount` checks the combined total (165–167) and says "...per request".
  - `prepareDecisionsRequest` validates the input through `prepareDecisionsInput`, which counts images using `validatePart`'s return value. It then counts the same input again with a separate `imageCount` helper.
  - `prepareDecisionsInput` now has only one production caller. So the input-only check is fully covered by the combined check, yet it still runs first and uses different wording.
  - Writing the number into the message string matches the existing `MaxChoices` style, so the real concern is the inconsistent wording and the double count.
- **Risk**: The same violation gets different messages depending on where the excess images are. Changing `MaxImages` means editing three strings. Because tests match only the substring, they won't catch the wording drifting further apart.
- **Suggested Fix**: Define one message from `MaxImages`, such as `s"Decisions accepts at most $MaxImages images per request"`, and use it at all three sites. Have `validateDecisionsInput` return the image count it already computes, so `prepareDecisionsRequest` can reuse it instead of counting again with `imageCount`.

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

## Driver disposition

The three recommendations are closed without implementation for this focused
fix. Their original text, severity and `ISSUES_FOUND` verdict remain unchanged.
No current behavior defect was demonstrated, and no reviewed content changed.

1. **Subclass hooks and format organization:** this repeats the optional
   architecture recommendations in pass 16 round 2 and pass 17 round 1. The
   inherited helpers execute on the facade's runtime API, so the Decisions
   branches are reachable and intentional. The current extension/MIME sets
   agree. Adding protected hooks and moving policy between packages would
   broaden the extension surface without correcting present behavior.
   The production file is within the enforced 800-line limit and root style
   passes; future additions must respect that same guard. Spare line capacity
   is not a requirement of this patch.
2. **Usable-path helper and adapter refactor:** the current predicates both
   accept exactly trimmed nonempty cells, and each accepted Decisions path
   emits one image. `AIPrompt.scala:474-478` explicitly preserves eager
   Chat/Responses preparation and lazy Decisions preparation. Tests now pin
   both sides of that distinction, as well as blank-path exclusion and the
   combined pre-IO count. Extracting a helper or changing the adapter interface
   remains an optional organizational alternative, not a required fix for
   future hypothetical divergence.
3. **Repeated counting and messages:** the input-only validator and combined
   request validator each enforce their respective boundaries. Both messages
   accurately state the current limit, and 128/129 boundaries across shared
   input and multiple questions are tested. Keeping the standalone input
   validator's contract and rechecking the assembled request is deliberate;
   no measured performance problem or incorrect count was found. Consolidating
   strings or changing the validator's return contract is not needed here.

Verification uses the inspected source locations in the checklist, the latest
57 passing media/attachment tests, and unchanged native-question boundary tests
from their prior passing snapshot. Root style/compile/test-compile/codegen and
static wrapper/example checks pass. No optional refactor is claimed as applied.

All 13 files were reviewed from the complete immutable diff. Continue pass 17
on unchanged product patch
`5f939e3f515e6f44844ce05af6b10327dd65b1a020e282378b72bff27834d3d7`.