---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 2
timestamp: "2026-11-18T04:12:09Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "dcfa88c48170ef1075c6da33610dfa980b9992c48338e75b8c5e5807a4685869"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-18-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 13 file diffs in `.review-input-decisions-paths.diff`: `AIPrompt`, `AIRequestAdapter`, `NativeQuestions`, `OpenAIAttachmentUtils`, `OpenAIPrompt`, the docs and seven test suites. I compared the hunks with the post-change source to confirm which lines this patch added.
- [x] Checked the change against the repository conventions in AGENTS.md: follow nearby code, keep behavior Scala-first, and keep public JVM signatures and serialized params. I also checked it against `.github/skills/scala-code/SKILL.md`.
- [x] Checked the existing ways that `OpenAIPrompt` lets subclasses change behavior. These hooks are `supportedApiTypes`/`defaultApiType` (`OpenAIPrompt.scala` 192-195), `useBoundedAttachments` (line 626, overridden in `AIPrompt.scala` 218) and `hasExplicitPostProcessing` (`AIPrompt.scala` 216). `OpenAIDecisions` extends `AIPrompt` (`OpenAIDecisions.scala` 15), so both stages share these changes.
- [x] Traced the order of a Decisions request: `AIPrompt.prepareRequests` (474-478), then `AIRequestAdapter.request` (610-631), then `nativeEnvelope` (582-598), then `NativeQuestions.prepareDecisionsRequest` (151-163). I also checked how errors map to codes (`AIPrompt.scala` 483-495). Chat/Responses still build their messages before selecting the model.
- [x] Confirmed the image-format checks agree today. For example, `data:image/bmp` becomes `"bin"` in `extensionForMimeType`, and `categorizeFileType` then marks it unsupported (`OpenAIAttachmentUtils.scala` 156-157, 320-338, 431-442). This matches the after-read MIME set (`OpenAIPrompt.scala` 649).
- [x] Confirmed the WebP fix only changes `inferMimeType` (`OpenAIAttachmentUtils.scala` 295-308). It requires at least 12 bytes and checks for `WEBP` at offset 8. Other RIFF files, including WAV, still go through JDK detection.
- [x] Confirmed durable `outputPath` reuses the existing generic path-to-inline check (`AIDurableInput.scala` 84-90, 172) with no Decisions-specific code. `validateDecisionsInputColumn` uses `AIColumnPlan.matches`, which applies Spark's column resolver (`AIRequestAdapter.scala` 325-332).
- [x] Confirmed no public API surface changed:
  - New members are scoped `private[ai]`, `private[services]` or `private[openai]`.
  - There are no new Params, so persistence and codegen are unaffected.
  - No hand-written Python wrapper covers `AIPrompt` or `OpenAIDecisions`.
- [x] Checked that the docs match the code: `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 119-122, 255-312 and 740-741.
- [x] Reviewed test coverage:
  - `AIPromptMediaInputSuite.scala` 612-636 covers JSON plus path, and `messagesCol` plus path.
  - The Chat/Responses loop in `AIPromptAttachmentSuite.scala` (313/336) guards the eager IO error order.
  - No test covers JSON mode with text-only `columnTypes`.
- [x] Did not read `reviews/**`, other reviewers' artifacts or session data. Searches were limited to the source, test and docs paths under review.
- [ ] I ran no tests; this review was read-only. I relied on the validation evidence the driver supplied. There is no Python runtime or live-provider evidence.
- [ ] I did not check whether the artifact already exists, because the plan forbids access to `reviews/`.

## Issues

### Issue 1: Decisions-specific rules are hard-coded into the legacy `OpenAIPrompt` base class instead of using its existing protected hooks
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 577-583, 614-616, 628-629, 634-637, 645-651. For comparison, the existing hooks are at 192-195 and 626, and in `AIPrompt.scala` at 216-218.
- **Description**: This is a maintainability and convention finding that should be fixed. It is not a demonstrated functional defect.
  - `OpenAIPrompt` supports only `chat_completions` and `responses` (line 192), and `apiType` is validated against that list. The class can therefore never have `apiType` `"decisions"` itself, yet it now checks `getApiType == "decisions"` in five places.
  - As a result, the base class now holds rules that belong to its `AIPrompt` subclass:
    - Decisions uses `input_text` parts (`stringMessageWrapper`).
    - Decisions sends a single user message with no system message (`createMessagesForRow`).
    - Path columns accept only JPEG, PNG, GIF and WebP images, along with the matching error message.
    - Two non-exhaustive `String` matches now dispatch on `case "responses" | "decisions"`.
  - The rest of the Decisions rules live in the `ai` package (`AIRequestAdapter.validateCapabilities`, `NativeQuestions.validatePart`). These two classes already let subclasses change behavior through protected hooks: `supportedApiTypes`, `useBoundedAttachments` and `hasExplicitPostProcessing`. AGENTS.md says: "Follow nearby code before introducing a new pattern."
- **Risk**:
  - Decisions behavior is split across two packages and can't be seen from `AIPrompt` or the adapter.
  - Each future native or multimodal API will need edits to this public legacy class.
  - The string matches could throw `MatchError` if another `AIPrompt` API type ever reaches attachment wrapping.
  - Chat/Responses behavior is unchanged today: the Decisions branches can't be reached, and the format checks use by-name parameters that are only evaluated for Decisions.
- **Suggested Fix**:
  - Add a few `protected` hooks that default to Chat/Responses behavior, and override them in `AIPrompt`. The hooks must be `protected`, because `private[openai]` members can't be overridden from package `ai`. Examples:
    - `usesInputContentParts`, which replaces the wrapper check and both `"responses" | "decisions"` matches.
    - `composeGeneratedMessages(parts)`, defaulting to `getPromptsForMessage(Left(parts))`.
    - `validateAttachmentBeforeRead(path)` and `validateAttachmentAfterRead(mimeType)`, both no-ops by default.
  - Implement the overrides with a Decisions image rule in the `ai` package (see Issue 3), so `OpenAIPrompt` contains no Decisions literals.

### Issue 2: The before-IO image count and error order are split between `AIPrompt` and `AIRequestAdapter`, and the order depends on which overload is called
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: `AIPrompt.scala` 377-384, 474-478; `AIRequestAdapter.scala` 541, 582-598, 610-614; `OpenAIPrompt.scala` 608-609
- **Description**: This is a maintainability finding that should be fixed; there is no demonstrated defect.
  - (a) The stage counts path images with its own filter (`Option(row.getString(...)).exists(_.trim.nonEmpty)`, line 476). That filter must stay equivalent to the attachment filter in `createMessagesForRow` (`OpenAIPrompt.scala` 608-609). It also assumes that each path becomes exactly one image part.
  - (b) The adapter's `request` gains a `pathImageCount: Int` parameter that only Decisions uses. The `Option` overload hard-codes `0`, so the adapter can't tell "no path images" apart from "not counted".
  - (c) Chat/Responses report IO errors before missing-model errors only because `adapter.request(row, prompt, inputMessages)` evaluates the lazy value as an argument before `selectedModel` runs. The ordering therefore depends on which overload is called. Only the Chat/Responses test at `AIPromptAttachmentSuite.scala` 313/336 protects it.
  - (d) `messages` now mixes three `getApiType == "decisions"` checks into the generative branch. It also repeats the adapter's `"input is null or empty"` message (`AIRequestAdapter.scala` 541).
- **Risk**:
  - If attachment filtering changes, the before-IO count can drift from the real count. Over-counting would reject valid rows before IO. Under-counting is caught only after the reads.
  - Merging the two overloads in a later refactor could silently change Chat/Responses error order.
  - The Decisions before-IO guarantees are hard to audit in mixed-API conditionals.
- **Suggested Fix**:
  - Use a single thunk-based `request`:
    - For Chat/Responses, the adapter calls `messages()` first, with a comment explaining the error order this preserves.
    - The adapter counts Decisions path images itself from `stage.getColumnTypes` and the schema, using the same helper that `createMessagesForRow` uses to pick usable attachments.
  - At minimum, share one usable-attachments helper between the count and message construction. Also move the Decisions branch of `messages` into its own method and reuse one empty-input message constant.

### Issue 3: The supported Decisions image formats are defined in four separate places
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: `OpenAIPrompt.scala` 313, 628-629, 634-635, 645, 649; `OpenAIAttachmentUtils.scala` 137-157, 315-318, 320-338, 431-442
- **Description**: This is a maintainability finding that should be fixed. The four sources agree today. The Decisions rule "JPEG, PNG, GIF and WebP" is enforced by:
  1. The Chat/Responses `imageExtensions` set (line 313), used through `mayBeImagePath` before a file is read.
  2. `MimeTypeExtensions` and `categorizeFileType`, used through `dataUrlFileType` for data URLs.
  3. An inline `Set("image/jpeg", "image/png", "image/gif", "image/webp")` checked after the read (line 649). It is rebuilt for every file.
  4. A hard-coded error message (line 629).

  Sources 1 and 2 are shared Chat/Responses tables, not Decisions-specific lists.
- **Risk**: Suppose Chat/Responses image support is broadened, for example by adding an AVIF or HEIC mapping:
  - Decisions data URLs and the before-read extension check would broaden silently.
  - Files of the same type would still pass that check and then be rejected after IO by source 3.
  - Data URLs and files would then behave differently, and the error message would be misleading.

  No code or test ties the four sources together.
- **Suggested Fix**: Define one Decisions image constant next to the other Decisions rules, for example a map from MIME type to allowed extensions. Derive all four checks from it: the before-read extension check, the declared-MIME check for data URLs, the after-read MIME check and the error message. At minimum, move the inline `Set` into a named `val` and compare data-URL MIME types against that same set.

### Issue 4: optional — In JSON mode, Decisions now accepts `columnTypes` entries that can never take effect
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: 248-260 (compare with 275-281); `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 119-122, 740
- **Description**: This is an optional design-consistency point. The behavior is a harmless no-op, not a defect.
  - `decisionsSettings` (line 249) now allows `columnTypes` in every Decisions configuration. The JSON-mode check (256-257) rejects only `"path"` entries.
  - So `inputMode="json"` combined with a text-only `columnTypes` such as `Map("c" -> "text")` is accepted and ignored. Before this patch, any `columnTypes` on Decisions failed.
  - Similar rules elsewhere reject unused settings:
    - `messagesCol` rejects any non-empty `columnTypes`, even text-only ones (275-281). The docs explain this at 119-122 as "message structs are already structured input".
    - This patch itself rejects `fileSizeLimitMB` when there is no path column (259-260).

    JSON mode is structured input in the same sense.
  - No test covers JSON mode with text-only `columnTypes`, and the docs table (line 740) doesn't describe the case.
- **Risk**: One unused setting is rejected while a similar one is silently accepted. Users may also assume text mappings affect how JSON input is parsed. The impact is low.
- **Suggested Fix**: Choose one:
  - Widen the check to `require(api != "decisions" || stage.getInputMode == "text" || stage.getColumnTypes.isEmpty, ...)`, which matches `validateMessageConstruction`.
  - Or document that text-only mappings are accepted and ignored in JSON mode, and add a test for it.

### Issue 5: `NativeQuestions` counts Decisions images in two different ways and reports the 128-image limit with two different messages
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 89-93, 118, 130-136, 151-175; `AIRequestAdapter.scala` 588-596
- **Description**: This is a maintainability finding that should be fixed; there is no demonstrated defect.
  - There are two definitions of "an image":
    - `validatePart` returns `1` for each validated `input_image`, and `validateDecisionsInput` sums these (130-136).
    - The new `imageCount` and `questionImageCount` (169-175) count raw parts whose `type` is `"input_image"`.
  - Several images are counted twice:
    - `decisions()` throws away the counts from `validatePart` (line 91) and counts again at line 118.
    - `prepareDecisionsRequest` validates the input, which counts its images, and then counts them again (153-157).
  - The same limit uses two messages: "Decisions accepts at most 128 images" (136) and "... per request" (118, 167).
  - For every row, the adapter walks the question images at least twice (589, and 157 → 166) and rebuilds the instruction parts (158-161). This happens even when the questions are a constant (`constantQuestions`) computed once per adapter.
- **Risk**:
  - If the accepted image-part shapes change in one counter but not the other, the limit check and validation will disagree.
  - The same limit violation produces different messages depending on which check fires first.
  - Work on constant questions is repeated for every row.
- **Suggested Fix**:
  - Use a single image counter: return image counts from question validation and from `validateDecisionsInput`.
  - Send every limit check through `validateDecisionsImageCount` with one message.
  - In `AIRequestAdapter`, precompute the prepared constant questions and their image count alongside `constantQuestions`.

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

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver disposition

All five suggestions are closed without implementation for this focused
compatibility fix. Original descriptions, severities (including Medium for
issue 1) and the `ISSUES_FOUND` verdict remain intact. None demonstrates a
current functional defect. These are explicit scope decisions, not claims that
the proposed refactors were applied or that the reviewer returned CLEAN.

1. **Protected attachment hooks:** duplicate of prior architecture feedback.
   The helpers run on subclass instances whose overridden supported API set
   includes Decisions; the Decisions branches are therefore intentional,
   reachable shared implementation, not dead base-class behavior. Current
   capability checks prevent unsupported API types reaching the string matches.
   Existing hooks do not require a separate hook for every added API branch.
   Four new extension points and policy moves across packages would expand the
   subclass contract without fixing current behavior. Retain the narrow reuse
   of the established reader/serializer; hypothetical future APIs need their
   own compatibility work regardless of dispatch organization.
2. **Counting helper and overload organization:** both filters currently select
   exactly trimmed nonempty cells, each yielding one image or a controlled
   error. Decisions explicitly selects deferred preparation; Chat/Responses
   explicitly select eager preparation. Both orders and combined pre-IO count
   boundaries are covered through the public stages. Moving schema/path
   selection into the request adapter or adding helpers is an organizational
   alternative, not a fix for a demonstrated count/order discrepancy.
3. **Format policy consolidation:** duplicate of earlier format-policy
   findings. Current extension, metadata, detected-MIME and diagnostic rules
   agree, as this reviewer independently confirms. AVIF/HEIC expansion is not
   in this patch and would require cross-API tests when introduced. Avoid adding
   a second extension taxonomy or parser policy solely for hypothetical drift.
4. **Text-only mappings in native JSON mode:** optional restriction not adopted.
   `AIRequestAdapter.scala:310-318` still validates mapping names, kinds and
   resolver ambiguity, so accepting a text tag does not bypass schema checks.
   A text tag does not request conversion or attachment loading; native JSON
   parsing is selected by inputMode. Path tags in JSON are explicitly rejected.
   The messagesCol construction gate is unchanged and need not imply a new
   blanket prohibition on harmless text tags for native input. No silent loss
   of a requested path transformation or wrong wire payload was demonstrated.
5. **Counters, diagnostics and constant-question caching:** duplicate optional
   refactoring/performance feedback. Raw counting follows part validation, so
   both definitions count the same accepted input_image shapes. Independent
   input and assembled-request validation deliberately preserve their checks;
   the 128 limit and 128/129 boundaries are correct. Both messages state that
   current limit. There is no measured regression supporting a caching or
   validator-return-contract change in this fix.

Verification: inspected the cited implementation, including capability/input
validation and the shared native/attachment validators. The complete final
patch has 290 unique applicable deterministic passes across four runs, including
fresh durable public-input and incomplete-resume coverage. Production and docs
are unchanged since the prior validation snapshots. No new tests, performance
measurements, Python runtime or provider E2E are claimed by this disposition.

Continue pass 18 on unchanged full patch
`1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22`.