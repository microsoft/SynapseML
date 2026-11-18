---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 2
timestamp: "2026-11-17T16:01:06Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "7b4500d5e45c5ba28b30dcad77a8cccb9e433398dec0b986b2d5707ed7b9df75"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-13-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Repository guidance and style limits.** I read `AGENTS.md`, `.github/skills/scala-code/SKILL.md` and `.github/skills/code-review/SKILL.md`. They ask contributors to follow nearby patterns, reuse helpers, and keep lines to 120 characters and files to 800 lines. I also read `scalastyle-config.xml`: `FileLengthChecker` maxFileLength is 800, line length is 120, method length is 60, methods per type is 50, and `MagicNumber` ignores -1 through 4.
- [x] **Hook precedent and file size in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`.**
  - `supportedApiTypes` lists only chat_completions and responses (192–199).
  - The existing ways a subclass changes behavior are protected hooks: `hasExplicitPostProcessing` (114), `useBoundedAttachments` (626) and `supportedApiTypes`/`defaultApiType`.
  - The patch adds Decisions branches at lines 578, 615, 628, 637 and 651.
  - The file now has exactly 800 lines. `number.of.methods` is already suppressed at lines 33 and 800.
- [x] **Public API and persistence are unchanged.**
  - No Param or setter is added or changed.
  - `AIRequestAdapter` is `private[ai]`; `NativeQuestions` and `OpenAIAttachmentUtils` are `private[services]`.
  - The new or changed `OpenAIPrompt` helpers are private or `private[services]`.
  - The 3-arg `request` overload is kept: production caller at `AIPrompt.scala:478`, 4-arg caller at 477, plus test callers.
  - `cognitive/src/main/python` has no hand-written `AIPrompt` wrapper, so generated wrappers are unaffected.
- [x] **Request ordering in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`.**
  - `request` resolves model, version and options before `nativeEnvelope` runs (571–587).
  - `nativeEnvelope` checks questions, then the path image count, then forces `messages()` (file IO), then calls `prepareDecisionsRequest`.
  - In `AIPrompt.scala` 474–478, the `lazy val` is declared inside `try`, so the catch block can't trigger IO again. Other APIs still build messages eagerly.
- [x] **Capability rules match the docs.** `validateCapabilities` (247–264) allows `columnTypes` for Decisions. It allows `fileSizeLimitMB` only with path columns, and path columns require `inputMode="text"`. This matches `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (262–347, capability table 727–729). System One still rejects `columnTypes`.
- [x] **Durable mode needs no changes.** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala` (81–98, 150–176) already requires inline images for path columns and question `image_url` values.
- [x] **The separate hosted-URL check is justified.** `NativeQuestions.hostedImageUrl` (203–213) is intentionally separate from the endpoint check in `AIRequestAdapter.scala` (39–49), which rejects underscore hosts, user info and fragments. Reusing the endpoint check would break the documented acceptance of underscore, IDN and user-info hosts.
- [x] **Shared WebP fix.** `OpenAIAttachmentUtils.inferMimeType` (295–308) checks the RIFF format tag with named constants before the JDK guesses the type. Other RIFF files, including WAV, still go to the JDK, so WAV stays audio.
- [x] **Tests follow existing conventions.**
  - They reuse `AIRecordingServer`, `AIPromptTestBase`, `AIPromptOutputTestBase` and `Using.resource`.
  - They cover both public stages, copy and save/load, and assert that `transformSchema` matches the runtime schema (for example `NativeQuestionsSuite.scala` 152–172).
  - `AIPromptMediaInputSuite.scala` has 708 lines.
- [x] **Nothing was executed.** This review is read-only. I considered the driver's validation evidence (128 targeted tests, ScalaStyle, compile, test compile, codegen) but did not re-run it.
- [x] **Coverage limitation.** The diff in this prompt is truncated after the `inferMimeType` hunk. I reviewed the later hunks from the staged source files at the cited lines: the `OpenAIAttachmentUtils` helpers, `OpenAIPrompt`, the docs and the test suites. I did not read `reviews/**`, `.git/**` or sibling artifacts.

## Issues

### Issue 1: The legacy `OpenAIPrompt` base class now contains Decisions-only rules
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 577–583, 614–616, 620–621, 628–629, 634–651 (file length 800)
- **Description**: The patch adds five `getApiType == "decisions"` checks to `OpenAIPrompt`:
  - `stringMessageWrapper` (578);
  - a user-only message in `createMessagesForRow` (615);
  - a new image-format check, `validateImageAttachment`, with its error message (628–629);
  - both request-shape `match` statements in `wrapFileToMessagesList` (637, 651).

  `OpenAIPrompt` itself rejects `decisions` in `supportedApiTypes`, so the base class now holds rules that only its `ai` subclass can reach. Until now, `AIPrompt` changed base behavior only through protected hooks such as `useBoundedAttachments`. AGENTS.md says to follow nearby patterns before adding a new one.

  Decisions rules are now split across two packages:
  - `ai.NativeQuestions` and `ai.AIRequestAdapter` own the part, question, count and capability rules.
  - `openai.OpenAIPrompt` owns the image-format allowlist and the message shape.

  Chat/Responses stay unaffected only because `isImage` is a by-name parameter combined with a `&&` short-circuit. If someone makes that argument eager, `dataUrlFileType` would also run for Chat/Responses, and it throws on malformed data URLs.

  The file now has exactly 800 lines, the `maxFileLength` limit. The same hunks remove two explanatory comments and the explicit return type of `buildContentParts`, and add another method under the existing `number.of.methods` suppression.
- **Risk**: There is no functional defect today. However:
  - The next unrelated line added to `OpenAIPrompt` will fail ScalaStyle.
  - Future Decisions changes will need edits in two packages.
  - The legacy public stage carries behavior that its own API-type validation rejects.
  - A refactor of the lazy check could change Chat/Responses error behavior.
- **Suggested Fix**: Follow the `useBoundedAttachments` pattern with a few small hooks. This is a targeted move of existing code, not a wider redesign.
  - Add a `protected def attachmentWireApi: String = getApiType` hook. Override it in `AIPrompt` so `decisions` maps to `responses`, and use it in `stringMessageWrapper` and both `match` statements.
  - Add no-op hooks such as `protected def validateAttachmentPath(path: String): Unit = ()` (before reading) and `validateAttachmentMime(mime: String): Unit = ()` (after classification). Override them in `AIPrompt` so the Decisions allowlist and its message live in the `ai` package.
  - Have `AIPrompt.generatedMessages` (`AIPrompt.scala` 353–362) keep only the `user` message for Decisions, instead of branching in `createMessagesForRow`.

  This frees lines under the file-length limit and makes it explicit that the checks do nothing for Chat/Responses.

### Issue 2: The Decisions image-format allowlist is written four ways and depends on the shared Chat/Responses sets
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala
- **Line(s)**: OpenAIPrompt.scala 313, 629, 634–635, 645, 649; OpenAIAttachmentUtils.scala 315–318, 431–442
- **Description**: The allowlist appears in four places:
  - (a) the before-read `mayBeImagePath(path, imageExtensions)` check (645), which uses the `imageExtensions` set shared with Chat/Responses (313);
  - (b) the inline data-URL check `dataUrlFileType(...) == "image"` (634–635), which depends on the shared `MimeTypeExtensions` and `imageExtensions`;
  - (c) a hard-coded after-read set, `Set("image/jpeg", "image/png", "image/gif", "image/webp")` (649), which inline data URLs never reach;
  - (d) the error message text (629).

  They agree only because the shared maps currently cover the same four formats. The visibility is also inconsistent: the new `dataUrlFileType` has no access modifier, so it is visible across `services`, while its sibling `mayBeImagePath` is `private[openai]`. Both have a single caller in `OpenAIPrompt`.
- **Risk**: Suppose a later Chat/Responses change adds BMP to `imageExtensions` and `MimeTypeExtensions`. Decisions would then accept inline `data:image/bmp` path cells with no Decisions change. `.bmp` files would pass the before-read check, be read, and only then be rejected. That breaks the documented "JPEG, PNG, GIF and WebP" contract, makes inline and file inputs behave differently, and loses the reject-before-reading guarantee.
- **Suggested Fix**: Define one Decisions-owned constant, such as `DecisionsImageMimeTypes`, next to `NativeQuestions` or in the hook from Issue 1. Derive these from it:
  - the before-read extension set (the MIME-to-extension mapping plus `jpeg`);
  - the declared MIME check for data URLs;
  - the error message.

  Replace `dataUrlFileType` with a `private[openai]` helper that returns the declared MIME type, so the Decisions check tests MIME membership instead of a shared category.

### Issue 3: The image-path route repeats the adapter's input rules and the usable-attachment check
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: AIPrompt.scala 377–383, 474–478; AIRequestAdapter.scala 524–532, 599–600; OpenAIPrompt.scala 608–610
- **Description**: The patch duplicates logic that already exists elsewhere:
  - **Prompt-text rules:** `AIPrompt.messages` re-implements the adapter's Decisions text rules inside the shared Chat/Responses branch, using three inline `getApiType == "decisions"` checks. These cover the non-empty filter, the literal `"input is null or empty"` (also at `AIRequestAdapter.scala:530`), and `validateDecisionsInput` for whitespace-only text. Prompts without image paths enforce the same rules through `AIRequestAdapter.input` and `prepareDecisionsInput`.
  - **Usable-attachment check:** `prepareRequests` counts usable images with `exists(_.trim.nonEmpty)` (476). `createMessagesForRow` decides which attachments to load with `map(_.trim).filter(_.nonEmpty)` (`OpenAIPrompt.scala` 609).
  - **Image count:** the count reaches the adapter as a positional `pathImageCount` argument. The 3-arg overload silently passes `0`.
- **Risk**: Decisions prompts with and without image paths must return the same `missing_input`/`invalid_request` errors, which this PR explicitly preserves, but the two code paths can now drift apart. If the two attachment checks or the overload diverge, the count check that runs before IO stops working without any error. `prepareDecisionsRequest` still enforces the 128-image limit after reading, so what would be lost is the documented "fail before reading images" ordering.
- **Suggested Fix**:
  - Extract one helper for the Decisions null/empty text rule, for example on the `AIRequestAdapter` companion, and call it from both `input` and `AIPrompt.messages`.
  - Give the Decisions image-path case its own branch instead of sharing the Chat/Responses `else if`.
  - Use one usable-attachment helper (for example `private[services]` in `OpenAIPrompt`) for both message building and counting. Alternatively, have the adapter compute the count from `stage.getColumnTypes`, so callers can't omit it.

### Issue 4: The 128-image limit is checked twice and reported with different hard-coded messages
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 136, 165–167
- **Description**: The new question-only check at line 118 is the same as calling the new `validateDecisionsImageCount(0, questions)` (165–167). Both new messages write `128` directly instead of using `MaxImages` (15). The existing input-only check (136) reports the same provider limit with different wording ("Decisions accepts at most 128 images", without "per request"). Which message a user sees depends on where the extra images are.
- **Risk**: Changing the limit means updating the constant and three string literals. Users and tests see two different messages for one rule.
- **Suggested Fix**: Make line 118 call `validateDecisionsImageCount(0, questions)`, and build one message from `MaxImages`. If the input-only wording at line 136 isn't a compatibility contract, switch it to the same message.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not adopted; non-blocking architectural alternative.
- **What changed**: No product change. Retain the shared builder and existing
  subclass opt-in, without adding protected extension points.
- **Why**: The legacy stage still rejects Decisions, while `AIPrompt` explicitly
  supports it. The proposed hooks are a valid alternative, but expand the
  extension surface and redistribute working behavior without fixing a current
  defect. The by-name predicate intentionally preserves other APIs' behavior.
  Exactly 800 lines passes the configured limit; future edits must still pass
  style checks. This repeats the earlier assessed hook proposal.
- **How verified**: Rechecked the supported-API gates and before/after source
  fingerprint. Public-stage, malformed-input and WebP/WAV compatibility tests
  cover the relevant branches. Root main/test ScalaStyle passes.

### Issue 2
- **Status**: Not adopted; future extensibility observation, not current drift.
- **What changed**: No product change. Preserve the shared classifier and the
  Decisions-only exact post-read MIME restriction.
- **Why**: The sets agree for every currently supported format. Adding BMP to
  shared maps would not silently change Decisions: the public-stage regression
  explicitly rejects inline BMP/TIFF/X-bitmap and non-image file paths, with
  exact diagnostics and zero inference requests. Such a future change must
  update or preserve those tests. A second derived MIME/extension policy and
  narrower package visibility are optional maintenance choices for this scope.
- **How verified**: Inspected `AIPromptMediaInputSuite`'s invalid-path cases,
  the pre-IO extension check and post-read MIME check. Both Decisions stages
  pass those assertions in the current 128-test run.

### Issue 3
- **Status**: Not adopted; retain tested entry-point and ordering behavior.
- **What changed**: No product change or new request-builder abstraction.
- **Why**: Ordinary native text and generated attachment messages enter through
  different existing paths. Both delegate whitespace semantics to
  `NativeQuestions`; the generated path must reject missing text before IO.
  The pre-IO count deliberately uses the same nonnull/trimmed-nonempty predicate
  as the loader, and each eligible path produces exactly one image or fails.
  The three-argument overload preserves callers that already supply messages;
  the production Decisions path uses the four-argument overload.
- **How verified**: Rechecked both adapter overloads and their production call
  sites. Tests cover prompt binding, missing/empty/whitespace text, row metadata,
  skipped empty attachments, the combined image boundary, and zero IO on
  rejected rows. The final serialized request has its own count check.

### Issue 4
- **Status**: Not adopted; non-blocking message/deduplication cleanup.
- **What changed**: No product change. Preserve the existing input-only error
  wording and the request-wide wording for shared-input/question limits.
- **Why**: Every numeric comparison already uses `MaxImages`; no independent
  enforcement constant was introduced. The repeated checks protect standalone
  question/input validation, pre-IO rejection and the final serialized request.
  Factoring the question-only check or interpolating the unchanged 128 into
  messages is reasonable cleanup, but does not correct current behavior.
- **How verified**: Read `decisions`, `validateDecisionsInput`,
  `prepareDecisionsRequest` and `validateDecisionsImageCount`. All compare with
  the same constant, and the 128/129 and combined-count boundary tests pass.

## Driver disposition

The original `ISSUES_FOUND` verdict is preserved. These four suggestions have
explicit non-blocking dispositions, not a substituted reviewer `CLEAN` result.
No reviewed source changed; the driver will continue the remaining independent
lenses on patch
`7c09664fd9bcc7c99f3cf510e66758ecbf2ab6bd092c616ddfbbed63161bd9e0`.
All ten staged blobs and the target were rechecked. The full binary diff was
transported and apply-verified; the review's context-visibility limitation
remains recorded above.