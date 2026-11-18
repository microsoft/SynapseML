---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 2
timestamp: "2026-11-17T23:26:35Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "bee21b5749534e3ca9fc0b7dbc2c98912dbf222f52de0a9df5f888229e2f6cf1"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-16-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Conventions read.** The supplied AGENTS.md rules (Scala-first behavior, stable public JVM signatures and param shapes, codegen and persistence checks) and `.github/skills/scala-code/SKILL.md` (follow nearby patterns, reuse helpers and traits, keep changes focused).
- [x] **Subclass-hook precedent.**
  - `OpenAIPrompt.scala:190-192`: `supportedApiTypes` cannot select `decisions`.
  - `OpenAIPrompt.scala:623-626`: the protected `useBoundedAttachments` hook.
  - `AIPrompt.scala:184, 208, 216, 218`: existing protected-hook overrides.
  - This is the baseline for Issue 1.
- [x] **Generated-attachment flow.**
  - `OpenAIPrompt.scala:577-583, 602-658`.
  - `OpenAIAttachmentUtils.scala:121-123` (RIFF/WebP constants), `:315-338` (`mayBeImagePath`, `categorizeFileType`), `:428-442` (`dataUrlFileType`).
  - The by-name `validateImageAttachment` at `OpenAIPrompt.scala:628-629` evaluates its predicate only for `decisions`, so Chat/Responses never run the Decisions prechecks.
- [x] **Facade and adapter ordering.**
  - `AIPrompt.scala:364-386`: the Decisions text precheck runs before `generatedMessages`.
  - `AIPrompt.scala:471-495`: lazy thunk, path precount, and the same try/catch error mapping.
  - `AIRequestAdapter.scala:582-598` runs in this order: questions, image count, `messages()`, `prepareDecisionsRequest`.
  - The overloads are at `AIRequestAdapter.scala:610-632`. `input()` at `:535-543` holds the duplicated "input is null or empty" message.
- [x] **Capability and schema rules.**
  - `AIRequestAdapter.scala:247-265`. `validateMessageConstruction` (`:275-281`) runs before the Decisions `fileSizeLimitMB` rule, so a messagesCol conflict reports its own error.
  - `isSet` gating means any `fileSizeLimitMB` default (read by `attachmentSizeLimit`, `OpenAIPrompt.scala:623-624`) cannot trigger the "requires a path column" rule. `fileSizeLimitMB` is not a `GlobalKey`.
  - `validateDecisionsInputColumn` (`:316, 325-332`) uses `AIColumnPlan.matches`, which respects Spark's resolver.
- [x] **Wire-validation reuse in `NativeQuestions.scala`.**
  - Array instructions go through the existing `validatePart`.
  - `prepareParts` is extracted once and reused for input and questions.
  - `inlineImageHeader` (`:191-201`) stays subtype-agnostic for explicit parts.
  - `hostedImageUrl` (`:203-213`) requires HTTP(S) and a non-empty host, and rejects on any parse failure.
  - The stale "inline-only common subset" comment was removed.
- [x] **Durable output.** `AIDurableInput.scala` is unchanged. Its existing content walk covers question-instruction `image_url` values and path columns, so inline-only media semantics hold without new code.
- [x] **API surface.** No new Params and no public JVM signature changes (`AIRequestAdapter` is `private[ai]`, `NativeQuestions` is `private[services]`). No generated-wrapper or persisted-shape change. No hand-written Python under `cognitive/src/main/python` or `cognitive/src/test/python` checks Decisions `columnTypes`/`fileSizeLimitMB` capability.
- [x] **Docs.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` has a Decisions path example at ~273-279 (promptTemplate plus a `columnTypes` path plus `fileSizeLimitMB`). The capability table and the messagesCol/fileSizeLimitMB sections match the code. The `columnTypes`/`fileSizeLimitMB` param docs (`OpenAIPrompt.scala:241-271`) remain accurate.
- [x] **Callers of the 3-arg `request` overload.** Tests: `AIPromptSuite.scala:153, 270, 277, 289` and `AIPromptPreflightSuite.scala:181`. Production: only the non-Decisions branch at `AIPrompt.scala:478`.
- [x] **Diff coverage.** The supplied diff was cut off inside the `OpenAIAttachmentUtils.scala` hunk. I read the rest of the changed production and docs content directly from the current sources. The thirteen changed files cover 5 production, 1 doc and 7 test files.
- [x] **Validation.** I did not run any commands or tests. Validation evidence comes from the driver: 58 targeted tests passed in a fresh run, plus 228 unchanged tests from the earlier full run. This is not one 286-test run.
- [ ] **Artifact pre-existence not checked.** This plan forbids listing `reviews/`; the runner owns writing the artifact.

## Issues

### Issue 1: Decisions-only logic sits in the legacy `OpenAIPrompt` base class instead of behind a subclass hook
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 578, 615, 628-629, 634-637, 645, 649-651 (hook precedent at 623-626; AIPrompt.scala:218)
- **Description**: This is a maintainability finding, not a demonstrated defect. `OpenAIPrompt` cannot select `decisions` (line 192), yet the diff adds five `getApiType == "decisions"` branches to it:
  - the `input_text` wrapper choice (578);
  - the user-only message shape (615);
  - a Decisions format gate with a by-name predicate (628-629, called at 634-635, 645 and 649);
  - two `"responses" | "decisions"` wire arms (637, 651).

  The class already has a pattern for AIPrompt-only attachment behavior: the protected `useBoundedAttachments` hook (626), overridden at AIPrompt.scala:218 alongside other hook overrides (184, 208, 216). Because that pattern was not used, Decisions request rules are now spread across two packages and four files:
  - OpenAIPrompt: part type, message shape and format gate.
  - AIPrompt: text precheck, image precount and laziness.
  - AIRequestAdapter: capability rules.
  - NativeQuestions: wire validation.

  The by-name parameter exists only to keep a subclass concern out of the base class's own API paths.
- **Risk**: Chat/Responses behavior is pinned by tests today. However, the legacy class now encodes an API it can never run. Future Decisions changes must edit the `openai` package, and Chat/Responses maintainers must reason about unreachable branches in `wrapFileToMessagesList`. A second native API with attachments would add more string-literal branches instead of reusing hooks.
- **Suggested Fix**: Keep the behavior, but move these decision points into narrow protected hooks next to `useBoundedAttachments`. Give each hook a default that matches today's Chat/Responses behavior and override them in `AIPrompt` for Decisions. Examples:
  - `protected def attachmentWireApi: String = getApiType` (AIPrompt maps `decisions` to `responses`);
  - `protected def generatedUserMessageOnly: Boolean = false`;
  - `protected def precheckAttachment(path: String): Unit = ()`;
  - `protected def validateLoadedAttachment(mimeType: String): Unit = ()`.

  Expose the extension sets as `protected` if the override needs them. This is a local refactor, not a redesign, and the existing media and preflight suites cover it.

### Issue 2: The Decisions image-format allowlist is defined in several separate places
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 313, 628-629, 634-635, 645, 649; related OpenAIAttachmentUtils.scala 315-318, 320-338, 431-442
- **Description**: The rule "Decisions path images must be JPEG, PNG, GIF or WebP" is written four times:
  - (a) Data URLs: `dataUrlFileType(..., imageExtensions, ...) == "image"` (634-635). This derives from the shared Chat/Responses `imageExtensions` (313) and from `MimeTypeExtensions`/`extensionForMimeType`.
  - (b) The pre-IO path gate: `mayBeImagePath(path, imageExtensions)` (645), which also uses the shared set.
  - (c) The post-read gate: the inline literal `Set("image/jpeg", "image/png", "image/gif", "image/webp")` (649).
  - (d) The diagnostic message text (629).

  These checks are not interchangeable. `categorizeFileType` accepts any `image/*` MIME type behind an allowed extension, so (c) is still needed. Helper visibility is also inconsistent: `mayBeImagePath` is `private[openai]`, while `dataUrlFileType` is visible to all of `services`.
- **Risk**: All four agree today. If Chat/Responses later add formats to `imageExtensions`/`MimeTypeExtensions` (for example AVIF or HEIC), Decisions checks (a) and (b) would silently accept them while (c) still rejects them. Decisions would then read or download files, up to the size limit, only to reject them. That breaks the promise that known unsupported extensions fail before IO, and data-URL and path inputs would disagree about the same format. No compile error or Decisions-specific test would catch this, because the change would be made for a different API.
- **Suggested Fix**: Define one Decisions constant, for example `DecisionsImageMimeTypes` with extensions derived through `MimeTypeExtensions`, in `OpenAIAttachmentUtils` or in the Issue 1 hook. Use it for the data-URL MIME check, the pre-IO extension check, the post-read MIME check and the diagnostic text. Leave the shared Chat/Responses `imageExtensions` independent. The existing Decisions format tests already pin the observable behavior.

### Issue 3: The Decisions "validate before IO" ordering is split between the caller and two adapter `request` overloads
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: AIRequestAdapter.scala 535-543, 582-598, 610-632; AIPrompt.scala 377-384, 474-478; OpenAIPrompt.scala 609-611
- **Description**: Decisions rows fail before image IO only because `AIPrompt.prepareRequests` checks `getApiType == "decisions"`, picks the thunk overload and computes `pathImageCount` itself (AIPrompt.scala:476). Two pieces of logic are duplicated to make this work:
  - The image precount re-implements the "usable path" predicate from `createMessagesForRow` (OpenAIPrompt.scala:609-611).
  - `AIPrompt.messages` (377-384) re-implements the adapter's text resolution and its "input is null or empty" failure (AIRequestAdapter.scala:535-543).

  The 3-arg overload (610-611) still compiles for Decisions, but it forces attachment IO before the model, version, options, questions and count checks, and it passes a count of 0. Its other callers are tests and the non-Decisions production branch.
- **Risk**: Behavior is correct today, because `prepareDecisionsRequest` recounts images after preparation, so the 128-image limit and the wire shape are still enforced. But the adapter API does not encode the ordering. A refactor of `prepareRequests`, or a new caller, that uses the 3-arg overload for Decisions would silently move file/HTTP reads ahead of the cheap row checks. The duplicated predicate and message can also drift, which would change which error is reported first and how much IO happens, though not wire correctness.
- **Suggested Fix**:
  - Make the thunk overload the only production entry point and let the adapter decide eagerness. For non-Decisions APIs it should call `messages()` first, which preserves Chat/Responses error precedence.
  - Compute the path count inside the adapter from the same column plan.
  - Extract one shared "usable attachment" helper (trimmed, non-empty) for `createMessagesForRow` and the precount.
  - Remove the 3-arg overload, or keep it only as a test convenience, and update the five test call sites.

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

## Driver disposition

The original findings and `ISSUES_FOUND` verdict above are preserved. No
reviewed source, test, or documentation content changed during this triage.
These three architectural recommendations are not adopted for this focused
bug fix; this is a driver disposition, not a claim that the reviewer returned
a clean verdict or that the suggested refactors were implemented.

### Issue 1: Closed without changes; optional extension-point refactor

- **What/why:** Keep the existing shared attachment implementation. Its
  inherited helpers execute on `AIPrompt`, whose runtime `getApiType` can be
  Decisions even though the legacy stage itself cannot select it. The explicit
  Decisions guards therefore are reachable and intentional, not dead code.
  Adding four protected hooks would expand the extension surface to prepare
  for a hypothetical second native API, without correcting an observed defect.
- **Evidence:** `OpenAIPrompt.scala:577-658` guards the message shape and image
  prechecks by runtime API; the by-name predicate leaves Chat/Responses checks
  unchanged. `AIPrompt.scala:364-386,471-495` uses those inherited helpers and
  preserves eager message construction for non-Decisions APIs. The reviewer
  also explicitly classified this as maintainability, not a demonstrated defect.
- **Verification:** Existing deterministic media, attachment, adapter and
  recovery coverage remains applicable; the latest media-suite run passed all
  26 tests. No additional behavior change or test run is warranted by this
  declined refactor alone.

### Issue 2: Closed without changes; future-format drift recommendation

- **What/why:** The pre-read extension check, data-URL classification, and
  post-read MIME validation serve different trust boundaries. All currently
  agree on JPEG, PNG, GIF and WebP. The reported failure requires a later,
  hypothetical expansion of the shared format set; no such expansion is in
  this patch. A future format addition must update and validate the affected
  APIs, rather than being assumed compatible. Consolidation is not required
  to deliver the current contract.
- **Evidence:** `OpenAIPrompt.scala:313,628-651` uses exactly
  `jpg/jpeg/png/gif/webp` and the four corresponding MIME types.
  `OpenAIAttachmentUtils.scala:137-157,315-338,431-442` maps the same four image
  MIME types. None of the alleged divergent formats is admitted today.
- **Verification:** `AIPromptMediaInputSuite.scala:413-503` covers unsupported
  MIME types, misleading filenames, missing non-image paths, unknown `.ashx`
  extensions and bounded/unbounded rejection before IO. Those tests passed in
  the fresh 58-test run. The review's future-change caution is retained here;
  it is not misrepresented as a fixed present-day bug.

### Issue 3: Closed without changes; optional adapter-interface refactor

- **What/why:** Retain the explicit Decisions-only lazy overload selection,
  which deliberately preserves other APIs' established error/IO precedence.
  The current production caller passes the precount and thunk; the by-value
  overload is used by the non-Decisions branch and adapter tests. Removing it
  and relocating counting would enlarge this patch without fixing a current
  call path. The identical trimmed/non-empty predicates are small, and the
  authoritative post-construction count independently enforces the wire limit.
- **Evidence:** `AIPrompt.scala:474-478` chooses the lazy overload for Decisions.
  `AIRequestAdapter.scala:582-598,610-632` validates row questions and the
  precount before forcing messages, then validates the complete input again.
  `OpenAIPrompt.scala:609-611` uses the same usable-path predicate.
- **Verification:** `AIPromptMediaInputSuite.scala:395-409,505-553` asserts zero
  attachment/inference requests for invalid prompt input and excessive image
  totals, including multiple questions, two path columns and blank paths.
  These assertions passed in the latest media-suite run. No current ordering
  violation was identified.

**Disposition outcome:** All three recommendations have an explicit,
source-backed no-change disposition. The frozen product patch remains
`05a6a91f7184b4b33639b785368163082b8e4e00272a6f2d34e6beb7c7924dee`;
continue this same pass without resetting counters or hiding the findings.