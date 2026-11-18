---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 2
timestamp: "2026-11-18T07:16:46Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "62d2567ca6209616283d25f10686a03c7cbf9855044ed5cc296a16a5b8b139da"
---

I found three design issues in this PR: one Medium, two Low. None is a demonstrated runtime defect: current behavior matches the docs and the pinned tests. I didn't run any tests or builds; I relied on the driver's supplied results.

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-19-stage-attempt-1-review-2-decisions-paths-opus.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff coverage:** `.review-input-decisions-paths.diff` has exactly 13 `diff --git` headers, matching the prompt: AIPrompt, AIRequestAdapter, NativeQuestions, OpenAIAttachmentUtils, OpenAIPrompt, seven test suites, and `docs/Explore Algorithms/OpenAI/Unified AI prompting.md`. I checked attribution against the diff hunks, not only post-change source. Every `"decisions"` branch in `OpenAIPrompt.scala` is new (diff lines 349–421).
- [x] **Conventions:**
  - Trusted guidance: AGENTS.md and `.github/skills/scala-code/SKILL.md` ("Prefer small, focused changes and reuse existing helpers/traits").
  - `scalastyle-config.xml:3-4` sets `maxFileLength` to 800. `OpenAIPrompt.scala` is now exactly 800 lines: net +4 from 796 per the hunks, after deleting two comments and compressing `buildContentParts`.
- [x] **Existing extension pattern:** `OpenAIPrompt` customizes behavior through protected methods that `AIPrompt` overrides (`OpenAIPrompt.scala` 114/192/195/626 → `AIPrompt.scala` 217/225/227). `OpenAIPrompt`'s own `apiType` check (`OpenAIPrompt.scala:192-199`) rejects `"decisions"`.
- [x] **Request ordering:** traced `AIPrompt.scala:484-488` (lazy messages, overload choice), then `AIRequestAdapter.scala:615-636` (model, version and options evaluated first), then `nativeEnvelope` 584–600 (questions → pre-IO count → `messages()` → `prepareDecisionsRequest`). `AIPrompt.messages` (387–394) checks text before attachment IO. Chat/Responses still build messages eagerly at the call site.
- [x] **Setting validation:** `AIRequestAdapter.scala:249-268` covers the allowed Decisions settings, `inputMode="text"`, and the fileSizeLimitMB-requires-path ordering after `validateMessageConstruction`. Lines 312–317 restrict `columnTypes` to text/path; lines 327–334 resolve `inputCol` conflicts through `AIColumnPlan.matches`.
- [x] **NativeQuestions:**
  - Instructions accept text or parts.
  - `hostedImageUrl` parses with URI then `toURL.getHost`, accepts only http/https and ASCII-digit ports, and rejects empty hosts. `prepareParts` drops only null `file_id`/`detail`, so URLs are forwarded unchanged.
  - The 128-image checks are at lines 118, 136 and 166–167. Tests match only `contains("at most 128 images")` (`NativeQuestionsSuite.scala:138,141`; `AIPromptMediaInputSuite.scala:547,661`).
- [x] **Path formats:** path-column data URLs are classified through `imageExtensions` (`OpenAIPrompt.scala:313`), `MimeTypeExtensions` (`OpenAIAttachmentUtils.scala:137-141`), `categorizeFileType` (320–337) and `dataUrlFileType` (431–442). `image/bmp` is rejected today, consistent with docs lines 311–321.
- [x] **Upstream interaction:** `AIDefaults.scala` scopes Decisions to endpoint, apiKey, model and apiVersion only, so it doesn't interact with the `isSet(columnTypes/fileSizeLimitMB/inputMode)` checks. `AIDurableInput.scala` already enforces inline-only media for path columns and question images.
- [x] **No stale references:** no hand-written Python (`cognitive/src/main/python`) and no test or doc still references the removed Decisions rejections ("must be inline base64 image data URLs", "columnTypes is not supported by apiType='decisions'").
- [x] **Test suite sizes:** `AIPromptMediaInputSuite.scala` (788 lines) and `AIPromptPreflightSuite.scala` (789) are modified, not new, and remain under the cap.
- [ ] **Not executed by this reviewer:** tests, builds, ScalaStyle and codegen; review was read-only. I relied on the driver's evidence: 303 passes, 14 suites, 3 credential-gated cancellations, and ScalaStyle/compile/codegen passing. Python wrapper behavior and provider E2E were not independently verified.

## Issues

### Issue 1: Decisions attachment rules are hard-coded into the legacy `OpenAIPrompt` class instead of using its overridable methods
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 192–199, 313, 577–583, 615–616, 620–621, 628–629, 634–637, 645, 649–651; scalastyle-config.xml 3–4
- **Description**: This is a design finding, not a demonstrated runtime defect.
  - **Bypasses the existing pattern:** `OpenAIPrompt` only accepts chat_completions/responses (192–199), yet this PR adds `getApiType == "decisions"` checks throughout it:
    - `stringMessageWrapper` (578)
    - the single-user-message branch in `createMessagesForRow` (615–616)
    - a new `validateImageAttachment` (628–629) with three call sites (634–635, 645, 649)
    - two `"responses" | "decisions"` match arms (637, 651)

    The established way to specialize this class for `AIPrompt` is protected methods that `AIPrompt` overrides (`supportedApiTypes`, `hasExplicitPostProcessing`, `useBoundedAttachments`).
  - **Format rules split three ways:**
    - `NativeQuestions` validates explicit image parts.
    - A new literal `Set("image/jpeg","image/png","image/gif","image/webp")` in `OpenAIPrompt` (649) checks files after reading.
    - The data-URL and extension checks before reading (634–635, 645) reuse Chat/Responses' private `imageExtensions` (313).
  - **File-length cap reached:** the file is now exactly at ScalaStyle's 800-line maximum. The +4 net only fit after deleting two explanatory comments in `createMessagesForRow` and dropping `buildContentParts`'s explicit return type (620–621), which is unrelated churn in a shared class.
- **Risk**:
  - The next `OpenAIPrompt` change must first delete lines elsewhere or split the file.
  - Changing Decisions formats requires coordinated edits in two packages, and `OpenAIPrompt`'s own tests can never reach these branches.
  - If Chat/Responses ever adds an image type to `imageExtensions`/`MimeTypeExtensions`, Decisions would silently accept that type in path data URLs. It would also reject such filenames only after reading them, contradicting the documented "rejected without reading the file" behavior (docs 311–321).
- **Suggested Fix**:
  - Add a few protected methods in `OpenAIPrompt` whose defaults keep today's Chat/Responses behavior, and override them in `AIPrompt` for Decisions:
    - a wire-format selector that maps Decisions to the Responses format;
    - a `composeMessages(parts)` method (default `getPromptsForMessage(Left(parts))`);
    - an attachment check (default no-op).
  - Move the Decisions extension/MIME allowlist and error message into `NativeQuestions`, as one Decisions-owned source used by all three checks.
  - Restore the deleted comments and `buildContentParts` signature if the reduced line count allows.
  - If deferred to keep this fix focused, record the 800/800 file-length state as a known follow-up.

### Issue 2: Decisions request preparation is split across `AIPrompt` and `AIRequestAdapter`, with duplicated checks and a zero-default overload
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala
- **Line(s)**: AIPrompt.scala 387–394, 484–488; AIRequestAdapter.scala 537–545, 584–600, 612–617; OpenAIPrompt.scala 608–610
- **Description**: This is an abstraction-quality finding, not a demonstrated defect: the duplicated checks agree today and tests pin the 128/129 and no-IO cases. The documented guarantee that checks run before attachments load (docs 304–307) is assembled from separate pieces:
  - `prepareRequests` branches on `getApiType == "decisions"` to pick a `request` overload (484–488).
  - It computes the Decisions-only image count with `exists(_.trim.nonEmpty)`, a separate copy of `createMessagesForRow`'s filter `map(_.trim).filter(_.nonEmpty)` (OpenAIPrompt.scala 608–610).
  - The 3-argument `request` overload silently passes `pathImageCount = 0` (AIRequestAdapter.scala 612–613).
  - The null-or-empty text rule ("input is null or empty") now exists in both `AIPrompt.messages` (390–393, which also checks whitespace before IO) and `AIRequestAdapter.input` (537–545, checked later by `prepareDecisionsRequest`).
- **Risk**: Any of these changes would silently move failures from before attachment IO to after it:
  - a change to attachment filtering;
  - a change to empty-text handling;
  - a new Decisions caller using the 3-argument overload.

  Results stay correct because `prepareDecisionsRequest` re-checks after IO, but the documented pre-IO behavior would quietly break. The adapter's public methods also gain a Decisions-only parameter.
- **Suggested Fix**:
  - Have one helper (e.g., `usableAttachments(map, order)`) feed both `createMessagesForRow` and the pre-IO count.
  - Move the Decisions text rule into a single `NativeQuestions` helper: null/empty raises `AIMissingInputException`, then `validateDecisionsInput` runs. Call it from both sites.
  - Optionally merge into one `request` that takes a deferred messages function and runs it immediately for Chat/Responses. This removes the call-site branch and the zero default while keeping their eager ordering.

### Issue 3: The 128-image limit has two different error messages and two separate counters
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 121–138, 151–163, 165–175, 215–242
- **Description**:
  - **Inconsistent messages:** the existing input check says "Decisions accepts at most 128 images" (136). The new question check (118) and combined check (165–167) say "... per request". The literal "128" is repeated in all three messages instead of using `MaxImages` (15).
  - **Double counting:** input images are counted by summing `validatePart` results (132–135), then counted again by `imageCount` (172–175) over the same input in `prepareDecisionsRequest` (152–156). `AIPrompt` keeps a third count (Issue 2).
- **Risk**:
  - Which message users see for one documented combined limit (docs 259–260) depends on which part overflowed.
  - Changing the limit or the image-part format means updating several literals and counters that must stay in agreement.
- **Suggested Fix**:
  - Use one message built from `MaxImages`, e.g. `s"Decisions accepts at most $MaxImages images per request"`, in all three checks.
  - Add an internal function that validates and returns the input image count, and use it in both `validateDecisionsInput` and `prepareDecisionsRequest` instead of re-counting.
  - Existing `contains("at most 128 images")` test assertions still match.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage (fix now, or defer as a follow-up for this focused compatibility change)
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver disposition

All three recommendations are closed without implementation for this focused
compatibility fix. The original Medium/Low severities and `ISSUES_FOUND` verdict
are preserved. The reviewer explicitly found no demonstrated runtime defect;
the design trade-offs below are acknowledged, not represented as fixed.

1. **Attachment specialization:** the inherited shared reader is intentionally
   reused instead of introducing additional hooks and protocol ownership changes.
   The current extension set is jpg/jpeg/png/gif/webp and the detected MIME check
   names the corresponding four formats. They agree today. No new image format
   is being added, and any future format expansion must revisit both checks and
   native serialization. The by-name gate leaves Chat/Responses predicates
   unevaluated. Public-stage coverage exercises the inherited Decisions branches.
   **Known maintenance constraint:** `OpenAIPrompt.scala` is 800/800 lines.
   A future change needing more space should consider extracting focused shared
   attachment behavior rather than removing unrelated logic. That extraction,
   comment/signature restoration and allowlist consolidation are not part of this
   patch and are not claimed completed.
2. **Preparation boundaries:** the two overloads intentionally preserve eager
   argument evaluation for Chat/Responses while deferring Decisions attachment
   IO. Current path filtering and counting use the same trimmed/nonempty rule;
   the Decisions call site supplies its count explicitly. Preflight and final
   protocol validation serve different boundaries. Factoring these rules could
   simplify future maintenance, but no current mismatch or failing ordering
   contract was shown. The public pre-IO, blank-cell and 128/129 regressions
   passed, as did the preserved Chat/Responses ordering tests.
3. **Counts and messages:** both message forms accurately describe the current
   128-image restriction, and the final check enforces input plus questions.
   Earlier checks preserve direct-validator rejection and avoid attachment IO.
   The additional content-part counts do not rescan image bytes; no measured
   performance regression was established. Consolidating wording and returned
   counts is optional maintenance, not a correctness fix for the current limit.

The driver inspected the cited attachment, overload and counting source and
confirmed the 800-line state. Applicable evidence remains the fresh 303-pass
integration run with all required local Scala gates and codegen passing.
No reviewed content changed. This permits the next lens; it is not three
implemented fixes, a reviewer CLEAN result, or final CI qualification.