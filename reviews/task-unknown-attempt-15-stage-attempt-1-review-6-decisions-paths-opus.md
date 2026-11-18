---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 6
timestamp: "2026-11-17T22:48:12Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9d9858d5fcfbac5b0b6d00dfbfe8eb18b6715e52dbd3a110c3933e10477b50fc"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-15-stage-attempt-1-review-6-decisions-paths-opus.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Re-read the changed production paths from the worktree:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 364–386 and 453–498
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` 247–265, 325–332 and 582–634
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` 1–246
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` 596–660
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` 159–350 and 425–512
- [x] Performance, lazy preparation: `lazy val inputMessages` (AIPrompt.scala 474) runs at most once per row.
  - For Decisions, only `nativeEnvelope` forces it (AIRequestAdapter.scala 590).
  - Other APIs still prepare messages eagerly through the overload argument (AIPrompt.scala 478).
  - The catch blocks (482–494) never touch it.
  - Retained messages are GenerativeApis-only, so line 481 never forces it for Decisions. No IO runs twice.
- [x] Pre-IO ordering matches the docs: `selectedModel`, then `requestVersion`, then `mergedOptions`, then questions parse/validate, then `validateDecisionsImageCount` (AIRequestAdapter.scala 589), then text checks (AIPrompt.scala 378–383), then `generatedMessages` IO (384), then `prepareDecisionsRequest`.
  - Headers are validated in `buildRequest` (AIPrompt.scala 388–401), after IO.
  - This matches `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 304–308.
- [x] AIPrompt is the only production caller of `adapter.request`. The eager overload passes `pathImageCount = 0` (AIRequestAdapter.scala 610–611), and Decisions always uses the lazy overload (AIPrompt.scala 475–477).
- [x] The WebP fix costs two 4-byte ASCII decodes per read file (OpenAIAttachmentUtils.scala 295–308). WAV and the 11-byte RIFF fixture stay audio (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` 335–371).
- [x] The by-name predicate keeps format prechecks Decisions-only (OpenAIPrompt.scala 628). The extension check (`mayBeImagePath`, OpenAIAttachmentUtils.scala 315–318) runs before `prepareFile` (OpenAIPrompt.scala 645–648), so unsupported extensions are rejected without IO.
- [x] Checked the Decisions documentation (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 245–329, 737–738, 752–766) against the code:
  - hosted URL and inline image validation (NativeQuestions.scala 191–241)
  - `.jfif`/`.jpe` and `image/jpg` rejection (OpenAIPrompt.scala 313; OpenAIAttachmentUtils.scala 320–338)
  - the combined 128-image limit and the `missing_input`/`invalid_request` split
  - the capability-table rows and the bounded-read claims
- [x] No stale docs:
  - Searched `cognitive/src/main` and `docs/Explore Algorithms/OpenAI` for inline-only or text-only claims and found none outdated.
  - The `columnTypes` and `fileSizeLimitMB` param docs (OpenAIPrompt.scala 241–243, 269–271), `messagesCol` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` 25–36) and the OpenAIDecisions scaladoc (line 14) remain accurate.
  - The removed "inline-image common subset" comment was correctly dropped.
- [x] The new preflight messages are specific (AIRequestAdapter.scala 256–260, 325–332). "input is null or empty" reuses the existing text-only Decisions message (AIRequestAdapter.scala 541).
- [x] For comparison with Issue 1, Chat and Responses diagnostics include the MIME type (OpenAIAttachmentUtils.scala 459, 483–486, 505–506). Exact-message assertions are at AIPromptMediaInputSuite.scala 363, 455 and 494; limit substring assertions are at 529 and 637.
- [x] Decisions attachment cancellation stays task-fatal and sanitized (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptAttachmentSuite.scala` ~358–381; AIPrompt.scala 483–484).
- [x] Considered and not raised:
  - recomputing `questionImageCount`/`prepareParts` per row for constant questions (bounded, negligible next to HTTP)
  - the cost of `hostedImageUrl` parsing
  - the names `decisionsSettings`/`hostedImageUrl` (consistent with `inlineImageHeader`)
- [ ] Test execution: not performed because this lens is read-only. I relied on the driver's evidence (284 deterministic tests). OpenAIDecisionsLiveSuite was canceled, so there is no live-provider evidence.
- [ ] Line-by-line review of every test hunk: the supplied diff was truncated in my context after the OpenAIAttachmentUtils.scala hunk.
  - Production files and the Decisions doc sections were re-read from the worktree.
  - Tests were read selectively: AIPromptMediaInputSuite.scala 20–165 and 330–540, and AIPromptAttachmentSuite.scala 280–400.
- [ ] Check whether the artifact already exists: `reviews/` is off-limits to this reviewer.

## Issues

### Issue 1: Decisions format rejections use one message for three causes and omit the detected type
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628–629, 634–635, 645, 649
- **Description**: `validateImageAttachment` throws the same fixed text for three different causes:
  - (a) A path data URL declares a non-allowed MIME type, such as `data:image/jpg` or `data:image/bmp`.
  - (b) The filename extension is not allowed, so the file is rejected before any read (`.jfif`, `.ashx`, `.pdf`).
  - (c) The file was read, but its content was detected as a disallowed type. Examples: a `.png` file containing XBM, detected as `image/x-bitmap` (test at AIPromptMediaInputSuite.scala 455); an extensionless WAV, detected as `audio/x-wav`; or an extensionless JPEG without a JFIF/Exif marker, detected as `application/octet-stream`.

  For the same inputs, Chat reports `MIME type 'audio/x-wav'` and Responses reports `Unsupported file type: <mime>` (OpenAIAttachmentUtils.scala 459, 483–486, 505–506). The WAV test (AIPromptMediaInputSuite.scala 335–371) shows that Decisions is the only API that gives no type information.
- **Risk**: The `errorCol` text cannot tell users whether a file was skipped without IO, whether its MIME declaration was non-standard, or whether its content was rejected. Users with real JPEGs or PNGs are told "only support JPEG, PNG…" and must re-download and inspect files to find out why. There is no functional defect.
- **Suggested Fix**: Keep the leading sentence that names JPEG, PNG, GIF and WebP, and append a fixed reason:
  - "unsupported filename extension; file not read"
  - "data URL declares '<mime>'"
  - "content detected as '<mime>'"

  Never include the path or filename. Echo a declared MIME type only if it matches a MIME-token pattern. Update the exact assertions at AIPromptMediaInputSuite.scala 363, 455 and 494, and assert one reason for each cause.

### Issue 2: The Decisions-only format check has a generic name, and its allowlist is split across shared and inline definitions
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 313, 628–629, 634–635, 645, 649
- **Description**: `validateImageAttachment` sits in the shared Chat/Responses/Decisions attachment path but enforces only a Decisions rule. A reader could infer that all APIs reject non-image paths, but Chat and Responses accept text, PDF and Office files. The Decisions format rules are spread across three places:
  - the shared `imageExtensions` (313), which drives both the pre-IO extension check (645) and data-URL classification (634–635, via `categorizeFileType` at OpenAIAttachmentUtils.scala 333)
  - an inline `Set("image/jpeg", …)` for the post-read check (649)
  - the message text (629)
- **Risk**: If `imageExtensions` is broadened for Chat/Responses (for example `avif`, `heic` or `bmp`), Decisions would silently change in two ways:
  - It would read those files before rejecting them, which breaks the documented "rejected without reading the file" behavior (docs 320–321).
  - It would forward such data URLs to the provider instead of rejecting them locally.
- **Suggested Fix**: Rename the method to `requireDecisionsImage` (or similar). Define one set of Decisions constants, for example `DecisionsImageMimeTypes` and `DecisionsImageExtensions`, and use them for all three checks and the message.

### Issue 3: The 128-image limit is reported with two messages and no counts
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 136, 165–167
- **Description**: The same request-wide limit produces two different messages:
  - "Decisions accepts at most 128 images" when the input alone exceeds it (line 136, which runs first inside `prepareDecisionsRequest`)
  - "Decisions accepts at most 128 images per request" from the new combined checks (118, 165–167)

  Neither message reports the counts, and "128" is hard-coded instead of using `MaxImages`.
- **Risk**: The documented rule (docs 259–260) is reported inconsistently. Users with row-bound questions cannot tell whether to trim input images or question images. The text can go stale if the limit changes.
- **Suggested Fix**: Use one helper message for all three checks, for example `s"Decisions accepts at most $MaxImages images per request (input: N, questions: M)"`, including whichever counts are known at that point. The existing tests (529, 637) use `contains("at most 128 images")`, so they still pass.

### Issue 4: Path-loaded images are copied and scanned as base64 again before each Decisions request (optional optimization)
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: NativeQuestions.scala 227–238 (copy at 231, scan at 233); OpenAIAttachmentUtils.scala 170, 232, 437–438, 500
- **Description**: Path images now pass through `validatePart`, which copies the whole base64 body (`url.substring(comma + 1)`) and scans it again. The earlier validation was already sufficient:
  - File and HTTP payloads were just produced by `Base64.getEncoder` (line 500), so they are valid by construction.
  - Data-URL paths were already copied and validated (170, 232).

  The codebase avoids such copies elsewhere ("without copying the potentially large base64 body", 437–438). This is not a demonstrated defect.
- **Risk**: Each image costs an extra transient allocation of about 1.33× the file size, plus one extra linear scan. When `fileSizeLimitMB` is unset, reads are unbounded, so executor peak memory and GC pressure rise. Correctness is unaffected.
- **Suggested Fix**: Add an offset overload, `validateBase64Content(value: String, start: Int)`, that scans in place. Use it in `validatePart`, replacing `encoded.nonEmpty` with `url.length > comma + 1`. This also benefits explicit, JSON and question images, and the request shapes do not change.

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

## Driver dispositions

The original findings and `ISSUES_FOUND` verdict remain unchanged. These
dispositions supersede the pending entries; none requires a production change
for this compatibility fix.

- **Issue 1 - optional diagnostic detail, deferred.** The current message names
  the accepted formats without echoing user-controlled metadata. Documentation
  separately explains extension rejection, standard MIME spelling and content
  inspection. Detection-phase details would improve troubleshooting but do not
  correct serialization, rejection or sanitization behavior.
- **Issue 2 - optional factoring, deferred.** The current extension/MIME rules
  agree and the Decisions predicate is explicitly short-circuited for other
  APIs. Broadening shared image support would require coordinated tests for that
  future behavior change. No current allowlist mismatch was demonstrated.
- **Issue 3 - optional wording consolidation, deferred.** Both diagnostics
  describe the same enforced request-wide limit; neither promises per-question
  allowances. The focused coverage batch now pins sums across multiple questions
  as well as shared paths. Counts in diagnostics are not an API requirement.
- **Issue 4 - optional allocation optimization, deferred.** The repeated
  base64 validation is real but preserves a single validation boundary for all
  Decisions inputs. Offset-based scanning would affect explicit and question
  inputs too and is a separable optimization, not a demonstrated correctness
  defect. Finite limits and whole-file memory behavior are documented.

Evidence: the cited reader/validator source, the preceding 284-test full run,
and the unchanged production/documentation blobs. This review's stated context
truncation and selective test reads are retained; it is polish-lens evidence,
not a claim to have line-reviewed every test. Pass 15 is invalidated by the
separate focused test update and will not qualify the next snapshot.