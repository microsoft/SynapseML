---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 1
timestamp: "2026-11-18T01:30:19Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e72e7fbb6a495e2881d32e3e87601ef9adefec94efdf3c69480008d4aefb2663"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-17-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff completeness.** Read all 13 file entries and 42 hunks of `.review-input-decisions-paths.diff`: production at lines 1-422, tests at 422-1540, docs at 1540-1720. No hunk was left unreviewed.
- [x] **Image loading starts only after the cheap row checks.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala:471-481`, Decisions passes the `lazy val inputMessages` as a thunk. In `AIRequestAdapter.scala`, `request` (~610-633) runs `selectedModel` (415-420), `requestVersion` and `mergedOptions` before calling `messages()`. `nativeEnvelope` (~582-598) also parses and validates questions and calls `validateDecisionsImageCount(pathImageCount, …)` before that call. Chat, Responses, embeddings and System One use the strict 3-argument overload (`() => messages, 0`), so they keep eager preparation and their existing error order.
- [x] **Prompt-text error codes.** In the new path branch of `AIPrompt.messages` (364-386), null or empty text raises `AIMissingInputException("input is null or empty")` (`missing_input`). Whitespace-only text reaches `NativeQuestions.text` (`NativeQuestions.scala:32-35`) and raises `invalid_request`. This matches the non-path behaviour of `AIRequestAdapter.input` (535-543), and both checks run before `generatedMessages`.
- [x] **Image count.**
  - The count `paths.count(... _.trim.nonEmpty)` uses the same filter as `createMessagesForRow` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala:608-610`).
  - Each accepted Decisions path produces exactly one `input_image`, since `wrapFileToMessagesList` returns `Seq(fileMessage)` and non-images are rejected.
  - `prepareDecisionsRequest` (`NativeQuestions.scala:151-163`) re-checks the combined input-plus-question total against 128.
- [x] **Format gates.**
  - `validateImageAttachment(isImage: => Boolean)` (`OpenAIPrompt.scala:628-629`) takes a by-name predicate and checks `apiType == "decisions"` first, so Chat and Responses never evaluate it.
  - `mayBeImagePath` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala:315-318`) runs before `prepareFile` and uses the same filename and extension logic.
  - Data URLs go through the metadata-only `dataUrlFileType`. `image/jpg`, tiff, bmp and x-bitmap map to `bin` and are rejected before the base64 and size checks.
- [x] **WebP fix.** `inferMimeType` (`OpenAIAttachmentUtils.scala:295-308`) requires at least 12 bytes, with `RIFF` at offset 0 and `WEBP` at offset 8. Short RIFF files and WAV still resolve to audio. The tests cover all four prompting stages.
- [x] **Hosted URL validation.** Traced `hostedImageUrl` (`NativeQuestions.scala:203-213`) through JDK 11 `URI` and `URL`:
  - Accepted: underscore and IDN hosts (via the registry-authority fallback), IPv6, and an empty port.
  - Rejected: empty hosts, signed or Unicode-digit ports, and non-HTTP schemes.
  - The error message is fixed text and never echoes the URL.
  - The missing port-range check is reported as Issue 1.
- [x] **Configuration checks.**
  - `validateCapabilities` (`AIRequestAdapter.scala:247-263`) allows only `messagesCol`, `columnTypes` and `fileSizeLimitMB` for Decisions.
  - `validateMessageConstruction` runs before the new `fileSizeLimitMB` rule, so `messagesCol` conflicts keep their own message.
  - `validateDecisionsInputColumn` uses Spark's resolver through `AIColumnPlan.matches`.
  - System One and embeddings still reject `columnTypes`.
- [x] **Request shape and output schema.**
  - Generated Decisions messages are a single `user` message with `input_text` and `input_image` parts (`OpenAIPrompt.scala:615`).
  - `prepareParts` only drops null `file_id` and `detail` fields.
  - The ordered AIJson map keeps question key order when `instructions` is replaced.
  - `retainedMessagesName` (`AIPrompt.scala:295-300`) applies only to generative APIs, so the Decisions output schema is unchanged.
- [x] **Durable runs and persistence.** `AIDurableInput` is unchanged, so durable path and question images remain inline-only, and the tests cover durable runs and resume. No new Params were added, so the persisted shape and generated wrappers are unchanged. There is no duplicate of these restrictions under `cognitive/src/main/python`.
- [x] **Docs.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (~244-332, plus the parameter table) matches the code: hosted-URL forwarding, per-file ordering, rejecting unsupported extensions before IO, Hadoop query/percent limits, JPEG naming, and the 128-image total.
- [ ] **Not executed.** This reviewer ran no tests (read-only tools only). The driver's results (57/57 in the latest focused run; 288 unique passes across three runs) were not reproduced. Live provider and Python runtime behaviour were not verified.

## Issues

### Issue 1: Hosted Decisions image URLs accept out-of-range ports
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 203-213 (called at 240); compare `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` lines 31 and 46-47.
- **Description**: The new `hostedImageUrl` only checks that the port is made of ASCII digits.
  - Java's URI parser does not check the port range. Java's URL parser rejects only negative ports and values too large for an `int`.
  - So `https://example.invalid:99999/photo.png`, `:65536`, `:0` and `https://[2001:db8::1]:99999/` all pass local validation. They are then forwarded as publicly accessible hosted images.
  - The endpoint check in the same package rejects these ports ("Endpoint port must be omitted or between 1 and 65535"). The docs also state that rule for endpoints.
  - The WHATWG URL standard treats ports above 65535 as a parse failure.
  - The new `NativeQuestionsSuite` rejection cases (diff lines 1446-1453) cover `bad`, `-1`, `+443` and Unicode digits, but no out-of-range values.
- **Risk**: These URLs can never be fetched. Each affected row makes a provider call that uses quota and returns a provider error the stage doesn't control, instead of failing locally with the existing `invalid_request` message. Endpoint URLs and image URLs are also validated inconsistently. RFC 3986 allows any digit string as a port, so this is hardening, not a spec breach, and there is no security exposure.
- **Suggested Fix**: After the digit check, accept only an empty port or a value from 1 to 65535. Add a length guard or parse as `Long` to avoid overflow, and share the constant with `MaxPort`. Add rejection tests for `:0`, `:65536` and `:99999` on a DNS host, a bracketed IPv6 host and an IDN host. Keep the existing tests that accept `:443` and an empty port.

### Issue 2: Decisions image-format allowlist is split across shared and inline sets (maintainability, not a current defect)
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 313, 628-629, 634-635, 645, 649
- **Description**: The JPEG/PNG/GIF/WebP restriction is enforced in three separate places:
  - the pre-read extension check (`mayBeImagePath(filePathStr, imageExtensions)`, line 645);
  - the data-URL check (`dataUrlFileType(..., imageExtensions, ...)`, lines 634-635);
  - an inline post-read MIME set (line 649), with the format list repeated in the error message at line 629.

  The first two reuse the shared Chat/Responses `imageExtensions` (line 313). The data-URL branch has no Decisions-specific MIME check. Native validation (`NativeQuestions.inlineImageHeader`) accepts any `data:image/<token>`. Today all three agree.
- **Risk**: Suppose a later Chat/Responses change adds a format such as avif, heic or svg to `imageExtensions` and `MimeTypeExtensions`. Decisions would then silently change in two ways:
  - It would accept and forward data URLs in that format to the provider.
  - It would read and size-check files with that extension before rejecting them. That breaks the documented promise that an unsupported extension is rejected before the file is read or size-checked.

  The tests only pin bmp, tiff, x-bitmap, `image/jpg`, pdf, office, text and audio (diff lines 836-912), so other formats would slip through unnoticed.
- **Suggested Fix**: Define one Decisions-specific allowlist mapping MIME types to extensions, next to `validateImageAttachment`. Derive all three checks from it, comparing the declared lower-cased MIME directly for data URLs. Alternatively, add a test that `imageExtensions` equals the Decisions extension set, so any broadening forces an explicit Decisions decision.

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

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent assessment of the frozen thirteen-file patch",
    "model": "Installed review-code complementary-provider and capability-first rules",
    "reasoning": "Installed review-code maximum supported reasoning",
    "stageBehavior": "Trusted repository review-gauntlet.md: six sequential themes",
    "artifactDirectory": "Explicit driving-user exception",
    "safetyStopPolicy": "Explicit driving-user authorization to exceed previous limits; next attempt uses a finite 3600-second timeout per reviewer and preserves consumed counters."
  },
  "changeSize": "Thirteen files: Decisions hosted/question/path images, shared WebP classifier correction, focused error consistency and examples; most additions are tests and documentation.",
  "cyclomaticComplexity": "Moderate: shared capability validation and attachment serialization branches, existing image URL validation and request-wide counting. No new concurrency mechanism, credential flow, storage format, or public signature.",
  "rationale": "Six specialist lenses cover the bounded cross-API behavior and persistence interactions. Repository policy selects sequential execution rather than the skill's concurrent Medium default. No tier downgrade or review waiver.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    {
      "model": "claude-opus-5.5",
      "provider": "Anthropic",
      "reasoningEffort": "max",
      "selection": "The live task catalog supports this exact ID and max reasoning. Anthropic's current model overview recommends Opus 5.5 for long-running agentic coding. Fable 5.2 is not in the live catalog; Sonnet emphasizes speed/intelligence balance and Haiku latency. Opus is the most capable eligible Anthropic coding model supported here.",
      "capabilitySource": "https://platform.claude.com/docs/en/models/overview",
      "capabilityVerifiedAt": "2026-11-18T01:06:50.609445+00:00"
    }
  ],
  "overrideSource": {
    "artifactDirectory": "User input: \"I approve that exception, but tsince this might be a new PR you might need a new path\"",
    "scope": "One-time repository-root reviews/ exception for this Decisions path-column follow-up. PR #2853 is still open, so no replacement PR is needed. Use fresh filenames; do not overwrite or migrate prior artifacts.",
    "stageBehavior": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 2982f5a9b198fc3c35e601187887e18b57634faa"
  },
  "bounds": {
    "fastCyclesUsed": 19,
    "fastCycleLimit": 19,
    "reconstruction": "Nineteen bounded fix/integration cycles and sixteen pre-commit passes retained.",
    "precommitPassesUsed": 16,
    "precommitPassLimit": 17,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 17,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "5f939e3f515e6f44844ce05af6b10327dd65b1a020e282378b72bff27834d3d7",
  "manifestSha256": "7fe5bbe25daeb9245dcf700da66677f6e5998d5edd31035c5b28c895600e04f2",
  "transport": {
    "completeDiffAvailable": ".review-input-decisions-paths.diff",
    "reason": "The CLI truncated the visible initial diff on earlier dispatches. Permit reading this exact current immutable product diff in addition to the complete supplied prompt; do not permit prior feedback, other prompts, checkpoints or transport."
  }
}

## Driver disposition

The original two findings and `ISSUES_FOUND` verdict remain intact. Neither
recommendation is adopted as a required change to this focused compatibility
fix; no reviewed content changed.

### Issue 1: Closed without changes; optional provider-URL hardening

The observation is valid: numeric ports outside the TCP destination range can
pass the existing URI/URL syntax checks and later fail provider fetching. The
reviewer explicitly distinguishes this from a specification violation or
security exposure. The current hosted-image contract is syntactic URI
validation followed by unchanged forwarding, not a local guarantee that a
provider can fetch the resource. Rejecting those ports locally could save a
provider call, but is optional additional validation.

`NativeQuestions.scala:203-213` deliberately validates HTTP(S), a nonempty host,
and RFC 3986 ASCII port syntax. `AIRequestAdapter.scala:33-50` validates a
different boundary: the endpoint to which SynapseML itself connects. Its
stricter rules also reject underscores, userinfo and fragments that may be
preserved in image inputs. Reusing the endpoint policy wholesale would not be
an appropriate equivalence requirement.

No claim is made that an out-of-range port is fetchable or locally rejected.
Provider errors remain provider errors. A future numeric-range precheck can
be considered separately without blocking the requested image support.
The relevant source and current hosted-URL tests were inspected; no production
change or additional validation run is needed for this no-change disposition.

### Issue 2: Closed without changes; duplicate future-format recommendation

This repeats the architecture finding retained in pass 16 round 2. Current
extension/MIME gates all describe JPEG, PNG, GIF and WebP:
`OpenAIPrompt.scala:313,628-651` and
`OpenAIAttachmentUtils.scala:137-157,315-338,431-442`. No added format in this
patch makes the sets diverge. The proposed failure depends on a later shared
format expansion, which must itself be reviewed for cross-API effects.

A Decisions-specific mapping is a reasonable alternative organization, not a
fix for a demonstrated current defect. Preserve the focused implementation
and its positive/negative format, extensionless WebP, unknown-extension,
bounded-read and no-IO tests. These passed in the latest media/attachment run.
This does not claim that a hypothetical future format addition is already
covered or safe.

**Coverage and continuation:** the reviewer explicitly read all 13 files and
42 hunks from the immutable diff, resolving the earlier transport truncation
limitation. Continue the same pass on unchanged product patch
`5f939e3f515e6f44844ce05af6b10327dd65b1a020e282378b72bff27834d3d7`,
retaining both recommendations and the actual reviewer verdict.
