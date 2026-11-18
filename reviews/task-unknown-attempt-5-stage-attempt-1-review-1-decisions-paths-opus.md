---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 1
timestamp: "2026-11-17T10:56:16Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "72049987372bc28790bb6fb0a85e0bf1cb9a92c8a985c9f87aec187d300d32b4"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-5-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Checks before image reads.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` (around lines 571-623), `request` runs these steps in order: `selectedModel` (missing model), `requestVersion` (missing version), `mergedOptions` (malformed row options), then `nativeEnvelope`.
  - Inside `nativeEnvelope`, the order is: the null-questions check, row `NativeQuestions.validate`, `validateDecisionsImageCount(pathImageCount, …)`, and only then the `messages()` thunk that reads images. `AIJson.parse` wraps parse failures as `IllegalArgumentException`, so they become row errors.
- [x] **Prompt validation.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` (around lines 377-385), null or empty prompts raise `AIMissingInputException("input is null or empty")` (missing_input). Whitespace-only prompts raise `IllegalArgumentException` from `validateDecisionsInput` (invalid_request). Both happen before `generatedMessages`.
  - In `prepareRequests` (around lines 471-480), the `lazy val` is forced by the by-value overload, so other APIs still prepare messages eagerly.
  - `pathImageCount` counts the same trimmed non-blank cells that `createMessagesForRow` uses.
- [x] **Capability checks.** `validateCapabilities` (around lines 247-263) does the following:
  - allows `columnTypes` for Decisions;
  - allows `fileSizeLimitMB` only when a path column exists;
  - requires `inputMode='text'` when path columns are present;
  - keeps System One rejecting these parameters.
  - `columnTypes` defaults to `Map.empty` (`OpenAIPrompt.scala:293`), so `getColumnTypes` is safe when unset.
- [x] **`OpenAIDecisions` uses the same path.** It extends `AIPrompt` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisions.scala:15`), so it inherits the same flow. `AIPromptMediaInputSuite.scala` runs both stages in its tests (lines 136-458).
- [x] **`NativeQuestions.scala`:**
  - instructions accept text or text/image parts (around lines 90-94);
  - question images are capped (line 119);
  - `prepareDecisionsInput` validates before indexing `content`;
  - question rebuilding keeps key order (`ListMap`), removes only null `file_id`/`detail`, and forwards `image_url` unchanged.
- [x] **Hosted URL check** (around lines 219-238), traced against JDK `URI`/`URL` parsing:
  - Underscore and IDN hosts are accepted through the registry-authority fallback.
  - Empty hosts, relative URLs, opaque URLs, malformed escapes and other schemes are rejected.
  - There is no DNS lookup or client-side fetch, and error messages never echo URLs, so signed query strings do not leak.
- [x] **Durable mode.** `AIDurableInput.scala` (`validate`, `inline`, `validateRows`) and `AIPrompt.scala` `validateDurableRows` (around lines 560-700) still require inline `data:` URLs for path cells and question `image_url` parts. Resume logic is untouched.
- [x] **Attachment flow** in `OpenAIPrompt.scala` (lines 602-650):
  - Decisions gets a single user message with `input_text`.
  - Non-image data URLs are rejected before decoding (line 636); files are rejected after the read but before encoding (line 649).
- [x] **Shared reader.** `OpenAIAttachmentUtils.scala` `inferMimeType`/`categorizeFileType` (lines 293-325) and `prepareFile` (lines 382-413) are the basis of Issue 1. In the same file, Chat/Responses throw for `unsupported` at lines 433 and 480.
- [x] **Considered and not flagged:**
  - Blank row `apiVersion` and row header/credential errors still occur after image reads. The spec only requires *missing* bindings to fail first.
  - Reads are unbounded when `fileSizeLimitMB` is unset; the docs say so.
  - Executor-side fetching of HTTP path cells is existing shared behavior, and durable mode blocks it.
- [ ] Not checked: whether the provider applies the combined 128-image limit across input and questions, and whether it accepts underscore or IDN image hosts. I had no network access.
- [ ] Not checked: I ran no tests or builds. I relied on the driver's evidence (231 tests, ScalaStyle, compile and codegen).

## Issues

### Issue 1: WEBP files in path columns are misclassified as audio and rejected with a misleading error
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: OpenAIAttachmentUtils.scala 293-300, 307-325 and 409-411; OpenAIPrompt.scala 630-631 and 649
- **Description**: `inferMimeType` tries `URLConnection.guessContentTypeFromStream` first. The JDK implementation, which I read but did not run, returns `audio/x-wav` for any stream that starts with `RIFF`, and WEBP files start with `RIFF…WEBP`.
  - Because that result is accepted, the extension-based fallback never runs.
  - `categorizeFileType("audio/x-wav", "webp", …)` then returns `"unsupported"`: the MIME type is not `image/*`, and `webp` is not an audio or text extension.
  - Decisions then reports "Decisions path columns only support image attachments", even though `webp` is in `imageExtensions` and the file is an image.
  - The root cause predates this PR; Chat/Responses fail the same way with "Unsupported file type: audio/x-wav." But this PR adds the misleading Decisions-specific message. There is no positive WEBP path test; only a RIFF/WAVE non-image fixture exists (`AIPromptMediaInputSuite.scala:333`).
- **Risk**: Users cannot send valid WEBP files through Decisions path columns, and the error tells them their image is not an image. Inline `data:image/webp` URLs still work.
- **Suggested Fix**: Choose one of these:
  - **Fix the shared reader.** In `inferMimeType`, check for the WEBP signature (`RIFF` at bytes 0-3 and `WEBP` at bytes 8-11) and return `image/webp` before calling the JDK stream guess. Keep `RIFF…WAVE` classified as audio so the existing non-image test still passes. Add a Decisions path regression test using a minimal WEBP fixture. This also changes Chat/Responses behavior, from an error to success.
  - **Document and defer.** If changing the shared reader is out of scope under "preserve Chat/Responses behavior", document that WEBP path files are unsupported and must be sent as `data:image/webp;base64,…`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent assessment of the frozen eight-file patch",
    "model": "Installed review-code complementary-provider and capability-first rules",
    "reasoning": "Installed review-code maximum supported reasoning",
    "stageBehavior": "Trusted repository review-gauntlet.md: six sequential themes",
    "artifactDirectory": "Explicit driving-user exception",
    "safetyStopPolicy": "Explicit driving-user authorization to exceed previous limits; next attempt uses a finite 3600-second timeout per reviewer and preserves consumed counters."
  },
  "changeSize": "Eight files, with most additions in regression tests and documentation; bounded changes to shared request validation and media serialization.",
  "cyclomaticComplexity": "Moderate: shared capability validation and attachment serialization branches, existing image URL validation and request-wide counting. No new concurrency mechanism, credential flow, storage format, or public signature.",
  "rationale": "Six specialist lenses cover the bounded cross-API behavior and persistence interactions. Repository policy selects sequential execution rather than the skill's concurrent Medium default. No tier downgrade or review waiver.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    {
      "model": "claude-opus-5.5",
      "provider": "Anthropic",
      "reasoningEffort": "max",
      "selection": "The live task catalog supports this exact ID and max reasoning. Anthropic's current model overview recommends Opus 5.5 for long-running agentic coding. Fable 5.2 is not in the live catalog; Sonnet emphasizes speed/intelligence balance and Haiku latency. Opus is the most capable eligible Anthropic coding model supported here.",
      "capabilitySource": "https://platform.claude.com/docs/en/models/overview"
    }
  ],
  "overrideSource": {
    "artifactDirectory": "User input: \"I approve that exception, but tsince this might be a new PR you might need a new path\"",
    "scope": "One-time repository-root reviews/ exception for this Decisions path-column follow-up. PR #2853 is still open, so no replacement PR is needed. Use fresh filenames; do not overwrite or migrate prior artifacts.",
    "stageBehavior": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at d17f57e314517e4b413cda3cdcd86e7ad15ddb04"
  },
  "bounds": {
    "fastCyclesUsed": 7,
    "fastCycleLimit": 10,
    "reconstruction": "Seven fix/integration cycles and four consumed pre-commit attempts retained.",
    "precommitPassesUsed": 4,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 5,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "d17f57e314517e4b413cda3cdcd86e7ad15ddb04",
  "localHead": "330c9030d3d51aba5d06c6bfbc4c913f1d313c96",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "4cfd91a997752c511d3114011023ee6a21c9f3a311a39bb15fe88d4d6dc946b4",
  "manifestSha256": "e7d78e6017ee62aaa0d6e18aace4dada4a876a2b32f8bbf5d44ccfecea6dde0b"
}

## Driver Resolution: Issue 1

**Status: Fixed.** This pass is consumed; a new frozen pass is required.

A network-free probe confirmed that the selected JDK 11 reports both RIFF/WEBP
and RIFF/WAVE as `audio/x-wav`. The shared reader now checks the RIFF container
and WEBP format tags before invoking the JDK detector. Only WebP receives the
corrected `image/webp` MIME type; WAV retains its existing audio classification.

The regression uses a real synthetic 2x2 lossless WebP generated and decoded
successfully with an image library. It failed before the fix for local and HTTP
paths while the inline data URL succeeded. After the fix, filesystem, HTTP and
inline inputs pass through both Decisions stages, Chat Completions and Responses,
with exact MIME and image bytes preserved. Existing WAV, PDF, text, Office,
size, missing-input and IO-error cases also pass.

This intentionally corrects shared Chat/Responses WebP rejection as well, rather
than duplicating a Decisions-only classifier or documenting a known-broken image
format as supported. The focused WebP/non-image regressions and cognitive
main/test style checks pass; all affected attachment/media tests and root build
gates are rerun before the replacement review.
