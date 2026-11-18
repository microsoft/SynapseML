---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 1
timestamp: "2026-11-17T12:25:05Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "3917fa33d45ff41643308bd2cd5529c971e24b77624461adf92435b092d43d44"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-7-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Checks run before image IO.** I traced the Decisions path flow in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`, `request` (lines 599-622) and `nativeEnvelope` (lines 571-587). The order is: `selectedModel`, then `requestVersion` ("apiVersion is null"), then per-row `mergedOptions`, then per-row question parse/`NativeQuestions.validate`, then `validateDecisionsImageCount(pathImageCount, questions)`. Only after that does `messages()` load images.
- [x] **Prompt text checks.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`, `messages` (lines 364-386) checks the prompt before `generatedMessages` loads any image:
  - null or empty text → `AIMissingInputException("input is null or empty")` (missing_input);
  - whitespace-only text → `validateDecisionsInput(JsString)` (invalid_request).
  - These messages and codes match the non-path Decisions `input()` path (AIRequestAdapter.scala lines 524-532).
- [x] **Lazy vs. eager preparation.**
  - `AIPrompt.scala` `prepareRequests` (lines 453-498): messages are lazy only for `apiType == "decisions"`. All other APIs use the eager overload (line 478).
  - The pre-IO `imageCount` uses the same trim/non-empty rule as `createMessagesForRow` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` lines 602-617).
  - `indices` includes every `columnTypes` key (line 458).
  - Path columns are required to be `StringType` (AIRequestAdapter.scala lines 309-313).
  - Exceptions map to `input_io_error` (sanitized), `missing_input` or `invalid_request`.
- [x] **Capability gating.**
  - `validateCapabilities` (AIRequestAdapter.scala lines 247-262): Decisions allows `columnTypes`, and allows `fileSizeLimitMB` only when a path column exists.
  - Path columns require `inputMode='text'` (default "text": `AIPromptParams.scala` line 105).
  - `validateMessageConstruction` (lines 274-280) still rejects non-empty `columnTypes` or `fileSizeLimitMB` combined with `messagesCol`.
  - System One and embeddings gating is unchanged.
  - No hand-written Python capability matrix exists (no matches under `cognitive/src/main/python`).
- [x] **NativeQuestions.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` (lines 86-119, 140-189, 191-238):
  - Question instructions can be a string or a non-empty array of parts.
  - The 128-image limit is checked for question images alone and for input plus questions.
  - Null `file_id`/`detail` are omitted in both input and question parts.
  - In the hosted branch, a failed `java.net.URI` parse, a scheme other than http/https, or an empty `URL.getHost` all lead to rejection. Underscore and IDN hosts use registry authorities whose URL host is non-empty, so they are accepted. The URL string is forwarded unchanged, which preserves query strings and `%xx` escapes. Error messages contain no URL text.
- [x] **WebP fix and Chat/Responses error precedence.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`:
  - WebP needs at least 12 bytes with RIFF at offset 0 and WEBP at offset 8 before the JDK guess (lines 295-307). WAV still maps to audio through the JDK.
  - `dataUrlFileType` (lines 426-437) raises the same metadata exceptions as `prepareDataUrlAttachment` (lines 221-239). The new eager call in `wrapFileToMessagesList` (OpenAIPrompt.scala lines 632-655) therefore does not change Chat/Responses error precedence.
  - `responsesMessage` (lines 485-507) emits `data:<lowercase mime>;base64,…`, which `inlineImageHeader` accepts.
- [x] **Bounded reads.** `AIPrompt.useBoundedAttachments = true` (AIPrompt.scala line 218), so file and HTTP reads use the bounded reader whenever `fileSizeLimitMB` is set. There is no default limit, which matches the docs ("Unset … limits remain unlimited").
- [x] **Durable mode.** `AIDurableInput.scala` (lines 81, 172) still enforces inline-only path columns. The resume fingerprint (`AIPromptOutput.scala` lines 186-209) is built from parameters and library version, not request bodies. The question re-serialization in `prepareDecisionsRequest` therefore cannot affect resume semantics.
- [x] **Docs match code.** In `docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, lines 236-335 and the parameter table (lines 712-740) match the code. This covers:
  - hosted vs. inline images;
  - the combined image limit;
  - the Hadoop query and `%` limitation;
  - null/blank path cell semantics;
  - which checks run before IO;
  - durable inline-only media;
  - System One staying text/JSON-only.
- [x] **Per-row header and version text.** Static endpoint and header configuration is validated in `AIRequestAdapter.validate()` (lines 363-379). Only malformed per-row header text (`buildRequest`, AIPrompt.scala lines 388-400) and per-row apiVersion text (`versionedEndpoint`) are checked after image IO. The spec's pre-IO list doesn't include these, so this is not reported as an issue.
- [x] **Tests read.** I read `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`:
  - lines 292-300: WebP across Decisions, Chat and Responses;
  - lines 363-410: non-image rejection, sanitized errors, size limits, no HTTP sent.
  - There is no JPEG-variant coverage (see Issue 1).
- [ ] **Independent test execution was not performed.** This lens is read-only. I relied on the driver's evidence that 232 tests pass, plus the style, compile and codegen results.

## Issues

### Issue 1: Valid JPEG path inputs can be rejected with the contradictory "only support JPEG…" error
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala (with cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala and docs/Explore Algorithms/OpenAI/Unified AI prompting.md)
- **Line(s)**:
  - OpenAIAttachmentUtils.scala: 156-157, 295-307, 315-333 (image branch at 329), 426-437
  - OpenAIPrompt.scala: 313, 629-636, 649
  - Unified AI prompting.md: 309-310
- **Description**: Decisions path columns now accept an image only when the shared classifier returns `"image"`. That requires both an `image/*` MIME and an effective extension in {jpg, jpeg, png, gif, webp}. A filename extension takes priority over the sniffed MIME. Per the JDK's `URLConnection.guessContentTypeFromStream` logic, content sniffing only recognizes JPEGs whose first segment is APP0 (`FF D8 FF E0`), Adobe APP14 (`FF D8 FF EE`), or Exif APP1 (`FF D8 FF E1` + "Exif\0"). Tracing three kinds of valid JPEG input:
  1. **`.jfif` or `.jpe` names.** A file or HTTP path named `photo.jfif` or `photo.jpe` sniffs as `image/jpeg`, but the effective extension `jfif` is not allowed, so it becomes `unsupported`.
  2. **Extensionless paths with other first markers.** Object-store keys or HTTP paths without an extension, holding a JPEG that starts with another marker, are affected. Examples: DQT `FF D8 FF DB`, ICC `FF E2`, Photoshop `FF ED`, or XMP-first APP1. The JDK returns null and there is no name fallback, so the MIME becomes `application/octet-stream` and the result is `unsupported`.
  3. **`data:image/jpg` data URLs.** A path data URL `data:image/jpg;base64,…` maps to extension `bin`, so it becomes `unsupported`. The identical string is accepted as an explicit `input_image` part by `NativeQuestions.validatePart` (inline branch, lines ~215-226), so path and explicit handling disagree.

  In all three cases, `validateImageAttachment` returns "Decisions path columns only support JPEG, PNG, GIF, and WebP images" for an input that is a JPEG. The new docs also state that Decisions path columns accept JPEG. The classifier behavior is inherited (Responses reports "Unsupported file type: …"), so this is not a Chat/Responses regression. The new Decisions contract and error text are what make it user-visible. The tests cover only PNG and WebP positives plus TIFF, BMP, PDF, text, audio and Office negatives.
- **Risk**: Users with `.jfif` downloads, extensionless lake or CDN JPEGs, or `image/jpg` data URLs get row-level `invalid_request` errors whose message contradicts their input, which makes the failure hard to diagnose. The impact is limited: errors are controlled, nothing leaks, and workarounds exist (rename to `.jpg`/`.jpeg`, use `image/jpeg` data URLs, or use explicit parts).
- **Suggested Fix**: This is an inherited limitation rather than a defect in this patch.
  - **Minimal, in scope:** document the classification rule next to line 309: path images need a recognized extension or a JFIF/Exif header, and data URLs need a standard MIME such as `image/jpeg`. Optionally, make the Decisions error message say this.
  - **Behavior change:** this would need explicit approval, because the spec limits Chat/Responses changes to the WebP correction. Options:
    - add a generic JPEG SOI check (`FF D8 FF`) next to the RIFF/WEBP check; or
    - for Decisions only, treat sniffed or declared `image/jpeg|png|gif|webp` (optionally normalizing `image/jpg`) as images regardless of extension.

    Either way, add regression tests for an extensionless DQT-first JPEG and a `.jfif` file.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed by documentation clarification; classifier behavior intentionally unchanged.
- **What changed**: Documented the accepted filename extensions, runtime-dependent detection for extensionless paths, and standard `image/jpeg` MIME for inline paths. Explained the `.jfif`/`.jpe` rename or explicit-image workaround.
- **Why**: Verified the extension allowlist, MIME map and non-WebP detection behavior are unchanged from the target. Decisions intentionally reuses the established reader. Broadening shared JPEG detection or MIME aliases would be an unrelated behavior change across Chat/Responses; the narrower documentation correction addresses the misleading expectation without expanding scope.
- **How verified**: Compared the target's `OpenAIPrompt.imageExtensions` and `OpenAIAttachmentUtils` with the full staged patch; only the previously reviewed WebP detection and metadata-only classification helper differ. Read the updated paragraph against `inferMimeType`, `categorizeFileType` and `extensionForMimeType`. This documentation edit consumes the pass; the new pass will cover it.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent assessment of the frozen nine-file patch",
    "model": "Installed review-code complementary-provider and capability-first rules",
    "reasoning": "Installed review-code maximum supported reasoning",
    "stageBehavior": "Trusted repository review-gauntlet.md: six sequential themes",
    "artifactDirectory": "Explicit driving-user exception",
    "safetyStopPolicy": "Explicit driving-user authorization to exceed previous limits; next attempt uses a finite 3600-second timeout per reviewer and preserves consumed counters."
  },
  "changeSize": "Nine files: Decisions hosted/question/path images, shared WebP classifier correction, focused error consistency and examples; most additions are tests and documentation.",
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
    "fastCyclesUsed": 9,
    "fastCycleLimit": 10,
    "reconstruction": "Nine fix/integration cycles and six consumed pre-commit attempts retained.",
    "precommitPassesUsed": 6,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 7,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "d17f57e314517e4b413cda3cdcd86e7ad15ddb04",
  "localHead": "330c9030d3d51aba5d06c6bfbc4c913f1d313c96",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "ec1be9462ad345440d69e75b54cd7e0a881c7c209b880e37f693c0c453f488ea",
  "manifestSha256": "d19788cadd23bdd26fda67bbcd038ba9e997b76b71cf033b7034be6212999ba0"
}
