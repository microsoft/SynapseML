---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 9
stage_attempt: 1
round: 1
timestamp: "2026-11-17T13:32:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "da2a445b93521f7f93be12d2a92990741039ae24a6ae9fcdabd0f78f295a3a8e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-9-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Prompt checks happen before IO.** See `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 364-386.
  - When Decisions has path columns, null or empty text throws `AIMissingInputException("input is null or empty")` and maps to `missing_input`.
  - Whitespace-only text fails in `NativeQuestions.validateDecisionsInput` and maps to `invalid_request`.
  - Both messages match the non-path Decisions path in `AIRequestAdapter.input` (`AIRequestAdapter.scala` 524-532).
  - Both checks run before `generatedMessages` reads any attachment.
  - Chat and Responses keep "prompt is null" and still accept empty strings.
- [x] **Request ordering is correct.**
  - Only Decisions uses a per-row `lazy val` thunk (`AIPrompt.scala` 474-478). Other APIs force messages when the `Option` overload's argument is evaluated, which keeps the old eager order.
  - In `AIRequestAdapter.scala` 571-623 the order is: `selectedModel`, then `requestVersion` (a missing bound version gives `missing_input`, 589-597), then `mergedOptions`, then question parsing and validation, then `validateDecisionsImageCount`, then `messages()`.
  - All exceptions still reach the same catch block (`AIPrompt.scala` 482-494).
- [x] **The pre-IO image count cannot undercount.** It uses the same trimmed, non-empty rule as `OpenAIPrompt.createMessagesForRow` (`OpenAIPrompt.scala` 608-610). `NativeQuestions.prepareDecisionsRequest` re-checks the combined input and question total after preparation (`NativeQuestions.scala` ~151-184).
- [x] **Capability gating is correct.** See `AIRequestAdapter.scala` 247-264, 274-280 and 309-314.
  - Decisions now accepts `columnTypes`.
  - `fileSizeLimitMB` is accepted only when some column is typed `path`.
  - JSON `inputMode` combined with path columns is rejected.
  - `messagesCol` with a non-empty `columnTypes` is still rejected.
  - SystemOne and embeddings rules are unchanged.
  - Column kinds are limited to `text` and `path`, and path columns must be strings.
  - `columnTypes` defaults to `Map.empty` (`OpenAIPrompt.scala` 293).
- [x] **Hosted URL validation is sound.** See `validatePart` in `NativeQuestions.scala` (~203-239).
  - A case-insensitive `data:image/` prefix goes through the existing inline checks.
  - Any other URL needs a parseable `java.net.URI`, an `http` or `https` scheme, and a non-empty `toURL` host.
  - Underscore and IDN hosts pass because the `URL` parser supplies the host.
  - Empty hosts, `:443`-only authorities, spaces, bad `%` escapes, opaque or relative URLs, and other schemes are rejected.
  - Nothing is fetched and no DNS lookup happens.
  - The JSON string is forwarded unchanged, so signed query strings and escapes are preserved.
  - Error messages do not echo URLs.
- [x] **Question instructions are handled correctly.** See `NativeQuestions.scala` 86-119 and ~151-184.
  - Instructions accept a string or a validated part array.
  - Question images are capped at 128, and the cap also applies to input plus question images combined.
  - Null `file_id` and `detail` fields are dropped from both input and question image parts.
  - `AIJson.OrderedMap` `+` and `-` keep each key in place (`AIJson.scala` 23-44), so the request field order is preserved.
  - SystemOne question validation is unchanged.
- [x] **Format prechecks are sound.** See `OpenAIPrompt.scala` 628-656 and `OpenAIAttachmentUtils.scala` 315-337 and 428-442.
  - The check is a by-name parameter that is evaluated only for Decisions, so Chat and Responses are unaffected.
  - Data URLs are classified from the header alone, before the size check.
  - `mayBeImagePath` cannot wrongly reject a file. `categorizeFileType` never labels a non-empty extension outside `imageExtensions` as an image, and both use the same `attachmentFilename` and `fileExtension`.
- [x] **The WebP fix is correct.** See `OpenAIAttachmentUtils.scala` 295-308. Files of at least 12 bytes starting `RIFF....WEBP` now map to `image/webp`. RIFF/WAVE files still go through the JDK and come out as `audio/x-wav`. Data URLs keep their declared MIME type.
- [x] **Durable mode is unchanged and still enforced.** See `AIDurableInput.scala` 84-101 and 162-195.
  - Path columns are checked as inline for every API, including Decisions.
  - Hosted question and input images are caught by `content`/`inline`.
  - The patch does not touch the resume or fingerprint code.
- [x] **No paths or secrets leak into errors or prompts.**
  - Read failures map to "Unable to read input attachment", and cancellation is rethrown (`AIPrompt.scala` 482-487).
  - Template placeholders render `attachmentFilename`, which uses only the URI path (`AIPrompt.scala` 419-438; `OpenAIAttachmentUtils.scala` 172-187), so query tokens never reach the prompt text.
  - The new diagnostic contains no path.
- [x] **Public API and persistence are unchanged.** `OpenAIDecisions` only locks `apiType` (`OpenAIDecisions.scala` 15-21). The patch adds no params, output columns or Python sources, and Decisions does not keep generated messages.
- [x] **Docs match the code.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 291-326 covers:
  - the Hadoop query-string and percent-escape limits
  - text mode only for path columns
  - error codes
  - pre-IO checks
  - standard JPEG MIME naming
  - `.jfif`/`.jpe` files
  - inline-only images in durable mode

  `Durable AI results.md` 282-301 is still accurate.
- [x] **Tests were read, not run.** I read `AIPromptMediaInputSuite.scala`, `NativeQuestionsSuite.scala` and `AIPromptRecoveryAttachmentSuite.scala`. The pass counts are as reported by the driver.
- [ ] **No execution was possible.** My tools are read-only, so no build, test or live-provider run was done. The supplied diff was cut off after the `OpenAIAttachmentUtils.scala` hunk header. I reviewed the later changes (OpenAIPrompt, tests, docs) from the worktree, which I assume matches the staged index.

## Issues

### Issue 1: Path files whose content is identified as another image subtype skip the Decisions JPEG/PNG/GIF/WebP diagnostic
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628-629, 649. The type comes from `OpenAIAttachmentUtils.scala` 295-308, 334 and 498-501, and is accepted at `NativeQuestions.scala` 191-200.
- **Description**:
  - After reading a file or HTTP path, Decisions checks only `validateImageAttachment(fileType == "image")`.
  - The shared `categorizeFileType` treats any `image/*` MIME type with an allowed extension as an image.
  - `inferMimeType` trusts content sniffing first, and `URLConnection.guessContentTypeFromStream` can identify `image/x-bitmap` (`#def`), `image/x-pixmap` (`! XPM2`) and `image/vnd.fpx`.
  - So a path named `.png`, `.jpg`, `.gif` or `.webp` whose bytes sniff as one of those types passes the check. `responsesMessage` then builds `data:image/x-bitmap;base64,...`, and `inlineImageHeader` accepts any `image/<token>`. The request goes to the provider instead of failing locally.
  - Data-URL path cells do not have this gap. `extensionForMimeType` maps unusual image types to `bin`, so those cells get the diagnostic.
  - The docs (lines 309-310) say Decisions path columns accept JPEG, PNG, GIF and WebP, "not … other image formats". The spec says identified unsupported formats must get the diagnostic.
- **Risk**:
  - This is rare because it needs an obscure format saved under a misleading name.
  - The cost is a wasted provider call and a provider-side 4xx error instead of the local `invalid_request` diagnostic.
  - It is inconsistent with how data-URL cells are handled.
  - What Decisions accepts is tied to the shared Chat/Responses `imageExtensions` and category rules, so widening those later would quietly widen Decisions too.
  - There is no security or data impact.
- **Suggested Fix**:
  - Tighten the Decisions-only post-read check, for example: `validateImageAttachment(fileType == "image" && Set("image/jpeg", "image/png", "image/gif", "image/webp")(mimeType))`.
  - The check stays lazy and Decisions-only, so Chat and Responses are unchanged.
  - Genuine supported images always resolve to these four types through sniffing or the standard JDK filename map.
  - Add one regression test to `AIPromptMediaInputSuite`: a `.png` file starting with `#define`, expecting the diagnostic and zero inference requests.
  - Alternatively, defer the fix and document that content identified as another image subtype under a supported filename is forwarded with that MIME type.
  - Full magic-byte checks for misnamed content the JDK cannot identify (for example, a TIFF named `.png`) would be a broader redesign and are not requested.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed; this pass ended and a fresh complete pass is required.
- **What changed**: The Decisions-only post-read guard accepts exactly `image/jpeg`,
  `image/png`, `image/gif`, and `image/webp`. The preceding extension guard and the
  shared Chat/Responses classifier are unchanged.
- **Why**: A supported filename does not make a content-identified X bitmap a
  supported Decisions image. Both public Decisions stages must reject it locally.
- **How verified**: The baseline regression failed because `local-png` had no
  error. After the fix, all 74 tests across `AIPromptMediaInputSuite`,
  `AIPromptAttachmentSuite`, and `OpenAIPromptOfficePathSuite` passed with no
  failures or skips. The regression checks the exact diagnostic, null output and
  zero inference requests for the misnamed file and inline `image/x-bitmap`.
  Root ScalaStyle, test ScalaStyle, compilation, test compilation and codegen
  passed. Generated Python wrappers and six documentation examples parse.
  Final nine-file patch SHA-256:
  `2f601710028848d175c595fc795321b27f1009be2a1b83a231f237df4bea33b9`.

### Transport coverage note

The driver verified that the submitted prompt contained the complete frozen
binary diff. The reviewer used worktree source reads after context compaction;
its original coverage limitation above is retained, not converted into clean
signoff. The source change ends this pass regardless of that limitation. Fresh
review checks the staged source blobs before and after each invocation.

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
    "stageBehavior": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 2982f5a9b198fc3c35e601187887e18b57634faa"
  },
  "bounds": {
    "fastCyclesUsed": 11,
    "fastCycleLimit": 14,
    "reconstruction": "Eleven fix/integration cycles and eight consumed pre-commit attempts retained. User authorized exceeding prior limits; next finite fast-cycle bound is 14.",
    "precommitPassesUsed": 8,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 9,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "bc1429a59ba9586c8647b8c14173e7b505e6256e300771567ce61adf18514274",
  "manifestSha256": "8c458734561b6bd785caa2f7067402625f89e77da09188e9ce00729e5a484566"
}
