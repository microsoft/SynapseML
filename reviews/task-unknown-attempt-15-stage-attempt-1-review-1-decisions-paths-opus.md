---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 1
timestamp: "2026-11-17T21:02:02Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "06bfdf6cec921ee6137e24dfb833d076c416fffc3c90486a43ea3d71443d8154"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-15-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Lazy Decisions preparation and pre-IO ordering**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala:474-478`: only Decisions passes `() => inputMessages` plus the count of non-blank paths. Every other API uses the by-value overload at `AIRequestAdapter.scala:610-611`, so their messages are still prepared eagerly.
  - `AIRequestAdapter.scala:613-633` checks `selectedModel`, then `requestVersion`, then `mergedOptions`, then calls `nativeEnvelope`.
  - `nativeEnvelope` (`582-598`) parses and validates row questions and runs `validateDecisionsImageCount` before `messages()` is called. Endpoint checks and row header validation stay after IO, as the docs state.
  - Errors from the lazy initializer reach the existing catch in `AIPrompt.scala:482-495` unwrapped, so cancellation remains task-fatal.
  - `retainedMessagesName` (`AIPrompt.scala:295-296`) applies only to generative APIs, so the lazy value is never forced again for Decisions.
- [x] **Prompt-text errors**
  - `AIPrompt.scala:377-384`: null or empty Decisions text raises `missing_input` with "input is null or empty". This matches the text-only message in `AIRequestAdapter.input` (`535-543`).
  - Whitespace-only text fails in `NativeQuestions.text` with `invalid_request`. Both checks run before `generatedMessages`.
  - Chat and Responses keep "prompt is null".
- [x] **Precheck count matches the generated count**
  - `AIPrompt.scala:476` counts paths with `_.trim.nonEmpty`, the same rule as `OpenAIPrompt.createMessagesForRow` (`OpenAIPrompt.scala:608-610`).
  - Each usable path produces exactly one part, because `wrapFileToMessagesList` returns `Seq(fileMessage)`.
  - `NativeQuestions.prepareDecisionsRequest` (`NativeQuestions.scala:151-163`) checks the combined limit again after IO.
- [x] **Capability matrix**
  - `AIRequestAdapter.scala:247-265`: `messagesCol`, `columnTypes` and `fileSizeLimitMB` are allowed only for Decisions. System One and embeddings behave as before.
  - Path columns require `inputMode="text"`.
  - `validateMessageConstruction` (`275-281`) runs before the check that `fileSizeLimitMB` needs a path column, so `messagesCol` reports its own conflict.
  - Covered by `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala:561-613`.
  - Not raised: when `messagesCol`, `inputMode="json"` and a path column are all set, the `inputMode` error is reported first. That message is accurate, so the order is a design choice, not a defect.
- [x] **`inputCol` overlap**
  - `AIRequestAdapter.scala:316` and `325-332` use `AIColumnPlan.matches`, so Spark's configured column resolver applies.
  - The check runs during `transformSchema`, and in `infer` before preparation (`AIPrompt.scala:582`).
  - `AIPromptRecoveryPreflightSuite.scala:32-56` covers `asset`/`ASSET` for both stages, with and without durable output, and asserts no HTTP request or storage.
- [x] **Format gating is Decisions-only and happens before IO**
  - `OpenAIPrompt.scala:628-629`: the predicate is by-name and evaluated only for Decisions.
  - Data URLs are classified from metadata only (`OpenAIAttachmentUtils.scala:431-442`).
  - Other paths go through `mayBeImagePath` (`315-318`) before `prepareFile`. `pathFilename` (`172-179`) ignores query strings and falls back to "attachment" for host-only URLs.
  - After the read, only JPEG, PNG, GIF and WebP MIME types pass. Through `MimeTypeExtensions` (`137-154`) and `categorizeFileType` (`320-337`), these four always become file type "image", so an accepted image never reaches the "unsupported" branch.
- [x] **WebP correction**
  - `OpenAIAttachmentUtils.scala:295-308` overrides the type only for data of at least 12 bytes with `RIFF` at offset 0 and `WEBP` at offset 8.
  - RIFF/WAVE and truncated RIFF data still return `audio/x-wav`, so `.wav` files stay audio.
- [x] **Native validation and request shape**
  - `NativeQuestions.scala:86-119` accepts instructions as a string or a non-empty part array, validates each part with `validatePart` (`215-246`), and limits question images to 128. `151-167` enforce the request-wide limit.
  - `prepareParts`/`omitNulls` (`177-189`) remove only null `file_id`/`detail`. Key order is preserved because `AIJson.OrderedMap` replaces keys in place (`AIJson.scala:23-44`).
  - The text-only envelope keys are unchanged.
- [x] **Hosted URLs**
  - `hostedImageUrl` (`NativeQuestions.scala:203-213`) requires an http/https scheme, ASCII-digit ports and a non-empty host from `URI.toURL`.
  - Underscore and IDN hosts are accepted as registry authorities. Empty hosts, `:443` without a host, `user@` without a host, opaque forms and malformed escapes are rejected.
  - URLs are forwarded unchanged and never fetched by the client. Diagnostics do not echo the URL.
- [x] **Durable output stays inline-only**
  - `AIDurableInput.scala:84-100` and `172` validate path columns as inline data URLs.
  - Question images are traversed through `externalBindings` → `content` → `inline` (`47-60`), so hosted question images are rejected in durable mode.
- [x] **Security and API surface**
  - No new credential flow.
  - Executors read files or URLs only for columns the pipeline author explicitly marks as `path`, using the same reader as Chat/Responses.
  - Decisions additionally rejects non-image extensions before IO. Error messages do not include paths.
  - `OpenAIAttachmentUtils` stays `private[services]` (`:22`). No new Params or public JVM signatures.
- [x] **Documentation**
  - `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 118-122, 142-149, 241-343 and 737-738 match the code. They cover `messagesCol` conflicts, blank-text error codes, the four formats and the pre-IO extension rule, Hadoop query and percent limitations, header validation after IO, inline-only durable output and the capability table.
  - The Python examples use only existing setters.
- [x] **Cancellation and live-test gating**
  - `AIPromptAttachmentSuite.scala:358-381` covers cancellation for Chat, Responses and Decisions.
  - `OpenAIDecisionsLiveSuite.scala` is gated with `assume` and never prints credentials.
- [ ] **Not executed**
  - I did not run any build or test; this review used read-only tools.
  - The driver's results (284 deterministic tests, style checks, compile and codegen) were treated as supplied evidence, not reproduced.
  - There is no live-provider acceptance evidence because the three live tests were canceled.

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
      "capabilityVerifiedAt": "2026-11-17T20:41:15.473355+00:00"
    }
  ],
  "overrideSource": {
    "artifactDirectory": "User input: \"I approve that exception, but tsince this might be a new PR you might need a new path\"",
    "scope": "One-time repository-root reviews/ exception for this Decisions path-column follow-up. PR #2853 is still open, so no replacement PR is needed. Use fresh filenames; do not overwrite or migrate prior artifacts.",
    "stageBehavior": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 2982f5a9b198fc3c35e601187887e18b57634faa"
  },
  "bounds": {
    "fastCyclesUsed": 17,
    "fastCycleLimit": 18,
    "reconstruction": "Seventeen bounded fix/integration cycles and fourteen pre-commit passes retained.",
    "precommitPassesUsed": 14,
    "precommitPassLimit": 16,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 15,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "e37ef3864d28d028a4ab89f9425fe4000fbd2e0da319fb3087d70b94145b7b19",
  "manifestSha256": "3021aafcd5019d15c2685b903ff8be4d21a506fc32ba62dd45cc2616a071cbd5"
}
