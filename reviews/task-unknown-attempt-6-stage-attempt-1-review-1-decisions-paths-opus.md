---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 1
round: 1
timestamp: "2026-11-17T11:22:55Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "53f55693dce0a8de9987ede8f342de5bbbae84d6aa06ede462ff587761cca909"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-6-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Prompt text checks happen before image IO.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 364-387 (`messages`), the Decisions path branch handles null or empty text with missing_input "input is null or empty". Whitespace-only text fails `NativeQuestions.validateDecisionsInput` with invalid_request. Both checks run before `generatedMessages` reads any image. The codes and messages match the non-path behaviour in `AIRequestAdapter.input` (lines 524-532) followed by `prepareDecisionsInput`.
- [x] **Message building stays lazy only for Decisions.** In `AIPrompt.prepareRequests` (lines 471-494), Decisions passes `() => inputMessages` to the adapter. Every other API still passes the messages by value, so their eager preparation is unchanged. `retainGeneratedMessages` applies only to Chat and Responses, so it does not evaluate the messages a second time.
  - The new image pre-count uses the same `Option(...).trim.nonEmpty` rule as `OpenAIPrompt.createMessagesForRow` (lines 603-616).
  - Each path column produces exactly one part, so the pre-count equals the real image count for any row that succeeds.
- [x] **Order of checks in `AIRequestAdapter.request` (lines 599-622) and `nativeEnvelope` (lines 571-586).** The checks run in this order, all before `messages()` is called:
  1. `selectedModel` (missing model)
  2. `requestVersion` (missing version)
  3. `mergedOptions` (malformed row options)
  4. null or invalid row questions
  5. `validateDecisionsImageCount(pathImageCount, questions)`

  System One receives `pathImageCount=0`, skips both Decisions branches, and keeps `(content, questions)` unchanged.
- [x] **Capability checks.** In `AIRequestAdapter.scala`:
  - In `validateCapabilities` (lines 247-264), Decisions accepts `columnTypes`, and accepts `fileSizeLimitMB` only when a column is typed "path".
  - Path columns require `inputMode='text'`.
  - `validateMessageConstruction` (lines 274-280) still rejects `messagesCol` combined with non-empty `columnTypes`.
  - `validateInputs` (lines 309-314) applies the text/path kind and string-type checks to every API.
  - System One and Embeddings keep the same error messages.
- [x] **Question and image validation.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`:
  - Lines 87-120: `instructions` may be text or a list of text/image parts.
  - Lines 152-188: the 128-image limit is checked on input plus question images, both before and after preparation.
  - `ListMap(fields.toSeq)` keeps the original key order. `prepareParts` only removes null `file_id`/`detail`.
  - Lines 207-242: hosted URLs must parse as a URI, use http(s), and have a non-empty `URL.getHost`. This accepts underscore and IDN hosts, and rejects empty hosts and malformed syntax.
  - URLs are never fetched, rewritten or included in error messages. Non-image data URLs and other schemes are rejected.
- [x] **WebP fix.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` lines 295-308, a file becomes `image/webp` only if it has at least 12 bytes, starts with RIFF and has WEBP at offset 8. WAV files still go through the JDK detection and stay audio.
  - `MimeTypeExtensions` (lines 137-141) maps `image/webp` to `webp`, so WebP files without an extension work.
  - Mismatched extensions, such as a WebP named `.wav` or a WAV named `.webp`, are categorized as unsupported (lines 322-333).
- [x] **Shared attachment reader.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` lines 630-656, `validateImageAttachment` rejects non-image files before they are converted to data URLs. `OpenAIDecisions` extends `AIPrompt` (`OpenAIDecisions.scala` line 15), so both stages use the same reader and the `useBoundedAttachments` size limit (`AIPrompt.scala` line 218).
- [x] **Durable output.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala` lines 161-193, `validateRows` treats every path column as "inline" for all APIs, so durable Decisions accepts only inline media. The `columnTypes` and `fileSizeLimitMB` params are unchanged, so persistence and resume behaviour are unaffected.
- [x] **Docs and existing tests.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (lines 116-122, 242-337 and 715-717) matches the code. The existing preflight suites (`AIPromptPreflightSuite.scala` lines 61-90 and `AIPromptRecoveryPreflightSuite.scala` lines 52-90) still expect only `inputMode` conflicts with `messagesCol` for Decisions, which the code still enforces.
- [x] **Considered but not reported:**
  - Per-row header checks and unpaired-surrogate checks on `apiVersion` and the body still run after IO. The spec covers only *missing* bindings.
  - Reads are unbounded when `fileSizeLimitMB` is unset. This already happens for Chat and Responses.
  - Hosted URLs that contain userinfo are passed to the provider unchanged. This follows the caller's input, and nothing is fetched or logged.
- [ ] **No commands or tests run by me.** This review used only read-only tools. I relied on the driver's evidence (73 final-patch tests, plus an earlier run of 231 tests) and did not reproduce it.

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
  "changeSize": "Nine files: the existing Decisions image/path work plus a small shared RIFF/WEBP MIME fix; most additions are tests and documentation.",
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
    "fastCyclesUsed": 8,
    "fastCycleLimit": 10,
    "reconstruction": "Eight fix/integration cycles and five consumed pre-commit attempts retained.",
    "precommitPassesUsed": 5,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 6,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "d17f57e314517e4b413cda3cdcd86e7ad15ddb04",
  "localHead": "330c9030d3d51aba5d06c6bfbc4c913f1d313c96",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "cd405e4e7fd1917971f930e08f72e1281d060585b24147f9f7f0676a0daba45a",
  "manifestSha256": "9c94359490b724df9389dc42e4f49a8a1d97fadce5ea68b7fcecffbb8c2c8771"
}
