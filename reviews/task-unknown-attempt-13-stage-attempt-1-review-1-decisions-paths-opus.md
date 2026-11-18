---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 1
timestamp: "2026-11-17T15:43:40Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "bfca3d5c824de7d37c4042541459bdf57a93da79ba47a789d0f6bbdddc81e173"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-13-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Hosted URL validation** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`, `hostedImageUrl` and `validatePart`). I traced how the code handles each URL form:
  - Underscore and IDN hosts: `java.net.URI` falls back to a registry-based authority, and the host is then read from `toURL().getHost`.
  - User info: the authority is cut at the last `@`, so user info cannot hide the port.
  - Ports: the ASCII-digit check runs only on a `:` that appears after any IPv6 `]`.
  - Accepted: underscore and IDN hosts, IPv6 with and without a port, user info, and an empty port.
  - Rejected: signed or Unicode-digit ports, empty hosts, invalid `%` escapes, spaces, non-HTTP(S) schemes and relative URLs.
  - Only `http`/`https` URIs reach `toURL`, and nothing is fetched. `prepareParts` drops only null `file_id`/`detail`, so signed query strings and percent escapes pass through unchanged.
  - These behaviors match the accept and reject cases in `NativeQuestionsSuite`.
- [x] **Question instructions and the image limit** (`decisions`, `prepareDecisionsRequest`, `validateDecisionsImageCount`, `questionImageCount`).
  - Instructions can be a string or validated `input_text`/`input_image` parts. `input_file` is still rejected, so no document support was added.
  - The 128-image limit counts shared input plus all question instructions together.
  - Every unchecked `Map.apply` lookup runs only after validation. Row questions are validated in `nativeEnvelope`; constant questions are validated in `validateNativeBindings` through `validate()`, which `infer` calls before `prepareRequests`. So these lookups cannot throw a `NoSuchElementException` that escapes the row-error mapping.
- [x] **Fail-before-IO order for Decisions** (`AIRequestAdapter.request`/`nativeEnvelope` and `AIPrompt.prepareRequests`/`messages`). The steps run in this order:
  1. `selectedModel`, then `requestVersion`, then `mergedOptions`.
  2. Questions are parsed and validated.
  3. The combined path-plus-question image count is checked.
  4. The `messages()` thunk checks the prompt text: empty or null gives `missing_input` "input is null or empty"; whitespace-only gives `invalid_request` from `validateDecisionsInput`.
  5. Only then does `generatedMessages` read the images.
  
  The Decisions route without path columns still uses `input()` and returns the same codes and messages. For other APIs, the lazy value is evaluated before `adapter.request`, so they keep eager message preparation. System One gets `() => messages` with a count of 0 and behaves as before.
- [x] **Image count matches the real request.**
  - The pre-IO count (`paths.count(... _.trim.nonEmpty)`) uses the same filter as `OpenAIPrompt.createMessagesForRow`.
  - Each usable path either produces exactly one `input_image` part or throws, so the count cannot over-reject.
  - `prepareDecisionsRequest` checks the limit again on the final content.
  - A row with no usable path still returns `missing_input` "attachment input is missing", the same as Chat and Responses.
- [x] **Decisions-only format checks** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`, `validateImageAttachment` and `wrapFileToMessagesList`).
  - The predicate is passed by name and guarded by `getApiType == "decisions" &&`, so Chat and Responses never evaluate it.
  - Data URLs are classified from their metadata only, before the body is decoded.
  - Unsupported file and HTTP extensions are rejected by `mayBeImagePath` before `prepareFile` reads anything. The filename comes from the URI path via Hadoop `getName`, so query strings and dotted directory names do not affect the extension.
  - After the read, the MIME allowlist (JPEG, PNG, GIF, WebP) applies to Decisions only.
  - Decisions sends a single user message with no system message.
- [x] **WebP correction** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, `inferMimeType`).
  - A file of at least 12 bytes with `RIFF` at offset 0 and `WEBP` at offset 8 is detected as `image/webp`.
  - WAV and other RIFF files still go to the JDK check and stay `audio/x-wav`.
  - `MimeTypeExtensions` maps `image/webp` to `webp`, so extensionless WebP files are classified as images.
  - An X bitmap (`#def`) in a `.png` is detected as `image/x-bitmap` and rejected by the Decisions allowlist only.
- [x] **Settings validation** (`AIRequestAdapter.validateCapabilities`, `validateMessageConstruction`, `validateInputs`).
  - Decisions allows `messagesCol` and `columnTypes`, and allows `fileSizeLimitMB` only when there is a path column.
  - `systemPrompt`, `dropPrompt` and the post-processing settings are rejected.
  - Path columns require `inputMode="text"` and string type.
  - Non-empty `columnTypes` with `messagesCol` is rejected, and System One still rejects `columnTypes`.
  - Without a path column, the `fileSizeLimitMB` error uses the generic "not supported by apiType='decisions'" text. That fits the documented capability table and is a wording choice, not a defect.
- [x] **Durable mode stays inline-only** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala`, `validateRows`, `externalBindings`, `content`, `inline`). Path cells, `messagesCol`, native JSON input, scalar questions and `questionsCol` cells must all use inline data URLs for `image_url`. Hosted question images and file or HTTP paths are therefore rejected before inference, and resume behavior is unchanged.
- [x] **Request shape and schema.**
  - In `AIJson.scala`, `OrderedMap.+` replaces an existing key in place, and in Scala 2.12 `updated` calls `+`. Rebuilding question objects therefore keeps member order, so the exact native shape is preserved.
  - `AIPrompt.retainedMessagesName` returns `None` for Decisions, so path columns add no output column. Tests assert that `transformSchema` matches the runtime schema.
- [x] **Security.** Hosted URLs in explicit image parts are never fetched by the driver or executors. Executor-side HTTP reads for path columns are existing shared reader behavior, required here by the spec, and Decisions narrows them with the image-only allowlist. Validation errors do not echo the supplied URL or data.
- [x] **Documentation** (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, Decisions image section and capability table). The docs match the code on:
  - `columnTypes` keys being DataFrame column names;
  - the Hadoop query-string and percent-escape limitation;
  - which checks run before IO;
  - extension rejection without a read, while supported or extensionless names are read before classification;
  - standard `image/jpeg` naming;
  - durable inline-only media;
  - the combined 128-image limit;
  - no Files API, PDF, Office or audio support.
- [x] **Tests inspected** (not run):
  - `AIPromptMediaInputSuite`: hosted and inline payload preservation using `.invalid` hosts; path loading from local, HTTP, extensionless and inline sources with copy and save/load; pre-IO text and metadata failures with zero image or inference requests; extension rejection with and without a size limit; X bitmap in a `.png`; WebP and WAV across APIs; 127/128 combined-limit cases.
  - `NativeQuestionsSuite`: the URL accept and reject cases.
  - `AIPromptPreflightSuite`: `messagesCol` conflicts.
- [x] **Existing behavior checked and not counted as findings:**
  - Extensionless JPEGs whose header the JDK does not recognize are rejected (documented).
  - A `.png` containing unrecognized bytes is sent as `image/png` by the shared classifier, and the provider validates it.
  - Hadoop path loading cannot preserve query strings or percent escapes (documented).
- [ ] **Tests were not run by this reviewer.** The tools available were read-only. Pass/fail status comes from the validation evidence supplied by the driver.
- [ ] **Coverage limitation.** The diff in the prompt was cut off inside `hostedImageUrl`. I reviewed the rest of the affected files from current worktree source, tests and docs, not from the diff hunks or merge-base files. I did not independently verify the patch SHA-256 or the ten-file list.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent assessment of the frozen ten-file patch",
    "model": "Installed review-code complementary-provider and capability-first rules",
    "reasoning": "Installed review-code maximum supported reasoning",
    "stageBehavior": "Trusted repository review-gauntlet.md: six sequential themes",
    "artifactDirectory": "Explicit driving-user exception",
    "safetyStopPolicy": "Explicit driving-user authorization to exceed previous limits; next attempt uses a finite 3600-second timeout per reviewer and preserves consumed counters."
  },
  "changeSize": "Ten files: Decisions hosted/question/path images, shared WebP classifier correction, focused error consistency and examples; most additions are tests and documentation.",
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
    "fastCyclesUsed": 15,
    "fastCycleLimit": 16,
    "reconstruction": "Fifteen fix/integration cycles and twelve pre-commit attempts retained. Two further bounded passes authorized under the user's extension.",
    "precommitPassesUsed": 12,
    "precommitPassLimit": 14,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 13,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "7c09664fd9bcc7c99f3cf510e66758ecbf2ab6bd092c616ddfbbed63161bd9e0",
  "manifestSha256": "57dfc0a2361d8831dab60e69e6c1e76461aed11b8f8f4e77c2a7b72acc03371f"
}
