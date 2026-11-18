---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 1
timestamp: "2026-11-17T18:16:14Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a2d559a317a2cf498d7a38cbf33a5bfb8915c7e5e84914ba7a2fa90fa109ba65"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-14-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Prompt checks and pre-IO ordering.** Files: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 364–386 and 453–499, `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` 571–622.
  - For Decisions, `inputMessages` is a `lazy val` passed as a thunk. Evaluation order: `selectedModel`, `requestVersion`, `mergedOptions`, question parse/validation, `validateDecisionsImageCount`, then `messages()`, where image IO happens.
  - Null or empty prompt text gives `missing_input` "input is null or empty", the same text as text-only `input()` (525–531). Whitespace-only text gives `invalid_request` through `validateDecisionsInput`, before `generatedMessages`.
  - Other APIs force the lazy value before calling the 3-argument overload, so eager preparation is unchanged.
  - The catch block (484–496) still maps read, size, missing and invalid errors to row errors. `AttachmentSizeException` extends `IllegalArgumentException` (`OpenAIAttachmentUtils.scala` 24–30).
- [x] **Pre-IO image count is exact.**
  - Each non-blank path produces exactly one `input_image` (`OpenAIAttachmentUtils.scala` `responsesDataUrlMessage` 444–466 and `responsesMessage` 490–512), and Decisions rejects non-images first.
  - The count uses the same `trim`/non-empty rule as `createMessagesForRow` (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` 602–618).
  - Path columns must be `StringType` at planning (`AIRequestAdapter.scala` 309–314), so `getString` is safe.
  - A row with no usable path gives `missing_input` (`AIPrompt.scala` 353–362), as documented.
- [x] **Capability gating** (`AIRequestAdapter.scala` 247–265, 274–290).
  - System One and embeddings still reject `columnTypes` and `messagesCol`.
  - Decisions allows `messagesCol` and `columnTypes`, and allows `fileSizeLimitMB` only with a path column. `systemPrompt`, `dropPrompt` and post-processing are rejected. Path columns require `inputMode="text"`.
  - `validateMessageConstruction` still rejects non-empty `columnTypes`, `fileSizeLimitMB` and `inputMode` with `messagesCol`. `stateCol` stays System One-only (262).
  - `columnTypes` defaults to an empty map (`OpenAIPrompt.scala` 293). The docs' capability table (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 735–737) matches.
- [x] **Question validation and wire shape** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` 86–184).
  - Instructions must be non-blank text or a non-empty array of validated parts; other types get a controlled diagnostic.
  - The question-only and combined (input plus questions) 128-image limits are both enforced.
  - `obj(question)("instructions")` is reached only after validation: constant questions in `validateNativeBindings` (282–290), per-row questions at 577.
  - `prepareParts` drops only null `file_id`/`detail` and keeps extension members. Rebuilt question objects keep field order through `AIJson`'s ordered map.
- [x] **Hosted-URL validation** (`NativeQuestions.scala` 203–213).
  - The scheme check short-circuits before `uri.toURL`, so there is no fetch or DNS lookup.
  - Ports must be ASCII digits. IPv6 brackets and user info are handled.
  - Rejected: opaque `https:host`, empty hosts, malformed syntax and duplicate ports.
  - Accepted: underscore and IDN hosts (parsed as registry authorities).
  - The original string is forwarded unchanged, and diagnostics do not echo the URL.
- [x] **Attachment classification** (`OpenAIAttachmentUtils.scala` 294–338, 395–442; `OpenAIPrompt.scala` 312–314, 628–658).
  - WebP is detected only for files of at least 12 bytes with `RIFF` and `WEBP` tags. Other or truncated RIFF files keep the JDK's `audio/x-wav`.
  - `validateImageAttachment` takes its predicate by name and checks the API type first, so Chat/Responses never evaluate the Decisions prechecks or allowlist. Legacy `OpenAIPrompt` cannot select `"decisions"` (192).
  - `mayBeImagePath` runs before `prepareFile`, so known non-image extensions are rejected without IO or size checks.
  - `dataUrlFileType` checks only the metadata, before data-URL size checks, and keeps the existing no-comma diagnostic. The post-read allowlist rejects `image/x-bitmap`.
- [x] **Durable mode** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala` 44–58, 84–91, 162–195).
  - The inline check on path columns does not depend on the API (172).
  - Question images, whether constant or from `questionsCol`, pass through `externalBindings`, `content` and `inline`. Hosted URLs and paths are rejected before inference, consistent with `docs/Explore Algorithms/OpenAI/Durable AI results.md` 280–352.
- [x] **Template and retained-message behavior.** Path placeholders become filenames for any API (`AIPrompt.scala` 419–451). Decisions never retains generated messages, because `retainedMessagesName` (295–300) applies only to generative APIs.
- [x] **Documentation accuracy** (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 241–345, 725–760). The following match the code:
  - finite limit reads up to limit + 1 byte; unset or infinite limits read the whole file;
  - the Hadoop query-string/percent limitation;
  - which checks run before IO, and that header validation runs after it;
  - standard MIME naming and the `.jfif`/`.jpe` guidance;
  - the durable inline requirement;
  - env-var names and imports, which match earlier examples (56–57, 177–179, 218–220).
- [x] **Behavior change judged not a defect.** Decisions `messagesCol` rows are now prepared lazily too. A row with both a null message cell and, for example, a missing model binding or malformed questions now reports the latter first. Successful rows are unchanged, the order matches existing Decisions `inputCol` precedence, and no test under `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai` asserts the old order.
- [x] **API and persistence compatibility.** No new Params. `AIRequestAdapter` is `private[ai]` and keeps the 3-argument overload. The helpers are package-private. `OpenAIDecisions` inherits `columnTypes` and `fileSizeLimitMB` without hiding them (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisions.scala` 15–21).
- [x] **Tests reviewed by reading, not running.** The preflight logic (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptPreflightSuite.scala` 61–97) matches the capability rules. The live suite is gated on credentials and `SYNAPSEML_DECISIONS_IMAGE_URL` (`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisionsLiveSuite.scala` 28–32, 76), with no embedded secrets. I ran no tests or builds; the driver's results are taken as supplied, not reproduced.
- [ ] **Provider-side acceptance not verified.** Whether the provider accepts hosted, underscore or IDN image URLs and image question instructions is unknown, because the credential-gated live tests were skipped and I made no network requests.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent assessment of the frozen eleven-file patch",
    "model": "Installed review-code complementary-provider and capability-first rules",
    "reasoning": "Installed review-code maximum supported reasoning",
    "stageBehavior": "Trusted repository review-gauntlet.md: six sequential themes",
    "artifactDirectory": "Explicit driving-user exception",
    "safetyStopPolicy": "Explicit driving-user authorization to exceed previous limits; next attempt uses a finite 3600-second timeout per reviewer and preserves consumed counters."
  },
  "changeSize": "Eleven files: Decisions hosted/question/path images, shared WebP classifier correction, focused error consistency and examples; most additions are tests and documentation.",
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
    "fastCyclesUsed": 16,
    "fastCycleLimit": 16,
    "reconstruction": "All sixteen fix/integration cycles and thirteen pre-commit attempts retained.",
    "precommitPassesUsed": 13,
    "precommitPassLimit": 14,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 14,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "008f86591bfcb8609c7699e9c7f0f25d633629fe26c417c83762cee9088eb8c8",
  "manifestSha256": "fc7116119e7ca89a2cf89995abb73e0f27a700497c40a95c73faefb898b47651"
}

## Driver evidence-retention note

Attempt 1 timed out without a finalized verdict and does not count as review
coverage. Its diagnostic transport remains preserved privately.

Attempt 2's original artifact contains a local workstation path and is excluded
from public publication under the unsafe-draft rule. The original is preserved
privately with SHA-256
`3421b1f7b0cb8ddd3c1753009f35befb191aed6e5942db09efbefbdaf986b97c`.
Its findings were retained and addressed before later independent reviews; it is
not counted as qualifying evidence. Subsequent public-safe review artifacts
retain their original findings and appended dispositions.
