---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 1
timestamp: "2026-11-17T23:13:30Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "b39cb032454639b611be551db361bc64b0f5ecf97fa51ada5241f5a574c3f107"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-16-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Configuration checks** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`, lines 247-281):
  - Decisions accepts only `messagesCol`, `columnTypes` and `fileSizeLimitMB`.
  - Path columns require `inputMode="text"`.
  - `validateMessageConstruction` reports `messagesCol` conflicts, including `fileSizeLimitMB`, before the new "requires a 'path' column" check.
  - System One and embeddings still reject `columnTypes`, because `GenerativeApis` (line 22) contains only Chat and Responses.
  - `fileSizeLimitMB` has no default and is not a global parameter (`OpenAIPrompt.scala`, lines 269-296), so checking `isSet` is correct.
- [x] **Input column cannot be a path column** (`AIRequestAdapter.scala`, lines 293-332):
  - The rule that exactly one input is configured forces path rows to use `promptTemplate` or `inputCol`.
  - `validateDecisionsInputColumn` uses `AIColumnPlan.matches`, which is `SQLConf.get.resolver` (`AIColumnPlan.scala`, line 37). Spark's resolver equivalence means any two names that resolve to the same column are caught.
  - Endpoint shape is still validated when the schema is checked, in `validate()` (lines 374-390).
- [x] **Checks run before image IO** (`AIRequestAdapter.scala`, lines 582-634):
  - Order before `messages()` is forced: `selectedModel`, `requestVersion`, `mergedOptions`, row questions parsed and validated, then `validateDecisionsImageCount(pathImageCount, …)`.
  - In `AIPrompt.scala` (lines 474-478), only Decisions keeps `inputMessages` lazy. Other APIs use the by-value overload, so their message preparation stays eager.
  - Null or empty text gives `missing_input` ("input is null or empty"). Whitespace-only text gives `invalid_request`. Both happen before `generatedMessages` (`AIPrompt.scala`, lines 364-386).
  - The path count (`exists(_.trim.nonEmpty)`) uses the same filter as `createMessagesForRow` (`OpenAIPrompt.scala`, lines 602-617).
  - `prepareDecisionsRequest` re-checks the combined image total after IO.
- [x] **Question instructions and request shape** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`, lines 86-119 and 140-184):
  - Instructions accept a string or a validated array of parts.
  - The 128-image limit is summed across all questions plus input images.
  - Null `file_id`/`detail` are removed from input and question image parts; other members are kept.
  - `prepareParts` is equivalent to the inline logic it replaced.
  - Envelope key order (questions, input/state, model) is unchanged.
- [x] **Hosted image URLs** (`NativeQuestions.scala`, `hostedImageUrl`/`validatePart`, about lines 203-245):
  - Scheme must be http or https, case-insensitive.
  - Underscore and international (IDN) hosts pass through the URI registry authority plus `URL.getHost`.
  - Ports must be ASCII digits; colons inside IPv6 brackets are excluded.
  - Empty or malformed hosts are rejected through `Try`.
  - URLs are forwarded unchanged, never fetched by the client, and not echoed in errors.
- [x] **Image format checks for path columns** (`OpenAIPrompt.scala`, lines 623-657; `OpenAIAttachmentUtils.scala`, lines 173-188 and 315-338):
  - The by-name `validateImageAttachment` short-circuits for Chat and Responses.
  - Data URLs are classified by declared MIME type before any size check.
  - `mayBeImagePath` rejects non-image extensions before IO.
  - After reading, the MIME type must be JPEG, PNG, GIF or WebP.
  - `pathFilename` never throws; it falls back to "attachment".
- [x] **WebP and WAV detection** (`OpenAIAttachmentUtils.scala`, lines 295-307 and 137-157):
  - WebP needs at least 12 bytes with RIFF at offset 0 and WEBP at offset 8.
  - Real WAV files and truncated RIFF files still resolve to `audio/x-wav`.
  - `MimeTypeExtensions` maps `image/webp` to `webp`, so extensionless WebP files are classified as images.
- [x] **Durable output and resume** (`AIDurableInput.scala`, lines 62-100 and 166-190; `AIPromptOutput.scala`, lines 183-196):
  - Path cells must be inline data URLs.
  - Question `image_url` values must be inline, both for questions set on the stage and for `questionsCol`.
  - The resume fingerprint includes `columnTypes` and `fileSizeLimitMB` through `extractParamMap()`.
  - The Decisions output schema is unchanged, because `retainedMessagesName` applies only to Chat and Responses (`AIPrompt.scala`, lines 295-300).
- [x] **Security:**
  - Path loading is opt-in through `columnTypes`, uses the existing shared reader, and is narrower than Chat/Responses (images only).
  - Hosted URLs in explicit parts are not fetched by the client.
  - IO failures map to a generic `input_io_error`.
  - Tests assert that errors do not disclose paths or URLs.
  - `OpenAIDecisionsLiveSuite.scala` (lines 23-33) skips via `assume` when credentials are absent and contains no secrets.
- [x] **Documentation and tests** (read, not run):
  - The Decisions section of `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` matches the code: `columnTypes` keys are DataFrame column names; row-level headers are validated after loading; non-image extensions are rejected before IO; the Hadoop query-string/percent limitation is documented; durable mode requires inline media.
  - Suites read: `AIPromptMediaInputSuite.scala` (lines 207-655), `NativeQuestionsSuite.scala` (101-200), `AIPromptRecoveryPreflightSuite.scala` (31-103), `AIPromptRecoveryAttachmentSuite.scala` (26-100), `AIPromptPreflightSuite.scala` (61-97), `AIPromptAttachmentSuite.scala` (358-380).
- [x] **Considered and not treated as defects:**
  - Hosted URL ports above 65535 are accepted; RFC 3986 syntax allows any digit string.
  - For Decisions with `messagesCol`, error precedence now follows the model and question checks. No IO is involved and error codes are unchanged.
  - Malformed but non-null row text or version encodings fail after IO; the spec only promises that missing bindings fail first.
  - Two equivalent definitions of a usable path cell (pre-IO count and message builder) are backed by the post-IO recount.
- [x] **Coverage limitation:** the supplied diff text was cut off after `NativeQuestions.omitNulls`. I reviewed the rest of the changed files (`OpenAIAttachmentUtils.scala`, `OpenAIPrompt.scala`, the test suites and the docs) from their current worktree content rather than as diff hunks. The driver states the worktree matches the frozen files.
- [ ] **Test and build execution:** not performed, because the review tools are read-only. All pass results are driver-supplied and were not reproduced.

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
    "fastCyclesUsed": 18,
    "fastCycleLimit": 18,
    "reconstruction": "Eighteen bounded fix/integration cycles and fifteen pre-commit passes retained.",
    "precommitPassesUsed": 15,
    "precommitPassLimit": 16,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 16,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "05a6a91f7184b4b33639b785368163082b8e4e00272a6f2d34e6beb7c7924dee",
  "manifestSha256": "9f187b6f3bb459c642b0940d9d73e048671a35a444d0dbed7e1a339e0a904116"
}
