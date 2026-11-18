---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 1
timestamp: "2026-11-17T15:12:59Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1a677576130023457d7e89c66f9e1a8e550896aee6e0d6400f629003f402ccb3"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-12-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Row preparation.** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` ~350-387 and 453-494:
  - Decisions builds messages only when a lazy thunk is called.
  - `imageCount` skips blank cells with `_.trim.nonEmpty`, the same filter as `OpenAIPrompt.createMessagesForRow` (`OpenAIPrompt.scala` 608-617).
  - Null or empty prompt text gives `missing_input` ("input is null or empty"). Whitespace-only text gives `invalid_request`. Both happen before `generatedMessages`.
  - Other APIs pass the lazy value by value, so they still build messages eagerly.
  - `retainedMessagesName` (295-300) excludes Decisions, so messages are never built twice.
- [x] **Request order.** `AIRequestAdapter.scala` 569-622 runs model → version → `mergedOptions` → questions null and row checks → `validateDecisionsImageCount(pathImageCount, …)` → `messages()` → `prepareDecisionsRequest` → `endpoint`.
  - Chat, Responses, Embeddings and System One build the same envelope as before.
  - The 3-argument overload passes a count of 0. Its callers are `AIPromptPreflightSuite.scala:178` and `AIPromptSuite.scala:153`.
- [x] **Capability and schema gates.** `AIRequestAdapter.scala` 247-280 and 292-321; `AIPromptParams.scala:105` (`inputMode` defaults to "text"); `OpenAIPrompt.scala:293` (`columnTypes` defaults to empty).
  - Path columns must be StringType.
  - These stay rejected: path columns with JSON mode, path columns with `messagesCol`, and `columnTypes` on System One or Embeddings.
  - Decisions accepts `fileSizeLimitMB` only when a path column exists.
- [x] **`NativeQuestions.scala` 86-245.**
  - Instruction arrays use the same `validatePart` check as input parts.
  - The 128-image limit is checked per question set and again for the whole request.
  - `prepareParts` removes only null `file_id` and `detail`, and `updated` keeps key order.
  - I traced `hostedImageUrl` for IPv6 with and without a port, user info, multiple `@`, empty ports, signed and non-ASCII-digit ports, underscore and IDN hosts, empty hosts, bad escapes and non-HTTP schemes. It only parses the URI/URL; it does not fetch, resolve or rewrite.
- [x] **Attachment reader.**
  - `OpenAIPrompt.scala` 628-660:
    - The Decisions-only check takes a by-name predicate, so Chat and Responses never evaluate it.
    - Data URL metadata is classified before decoding, and the extension check runs before `prepareFile`.
    - After the read, only `image/jpeg`, `image/png`, `image/gif` and `image/webp` are allowed.
  - `OpenAIAttachmentUtils.scala` 100-112, 137-188, 295-337 and 384-442:
    - The RIFF + `WEBP` tag is checked before the JDK maps RIFF to WAV, so real WAV ("WAVE") stays `audio/x-wav`.
    - `image/webp` maps to `webp`, and IO errors are sanitized.
  - Because both checks must pass, `responsesMessage` (490-512) can only emit `input_image` for Decisions.
- [x] **Durable mode.** `AIDurableInput.scala` (unchanged) still requires inline data URLs for path cells, question instructions and JSON input. `AIPromptRecoveryAttachmentSuite.scala` 26-104 covers resume.
- [x] **Tests read:**
  - `AIPromptMediaInputSuite.scala` 122-592 covers:
    - hosted URLs, question-image persistence, and path columns through both stages, copies and load, with exact payloads;
    - pre-IO checks for prompt, metadata, extension and image limit, proven by image-server request counters;
    - WebP across four stages, extensionless WAV, IO and size errors, input-mode and System One gates, and null-option normalization.
  - `NativeQuestionsSuite.scala` 100-240, `AIPromptPreflightSuite.scala` 61-90, and `AIPromptRecoveryPreflightSuite.scala` 52-90.
- [x] **Docs.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 107-122, 240-345 and 728-729 match the code.
- [ ] **Not done: running tests or reproducing the driver's validation.** This review is read-only and based on code inspection.
- [ ] **Partly confirmed: the patch text after the `validatePart` hunk was truncated.** I reviewed those hunks from the matching worktree source files, so I did not check the nine-file patch boundaries line by line.

## Issues

### Issue 1: No test covers the rejecting side of the conditional Decisions capability gate
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala (missing coverage in cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptPreflightSuite.scala)
- **Line(s)**: AIRequestAdapter.scala 247-258; AIPromptPreflightSuite.scala 61-90; AIPromptMediaInputSuite.scala 206-260, 515-536
- **Description**: The new check is `api == "decisions" && decisionsOptions(p)`, and it allows `fileSizeLimitMB` only when a `"path"` column exists.
  - Tests cover only the accepting side (`setFileSizeLimitMB(1)` with a path column, AIPromptMediaInputSuite.scala:223) and the rejections for `inputMode`, `messagesCol` with `columnTypes`, and System One `columnTypes`.
  - The Decisions branch of the preflight test checks only `inputMode` (AIPromptPreflightSuite.scala:64-65). The docs (lines 118-122) also promise that Decisions with `messagesCol` rejects `systemPrompt`, `fileSizeLimitMB`, `dropPrompt` and non-empty `columnTypes`.
  - If `fileSizeLimitMB` became allowed unconditionally, or `systemPrompt` were added to `decisionsOptions`, the current suite would still pass.
  - The gate matters more now. Decisions-generated messages never include a system message (OpenAIPrompt.scala:615), so a wrongly accepted `systemPrompt` would be silently dropped. A wrongly accepted `fileSizeLimitMB` on JSON or message input would do nothing.
  - Also, the rejection without a path column says "fileSizeLimitMB is not supported by apiType='decisions'". It does not mention the path-column condition documented at line 729.
- **Risk**: Nothing misbehaves today. A future edit could start silently ignoring settings without failing CI, and the current error text may confuse users.
- **Suggested Fix**:
  - Extend the Decisions branch of the existing preflight test with the same `systemPrompt`, `columnTypes` (path and text), `fileSizeLimitMB` and `dropPrompt` cases used for Chat and Responses. Run them for both `AIPrompt(apiType="decisions")` and `OpenAIDecisions`, and assert the error happens before HTTP.
  - Add one non-`messagesCol` case: `inputCol` with no path column plus `fileSizeLimitMB` is rejected, and `systemPrompt` or `dropPrompt` with a path column is rejected.
  - Optionally, change the message when there is no path column to state the condition (e.g. "fileSizeLimitMB requires a Decisions image path column") and assert it.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Coverage added; this pass ended and a fresh complete pass is required.
- **What changed**: The existing message-preflight test now exercises
  `systemPrompt`, path/text `columnTypes`, `fileSizeLimitMB`, and both
  `dropPrompt` values for both Decisions stages. A media-suite case rejects
  `fileSizeLimitMB` with no path column (including text-only `columnTypes`)
  and rejects `systemPrompt`/`dropPrompt` when path columns exist.
- **Why**: Both accepting and rejecting configurations need explicit coverage.
  The new case lives in the media suite to respect the preflight suite's
  800-line limit. The optional error-message rewrite was not adopted: current
  behavior and diagnostics are correct, and the guide states the path condition.
- **How verified**: The new cases assert failure from both `transformSchema`
  and `transform`, the relevant parameter name, and zero inference requests.
  All 128 tests across five suites pass with no failures or skips. Root
  ScalaStyle, test ScalaStyle, compilation, test compilation and codegen pass;
  generated Python wrappers and six documentation examples parse.
  No production code changed in this coverage fix. Final ten-file patch SHA-256:
  `7c09664fd9bcc7c99f3cf510e66758ecbf2ab6bd092c616ddfbbed63161bd9e0`.

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
    "fastCyclesUsed": 14,
    "fastCycleLimit": 14,
    "reconstruction": "Fourteen fix/integration cycles and eleven pre-commit attempts retained.",
    "precommitPassesUsed": 11,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 12,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "8ef0a08163bb1f9a3354343a75f4b9a77e028d9281173152599169b2736989d9",
  "manifestSha256": "750758a90cfc44dc50bf3bd2b362c06535d3e454d72c34ffd6e2425ea9f99cbe"
}
