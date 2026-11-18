---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 1
timestamp: "2026-11-17T14:31:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f5d253df6f0d2e2faa2c91d8bda44de2d2545ff40a6b10e036e2f565c81c58b7"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-11-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Question instructions** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` L86–118. Instructions can be non-blank text or a non-empty array of parts. Each part goes through `validatePart`, and question images are capped at 128.
- [x] **Request-wide image limit** — same file, L151–176. `prepareDecisionsRequest` counts input images and question images together against 128. `prepareParts` removes only null `file_id` and `detail`. String instructions pass through unchanged, and the AIJson-ordered `updated` keeps the wire order.
- [x] **Hosted image URLs** — same file, L203–237. URLs are forwarded unchanged. Building `URI`/`URL` and calling `getHost` does not fetch anything or resolve DNS. Underscore and IDN hosts pass through the registry-authority fallback. Empty hosts, malformed `%` escapes, `:bad` ports, spaces, and non-HTTP schemes are rejected (NativeQuestionsSuite L101–110 and L196–221). The inline data-URL branch keeps its existing behavior.
- [x] **Capability checks** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` L247–264, L274–280, L309–313.
  - Decisions accepts `columnTypes`. It accepts `fileSizeLimitMB` only when there are path columns, and path columns require `inputMode="text"`.
  - Combining `messagesCol` with path columns is still rejected.
  - System One and embeddings still reject `columnTypes`.
  - Path columns must be strings.
  - `columnTypes` defaults to `Map.empty` (OpenAIPrompt.scala L293), and `inputMode` defaults to `"text"` (AIPromptParams.scala L105).
- [x] **Order of checks before image IO** — AIRequestAdapter.scala L571–622. The order is: model → version → merged options → questions parse/validate → path-plus-question image count → `messages()` (IO) → final prepare. The three-argument `request` overload still prepares messages eagerly for Chat, Responses, System One and embeddings.
- [x] **Prompt checks** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` L362–386 and L453–496.
  - Null or empty Decisions prompts give `missing_input` "input is null or empty".
  - Whitespace-only prompts give `invalid_request` from `validateDecisionsInput`.
  - Both happen before `generatedMessages`.
  - The `imageCount` predicate (trim and non-empty) matches `createMessagesForRow`.
  - Only Decisions uses the lazy `inputMessages`.
  - `retainedMessagesName` (L295–300) applies only to generative APIs, so the Decisions output schema is unchanged.
- [x] **Attachment validation** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` L577–583, L602–618, L627–657.
  - Decisions builds a single user message from `input_text` parts, with no system prompt.
  - The format checks take by-name arguments and stop early unless the API is Decisions, so Chat/Responses never evaluate them.
  - Extension check before IO, then a post-read check against image/jpeg, png, gif and webp. Because of the extension check, an allowed MIME type always means `fileType == "image"`.
- [x] **Shared attachment reader** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` L295–318, L320–338, L395–442.
  - The RIFF/WEBP check runs before the JDK sniff, and other RIFF files still come back as `audio/x-wav`.
  - `mayBeImagePath` uses the same filename/extension source as `categorizeFileType`.
  - `dataUrlFileType` parses only the header.
  - `AttachmentSizeException` is an `IllegalArgumentException`, so it maps to `invalid_request`.
- [x] **Durable mode** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala` L20–33, L84–99, L166–194. Path cells must be data URLs for every API. `externalBindings` → `content` walks question instruction arrays and JSON or `messagesCol` inputs recursively, so hosted URLs are rejected. `fileSizeLimitMB` and `columnTypes` are part of the durable fingerprint.
- [x] **OpenAIDecisions** — `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisions.scala` L15–21. It extends `AIPrompt`, so it uses the same code path. No hand-written Python under `cognitive/src/main/python` mentions questions, `columnTypes` or decisions.
- [x] **Docs** — `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` L107–122, L245–350, L724–730, L743–759 match the code. This includes the Hadoop query-string and `%` limitation, the standard JPEG MIME name, the extension check, durable inline-only media, the combined 128-image count, and absent paths giving `missing_input`.
- [x] **Tests read** — `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` L122–564 and `NativeQuestionsSuite.scala` L101–227. They cover hosted, question, path (local, HTTP, extensionless HTTP, inline), pre-IO failures, WebP, WAV, the image limit, conflicting modes, and null-option normalization. Pass status comes only from the driver's supplied evidence.
- [x] **Checked and not counted as defects:**
  - Decisions `messagesCol` now reports "messages is null" after model, version, options and questions errors; every outcome is still a row error.
  - Row header and endpoint-text validation still happens after image IO; the spec doesn't list these as pre-IO checks.
  - The two over-limit messages differ only in wording.
- [ ] Tests and builds were not run (read-only tools). I relied on the supplied validation statement.

## Issues

### Issue 1: Hosted-URL check accepts RFC-invalid port syntax (signed or non-ASCII digits)
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 227–234
- **Description**: The hosted branch accepts any http/https URI whose `toURL.getHost` is non-empty.
  - To allow underscore and IDN hosts, `java.net.URI` falls back to a registry-based authority whenever server parsing fails. After that, only `java.net.URL` parses the port.
  - `URL` uses `Integer.parseInt`, which accepts a leading `+` or `-` and non-ASCII Unicode digits. It only rejects ports below -1.
  - So `https://example.invalid:-1/photo.png`, `https://example.invalid:+443/photo.png` and `https://example.invalid:٤٤٣/photo.png` pass validation and are forwarded.
  - Meanwhile `https://example.invalid:bad/image.png` is rejected (NativeQuestionsSuite L201).
  - RFC 3986 defines the port as `*DIGIT`, so these URLs are malformed. That contradicts the requirement that malformed URI syntax stays rejected and the intent shown by the existing `:bad` test.
- **Risk**: Minimal; there is no security impact, because SynapseML never fetches these URLs. Affected rows still cost a provider round-trip and show up as a provider `http_error` instead of a local `invalid_request`. Port validation is also inconsistent: `:bad` fails but `:-1` passes.
- **Suggested Fix**:
  - Add an ASCII-digit check on the raw port text:
    1. Take `uri.getRawAuthority`.
    2. Remove everything up to the last `@`.
    3. Skip past any `[...]` IPv6 literal.
    4. Require the text after the host's `:` to match `[0-9]*`.
  - This keeps underscore and IDN hosts accepted.
  - Add `:-1`, `:+443` and a non-ASCII-digit port to the rejected list in NativeQuestionsSuite ("Decisions rejects invalid image URLs…").
  - If this leniency is intended, document instead that only scheme, host and `java.net.URI` syntax are checked locally.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed; this pass ended and a fresh complete pass is required.
- **What changed**: A private hosted-URL validator checks the raw port against
  ASCII digits after excluding user info and bracketed IPv6. Existing scheme
  and nonempty-host checks remain, and the original URL is forwarded unchanged.
- **Why**: Java URL parsing accepts signed and Unicode-digit ports that do not
  match RFC 3986 port syntax. The helper also keeps part validation within the
  repository's cyclomatic-complexity limit.
- **How verified**: The invalid-URL regression failed before the guard because
  a malformed port was accepted. Final tests reject signed and Unicode-digit
  ports, including IPv6/user-info combinations, and preserve IDN/underscore
  hosts, IPv6 with/without ports, user info and empty ports. All 87 tests across
  four native-question/media/attachment suites pass with no failures or skips.
  Root ScalaStyle, test ScalaStyle, compilation, test compilation and codegen
  pass; generated Python wrappers and six documentation examples parse.
  Final nine-file patch SHA-256:
  `8ef0a08163bb1f9a3354343a75f4b9a77e028d9281173152599169b2736989d9`.

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
    "fastCyclesUsed": 13,
    "fastCycleLimit": 14,
    "reconstruction": "Thirteen fix/integration cycles and ten consumed pre-commit attempts retained.",
    "precommitPassesUsed": 10,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 11,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "4e2f1b8e7716879fd9be668dc410cfc5f003563d52fa915dd7882c99e413be68",
  "manifestSha256": "682075b9a0dd8e572a4d52c0c18d4e2b40510fc3d09b32de7fb17488f1e57684"
}
