---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 10
stage_attempt: 1
round: 1
timestamp: "2026-11-17T14:02:20Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "9060b73760940d4d598b14591b9bd0b73aca6528f3bc25fa6abb3df5fd0e3aee"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-10-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Eager message preparation outside Decisions.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 473–478, APIs other than Decisions pass a strict `Option` to `request`. That forces the lazy `inputMessages` before the call. In `AIRequestAdapter.scala` lines 597–598, the three-argument overload passes the already-evaluated value as `() => messages`. A search of `services/` for `adapter.request(` found only this one call site.
- [x] **Decisions pre-IO order.** In `AIRequestAdapter.scala` lines 600–604 and 571–580, a Decisions request runs these steps in order:
  1. `selectedModel`
  2. `requestVersion`
  3. `mergedOptions`
  4. Questions null check, parse and validation
  5. `validateDecisionsImageCount(pathImageCount, …)`
  6. `messages()`, which is the first image IO
  7. `prepareDecisionsRequest`

  This matches the spec's list of failures that must happen before IO. `AIPromptMediaInputSuite.scala` lines 326–362 and 452–478 assert that the image server receives zero or one request.
- [x] **Prompt validation.** In `AIPrompt.scala` lines 377–383, null or empty prompt text returns `missing_input` with "input is null or empty". This is the same text as the non-path `input()` message at `AIRequestAdapter.scala` line 530. Whitespace-only text goes through `validateDecisionsInput` and returns `invalid_request`. Both checks run before `generatedMessages`. The test at lines 262–291 asserts zero image and inference requests.
- [x] **Image count consistency.** The pre-IO count at `AIPrompt.scala` line 476 uses the same `trim.nonEmpty` rule as `createMessagesForRow` (`OpenAIPrompt.scala` lines 608–610). Each usable path produces exactly one image part. Decisions-only checks reject any part that is not an image (`OpenAIPrompt.scala` lines 628–653). So the pre-IO count and the count after preparation always agree.
- [x] **Hosted URL validation.** In `NativeQuestions.scala` lines 215–235, the hosted branch parses the URL with `java.net.URI`. It requires an `http` or `https` scheme and a non-empty `toURL.getHost`. It never opens a connection, and its error messages do not repeat the URL. Tests cover both sides:
  - Accepted and forwarded unchanged (`AIPromptMediaInputSuite.scala` lines 122–162): signed query strings, `%2B`, an explicit port, underscore hosts and IDN hosts.
  - Rejected (`NativeQuestionsSuite.scala` lines 127–128 and 198–205): empty hosts, `:443` with no host, `user@` with no host, `%xy`, `file` and `ftp`.
- [x] **Decisions-only format guards.** Checked in `OpenAIPrompt.scala` lines 313 and 628–653, and `OpenAIAttachmentUtils.scala` lines 156–157, 315–318 and 431–442:
  - The by-name `validateImageAttachment` check runs only for Decisions, so Chat/Responses never evaluate it.
  - `mayBeImagePath` uses the same `attachmentFilename`/`fileExtension` logic as `prepareFile`. URI query strings are stripped first.
  - The image extension set is exactly jpg, jpeg, png, gif and webp.
  - After the read, Decisions allows only the four standard MIME types.
  - Inline data URLs with `image/jpg`, tiff, bmp or x-bitmap map to `"bin"`, so they are classified as unsupported.
- [x] **WebP correction.** In `OpenAIAttachmentUtils.scala` lines 295–307, the WebP override needs at least 12 bytes, `RIFF` at offset 0 and `WEBP` at offset 8. Any other RIFF file still goes to the JDK sniffer and gets `audio/x-wav`, so WAV stays audio. Issue 1 covers the missing test for this.
- [x] **Capability validation.** In `AIRequestAdapter.scala` lines 247–264, 274–280 and 309–314:
  - Decisions allows `columnTypes`. It allows `fileSizeLimitMB` only when there are path columns.
  - System One and Embeddings rejections are unchanged.
  - Path columns require `inputMode="text"` and a string column type.
  - `messagesCol` combined with a non-empty `columnTypes` is rejected.

  These are covered by the test at lines 480–501.
- [x] **Durable output stays inline-only.** In `AIDurableInput.scala` lines 19–33, 84–100, 172 and 183–186, durable path cells must be data URLs. Image parts in constant questions, `questionsCol` and JSON input are checked recursively through `externalBindings` → `content` → `inline`. Hosted URLs are therefore still rejected in durable mode.
- [x] **Output schema unchanged.** In `AIPrompt.scala` lines 295–300 and 589–595, `retainedMessagesName` applies only to generative APIs. The Decisions output schema is unchanged, and the lazy messages are never forced for retention. Tests at lines 206–260 assert schema equality across copy and save/load.
- [x] **No public API changes.** `AIRequestAdapter` is `private[ai]`, `NativeQuestions` is `private[services]` and `mayBeImagePath` is `private[openai]`. No public JVM signatures or Params changed. `OpenAIDecisions.scala` lines 15–21 inherits every `AIPrompt` Param. `cognitive/src/main/python` contains no hand-written `columnTypes` or Decisions logic.
- [x] **Docs match the code.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 245–343 and 728–729 describe what the code does. That includes the pre-IO checks, the Hadoop query-string and percent-escape limit, inline-only durable media, no document support and System One staying text/JSON only.
- [x] **Reviewed and accepted, no finding.** With `messagesCol`, Decisions now builds messages after model, version, options and questions validation (`AIPrompt.scala` lines 474–477). For a row with several problems, a different error may now be reported first. No IO is involved, and the order now matches Decisions text and JSON input, so I did not treat this as a defect.
- [ ] **Tests and builds were not run.** This review is read-only. I relied on the driver's reported validation (74 tests, ScalaStyle, compile and codegen) without reproducing it.

## Issues

### Issue 1: The "WAV stays audio" side of the RIFF/WEBP classifier change has no regression test
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala; cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala
- **Line(s)**: OpenAIAttachmentUtils.scala 295–307; AIPromptMediaInputSuite.scala 293–324, 367, 380
- **Description**: The shared `inferMimeType` now maps a RIFF file to `image/webp` when the format tag at offset 8 is `WEBP`. Every other RIFF file still goes to the JDK sniffer. The code is correct, but no test sends WAVE bytes through this classifier:
  - The only RIFF/WAVE file fixture (line 367) is named `.wav`. Decisions rejects it in `mayBeImagePath` before reading it.
  - The `data:audio/wav` case (line 380) uses its declared MIME type and skips `inferMimeType`.
  - No Chat/Responses test reads a WAV attachment. The speech suites use WAV files with the Speech SDK, not `OpenAIAttachmentUtils`.

  The WebP test at lines 293–324 covers only the positive case.
- **Risk**: A later change could treat every `RIFF` prefix as WebP or read the tag at the wrong offset, and all tests would still pass. Chat/Responses would then send WAV bytes as `image/webp`. Extensionless WAV files would also pass Decisions' post-read image allowlist. Both break the stated "WAV stays audio" rule.
- **Suggested Fix**: Add one focused case next to the existing extensionless `prepareFile` test in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptMultimodalRequestSuite.scala` (about lines 280–293):
  - An extensionless file with a minimal `RIFF....WAVEfmt ` header should return `("audio", "audio/x-wav")` when `audioExtensions = Set("wav")`.
  - Extensionless RIFF/WEBP bytes should return `("image", "image/webp")`.

  Optionally, add an extensionless HTTP RIFF/WAVE asset to the Decisions rejection table. It should get the JPEG/PNG/GIF/WebP error and make zero inference requests.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed coverage gap; this pass ended and a fresh complete pass is required.
- **What changed**: Added a public-transformer regression using a WAV fixture
  under extensionless filesystem and HTTP paths. Both Decisions stages,
  Responses and Chat read those paths, report their audio/unsupported-image
  diagnostic, and make zero inference requests. No production code changed.
- **Why**: Extensionless names bypass the early extension rejection, exercising
  the shared RIFF content classifier rather than the filename or data-URL
  metadata path. The Chat assertion verifies `audio/x-wav`; Responses and
  Decisions verify their exact local diagnostics.
- **How verified**: All 75 tests across `AIPromptMediaInputSuite` (24),
  `AIPromptAttachmentSuite` (28), and `OpenAIPromptOfficePathSuite` (23) passed,
  with no failures or skips. Root ScalaStyle, test ScalaStyle, compilation,
  test compilation and codegen passed. Generated Python wrappers and six
  documentation examples parse.
  Final nine-file patch SHA-256:
  `4e2f1b8e7716879fd9be668dc410cfc5f003563d52fa915dd7882c99e413be68`.

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
    "fastCyclesUsed": 12,
    "fastCycleLimit": 14,
    "reconstruction": "Twelve fix/integration cycles and nine consumed pre-commit attempts retained.",
    "precommitPassesUsed": 9,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 10,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "2f601710028848d175c595fc795321b27f1009be2a1b83a231f237df4bea33b9",
  "manifestSha256": "dfa2d01602e6154c7afaaab40bf451eaa1b34df93f010a65b39c22d46d55d5e3"
}
