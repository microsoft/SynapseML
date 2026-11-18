---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 1
timestamp: "2026-11-17T09:13:11Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "06b7fbc99ca4b4930b38a8ae8c78ce3cd00870e8cf07e69044d0ff3f0c9a06d3"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-3-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the full supplied patch for all eight files. I also read the changed production code where it sits: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 349-384 and 452-492, `AIRequestAdapter.scala` lines 247-264 and 571-585, `NativeQuestions.scala` lines 16-36, 87-185 and 196-240, and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` lines 241-266, 499-513 and 577-657.
- [x] **Missing prompt fails before image IO.** In `AIPrompt.scala` lines 377-383, null or empty Decisions text raises `AIMissingInputException("input is null or empty")` before `generatedMessages`. The first IO is in `createMessagesForRow` → `buildContentParts` at `OpenAIPrompt.scala` line 625, so missing text never reaches it. The regression test (`AIPromptMediaInputSuite.scala` lines 260-285) asserts zero image-server and inference requests for both stages and both binding styles. It covers only `None` and `""`; whitespace-only text is Issue 1.
- [x] **Capability gating.** `AIRequestAdapter.scala` lines 247-264 now accept `columnTypes` for Decisions. Its default is `Map.empty` (`OpenAIPrompt.scala` line 293), and values are limited to exactly `text` or `path` (lines 244-262). `fileSizeLimitMB` is accepted only when a path column exists.
  - System One still rejects both parameters with the same message.
  - Path columns with JSON input mode give an explicit error.
  - `messagesCol` combined with path columns still fails in the existing `validateMessageConstruction`.
- [x] **Native request shape.** In `nativeEnvelope`, row-level questions are validated at line 577 before `prepareDecisionsRequest` runs at line 582. So the `obj(question)("instructions")` lookups at `NativeQuestions.scala` lines 163 and 171 cannot throw an uncaught `NoSuchElementException`.
  - Questions are rebuilt with a `ListMap`, which keeps field order, so text-only questions serialize exactly as before.
  - `prepareParts` removes only null `file_id`/`detail` fields from `input_image` parts.
  - The combined limit (input plus question images ≤ 128) and the question-only limit are both enforced.
- [x] **Image URL validation.** In `validatePart`, URLs starting with `data:image/` (any case) still go through the existing RFC 2397 header, base64 and non-empty checks. Any other URL must parse as a `java.net.URI` with an http or https scheme and a non-empty host. Non-image data URLs therefore fail.
  - URLs are forwarded unchanged, so signed query strings survive; the test at `AIPromptMediaInputSuite.scala` lines 154-158 asserts this.
  - No error message includes the URL.
- [x] **Shared attachment reader.** Decisions uses the Responses data-URL path and the bounded `prepareFile`/`responsesMessage` path. It builds a single user message with no default system message and rejects any attachment that isn't `input_image`.
  - `AIPrompt` overrides `transform` (line 334), and the legacy `OpenAIPrompt` doesn't support Decisions, so the legacy UDF path is untouched.
  - `OpenAIDecisions` extends `AIPrompt` with only the Decisions API type, so it behaves the same.
- [x] **No error leakage.** `AttachmentReadException` becomes a generic `input_io_error` (`AIPrompt.scala` lines ~480-483). Bounded reads raise "Attachment exceeds the N-byte limit" with no filename (`OpenAIAttachmentUtils.scala` line 360), and data-URL size errors say only "Data URL attachment" (lines 229 and 255).
- [x] **Durable output and resume.** `AIDurableInput.scala` still enforces inline-only media for path columns, and recursively for question `image_url` values (line ~96), for both fixed and row-level questions. `AIPromptOutput.fingerprint` (lines 186-208) hashes the full `extractParamMap()`, so the newly allowed Decisions `columnTypes` and `fileSizeLimitMB` are part of resume identity.
- [x] **Scope.** The only main-code callers of `NativeQuestions` are in `AIRequestAdapter` (lines 288, 577 and 582). System One instructions remain text-only (`NativeQuestions.scala` line 64). The Decisions output schema is unchanged because generated messages are retained only for generative APIs. There is no hand-written Python gating under `services/ai`.
- [x] **Docs.** The Decisions sections and capability table in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` match the code: hosted and inline images, question image parts, path columns, durable inline-only media and the Hadoop query-string limitation.
- [ ] I did not run any tests; my tools are read-only. The driver reported 230 passing tests and passing style, compile and codegen checks; I recorded that but did not re-run anything.

## Issues

### Issue 1: Whitespace-only Decisions prompt text still triggers attachment IO before a certain rejection
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala
- **Line(s)**: 377-383. Related: `OpenAIPrompt.scala` line 625, `NativeQuestions.scala` lines 33-35 and 122-123, `AIRequestAdapter.scala` lines 576-577, and the test gap at `AIPromptMediaInputSuite.scala` lines 260-285.
- **Description**: The new check before IO rejects only null or empty text (`value.nonEmpty`), so whitespace-only text gets through. It can come from `inputCol` or from a template filled with blank values. The sequence is:
  1. `generatedMessages` → `buildContentParts` immediately reads every non-blank path cell (local, Hadoop or HTTP GET). Each read is capped at `fileSizeLimitMB`, or unlimited if it's unset.
  2. `OpenAIResponses.validateInputTextPart` accepts any string, so the request is built.
  3. Only then does `NativeQuestions.text` reject the row as `invalid_request` with "Native question or input text must be nonempty".

  The row's outcome is certain before any IO happens. The missing-versus-blank error codes are kept, so this isn't a strict spec violation, but the "fail before image IO" intent only covers null and empty text. The same order applies to a null or invalid row-level `questionsCol` and to the combined 128-image limit: both are checked only after images are downloaded.
- **Risk**: No wrong output or data leak occurs, because the row still gets a controlled error. The cost is wasted executor IO, network traffic and outbound requests to hosted path URLs for rows that can never be sent. That adds up on large DataFrames with sparse templated text, and reads are unbounded when `fileSizeLimitMB` is unset.
- **Suggested Fix**:
  - In `AIPrompt.messages`, reject blank Decisions text before `generatedMessages`. Call `NativeQuestions.validateDecisionsInput(JsString(text))`, or throw `new IllegalArgumentException("Native question or input text must be nonempty")` directly. Avoid `require`, which would add a "requirement failed:" prefix to the message.
  - Add a `" \t "` prompt case to the existing missing-prompt test. Assert `invalid_request` and zero image-server requests.
  - Optional: for Decisions, validate row-level questions and the path-plus-question image count before loading attachments.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary-agent assessment of the frozen eight-file patch",
    "model": "Installed review-code complementary-provider and capability-first rules",
    "reasoning": "Installed review-code maximum supported reasoning",
    "stageBehavior": "Trusted repository review-gauntlet.md: six sequential themes",
    "artifactDirectory": "Explicit driving-user exception",
    "safetyStopPolicy": "Explicit driving-user authorization to exceed previous limits; next attempt uses a finite 3600-second timeout per reviewer and preserves consumed counters."
  },
  "changeSize": "Eight files, with most additions in regression tests and documentation; bounded changes to shared request validation and media serialization.",
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
    "fastCyclesUsed": 5,
    "fastCycleLimit": 10,
    "reconstruction": "Preserve five checkpointed fix/integration cycles and two consumed pre-commit attempts. Earlier execution history is retained separately and is not reviewer evidence.",
    "precommitPassesUsed": 2,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 3,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "d17f57e314517e4b413cda3cdcd86e7ad15ddb04",
  "localHead": "330c9030d3d51aba5d06c6bfbc4c913f1d313c96",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "7586ea1b9c556ea6c237ad99b050356c94d92de8841d02c864a6bdcecbf05a7c",
  "manifestSha256": "3b3a3d171bf0ea69d8a68f9e996a355191afd55832793728dbba634061ad59f2"
}

## Driver Resolution: Issue 1

**Status: Fixed; this pass is consumed and will not count toward the replacement pass.**

`AIPrompt.messages` now calls the existing `NativeQuestions.validateDecisionsInput`
on the prompt text before generating messages or reading attachments. Null and
empty text retain `missing_input`; whitespace-only text retains `invalid_request`
and the original message, without a new `requirement failed:` prefix.

The regression now covers null, empty and whitespace-only text through both public
stages and both prompt bindings. Before the fix it failed because the image server
received `/photo.png`; after the fix it passed with no image or inference requests.
The optional early validation of row-level questions and combined image counts is
not part of this fix: existing validation still rejects those requests before
inference, and changing their error ordering is not required for path support.

An earlier review contained workstation-local paths and was excluded from public
publication, with its original retained privately. Its actionable findings were
fixed: the HTTP test now asserts a query-free request path, missing prompts fail
before image IO, and durable question-image rejection and resume have regression
coverage. That excluded review does not count as a qualifying signoff.