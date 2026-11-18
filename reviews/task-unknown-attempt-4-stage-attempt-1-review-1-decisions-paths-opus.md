---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 1
timestamp: "2026-11-17T09:59:55Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "43acd70dcda09b8b5d569e0c78260f14fe3f6e7ab4e4b43cf0dd85260b403648"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-4-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I reviewed the complete supplied eight-file patch against the stated goal, using target `d17f57e314517e4b413cda3cdcd86e7ad15ddb04` as context. I did not read `reviews/**`, `.git/**`, or any other reviewer's artifacts.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 349–386 and 453–492:
  - In `messages()` (377–385), null or empty prompt text throws `AIMissingInputException("input is null or empty")` and becomes `missing_input`.
  - Whitespace-only text fails `validateDecisionsInput` with "Native question or input text must be nonempty" and becomes `invalid_request`.
  - Both checks run before `generatedMessages` reads any image. The codes and messages match the non-path `AIRequestAdapter.input()` (524–532).
  - The `prepareRequests` catch maps `AttachmentReadException` to a sanitized `input_io_error`.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` 247–265, 274–280 and 303–314:
  - `columnTypes` is allowed only for Decisions. `fileSizeLimitMB` is allowed only when a column is typed `path`, and path columns require `inputMode="text"`.
  - `messagesCol` combined with a non-empty `columnTypes` is still rejected.
  - System One and embeddings still reject `columnTypes`.
  - Path columns must exist and be strings.
  - Chat/Responses are unaffected because `decisionsPaths` is false for them.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` 87–260:
  - Instructions may be a string or a non-empty array of parts, and each part is validated.
  - The 128-image limit is enforced on questions alone (`decisions()`) and on input plus questions combined (`prepareDecisionsRequest`).
  - Questions are rebuilt with `ListMap(fields.toSeq)`. This keeps the original field order, so text-only question bytes and the native envelope order are unchanged.
  - `prepareParts` removes only null `file_id`/`detail`. Hosted URLs, including signed query strings, are forwarded unchanged.
  - Error messages are constant and never echo the URL.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` 577–657:
  - Every new branch is guarded by `decisions`. Decisions sends a single user message with no system prompt.
  - `OpenAIPrompt.supportedApiTypes` excludes `decisions`. `OpenAIDecisions.scala` is a thin `AIPrompt` subclass, so both stages share the same path.
  - `stringMessageWrapper` and `createMessagesForRow` are not used anywhere else.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIDurableInput.scala` 45–200: the inline-only restriction still holds under durable mode.
  - Path cells use the `inline` kind.
  - Scalar questions (`validate`), per-row `questionsCol` (`validateDeferredJson`) and JSON input or messages all go through `externalBindings`, `content()` and `inline()`, which reject non-data `image_url` values.
  - `columnTypes` and `fileSizeLimitMB` are ordinary fingerprinted params, so resume semantics are unchanged.
- [x] `AIPrompt.scala` 295–300: `retainedMessagesName` covers generative APIs only, so Decisions gains no output column.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` 100–113, 171–186 and 327–413:
  - The existing reader is reused.
  - Reads are bounded only when `fileSizeLimitMB` is set, because it has no default.
  - Read errors are sanitized.
  - `pathFilename` uses `URI.getPath`, so query tokens do not leak into template filenames.
- [x] Tests are logically sound: `AIPromptMediaInputSuite.scala` 119–420, `AIPromptRecoveryAttachmentSuite.scala` 26–103 and `NativeQuestionsSuite.scala` 102–200.
  - Invalid prompts produce no image or provider requests.
  - Each transform downloads each image exactly once.
  - Content shapes are asserted exactly, and the 127/128 image boundary is covered.
  - Durable mode rejects `file://` and `https://` paths and hosted question images.
  - Hosted URLs are accepted, and `https:///`, `file:`, `ftp:`, URLs with spaces and bad escapes are rejected.
  - I did not run these tests; I relied on the driver's validation evidence.
- [x] I checked the docs in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (lines 110–122, 240–312 and 701–716) against the code. I also checked `AIPromptPython.scala`: it has no apiType-specific Python rules, so no Python changes are needed.

## Issues

### Issue 1: Decisions path rows load every image before the questions, model, version and combined image-limit checks
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: AIPrompt.scala 474–475 (with 377–385); AIRequestAdapter.scala 571–584 and 597–600
- **Description**: `prepareRequests` calls `messages()`, and therefore `generatedMessages`, before `adapter.request()`. `generatedMessages` reads every local or HTTP path image. Only after that does `request()` check:
  - `selectedModel(row)`, `requestVersion(row)` and `mergedOptions(row)`;
  - per-row `questionsCol` in `nativeEnvelope`: a null value gives "questions is null", and invalid JSON or schema gives `invalid_request`;
  - the combined 128-image limit in `prepareDecisionsRequest`.

  This also changes which error is reported first. Without path columns, Decisions resolves questions before input, so a row with null input and null questions reports "questions is null". With path columns it reports "input is null or empty".
- **Risk**: Rows that are already invalid still download or read every attachment. Examples are null or invalid per-row questions, null model or version bindings, malformed per-row options, and question images that push the total over 128. These reads are unbounded when `fileSizeLimitMB` is unset, which wastes executor memory and network and hits remote hosts for nothing. The reported error message also depends on whether a column is typed `path`. The row outcome is still a controlled error.
- **Suggested Fix**: For Decisions, resolve and validate the non-input parts of the row before any image IO. For example, split a pre-validation step out of `AIRequestAdapter.request` that returns the model, version, options and questions, and call it before `messages()`. Optionally pre-check that question images plus non-blank path cells are at most 128. Add regressions asserting zero image-server requests for:
  - a row with null `questionsCol`;
  - a row whose question images plus path cells exceed 128.

### Issue 2: Non-image Decisions path attachments are fully built before rejection, with inconsistent Responses-specific messages
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 632–657; the builders are in OpenAIAttachmentUtils.scala 418–440 and 464–486
- **Description**: For Decisions, `wrapFileToMessagesList` uses the Responses message builders and applies the image-only `require` only after the message exists. As a result:
  - text files are decoded into a full UTF-8 `String`;
  - PDFs and other `file` types are fully base64-encoded into an `input_file` data URL, then thrown away;
  - audio fails inside the builder with "Audio input is not supported in the current API version.";
  - unsupported and Office types fail with "Unsupported file type: …".

  The single documented rule ("images only, not text, PDF, Office, or audio") therefore produces three different messages. The audio message wrongly suggests that a different API version might work.
- **Risk**: CPU and memory are spent on encoding or decoding that is discarded, without any bound when `fileSizeLimitMB` is unset. Users get confusing, inconsistent messages for one rule. The error code is correctly `invalid_request` in all cases.
- **Suggested Fix**: For Decisions, check the type before building a message:
  - for file paths, require `fileType == "image"` immediately after `prepareFile`;
  - for data URLs, check the declared MIME type before calling `responsesDataUrlMessage`;
  - throw the single "Decisions path columns only support image attachments" message in both cases.

  Add an audio case and exact-message assertions to the existing invalid-types test.

### Issue 3: Hosted image validation rejects valid HTTP(S) URLs that java.net.URI cannot parse as server-based
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 228–233
- **Description**: A hosted URL is accepted only when `new URI(url).getHost` is non-empty. Java's URI parser follows RFC 2396, so it treats hosts containing `_` (for example `https://img_cdn.example.com/a.png`) or non-ASCII IDN labels as registry-based and returns `getHost == null`. These URLs are valid under RFC 3986 and WHATWG. They are rejected before HTTP with "Decisions images must use HTTP(S) URLs or inline base64 image data URLs", even though the provider, not SynapseML, would fetch them.
- **Risk**: This is a small deviation from "Decisions accepts hosted HTTP(S) images", and the error is misleading for a URL that really is HTTP(S). It only affects edge cases.
- **Suggested Fix**: For `http`/`https` schemes, when `getHost` is null, accept a non-empty `getRawAuthority` while still rejecting the empty-host forms that tests already cover. Alternatively, extract the host with `java.net.URL`. Keep the constant message, and add an underscore-host acceptance case to `NativeQuestionsSuite`.

### Issue 4: The new Decisions docs name only query strings as an HTTP(S) path-URL limitation; percent-escaped paths also fail
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Unified AI prompting.md
- **Line(s)**: 291–299
- **Description**: `prepareFile` passes the raw string to `new org.apache.hadoop.fs.Path(String)` (OpenAIAttachmentUtils.scala 396–400). Hadoop's `Path` treats its input as unescaped text and rebuilds the URI with the multi-argument `java.net.URI` constructor, which always escapes `%`. The HTTP filesystem then opens `toUri().toURL()`. So `https://host/my%20photo.png` is requested as `/my%2520photo.png` and fails with `input_io_error`. This is the same mechanism behind the documented `?` limitation.

  This is existing shared-reader behavior. The spec rightly keeps the reader unchanged, but the new paragraph documents only part of the limitation.
- **Risk**: Users who follow the docs with ordinary percent-encoded image URLs, such as file names with spaces or non-ASCII characters, get an IO error the docs do not explain.
- **Suggested Fix**: Change only the docs. State that HTTP(S) path URLs must not contain query strings or percent-encoded characters, and point to explicit `input_image` parts (which forward the URL unchanged) or Spark-readable paths for such URLs. Optionally add the same wording to the Chat/Responses path-column docs.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 4
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
    "fastCyclesUsed": 6,
    "fastCycleLimit": 10,
    "reconstruction": "Six fix/integration cycles and three consumed pre-commit attempts retained.",
    "precommitPassesUsed": 3,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 4,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "d17f57e314517e4b413cda3cdcd86e7ad15ddb04",
  "localHead": "330c9030d3d51aba5d06c6bfbc4c913f1d313c96",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "42136a4a76b50f884ee424c6de7f0a67193b5ef65a1ae936896f80de78891bcb",
  "manifestSha256": "a00fef19f07e5467df5e166ffab31f8f8b81e17d3241fc7c0db756ef707782f2"
}

## Driver Resolutions

This pass is consumed by the following fixes and does not count toward the
replacement six-lens pass. Original findings and verdict above are unchanged.

1. **Fixed: row validation before attachment IO.** Decisions now defers message
   preparation until model, version, options and questions have been resolved.
   The shared image-count check also runs against nonblank path cells before
   attachment reads. Other APIs retain eager message preparation, and the
   existing three-argument adapter method remains available. The metadata
   regression covers both stages and both prompt bindings; the 127/128-question
   image boundary now verifies exactly one image download for the valid row
   and none for the rejected row.
2. **Fixed: consistent image-only errors before conversion.** The declared data
   URL MIME prefix or detected file category is checked before invoking the
   Responses message builder. PDF, text, Office and audio inline inputs, plus
   local PDF/text/audio files, assert the exact Decisions image-only message.
   Existing size, IO, base64 and missing-input cases remain covered.
3. **Fixed: hosted URL host parsing.** URI syntax validation is retained; host
   extraction uses the resulting URL without fetching it. Underscore and IDN
   hostnames are accepted and forwarded unchanged through both public stages.
   Empty hosts, invalid ports, unsupported schemes, spaces and malformed
   percent escapes remain rejected.
4. **Fixed: HTTP path documentation.** The guide now explains query strings and
   percent-encoded paths, with explicit image parts as the unchanged-URL route.
   A network-free probe of the resolved Hadoop 3.3.4 `Path` class confirmed
   `/my%20photo.png` becomes `/my%2520photo.png` and `?` becomes `%3F`.

Five focused regressions failed before these fixes. Afterwards all 33 tests
in `AIPromptMediaInputSuite` and `NativeQuestionsSuite` passed with no skips,
and cognitive main/test ScalaStyle passed. Full validation and a new frozen
review pass are required before publication.
