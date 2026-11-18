---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 1
timestamp: "2026-11-18T03:57:10Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "8987b0ad6b66b14ad92c60c43de2a176b806d7ecbd45f7042eb001244e5932f9"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-18-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Patch coverage.** I reviewed the whole 13-file patch in `.review-input-decisions-paths.diff`, including the durable-test hunk (diff lines 1125–1331) and the live-suite hunk (diff lines 1573–1643). I compared it with the post-change source. I did not open `reviews/`, `.git/` or any other reviewer's artifacts.
- [x] **Decisions messages are lazy; other APIs stay eager.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 465–494:
  - Only Decisions uses `lazy val inputMessages`, a thunk, and a count of trimmed, non-blank path cells.
  - Chat and Responses still call the by-value `request` overload, so they prepare messages eagerly as before.
  - `retainGeneratedMessages` applies only to generative APIs.
  - Exceptions thrown inside the thunk are caught by the same `try`. They map to `input_io_error`, `missing_input` or `invalid_request`, or become a task-fatal `CancellationException`.
- [x] **No IO before request validation.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` (`request` and `nativeEnvelope`, about lines 582–635), the order is:
  1. model, then API version, then `mergedOptions`;
  2. question parsing and validation;
  3. `validateDecisionsImageCount(pathImageCount, …)`;
  4. `messages()`, the only step that reads images;
  5. `prepareDecisionsRequest`.

  Row-level header validation in `buildRequest` runs after IO, which matches the docs. `AIPromptMediaInputSuite.scala` lines 383–419 checks null questions, malformed questions, missing model, missing version and malformed options for both stages and both input forms, and asserts that the image server received no requests.
- [x] **Prompt-text errors.** In `AIPrompt.scala` lines 377–384, null or empty Decisions text raises `missing_input` ("input is null or empty"). Whitespace-only text raises `invalid_request` through `NativeQuestions.validateDecisionsInput`. Both happen before `generatedMessages`.
- [x] **Construction gates.** Checked `AIRequestAdapter.scala` lines 247–265, 275–281, 293–332:
  - Decisions allows only `messagesCol`, `columnTypes` and `fileSizeLimitMB` from the previously rejected settings.
  - Path columns require `inputMode='text'`.
  - A `messagesCol` conflict is reported before the "fileSizeLimitMB requires a path column" check.
  - `validateInputs` requires exactly one input source, so a Decisions path row always has `promptTemplate` or `inputCol`.
  - The `inputCol`/path conflict uses `AIColumnPlan.matches`, which applies Spark's column resolver.
- [x] **Facade cannot trip the `fileSizeLimitMB` gate.** `OpenAIDecisions` extends `AIPrompt` directly (`OpenAIDecisions.scala` line 15). `fileSizeLimitMB` has no default (`OpenAIPrompt.scala` lines 269–296), so the `isSet` check cannot fire for facade users who never set it.
- [x] **`NativeQuestions.scala` validation and normalization:**
  - Question instructions may be a string or a non-empty parts array.
  - The 128-image limit is checked three times: across questions in `decisions()`, across input plus questions in `prepareDecisionsRequest`, and before IO.
  - `prepareParts` drops only null `file_id` and `detail`.
  - Every map lookup that runs after validation is safe, so no new uncaught exception types appear.
  - I traced `hostedImageUrl` (lines 203–213) by hand. Accepted: underscore, IDN, IPv6, userinfo and empty-port hosts. Rejected: empty hosts, signed or non-ASCII ports, malformed escapes and non-HTTP(S) schemes.
  - Error messages never echo the URL.
- [x] **Attachment loading.** In `OpenAIPrompt.scala` lines 602–657:
  - The by-name `validateImageAttachment` runs only for Decisions, so Chat and Responses never evaluate these predicates.
  - Data URLs are classified from the header alone.
  - `mayBeImagePath` runs before `prepareFile`. In `OpenAIAttachmentUtils.scala` lines 173–188 and 310–318, the extension is lowercased and the query string is removed by `URI.getPath`. So `.JPG` and `photo.png?sig=` are not wrongly rejected, and `.ashx`, `.jfif` and `.jpe` are rejected before any read.
  - After the read, the MIME type must be JPEG, PNG, GIF or WebP.
  - In `inferMimeType` (lines 295–308), only RIFF files with WEBP at offset 8 become `image/webp`; other RIFF files keep the JDK's WAV detection.
- [x] **Template and blank-cell behavior.** In `AIPrompt.scala` lines 419–451, direct placeholders for path columns become filenames for every API. In `createMessagesForRow` (`OpenAIPrompt.scala` lines 609–613), blank cells are skipped and a row with no usable image gives `missing_input`. This uses the same trim/blank rule as the pre-IO count in `prepareRequests`.
- [x] **Durable mode.** In `AIDurableInput.scala` lines 84–88 and 172, path cells are trimmed, blanks are skipped, and the rest must be data URLs, matching request-time normalization. The new durable tests in `AIPromptRecoveryAttachmentSuite` assert:
  - hosted or path media stop the run before any HTTP request or `results` directory;
  - committed and pending counts and ownership are correct, and committed rows are not resent;
  - the exact `/v1/decisions` input shape and image order hold across copied, reordered and saved/loaded stages.
- [x] **Docs.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 245–345 match the code on:
  - the bounded read of the limit plus one byte, and whole-file reads when the limit is unset or infinite;
  - the Hadoop query-string/percent-encoding limitation;
  - column-order processing;
  - which checks happen before IO and that headers are checked after it;
  - durable inline-only media;
  - the `.jfif`/`.jpe` and `image/jpg` notes.
- [x] **Security.** Path columns let executors read local files and fetch URLs; Chat and Responses already allow this. For Decisions the exposure is smaller, because only JPEG/PNG/GIF/WebP content is sent. Hosted URLs in explicit parts are passed to the provider, not fetched by Spark. Size, IO and URL errors are sanitized. The live suite is skipped via `assume` when credentials are missing, contains no secrets and does not print error contents.
- [x] **Behavior change accepted, not filed.** Because Decisions messages are now prepared lazily, a `messagesCol` or JSON row with several problems at once reports the model, version, options or questions error before "messages is null". The spec limits eager preparation to the other APIs, and no test asserts the old order, so I don't consider this a defect.
- [ ] **No tests run by me (this review is read-only).** I relied on the driver's evidence. Credential-gated live tests, Python wrappers and provider end-to-end runs were not checked by me, and I did not recompute the diff's SHA-256.

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
  "changeSize": "Thirteen-file full patch with Decisions hosted/question/path support; latest change adds two focused public durable-input/resume tests in one existing suite. Production and documentation unchanged since preceding validation.",
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
      "capabilityVerifiedAt": "2026-11-18T03:44:07.7902591Z"
    }
  ],
  "overrideSource": {
    "artifactDirectory": "User input: \"I approve that exception, but tsince this might be a new PR you might need a new path\"",
    "scope": "One-time repository-root reviews/ exception for this Decisions path-column follow-up. PR #2853 is still open, so no replacement PR is needed. Use fresh filenames; do not overwrite or migrate prior artifacts.",
    "stageBehavior": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 2982f5a9b198fc3c35e601187887e18b57634faa"
  },
  "bounds": {
    "fastCyclesUsed": 20,
    "fastCycleLimit": 20,
    "reconstruction": "Twenty fix/integration cycles and seventeen pre-commit passes retained. This next pass covers one test-only durability coverage batch.",
    "precommitPassesUsed": 17,
    "precommitPassLimit": 18,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 18,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22",
  "manifestSha256": "64193fdbd2342005a4ba83db03af7933f2e3dd51030c11e1d6f295712019ff2e",
  "transport": {
    "completeDiffAvailable": ".review-input-decisions-paths.diff",
    "reason": "The CLI truncated the visible initial diff on earlier dispatches. Permit reading this exact current immutable product diff in addition to the complete supplied prompt; do not permit prior feedback, other prompts, checkpoints or transport."
  }
}

## Driver completion

All six independent lenses completed against the unchanged frozen patch. Round
1 returned CLEAN; rounds 2-6 returned ISSUES_FOUND, with each recommendation
closed by an explicit source-backed no-change disposition in its own artifact.
The driver accepts this mandatory pre-commit pass for publication, preserving
the original verdicts and optional improvements rather than claiming six clean
reviewer responses. The applicable local evidence is 290 unique deterministic
passes across four runs. Current-head CI, a final CI-qualified pass, human
approval and real-provider validation are not established by this review.