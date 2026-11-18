---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 8
stage_attempt: 1
round: 1
timestamp: "2026-11-17T12:52:23Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6b3f66552518b691849355ab37541f591ef135c4c8c81cad89f0d321cc82765e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-8-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Checks before image IO.**
  - In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` (lines 471-481), Decisions passes `() => inputMessages` from a `lazy val`.
  - In `AIRequestAdapter.scala` (lines 571-581 and 602-608), `request` runs `selectedModel`, `requestVersion` and non-static `mergedOptions` first. `nativeEnvelope` then parses and validates questions and runs `validateDecisionsImageCount` before it calls `messages()`.
  - Chat and Responses still evaluate messages eagerly through the by-value overload.
  - Tests in `AIPromptMediaInputSuite.scala` (lines 261-290, 325-361 and 418-444) assert that the image server receives no request.
- [x] **Prompt-text error parity.**
  - `AIPrompt.scala` (lines 377-384) matches `AIRequestAdapter.input()` (lines 524-532).
  - Null or empty text gives missing_input "input is null or empty".
  - Whitespace-only text gives invalid_request "Native question or input text must be nonempty".
  - Other APIs keep "prompt is null".
- [x] **Capability gating** (`AIRequestAdapter.scala`, lines 247-263 and 274-280).
  - `columnTypes` is allowed for Decisions.
  - `fileSizeLimitMB` is allowed only when a path column exists.
  - Path columns require inputMode "text".
  - `messagesCol` with non-empty `columnTypes` or `fileSizeLimitMB` is still rejected.
  - System One still rejects all of these (`AIPromptMediaInputSuite.scala`, lines 446-467).
  - The `inputMode` default is "text" (`AIPromptParams.scala`, line 105).
- [x] **Hosted URL validation** (`NativeQuestions.scala`, roughly lines 214-234).
  - Requires an http(s) scheme and a non-empty `URL.getHost`.
  - Underscore and IDN hosts pass through the registry-authority fallback.
  - Empty hosts, bad ports, malformed `%` escapes and spaces are rejected through `Try`.
  - Non-image `data:` URLs fall to the hosted branch and are rejected.
  - URLs are forwarded unchanged; `prepareParts` drops only null `file_id`/`detail`.
  - Error messages do not echo the supplied values.
- [x] **Question images and request-wide limit** (`NativeQuestions.scala`, lines 86-119 and 151-175).
  - Instructions must be text or validated part arrays.
  - The combined input-plus-question count is checked against 128.
  - The path pre-count in `AIPrompt.scala` (lines 475-476) uses the same trim/non-empty rule as `createMessagesForRow` (`OpenAIPrompt.scala`, lines 608-610), which emits one part per path. The blank-path case is pinned by the test at lines 418-444.
- [x] **Durable output stays inline-only** (`AIDurableInput.scala`).
  - Lines 84-100 and 141-147: path cells use the "inline" kind (line 172).
  - Lines 58-79: question `image_url` values must be data URLs, for both scalar questions (`questions(...)`) and `questionsCol` (lines 125-134).
  - Decisions has no retained-message column (`AIPrompt.scala`, lines 295-300), so the output schema is unchanged.
- [x] **WebP fix** (`OpenAIAttachmentUtils.scala`, lines 295-308).
  - "RIFF" at bytes 0-3 plus "WEBP" at bytes 8-11 now maps to image/webp before JDK sniffing.
  - RIFF/WAVE files are still audio.
  - `MimeTypeExtensions` (lines 137-155) maps image/webp, and `categorizeFileType` (lines 315-333) still checks the extension first.
- [x] **Chat/Responses behavior preserved** (`OpenAIPrompt.scala`, lines 577-583, 602-618 and 629-657).
  - The eager `dataUrlFileType` call (`OpenAIAttachmentUtils.scala`, lines 426-437) can only raise the same `dataUrlMetadata` errors, with the same messages, that `prepareDataUrlAttachment` (lines 219-235) raises first.
  - Legacy `OpenAIPrompt` never has apiType "decisions" (line 192).
- [x] **Tests and docs consistency.**
  - `AIPromptPreflightSuite.scala` (lines 60-97) and `AIPromptRecoveryPreflightSuite.scala` (lines 52-63) only vary `inputMode` for Decisions, so they don't conflict with the new allowances.
  - No test or doc references the removed messages.
  - No Python-side capability check duplicates these rules (`AIPromptPython.scala`).
  - Doc claims in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` (lines 245-322 and 725-726) match the code.
- [x] **Observation, not reported.** Per-row header and auth validation in `buildRequest` (`AIPrompt.scala`, lines 388-400) still runs after image IO. The spec does not list it among the checks that must run first, and moving it would mean reordering how the address is built.
- [ ] **Tests not executed.** This review is read-only. I relied on the supplied validation evidence and did not rerun any suite.

## Issues

### Issue 1: Decisions file paths are read before format rejection, so unsupported-format errors differ between files and data URLs
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: 632-657
  - Data URLs are type-checked at lines 635-636, before reading.
  - Files are type-checked at line 649, after `prepareFile` at lines 646-648.
  - Related: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`, lines 390-420.
- **Description**:
  - **Data URLs:** the format check runs before the size and base64 checks. Any non-image data URL therefore gets "Decisions path columns only support JPEG, PNG, GIF, and WebP images".
  - **Filesystem and HTTP paths:** `prepareFile` first reads the whole attachment. The read is bounded by `fileSizeLimitMB` when set; `AIPrompt` sets no default, so otherwise it is unbounded. The size is then validated, and only after that is the format checked.
  - **Why the read is avoidable:** `categorizeFileType` checks the extension first, so a file whose non-empty extension is not in `imageExtensions` can never be classified as an image. For names like `report.pdf` the rejection is known before any IO.
  - **Effect on errors:**
    - An oversize `report.pdf` returns invalid_request "Attachment exceeds the N-byte limit". The equivalent oversize `data:application/pdf;...` returns the format error.
    - A missing `report.pdf`, or an unreachable `https://host/report.pdf`, returns input_io_error instead of the format error.
  - **Spec:** the spec requires one clear error for unsupported Decisions path formats, consistently for files and data URLs. The current tests (`AIPromptMediaInputSuite.scala`, lines 363-416) cover only small, readable non-image files, so this gap is untested.
- **Risk**:
  - Users can get a misleading error: they raise the size limit, only to hit the format error next.
  - Executors do avoidable local reads or HTTP fetches. When no limit is set, they buffer the whole file in memory for content Decisions will always reject.
  - Accepted images are not affected.
- **Suggested Fix**:
  - In the non-data-URL branch of `wrapFileToMessagesList`, when the apiType is Decisions, take the extension of `OpenAIAttachmentUtils.attachmentFilename(filePathStr)`. You can expose the existing `fileExtension` helper for this.
  - If that extension is non-empty and not in `imageExtensions`, call `validateImageAttachment(false)` before `prepareFile`.
  - Keep the check after the read for files without an extension.
  - This leaves the classification rules unchanged and only moves the check earlier.
  - Add regression rows for an oversize `.pdf` file and a missing or HTTP `.pdf` path. Each should expect the JPEG/PNG/GIF/WebP message, with zero requests to the image server.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed and locally validated.
- **What changed**: Decisions checks the existing filename-extension rule before filesystem/HTTP reads. Supported and extensionless names retain content inspection and its IO/size errors. Validation predicates are lazy so the Decisions-only precheck does not add work or validation to Chat/Responses.
- **Why**: A known unsupported extension cannot produce an accepted image under the existing classifier, so reading its bytes is unnecessary. The format error now precedes missing-file and size failures for those paths without broadening accepted formats.
- **How verified**: Added a public-stage regression with readable/oversized PDF files, a missing PDF, an HTTP PDF and an inline PDF, with and without a size limit. Before the fix it fails with `input_io_error` instead of `invalid_request`. Both Decisions stages now give the common format diagnostic and make zero image/inference requests. Extended the positive path/copy/persistence test with an extensionless HTTP PNG to protect the unknown-extension branch. All 74 affected media/attachment/Office-path tests passed with no failures or skips, plus root style, compile, test compile and codegen. Both generated wrappers and six Python documentation examples parse.

The corrected nine-file patch has SHA-256
`bc1429a59ba9586c8647b8c14173e7b505e6256e300771567ce61adf18514274`
against target `2982f5a9b198fc3c35e601187887e18b57634faa`.
This fix consumes the pass; all six lenses restart on the corrected snapshot.
Earlier broader 232-test and 44-test results predate this isolated guard change.

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
    "fastCyclesUsed": 10,
    "fastCycleLimit": 10,
    "reconstruction": "Ten fix/integration cycles and seven consumed pre-commit attempts retained.",
    "precommitPassesUsed": 7,
    "precommitPassLimit": 12,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 8,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "2982f5a9b198fc3c35e601187887e18b57634faa",
  "localHead": "f42554d51e60e6e0d7ba7004f5a5301f2f3107b0",
  "expectedPublishedHead": "490f992c2f9609e1a6e632f035855d03d86141ee",
  "patchSha256": "52bb4a2553d59515c45c3a82071eead25c247c3a41b62ecbd39a147faec1fd5d",
  "manifestSha256": "8e48ea6c6184d79a4d51d5f8fc871b869a799cbde86f5763a198951ac035a189"
}
