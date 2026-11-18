---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 1
timestamp: "2026-11-18T06:58:38Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "37f8b2a194a059e7d4c004876ecb56065715f956263c5a766057df6440d5731c"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-19-stage-attempt-1-review-1-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Whole diff read.** I read all 13 files in `.review-input-decisions-paths.diff`: production code (lines 1–421), tests (422–1642) and docs (1643–1810). Where I needed to know which change introduced what, I checked the post-change source of `AIPrompt.scala`, `AIRequestAdapter.scala`, `NativeQuestions.scala`, `OpenAIPrompt.scala` and `OpenAIAttachmentUtils.scala`.
- [x] **Decisions validation order before image IO is as specified.** Files: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 484–487, and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` lines 584–600 and 612–634.
  - The order is: model, then version, then row options, then row questions (null, parse, validate), then the combined path-plus-question image count, then `messages()` (text check, then attachment IO), then `prepareDecisionsRequest`.
  - Chat and Responses use the three-argument by-value overload, so their messages are still built eagerly.
  - SystemOne and Embeddings are unchanged.
  - The only `adapter.request` call site is `AIPrompt.scala` 487–488.
- [x] **Prompt-text errors keep their distinct codes.** In `AIPrompt.scala` 387–394:
  - Null or empty Decisions text gives `missing_input` "input is null or empty".
  - Whitespace-only text gives `invalid_request` through `NativeQuestions.validateDecisionsInput` (`text()` at `NativeQuestions.scala` 31–34). Both checks run before `generatedMessages` touches any file.
  - Chat keeps "prompt is null".
- [x] **Capability checks match the spec.** `AIRequestAdapter.scala` 249–267, 277–283 and 327–334:
  - Decisions allows only `messagesCol`, `columnTypes` and `fileSizeLimitMB`.
  - Path columns require `inputMode=text`. The default is `"text"` (`AIPromptParams.scala` 105).
  - `validateMessageConstruction` runs before the `fileSizeLimitMB` path requirement.
  - The `inputCol`/path conflict check uses `AIColumnPlan.matches`, so Spark's configured resolver applies.
  - SystemOne and Embeddings rejections keep their old messages.
  - Tests: `AIPromptMediaInputSuite.scala` 585–640.
- [x] **Per-file path handling.** `OpenAIPrompt.scala` 602–659 and `OpenAIAttachmentUtils.scala` 156–188, 296–339 and 431–442:
  - `mayBeImagePath` never throws (`pathFilename` is wrapped in `Try`). It takes the extension from `URI.getPath`, so query strings are ignored, and rejects other extensions before reading or size-checking.
  - Data URLs are classified from the header only; `image/jpg` maps to `bin` and is rejected.
  - After the read, the MIME type must be one of the four allowed image types.
  - Decisions sends a single user message, with no default system prompt.
  - A row whose path cells are all blank gives `missing_input` "attachment input is missing" (`AIPrompt.scala` 363–372), as the docs say.
- [x] **WebP detection.** `OpenAIAttachmentUtils.scala` 296–301 requires at least 12 bytes, `RIFF` at offset 0 and `WEBP` at offset 8. Any other RIFF file still goes through JDK detection, so WAV stays audio.
- [x] **Hosted URL validation and forwarding.** `NativeQuestions.scala` 203–213 and 215–245; tests in `NativeQuestionsSuite.scala` 101–118 and 222+:
  - Accepted: underscore, IDN and IPv6 hosts, userinfo, empty port.
  - Rejected: empty host, signed or Unicode-digit ports, bad `%` escapes, spaces, non-HTTP(S) schemes, relative URLs.
  - Error messages are fixed text, so URLs are not leaked.
  - `prepareParts` only drops null `file_id`/`detail`. Key order is preserved through `AIJson` OrderedMap (`AIJson.scala` 23–44), so URLs reach the wire unchanged.
- [x] **Combined 128-image limit.** `NativeQuestions.scala` 118 and 151–167, plus the pre-IO count at `AIPrompt.scala` 486:
  - The pre-IO count trims cells the same way `createMessagesForRow` does (`OpenAIPrompt.scala` 608–610).
  - The total is checked again after IO.
  - Tests: `NativeQuestionsSuite.scala` 131–142 and `AIPromptMediaInputSuite.scala` 642+.
- [x] **Error mapping.** `AIPrompt.scala` 492–503:
  - A cancelled read becomes a task-fatal, sanitized `CancellationException`.
  - A failed read becomes `input_io_error` "Unable to read input attachment".
  - The `lazy val` is reused for retained messages, so files are not read twice.
- [x] **Interaction with AIDefaults.** `transform` builds an `effectiveCopy()` (`AIPrompt.scala` 364–372), and the defaults only touch endpoint, key, model and API version. The new checks use `isSet` on `columnTypes`, `fileSizeLimitMB`, `messagesCol` and `inputCol`, so they are unaffected. The default `messagesCol` value (`OpenAIPrompt.scala` 288) is not mistaken for an explicit setting.
- [x] **`OpenAIDecisions` shares the behavior.** It extends `AIPrompt` with `supportedApiTypes = Array("decisions")` (`OpenAIDecisions.scala` 15–21). The tests run both stages.
- [x] **Durable mode.** `AIDurableInput.scala` is unchanged; it still requires path cells, message/JSON image parts and question instructions to be inline. The docs match.
- [x] **Docs.** I checked `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` against the code: error codes, which checks run before IO, header validation after IO, the Hadoop query/percent limitation, JPEG MIME naming, column-order processing, durable inline-only behavior, and the capability table.
- [ ] **Test execution.** Not done; this review is read-only. I relied on the driver's evidence (303 passes, 0 failures) without reproducing it.
- [ ] **Provider end-to-end.** Not verified. The live tests need credentials and were cancelled, so whether the provider accepts hosted or question images is unconfirmed.
- [ ] **JDK coverage.** `hostedImageUrl` relies on lenient `java.net.URL` host parsing. I checked this against JDK 11 behavior by reading only; other JDKs were not checked.
- [ ] **Residual risks not reported as findings** (existing shared behavior or required by the spec):
  - Executors fetch HTTP(S) path cells through the established reader, an SSRF surface inherited from that reader.
  - A file with an image extension but unrecognized content falls back to a MIME type guessed from its name.
  - When `fileSizeLimitMB` is unset or infinite, the whole file is buffered.
  - Path data URLs have their base64 checked twice (a performance cost only).
  - If path columns, `inputMode=json` and `messagesCol` are all set, the inputMode error is reported first. Each error is accurate, and reordering would not save the user a round trip.
  - Decisions-specific branches now live in the `OpenAIPrompt` base class.

## Issues

### Issue 1: Bad parts in question instructions are reported as "Unsupported Decisions input part"
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 89–93, 215–245 (message at 243)
- **Description**: This PR makes `decisions()` send question `instructions` arrays through `validatePart` (line 91). Before, `validatePart` only validated user-message input. Its fallback for an unknown part type still says "Unsupported Decisions input part" (line 243). So an `input_file`, `input_audio` or misspelled part inside a question's `instructions` produces an error that points at the input. The other `validatePart` messages are neutral. The new test `NativeQuestionsSuite.scala` 144–170 only checks that sensitive values don't leak, not the message text, and no test or doc depends on the current wording.
- **Risk**: Users get misleading errors. With constant `questions`, the error appears at `transformSchema` right after the user set the questions, yet it names the input. With `questionsCol`, it shows up as a per-row `invalid_request` that sends users to inspect their input column. No correctness or security impact.
- **Suggested Fix**: Make the message neutral, for example "Unsupported Decisions content part; expected input_text or input_image". Alternatively, pass a context label ("input" or "question instructions") into `validatePart`. Add one assertion in the `NativeQuestionsSuite` malformed-instructions test for the `input_file` case.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driving-agent decision
- **How verified**: Pending

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
  "changeSize": "Thirteen-file existing Decisions hosted/question/path patch rebased onto the new AIDefaults target. Both commits replay equivalently; surrounding default/auth/version context changed in two shared production files.",
  "cyclomaticComplexity": "Moderate shared capability, attachment, URL-validation and persistence branches. Integration adds existing upstream scoped defaults/auth precedence to the review context, not a new feature in this diff.",
  "rationale": "Six specialist lenses review the full existing patch and relevant AIDefaults integration context. Sequential repository override retained; no tier downgrade or review waiver.",
  "primaryModel": "gpt-6-astra",
  "reviewers": [
    {
      "model": "claude-opus-5.5",
      "provider": "Anthropic",
      "reasoningEffort": "max",
      "selection": "Current task catalog includes claude-opus-5.5 with max reasoning; current Anthropic overview describes it as long-running agentic coding/knowledge work. Fable 5.2 is absent from the live catalog. Opus is the most capable eligible Anthropic coding reviewer available here.",
      "capabilitySource": "https://platform.claude.com/docs/en/models/overview",
      "capabilityVerifiedAt": "2026-11-18T06:31:41.606808+00:00"
    }
  ],
  "overrideSource": {
    "artifactDirectory": "User input: \"I approve that exception, but tsince this might be a new PR you might need a new path\"",
    "scope": "One-time repository-root reviews/ exception for this Decisions path-column follow-up. PR #2853 is still open, so no replacement PR is needed. Use fresh filenames; do not overwrite or migrate prior artifacts.",
    "stageBehavior": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at 2982f5a9b198fc3c35e601187887e18b57634faa"
  },
  "bounds": {
    "fastCyclesUsed": 21,
    "fastCycleLimit": 21,
    "reconstruction": "Retain all 21 fix/integration cycles and 18 precommit passes. One additional finite integration pass follows a substantive target advance after publication.",
    "precommitPassesUsed": 18,
    "precommitPassLimit": 19,
    "finalPassesUsed": 0,
    "finalPassLimit": 2,
    "perReviewerTimeoutSeconds": 3600,
    "additionalAutomaticTransportRetries": 1,
    "reviewAttempt": 19,
    "userOverride": "User input: \"review and publish, it's okay to go over the limits\"",
    "processControl": "Verified Windows kill-on-close Job Object containing the reviewer runner and all inherited descendants; explicit terminate and handle-close paths both tested."
  },
  "pr": "https://github.com/microsoft/SynapseML/pull/2853",
  "targetSha": "cd301ba2bbd4db522308952f50e9109db4fcb481",
  "localHead": "8c817e9eed63760cdbe990454dc877d95a37fbaa",
  "expectedPublishedHead": "3552de48a4a20d29ecca7c9b4e74670e4aa7d58d",
  "patchSha256": "ead06a97ea36893b0220e8d853b1ee4f883e4c78079937ce9d8f568c83052a2b",
  "manifestSha256": "a492be4791f70424c05c358e5ee7f9215fe91b4c471caa9036fb356ebea560ba",
  "transport": {
    "completeDiffAvailable": ".review-input-decisions-paths.diff",
    "reason": "The CLI truncated the visible initial diff on earlier dispatches. Permit reading this exact current immutable product diff in addition to the complete supplied prompt; do not permit prior feedback, other prompts, checkpoints or transport."
  }
}

## Driver disposition

Issue 1 is closed without a code change as an optional diagnostic refinement.
The possible ambiguity is acknowledged, not represented as fixed. In
`NativeQuestions.scala` 89-93 and 215-245, both locations deliberately use the
same content-part validator and the protocol's `input_text`/`input_image` type
names. "Input part" is a generic description here; the diagnostic does not name
the wire `input` field or the Spark `inputCol`, and no location-specific error
contract is promised.

The unsupported part is rejected at the correct validation boundary rather
than accepted, misrouted or sent to inference. The malformed-instructions test
at `NativeQuestionsSuite.scala` 144-173 verifies rejection and sensitive-value
sanitization; that suite's 13 tests passed in the fresh 303-test integration run.
Neutral or location-specific wording could improve usability, but is not
required to correct this compatibility fix. The shared existing wording is
retained; no additional wording assertion or production change is claimed.

The original Low finding and `ISSUES_FOUND` verdict remain intact. This driver
scope disposition permits the next lens on the unchanged patch; it is not a
reviewer CLEAN verdict or final CI qualification.

## Author-requested abbreviated completion

The author subsequently requested a simpler, faster review. That instruction
supersedes the original six-lens effort plan for this publication. Rounds 1-4
are complete with the preserved dispositions above and in their own artifacts.
Round 5 was canceled before a final verdict; its runner failure is retained.
Round 6 was not dispatched. This is not a completed six-lens pass.

The driver's brief final check confirmed the documented shared columnTypes
signature, supported image formats, input conflicts and durable inline-only
restrictions, and verified all 13 reviewed source blobs against the frozen
manifest. Product whitespace checks pass. Historical review bytes, including
their original formatting, remain unchanged.

There are no product changes after the recorded 303-test integration run.
Optional diagnostic and refactoring suggestions do not block this publication.
Current-head remote CI, live-provider evidence and human approval remain
separate gates; this abbreviated review does not supply or waive them.
