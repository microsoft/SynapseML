---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 4
timestamp: "2026-11-18T05:04:58Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "d438c2dc174d9c3e2cd54bee54dd62a212aa243313a46f851da38a45400dfd11"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-18-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the whole supplied patch in `.review-input-decisions-paths.diff`: all 13 files, from the `AIPrompt.scala` hunks through the capability table in the docs. I checked each changed production path against the post-change source.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`, lines 364–386: in the Decisions path branch, null or empty text raises `AIMissingInputException("input is null or empty")` (`missing_input`). Whitespace-only text fails in `NativeQuestions.validateDecisionsInput(JsString)` (`invalid_request`). Both happen before `generatedMessages` performs IO. Chat/Responses still use "prompt is null".
- [x] `AIPrompt.scala`, lines 470–495: Decisions passes a `lazy val` thunk and a count of non-blank path cells (`trim.nonEmpty`). The `catch` cannot force the thunk, and `retainGeneratedMessages` is false for Decisions. Other APIs force messages eagerly through the 3-argument overload. Line 477/478 is the only call site of `adapter.request`.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala`, lines 582–631: checks run in this order: model, version, options, questions, `validateDecisionsImageCount`, `messages()` (IO), then `prepareDecisionsRequest`. `buildRequest` validates headers after IO, as the docs state.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`:
  - `decisions()`, `validateDecisionsImageCount` and `prepareDecisionsRequest` all use the same `<= 128` limit: 128 is accepted and 129 rejected.
  - Instruction arrays are checked with `validatePart`, and `prepareParts` drops only null `file_id`/`detail`.
  - AIJson `OrderedMap.updated` keeps key positions, so text-only questions serialize unchanged.
- [x] I traced `hostedImageUrl` under JDK 11:
  - **Accepted:** underscore and IDN hosts (registry authority, non-empty `URL.getHost`), IPv6 with or without a port, and an empty port.
  - **Rejected:** empty or userinfo-only hosts, ports that are signed, Unicode-digit or non-numeric, malformed escapes, spaces, `file`/`ftp` schemes, and relative URIs.
  - No DNS lookup or fetch is done.
- [x] `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`:
  - WebP detection requires at least 12 bytes, with `RIFF` at offset 0 and `WEBP` at offset 8, so WAV and short RIFF files stay audio.
  - The extension gate runs before reads, is by-name, and is not evaluated for Chat/Responses.
  - The data-URL image set matches the MIME allow-list applied after reading.
- [x] Configuration checks:
  - `AIRequestAdapter.scala` 247–265 (`validateCapabilities`), 275–281 (`validateMessageConstruction`) and 325–332 (`validateDecisionsInputColumn`, which uses the Spark resolver through `AIColumnPlan.matches`).
  - `columnTypes` defaults to `Map.empty`, and its values must be exactly `text` or `path` (`OpenAIPrompt.scala` 241–247 and 293). `inputCol` has no default.
- [x] Durable mode (`AIDurableInput.scala`): path cells are treated as inline. Hosted media in path cells, messages or questions stops the run before inference.
- [x] Tests reviewed:
  - `AIPromptMediaInputSuite.scala` 613–637
  - `AIPromptPreflightSuite.scala` 61–96
  - `AIPromptRecoveryPreflightSuite.scala` 104–144, plus the new inputCol/path and case-sensitive tests
  - New cases in `NativeQuestionsSuite.scala` and `OpenAIDecisionsLiveSuite.scala`
- [x] Docs: `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 103–122 and 240–330, and the capability table, match the code apart from Issue 1.
- [ ] Test execution: not done; this lens is read-only. I relied on the validation evidence the driver supplied and did not reproduce it.
- [ ] Live provider behavior is unverified. `OpenAIDecisionsLiveSuite` needs credentials, so provider acceptance of hosted, IDN and underscore-host URLs was not checked.
- [ ] Coverage gaps:
  - No test combines `messagesCol`, a `path` column type and `inputMode="json"` (Issue 1).
  - No test covers malformed row-level `apiVersion` text, which is rejected after IO. This is acceptable because the docs promise only that missing bindings fail before IO.

## Issues

### Issue 1: Decisions path/inputMode check runs before messagesCol's own construction conflict
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: 256–258 (interacts with `validateMessageConstruction` at 275–281)
- **Description**:
  - The new `require(!hasDecisionsPathColumns || stage.getInputMode == "text", ...)` runs before `if (stage.isSet(stage.messagesCol)) validateMessageConstruction()`.
  - The trigger is a Decisions stage, `AIPrompt(apiType="decisions")` or `OpenAIDecisions`, configured with all three of:
    - `messagesCol`;
    - a `columnTypes` entry of type `"path"`;
    - `inputMode="json"`, set directly or through `setStateMode("json")`.
  - That stage fails with "requirement failed: Decisions path columns require inputMode='text'; use explicit image parts with JSON input". It should instead report its own messagesCol conflict, "inputMode cannot be combined with messagesCol".
  - The new `fileSizeLimitMB` check was correctly placed after `validateMessageConstruction`, so the two new checks are ordered inconsistently.
  - Existing tests miss this case. `AIPromptMediaInputSuite.scala` 618–629 covers only json with `inputCol`, and `messagesCol` with `inputMode` unset. The messagesCol suites in `AIPromptPreflightSuite.scala` and `AIPromptRecoveryPreflightSuite.scala` apply one setting at a time.
- **Risk**:
  - The error message is misleading. It contradicts the stated goal that "messagesCol reports its own construction conflict" and the docs at lines 119–122.
  - Its hint sends the user in a loop:
    1. Following the advice and setting `inputMode='text'` gives "inputMode cannot be combined with messagesCol".
    2. Removing `inputMode` then gives "Non-empty columnTypes cannot be combined with messagesCol".
  - Severity is Low because validation still fails before any HTTP request or durable storage, so no wrong request is sent.
- **Suggested Fix**:
  - Move the path/inputMode `require` below `if (stage.isSet(stage.messagesCol)) validateMessageConstruction()`, next to the `fileSizeLimitMB` check. Alternatively, guard it with `!stage.isSet(stage.messagesCol)`.
  - Add a case for `messagesCol` + `Map("asset" -> "path")` + `inputMode="json"` to "Decisions path columns reject conflicting input modes and keep System One text only", for both public Decisions stages. It should assert that `transformSchema` and `transform` both report "inputMode cannot be combined with messagesCol" and that no HTTP request is made.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Awaiting driver fix
- **How verified**: Pending

## Driver disposition

Issue 1 is closed without a change as a multiple-invalid-setting diagnostic
refinement. The original Low finding and `ISSUES_FOUND` verdict are preserved.
The ordering described by the reviewer is correct; no different first error or
new regression assertion is claimed as implemented.

The cited documentation at `Unified AI prompting.md:119-123` lists incompatible
settings and promises "These conflicts fail before HTTP." It does not specify
which conflict wins when a configuration violates several independent rules.
The triple-setting example is rejected during adapter/schema validation before
HTTP and durable initialization, so the documented contract is satisfied.
The reported path/inputMode restriction is itself true, even though addressing
that one restriction alone need not repair the other invalid settings.

The supplied review context's "messagesCol reports its own construction conflict"
is attached to the `fileSizeLimitMB` requirement: an explicit message binding
must not be described as an API that never supports size limits. That specific
diagnostic regression is covered and remains fixed. It is not a new global
precedence guarantee for every combination of messagesCol, path tags and native
input mode. This proposed reorder would be a reasonable clarification, but no
supported configuration, accepted request or documented error category needs
it to work.

The driver inspected `AIRequestAdapter.scala:247-265,275-281` and the cited
documentation. Existing schema/transform preflight tests cover the independent
conflicts and no-HTTP/no-storage invariant. Avoid locking in additional
combinatorial error-priority behavior that the API does not promise. No product
or test content changed; continue the remaining lenses on patch
`1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22`.