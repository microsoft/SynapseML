---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 17
stage_attempt: 1
round: 5
timestamp: "2026-11-18T03:09:49Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ef968203c2faf573d3bc1f5f3c9984dfa158dfa797b6251cab77f6859d1f2c90"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-17-stage-attempt-1-review-5-decisions-paths-opus.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff completeness.** `.review-input-decisions-paths.diff` has 13 `diff --git` headers: 5 production files, 7 test files and 1 doc. I read every hunk from the file, including the parts cut off in the prompt (diff lines 289–1700).
- [x] **Prompt text and pre-IO checks.** The Decisions branch of `AIPrompt.messages` and the lazy `prepareRequests` path (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 362–386 and 453–498) are covered in `AIPromptMediaInputSuite.scala`:
  - **Lines 277–306:** exact code and message for null, empty and whitespace text, through both a template and `inputCol`, on both stages, with zero image or inference requests.
  - **Lines 383–419:** missing model or version, and malformed options or questions, rejected before IO.
  - **Lines 515–564:** combined path and question image count checked before IO. The blank path cell is not counted, and the rejected row never reaches the image servers.
- [x] **Chat/Responses still prepare messages eagerly.** `AIPromptAttachmentSuite.scala` 336–351 asserts that `input_io_error` comes before the missing-model error. Lines 375–399 show that cancellation still propagates for Chat, Responses and Decisions.
- [x] **Capability and input-column checks.** `validateCapabilities` and `validateDecisionsInputColumn` (`AIRequestAdapter.scala` 247–266 and 325–332) are covered with exact messages, through both `transform` and `transformSchema`, with no HTTP calls or storage writes:
  - `AIPromptMediaInputSuite.scala` 585–639.
  - The `AIPromptPreflightSuite.scala` hunk.
  - The `AIPromptRecoveryPreflightSuite.scala` hunks, which cover case-insensitive and case-sensitive column resolution, durable and non-durable.

  The `columnTypes` checks in `validateInputs` (lines 293–318) apply to every API, not just generative ones.
- [x] **NativeQuestions routing.** Hosted versus inline routing, instruction arrays, the 128-image totals and dropping null `file_id`/`detail` are covered by:
  - The new `NativeQuestionsSuite.scala` tests.
  - Stage-level tests in `AIPromptMediaInputSuite.scala` at 123–168, 170–210, 641–665 and 667–695. Hosted URLs use `.invalid` hosts and forwarded URLs are compared exactly, so any client fetch or URL rewrite would fail the test.
- [x] **Attachment changes.** WebP detection, `mayBeImagePath`, `dataUrlFileType` and the post-read MIME allowlist are covered by `AIPromptMediaInputSuite.scala` 308–341, 343–381, 421–482 and 484–513. Those tests cover bounded and unbounded reads and assert that missing files and HTTP URLs are never read.
  - Bounded and unbounded `prepareFile` both reach `inferMimeType` (`OpenAIAttachmentUtils.scala` 395–426).
  - Existing Chat/Responses PDF, Office and text attachment tests (`services/openai/OpenAIPromptMultimodalRequestSuite.scala`, `OpenAIPromptOfficePathSuite.scala`) would catch the Decisions format check leaking into other APIs.
- [x] **Durable mode.** I traced how it works:
  - `AIDurableInput.validateRows` maps path cells to `inline`, `messagesCol` to `messages`, and native JSON input to the API kind.
  - `AIPromptOutput.scala` 280 validates rows before creating results.
  - `AIPromptOutput.scala` 309–314 re-validates rows on resume only if the run is incomplete.
  - The fingerprint includes every param (`AIPromptOutput.scala` 186–207).

  Existing coverage:
  - `AIPromptRecoveryAttachmentSuite.scala` 26–107: Decisions path cells and question instructions.
  - `AIPromptOutputSuite.scala` 652–659: Decisions JSON input, helper call only.
  - `AIPromptDurableBindingsSuite.scala` 683–709: Responses `messagesCol` only.
  - `AIPromptOutputPreflightSuite.scala` 202–249: partial resume for Chat/Responses only.
- [x] **Facade.** `services/openai/OpenAIDecisions.scala` only sets defaults and the supported API type, and the new tests run both public stages.
- [x] **Live suite and test runs.** The live tests skip without credentials. I ran no tests; the driver's pass counts are treated as claims checked against test source.
- [x] **Checked but not raised:**
  - Lazy messages change which error a multi-error Decisions `messagesCol` row reports; the code stays the same and the change is intentional.
  - The doc sentence that row headers are validated after attachment loading matches `buildRequest` running after `adapter.request`; it describes a non-guarantee.
  - Decisions reuses the bounded-read and default-limit logic already tested for Chat/Responses.

## Issues

### Issue 1: Durable Decisions explicit image parts lack public-transform inline-only tests
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptRecoveryAttachmentSuite.scala
- **Line(s)**: 26–55 (new durable test covers only path cells). Related: `AIPromptOutputSuite.scala` 652–659; `AIPromptDurableBindingsSuite.scala` 683–709; `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 327–331
- **Description**: This change makes hosted `input_image` URLs valid in non-durable Decisions `messagesCol` and JSON input. The new docs promise that durable runs still stop before inference for hosted URLs in "explicit `messagesCol` or JSON-input image parts".
  - Public `transform` tests cover only path cells and question instructions.
  - **JSON input:** the only Decisions evidence is a direct `validateDurableRows` call on `AIPrompt`. It does not go through `transform` or `OpenAIDecisions`, and it asserts nothing about requests or results.
  - **`messagesCol`:** the only durable hosted-image test uses `apiType="responses"`.
- **Risk**: Before this change, native validation rejected hosted Decisions URLs as row errors, so the durable gate was redundant. It is now the only safeguard. If a change to `AIDurableInput.validateRows` (the native-JSON kind mapping or the `messagesCol` `to_json` mapping) or to the `transform` wiring broke it for Decisions, hosted URLs would be sent and their results committed. Durable results would then depend on mutable external content, and no test would fail. AGENTS.md states that a helper-only test does not prove the public transformer path works.
- **Suggested Fix**: Add a durable Decisions case for both `AIPrompt(apiType="decisions")` and `OpenAIDecisions`:
  - Cover JSON input (`setInputCol(...).setInputMode("json")`) and `messagesCol`, each with a hosted part at `server.url + "/photo.png"`.
  - Assert a `SparkException` containing "Durable media must be inline", `server.requests.isEmpty`, and no `results` directory.
  - Add an inline data-URL counterpart for each form that completes and resumes.

### Issue 2: Decisions path-image durable resume is only tested after the run completes
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptOutputPreflightSuite.scala
- **Line(s)**: 202–249 (partial-resume test limited to Chat/Responses); `AIPromptRecoveryAttachmentSuite.scala` 39 (Decisions resume after completion)
- **Description**: The new Decisions durable test calls `resume` only after the run has completed.
  - For a completed run, `AIPromptOutput.resumeObserved` (`AIPromptOutput.scala` 309–314) skips `validateDurableInput`, `validateDurableRows` and inference. The test only proves that stored results come back.
  - The interrupted-run test (stop after a committed batch, then resume with copied, reordered and loaded stages, checking attachment order and request shape) runs only for `chat_completions` and `responses`.
- **Risk**: Decisions behavior during resume inference is untested:
  - the new preflight checks run against the decoded snapshot schema;
  - the lazy pre-IO image count runs over decoded rows;
  - the fingerprint must match for a reordered, copied or loaded stage with Decisions `columnTypes` and `fileSizeLimitMB`;
  - the request must be a single user message with `input_text` followed by ordered `input_image` parts.

  The intent requires Decisions path inputs to keep the existing resume semantics.
- **Suggested Fix**: Add `"decisions"` to the loop at `AIPromptOutputPreflightSuite.scala` line 202. Set `NativeRequestFixtures.DecisionQuestions`, reply with `NativeRequestFixtures.DecisionResponse`, and assert the `/v1/decisions` route. Alternatively, add an equivalent stop-after-first-batch case to the new Decisions durable test. Either way, assert pending counts, total requests and image order after resuming with the copied, reordered and loaded stages.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver triage

Both findings are accepted coverage gaps, not demonstrated production defects.
Issue 1 requires public-transform coverage for durable Decisions `messagesCol`
and JSON input, with external-media rejection and successful inline inputs.
Issue 2 requires inference on an incomplete Decisions path-image run after
resume, not merely reading an already completed run.

Collect the last independent lens before changing reviewed content, then
implement and validate one focused test batch. This pass cannot qualify the
resulting patch; a fresh complete pass will be required. Original findings and
verdict remain unchanged, and neither issue is marked fixed yet.

## Driver resolution after validation

Both accepted coverage gaps are now addressed in
`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptRecoveryAttachmentSuite.scala`.
The original review and interim triage above remain historical evidence.

- **Issue 1 fixed:** the new durable explicit-image test exercises JSON input
  and `messagesCol` through both `AIPrompt(apiType="decisions")` and
  `OpenAIDecisions`. Hosted parts raise the inline-only error with zero server
  requests and no results directory. Inline counterparts check schema, native
  output, completed state, exact user/text/image request payload and completed
  resume without another request.
- **Issue 2 fixed:** the new interrupted path-image test stops after committed
  batches, verifies pending/committed counts, releases stopped ownership, resumes
  through reordered copied mappings, then saves and reloads the actual public
  stage. It checks the remaining inference, exact image order and native route,
  successful final results and no duplicate calls on completed resume.

The entire affected suite passed: 12 tests, zero failures/errors/skips. Root
ScalaStyle, test ScalaStyle, compile and test compile also passed with JDK
11.0.33, Spark 3.5.0 and the test-only Delta 3.2.0 overlay. Inference uses a
local recording server; no real-provider pass is claimed. Production and all
other reviewed files are unchanged.

Validated full-patch SHA-256:
`1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22`.
Pass 17 is invalidated by the test additions; a new full pass is still required.