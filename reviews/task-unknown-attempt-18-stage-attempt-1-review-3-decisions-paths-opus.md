---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 3
timestamp: "2026-11-18T04:40:25Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "6243928ccdacd6f011d6af5f854c76c6d369810aae42c3cf493fd7d73ab51c57"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-18-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read every hunk of the 13-file patch in `.review-input-decisions-paths.diff`. This covers 6 production files, 7 test files and the docs. The `AIPromptRecoveryPreflightSuite` hunks are at lines 1331–1448 and the docs hunks at lines 1643–1810.
- [x] Traced Decisions request preparation in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` (lines 364–386 and 471–496) and `AIRequestAdapter.scala` (lines 582–598 and 610–634).
  - Checks run in this order: model, version, options, questions, combined image count, prompt null/empty/whitespace, then per-file IO.
  - `retainedMessagesName` (lines 295–300) is `None` for Decisions, so the local `lazy val` is forced at most once. A failed read is never re-run.
  - Chat and Responses still force messages eagerly through the by-value overload.
- [x] Checked the capability gates in `AIRequestAdapter.scala` (lines 247–265, 275–281 and 293–332).
  - Path columns require `inputMode="text"`, and `fileSizeLimitMB` requires a path column.
  - The `messagesCol` construction conflict is reported first. System One and embeddings gating is unchanged.
  - The `inputCol`/path conflict check uses the same resolver as `AIColumnPlan.resolve` (`AIColumnPlan.scala` lines 37–50), so an `inputCol` and a path column that resolve to the same field are always detected.
- [x] Checked `NativeQuestions.scala` lines 86–245.
  - Question instructions can be a string or a list of parts. The 128-image limit is enforced before IO and again after IO, and null `file_id`/`detail` are dropped from question parts.
  - `hostedImageUrl` cases checked: scheme case, userinfo/`@`, IPv6 brackets, ASCII-only ports, empty host, malformed escapes, spaces, `data:` URLs that are not images, scheme-relative URLs, and underscore/IDN hosts (accepted through `URL.getHost`).
  - It is wrapped in `Try`, uses fixed error messages, and makes no network or DNS calls.
- [x] Checked `OpenAIPrompt.scala` (lines 603–656) and `OpenAIAttachmentUtils.scala` (lines 159–188, 293–318, 320–338 and 428–465).
  - The by-name image predicate is never evaluated for Chat/Responses, and the extension check runs before `prepareFile`.
  - Data-URL classification reads only the metadata. Passing the post-read MIME allowlist means the file type is `image`.
  - The WebP check needs at least 12 bytes, so WAV and short RIFF files stay audio.
  - Size and read errors never include the full path or URL, only the last filename segment, so SAS tokens are not exposed.
- [x] Checked `AIDurableInput.scala` lines 80–200. In durable mode, path cells, `messagesCol` and JSON input, and question image parts (constant or `questionsCol`) must all be inline data URLs. Hosted URLs and file paths stop the run before inference.
- [x] Ruled out three candidate problems:
  - AIJson `OrderedMap.+` replaces existing keys in place (`AIJson.scala` lines 23–42), so `prepareDecisionsRequest` keeps member order.
  - The pre-IO `imageCount` ignores blank cells after trimming, the same way `createMessagesForRow` does, and `attachmentColumns` orders path columns by schema position, so the order of the `columnTypes` map doesn't change image order.
  - The endpoint and API version can't add new row errors after IO.
- [x] Compared the docs (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`) with the code. They match on pre-IO ordering, per-file column order, the Hadoop query-string limitation, inline-only durable media and the capability table.
- [x] Read the tests in the diff: MediaInput, Attachment, Preflight, RecoveryAttachment, RecoveryPreflight, NativeQuestions and DecisionsLive suites. The case-sensitivity tests restore `spark.sql.caseSensitive` in a `finally` block.
- [ ] I ran no tests or builds (read-only lens) and relied on the driver's evidence.
- [ ] Not verified: Python wrapper runtime behaviour and live provider end-to-end runs. There is no Python runtime, and the live tests only run with credentials.
- [ ] No test pins Decisions `messagesCol` row-error precedence (see Issue 1).

## Issues

### Issue 1: optional — Lazy message preparation reorders Decisions messagesCol row errors
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala
- **Line(s)**: 474–478 (messagesCol branch at 367–376). Also `AIRequestAdapter.scala` lines 590 and 615–617.
- **Description**: Laziness now applies to every Decisions row, not just rows with path columns.
  - Before this change, `messages()` ran before `adapter.request`. A null `messagesCol` cell therefore always reported `missing_input: messages is null`.
  - Now `selectedModel`, `requestVersion`, `mergedOptions` and questions parsing run first.
  - Example: a null messages cell plus malformed `questionsCol` JSON used to give `missing_input` and now gives `invalid_request`.
  - Example: a null messages cell plus a null model column now reports "explicit model/deployment is missing for this row".
  - `messagesCol`, JSON and plain-text Decisions rows read no attachments, so laziness gains nothing for them. The new order is not documented or tested.
  - This is not a demonstrated break of a documented contract. It is an unintended behaviour change in a fix meant to stay narrow.
- **Risk**: In existing `messagesCol` Decisions pipelines, rows with more than one fault silently get a different error code or message. Error triage keyed on the code can shift. There is no data loss and no extra HTTP traffic.
- **Suggested Fix**: Use the lazy thunk only for rows with path columns: `if (getApiType == "decisions" && paths.nonEmpty) adapter.request(row, prompt, () => inputMessages, imageCount) else adapter.request(row, prompt, inputMessages)`. Add a regression test for both Decisions stages that pins `messagesCol` precedence (null messages plus a null model or malformed `questionsCol`). If uniform laziness is intended, document the precedence and pin it with that test instead.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Waiting for the driving agent to choose between limiting laziness to path rows and documenting and pinning the new precedence.
- **How verified**: Pending

## Driver disposition

Issue 1 is closed without a change. The original Low/optional finding and
`ISSUES_FOUND` verdict remain intact. The described difference for doubly-invalid
messagesCol rows is real; it is not being denied or represented as a fix.

Uniform Decisions request preflight is intentional: the adapter validates
model/version/options/questions before obtaining native input. The supplied
contract specifically preserves the existing eager order for Chat/Responses,
while Decisions uses the deferred native request path. It does not promise
which fault wins when a Decisions row has multiple independent invalid fields.
The source at `AIPrompt.scala:474-478` explicitly selects that distinction.

A null message with otherwise valid metadata still reports missing input;
invalid metadata still produces a controlled row error, and neither case sends
an inference request. No valid input is newly rejected, and there is no silent
success or data loss. Restricting the thunk to path rows would add a second
Decisions error-order policy without restoring a documented guarantee.
Documentation/tests need not establish a new precedence contract for every
combination of independent invalid fields.

This was independently considered and not filed as a defect in pass 17's
detailed review and this pass's broad review. Those are duplicate-disposition
context for the driver only, not evidence supplied to this reviewer. Current
public path tests cover missing inputs, invalid row metadata, controlled error
codes, no-IO/no-inference behavior and the required Chat/Responses eager order.
No new test execution or error-order guarantee is claimed.

Scope note: the checklist's "6 production files" is an arithmetic typo. The
apply-verified manifest contains 5 production files, 7 tests and 1 document,
13 in total. The complete supplied diff remains unchanged:
`1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22`.