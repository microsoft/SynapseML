---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 4
timestamp: "2026-11-18T08:15:44Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "195aa28c7ea517252ee1b1f38405841dfada9fec55430d129fd35346c1f91670"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-19-stage-attempt-1-review-4-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the whole supplied patch in `.review-input-decisions-paths.diff`. It has 13 `diff --git` sections: 5 main Scala files, 7 test suites and 1 doc. I used view ranges to recover the hunks the prompt cut off, and I compared each change against the post-change source.
- [x] **Lazy request ordering.** I traced `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 484-491 and `AIRequestAdapter.scala` lines 584-600 and 612-635.
  - For Decisions, these steps run before `messages()` triggers any image IO, in this order: `selectedModel`, `requestVersion`, `mergedOptions`, question parse and validation, and the pre-IO `validateDecisionsImageCount`.
  - Chat, Responses, SystemOne and embeddings use the by-value overload, so their message preparation stays eager.
  - `adapter.request` has only two call sites. The `lazy val` is evaluated at most once, including when `retainGeneratedMessages` is set. The catch handlers never reference it.
- [x] **Pre-IO image count matches the final count.** `paths.count(... _.trim.nonEmpty)` uses the same filter as `createMessagesForRow` (`OpenAIPrompt.scala` lines 602-618). `wrapFileToMessagesList` produces one `input_image` per path.
  - `paths` holds `columnTypes` keys (`AIPrompt.scala` lines 359-361), and all of them are in `indices` (lines 467-469).
  - Path columns are checked to be StringType (`AIRequestAdapter.scala` lines 312-317), so `getString` cannot throw ClassCastException.
- [x] **128-image limit.** `NativeQuestions.scala` lines 117-118 and 151-168 use `<= MaxImages`, so 128 passes and 129 fails, with no off-by-one.
  - Question-only, input-only and combined counts are all enforced.
  - `prepareDecisionsInput` validates before it reads `obj(message)("content")` (lines 140-149), so `prepareDecisionsRequest` cannot hit a missing key.
- [x] **Error codes for prompt text.** In `AIPrompt.scala` lines 387-393 and the catch mapping at lines 492-505:
  - null or empty text gives `missing_input` ("input is null or empty");
  - whitespace-only text gives `invalid_request` through `validateDecisionsInput(JsString)`;
  - a row whose path cells are all blank gives `missing_input` ("attachment input is missing", line 371).
- [x] **`hostedImageUrl` (`NativeQuestions.scala` lines 203-213), traced by hand.**
  - Accepted: underscore and IDN registry authorities, IPv6 hosts with and without a port, and an empty port.
  - Rejected: empty host, `https:///`, opaque `https:host`, signed or Unicode-digit ports, non-HTTP(S) schemes, spaces and malformed percent escapes.
  - The inline branch is entered only for a case-insensitive `data:image/` prefix (lines 226-238). Other `data:` URLs fall through to the hosted branch and are rejected with the same message.
- [x] **WebP detection and WAV.** `OpenAIAttachmentUtils.scala` lines 295-308 guard on `length >= 12` before checking for RIFF at bytes 0-3 and WEBP at bytes 8-11.
  - WAV and truncated RIFF files still map to `audio/x-wav`.
  - `validateImageAttachment` takes its predicate by name (`OpenAIPrompt.scala` lines 628-657), so Chat and Responses never evaluate the Decisions-only image predicates.
- [x] **Capability checks.** In `AIRequestAdapter.scala` lines 249-266 and 276-282:
  - SystemOne and embeddings still reject `columnTypes` and `fileSizeLimitMB`.
  - Decisions allows only `messagesCol`, `columnTypes` and `fileSizeLimitMB` from that list.
  - The rule that `fileSizeLimitMB` needs a path column runs after `validateMessageConstruction`.
  - `inputMode` defaults to `"text"` (`AIPromptParams.scala` line 105), so `getInputMode` cannot throw.
- [x] **Column-name resolution.** `validateDecisionsInputColumn` (`AIRequestAdapter.scala` lines 327-334) uses `AIColumnPlan.matches`, which agrees with `resolve`. The case-insensitive conflict test and the case-sensitive distinct-column test are at diff lines 1340-1400.
- [x] **Interaction with AIDefaults.** `AIDefaults.applyPrompt` works on the effective copy. It uses `set` only for url, model and apiVersion, and `setDefault` for the rest. None of the new `isSet` checks depend on those parameters (`AIDefaults.scala` lines 1-230; `AIPrompt.scala` lines 60-140).
- [x] **Durable JSON gate, key order and docs.**
  - `AIDurableInput.scala` line 183 still applies the inline-only gate for durable JSON input.
  - `prepareParts` and `omitNulls` update fields in place, and AIJson's ordered map keeps the original key order.
  - The documentation (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md`, current lines 336-356) matches the code: no Files API IDs, PDF/Office or audio for Decisions, and System One stays text-only.
- [ ] Not done: I ran no tests, builds or provider E2E checks. The driver's results (303 passes, 0 failures, 3 credential-gated live tests canceled) were not independently verified.

## Issues

### Issue 1: optional — Decisions path/inputMode check runs before the messagesCol conflict check
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: 258-260
- **Description**: In `validateCapabilities`, the new `require(!hasDecisionsPathColumns || stage.getInputMode == "text", ...)` runs before `if (stage.isSet(stage.messagesCol)) validateMessageConstruction()`.
  - Take a Decisions stage with `messagesCol`, a path entry in `columnTypes`, and `inputMode="json"`.
  - The first error is "Decisions path columns require inputMode='text'; use explicit image parts with JSON input". That advice cannot be followed, because `inputMode` and non-empty `columnTypes` are both invalid with `messagesCol`.
  - If the user sets `inputMode` to `"text"` as told, the next error is "inputMode cannot be combined with messagesCol". After removing `inputMode`, they get "Non-empty columnTypes cannot be combined with messagesCol".
  - This goes against the stated goal that "messagesCol reports its own construction conflict". The same diff applies that rule to `fileSizeLimitMB` by placing its new check after `validateMessageConstruction()`.
  - Existing tests do not cover this combination. `AIPromptMediaInputSuite.scala` lines 613-640 test JSON plus a path column, and `messagesCol` plus a path column with the default `inputMode`, but never all three together.
- **Risk**: Users get misleading guidance and fix conflicts one error at a time. Requests are still rejected before any IO or HTTP, so there is no correctness or data-safety impact.
- **Suggested Fix**: Move the path/inputMode `require` after `if (stage.isSet(stage.messagesCol)) validateMessageConstruction()`.
  - When `messagesCol` is set, `validateMessageConstruction` already rejects a set `inputMode` and any non-empty `columnTypes`, so the reordered check only runs when `messagesCol` is not set. Existing expectations stay the same.
  - Add a case to "Decisions path columns reject conflicting input modes and keep System One text only" that combines `messagesCol`, a path `columnTypes` entry and `setInputMode("json")`. Assert that the message names the `messagesCol` conflict.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Optional fix to the order of error messages; behavior and safety are unaffected. Pending the driving agent's decision.
- **How verified**: Pending. Should be verified by the suggested `AIPromptMediaInputSuite` case, plus the existing `AIPromptRecoveryPreflightSuite` and `AIPromptPreflightSuite` tests for messagesCol construction settings.

## Driver disposition

Issue 1 is closed without a source change as optional precedence for a
multiply-invalid configuration. `validateCapabilities` (249-266) and
`validateMessageConstruction` (276-283) reject all three incompatible settings
before attachment IO or inference. Each reported error remains accurate.
The documentation requires text mode for path columns and prohibits combining
them with messagesCol; it does not promise a particular first error when the
caller violates multiple rules.

The specific fileSizeLimitMB diagnostic requirement is preserved: its missing
path check follows messagesCol construction validation. That requirement does
not establish a general precedence contract for inputMode plus path columns
plus messagesCol. The review-context shorthand about messagesCol should not
be read as adding that unrequested guarantee.

Reordering could reduce configuration retries, but the reviewer identifies no
correctness or data-safety impact. No new precedence assertion or implemented
fix is claimed. Existing public construction/preflight cases passed in the
303-test integration run. The original Low finding and `ISSUES_FOUND` verdict
are preserved; the frozen product patch remains unchanged for the next lens.