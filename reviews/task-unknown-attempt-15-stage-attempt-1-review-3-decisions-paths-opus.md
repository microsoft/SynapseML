---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 15
stage_attempt: 1
round: 3
timestamp: "2026-11-17T21:43:33Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "dbd42f100e9b47c84f75d2cf3cd8b924330f923e2a3791dec659ad493f3f2b1a"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-15-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Code read.** I read the changed production paths in targeted ranges:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` 349–386, 453–494 and 568–648
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala` 236–332, 370–455 and 455–633
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala` (whole file)
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` 240–316 and 570–656
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` 18–112, 173–199, 280–433
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisions.scala` (whole file). It is a thin `AIPrompt` subclass, so both Decisions stages share one request path.
- [x] **Lazy request ordering.** For Decisions, `messages()` runs once, inside `nativeEnvelope` (`AIRequestAdapter.scala` 588–590). It runs after `selectedModel`, `requestVersion`, `mergedOptions`, the per-row question parse/validate, and `validateDecisionsImageCount`.
  - The `lazy val` (`AIPrompt.scala` 474) caches the result for `retainGeneratedMessages` (481), so there is no second read.
  - If initialization fails, control goes to the catch block and the value is never read again.
  - Chat, Responses and System One still use the eager `Option` overload (`AIPrompt.scala` 478; `AIRequestAdapter.scala` 610–611).
- [x] **Prompt-text gates** (`AIPrompt.scala` 377–384) run before `generatedMessages` reads any file:
  - Null or empty text gives missing_input "input is null or empty", the same message as the text-only `input()` (`AIRequestAdapter.scala` 535–543).
  - Whitespace-only text gives invalid_request through `validateDecisionsInput`/`text()`.
  - Chat/Responses keep "prompt is null" and still accept empty prompts.
- [x] **Pre-IO image count** (`AIPrompt.scala` 476) uses the same `trim.nonEmpty` rule as `createMessagesForRow` (`OpenAIPrompt.scala` 608–610). Each non-blank cell yields exactly one `input_image` or an error, so the pre-check cannot reject a row that would otherwise succeed.
  - `prepareDecisionsRequest` (`NativeQuestions.scala` 151–163) re-checks the real combined count after reading.
  - Per-row questions are validated (`NativeQuestions.scala` 86–119) before `questionImageCount` runs, so `fields("instructions")` cannot throw an exception the catch block doesn't handle.
- [x] **Exception routing.** No new `Try` or `try` wraps any read; the `Try` in `hostedImageUrl` does no IO.
  - `CancelledAttachmentReadException` still becomes a task-fatal `CancellationException` (`AIPrompt.scala` 483–484).
  - `InterruptedException` and `TaskKilledException` from `readAttachment` (`OpenAIAttachmentUtils.scala` 100–112) still propagate.
  - All new validation failures are `IllegalArgumentException`s, so they become row-local invalid_request errors.
- [x] **`hostedImageUrl`** (`NativeQuestions.scala` 203–213), traced against `java.net.URI` and `URL` parsing:
  - Accepted: underscore and IDN hosts, bracketed IPv6 with or without a port, and empty ports.
  - Rejected: empty hosts, port-only or userinfo-only authorities, multiple `@`, signed or non-ASCII ports, out-of-range ports (caught as `MalformedURLException`), malformed IPv6 ports, non-HTTP(S) schemes, and illegal characters.
  - Error messages never echo the URL, and `getHost` does no DNS lookup.
- [x] **Inline image branch** (`NativeQuestions.scala` 215–245):
  - The case-insensitive `data:image/` dispatch is kept.
  - URLs with leading whitespace fail on both branches.
  - A zero-byte `.png` path image ends in the controlled error "Decisions image data must be nonempty".
- [x] **Construction checks** (`AIRequestAdapter.scala` 247–281, 325–332):
  - System One and embeddings still reject `columnTypes` and `fileSizeLimitMB`.
  - The `fileSizeLimitMB` rule uses `isSet`. No default exists (`OpenAIPrompt.scala` 269–296), so defaults never trigger it.
  - `validateMessageConstruction` runs before the Decisions-only limit check.
  - `validateDecisionsInputColumn` uses the resolver-aware `AIColumnPlan.matches` on the driver.
  - Minor, not reported: if `messagesCol`, path columns and `inputMode="json"` are all set, the inputMode error is reported first. It is still a construction error.
- [x] **Per-file format checks** (`OpenAIPrompt.scala` 628–649):
  - The predicate is by-name, so Chat/Responses never evaluate it.
  - Data URLs are classified from metadata before the size check (634–635).
  - Files use the same `attachmentFilename` → `fileExtension` logic as `prepareFile` (645). `HPath.getName` keeps dotted directory names from misclassifying extensionless files.
  - The MIME allowlist is applied after the read (649).
- [x] **WEBP sniff** (`OpenAIAttachmentUtils.scala` 295–308). The `>= 12` length guard prevents out-of-range reads. WAV and truncated RIFF fall through to the JDK. A WebP file misnamed `.wav` becomes "unsupported" instead of audio, which matches the stated correction.
- [x] **Bounded reads** (`OpenAIAttachmentUtils.scala` 353–382) read at most the limit plus one byte, and their error message omits the filename. Infinite or overflowing limits fall back to an unbounded read, as documented in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 292–295.
- [x] **Durable resume.** The fingerprint (`AIPromptOutput.scala` 186–209) covers params and the library version. The per-row question rebuild and field reordering in `prepareDecisionsRequest` therefore cannot change resume identity. Inline-only media enforcement is unchanged.
- [x] **Docs** (`docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 245–329) match the code. They cover hosted URLs forwarded unchanged, the Hadoop query-string and percent-escape limitation, headers validated after loading, per-file rejection of unsupported extensions without a read, and inline-only durable media.
- [x] **Tests inspected.**
  - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`: prompt-text checks before IO (272), metadata checks before IO (376), IO/size/format errors with sanitization (414–471), no-IO rejection of a known extension in a single-path row (473–501), count boundaries (503–540), construction conflicts (561–616), and combined counts (617).
  - `NativeQuestionsSuite` hosted-URL cases (101–247).
  - `OpenAIDecisionsLiveSuite` cancels through `assume` when credentials are absent.
- [x] **Behavior change noted, not reported.** Decisions `messagesCol` preparation is now lazy. On a row with several defects, model, version, options or questions errors now come before "messages is null". No IO is involved, and the order matches the existing text-only Decisions flow.
- [ ] Tests were not run by this reviewer, because the tools are read-only. The driver's report of 284 passing deterministic tests and 3 canceled live tests is treated as context only.

## Issues

### Issue 1: An unsupported path in a later Decisions path column does not stop reads of earlier path columns in the same row
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 602–621 (`createMessagesForRow`, `buildContentParts`) and 628–649 (`validateImageAttachment` checks inside `wrapFileToMessagesList`). Related: `AIPrompt.scala` 474–477.
- **Description**: The Decisions format check (`mayBeImagePath` or `dataUrlFileType`) runs per file inside `wrapFileToMessagesList`. `buildContentParts` calls that function for each path in schema order.
  - Consider a row with two path columns: `asset` holds a supported `.png` URL or file (large, missing or slow), and `second` holds `report.pdf`.
  - The row reads `asset` (file or HTTP read plus size check) before rejecting `second`.
  - The row-level pre-IO gates this PR adds (prompt text, row bindings/options/questions, combined image count) do not include this cheap, metadata-only check.
  - The existing no-IO test ("Decisions known non-image paths reject before IO and size checks", `AIPromptMediaInputSuite.scala` 473–501) uses only single-path rows.
- **Risk**: No row succeeds incorrectly, so the impact is wasted IO, latency and order-dependent diagnostics:
  - A row that is already certain to fail still reads its other images. With no default `fileSizeLimitMB`, that can be up to 127 whole files in executor memory, or a delay on a slow host.
  - The reported error depends on column order and on what happens during the read. A missing earlier file gives `input_io_error` and an oversized one gives a size `invalid_request`, instead of the documented "JPEG, PNG, GIF, and WebP" diagnostic.
  - The docs promise that unsupported extensions are "rejected without reading the file". That holds per file but not per row.
- **Suggested Fix**:
  - Before `buildContentParts` reads anything, check every trimmed non-blank entry in `orderedAttachments` (Decisions only). Use `dataUrlFileType(...) == "image"` for data URLs and `mayBeImagePath` for other paths. Reuse the by-name `validateImageAttachment` so Chat/Responses stay unchanged. Keep the existing per-file checks as a second line of defense.
  - Add a two-path-column regression test for both `AIPrompt(apiType="decisions")` and `OpenAIDecisions`. Put a supported `.png` served by `AIRecordingServer` before a `.pdf`, then assert the format message and zero requests to both the image server and the provider. Optionally, put a missing `.png` first and assert `invalid_request` rather than `input_io_error`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver disposition on the unchanged snapshot

**Issue 1 - optional row-wide preflight optimization, deferred.** The observation
is valid: attachment processing is ordered, so an earlier supported image may
be read before a later unsupported path is rejected. No invalid row reaches
inference or succeeds.

The documented guarantee is explicitly per file ("rejected without reading the
file"), which the implementation satisfies. It does not promise that every
path's metadata is validated before any other path is read, nor that an
unsupported extension takes precedence over an earlier IO/size failure.
`buildContentParts` preserves schema order and reports the first actual failure,
consistent with the existing shared attachment reader. The docs distinguish
the request-wide model/options/questions/image-count prechecks from file
classification and explain finite versus unlimited reads.

A separate scan could save IO for this mixed-invalid-row case, but would add a
new request-wide format-error precedence rule beyond the promised behavior.
It would still not prevent IO for later misleading filenames or unsupported
extensionless content. Retain the finding as a performance opportunity rather
than expanding the current fix or claiming a row-wide no-IO guarantee.

Verification: `OpenAIPrompt.createMessagesForRow`, `buildContentParts` and
`wrapFileToMessagesList` show the per-file ordering; the existing single-file
precheck and two-path ordering/count tests establish the documented boundaries.
The exact patch has 284 deterministic passes. No source was changed, the
original low severity and `ISSUES_FOUND` verdict remain, and the remaining
lenses continue on the same frozen snapshot.