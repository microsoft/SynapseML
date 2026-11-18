---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 19
stage_attempt: 1
round: 3
timestamp: "2026-11-18T07:49:41Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1e1b20a86bc17665923afc179c839f8b5988b70952ab4f29e411421c706519e9"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-19-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Scope.** I reviewed the full 13-file patch in `.review-input-decisions-paths.diff` against the post-change source:
  - main source: AIPrompt, AIRequestAdapter, NativeQuestions, OpenAIAttachmentUtils, OpenAIPrompt;
  - seven test suites;
  - `docs/Explore Algorithms/OpenAI/Unified AI prompting.md`.

  I inspected target-only AIDefaults code only to check how it interacts with this PR. I did not open anything under `reviews/**`.
- [x] **Checks before image IO.**
  - `AIRequestAdapter.request` (612-636) runs `selectedModel`, then `requestVersion`, then `mergedOptions`, then `nativeEnvelope` (584-600).
  - `nativeEnvelope` parses and validates row questions and calls `validateDecisionsImageCount(pathImageCount, …)` before calling `messages()`.
  - `AIPrompt.messages` (387-395) raises `missing_input` for null or empty prompt text and `invalid_request` for whitespace-only text, before `generatedMessages` performs IO.
  - This matches docs lines 302-310 and the messages for Decisions without path columns (`input()`, AIRequestAdapter 537-545).
- [x] **Lazy vs. eager message preparation.**
  - `prepareRequests` (AIPrompt 484-488) forces the Decisions `lazy val` exactly once, inside `nativeEnvelope`.
  - If that initializer throws, nothing forces it again: Decisions never retains generated messages, because `retainedMessagesName` (305-310) only applies to generative APIs (call site 604-605).
  - Chat, Responses, SystemOne and embeddings use the by-value overload (AIRequestAdapter 612-613), so they stay eager.
- [x] **Image count matches the request.**
  - The pre-IO count `paths.count(... _.trim.nonEmpty)` (AIPrompt 486) uses the same trim rule as `createMessagesForRow` (OpenAIPrompt 608-617).
  - Each non-blank cell produces exactly one `input_image` or throws (`validateImageAttachment` 628-629; gates at 634-635, 645 and 649).
  - `prepareDecisionsRequest` (NativeQuestions 151-163) re-checks the combined count after IO.
- [x] **Exception types.**
  - Every new failure path raises `IllegalArgumentException`, `AIMissingInputException` or one of the attachment exceptions handled by the `prepareRequests` catch block (AIPrompt 492-504).
  - The NativeQuestions count and prepare helpers (165-184) only read fields that `decisions()` (86-119) or `validateDecisionsInput` (121-138) already validated, so no `MatchError` or `NoSuchElementException` can escape.
- [x] **Extension and MIME checks.**
  - `mayBeImagePath` (OpenAIAttachmentUtils 315-318) runs before `prepareFile` reads anything.
  - `dataUrlFileType` (431-442) runs before data URLs are decoded or size-checked.
  - `image/jpg`, `image/bmp` and an empty MIME type all map to non-image types (`MimeTypeExtensions` 137-157, `categorizeFileType` 320-338).
  - A missing comma or non-base64 data URL raises a controlled `IllegalArgumentException` (`dataUrlMetadata` 159-170).
- [x] **WebP vs. WAV.** `inferMimeType` (295-308) needs at least 12 bytes with `RIFF` at offset 0 and `WEBP` at offset 8. WAVE files and short RIFF files fall through to the JDK guess, so WAV is still classified as audio.
- [x] **Hosted image URLs.** I traced `hostedImageUrl` (NativeQuestions 203-213):
  - Accepted: underscore hosts, IDN hosts, IPv6 and an empty port.
  - Rejected: empty host, userinfo with no host, signed or non-ASCII-digit ports, integer-overflow ports, multiple ports, spaces and non-HTTP schemes.
  - This matches the NativeQuestionsSuite cases (101-116, 222-252). The remaining gap is Issue 1.
- [x] **Configuration validation.**
  - `validateCapabilities` (249-267) checks `inputMode` first, then messagesCol construction conflicts (277-283), then the `fileSizeLimitMB` path-column rule.
  - The `inputMode` default is `text` (AIPromptParams 105), so the updated preflight expectations are consistent (diff 1072-1130 and 1331-1450).
  - `validateDecisionsInputColumn` (327-334) uses the same Spark resolver as `AIColumnPlan.resolve` (AIColumnPlan 37, 48-50).
- [x] **Column order.** `attachmentColumns` (AIPrompt 359-361) sorts path columns by schema index, so the docs' "processed in column order" claim holds.
- [x] **Durable outputPath checks.** `AIDurableInput.inline`, `content` and `messages` (84-121), plus `validateRows` mapping path columns to the `inline` kind, require inline data URLs for:
  - path cells (after trimming);
  - messagesCol and JSON image parts;
  - question-instruction image parts, reached through the recursive `externalBindings` (20-33, 45-58).
- [x] **Bounded reads.**
  - `readBoundedFile` (OpenAIAttachmentUtils 353-382) reads at most the limit plus one byte. It only falls back to a whole-file read when the computed limit saturates at `Long.MaxValue`.
  - `readAttachment` (100-112) rethrows `AttachmentSizeException`, which becomes `invalid_request`.
- [x] **AIDefaults interaction.**
  - `transform` (AIPrompt 344-353) validates and runs inference on `effectiveCopy()`.
  - The Decisions path and size parameters do not depend on scoped defaults.
  - `OpenAIDecisions extends AIPrompt` (OpenAIDecisions.scala 15), so it inherits `useBoundedAttachments = true` (AIPrompt 227).
- [ ] **Tests not run.** I only had read-only tools, so I relied on the driver's evidence (303 passes) without re-running it.
- [ ] **Coverage gaps.** No test covers hosted-URL ports `0` or `65536+`, or a stalled HTTP(S) path read. The Hadoop `HttpFileSystem` behavior described in Issue 2 comes from Hadoop's implementation, not from this repository.

## Issues

### Issue 1: (optional hardening) Hosted Decisions image URLs accept ports outside 1-65535
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**:
  - 203-213, used by `validatePart` at 239-241.
  - Tests: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestionsSuite.scala 222-252.
- **Description**:
  - `hostedImageUrl` checks that the port text is ASCII digits and that `URL` can parse it, but it never checks the port range.
  - `URI` and `URL` accept any non-negative `int` port. So `https://example.invalid:0/a.png`, and ports from `:65536` up to `:2147483647`, pass validation in both shared input and question instructions. These URLs are forwarded to the provider unchanged.
  - The endpoint validator in the same package rejects these values (`AIRequestAdapter.scala` 31, 46-47: "Endpoint port must be omitted or between 1 and 65535").
  - The suite's rejection list covers `:-1`, `:+443`, `:bad`, Arabic-Indic digits and integer overflow, but has no `0` or `65536` case.
- **Risk**:
  - A URL that can never be fetched reaches the provider. The row fails remotely, with provider-specific error text, added latency and possible billing.
  - Other malformed image URLs instead fail locally as `invalid_request`, without any HTTP call.
  - RFC 3986 syntax (`port = *DIGIT`) technically allows these ports. This is hardening, not a breach of the "malformed URI syntax is rejected" contract.
- **Suggested Fix**:
  - After the existing checks, add a range check: `val port = uri.toURL.getPort; port == -1 || (port >= 1 && port <= 65535)`.
  - Ideally, share the upper bound with `AIRequestAdapter.MaxPort`.
  - Keep accepting `https://host:/…` (empty port), which `URL` reports as -1.
  - Add `:0` and `:65536` to the NativeQuestionsSuite rejection list, plus one question-instruction case.

### Issue 2: HTTP(S) path-column reads have no connect/read timeout, and stage timeouts or cancellation cannot interrupt a stalled read
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Unified AI prompting.md (behavior is in cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala)
- **Line(s)**:
  - docs 291-301, 637-654 and 985-997.
  - OpenAIAttachmentUtils.scala:
    - `readAttachment`: 100-112
    - `readFile`/`readBoundedFile`: 350-382
    - read call inside `prepareFile`: 410-414
- **Description**:
  - This PR newly lets Decisions path columns hold HTTP(S) URLs. The docs say "Path columns accept … HTTP(S) URLs" and "Spark loads file/HTTP inputs during execution".
  - These reads go through `new HPath(url)` and then `FileSystem.open` (`readFile`/`readBoundedFile`). SynapseML sets no connect or read timeout on them.
  - `readAttachment` checks for cancellation only before and after the read. Its interruption handling (docs 993-997) only applies once a read actually fails.
  - The stage's `timeout` and `concurrentTimeout`, and the bounded handler's cancellation monitor, are documented for inference requests only (docs 637-654), not for attachment loading.
  - Hadoop's `HttpFileSystem` (outside this repository) opens a plain `URLConnection` with the JDK's default infinite timeouts. Classic socket reads also ignore `Thread.interrupt()`.
  - Chat and Responses already had this behavior through the same shared reader. This PR makes it reachable from Decisions and documents HTTP(S) path loading there without a caveat.
- **Risk**:
  - A slow, deliberately stalling or half-open image host can tie up an executor task indefinitely.
  - Neither Spark task kill (including with `interruptOnCancel=false`) nor `timeout`/`concurrentTimeout` bounds this wait.
  - The job can hang without ever producing an `input_io_error` row. Each stalled URL blocks one task, and the stall repeats on every task retry.
- **Suggested Fix**:
  - In the Decisions path-column paragraph, state that HTTP(S) path reads are not covered by `timeout` or `concurrentTimeout`.
  - For untrusted or slow hosts, recommend explicit hosted `input_image` parts (which the provider fetches) or copying images to Spark-readable storage first.
  - Optionally mention the executor JVM options `-Dsun.net.client.defaultConnectTimeout` and `-Dsun.net.client.defaultReadTimeout` as a mitigation. They apply JVM-wide.
  - Optionally, in a follow-up, give the attachment reader explicit HTTP(S) connect and read timeouts tied to the stage `timeout`. This does not need to block this focused PR.

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

## Driver disposition

Both Low recommendations are closed as nonblocking scope decisions, with the
original findings and `ISSUES_FOUND` verdict retained. Neither is represented
as an implemented source fix.

1. **Hosted-image port ranges:** retain the current URI-syntax/pass-through
   contract. `NativeQuestions.scala` 203-213 checks HTTP(S), a nonempty host,
   parseability and the RFC's ASCII port syntax; it does not promise that every
   accepted resource URL is fetchable. The reviewer explicitly distinguishes
   this optional semantic restriction from malformed URI syntax. The inference
   endpoint's stricter range policy is a different, client-owned connection
   boundary. Adding that restriction to forwarded media URLs is not required
   for this compatibility fix. Port-range rejection tests were not added.
2. **Attachment-read deadlines:** acknowledged shared-reader limitation, not
   dismissed as bounded. `readFile`/`readBoundedFile` (350-382) delegate to the
   configured Hadoop filesystem without propagating stage inference timeouts.
   Actual low-level deadlines depend on that filesystem/JVM configuration; this
   review did not execute a stalled-read test. The cancellation documentation
   (985-997) describes interrupted/failed reads and does not promise that every
   blocked filesystem read can be interrupted. The HTTP-handler section
   (637-654) describes its own requests/workers, not a byte-reader deadline.
   The PR description now explicitly distinguishes these boundaries and points
   to existing hosted-image or preloaded-storage alternatives. The repository
   documentation and reader are unchanged; cross-API timeout controls remain a
   separate possible follow-up, and no global JVM settings were changed.

The driver inspected the cited URI, reader and documentation source. The
303-pass integration evidence remains applicable to the unchanged product
patch; it does not prove stalled-host behavior or provider acceptance.