---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 6
timestamp: "2026-11-18T00:54:13Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "0a98c92e72246e8541a3fcc4daffc8bce68eab097e9b4e7b1d6c3ba364739584"
---

## Runner Failure

Review sections contradict the Issues Found count or verdict

## Original Response

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-16-stage-attempt-1-review-6-decisions-paths-opus.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff coverage.** I reviewed the supplied diff hunks for `AIPrompt.scala`, `AIRequestAdapter.scala`, `NativeQuestions.scala` and `OpenAIAttachmentUtils.scala`. The diff in the prompt was cut off after the `OpenAIAttachmentUtils.scala` hunk. I therefore read the remaining changed production and documentation content directly from the worktree, which the driver states matches the frozen blobs:
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` lines 236–330 and 560–665
  - `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` lines 100–116, 240–372, 424–440, 722–775 and 1047–1068

  I did not open anything under `reviews/**` or `.git/**`.
- [x] **Lazy loading for Decisions (`AIPrompt.scala` 377–383, 474–482; `AIRequestAdapter.scala` 582–631).**
  - Checks run in this order, all before `messages()` triggers image IO: `selectedModel`, `requestVersion`, `mergedOptions`, question parse/validate, then `validateDecisionsImageCount(pathImageCount, …)`. Header validation in `buildRequest` runs after IO, which matches doc lines 304–310.
  - `AIPrompt.scala` 477–478 is the only caller of `adapter.request`. Chat/Responses still build messages eagerly through the eager overload.
  - The non-blank predicate in `imageCount` matches the `orderedAttachments` filter (`OpenAIPrompt.scala` 608–610).
- [x] **Error codes and messages.**
  - A null or empty Decisions path prompt gives `AIMissingInputException("input is null or empty")`, which becomes `missing_input`. The text matches the existing native `input()` message (`AIRequestAdapter.scala` 535–543).
  - Whitespace-only text fails in `NativeQuestions.text` and becomes `invalid_request`.
  - Both happen before `generatedMessages`. The Chat/Responses message "prompt is null" is unchanged.
- [x] **Preflight checks (`AIRequestAdapter.scala` 247–262, 310–332).**
  - `messagesCol` with `fileSizeLimitMB` reports the construction conflict before the Decisions "requires a 'path' column" rule.
  - `systemPrompt`, `dropPrompt` and `postProcessing` stay rejected for Decisions.
  - Path columns require `inputMode='text'`.
  - The `inputCol` path-overlap check uses `AIColumnPlan.matches`, so it follows Spark's case-sensitivity setting.
- [x] **File size limit docs are accurate.** `AIPrompt.scala` 218 sets `useBoundedAttachments = true`. With `OpenAIPrompt.scala` 623–649 and `OpenAIAttachmentUtils.scala` 398–420, a finite limit reads limit+1 bytes and an unset or infinite limit reads the whole file. This matches doc lines 291–296 and 754–759.
- [x] **RIFF/WebP fix (`OpenAIAttachmentUtils.scala` 295–308).** Only a RIFF file of at least 12 bytes with `WEBP` at offset 8 becomes `image/webp`. WAV files and short RIFF files keep `audio/x-wav`. This is pinned by `AIPromptMediaInputSuite.scala` 336–372, where the Chat message includes the MIME type.
- [x] **Hosted image URLs (`NativeQuestions.scala` 203–241).**
  - Validation only parses with URI/URL and makes no network call.
  - Accepted and rejected forms are pinned by `NativeQuestionsSuite.scala` 101–118 (accepted) and 223–257 (rejected).
  - The rejected-URL test (`NativeQuestionsSuite.scala` 223–257) uses `intercept` without checking the message text, so rewording that message will not break it.
  - The test "Native validation errors do not disclose supplied values" requires that error messages never include the input itself.
- [x] **Durable mode.** `AIDurableInput.scala` 84–120 and 143–186 reject non-data URLs in path cells, `messagesCol`, Decisions JSON input and questions. Violations stop the run before inference instead of becoming row errors. The general rule is documented in `docs/Explore Algorithms/OpenAI/Durable AI results.md` 281–296.
- [x] **Docs vs code.**
  - Parameter table rows (doc lines 733–738) match `validateCapabilities`.
  - The live-test text (doc lines 432–436) matches `OpenAIDecisionsLiveSuite.scala` 22–64: both stages, path-inline and hosted-JSON cases, and an explicit HTTPS `assume` message.
  - The stale "inline-only" class comment was removed.
  - There is no hand-written Python under `cognitive/src/main/python` that needs updating.
- [x] **Chat/Responses performance.** The only additions are a per-row `LazyRef` and closure, which are negligible. Because `validateImageAttachment` takes a by-name parameter, Decisions-only parsing never runs for Chat/Responses.
- [x] **Driver-supplied validation, reviewed but not re-run.** 58 tests in three suites passed on the frozen patch, plus 228 unchanged tests from the earlier full run. ScalaStyle, compile, test compile and codegen passed. No Python runtime or provider end-to-end test was run, and the three live tests were canceled.
- [x] **Severity.** I found no Critical, High or Medium defects in this lens. All six findings are Low polish items, and none breaks a stated requirement.
- [ ] **OpenAI doc links not checked.** The `developers.openai.com/api/docs/guides/decisions#…` anchors could not be verified without network access.
- [ ] **Python wrappers not exercised.** This review is read-only.

## Issues

### Issue 1: Decisions format rejections use one message for three different causes
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628–629, 634–635, 645, 649
- **Description**: Three different checks all throw the same text, "Decisions path columns only support JPEG, PNG, GIF, and WebP images":
  - the filename-extension check before IO (`.jfif`, `.jpe`, `.ashx`, `.bmp`);
  - the data URL media-type check (`image/jpg`);
  - the content check after the file is read (for example, a `.png` file containing XBM, as in `AIPromptMediaInputSuite.scala` 418–420).

  Two of these reject real JPEGs (`.jfif` and `data:image/jpg`), so the message tells the user to send a format they already sent. The matching Chat/Responses errors name the detected MIME type (`Unsupported file type: <mime>.` and `File type 'audio' with MIME type 'audio/x-wav'`).
- **Risk**: When many rows fail with the same text, users cannot tell rows they can fix by renaming from rows with unsupported content without reading doc lines 311–322. This makes errors harder to diagnose and increases support load.
- **Suggested Fix**: Keep the JPEG/PNG/GIF/WebP prefix and add a fixed hint for each check:
  - extension: "unsupported filename extension; use .jpg, .jpeg, .png, .gif or .webp";
  - data URL: "data URL media type must be image/jpeg, image/png, image/gif or image/webp";
  - content: append the detected base MIME type.

  Do not include the path, URL or user-declared parameters; `AIPromptMediaInputSuite.scala` 455 asserts no leakage. Update the exact-match assertions at `AIPromptMediaInputSuite.scala` 363, 455 and 495.

### Issue 2: The hosted-URL error says "must use HTTP(S)" even when the URL is HTTPS but malformed
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 203–213, 229–230, 240
- **Description**: Every `hostedImageUrl` failure gives "Decisions images must use HTTP(S) URLs or inline base64 image data URLs". That includes unescaped spaces, bad `%` escapes, signed or Unicode-digit ports, and empty hosts. For `https://cdn.example.com/my photo.png` the user did send an HTTPS URL, so the message points to the wrong fix.

  The code also uses `uri.toURL.getHost` on purpose: `URI.getHost` returns null for underscore and non-ASCII (IDN) hosts. Only the port check has a comment explaining it.
- **Risk**:
  - Users get unhelpful `invalid_request` rows. Spaces in blob names are common.
  - Someone could later "simplify" the code to `uri.getHost` without knowing why it was avoided. Tests would catch the break, but the reason is not visible in the code.
- **Suggested Fix**:
  - For non-data URLs, use a message such as "Decisions image URLs must be well-formed absolute HTTP(S) URLs with a host (percent-encode spaces), or inline base64 image data URLs". Do not include the URL. No test checks this exact text.
  - Add a one-line comment: `URI.getHost` is null for underscore and IDN hosts, but `URL.getHost` still returns them.

### Issue 3: The durable-mode paragraph doesn't mention explicit message and JSON-input images
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Unified AI prompting.md
- **Line(s)**: 246–250, 325–328
- **Description**: Lines 246–250 say explicit message and question parts send hosted URLs to the provider unchanged. The durable paragraph then lists only path cells and question instructions as needing inline data. In code, `AIDurableInput.scala` 84–120 and 143–186 also reject hosted `image_url` in `messagesCol` and in Decisions JSON input, and the whole run stops instead of producing a row error. `Durable AI results.md` 281–296 states the general rule, but the Decisions paragraph reads like a complete list.
- **Risk**: Users who combine the newly documented hosted explicit images with `outputPath` hit a run-level failure that this section suggests won't happen.
- **Suggested Fix**: Reword to: "With `outputPath`, every Decisions image (path cells, explicit `messagesCol`/JSON input parts and question instructions) must be an inline data URL; hosted URLs stop the run before inference. See Durable AI results."

### Issue 4: The 128-image limit has two message variants and three hard-coded `128`s
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 15, 118, 136, 165–167
- **Description**: The same request-wide limit gives "Decisions accepts at most 128 images" when the input alone is over (136). It gives "…per request" when the overflow comes from questions or the combined total (118, 167). All three messages hard-code `128` instead of using `MaxImages`, and none reports the counted total.
- **Risk**:
  - Inconsistent text makes error grouping and alerts harder.
  - Changing `MaxImages` would leave the messages out of date.
  - Users cannot see how far over the limit a row is.
- **Suggested Fix**: Use one shared message for all three checks, for example `s"Decisions accepts at most $MaxImages images per request"`. Optionally append the computed total; it is a count, not user input. Current tests use `contains("at most 128 images")` and would still pass.

### Issue 5: `validateImageAttachment` sounds like general validation but is a lazy, Decisions-only check
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala
- **Line(s)**: 628–629, 649
- **Description**: This private helper sits in the base class shared by `OpenAIPrompt` and `AIPrompt`. It does nothing for every API except Decisions, so it never does anything in legacy `OpenAIPrompt`. Its by-name parameter is what keeps Decisions-only parsing out of Chat/Responses. The allowed Decisions MIME types are an unnamed `Set(...)` literal, rebuilt on each call and defined apart from `imageExtensions`.
- **Risk**:
  - Someone could call it expecting it to check Chat/Responses images, and it would silently do nothing.
  - Someone could change `=> Boolean` to `Boolean`. That would run Decisions-only parsing on every Chat/Responses attachment and could change which error those APIs report first. The requirements say Chat/Responses must stay unchanged.
- **Suggested Fix**: Rename it to something like `requireDecisionsImage(isSupportedImage: => Boolean)` and add a short comment that the argument must stay lazy. Move the MIME set into a named private val next to `imageExtensions`.

### Issue 6 (optional): Decisions path images are copied and re-validated after they were already validated
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: NativeQuestions.scala 227–238; AIRequestAdapter.scala 594–596; OpenAIAttachmentUtils.scala 207–219, 228–232
- **Description**: Generated path messages now go through `prepareDecisionsRequest` and then `validatePart`. That step copies the whole base64 payload with `url.substring(comma + 1)` and scans it again with `validateBase64Content`. For file and HTTP paths, SynapseML produced the base64 itself. For inline data URL path cells, `prepareDataUrlAttachment` has already run the same check. The check cannot fail for these parts, but each image gets an extra temporary copy about 1.33× the file size plus another full scan. Chat/Responses path images skip this step.
- **Risk**: More executor memory and CPU for large images, especially with `fileSizeLimitMB` unset and higher `concurrency`. This is not a correctness problem.
- **Suggested Fix**: Validate without copying. Add a `validateBase64Content(value, start)` overload or one that takes a `CharSequence`, and check emptiness with `url.length > comma + 1`. This removes the copy for every Decisions image while keeping a single validator.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver triage (Low; makes errors easier to diagnose).
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver triage (Low; wording fix and a comment).
- **How verified**: pending

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver triage (Low; documentation completeness).
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver triage (Low; consistent messages).
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver triage (Low; naming and maintainability).
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: Pending driver triage (Low; optional performance improvement).
- **How verified**: pending

## Driver disposition

This invocation remains **failed evidence**, not a qualifying review. The
runner rejected the report structure; its original response and failure are
preserved above. The response is still useful feedback for driver triage.
Pass 16 is also invalidated by the subsequent test/documentation update.

- **Issue 1, diagnostic alternatives: not adopted.** The current sanitized
  message correctly states the supported formats, and the guide already names
  the required extensions and standard MIME spellings. Splitting it into three
  error contracts is optional polish, not a demonstrated behavior defect.
  Existing exact-message and no-leakage tests remain passing.
- **Issue 2, hosted-URL wording/comment: not adopted.** A malformed HTTPS string
  is not a valid HTTP(S) URL. The generic error describes the accepted inputs
  without exposing supplied values. The new public-path regression now pins
  that diagnostic; accepted underscore/IDN hosts remain covered. This preserves
  current behavior rather than adding a wording-only production edit.
- **Issue 3, durable-mode documentation: addressed.** The Decisions paragraph
  now explicitly includes path cells, messagesCol/JSON image parts, and both
  question bindings. It explains that external media stops the run before
  inference. This matches `AIDurableInput.inline`, `messages`, `validateCell`
  and `validateRows`; no durable behavior changed.
- **Issue 4, limit-message consolidation: not adopted.** Both existing messages
  accurately state the current 128-image limit. Boundary and combined-count
  tests pin its enforcement. Consolidating wording for a hypothetical future
  limit change is not required for this compatibility fix.
- **Issue 5, helper rename/constant extraction: not adopted.** The helper is
  private, and its sole API guard is explicit. Current callers and the lazy
  predicate are directly verified; no caller depends on it validating another
  API. Additional extension points or renaming are optional maintainability
  changes, retained as suggestions rather than required fixes.
- **Issue 6, copy/scan optimization: not adopted.** The temporary-copy cost is
  real, but no scale regression was measured. Keep authoritative native
  validation in this focused patch. In particular, reader acceptance does not
  imply native-header acceptance: the new `name`-without-value path regression
  demonstrates that boundary. An offset-aware base64 validator could reduce
  allocation without bypassing validation, but is a separate optimization.

The guide also now states explicitly that earlier supported paths may be read
before a later unsupported path is rejected, preserving the per-file ordering
disposition from round 3.

**Verification:** 57 focused media/attachment tests pass, as do root
style/compile/test-compile/codegen gates and static wrapper/example checks.
Production remains unchanged. The next frozen product patch is
`5f939e3f515e6f44844ce05af6b10327dd65b1a020e282378b72bff27834d3d7`
and requires a fresh complete review; neither this failed report nor pass 16
qualifies it.