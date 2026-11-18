---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 5
timestamp: "2026-11-17T17:15:09Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b54d28d86f3e66e807dfb109606f4b38193278eef91dee060b68492b9f4e732f"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-13-stage-attempt-1-review-5-decisions-paths-opus.md
- **Issues Found**: 6
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Matched every changed production branch in the supplied patch to tests:
  - `AIPrompt.scala`: `messages()` (377–384) and `prepareRequests` (474–478).
  - `AIRequestAdapter.scala`: `validateCapabilities` (248–258), `nativeEnvelope` (571–587) and both `request` overloads (599–620).
  - `NativeQuestions.scala`: instruction parts (89–93), image counting and preparation, `hostedImageUrl` (203–213) and `validatePart`.
  - `OpenAIAttachmentUtils.scala`: `inferMimeType` (295–308).
  - `OpenAIPrompt.scala`: the Decisions guards (628–649).
- [x] Confirmed coverage in `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`:
  - hosted and inline casing (122–163);
  - question images through bindings, copies and save/load (164–205);
  - PNG path loading from a local file, HTTP, extensionless HTTP and inline, with exact payloads (206–260);
  - invalid prompt text and metadata rejected before IO (262–292, 361–398);
  - WebP accepted and extensionless WAV kept as audio across all four stages (293–360);
  - exact error codes and messages for IO and format failures, including a missing PNG giving `input_io_error` (399–456);
  - known non-image paths rejected before IO (457–486);
  - the shared image limit and the JSON-plus-question limit (487–513, 561–586);
  - unused settings and input modes (515–560);
  - null image-option normalization (587–616).
- [x] Confirmed `NativeQuestionsSuite.scala` covers:
  - hosted URL positives, including IPv6, user info and an empty port (101–116);
  - image instructions (118–127);
  - malformed array instructions and the 128/129 question-only boundary (129–150);
  - the 128/129 input boundary (234–236).
- [x] Confirmed durable coverage in `AIPromptRecoveryAttachmentSuite.scala` 26–105:
  - inline path images succeed and resume with no extra requests;
  - external path rejections;
  - hosted question images are rejected with the durable message (74, 80).
- [x] Checked that `AIRequestAdapter.scala` 599–620 does not wrap exceptions, so Decisions' lazy message construction raises them inside the `prepareRequests` try block and the existing error mapping still applies (proven by `AIPromptMediaInputSuite.scala:411`). Decisions never forces messages again for retention, because only generative APIs retain messages.
- [x] Scoped greps found:
  - JPEG appears only as hosted `.jpg` URLs (`AIPromptMediaInputSuite.scala:127`, `NativeQuestionsSuite.scala:103`), and no GIF fixture exists;
  - `OpenAIDecisionsSuite.scala` and `OpenAIDecisionsLiveSuite.scala` have no path or image coverage;
  - the positive empty-`columnTypes` checks skip Decisions (`AIPromptPreflightSuite.scala:87`, `AIPromptRecoveryPreflightSuite.scala:83`);
  - `OpenAIRequestOptionsSuite.scala` covers only Chat and Responses.
- [x] I did not run any tests; pass status relies on the driver's validation. None of the findings is a demonstrated product defect. All are gaps in coverage, assertions or mock adequacy.
- [x] Independence: I read no `reviews/**`, `.git/**` or session content, and kept searches within `cognitive/src/main` and `cognitive/src/test`.
- [ ] Live-provider acceptance not checked: this pass is read-only with no credentials or network access (see Issue 6).

## Issues

### Issue 1: Decisions path images have no positive JPEG or GIF test
- **Severity**: Medium
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: tests 206–260 and 408–421; production 634–635 and 649
- **Description**:
  - Two new Decisions-only gates decide whether each path-loaded image is accepted, in both public stages:
    - the data-URL check, `dataUrlFileType(...) == "image"` (634–635);
    - the post-read allowlist, `Set("image/jpeg","image/png","image/gif","image/webp")` (649).
  - Positive tests use only PNG (`pngBytes` and `pngDataUrl`) and WebP.
  - The only JPEG references are hosted URLs, which are forwarded unchanged and never reach either gate. There are no GIF fixtures at all.
- **Risk**:
  - Reading the code, a JFIF JPEG is detected as `image/jpeg` and a GIF as `image/gif` (`MimeTypeExtensions` 137–141), so current behavior looks correct.
  - However, a typo in the allowlist (for example `image/jpg`) or a classifier change would reject every JPEG and GIF with an error message that names those formats as supported. All existing tests would still pass.
- **Suggested Fix**:
  - Add `ImageIO` fixtures for "jpg" (`TYPE_INT_RGB`) and "gif", each as a local file (`.jpg`, optionally `.jpeg`) and as an inline data URL.
  - Assert the exact `input_image` part for both stages.
  - Optionally add `data:image/jpg;base64,...` to the invalid list to pin the documented standard MIME naming. It maps to `bin` and is rejected as unsupported.

### Issue 2: Durable path-column rejection accepts any SparkException
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptRecoveryAttachmentSuite.scala`
- **Line(s)**: 47–50
- **Description**:
  - `intercept[SparkException](rejected.transform(...))` checks only the exception type and the request count.
  - It does not check the `AIDurableInput.scala:88` message ("Durable media must be inline base64...") or that no results were written.
  - The Responses version of this test (`AIPromptDurableBindingsSuite.scala` 707–708) asserts both.
- **Risk**: An unrelated failure (an NPE in the new lazy path, a Delta error, or a path-normalization exception) would satisfy the test. A regression that stops the inline-only guard from firing for Decisions path columns could then go unnoticed.
- **Suggested Fix**: Capture the failure and assert that its message contains "Durable media must be inline base64". Also assert `!new File(<outputPath>, "results").exists()`, and keep the request-count check.

### Issue 3: No positive Decisions test for empty or text-only `columnTypes`
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptPreflightSuite.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptRecoveryPreflightSuite.scala`
- **Line(s)**: `AIPromptPreflightSuite.scala` 76 and 87–91; `AIPromptRecoveryPreflightSuite.scala` 83; production `AIRequestAdapter.scala` 248–254 and `AIPrompt.scala` 377
- **Description**:
  - Decisions now always accepts `columnTypes`, and builds messages only when there are path columns. Empty and text-only mappings must therefore keep the native string or JSON `input`.
  - Both preflight suites skip their positive empty-mapping check for Decisions (`if (api != "decisions")`), and the Decisions branch uses `chatReply`.
  - Text-only mappings for Decisions appear only in negative tests (`AIPromptMediaInputSuite.scala:520`, `AIPromptPreflightSuite.scala:67`).
- **Risk**: Two kinds of regression would go unnoticed:
  - rejecting these documented configurations again;
  - gating message construction on `isSet(columnTypes)`, which would turn a text-only mapping's string `input` into a message array and change the exact native request shape.
- **Suggested Fix**:
  - Use `NativeRequestFixtures.DecisionResponse` for Decisions and run the empty-mapping positive check for both stages.
  - Add `setInputCol("text").setColumnTypes(Map("text" -> "text"))`, plus a template variant, asserting no error and `input == JsString(<text>)`.

### Issue 4: New defensive validation branches lack direct tests
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala`; `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`
- **Line(s)**: `NativeQuestions.scala` 89–93 (default branch at 92) and 211; `OpenAIAttachmentUtils.scala` 297–299; tests `NativeQuestionsSuite.scala` 101–116 and 129–150
- **Description**:
  - **(a)** `required` checks only that the field is present, so number, boolean, object and null instructions all reach the new default branch ("must be text or text/image parts"). Every rejection fixture uses array instructions, so this branch is never hit.
  - **(b)** `hostedImageUrl` compares schemes case-insensitively (`equalsIgnoreCase`), but every hosted positive uses lowercase `http` or `https`. Data-URL casing, by contrast, is tested.
  - **(c)** No `RIFF` fixture shorter than 12 bytes exercises the WebP length guard.
- **Risk**:
  - Dropping or reordering the fallback could forward malformed instructions or raise an unmapped `MatchError`.
  - Valid uppercase-scheme URLs could start being rejected.
  - An off-by-one in the length guard would throw `StringIndexOutOfBoundsException` on short RIFF files.
- **Suggested Fix**:
  - Add `42`, `true`, `{}` and `null` instructions to the rejection list, asserting the message.
  - Add `HTTPS://` and `Http://` hosted positives.
  - Add an 11-byte `RIFF` extensionless fixture and assert its existing classification result.

### Issue 5: Multi-image Decisions path rows are untested
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala`
- **Line(s)**: production 476; tests 487–513 and 76–113
- **Description**:
  - The pre-IO count `paths.count(nonBlank)` is pinned only by a row with one non-blank path and one blank path. That proves blanks are excluded, but a mutation to `if (paths.exists(nonBlank)) 1 else 0` would survive.
  - The order of multiple path images is tested only for Chat/Responses (76–113).
- **Risk**:
  - An undercount would download every image before the post-read check in `prepareDecisionsRequest` rejects the row. That breaks the required invariant that known path-plus-question limit failures happen before image IO.
  - A change in image order inside the single Decisions user message would also go unnoticed.
- **Suggested Fix**:
  - Add a second non-blank path column with 126 and 127 question images. Assert that 127 is rejected with the image-server request count unchanged, and that 126 succeeds with exactly two downloads.
  - Run the order test for both Decisions stages and assert the exact sequence of `input_image` parts.

### Issue 6: New hosted and image-instruction shapes are proven only against a permissive mock
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisionsLiveSuite.scala`
- **Line(s)**: 15–28; production `NativeQuestions.scala` 203–245
- **Description**:
  - The patch removes the comment saying the provider's guide and reference disagree on hosted images. It now accepts hosted URLs and image parts in question instructions.
  - `AIRecordingServer` returns a canned 200 response for any request body.
  - The credential-gated live suite covers only text questions.
- **Risk**: If the service rejects hosted `image_url` values or image instructions, users would only see `http_error` rows at runtime. No test, even when credentials are available, would reveal this.
- **Suggested Fix**:
  - Add one case to the live suite behind the same `assume(...)` gate, so it skips without credentials as AGENTS.md requires.
  - Cover an inline path image, a stable HTTPS image and an image instruction, through both Decisions stages, asserting there are no row errors.
  - Make no model-quality claim.

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

### Issue 3
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 4
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 5
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 6
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver triage before collecting the final lens

All six findings are coverage gaps, not demonstrated production defects. Queue
focused additions after the unchanged snapshot's final lens: positive JPEG/GIF
paths, precise durable rejection assertions, native text-only mapping shapes,
malformed instruction and URL-scheme cases, two-path limit/order coverage, and a
credential-gated live image case using the existing live-suite conventions.
Do not treat a skipped live test as provider acceptance. No source or test
changes have been made during collection; the original verdicts remain intact.
This pass cannot qualify the future corrected patch.

## Final driver resolution

1. **Fixed.** Real `ImageIO` JPEG (.jpg and .jpeg) and GIF fixtures now pass
   through local and inline paths, template/input-column modes, copies and
   persistence in both Decisions stages. Assertions verify exact image bytes
   and MIME types in the wire payload. Nonstandard `image/jpg` is rejected.
2. **Fixed.** Durable path rejection now checks the inline-base64 diagnostic,
   absence of the `results` directory, and unchanged inference request count.
3. **Fixed.** A public-stage test checks empty and text-only `columnTypes`
   for both template and input-column modes, including exact native string
   `input`, schema parity, successful answers and one request.
4. **Fixed.** Malformed number/boolean/object/null instructions assert the
   controlled diagnostic; hosted positives include `HTTPS` and `Http`.
   An 11-byte extensionless RIFF header is exercised over local/HTTP paths
   alongside WAV across both Decisions stages, Chat and Responses, with exact
   audio-rejection expectations and no inference.
5. **Fixed.** The path limit test now covers one and two nonblank paths.
   Two distinct GIF/PNG images follow input schema order, not mapping order.
   With two paths, 126 question images succeed with exactly two downloads,
   while 127 fail without additional downloads or inference. Blank exclusion
   remains covered by the one-path case.
6. **Test coverage added; live execution remains unverified.** Two opt-in live
   tests cover generated inline path images and image instructions, plus
   hosted input when `SYNAPSEML_DECISIONS_IMAGE_URL` supplies a stable public
   synthetic HTTPS image. Both public stages are exercised. The local run
   intentionally removes live credentials; all three live-suite tests cancel
   at their configuration gates. No provider acceptance or quality claim.

Verification: root ScalaStyle/test ScalaStyle, compilation/test compilation,
codegen, generated wrapper signatures and six documentation examples pass.
139 deterministic tests pass with no failures or skips; the three live cases
are separately recorded as canceled in ScalaTest's log (its JUnit XML does not
mark assumption cancellations as skips).

Corrected full-patch SHA-256:
`008f86591bfcb8609c7699e9c7f0f25d633629fe26c417c83762cee9088eb8c8`.
Pass 13 remains invalidated by these changes; the original reviewer verdict is
not replaced. A new complete pass is required before publication.