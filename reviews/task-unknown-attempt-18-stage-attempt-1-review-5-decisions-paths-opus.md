---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 5
timestamp: "2026-11-18T05:27:37Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "fe2690461c2449cd8ad98b74d29b6473809b7acf4762ea764ffd81ecafed15aa"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-18-stage-attempt-1-review-5-decisions-paths-opus.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff completeness.** `.review-input-decisions-paths.diff` contains 13 `diff --git` entries, at lines 1, 35, 141, 289, 349, 422, 498, 1072, 1125, 1331, 1449, 1573 and 1643. They match the supplied file list. I recovered the truncated hunks for `OpenAIAttachmentUtils.scala`, `OpenAIPrompt.scala`, `AIPromptAttachmentSuite.scala`, the end of `NativeQuestionsSuite.scala`, `OpenAIDecisionsLiveSuite.scala` and the docs from that file.
- [x] **Production paths traced.**
  - `AIPrompt.scala` 363–386: Decisions prompt-text branch.
  - `AIPrompt.scala` 453–500: lazy `inputMessages`, pre-IO path count, and the cancellation, IO, missing-input and invalid-request mapping.
  - `AIRequestAdapter.scala` 247–264: capability allow-list, `inputMode`, and `fileSizeLimitMB` requiring a path column.
  - `AIRequestAdapter.scala` 325–332: `inputCol` path conflict.
  - `AIRequestAdapter.scala` 582–633: `nativeEnvelope` ordering and the `request` overloads.
  - `NativeQuestions.scala`: `decisions()`, `prepareDecisionsRequest`, `hostedImageUrl` and `validatePart`.
  - `OpenAIAttachmentUtils.scala` 295–318: WebP sniffing, `fileExtension` using `Locale.ROOT`, `mayBeImagePath`, `pathFilename` via `HPath.getName` (dotted directories are safe), and `dataUrlFileType`.
  - `OpenAIPrompt.scala` 575–657: Decisions builds a single user message, and `validateImageAttachment` takes its predicate by name.
- [x] **Pre-IO ordering is pinned by request-count assertions on the image server.** All in `AIPromptMediaInputSuite.scala`:
  - 277–306: null and empty prompt give `missing_input`; whitespace gives `invalid_request`. Exact messages are checked for both stages and for both template and `inputCol`.
  - 383–419: six bad metadata cases (questions, model, version, options).
  - 484–513: the extension gate runs before IO and size checks, bounded and unbounded.
  - 515–564: 128/129 path-plus-question images, with skipped whitespace path cells, download counts, schema-order images and null-field normalization.
- [x] **Chat and Responses behavior is preserved.**
  - `AIPromptAttachmentSuite.scala` 336–351: eager IO still precedes missing-model errors.
  - `AIPromptAttachmentSuite.scala` 375–399: sanitized task-fatal cancellation, now also run for Decisions.
  - `AIPromptMediaInputSuite.scala` 308–381: WebP across four stages and five sources keeps its exact `data:image/webp` payload. WAV and 11-byte RIFF stay audio.
- [x] **URL validation.**
  - `NativeQuestionsSuite.scala` 101–170 and 219–248: accepted and rejected hosted and inline URLs, mixed instruction parts, and per-question and cross-question limits. Each malformed-URL row can fail only through `hostedImageUrl`, so the plain `intercept` assertions are adequate.
  - `AIPromptMediaInputSuite.scala` 123–168: both stages, through both `messagesCol` and JSON, forward exactly the accepted URL set. The `https:///` rejection message is checked exactly.
- [x] **Configuration checks.**
  - `AIPromptMediaInputSuite.scala` 566–639: empty and text-only mappings, rejection of unused settings with the exact `fileSizeLimitMB` message, JSON and `messagesCol` conflicts, and System One `columnTypes`.
  - `AIPromptPreflightSuite.scala` 61–97: the Decisions `messagesCol` matrix covers both stages.
  - `AIPromptRecoveryPreflightSuite.scala` 31–55 and 78–102: the `inputCol` path conflict is rejected case-insensitively, including in durable mode. Distinct column names are accepted under case-sensitive resolution.
  - The new pre-count's `indices(name)` lookup is keyed by the original `columnTypes` key and resolved through `AIColumnPlan.resolve`, at `AIPrompt.scala` 459–461.
- [x] **Durable output.** `AIPromptRecoveryAttachmentSuite.scala` 28–201 covers:
  - local and HTTP path rejection, and hosted images rejected in questions, `messagesCol` and JSON input, with no `results` directory;
  - inline resume with exact `/v1/decisions` input;
  - partial resume with a reordered `ListMap` and a saved and loaded stage, checking committed and pending counts, ownership and image order.
- [x] **Python impact.** I inspected `test_AIPrompt.py` lines 700–830 and 975–1060 and searched `cognitive/src/test/python/synapsemltest/services`. No Python test asserts the old Decisions rejection of `columnTypes`, `fileSizeLimitMB` or hosted images, so no stale test is expected. I did not run these tests; the driver reports that no Python runtime is installed.
- [x] **Docs.** `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 245–347 matches the tested behavior: blank cells are skipped, rows with no usable path give `missing_input`, `image/jpg` and `.ashx` are rejected, and durable output is inline-only. The hosted URL inside question instructions at line 343 has only mocked coverage (Issue 3).
- [ ] **Intentionally not pinned:** reads of earlier paths in column order, row header validation after IO, and `fileSizeLimitMB` not applying to question images. The docs list these as accepted limitations. Pinning them in tests would block future preflight improvements, so I did not raise them as findings.
- [ ] **Test execution:** not performed; this lens is read-only. I relied on the driver's evidence (12 fresh passes in `AIPromptRecoveryAttachmentSuite`, 290 unique passes across four runs, and ScalaStyle and compile passes) without re-verifying it.
- [ ] **Artifact pre-existence:** not checked, because the plan prohibits listing or viewing `reviews/`.

## Issues

### Issue 1: Rewritten Decisions capability allow-list has no negative coverage for postProcessing/postProcessingOptions
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala
- **Line(s)**: 585–611, settings list at 588–593. The production code is `AIRequestAdapter.scala` 248–255.
- **Description**:
  - The PR replaced `require(p == stage.messagesCol && api == "decisions")` with `require(api == "decisions" && decisionsSettings(p))`, where `decisionsSettings` contains `messagesCol`, `columnTypes` and `fileSizeLimitMB`.
  - Tests reject `systemPrompt` and `dropPrompt` for Decisions, but no Scala or Python test rejects `postProcessing` or `postProcessingOptions` for either Decisions stage.
  - Search results confirm this. Every `setPostProcessing*` or `.postProcessing` reference in `cognitive/src/test` targets a generative API or a check at the parameter level. One example is `AIPromptPreflightSuite.scala` 131–152, which never transforms.
  - This is a coverage gap, not a demonstrated defect.
- **Risk**: If someone accidentally widens the allow-list, for example by adding output-parsing settings for image workflows, Decisions would silently ignore those settings, and no test would fail.
- **Suggested Fix**:
  - Add two cases with path columns to the existing settings list: `setPostProcessing("csv")` and `setPostProcessingOptions(Map("jsonSchema" -> "value STRING"))`.
  - For these cases, assert that the message contains `is not supported by apiType='decisions'`. Checking only the parameter name could pass on an unrelated option-validation error.

### Issue 2: Decisions-only pre-IO extension gate is never exercised with uppercase or mixed-case image extensions
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala
- **Line(s)**: 212–275 (source list at 216–228), 308–341 and 484–513. The production code is `OpenAIAttachmentUtils.scala` 310–318 and the `validateImageAttachment` calls in `OpenAIPrompt.scala`.
- **Description**:
  - Every positive Decisions path fixture uses a lowercase extension: `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`, `/photo.png` and `/photo.webp`.
  - The only uppercase test, at `OpenAIPromptMultimodalRequestSuite.scala` 169–189, calls `prepareFile` directly. It never reaches `mayBeImagePath` or `validateImageAttachment`.
  - The current code is correct, because `fileExtension` lowercases with `Locale.ROOT`.
- **Risk**: Camera exports commonly use names like `IMG_0001.JPG`. If the gate's extension handling regressed, those valid images would be rejected before IO with a misleading "only support JPEG, PNG, GIF, and WebP" error, and no test would fail.
- **Suggested Fix**: Add a local `.JPG` source and an HTTP `/photo.PnG` source to the real-image matrix. Assert success and the exact `data:image/...` `input_image` payload.

### Issue 3: Opt-in live coverage never sends a hosted image inside question instructions
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIDecisionsLiveSuite.scala
- **Line(s)**: 22–69, with question instructions at 40–44 and hosted input at 45–46.
- **Description**:
  - The PR removes the inline-only compatibility rule. That rule's comment noted that the provider guide and reference disagreed about hosted images.
  - The PR now accepts hosted URLs both in shared input and in question `instructions`, and the docs example at line 343 shows the question form.
  - The mocked `AIRecordingServer` returns a canned reply for any request body, so it proves forwarding, not provider acceptance.
  - The only provider-level check is the live `hosted=true` variant. It puts the hosted URL only in shared input and always uses the inline PNG in question instructions.
- **Risk**: Users following the documented example would get provider 400 errors if the provider rejects hosted URLs in instructions. A fully configured live run would still pass.
- **Suggested Fix**: In the `hosted=true` variant, put `image(hostedUrl.get)` in the question instructions as well, or add a variant with a hosted question image and inline input. Keep the existing `SYNAPSEML_DECISIONS_IMAGE_URL` gate and assert the typed predicate answer.

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

## Driver disposition

The three Low findings are retained as optional additional coverage and closed
without changing this patch. Their original text and `ISSUES_FOUND` verdict
remain intact. This is a bounded coverage decision, not a claim that the
suggested cases exist or that live provider acceptance has been established.

1. **Additional negative settings:** `postProcessing` and
   `postProcessingOptions` still pass through the same rejected-parameter loop
   and are absent from `decisionsSettings`. The public-stage matrix tests both
   accepted settings and rejected `systemPrompt`/`dropPrompt` branches through
   schema and transform with no HTTP. The validation mechanism is covered;
   enumerating every unchanged member of the rejected set would add defensive
   cases rather than cover a distinct new branch or fix a demonstrated defect.
   The suggestion is useful but is not required for this bounded compatibility
   change.
2. **Uppercase extension matrix:** the new pre-read gate calls the shared
   `fileExtension` helper, which lowercases with `Locale.ROOT`; the reviewer
   confirms current correctness. Existing helper/reader coverage exercises
   uppercase names, and public Decisions tests exercise the extension gate,
   formats and exact image payloads. A complete case-by-source-by-format public
   matrix is not necessary in addition to that layered coverage. No alternate
   Decisions case-folding implementation was introduced.
3. **Hosted question images in live tests:** the current live test does use
   inline question images and hosted shared input, so it is not evidence of
   hosted-question provider acceptance. That limitation is accepted explicitly.
   The driver re-read OpenAI's current Decisions guide, whose
   `include-images-in-question-instructions` section gives a concrete request
   with `https://example.com/candidate.jpg` in question instructions and states
   that public URLs or inline data URLs are accepted. Exact client forwarding
   is covered by native and public-stage recording-server tests. This is
   documented API support, not an inferred capability from the canned reply.
   No live endpoint/credential evidence is available here, and all provider E2E
   remains unverified in the PR and handoff. Adding another credential-skipped
   variant would not establish that missing deployment evidence. Retain the
   suggestion for the authorized real-provider qualification rather than
   treating it as a product defect or claiming a provider pass.

Source: https://developers.openai.com/api/docs/guides/decisions#include-images-in-question-instructions

The preceding accepted durability gaps were different: they lacked public
transform/incomplete-resume coverage for the newly reachable storage invariant.
Those are now covered and passed. Current evidence remains 290 unique
deterministic passes across four runs, with no live-provider claim. No source,
test or documentation content changed during this disposition; proceed to the
last lens on patch
`1258f6d937c24fbe8ac3daf070403741932fc986faf4c0b6034df2411b6c1a22`.