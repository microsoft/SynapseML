---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 5
timestamp: "2026-11-17T19:49:12Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "b435c863e1ebfefeb546b17c2366be0e6aece0152c91f6d2e13f17f449289853"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-14-stage-attempt-1-review-5-decisions-paths-opus.md
- **Issues Found**: 7
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Production paths read:**
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 474-478 (lazy Decisions request) and 482-493 (error mapping).
  - `AIRequestAdapter.scala` lines 247-258 (capability allowlist and the path `inputMode` guard).
  - `AIRequestAdapter.scala` lines 571-623. Model, version and options run at 604-606. `nativeEnvelope` forces the messages thunk at 579 with no catch in between.
  - `NativeQuestions.scala` lines 121-245.
  - `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` lines 628-629. Its call sites at 634, 645 and 649 use the same Decisions-only gate with by-name predicates.
  - `OpenAIAttachmentUtils.scala` lines 295-318.
- [x] **Prompt text checked before IO:** `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala` lines 272-301. Exact codes and messages are asserted at 276-278 for both stages, with templates and `inputCol`, and with zero image and inference requests.
- [x] **Row metadata checked before IO:** lines 376-412 (cases at 381-386).
- [x] **Combined image limit checked before IO:**
  - Lines 503-540: two path columns, a blank second cell (519) and download counts.
  - Lines 607-631: 127 vs. 128 differential.
- [x] **Extension guard before IO and size checks:** lines 473-501.
- [x] **Post-read checks:** lines 414-471. Null or blank cells produce `missing_input` (424-425). Also covers XBM content in a `.png` file and inline TIFF/BMP/XBM (432-434).
- [x] **Positive formats (both stages, with copy, save/load and exact payloads):**
  - JPEG, PNG and GIF: lines 207-270.
  - WebP across four stages: lines 303-334. The WebP check (`OpenAIAttachmentUtils.scala` 297-301) inspects content before the name, so the `.webp` fixtures do pin the RIFF fix.
  - WAV and the 11-byte RIFF file stay audio, with exact per-API messages and HTTP request counts: lines 336-374.
- [x] **Hosted and inline URLs:**
  - `AIPromptMediaInputSuite.scala` lines 123-163: IDN, underscore and signed-query hosts; rejected data-URL variants; the exact set of forwarded URLs.
  - `NativeQuestionsSuite.scala` lines 209-245: signed and non-ASCII-digit ports, IPv6, user info, file/ftp/relative URLs and bad base64.
- [x] **Question images:**
  - `NativeQuestionsSuite.scala` lines 120-157: exact message at 135 and a no-leak assertion at 150.
  - `AIPromptMediaInputSuite.scala` lines 165-205.
  - The refactored `prepareParts` null normalization: lines 633-660.
- [x] **Question key order:**
  - `AIJson.scala` lines 198-202 build ordered maps, so `fields.updated("instructions", …)` in `NativeQuestions.scala` 158-161 keeps key positions.
  - `AIJsonSuite.scala` lines 307-342 pin this order (see Issue 7).
- [x] **Unused settings rejected:** `AIPromptPreflightSuite.scala` lines 61-93 and `AIPromptMediaInputSuite.scala` lines 561-582 assert `transformSchema` and `transform` with zero requests.
- [x] **Durable output and live tests:** `AIPromptRecoveryAttachmentSuite.scala` lines 26-106 cover inline-only durable behavior. `OpenAIDecisionsLiveSuite.scala` lines 22-69 skip without credentials.
- [x] **Chat/Responses isolation:** if the Decisions gate leaked, Chat/Responses PDF/Office path tests in the exact-patch run would fail.
- [x] **Python:** `cognitive/src/test/python/synapsemltest/services/test_AIPrompt.py` lines 722-812 are Chat/Responses-only, with no stale Decisions assertions.
- [x] **No demonstrated product defect in this lens.** All findings are Low coverage or evidence gaps. Issue 2 also flags an unpinned change in error precedence.
- [ ] Tests were not executed (read-only lens). Results rely on validation evidence supplied by the driver.
- [ ] No Python end-to-end test exists for Decisions path columns. Parameter conversion is API-agnostic and covered for Chat/Responses, so this is not raised as a finding.
- [ ] `reviews/` and sibling artifacts were not inspected, per the independence rules.

## Issues

### Issue 1: Durable preflight twin was not updated for the new Decisions setting rejections
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptRecoveryPreflightSuite.scala
- **Line(s)**: 52-91 (Decisions settings at 55-56; positive check skipped at 83)
- **Description**:
  - The non-durable twin (`AIPromptPreflightSuite.scala` lines 64-71) now runs the systemPrompt, columnTypes (path and text), fileSizeLimitMB and both dropPrompt settings against Decisions.
  - The durable twin is titled "reject … before HTTP or durable storage", but still runs only the two `inputMode` settings for Decisions.
  - This patch added `columnTypes` and `fileSizeLimitMB` to the Decisions capability allowlist (`AIRequestAdapter.scala` 248-250). With messagesCol, these conflicts are now rejected later, by `validateMessageConstruction`, and that is not tested in durable mode.
  - This suite was not part of the exact-patch run.
- **Risk**: Static ordering currently validates before any durable work. If durable preflight were reordered, a misconfigured Decisions stage could create an output root or owner without any test failing.
- **Suggested Fix**: Reuse the full settings list for Decisions in the durable twin, or share one helper between both suites. For both stages, assert the message and `!Files.exists(root.resolve("run"))`.

### Issue 2: Decisions messagesCol acceptance and error precedence changes are unpinned
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptPreflightSuite.scala
- **Line(s)**: AIPrompt.scala 474-478; AIRequestAdapter.scala 248-250, 573-579, 604-606; AIPromptPreflightSuite.scala 87-91; AIPromptRecoveryPreflightSuite.scala 83-87
- **Description**:
  - **(a) Empty columnTypes is now accepted.** Decisions with messagesCol and an explicitly empty `columnTypes` used to fail the capability check. It is now accepted, as the docs imply (line 120, "non-empty `columnTypes`"). Both preflight suites skip their empty-map positive check for Decisions, and `AIPromptMediaInputSuite.scala` 542-559 covers only templates and `inputCol`.
  - **(b) Error precedence changed.** Message preparation is now lazy for every Decisions row, including messagesCol rows, which do no IO. Before, a null messages cell failed first with `missing_input` "messages is null". Now model, version, options and question checks run first.
    - Null messages plus malformed row options now produce `invalid_request`.
    - Null messages plus null questions now report "questions is null".
    - No test pins either order, and no test asserts "messages is null".
- **Risk**: Error codes and messages for rows with several problems changed without test evidence, and the newly accepted empty map could regress silently.
- **Suggested Fix**:
  - Enable the empty-map positive check for Decisions in both preflight suites.
  - Add a messagesCol row with null messages plus a second row-level error, and assert the intended code and message.
  - If messagesCol rows should keep eager precedence, run the lazy branch only when `paths.nonEmpty`. That is a product change and needs revalidation.

### Issue 3: Path inputMode guard test checks only the schema, only the AIPrompt facade, and only a substring
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala
- **Line(s)**: 584-605 (guard: AIRequestAdapter.scala 257-258)
- **Description**:
  - The JSON-mode case asserts only `AIPrompt.transformSchema(...)` and `.contains("inputMode")`.
  - `transform` is never called, so the final `server.requests.isEmpty` check proves nothing.
  - `OpenAIDecisions` is not covered.
  - The new guidance text is not asserted. Other messages that mention `inputMode` would also satisfy the substring check.
  - The messagesCol case uses data with no `messages` column, so it passes only because capability validation runs before input resolution.
- **Risk**: A regression in runtime ordering, in the direct stage, or in the user guidance would go undetected.
- **Suggested Fix**: Loop over both stages. Assert the exact message from both `transformSchema` and `transform`, with zero requests after `transform`. Add a `messages` column to the messagesCol case.

### Issue 4: Cancellation propagation is untested for Decisions path loading, which now runs inside `adapter.request`
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptAttachmentSuite.scala
- **Line(s)**: 313, 336-357 (production: AIPrompt.scala 474-478, 483-484; AIRequestAdapter.scala 579)
- **Description**:
  - Decisions now reads attachments inside the lazy messages thunk, which `nativeEnvelope` forces.
  - The only test proving that a cancelled attachment read becomes a task-failing `CancellationException`, rather than a committed row error, covers only `Seq("chat_completions", "responses")`.
  - Static reading shows no catch in between, so current behavior is correct but unpinned for this new call path.
- **Risk**: A later catch or wrapper around the envelope would silently turn cancellations into persistent Decisions row errors.
- **Suggested Fix**: Add both Decisions stages, configured with `DecisionQuestions`, to this test. The existing `.png` fixture already passes the pre-IO extension guard.

### Issue 5: New Decisions URL and image-limit diagnostics are not pinned
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestionsSuite.scala
- **Line(s)**: NativeQuestionsSuite.scala 154, 239, 242-244; AIPromptMediaInputSuite.scala 151-154, 529, 627; NativeQuestions.scala 118, 136, 166-167, 226-240
- **Description**:
  - **Messages never asserted:** the patch rewrites the user-facing rejection to "Decisions images must use HTTP(S) URLs or inline base64 image data URLs", but no test asserts it. No test asserts "must be valid base64" or "Decisions image data must be nonempty" either.
  - **Weak assertions:** the 26-case loop (line 239) uses only `intercept`, and the media test asserts only that the error is `invalid_request`.
  - **Two limit messages not distinguished:** the input-only limit reports "at most 128 images" (136), while the combined limit reports "at most 128 images per request" (118, 167). Tests either use `contains("at most 128 images")` or bare intercepts, so they cannot tell the two apart.
  - The 128 vs. 129 differential tests do pin the limits themselves.
- **Risk**: A wrong message, or a rejection by an unintended validator, would pass silently.
- **Suggested Fix**: Assert exact messages for representative hosted-URL, header, base64 and empty-data cases, and for both count cases. Keep the assertions that no sensitive value leaks into messages.

### Issue 6: Path-column edge cases for the new guard are untested
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIPromptMediaInputSuite.scala
- **Line(s)**: 207-270, 414-501 (OpenAIAttachmentUtils.scala 306, 311, 315-318; docs/Explore Algorithms/OpenAI/Unified AI prompting.md 307-308)
- **Description**: The following cases have no test:
  - **Mixed-case extensions:** accepting `.PNG`/`.JPG` and rejecting `.PDF` before IO depend on lowercasing (311), but every fixture name is lowercase.
  - **Zero-byte or unrecognized content with a supported extension:** these fall back to name-based MIME detection (306). A zero-byte `.png` then fails at request time with "image data must be nonempty", and unrecognized bytes are forwarded as `image/png`. Neither behavior is pinned. The docs sentence "Header validation still happens…" may read as stronger than this.
  - **Invalid `fileSizeLimitMB` values (0, negative, NaN):** this patch newly allows the setting for Decisions path columns, but these values are tested only for Chat/Responses (`AIPromptRecoveryAttachmentSuite.scala` 109-178).
  - **Data URLs with parameters as path values:** forms such as `data:image/png;base64;name=a.png,…` are tested only through messagesCol and JSON input (130-133).
  - **Extensionless valid WebP:** only extensionless WAV is covered.
- **Risk**: How these edge cases are classified can drift without any test failing.
- **Suggested Fix**: Extend the existing tables with:
  - uppercase names;
  - a zero-byte `.png`, asserting exact `invalid_request` and zero inference requests;
  - invalid size limits for both Decisions stages;
  - one data URL with parameters as a path value.

  Optionally add an extensionless WebP case and clarify the docs sentence.

### Issue 7: Exact-patch validation omits suites that pin changed Decisions invariants
- **Severity**: Low
- **File**: cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/ai/AIJsonSuite.scala
- **Line(s)**: AIJsonSuite.scala 307-342; AIPromptSuite.scala 288-289; AIPromptRecoveryPreflightSuite.scala 52-91
- **Description**:
  - Every Decisions request now goes through changed code:
    - `prepareDecisionsRequest` rebuilds every question object (NativeQuestions.scala 158-161).
    - The 3-argument `request` delegates to the new 4-argument overload.
    - The capability allowlist changed.
    - System One shares `nativeEnvelope`.
  - Several suites that directly pin these invariants were not in the exact-patch run:
    - `AIJsonSuite` 307-342 pins exact question JSON order through both Decisions stages, persistence and column bindings.
    - `AIPromptSuite` 288-289 drives a Decisions adapter through the 3-argument overload.
    - The durable preflight twin (Issue 1).
  - By the driver's own statement, the broader runs predate later changes.
- **Risk**: Static analysis found no break, but there is no evidence on this exact patch that the exact native request shape for existing text-only questions, or System One behavior, is preserved.
- **Suggested Fix**: Before commit, run AIJsonSuite, AIRecoveryJsonSuite, AIPromptSuite, AIPromptRecoveryPreflightSuite, AIPromptOutputPreflightSuite and AISystemOneSuite, or the whole `services.ai` test package, and record the results.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 3
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 4
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 5
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 6
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

### Issue 7
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending driver triage
- **How verified**: Pending

## Driver triage before collecting the final lens

No product defect is demonstrated by this lens. Batch the directly relevant
coverage with the already queued preflight fix:

- Align durable and non-durable Decisions setting-conflict checks, including
  the newly accepted empty mapping and the improved file-limit diagnostic.
- Exercise the image `inputMode` conflict through schema and transform on both
  public stages, with a real messages column for the conflict case.
- Extend the existing cancellation test to the Decisions lazy attachment path.
- Run the existing native JSON/order, adapter, System One and durable preflight
  suites on the final corrected patch, rather than relying on earlier runs.

The additional malformed-row precedence, exact wording for unchanged validators,
and exhaustive path permutations are assessed as optional expansions. Existing
tests already pin the supported/rejected formats, limit boundaries, no-IO
ordering, null/blank handling, case-insensitive URL schemes, error codes, and
non-disclosure. Do not equate exact diagnostic wording everywhere with behavior
coverage, or claim broader live evidence from local mocks.

Keep the source frozen until lens 6 returns. Preserve every original finding
and add final dispositions with the corrected snapshot's validation evidence.

## Final driver dispositions

These dispositions supersede the pending entries, not the original findings
or `ISSUES_FOUND` verdict. Source changes invalidate pass 14.

- **Issue 1 - addressed.** The durable twin now runs the same complete
  construction-conflict matrix for both Decisions stages and verifies no HTTP
  requests or output-root creation. The file-limit conflict is asserted exactly.
- **Issue 2(a) - addressed.** Both preflight suites run their empty-mapping
  messages-input acceptance checks for Decisions with the native response
  fixture. **Issue 2(b) - optional coverage retained.** Multiple simultaneously
  invalid row fields do not have a promised stable error priority. The chosen
  order consistently checks metadata/questions before input preparation and is
  documented; no eager-only exception or additional precedence contract is added.
- **Issue 3 - addressed.** Both public stages run schema and transform checks.
  Messages-conflict data now contains a real messages column, and assertions
  identify the intended input-mode and nonempty-mapping diagnostics.
- **Issue 4 - addressed at the shared implementation boundary.** The existing
  attachment-cancellation test now includes Decisions, verifies task failure,
  sanitized cancellation and no inference request. `OpenAIDecisions` inherits
  that request path unchanged; its ordinary path behavior is separately tested.
- **Issue 5 - optional assertion expansion, deferred.** The tests already pin
  accepted/rejected URL values, no sensitive-data disclosure, error codes, exact
  forwarding and the count boundary. Exact wording for every rejection is not
  a separate API contract. Changed file-limit diagnostics are pinned explicitly.
- **Issue 6 - optional permutation expansion, deferred.** Existing cases cover
  supported/rejected formats, content-vs-name mismatch, IO/count/size boundaries,
  null/blank paths and malformed data URLs. Invalid numeric limits still use
  the unconditional `validateOptions` check before the API capability gate.
  Documentation now distinguishes HTTP headers and rejects all non-image
  extensions explicitly. No stronger content-validation claim is made.
- **Issue 7 - addressed.** The exact corrected patch passed AIJsonSuite (46),
  AIRecoveryJsonSuite (2), AIPromptSuite (40), AIPromptRecoveryPreflightSuite (18),
  AIPromptOutputPreflightSuite (26) and AISystemOneSuite (12), in addition to the
  six media/attachment/native suites (140 tests).

Total: 284 deterministic passes, zero failures; root style/build/codegen gates,
generated-wrapper parsing and six documentation examples also pass. Three live
tests canceled at credential/configuration gates and are not provider evidence.
Validated patch SHA-256:
`e37ef3864d28d028a4ab89f9425fe4000fbd2e0da319fb3087d70b94145b7b19`.