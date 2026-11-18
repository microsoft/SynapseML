---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 14
stage_attempt: 1
round: 3
timestamp: "2026-11-17T19:02:35Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "2f96b8973db1f239587d175c4fee33cec3df380182a89f9d48146d2a1d14a78f"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/task-unknown-attempt-14-stage-attempt-1-review-3-decisions-paths-opus.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Request order checked before image reads.** In `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala` lines 471–494, Decisions builds messages lazily and counts the non-blank path cells first. `AIRequestAdapter.scala` is the only adapter call site (lines 477–478). The order there is: `selectedModel` (404–409), then `requestVersion` (589–597), then `mergedOptions` (606), then question validation, the combined image count and only then `messages()` (`nativeEnvelope` 571–587). So missing model/version bindings, malformed options or questions, and the 128-image limit all fail before any image IO. Chat, Responses, System One and embeddings still prepare messages eagerly through the 3-argument overload (599–600).
- [x] **Error codes from the lazy image read.** Searching `AIRequestAdapter.scala` for try/catch finds only endpoint URI parsing (lines 39–40), so nothing in the request path reclassifies the deferred read's exceptions. `OpenAIAttachmentUtils.readAttachment` (lines 100–112) turns non-fatal read failures into `AttachmentReadException`, reported as `input_io_error`. Interruption and task kills still propagate. `AttachmentSizeException` is an `IllegalArgumentException`, reported as `invalid_request`. The catch order is in `AIPrompt.scala` 482–494.
- [x] **Prompt text.** In `AIPrompt.scala` 377–384, null or empty text gives `missing_input` with "input is null or empty". Whitespace-only text gives `invalid_request` via `NativeQuestions.validateDecisionsInput`. Both match the non-path `AIRequestAdapter.input` (524–532). `AIPromptMediaInputSuite.scala` 272–300 and 376–410 assert zero image and inference requests for these cases.
- [x] **Image-count boundary.** The pre-IO count trims the same way as `OpenAIPrompt.createMessagesForRow` (602–618). Each non-blank path yields exactly one part or throws (`wrapFileToMessagesList` 631–658), so the pre-check is exact. `NativeQuestions.prepareDecisionsRequest` and `validateDecisionsImageCount` (151–167) recheck after IO. Empty instruction arrays are rejected by `array` (27–30). Path columns must be `StringType` (`AIRequestAdapter.scala` 309–314), so the new `row.getString` count cannot throw a `ClassCastException`.
- [x] **Format guard scope.**
  - `OpenAIPrompt.validateImageAttachment` (628–629) takes a by-name predicate, so the Decisions-only checks never run for Chat or Responses.
  - In `OpenAIAttachmentUtils.scala`, `mayBeImagePath` (315–318) agrees with `categorizeFileType` (320–338). The filename pre-check therefore rejects only names the reader could never accept.
  - `dataUrlFileType` (431–442) fails with a controlled error when there is no comma.
  - The WebP check (295–308) needs 12 bytes, and RIFF/WAV files stay audio.
  - `readBoundedFile` (353–382) caps reads at the limit plus one byte and handles zero-length reads.
  - An empty image ends in the controlled error "Decisions image data must be nonempty" (`NativeQuestions.scala` 238).
- [x] **Hosted URL parsing.** I checked `NativeQuestions.hostedImageUrl` (203–213) against JDK `URI`/`URL` parsing. Correctly handled: user info containing `:` or `@`, IPv6 with and without a port, an empty port, ports that overflow `Int` (`MalformedURLException`, so rejected), opaque or relative URIs, a null scheme (no NPE) and mixed-case schemes. There is no DNS lookup or fetch. The cases covered by `NativeQuestionsSuite.scala` 102–113 and 209–239 are consistent with this. The one gap, out-of-range ports, is Issue 2.
- [x] **Durable output and shared stage.** `AIDurableInput.scala` (`inline` 84–92, `validateCell` "inline" around 145–149, path registration at 172) trims path cells the same way the reader does, requires inline data URLs, and covers images in question instructions. `OpenAIDecisions.scala` 15–22 only extends `AIPrompt`, so both public stages share every path reviewed here.
- [x] **Concurrency.** The `lazy val` is per row, `NativeQuestions` and `OpenAIAttachmentUtils` hold no state (a new regex matcher is created per call), and the Hadoop configuration is per partition. The patch adds no shared mutable state.
- [x] **Capability gating.** I checked `validateCapabilities` (247–259), `validateMessageConstruction` (274–280) and `validate()` (363–379, which validates endpoint and scalar headers at the stage level). The tests at `AIPromptMediaInputSuite.scala` around 561–581 and `AIPromptPreflightSuite.scala` around 61–95 only check that the message contains the parameter name.
- [x] **Considered and not reported** (documented or pre-existing):
  - Header validation after attachment loading is documented in `docs/Explore Algorithms/OpenAI/Unified AI prompting.md` 307–308.
  - Body checks for unpaired surrogates run after IO, but Spark `StringType` values cannot carry unpaired surrogates and the templates are validated beforehand.
  - Misnamed content that can't be sniffed falls back to the filename's MIME type. Extensionless JPEGs without JFIF/Exif markers are not recognized. Both are documented at docs 313–319.
  - An unset `fileSizeLimitMB` reads whole files (documented at 293–295).
  - The shared Hadoop HTTP reader has no connect or read timeouts. This predates the patch and the patch doesn't change it.
- [ ] Tests not executed: this lens is read-only, and pass/fail status comes from the driver's validation evidence.

## Issues

### Issue 1: The `fileSizeLimitMB` error says Decisions doesn't support the setting
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: 247–259 (also 274–277)
- **Description**:
  - Decisions now accepts `fileSizeLimitMB`, but only when `columnTypes` contains a `path` column.
  - When the setting is used without one, the generic loop throws `requirement failed: fileSizeLimitMB is not supported by apiType='decisions'`. That covers text-only `columnTypes`, an image column the user forgot to mark as `path`, or use with `messagesCol`.
  - This contradicts the new docs, which tell Decisions users to set a finite `fileSizeLimitMB` (`Unified AI prompting.md` 293–295).
  - With `messagesCol`, Chat and Responses give the accurate "fileSizeLimitMB cannot be combined with messagesCol". Decisions never reaches `validateMessageConstruction` because the loop throws first.
  - The existing tests only check that the message contains "fileSizeLimitMB", so nothing pins the wording.
- **Risk**: The most likely cause of this error is forgetting to mark the image column as `path`. The message instead says the feature is unsupported, so users may conclude Decisions can't cap image reads or can't load path images at all. They may then drop the cap and fall back to unbounded whole-file reads into executor memory.
- **Suggested Fix**:
  - When `api == "decisions"` and `p == stage.fileSizeLimitMB`, report the real precondition, e.g. `fileSizeLimitMB requires a 'path' column in columnTypes for apiType='decisions'`. Alternatively, let `validateMessageConstruction` report the case when `messagesCol` is set.
  - Pin the new message in the existing unused-settings tests.
  - Behavior and the error code stay the same.

### Issue 2: Hosted image URLs with out-of-range ports pass local validation
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/NativeQuestions.scala
- **Line(s)**: 203–213 (port check 207–210)
- **Description**:
  - The new raw-authority guard exists because `java.net.URL` accepts ports such as `+443` that no client can use. It checks only that the port is ASCII digits.
  - Neither `java.net.URI.parseServer` nor `URLStreamHandler.parseURL` checks the upper bound, so `https://example.invalid:65536/x.png` and `:99999` are accepted and forwarded.
  - No HTTP client can fetch these URLs. Only ports that overflow `Int` happen to be rejected, because `URL` parsing throws inside `Try`.
  - The tests (`NativeQuestionsSuite.scala` 102–113, 209–239) don't cover the 65535/65536 boundary.
- **Risk**: These rows pass client-side validation and fail only after a provider round trip. That costs rate limit and latency and returns a less specific error than the local `invalid_request`. It affects images in explicit message parts, JSON input and question instructions. The guard only partly enforces what it was added for: rejecting ports nobody can fetch from.
- **Suggested Fix**:
  - When a port is present, also require `port.length <= 5 && port.toInt <= 65535`, and decide explicitly whether to accept port 0.
  - Add boundary tests: `:65535` accepted; `:65536` and `:99999` rejected, for both bracketed IPv6 and IDN/underscore hosts.
  - The check still won't fetch or rewrite the URL.

### Issue 3: Using a path column as `inputCol` sends the raw path or data URL as Decisions prompt text
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIPrompt.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/ai/AIRequestAdapter.scala
- **Line(s)**: AIPrompt.scala 377–384; AIRequestAdapter.scala 297, 309–314
- **Description**:
  - In the new Decisions path branch, the prompt text is the `inputCol` cell value, taken as-is. The row's path columns are then attached as images.
  - `validateInputs` only checks that both columns are strings. Nothing rejects `setInputCol("image").setColumnTypes({"image": "path"})`, a natural way to say "this image is my input".
  - The whole cell value then goes out as `input_text` next to the loaded image:
    - For an inline data URL, the base64 payload is sent twice, once as text.
    - For a filesystem or HTTP path, the full path or URL reaches the provider. Template placeholders, by contrast, deliberately send only filenames (docs 295–296).
  - The docs say to use "another column" (302–303), but nothing enforces it.
  - Decisions rejected `columnTypes` before this PR, so the combination is newly reachable. Chat and Responses already behave this way and should stay unchanged.
- **Risk**: Request size and text-token count grow sharply; a 1 MB image becomes about 1.3 million characters of text. That can exceed provider limits, giving a per-row `http_error`, or quietly bill extra input tokens. Storage paths or URLs are exposed in prompt content, and the path text may bias answers. No local diagnostic warns about any of this.
- **Suggested Fix**:
  - For `apiType="decisions"`, reject an `inputCol` that resolves (via `AIColumnPlan.resolve`/`matches`) to a `path` key in `columnTypes`. The message should point to a separate text column or a `promptTemplate` filename placeholder.
  - Put the check in `validateInputs` so `transformSchema` fails before any IO. `validateCapabilities` runs first, so the existing systemPrompt/dropPrompt tests that use this overlap will still see their own messages.
  - Add the case for both public stages.
  - If the overlap is meant to be supported, document the duplication instead.

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

## Driver triage before collecting the remaining lenses

- **Issue 1:** repeated diagnostic suggestion. Include the small conditional
  capability-message improvement in the upcoming validation fix, with exact
  assertions, without changing which configurations are accepted.
- **Issue 2:** optional stricter network validation, not a violation of the
  current URI-syntax contract. The guard intentionally checks raw ASCII port
  syntax, not reachability or provider fetch success. The previous correctness
  lens explicitly assessed this distinction. Preserve unchanged forwarding and
  existing parser semantics; numeric transport-port range hardening is deferred.
- **Issue 3:** confirmed newly reachable configuration gap. `inputCol` currently
  supplies its value as prompt text even when the same resolved column is also
  a path. Queue a Decisions-only schema preflight rejection, including
  case-insensitive resolved-column coverage and an actionable diagnostic.
  Keep Chat/Responses behavior unchanged.

No source change has occurred during this triage. Continue collecting the
remaining lenses on the frozen snapshot, then reproduce and fix the input/path
overlap before a fresh complete review. This pass cannot qualify that future
patch and the original `ISSUES_FOUND` verdict is preserved.

## Final driver dispositions

These updates supersede the pending entries above; the original review remains
unchanged. Pass 14 is invalidated by the following fixes, not claimed clean.

- **Issue 1 - fixed.** Decisions without path columns now explains the path
  prerequisite for `fileSizeLimitMB`; with `messagesCol`, the ordinary
  construction-conflict diagnostic applies. Exact-message tests cover schema
  and transform, both public stages, and durable preflight.
- **Issue 2 - optional hardening, deferred.** The contract validates URL syntax,
  does not guarantee fetchability, and forwards hosted URLs unchanged. A
  numeric port-range policy is not needed to correct image serialization.
  The original low-priority recommendation remains recorded.
- **Issue 3 - fixed.** `AIRequestAdapter.validateDecisionsInputColumn` rejects an
  `inputCol` resolving to a path-mapped column, with separate-text/template
  guidance. Both stages, `asset`/`ASSET`, schema/transform, and ordinary/durable
  configurations reject before HTTP or output-directory creation.

Four focused tests failed before these changes, including the missing overlap
exception. The corrected 13-file patch passed 284 deterministic tests in 12
suites, root ScalaStyle/test ScalaStyle, compile/test compile, codegen, generated
wrapper parsing and six documentation examples. Three credential-gated live
tests canceled; no live-provider acceptance is claimed.
Validated patch SHA-256:
`e37ef3864d28d028a4ab89f9425fe4000fbd2e0da319fb3087d70b94145b7b19`.