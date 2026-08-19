# Round 1 — Broad sweep — GPT-5.6 Sol

## Finding 1 — High — Malformed message schemas can abort the Spark job

**File:** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`

Validation does not fully protect the required `role` field and unsupported
declared/runtime `content` shapes. Invalid rows can reach request serialization,
where role extraction or content encoding throws inside the Spark request UDF.

**Required resolution:** Validate role presence/type and content shape before
request creation. Route failures to `errorCol`, skip HTTP, and add public
transform regressions proving the Spark job remains alive.

**Status:** Resolved.

**Resolution (what / why / how / tests):**

- *What.* Added `validateRole` and a declared-content-type branch to
  `validateContent` in the `OpenAIChatCompletion` validation object so every
  message is checked for a present, `String` `role` and a serializable content
  shape *before* `encodeMessagesToMap` runs.
- *Why.* Pre-change validation never inspected `role`, and `validateContent`
  only looked at the runtime value — so a missing/null/non-string role, or an
  unsupported declared content type even with a `null` value, slipped through and
  threw inside the request UDF (`row.getAs[String]("role")` / the `case other =>
  throw` in the shared encoder), aborting the whole Spark job.
- *How.* `validateMessage = validateRole(...).orElse(validateContent(...))`.
  `validateRole` flags missing role field / null role / non-string role (the
  role *value* is deliberately not restricted — the service owns role names).
  `validateContent` branches on `message.schema.fields(i).dataType`: `StringType`
  → ok, `ArrayType` → per-part validation, anything else → "unsupported content
  type" (covers the null-valued case). All validation stays wrapped in
  `try/catch NonFatal` → any drift becomes a row-local error, never a throw.
  `transformSchema` additionally calls `validateMessagesSchema`, which
  `require`s a `String` role when the messages element struct declares one, so a
  deterministically-incompatible schema fails fast; per-row problems remain
  row-local (transform never invokes it, so no data row aborts the job).
- *Tests.* Unit (`OpenAICoreOfflineSuite` "classifies malformed and null-safe
  content"): missing-role / null-role / non-string-role and null-valued
  unsupported-content assertions. Handler-backed public transform
  (`OpenAIChatCompletionSuite`): the malformed matrix routes a null-role row to
  `errorCol` with zero HTTP; "transformSchema rejects colliding column names and
  a non-string role" proves both fail-fast schema rejection and per-row skip;
  the AIFoundry test proves the inherited path behaves identically.
  **RED proof:** neutering the role check made the "missing a role field"
  assertion fail (`OpenAICoreOfflineSuite.scala:385`).

## Finding 2 — Medium — Null struct text can emit an invalid multimodal payload

**File:** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala`

A struct-backed `type="text"` part with null/missing `text` can pass validation.
With an image sibling it serializes as `{"type":"text"}`, which is not a valid
Chat text content part.

**Required resolution:** Require the text field to be present and string-typed
for text parts while continuing to permit empty and whitespace strings. Add
map- and struct-backed public-path regressions.

**Status:** Resolved.

**Resolution (what / why / how / tests):**

- *What.* Added `validateTextField`, invoked from `validatePartFields` for
  `type="text"` parts (both map- and struct-backed), so a text part must carry a
  present `String` `text`.
- *Why.* A present-null or absent `text` previously passed; beside an image it
  serialized as a bare `{"type":"text"}` (invalid Chat content part), and a
  map-backed present-null `text` could NPE at `serializeChatContent`. Empty and
  whitespace strings must stay valid (wave-2 backward compat — `OpenAIPrompt`
  injects a system text part and `setSystemPrompt("")` must keep the legacy
  empty-string path).
- *How.* `validateTextField`: `None` → "missing a text value"; `Some(null)` →
  "null text value"; `Some(_: String)` → ok (allows `""`/whitespace);
  `Some(other)` → "non-string text value". Struct field presence is read with
  `rowFieldPresent` (distinguishes present-null from absent). Map/struct
  semantics are intentionally identical.
- *Tests.* Unit: struct- and map-backed absent/null/non-string text assertions,
  plus explicit empty/whitespace "not flagged" assertions, and an image-part
  present-null-text "ignored" assertion. Handler-backed public transform: the
  malformed matrix covers struct null-text text-only and struct null-text beside
  an image, both → `errorCol`, zero HTTP. **RED proof:** neutering
  `validateTextField` made the "null text value" assertion fail
  (`OpenAICoreOfflineSuite.scala:428`).

## Notes

- Scope held to the 4 SF-001 files; only `OpenAIChatCompletion.scala` +
  the two SF-001 test suites were touched this round (`OpenAI.scala` untouched to
  preserve Responses non-regression).
- Also fixed a pre-existing test-only flake surfaced while consolidating: the
  "typed nested image_url" test read `requestBodies(token)` before any Spark
  action executed the transform, so the handler had not yet run. It now collects
  once up front (forcing the HTTP call) before inspecting captures.
- Validation evidence: scalastyle (main + test) 0 errors; `cognitive/compile`
  and `cognitive/test:compile` clean; `OpenAICoreOfflineSuite` 20/20;
  `OpenAIChatCompletionSuite -- -z offline` 7/7. JDK11 / Scala 2.12 / Spark 3.5.
  Scala 2.13 / Spark 4.1 replay and codegen/generated-Python checks deferred to
  validation (not claimed here).

## Post-fix rerun

After final black-box verification tightened null content and non-string image
URL validation, Round 1 was regenerated and rerun against the exact current
diff. The reviewer reported zero issues and a **CLEAN** verdict.
