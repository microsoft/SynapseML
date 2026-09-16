## Review summary

- **Round:** 1
- **Theme:** Broad correctness, security, and spec-conformance sweep
- **Mode:** Sequential
- **Actual model:** GPT-6 Astra, `gpt-6-astra`
- **Artifact:** `reviews/pr-2708/pr-2708-attempt-1-review-1-gpt-6-astra.md`
- **Issue count:** 2
- **Verdict:** **ISSUES_FOUND**

## Issues

### Issue 1: Scala cannot override strictness while retaining the default name

**File:** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala:13-23`

**Severity:** Medium

**Problem:** The agreed API makes `name` and `strict` independently optional, but the Scala overloads only support `(schema)`, `(schema, name)`, and `(schema, name, strict)`. Consequently, this expected call has no matching overload on any of the three Scala stages:

```scala
new OpenAIChatCompletion().setResponseSchema(schema, strict = false)
```

Users requesting non-strict output must repeat `"response_schema"` explicitly, despite the promised default name. The generated Python method supports this combination.

**Evidence:** Read all three declarations and inspected the existing compiled trait and stage classes with `javap`. They expose neither a `(Map, Boolean)` overload nor default-argument getters that could supply the omitted name. No compilation was started during this review.

**Suggested fix:** Add a `(schema: Map[String, Any], strict: Boolean)` overload that delegates with `"response_schema"`, and cover the named `strict = false` call on all three Scala stages.

### Issue 2: The nested converter becomes a nonexistent public method in generated Python stubs

**File:** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala:57-70`

**Severity:** Medium

**Problem:** The local function `convert` is emitted inside `setResponseSchema`, but the stub generator treats every non-underscore-prefixed function declaration in `pyAdditionalMethods` as a public class method. It does not distinguish nested functions.

As a result, all three generated `.pyi` files advertise:

```python
def convert(value: Any) -> Any: ...
```

No corresponding class method exists in the generated runtime wrappers. The shipped type information therefore advertises a callable API that fails with `AttributeError` at runtime.

**Evidence:** Traced the extraction logic in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala:423-440`. AST comparisons of the generated wrappers and stubs identified `convert` as the sole stub-only method in all three classes. The declarations appear at:

- `cognitive/target/scala-2.12/generated/src/python/synapse/ml/services/openai/OpenAIChatCompletion.pyi:322`
- `cognitive/target/scala-2.12/generated/src/python/synapse/ml/services/openai/OpenAIResponses.pyi:340`
- `cognitive/target/scala-2.12/generated/src/python/synapse/ml/services/openai/OpenAIPrompt.pyi:400`

**Suggested fix:** Rename the nested helper to `_convert` and update its recursive calls. The existing stub generator excludes underscore-prefixed helpers. Add a generated-API assertion preventing this extra public declaration. Fix the source template, not files under `target/`.

## Evidence checklist

- [x] Confirmed the requested branch and supplied master base. Reviewed all three tracked changes and all three new files.
- [x] Traced the new setter through `ResponseFormatUtils.normalize`, Chat Completions' `response_format.json_schema`, Responses' `text.format`, and `OpenAIPrompt.configureService` for both API modes.
- [x] Reviewed validation before parameter mutation, inner-schema metadata preservation, unchanged legacy strictness, and reuse of the existing `responseFormat` parameter.
- [x] Followed ordered Python dictionary/list conversion through `ServiceParam.toMap` and the existing JSON encoder.
- [x] Checked `OpenAIPrompt.pyAdditionalMethods` composition, `Wrappable` generation, and the actual generated `.py` and `.pyi` artifacts.
- [x] Read refreshed Scala test reports: **9 schema-convenience tests, 8 core offline tests, and 6 response-format tests passed; zero failures, errors, or skips**. These were existing-run results, not reviewer-launched tests.
- [x] Read both cognitive Scala style reports; each recorded zero errors.
- [x] Parsed the new Python test source and notebook example without executing them.
- [x] Reviewed the changed input-to-JSON paths for security issues. No actionable security issue was identified in the changes.
- [ ] Python behavioral test execution was not independently verified. No builds, codegen runs, Spark jobs, cloud calls, commits, external posts, or file writes were initiated by this review.

## Resolution log

Issues 1 and 2 remain **open**. No fixes were implemented.

### Verified resolution after review

Both findings are **fixed**. The original review above describes the pre-fix state.

- Issue 1: added the `(schema: Map[String, Any], strict: Boolean)` overload to
  `HasOpenAIResponseSchema.scala`. The new named-argument regression compiles and
  runs on Chat Completions, Responses, and OpenAIPrompt without repeating the
  default name.
- Issue 2: renamed the nested converter `_convert` in the Scala source template,
  regenerated the wrappers and stubs, and added a Python AST/runtime API-parity
  test for all three stages. That test first reproduced the nonexistent `convert`
  method in all three old stubs and then passed after regeneration.
- Verification: 24 targeted Scala tests passed, along with cognitive main/test
  style and code generation. The generated-Python run passed 17 selected tests
  and 59 subtests, with no failures, errors, or skips. JVM and Python provenance
  pointed to this worktree's built classes and generated wrappers.
- The Python persistence fixture opts into legacy loading only around the
  trusted temporary model it creates and restores the previous setting in
  `finally`. A separate baseline using existing `setResponseFormat` reproduced
  the same restriction for Chat and Responses. The run also verified that the
  default deserialization policy was restored afterward.
