# Task 2700 - Attempt 1 - Review 3

| Field | Value |
| --- | --- |
| Round | 3 of 6 (sequential) |
| Theme | Edge cases and robustness |
| Reviewer role | Reliability engineer |
| Model | claude-opus-5 |
| Mode | Read-only; no edits, builds, Spark jobs, or external posts |
| Scope | `feat/openai-schema-convenience-2700` worktree, base `master` @ `dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24` |
| Issue count | 3 |
| Status | **ISSUES_FOUND** |

## Evidence checklist

| # | Check | Result |
| --- | --- | --- |
| 1 | Null / empty schema rejected before any state change | **Pass** - `HasOpenAIResponseSchema.scala:27` `require(schema != null && schema.nonEmpty, ...)`; Python guard at `HasOpenAIResponseSchema.scala:57-60`. Covered by `OpenAIResponseSchemaSuite.scala:118-127` and `test_OpenAIResponseSchema.py:123-140`. |
| 2 | Name boundary values (`""`, 1, 64, 65 chars, spaces, `null`) | **Pass** - regex `[A-Za-z0-9_-]{1,64}` at `HasOpenAIResponseSchema.scala:28`; anchored by `String.matches`, so embedded newlines cannot pass. Boundaries exercised at `OpenAIResponseSchemaSuite.scala:109-112` and `test_OpenAIResponseSchema.py:142-158`. |
| 3 | Failed setter leaves prior `responseFormat` untouched (state mutation atomicity) | **Pass** - `require` precedes `setResponseFormat`; Python assigns `self._java_obj` only after the JVM call returns (`HasOpenAIResponseSchema.scala:74`). Asserted in both suites. |
| 4 | No new `responseSchema` `Param`; serialized parameter shape unchanged | **Pass** - setter delegates to the existing `responseFormat` `ServiceParam`; `assert(!chat.hasParam("responseSchema"))` (`OpenAIResponseSchemaSuite.scala:146`) and `assertFalse(stage.hasParam("responseSchema"))` (`test_OpenAIResponseSchema.py:57`). |
| 5 | Chat vs Responses envelope divergence via existing normalization | **Pass** - `ResponseFormatUtils.normalizeJsonSchema` flat branch (`ResponseFormatUtils.scala:56-63`) feeds `OpenAIChatCompletion.scala:35-51` (nested `json_schema`) and `OpenAIResponses.scala:52-56` (`text.format`). Wire-level assertions at `OpenAIResponseSchemaSuite.scala:159-216`. |
| 6 | `OpenAIPrompt` routing re-normalization is idempotent | **Pass** - `OpenAIPrompt` inherits `HasOpenAITextParamsExtended` (`OpenAIPrompt.scala:36`), stores the Chat shape, and `configureService` (`OpenAIPrompt.scala:331-337`) re-normalizes through `normalizeJsonSchema`'s nested branch (`ResponseFormatUtils.scala:64-71`) for both targets. |
| 7 | Overload resolution (`(Map)`, `(Map, String)`, `(Map, Boolean)`, `(Map, String, Boolean)`) | **Pass** - the two 2-arg overloads have distinct erasures; `setResponseSchema(schema, null)` is unambiguous because `Null` conforms only to `String`. |
| 8 | Generated Python prerequisites (`SparkContext` import, stub filtering) | **Pass** - `from pyspark import SparkContext` is emitted by `Wrappable.scala:506`; `pyStubAdditionalMethods` skips `_`-prefixed names (`Wrappable.scala:425`), so the nested `_convert` cannot reach the `.pyi`. Stub arg typing resolves the unique 3-arg overload (`Wrappable.scala:400-406`). |
| 9 | Python setter does not mutate the caller's dict | **Pass** - `_convert` builds new Java collections; asserted at `test_OpenAIResponseSchema.py:48,59`. |
| 10 | Driver-side only; no per-row/executor state | **Pass** - setter touches only the driver `ParamMap`. |
| 11 | Test resource cleanup | **Pass** - HTTP server stopped and temp dir deleted in `finally` (`OpenAIResponseSchemaSuite.scala:84-86,151-156`); Python uses `TemporaryDirectory` and restores the conf key in `finally` (`test_OpenAIResponseSchema.py:167-181`). |
| 12 | Property-order preservation through the serializer | **FAIL** - see Issue 1. |
| 13 | JSON `null` values inside a user schema | **FAIL** - see Issue 2. |
| 14 | Integer schema values outside `Int` range | **FAIL** - see Issue 3. |

### Out-of-band verification performed

Read-only JVM inspection (no project build) against the exact pinned dependency versions resolved by `build.sbt` (`scalaVersion := "2.12.17"`, spray-json `1.3.5`), driving `spray.json.DefaultJsonProtocol.mapFormat` with an insertion-ordered `scala.collection.immutable.ListMap`:

```text
n=2 in=[e_one,d_two]                                 json={"e_one":..,"d_two":..}
n=3 in=[e_one,d_two,c_three]                         json={"e_one":..,"d_two":..,"c_three":..}
n=4 in=[e_one,d_two,c_three,b_four]                  json={"e_one":..,"d_two":..,"c_three":..,"b_four":..}
n=5 in=[e_one,d_two,c_three,b_four,a_five]           json={"a_five":..,"e_one":..,"d_two":..,"b_four":..,"c_three":..}
n=6 in=[e_one,d_two,c_three,b_four,a_five,z_six]     json={"a_five":..,"e_one":..,"d_two":..,"b_four":..,"c_three":..,"z_six":..}
```

Separately confirmed that `ListMap.toMap` on Scala 2.12.17 is a no-op (`scala.collection.immutable.ListMap$Node` in, same out), so the order loss is attributable to spray-json's `mapFormat`, not to `.toMap`.

## Issues

### Issue 1: Inner-schema key order is silently lost for any JSON object with 5 or more keys

**File:** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:27-41`
(reached from `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala:30-35`)
**Severity:** High

**Problem**
`setResponseSchema` promises to preserve the user's inner schema, and the order-preservation machinery it relies on (`ServiceParam.toScalaAny` / `ServiceParam.toMap` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/JsonEncodableParam.scala:56-82`, plus the `ListMap` branch in `UntypedArrayParam.scala:30-37`) is explicitly commented as existing for this purpose. The `ListMap` survives all the way to the final step, but the final step discards it:

```scala
// UntypedArrayParam.scala:36-37
// Use spray-json mapFormat via toMap to serialize while preserving order in the underlying fields
ordered.toMap.toJson
```

`toJson` resolves to spray-json's `CollectionFormats.mapFormat`, whose `write` is `JsObject(m.map { ... })` with static receiver type `Map[K, V]`. On Scala 2.12 that selects `immutable.Map.canBuildFrom`, which builds `Map1..Map4` and then switches to `HashMap`. At 5 or more keys the result is hash-ordered, and `CompactPrinter` emits `JsObject.fields` in that order.

Consequences for the new setter:
- A schema with 5+ properties (very common) has its `properties` emitted in arbitrary order, so `strict` structured output is generated in an order the caller did not request. This is exactly the guarantee `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/ResponseFormatOrderSuite.scala:51-113` was written to protect (`reason` before `ans`); that protection evaporates once the object exceeds four keys.
- The same `mapFormat` path is used by `ServiceParam.jsonEncode`, so saved models are persisted in scrambled order too.
- `ordered.toMap` is a no-op on 2.12, so the `ListMap` plumbing in `JsonEncodableParam.scala` and `UntypedArrayParam.scala` provides no benefit at the sizes where it is needed.

**Evidence**
- Empirical run above: order is byte-for-byte preserved at n <= 4 and scrambled at n = 5 and n = 6, using `scala-library 2.12.17` and `spray-json 1.3.5`, the versions pinned by `build.sbt:34` and the project's dependency set.
- No existing test can observe this. `ResponseFormatOrderSuite.scala:46-49` uses exactly two properties inside a four-key schema. `OpenAIResponseSchemaSuite.scala:29-36` uses a four-key schema with two properties, and `assertSchema` (`OpenAIResponseSchemaSuite.scala:45-49`) compares `JsObject`s, whose equality is `Map` equality and therefore order-insensitive.
- The new Python test named for this guarantee does not cover it either: `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py:94-121` grows the inner schema to six top-level keys but only asserts `assertEqual(actual, self.schema)` at line 114 (dict equality, order-insensitive); the two ordered `list(...)` assertions at lines 115-120 inspect objects of three and two keys, which are below the threshold. The test therefore passes while the six-key object it just constructed is emitted out of order.

**Suggested fix**
Serialize objects through an order-preserving `JsObject` rather than spray-json's `mapFormat`. Build the field map with `ListMap` explicitly and pass it to `JsObject`'s primary constructor instead of calling `.toJson` on a `Map`. Then extend `ResponseFormatOrderSuite` (and the Python order test) with a schema of at least five properties and at least five sibling keywords, asserting emitted order, so the regression is observable. If order preservation is not intended to hold in general, the claim must be removed from the comments at `UntypedArrayParam.scala:30,36` and `JsonEncodableParam.scala:56-57` and the limitation documented on `setResponseSchema`.

**Scope note:** the defective serializer predates this change and is shared with `setResponseFormat`. It is reported here because `setResponseSchema` is the API that advertises "the schema itself is not modified" (`HasOpenAIResponseSchema.scala:47`) and because the requirement under review states that inner-schema property order must be preserved.

### Issue 2: A JSON `null` anywhere in the schema fails late with a `NullPointerException` instead of a diagnosable error

**File:** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:17,42`
**Severity:** Medium

**Problem**
`null` is a legal JSON Schema value (`"default": null`, `"const": null`, `"enum": [null, "a"]`). The Python setter accepts it: `_convert` returns non-dict/non-list values unchanged (`HasOpenAIResponseSchema.scala:71`), py4j stores a Java `null` in the `LinkedHashMap`, and `ServiceParam.toScalaAny`/`toScalaPrimitive` pass it through (`JsonEncodableParam.scala:58-76`, final `case other => other`). The driver-side `require` at `HasOpenAIResponseSchema.scala:27` only checks the top-level map, so configuration succeeds.

Serialization then reaches:

```scala
// UntypedArrayParam.scala:17
def throwFailure(any: Any) = throw new IllegalArgumentException(s"Cannot serialize ${any} of type ${any.getClass}")
...
// UntypedArrayParam.scala:42
case _ => throwFailure(any)
```

`null` matches none of the preceding type tests (a Scala type test on `Any` compiles to `instanceof`, which is false for `null`), so it falls to `case _`, and `any.getClass` dereferences `null`. The user gets a bare `NullPointerException` raised from inside a string interpolation, not the intended `IllegalArgumentException`, and it surfaces during `transform` (executor task failure) or `save`, far from the `setResponseSchema` call that introduced it.

**Evidence**
Determined by reading the match arms at `UntypedArrayParam.scala:21-42`: no arm matches `null`, and the `try`/`catch` at lines 28-40 guards only the `Map` branch. `case v: Integer` at line 25 is already unreachable (`case v: Int` at line 21 matches boxed `java.lang.Integer`), confirming these arms are type-test based. The `toScalaPrimitive` chain at `JsonEncodableParam.scala:67-76` has no null handling and returns `null` unchanged.

**Suggested fix**
Add an explicit `case null => JsNull` to `anyFormat.write` (JSON `null` is representable and should round-trip), and make `throwFailure` null-safe so any remaining unsupported value reports a usable message. Add a test that a schema containing `"default": null` or `"enum": [null, ...]` serializes to a payload containing `null`.

### Issue 3: Integer schema values outside `Int` range are accepted at configuration time and rejected only at request/save time

**File:** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:21-42`
**Severity:** Medium

**Problem**
Numeric JSON Schema keywords routinely exceed 32 bits (`"maximum": 2147483648`, `"maxLength": 3000000000`, millisecond-epoch bounds). py4j encodes a Python `int` outside Java `int` range as `java.lang.Long`, and `ServiceParam.toScalaPrimitive` explicitly handles that case (`core/src/main/scala/com/microsoft/azure/synapse/ml/param/JsonEncodableParam.scala:69`: `case l: java.lang.Long => l.longValue()`), so the conversion layer clearly expects `Long` values to arrive.

The serializer has no `Long` arm. `anyFormat.write` handles `Int`, `Double`, `String`, `Boolean`, `Integer`, `Seq`, and `Map` (`UntypedArrayParam.scala:21-27`); a boxed `Long` matches none and falls to `case _ => throwFailure(any)` at line 42, producing `IllegalArgumentException: Cannot serialize 3000000000 of type java.lang.Long`.

The same asymmetry applies to `Float` and `Short`/`Byte`, which `toScalaPrimitive` converts at `JsonEncodableParam.scala:70-72` but `anyFormat` cannot write. `Long` is the one reachable from ordinary Python input.

As with Issue 2, `setResponseSchema` returns successfully and the failure appears later, inside a Spark task or during `save`.

**Evidence**
Match-arm analysis of `UntypedArrayParam.scala:21-42` (no `Long`/`Float`/`Short`/`Byte` arm, and the `try`/`catch` at lines 28-40 covers only the `Map` branch), cross-referenced against the conversions the same pipeline performs at `JsonEncodableParam.scala:67-76`. The reader side already handles wide integers (`UntypedArrayParam.scala:46-52` produces `Long` for values that are `isValidLong`), so a saved-then-loaded schema can also hold a `Long` that the writer cannot re-emit.

**Suggested fix**
Add `Long`, `Float`, `Short`, and `Byte` arms to `anyFormat.write` so the writer accepts everything `toScalaPrimitive` and `anyFormat.read` produce, and cover it with a test using a numeric keyword above `Int.MaxValue`. Alternatively, validate supported value types eagerly in `setResponseSchema` so the error is raised at the driver-side configuration call.

## Resolution

All three issues share one root: the shared JSON writer `AnyJsonFormat.anyFormat` in `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` is narrower and less order-preserving than the conversion layer (`ServiceParam.toScalaAny`/`toMap`) and the reader (`anyFormat.read`) that surround it. `setResponseSchema` does not introduce the defect, but it is the first API whose contract is "pass your inner JSON Schema through unchanged," so it widens the input domain that reaches the writer and makes the gaps user-visible.

Recommended order of work:

1. **Issue 1 (High)** - make object serialization order-preserving by constructing `JsObject` from an insertion-ordered field map instead of delegating to spray-json's `mapFormat`. Verify with the empirical harness described above (the emitted JSON must match insertion order at n = 5 and n = 6).
2. **Issues 2 and 3 (Medium)** - close the writer's type gaps (`null`, `Long`, `Float`, `Short`, `Byte`) and make `throwFailure` null-safe, so unsupported values produce an actionable message rather than an NPE.
3. **Test coverage** - the guarantee tests currently cannot fail:
   - `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/ResponseFormatOrderSuite.scala` - raise the property count above four.
   - `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py:94-121` - add an ordered assertion over the six-key inner schema the test already builds (compare `list(actual)` against `list(self.schema)`), not only `assertEqual` on dicts.
   - Add negative/boundary cases for `null` and for an integer above `Int.MaxValue`.

If the maintainers decide any of these are out of scope for this PR, Issue 1 still requires a documentation change: the order-preservation claims in `UntypedArrayParam.scala:30,36` and `JsonEncodableParam.scala:56-57`, and the guarantee implied by `setResponseSchema`'s docstring at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala:44-48` and by `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`, must be qualified rather than left stated-but-false.

### Items explicitly checked and found sound

Validation atomicity, name boundary values, empty/null schema rejection, absence of a new `Param`, Chat/Responses envelope divergence, `OpenAIPrompt` re-normalization idempotence, Scala overload resolution including the strict-only overload, generated-Python prerequisites (`SparkContext` import and `_convert` stub filtering), non-mutation of the caller's dict, driver-side-only state, and test resource cleanup. No concurrency defect was found: the setter mutates only driver-side `ParamMap` state, and `OpenAIPrompt` configures a freshly constructed service instance per call (`OpenAIPrompt.scala:670-694`).

### Verified resolution

All three findings are **fixed** in `UntypedArrayParam.scala`.

- Issue 1: construct `JsObject` directly from a `ListMap` of recursively encoded
  fields. This avoids `mapFormat` rebuilding larger objects as unordered maps.
  Explicit String-key matching replaces the unchecked cast and blanket catch.
- Issue 2: handle JSON null in both directions and keep unsupported-value
  messages null-safe. Nested null values now survive request serialization and
  model persistence.
- Issue 3: support the numeric primitives already produced by Java conversion,
  including signed 64-bit integers. Arbitrary-precision support was deliberately
  excluded after round 4 exposed separate metadata limitations.
- Before the fix, four new `VerifyUntypedArrayParam` regressions failed for
  five-key ordering, null serialization, Long serialization, and mixed-value
  persistence. Afterward, all 48 tests across the five selected core/cognitive
  suites passed, with no failures, errors, or skips.
- Outgoing tests now compare raw key order for 5/6-field schemas on both existing
  setters and all four loopback Spark transform routes. The final generated-Python
  run passed 18 selected tests and 77 subtests, including deep ordering and
  copy/save/load with nulls and signed 64-bit bounds.
- Core/cognitive main and test style checks, compilation, and code generation
  passed. The shared numeric reader and Spark metadata parsing remain unchanged
  except for handling JSON null; no model-reload key-order guarantee was added.
