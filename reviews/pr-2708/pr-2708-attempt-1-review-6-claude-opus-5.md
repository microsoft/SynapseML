# SynapseML #2700 - Review 6: Polish and Hardening

| Field | Value |
| --- | --- |
| Model | claude-opus-5 |
| Round | 6 of 6 (final source review before commit) |
| Theme | Performance, observability, documentation accuracy, naming clarity, compatibility, dead code, unnecessary scope |
| Mode | Read-only. No builds, no Spark execution, no edits, no commits, no external posts |
| Branch / base | `feat/openai-schema-convenience-2700` / `master` @ `dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24` |
| Scope | The nine source/doc files under review, plus read-only context reads of `ResponseFormatUtils.scala`, `JsonEncodableParam.scala`, `Wrappable.scala`, `OpenAIPrompt.scala`, `DatabricksUtilities.scala` |
| Issues | 1 (Medium) |
| Verdict | **ISSUES_FOUND** |

## Issue 1: Scala `Map` silently loses property order at five or more keys, and the new docs present it as equivalent to a Python dict

**File:** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala:24-25`
**Also:** `docs/Explore Algorithms/OpenAI/OpenAI.ipynb`, the two new cells "Schema-only structured output" and "Scala uses the same schema-only API"
**Severity:** Medium
**Category:** Documentation accuracy

**Problem**
Both new doc surfaces claim schema fidelity and present the Scala and Python inputs as interchangeable. The Scaladoc says the schema "is preserved"; the notebook says "The helper does not modify the JSON Schema or infer missing fields. Pass a Python dictionary or Scala `Map[String, Any]`". The Python path preserves key order at any size; the Scala path does not, once a JSON object literal exceeds four keys. That limit is invisible to the reader and unrecoverable inside the library.

**Evidence**
1. `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:34-40` emits `JsObject(ListMap(fields: _*))`, so wire order is exactly `v.toSeq` order of the map handed in. The in-repo comment at line 39 states the failure mode: "mapFormat rebuilds larger objects as HashMap, losing schema property order."
2. `build.sbt:34` pins Scala 2.12.17. There, `scala.collection.immutable.Map.apply` yields `Map1` through `Map4` up to four pairs and a `HashMap` at five or more; `HashMap.toSeq` is hash-trie order, not insertion order. Order is therefore destroyed in user code before any reviewed code runs, so no downstream fix is possible, only documentation.
3. Every order-sensitive test in this change avoids a plain `Map` and uses `ListMap`: `core/src/test/scala/com/microsoft/azure/synapse/ml/param/VerifyUntypedArrayParam.scala:69,73`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIResponseSchemaSuite.scala:30,34`; `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/ResponseFormatOrderSuite.scala:145,147`. No test exercises a plain Scala `Map` with five or more keys, so the constraint is both untested and unstated.
4. The Python path has no equivalent limit: `core/src/main/scala/com/microsoft/azure/synapse/ml/param/JsonEncodableParam.scala:59-61,78-80` rebuilds every nested `java.util.Map` as a `ListMap`, which is why `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIResponseSchema.py` can assert six-key nested ordering on all three stages. The two documented inputs genuinely differ.
5. The notebook's Scala sample happens to sit inside the safe boundary, four top-level keys and a single property, so it works and warns no one. A reader extending it to five properties gets scrambled `properties` order with no error, which is precisely the reason-before-answer ordering scenario `ResponseFormatOrderSuite` was written to protect.

**Suggested fix**
State the constraint where Scala users will read it: in the `setResponseSchema` Scaladoc and in the notebook's Scala cell, note that property order is preserved only if the map preserves it, and that `scala.collection.immutable.ListMap` is required beyond four keys. No code change is warranted.

## Evidence checklist

| Check | Result |
| --- | --- |
| Diff basis established | Confirmed against `dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24`; six modified files plus three untracked new files. Reviewed the real diff, not file snapshots alone |
| Compatibility - write surface | New `AnyJsonFormat.write` cases (`null`, `Long`, `Short`, `Byte`, `Float`) are a strict superset of the base version. The removed `case v: Integer` was unreachable behind `case v: Int` and produced identical JSON. No previously serializable value is now rejected |
| Compatibility - read surface | The `read` numeric branches and the `case _ => num` BigDecimal fallback are unchanged from base. The base writer also rejected arbitrary precision, so withdrawing it introduces no regression and no new asymmetry |
| Compatibility - blanket catch removal | Replacing `catch { case _: Throwable => ... }` preserves the `IllegalArgumentException` contract for non-string keys and unsupported values while no longer swallowing fatal errors |
| Compatibility - serialized shape | `responseFormat` remains the only parameter; `setResponseSchema` adds none. Asserted in `OpenAIResponseSchemaSuite.scala` and `test_OpenAIResponseSchema.py` |
| Compatibility - Python param transfer | Java-side-only mutation via `self._java_obj` is safe: PySpark's `_transfer_params_to_java` sets only Python-set params and defaults, and never clears Java-set ones. Matches the existing `OpenAIPromptPythonOverrides` precedent. Covered by the `copy` and save/load tests |
| Compatibility - `Wrappable` introduction | `HasOpenAIResponseSchema extends Wrappable` newly adds `Wrappable` to both text-param traits. All consuming stages were already `Wrappable`, and linearization keeps it base-most, so no existing `pyAdditionalMethods` override is displaced. Chaining through `super` is intact |
| Envelope correctness | `setResponseSchema` routes through `ResponseFormatUtils.normalize` into the flat `json_schema` branch, staying within four keys at each level, so envelope order survives independently of map size. Chat, Responses, and both `OpenAIPrompt` forwarding paths verified |
| Performance | Neutral to improved. The base already built a `ListMap` in the same place and then discarded it via `.toMap`; the new writer removes that round trip. Remaining `ListMap` build cost is bounded by documented structured-output schema limits. Setter-time validation serializes once and is not on the per-row path |
| Observability | No logging regression. `logClass` and `fit`/`transform` logging are untouched; the repo does not log setters |
| Naming clarity | `setResponseSchema` overloads are unambiguous under Scala overload resolution, including the `(Map, null)` case used in tests. Python keyword names match the Scala parameter names and defaults |
| Dead code | No unused imports or unreachable branches introduced. All three convenience overloads are exercised by tests. `_convert` is correctly underscore-prefixed and is filtered out of `.pyi` generation by `Wrappable.scala:425` |
| Unnecessary scope | Confined to the requested surface. `AIFoundryChatCompletion` inherits the setter through `HasAIFoundryTextParamsExtended`, which is a correct consequence since it shares the Chat Completions envelope, not scope creep |
| Documentation accuracy | Development-only availability, the no-inference guarantee, the dictionary/`Map` requirement, string-key enforcement, the `BigInt`/`BigDecimal` limit, and the unchanged output-column type are all truthful. The one gap is Issue 1 |
| Notebook executability | `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` is not excluded by `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala:257-268`, so the new cells run end to end. They reference only symbols defined in earlier cells, perform no transform, and mutate no stage reused later. Notebook JSON parses cleanly |
| Repository rules | No new dependencies, no workflow, pipeline, or release changes. No RDD APIs. No `target/` edits. No credentials in source or tests |
| Not verified here | No build, codegen, test execution, or Spark run was performed, per the review mode. Generated wrappers and stubs were not inspected; the parent owns final rebuild and validation |

## Resolution notes

The performance assessment above is source review, not a benchmark. As established
in round 3, `ListMap.toMap` itself is a no-op on this Scala baseline. The writer
now bypasses `mapFormat`; no measured speedup is claimed.

Issue 1 is **fixed**. The setter's Scaladoc now explains that ordinary `Map`
does not guarantee insertion order and recommends `ListMap` for order-sensitive
objects. The notebook includes the same warning and uses `ListMap` in the Scala
example, including its properties map. This documents the input collection's
contract without trying to reconstruct an order the caller already lost.

The exact Scala notebook snippet compiled with the pinned Scala compiler and
executed successfully against the worktree's compiled classes. The exact added
Python notebook cell also executed successfully against the generated wrappers,
with envelope contents checked for all three stages. Final validation passed
48 targeted Scala tests and 18 Python tests with 77 subtests, no skips, plus
style, compilation, generation, and formatting checks.
