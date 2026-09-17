# Round 4: detailed correctness

**Model:** GPT-6 Astra (`gpt-6-astra`)

**Theme:** Line-by-line data flow, type conversion, ordering, and exceptions

**Mode:** Independent, synchronous, read-only source review

**Base:** `master` at `dd220c9ead82245fb4b4f4c1624a5d2d22dd9d24`

**Status:** ISSUES_FOUND

**Issue count:** 3

## Issue 1: Newly supported big integers can change value during decoding

**File:** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:31-32,53`

**Severity:** Medium

**Problem:** The new arbitrary-precision writers expose a lossy decode/re-encode path. Binary representability, tested by `isExactDouble` and `isBinaryDouble`, does not guarantee preservation by Spray JSON's decimal `Double` writer.

**Evidence:** `BigInt("9223372036854775808")` initially writes exactly. It exceeds `Long.MaxValue`, but `isExactDouble` succeeds, so line 53 converts it to `Double`. Re-encoding produces `9.223372036854776E+18`, whose decimal value is `9223372036854776000`, a change of 192. This follows from the pinned Scala and Spray implementations and was checked with exact decimal arithmetic. The current numeric tests miss this boundary.

**Suggested fix:** Keep the Int/Long branches, but narrow to Double only when its decimal representation preserves the number, such as through `isDecimalDouble`; otherwise retain BigDecimal. Add regressions for this integer and an exact binary-fraction decimal.

## Issue 2: Model persistence independently rounds high-precision decimal schemas

**File:** `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala:32`

**Related path:** `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala:115-119,157-161`

**Severity:** Medium

**Problem:** The newly accepted BigDecimal values serialize correctly into outgoing requests but lose precision through model metadata. Fixing Issue 1 alone does not address this separate conversion.

**Evidence:** A schema bound of `BigDecimal("0.12345678901234567890123456789")` is written exactly by `AnyJsonFormat`. Metadata construction then calls json4s `parse(p.jsonEncode(v))` with its default Double parsing, reducing it to `0.12345678901234568`. Spark's metadata reader also uses default parsing. The direct formatter round-trip test bypasses both metadata conversions; the stage persistence fixtures cover integers, not high-precision fractional values.

**Suggested fix:** Preserve decimal precision through metadata construction and parameter restoration without changing the stored JSON shape. Add a public-stage save/load regression comparing the exact fractional bound before and after. This concerns numeric values, not object-key ordering after reload.

## Issue 3: Python silently coerces and merges nested non-string keys

**File:** `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/HasOpenAIResponseSchema.scala:61-64,73`

**Severity:** Medium

**Problem:** The generated helper passes non-string dictionary keys into the Java conversion unchecked. Nested keys are subsequently stringified, bypassing the new writer's String-key guards and potentially dropping entries.

**Evidence:** A nested `properties` dictionary containing both integer key `1` and string key `"1"` reaches `LinkedHashMap` with two entries. `ServiceParam.toScalaAny` at `core/src/main/scala/com/microsoft/azure/synapse/ml/param/JsonEncodableParam.scala:59-61` applies `k.toString` and constructs a `ListMap`, collapsing those entries into one. The setter therefore stores an altered schema instead of rejecting the unsupported key. The Scala writer's guard cannot detect the original key afterward.

**Suggested fix:** Require string keys recursively inside the new Python `_convert` before calling `put`. Leave existing `setResponseFormat` conversion unchanged. Add generated-wrapper regressions for nested numeric/null keys and collisions, asserting rejection without changing the configured parameter.

## Evidence checklist

- [x] Reviewed all nine changed/new source and notebook files against the specified base.
- [x] Traced Scala overloads, both API envelopes, Prompt forwarding, Python collection conversion, and stub generation.
- [x] Inspected ordering, null handling, numeric dispatch, exceptions, copy/persistence paths, and regression assertions.
- [x] Parsed the current Python method template and added notebook code without execution; checked numeric counterexamples against pinned dependency source.
- [x] No builds, Spark execution, edits, commits, or external posts. Concurrent validation results were not claimed as this review's evidence.
- [x] No requirement imposed for object-key ordering after model reload.

## Verified resolution

All three findings are **resolved**.

Issues 1 and 2 were addressed by withdrawing the newly added arbitrary-precision
writers rather than expanding this convenience API into a Spark metadata change.
`BigInt` and `BigDecimal` were unsupported in the base writer and remain so. The
new setter now validates nested values using the existing serializer before
changing the parameter. Core and public-stage regressions verify that the
reported counterexample values are rejected, not accepted and rounded. The
notebook documents this limit. Existing numeric decoding and metadata parsing
were not changed.

Issue 3 was fixed by checking String keys recursively inside the generated
Python converter before inserting them into Java collections. The existing
`setResponseFormat` converter is unchanged. Six invalid-key shapes, including
root/nested null keys, numeric keys, a string/numeric collision, and a key inside
a list, are tested on each of the three stages. All 18 cases reproduced failures
against the old generated wrapper and passed after regeneration, with unchanged
parameters after rejection.

Final verification passed 48 targeted Scala tests and 18 generated-Python tests
with 77 subtests, with no failures, errors, or skips. Core/cognitive style,
compilation, code generation, and Python formatting also passed.
