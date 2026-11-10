---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 18
stage_attempt: 1
round: 1
timestamp: "2026-11-09T11:16:50Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "89870c5b7882e415598476136c594bd7cc00f37bd7a7762fc035fc568c6f1130"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2819/task-unknown-attempt-18-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff completely, in the required ranges, and compared it with the current codegen sources.
- [x] Traced `PythonMethodExtractor` (`pythonIdentifier`, `logicalLines`, `extract`, `effectiveMethods`, `declarationTokens`, `MethodPattern`) in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala`.
- [x] Traced alias validation and stub rendering in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`, including `GenerationUtils.indent` in `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/GenerationUtils.scala`.
- [x] Checked fixture and suite coverage in `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala` and `PyCodegenSuite.scala`. Alias tests pair one service param with an existing param; they do not cover generated-name versus generated-name identity.
- [x] Read master branch guidance and versions (Spark 3.5.0, Scala 2.12.17, Python 3.11.8). This read-only pass did not execute tests.

## Issues

### Issue 1: NFKC identity is not enforced across generated Python names
- **Severity**: High
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 156-173
- **Description**: `validateServiceParamAliases` normalizes param names into a set, then checks only `${name}Col` against that set and `get`/`set`+`Col` against hand-written methods. It never rejects two raw param names that normalize to one identifier, two service aliases that normalize together, or two generated accessors that normalize together. ASCII concatenation used to make alias-versus-alias impossible; NFKC does not. `model` and `model\uA7F2` are the same class of pair already used in `PyCodegenFixtures.conflictingAliasNames`: neither param name is `modelCol`, but both generated aliases normalize to `modelCol`. `café` and `cafe\u0301` collapse in the set with no `require`.
- **Risk**: Generated `__init__`/`setParams` can fail to import because Python treats the duplicate parameters as the same identifier, or a later `def set…Col` can silently replace the other column accessor. `compile()` of ordinary stages does not catch the method overwrite. JVM params remain distinct, so the Python wrapper loses or breaks part of the service-column API.
- **Suggested Fix**: Build the full set of emitted Python identifiers (normalized param names, `${name}Col`, and `get`/`set` plus `get`/`set`+`Col`) and reject when normalization collapses any two. Add tests for two service params named `café`/`cafe\u0301` and `model`/`model\uA7F2`.

### Issue 2: Quote-blind `=` splitting defeats lexical stub comparison
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 424-439, 492
- **Description**: Alternatives are compared only after rendering. `pyStubAdditionalArgument` treats any `=` as a default (`contains`/`split`) and then splits the left side on the first `:`. Neither split is quote-aware, unlike `declarationTokens`. `quoted:"a=b"` becomes a truncated fake default such as `quoted: "a = ...`. Two alternatives that differ only inside quotes can render to the same broken token sequence and be emitted as one declaration.
- **Risk**: The new “quoted contents stay significant” check runs too late. Codegen can write a non-compilable `.pyi`, or keep one stub that matches neither runtime binding.
- **Suggested Fix**: Find the top-level annotation `:` and default `=` with `ExpressionState` (or compare `declarationTokens` of the source signatures before rewriting). Add a case with `=` inside a quoted annotation and assert the stub is either preserved exactly or omitted, never rewritten into a default.

### Issue 3: Form-feed dedent below the class body is still treated as a class member
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 156-163, 206-224
- **Description**: Indentation correctly prepends the generated class indent and resets the column on a form feed. `extract` then keeps any statement that is not inside a nested scope, including statements whose column is less than `indentation("")`. A leading form feed without enough following spaces lands at column 0, outside the class, but `MethodStart` still records it. Tests only cover `\f    ` and `\t\f    `, which land back on the class column.
- **Risk**: A form feed can cause a false service-column conflict, or a stub for a method that is not a class member in the generated file.
- **Suggested Fix**: Ignore statements whose computed indent is below the class-body indent. Add a fixture that dedents past the class and assert it neither conflicts nor appears in the stub.

### Issue 4: Parenthesized signatures never reach the new argument splitter
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 17, 137-153
- **Description**: `splitArguments` tracks parentheses, but `MethodPattern` captures arguments with `[^)]*`. Any `)` in a string, tuple default, or annotation makes the pattern fail before `splitArguments` runs. List and dict defaults in the new tests avoid this. `labels=("left", "right")` is therefore omitted even though the new splitter could parse it. Alias extraction still sees the name via `MethodStart`.
- **Risk**: Supported-looking continued signatures with parenthesized defaults or quoted `)` disappear from stubs with no diagnostic, while the runtime `.py` still defines them.
- **Suggested Fix**: Capture the argument list with the same delimiter-aware scan used by `splitArguments`, then keep the lambda exclusion. Add a tuple-default stub assertion.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Not applicable to this patch; pre-existing limitation.
- **What changed**: No product change for this finding; the original report is preserved.
- **Why**: This PR changes collision checks between generated service-column aliases and existing Params or hand-written methods, not the emitted names. Duplicate normalized service-Param names already produce duplicate Python constructor parameters on target `6c2699defcfb24d5bff5ba55f2252ba2d8cbea24`. The specific `model`/`model\uA7F2` alias example is also incorrect: their aliases normalize to `modelCol` and `modelCCol`, respectively.
- **How verified**: Compiled the exact target and reviewed `Wrappable.scala`/`PythonMethodExtractor.scala` separately on JDK 11/Scala 2.12.17, then executed the public `makePyFile` path for both inputs under each implementation. Python compilation fails for the accented duplicate-Param pair on both versions and succeeds for the modifier-letter example on both. All 12 baseline/current probe cases matched; no source changed during the comparison.

### Issue 2
- **Status**: Not applicable to this patch; pre-existing limitation.
- **What changed**: No product change for this finding.
- **Why**: The quote-blind default splitter is unchanged from the target. For the reported conditional `"a=b"`/`"a=c"` annotations, the target already renders the same truncated declaration and retains it through `.distinct`. This patch's token comparison does not introduce that failure. Extending the annotation grammar is outside this effective-binding fix.
- **How verified**: The isolated public `makePyFile` comparison generated both alternatives on the exact target and reviewed source. Both produced the same invalid stub (apart from the intentional annotation-colon spacing normalization); Python compilation rejected both for the same reason. The wrapper compiled on both versions.

### Issue 3
- **Status**: Not applicable to this patch; pre-existing limitation.
- **What changed**: No product change for this finding.
- **Why**: The target extractor also treats a form-feed-dedented declaration as a member. The indentation change in this PR corrects binding order for declarations that remain within the generated class; it does not introduce the separate existing problem of injected source escaping the class body.
- **How verified**: The isolated public `makePyFile` comparison used both an outside-class `outside` declaration and an outside-class `getModelCol` declaration. Both versions included `outside` in the stub, and both rejected `getModelCol` with the same service-alias conflict. Python compilation confirmed the generated non-conflicting wrapper remains valid.

### Issue 4
- **Status**: Not applicable to this patch; unchanged supported-signature limit.
- **What changed**: No product change for this finding.
- **Why**: `MethodPattern` is byte-for-byte unchanged from the target and intentionally restricts the existing supported signature subset. Adding a delimiter-aware full-signature parser would broaden that subset, not repair a regression in the new argument splitter.
- **How verified**: The isolated public `makePyFile` comparison generated tuple-default and quoted-close-parenthesis methods using the exact target and reviewed source. Both versions omitted those stubs, while both generated wrappers compiled. The new list/dict/quoted-comma cases remain covered by the existing passing codegen suite.