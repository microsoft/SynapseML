---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 3
timestamp: "2026-11-09T21:10:09Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "bfc058eeb0ba3b83b07248cf19bda8c9302819613a913cd453db21c3151ef3a7"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2819/task-unknown-attempt-20-stage-attempt-1-review-3-grok-4.7-slice1.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in the mandated ranges through line 1003 (PythonMethodExtractor, Wrappable, PyCodegenFixtures, PyCodegenSuite).
- [x] Inspected `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala` (`pythonIdentifier` at 42-64, indentation at 158-165, `extract` at 189-239) and `GenerationUtils.indent`.
- [x] Inspected `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` alias checks (154-167) and raw-name emission (`${sp.name} = Param` at 145, `self.${sp.name}` in the service setter).
- [x] Read branch context for master, code-review skill, `build.sbt` (Spark 3.5.0, Scala 2.12.17), and `environment.yml` (Python 3.11.8). NFKC collision handling for non-keyword names matches the new fixtures; these findings are holes around that path.
- [ ] Tests were not executed in this read-only review. Caller-reported codegen results were not treated as independent verification.

## Issues

### Issue 1: Keyword-normalized Unicode names pass validation and are emitted as syntax errors
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 42-64
- **Description**: `pythonIdentifier` treats ASCII names as already valid and, for non-ASCII names, only checks `str.isidentifier()` before returning NFKC. `isidentifier()` is true for keywords. A name such as U+FF43 + `lass` (`ｃlass`) is an identifier, NFKC-normalizes to `class`, and is accepted. The same hole exists for other keyword spellings (`Ｔrue`, fullwidth `async`, and so on). The ASCII fast path also returns reserved words unchanged. `Wrappable.scala` then interpolates the original spelling into `${sp.name} = Param(...)` (line 145) and `self.${sp.name}` (service setter). Python normalizes identifiers before the keyword check, so the generated module is a SyntaxError. Alias checks never see that failure: they only compare normalized strings.
- **Risk**: Codegen succeeds and can publish a wrapper that fails at import. The new validator is the gate added for Unicode identity, but it does not reject names that are illegal in the positions it emits. This is the same compatibility-character class as the new `ｒelabel` / Kelvin-sign fixtures, aimed at a keyword instead of another parameter.
- **Suggested Fix**: After NFKC, reject `keyword.iskeyword` in the interpreter script (include `unicodedata.unidata_version` in the error). On the ASCII fast path, reject the Python 3.11 keyword list without spawning a process. Add a fixture whose param or method name NFKC-folds to `class` or `True` and expect `IllegalStateException` before either file is written.

### Issue 2: Form-feed dedent below the class body is still treated as a class member
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 158-165, 208-232
- **Description**: `indentation` correctly implements Python’s form-feed column reset, and `GenerationUtils.indent(line, 1)` prefixes the simulated class body. A raw line `\fdef getModelCol(self):` therefore measures as column 0, which is outside the generated class (body indent is `indentation("")`, 4). `extract` never drops statements with `indent < indentation("")`. It still records the method and can set `nestedScopeIndent` to that column. The new tests only cover `\f    ` and `\t\f    `, which reset back to column 4.
- **Risk**: A form feed that dedents past the class makes alias checks and stubs disagree with the generated file. That can be a false accessor conflict, or a stub member the class does not have. If later class-body lines follow, the `.py` file is an IndentationError while codegen still treats the dedented `def` as a member.
- **Suggested Fix**: Ignore statements whose measured indent is less than `indentation("")` (do not extract them and do not open a scope). Add a fixture with a leading form feed and no following indent, and assert it is neither stubbed nor an alias hit. Keep the existing column-4 form-feed cases.

### Issue 3: Identifier normalization can hang codegen and hides interpreter failures
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 46-61
- **Description**: Every non-ASCII name blocks on `Process(...).!!` with no timeout and no handle to destroy the child. The executable is PATH `python` on Windows and `python3` elsewhere, not the Python 3.11.8 pin in `environment.yml`. A store alias or wedged interpreter hangs the build. On failure, the script’s `unicodedata.unidata_version` goes to the child stderr; `!!` does not guarantee that text is on the `IllegalStateException` callers catch. `effectiveMethods` and `validateServiceParamAliases` repeat the spawn with no cache.
- **Risk**: One bad PATH entry stalls all codegen. A version skew fails closed only when that interpreter rejects the character; the caught error often does not say which Unicode version or why. Repeated spawns make a transient failure more likely during stub generation.
- **Suggested Fix**: Wait with a short timeout and destroy the process on expiry. Capture stderr into the `IllegalStateException`. Resolve the interpreter the same way the pinned environment does, and memoize successful normalizations for the codegen run. Extend the unsupported-name test to assert the cause text includes the interpreter’s rejection reason when the process exits.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No product changes; preserved the original review and tested its Python premise.
- **Why**: Python accepts these non-keyword Unicode source spellings even when their NFKC binding names equal keywords. Rejecting the normalized names would break valid input. ASCII keyword parameter failures are pre-existing.
- **How verified**: Twenty-four baseline/current public `makePyFile` cases cover fullwidth class, True and async parameter/method names, ASCII controls, and form-feed variants. The generated Unicode modules and stubs compile on Python 3.11.8; AST bindings have the normalized names.

### Issue 2
- **Status**: Rejected
- **What changed**: No product changes; classified the limitation against the target-branch implementation.
- **Why**: Escaping `pyAdditionalMethods` out of the generated class using a column-zero form feed has the same stub mismatch and alias rejection before and after this change. It is a real pre-existing module-scope limitation, not a new regression or a claimed fix.
- **How verified**: Public baseline/current generation produces identical results for a module-global refresh, a dedented getModelCol alias conflict, and a valid column-four form-feed member. All observations are retained in the 24-case evidence.

### Issue 3
- **Status**: Rejected
- **What changed**: No product changes; retained the documented activated-interpreter contract and acknowledged optional hardening.
- **Why**: No supported-path failure was demonstrated for the finite identifier operation in the pinned environment. A wedged executable can block and stderr is not aggregated into the exception; timeout, diagnostic capture and caching are not claimed implemented.
- **How verified**: The unchanged 97-case Python 3.11.8 qualification includes explicit unsupported-name failure; documented environment activation and the CI codegen activation were inspected. The current patch matches that qualification and all 24 new public baseline/current cases.