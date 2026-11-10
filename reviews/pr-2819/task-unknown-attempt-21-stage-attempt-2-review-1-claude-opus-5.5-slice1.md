---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 2
round: 1
timestamp: "2026-11-09T23:54:45Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "fb659cb9558bf0ff95d384f9581854bb0e0f721dc201f6efc38e35d15bc8a3ef"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-21-stage-attempt-2-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Frozen diff:** read all 1035 lines of the assigned frozen diff in the required chunks: [1,150], [151,300], [301,450], [451,600], [601,750], [751,900] and [901,1035]. No read was truncated.
- [x] **Guidance and versions:**
  - Read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`.
  - The active targets are master, spark4.0 and spark4.1.
  - `build.sbt`: Spark 3.5.0 and Scala 2.12.17; spray-json is already a core dependency.
  - `environment.yml`: Python 3.11.8, which has Unicode 14 tables.
- [x] **Line joining, indentation and decorators** (`core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala` lines 171–252):
  - `logicalLines` joins backslash continuations only outside strings. It keeps the first physical line's indentation and inserts a token separator.
  - `indentation` measures after the `GenerationUtils.indent` prefix. It expands tabs to multiples of 8 and resets on form feed, as CPython does.
  - The decorator-continuation code at lines 213–216 cannot call `init` on an empty vector. `decoratorIndent` is only ever set together with a non-empty decorator list.
  - `scan` strips comments before `bindingDecorator` (lines 75–84) removes parentheses.
- [x] **Effective bindings** (`PythonMethodExtractor.scala` lines 65–73 and 20–35; `Wrappable.scala` lines 466–494):
  - The last def at class-body level wins, and earlier conditional defs are dropped.
  - Later conditional alternatives must produce identical `declarationTokens`; otherwise the name is omitted.
  - Unsupported effective bindings render `None` and are omitted. These are `async`, Unicode names, `_`-prefixed names, one-line defs and ambiguous lambdas.
  - Ordering is by first appearance, so existing output is unchanged for methods defined once.
  - The tokenizer keeps strings whole. It uses longest-match rules for numbers, identifiers (including non-ASCII runs) and multi-character operators, without ambiguous alternation.
- [x] **Argument handling** (`PythonMethodExtractor.scala` lines 139–169; `Wrappable.scala` lines 414–441 and 470–476):
  - Comma splitting respects strings and brackets.
  - A top-level lambda with several parameters yields `None`.
  - `*` and `/` pass through unchanged and are not counted for JVM type inference.
  - Annotation normalization splits only on the first `:`.
- [x] **Security of `pythonIdentifier`** (`PythonMethodExtractor.scala` lines 39–63):
  - The process is started from an argument list with no shell.
  - The name is passed as base64, and Python runs in `-I` isolated mode.
  - Output is ASCII JSON, parsed by spray-json.
  - Non-fatal errors are wrapped with their cause.
  - ASCII names return early, so production codegen starts no process.
  - Choosing `python`/`python3` from PATH matches the suite's existing `pythonExecutable` (`core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenSuite.scala` lines 18–19).
- [x] **Alias validation** (`Wrappable.scala` lines 156–173):
  - Python identifier identity (NFKC) is applied to params, to the `${name}Col` keyword alias emitted by `pyParamArg`, and to all extracted hand-written defs, including conditional and async ones.
  - Fixtures and tests cover precomposed and decomposed forms, the Kelvin sign, Unicode 14, continued declarations and async declarations. They also check an explicit failure for an unassigned code point (`core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala` lines 98–159; `PyCodegenSuite.scala` lines 398–424).
- [x] **Compatibility and style:**
  - `PythonMethodExtractor` is `private[codegen]`, and the new `Wrappable` helpers are private.
  - No public JVM signature or serialized Param shape changes, and no new dependencies.
  - The removed blank lines keep `Wrappable.scala` and `PyCodegenSuite.scala` within the 800-line scalastyle limit, and new lines are at most 120 characters.
- [ ] **Tests not run by this reviewer** (read-only contract). Caller-reported evidence only: 97 codegen cases passed with zero skips, plus core compile, Test/compile and both style gates, on JDK 11, Spark 3.5.0, Scala 2.12.17 and Python 3.11.8.

## Issues

### Issue 1: Class-level rebindings other than `def` bypass effective-binding resolution and alias rejection
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala (also core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala)
- **Line(s)**: PythonMethodExtractor.scala 65–73 and 230–248; Wrappable.scala 156–169 and 485–494
- **Description**:
  - `effectiveMethods` applies its "last unconditional binding wins" rule only to `def`/`async def` entries returned by `extract`.
  - Every other class-body statement goes through the `MethodStart.findFirstMatchIn(statement)` branch and is discarded (lines 231–248). Examples are `class NAME:`, which `ScopePrefix` already recognizes at line 244, and `NAME = …`, `NAME: T = …`, `del NAME` and `import … as NAME`. All of these rebind the class attribute in Python, yet none of them overrides an earlier def.
  - **Example 1:** `def fromPath(cls, path): …` followed by the classic `fromPath = classmethod(fromPath)` still renders `def fromPath(cls, path: Any) -> Any: ...` without `@classmethod`. That is the same effective-binding mismatch this PR fixes for decorator spellings.
  - **Example 2:** `def describe(self): …` followed by `class describe: …` or `describe = None` keeps a stale method stub.
  - **Inconsistent with the PR's approach:** elsewhere the PR omits stubs whose effective binding is unsupported (async, Unicode, ambiguous lambda). These forms instead render an outdated signature.
  - **Alias validation:** `validateServiceParamAliases` also builds `additionalMethods` from `extract`. A hand-written class-level `getModelCol = getModel` (or `class getModelCol`) therefore collides with the generated service accessor without being rejected.
  - The gap existed before this PR, but it falls within the PR's stated purpose of matching stubs to effective Python method bindings.
- **Risk**:
  - Generated `.pyi` declarations can still contradict the runtime class attribute. For example, type checkers reject `Stage.fromPath(path)` against an undecorated `cls` signature, and a non-callable can be advertised as a method.
  - A generated `get<Name>Col`/`set<Name>Col` can silently shadow, or be shadowed by, a hand-written assignment, with no codegen error.
  - Impact is limited to the API surface produced at generation time. JVM behavior and serialized Params are unaffected.
- **Suggested Fix**:
  - In `extract`, emit a non-renderable binding entry for each class-body name binding that is not a def, with the same `unconditional` flag. At minimum cover `class NAME`, simple and annotated assignment targets (`NAME =` and `NAME: … =`, excluding `==`), `del NAME` and `import … as NAME`. A `Method` whose signature does not match `MethodPattern` would work.
  - `effectiveMethods` will then let a later rebinding override earlier defs, and `pyStubAdditionalMethods` will omit the name.
  - Pass the same names through `pythonIdentifier` into the collision set in `validateServiceParamAliases`.
  - Add focused fixtures: `fromPath = classmethod(fromPath)` must produce no undecorated stub, and `getModelCol = getModel` must be rejected.
  - If def-only resolution is intended, document that limit next to `effectiveMethods` instead.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: The PR description now explicitly limits extraction and collision guarantees to method definitions and lists unsupported non-def rebinding forms. No product source changed.
- **Why**: The gap is real but pre-existing, as the review also notes. Tracking arbitrary assignments, class bindings, deletion and imports would expand this bounded definition-extraction fix into additional Python class-attribute analysis. Stale non-def stubs and undetected non-def alias collisions are acknowledged, not claimed fixed. Supported additional methods should use explicit `def` declarations and supported decorators.
- **How verified**: Eighteen public `makePyFile` calls compare nine inputs on the target baseline and current source: classmethod reassignment, None and annotated assignments, class/del/import rebinding, alias assignment/class bindings, and an explicit-def control. All generated Python modules and stubs compile; selected Python and stub AST bindings are identical before and after. Exact source hashes retain the existing 97-case qualification. The template-preserving scope clarification was published and read back with unchanged PR head, base and draft status.