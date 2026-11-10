---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 12
stage_attempt: 1
round: 1
timestamp: "2026-11-09T05:54:45Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f0f06fb6fd5bfd4fb1664899ca51d4ea31f77b4f2656d60b3592e5a3e4f6e340"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-12-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read all 758 lines of the assigned frozen diff using exactly the ranges [1,150], [151,300], [301,450], [451,600], [601,750] and [751,758]. The diff covers 4 files:
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala`
  - `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala`
  - `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenSuite.scala`
- [x] Read the guidance in `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`.
  - `build.sbt` already declares spray-json, so the PR adds no dependency.
  - The Python version pinned in `environment.yml` has the Unicode 14 tables that the `\uA7F2` fixtures need.
- [x] Confirmed that the current source matches the diff:
  - `PythonMethodExtractor.scala` (whole file).
  - `Wrappable.scala`: `validateServiceParamAliases`, `pyStubAdditionalArgument`, `pyStubAdditionalMethods`, `pythonClass`, `pythonStubClass` and `makePyFile`.
  - `GenerationUtils.scala`: `indent`.
  - The fixture and suite regions the diff touches.
- [x] Traced `logicalLines` and `extract` through every new fixture form. The results match how CPython reads the code.
  - Forms checked: `def \`, `async\`+`def\`, a line containing only `\` before `class`, `@\`, `@ x`, `@((x))`, multi-line parenthesized decorators with comments, and `\f` or `\t\f` indentation.
  - Matching behavior: tab stops are 8 columns, a form feed resets the column, and the first physical line sets the indentation.
  - The two `ExpressionState` instances stay in sync because only trailing backslashes in code (not in strings or comments) are removed.
- [x] Checked `effectiveMethods` in `PythonMethodExtractor.scala` lines 39-47.
  - `toMap` keeps the index of the last unconditional definition.
  - Earlier definitions, including conditional ones before it, are dropped. Conditional definitions after it are kept.
  - This matches the expectations for `TypedPythonStage` (`loadOutput`, `fromPath`, `describe`) and `ConditionalPythonStage` (`maybe` twice, `certain` once).
- [x] Checked alias identity in `Wrappable.scala` lines 159-171.
  - Generated alias and accessor names are compared after NFKC normalization against Param names and hand-written `def` names. This includes `async`, continued and Unicode declarations.
  - `NFKC(name + "Col")` equals `NFKC(name) + "Col"`, because ASCII `C` cannot combine with the character before it.
  - The ASCII fast path matches CPython, which normalizes only non-ASCII identifiers.
  - The 4 `conflictingAliasNames` cases and 16 `conflictingAliasStages` cases have consistent expected outcomes.
- [x] Checked the `*` and `/` markers.
  - `valueArgCount` and the typing map use the same predicate, so `inferredTypes.next()` cannot run out.
  - `def setAlias(self: _T, *, value: str) -> _T: ...` and `def setPositionalAlias(self: _T, value: str, /) -> _T: ...` are valid stub syntax.
- [x] Checked security and API compatibility.
  - The subprocess takes an argument list with no shell, Base64-encoded input and ASCII JSON output. There are no credentials and no cloud access.
  - Only private trait members and the `private[codegen]` `Method` class changed. No public JVM signature or serialized Param shape changed.
  - All generated parameterized test names are unique.
- [x] Considered these and did not report them:
  - **Duplicate `.pyi` entries** when a conditional definition follows an unconditional one (`maybe`): existing behavior, and the tests assert it on purpose.
  - **Stale signatures removed** when the effective binding cannot be rendered (`describe`): this follows the "supported stubs" contract, and the tests assert it.
  - **`replace("\\\n", "")`**: it never changes anything.
  - **Collisions with generated members or assignment bindings**: existing behavior outside this diff.
- [ ] Builds and tests not run (read-only review). Compile, test and scalastyle status comes only from the caller's evidence.

## Issues

### Issue 1: Identifier normalization runs Python without isolated mode
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 21-37 (the process starts at line 32)
- **Description**: This diff adds process execution to production code. `pythonIdentifier` runs `python` or `python3` from `PATH` with `-c` and imports `base64`, `json` and `unicodedata`.
  - With `-c`, Python puts the current working directory first on `sys.path`. It also honors `PYTHONPATH` and other `PYTHON*` variables.
  - So a `json.py`, `base64.py` or `unicodedata.py` in the codegen working directory, or on `PYTHONPATH`, would run instead of the standard-library module that decides identifier identity.
  - Failures show up only as a bare `Nonzero exit value: N` or `Cannot run program "python3"`. The error names neither the identifier nor the reason an interpreter is required.
- **Risk**: Codegen could run unintended code, or its alias and override decisions could depend on the environment. That could mean missed collisions or wrong stub pruning. The added risk is limited, because the build already runs repository code. Failed validation of non-ASCII names is hard to diagnose.
- **Suggested Fix**:
  - Start the process with `Seq(executable, "-I", "-c", script, encoded)`. Isolated mode exists since Python 3.4; it excludes the working directory and user site-packages and ignores `PYTHON*` variables.
  - Wrap the call so a failure is rethrown with the identifier and interpreter name.
  - Optionally cache results per name. `effectiveMethods` and both `validateServiceParamAliases` calls normalize the same names again.

### Issue 2: No stub test covers Unicode or `async` redefinitions in `effectiveMethods`
- **Severity**: Low
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenSuite.scala
- **Line(s)**: 229-262 and 405-420. Related: `PyCodegenFixtures.scala` 98-100 and 133-159; `PythonMethodExtractor.scala` 39-47.
- **Description**: `effectiveMethods` decides which definition wins by comparing `pythonIdentifier(method.name)`, and it now counts `async def` as a binding. No test runs that path with a non-ASCII or `async` redefinition:
  - Every Unicode and `async` fixture (`conflictingAliasNames`, `conflictingAliasStages`) is expected to throw from `makePyFile`.
  - `makePyFile` (`Wrappable.scala`, around lines 657-669) builds the `.py` first, and that step calls `validateServiceParamAliases()`. The exception is thrown there, so `pythonStubClass()` never runs for those fixtures.
  - The stub tests only redefine methods with the same ASCII spelling. For those names, `pythonIdentifier` returns before normalizing.
- **Risk**: A regression could pass every test and bring back stale stub signatures when a method's effective binding is an NFKC-equivalent or `async` redefinition. Two examples:
  - comparing raw `method.name` in `effectiveMethods`;
  - narrowing `MethodStart` back to synchronous definitions.

  The PR's claim that stubs handle "Unicode declarations" is therefore untested.
- **Suggested Fix**: Add one stub test with a fixture that does not collide with service-parameter accessors. It needs two pairs:
  - an ASCII `def relabel(self):` followed by `def ｒelabel(self, value):` (U+FF52);
  - an ASCII `def refresh(self):` followed by `async def refresh(self, value):`.

  Generate the files. Assert that the `.pyi` contains neither `def relabel(` nor `def refresh(`. Run `assertPythonCompiles` on both the `.py` and the `.pyi`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Start the normalizer with Python isolated mode (`-I`) and wrap nonfatal failures with the identifier and interpreter name while preserving the original cause. Keep the ASCII fast path and ASCII Base64 transport.
- **Why**: Identifier normalization must not import modules from the working directory or `PYTHONPATH`, and interpreter failures need useful context.
- **How verified**: An actual compiled-production-method probe under Linux/JDK 11/C locale reproduced four import-shadow failures (`base64` and `json`, each via cwd and `PYTHONPATH`) and one missing-context failure before the fix. All eight probe cases passed afterward, including two already-safe `unicodedata` cases and the ASCII fast path with a deliberately broken interpreter. The supported Windows/Python baseline passed all 95 codegen tests and both compile/style gates. The initial test run passed all 95 tests but exceeded the suite's 800-line style limit; the assertion was kept on one line and the complete gates passed on the final source.

### Issue 2
- **Status**: Addressed
- **What changed**: Extend the noncolliding conditional stage fixture with an ASCII `relabel` replaced by an NFKC-equivalent fullwidth-name definition, and an ASCII `refresh` replaced by an async definition. Assert that neither obsolete ASCII signature appears in the generated stub.
- **Why**: These fixtures reach stub generation rather than exiting during alias-conflict validation.
- **How verified**: The existing public `makePyFile` test generated both files, checked both stale signatures were absent, and compiled the runtime wrapper and stub. All 95 codegen tests passed with no skips; compilation and both scalastyle gates passed.