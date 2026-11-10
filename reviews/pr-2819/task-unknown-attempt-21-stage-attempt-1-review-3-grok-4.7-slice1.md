---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 1
round: 3
timestamp: "2026-11-10T04:37:05Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "a6280164fff56eb4672a28728af14f7bba7423a5db118e176161affb7821f9ce"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2819/task-unknown-attempt-21-stage-attempt-1-review-3-grok-4.7-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full (1035 lines) for `PythonMethodExtractor.scala`, `Wrappable.scala`, `PyCodegenFixtures.scala`, and `PyCodegenSuite.scala`.
- [x] Inspected `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala` (`pythonIdentifier` at 40-63, `MethodPattern` at 17, `splitArguments` at 151-168, `logicalLines` at 180-200, `extract`).
- [x] Inspected `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` alias checks at 156-169, stub rendering at 466-494, and `makePyFile` write order at 664-676.
- [x] Checked `GenerationUtils.indent`, branch guidance, `build.sbt` (Spark 3.5.0, Scala 2.12.17), and `environment.yml` (Python 3.11.8). Extractor state is per call; no shared mutable race. Alias checks fail closed for the tested Unicode and continuation forms.
- [ ] Tests were not executed in this read-only review. Supplied evidence of 97 passing codegen cases was not treated as coverage of a hung interpreter or parenthesized continued defaults.

## Issues

### Issue 1: Identifier normalization can hang and is repeated without a bound
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 44-60, 66-73
- **Description**: Non-ASCII names fork `python` or `python3` from `PATH` and block on `Process.!!` with no timeout, no memoization, and no check that the interpreter matches the pinned 3.11.8 baseline. `effectiveMethods` normalizes each name twice. `validateServiceParamAliases` (Wrappable.scala:156-169) and stub grouping (Wrappable.scala:485-487) do it again, and both `pythonClass` and `pythonStubClass` validate. On Windows the chosen executable is `python`, which may be an app-execution alias that waits instead of exiting.
- **Risk**: Codegen of one non-ASCII parameter or hand-written method can stall the build indefinitely. A second-phase spawn failure after Wrappable.scala:670-672 leaves a `.py` without its `.pyi`. A different Unicode database on `PATH` can reject a name the runtime accepts, or fail to apply the NFKC collision identity this change relies on.
- **Suggested Fix**: Resolve one interpreter, reject an unexpected version, memoize by name, and bound the process. Build both file bodies before writing either file so a normalization failure cannot leave a partial module.

### Issue 2: Parentheses still drop continued signatures before the new splitter
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 17, 151-168
- **Description**: Continued lines are joined, but `MethodPattern` still captures arguments with `[^)]*`. `splitArguments` runs only after that match (Wrappable.scala:467-468). A valid continued default such as `value=(1, 2)`, `value=helper(1)`, or `value=(lambda x: x)` fails the match. The stub is omitted with no diagnostic. Lists, dicts, and strings pass only because they do not contain `)`.
- **Risk**: The new continued-default path silently drops common parenthesized defaults. The runtime method remains, so generated stubs under-report the binding. Alias detection still sees the name through `MethodStart`, so this is a missing stub, not a missed collision.
- **Suggested Fix**: Capture the argument list with the existing delimiter scanner, then call `splitArguments`. Keep omitting only signatures that remain ambiguous after nesting is tracked.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No product code changed. Interpreter timeout, memoization, and atomic multi-file output remain unimplemented hardening suggestions, not claimed fixes.
- **Why**: The supported build activates pinned Python 3.11.8; an app-execution alias or arbitrary older PATH interpreter is outside that configured environment. No supported-environment hang or performance-bound violation was demonstrated. Launch and nonzero-exit failures propagate explicitly. `makePyFile` is byte-identical to the target implementation: runtime output precedes stub rendering/writing, and no atomic pair guarantee exists. A failed generation can leave partial output but is not reported as success. The absence of a process deadline and repeated non-ASCII launches are real limitations, not denied by this disposition.
- **How verified**: Retained the current 97-case qualification and exact source hashes, the documented environment/CI activation evidence, and a direct baseline/current comparison of the complete `makePyFile` method. No new tests were run, and the 97 passing cases are not claimed to test a hung interpreter.

### Issue 2
- **Status**: Rejected
- **What changed**: No product code changed. General parenthesized-default admission remains outside the existing supported signature subset.
- **Why**: `MethodPrefix` and `MethodPattern` are unchanged from the target and already excluded embedded closing parentheses. Explicit-continuation joining fixes discovery and safe signatures within that existing subset; it does not introduce a complete Python parameter parser. The reviewer confirms alias discovery still sees these declarations. Supporting parenthesized tuples, calls, and lambdas would expand the renderer's grammar rather than fix a newly introduced regression.
- **How verified**: Retained the exact baseline/current admission-expression comparison and the current public generation case asserting five safe lambda-default stubs plus ambiguous-lambda omission. The 97-case source/XML qualification remains unchanged. No new execution of the three proposed parenthesized examples, delimiter-aware admission, or expanded support is claimed.