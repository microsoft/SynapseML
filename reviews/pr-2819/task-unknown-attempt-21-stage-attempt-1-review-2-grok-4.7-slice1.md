---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 1
round: 2
timestamp: "2026-11-10T03:25:22Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "47e4552acc7de1ecf22d8e4b3f770a8436ec971f38c54d2da59043c3cc921362"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2819/task-unknown-attempt-21-stage-attempt-1-review-2-grok-4.7-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in the required ranges through line 1035, covering `PythonMethodExtractor.scala`, `Wrappable.scala`, `PyCodegenFixtures.scala`, and `PyCodegenSuite.scala`.
- [x] Read `AGENTS.md`, `.github/skills/code-review/SKILL.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `build.sbt` (Spark 3.5.0, Scala 2.12.17), and `environment.yml` (Python 3.11.8).
- [x] Compared admission at `PythonMethodExtractor.scala:17` with `splitArguments` and with stub rendering at `Wrappable.scala:466-468`.
- [x] Checked the annotated lambda contract in `PyCodegenFixtures.scala:256-261` and the exact-stub assertion in `PyCodegenSuite.scala:271-272`.
- [x] Checked class-body indent against `GenerationUtils.indent` and `indent(pyAdditionalMethods, 1)`; alias checks stay on `extract`, stubs on `effectiveMethods`. That split matches the collision-versus-effective-binding tests.
- [ ] Did not execute tests. This read-only review does not confirm the supplied pass claim.

## Issues

### Issue 1: Signature admission still uses a paren-blind regex
- **Severity**: High
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 17, 151-168
- **Description**: Supported stubs are admitted by `MethodPattern`, whose parameter group is `[^)]*`, and only then passed to delimiter-aware `splitArguments`. Those are two grammars for one signature. `[^)]*` does not respect strings, comments, or nested delimiters. `def lambdaDefault4(self, key: "Callable[[int], int]" = lambda value: value):` therefore cannot match: the `)` characters inside the quoted annotation end the group, and neither is the parameter-list closer followed by `:`. The Regex extractor requires a full match, so `Wrappable.scala:466-468` returns `None` and the only alternative is dropped. `splitArguments` would keep that text as one finished argument, but it never runs. Bracket annotations such as `list[str]` still pass, which is why the annotated-method fixture and this lambda fixture do not exercise the same grammar.
- **Risk**: The runtime wrapper still defines the method, but the generated `.pyi` silently omits it. `PyCodegenFixtures.scala:256-261` and `PyCodegenSuite.scala:271-272` require that stub once. The same gate drops any hand-written signature with `)` inside a string, nested call, or parenthesized default, even when the commas are unambiguous. The supplied claim that all five lambda-default forms pass is not supported by this admission regex.
- **Suggested Fix**: Parse the parameter list with the same `ExpressionState` used by `splitArguments`, including quotes and bracket depth, and stop using `[^)]*` as the admission gate. Keep the unfinished top-level-lambda rejection so `lambda left,right` stays omitted.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No product code changed; the original finding is retained.
- **Why**: The quoted `"Callable[[int], int]"` annotation contains square brackets, not parentheses. The reported declaration has only the actual closing parameter parenthesis, so the claimed early regex termination does not occur. Actual embedded closing parentheses remain a pre-existing unsupported signature form: `MethodPrefix` and `MethodPattern` are unchanged from the target branch. General nested-call or parenthesized-default support is not promised by this fix.
- **How verified**: The current 97-case `PyCodegenSuite` evidence is retained with exact source and XML hashes. Its passing `ContinuedDefaultPythonStage (4)` public-path case asserts all five lambda-default stubs occur once, including `lambdaDefault4` with this annotation, then compiles the generated wrapper and stub. Direct fixture inspection confirms no parenthesis inside the argument, and comparison with the target confirms identical signature-admission expressions. No new test run or expanded signature support is claimed.