---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 1
round: 1
timestamp: "2026-11-09T23:28:55Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "b52ee3255c0ec29e53fc2355104a491e84167ad80deaa4ac2534e874e1daabb9"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2819/task-unknown-attempt-21-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff in full across the mandated ranges (lines 1–1035) for `PythonMethodExtractor.scala`, `Wrappable.scala`, `PyCodegenFixtures.scala`, and `PyCodegenSuite.scala`.
- [x] Read `AGENTS.md`, `.github/skills/code-review/SKILL.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `build.sbt` (Spark 3.5.0, Scala 2.12.17), and `environment.yml` (Python 3.11.8).
- [x] Inspected `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala`, `Wrappable.scala`, and `GenerationUtils.scala` for identifier identity, continuation joining, lambda splitting, and stub selection.
- [x] Traced alias checks, effective-binding selection, decorator unwrapping, and token equivalence against `PyCodegenFixtures.scala` and `PyCodegenSuite.scala`. Subprocess arguments are base64 and `Seq`-invoked with `-I`, not shell-interpolated.
- [ ] Did not execute sbt or Python. Read-only review; the reported 97-case pass was not independently run.

## Issues

### Issue 1: Continued decorator fixture is not valid Python
- **Severity**: Medium
- **File**: core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala
- **Line(s)**: 477-479
- **Description**: In the triple-quoted fixture, `@_with_label \\` is two backslash characters. Scala `"""` strings do not treat `\\` as an escape. Codegen therefore emits `@_with_label \\` at end of line. Python treats the first `\` as a continuation only when the next character is a newline; the second `\` is a syntax error. The previous fixture had a single `\`, which is the valid continuation. `PyCodegenSuite.scala` line 314 compiles this file for every decorator-binding case, so those assertions cannot succeed against this source.
- **Risk**: The continued-decorator regression is unproven, and the reported compile pass is inconsistent with the fixture. A red compile also blocks the surrounding staticmethod/classmethod assertions.
- **Suggested Fix**: Restore a single backslash in the triple-quoted string (`@_with_label \`), then confirm `DecoratedPythonStage.py` compiles and `continuedStatic` still keeps `@staticmethod`.

### Issue 2: Annotation strings are treated as default separators
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 424-436
- **Description**: `pyStubAdditionalArgument` uses `contains("=")` and `split("=", 2)` before the new colon normalization. An `=` inside a quoted annotation is treated as the default operator. For `query: Annotated[int, "a=b"]` or `key: "a=b" = lambda value: value`, the stub is truncated to an invalid form such as `query: Annotated[int, "a = ...`. Argument splitting elsewhere already ignores commas inside strings; this path does not.
- **Risk**: A valid additional method can produce a `.pyi` that does not compile, or that invents a default, while the generated `.py` remains valid. This sits on the annotated-default path this change adds.
- **Suggested Fix**: Split defaults only on a top-level `=` outside strings and brackets, using the same expression scanner as `splitArguments`, then apply colon spacing to the name/annotation half only.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No product or test source changed.
- **Why**: The fixture uses an `s"""` interpolated string, not an ordinary `"""` string. Scala's `s` interpolator processes the doubled backslash into a single Python continuation character. Removing one source backslash is not the appropriate fix.
- **How verified**: The current-source 97-case qualification includes six successful public `makePyFile` decorator-binding cases. Each compiles `DecoratedPythonStage.py` and `.pyi` and checks `continuedStatic` retains `@staticmethod`. The fixture, suite and extractor hashes match the qualified source, and the archived XML records all six cases without failures or skips. This existing evidence was retained; no repeat test run is claimed.

### Issue 2
- **Status**: Rejected
- **What changed**: No source changed; the existing quoted-annotation equals limitation remains explicitly acknowledged.
- **Why**: This is a real pre-existing `pyStubAdditionalArgument` limitation, not introduced by the lambda-default follow-up. It is deferred rather than described as fixed or as passing stub compilation.
- **How verified**: Twelve baseline/current public `makePyFile` cases already include quoted-equals: the Python module compiles and its stub fails in both versions. `Wrappable.scala` is byte-identical to that evidence. The latest extractor change only preserves otherwise supported lambda defaults. The recorded disposition retains those baseline results and the exact current-source qualification.