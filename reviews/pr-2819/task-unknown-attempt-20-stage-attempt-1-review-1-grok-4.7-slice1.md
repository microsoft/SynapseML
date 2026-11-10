---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 1
timestamp: "2026-11-09T12:30:46Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "01ba93f463e28d3a1856d1f3a3dc7c0129b531a818c52fca5212567e7ff6bf3a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: reviews/pr-2819/task-unknown-attempt-20-stage-attempt-1-review-1-grok-4.7-slice1.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read the assigned frozen diff, lines 1–1003, in the required ranges, covering `PythonMethodExtractor.scala`, `Wrappable.scala`, `PyCodegenFixtures.scala`, and `PyCodegenSuite.scala`.
- [x] Checked `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala` (identifier normalization, effective-method selection, logical lines, argument splitting, scope and decorator extraction) against Spark 3.5.0 / Scala 2.12.17 and Python 3.11.8.
- [x] Checked `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala` alias validation and stub rendering, plus `GenerationUtils.indent`, which the new column measurement depends on.
- [x] Traced continued, async, Unicode NFKC, string, form-feed, and nested-scope fixtures in the frozen diff against the extractor. Subprocess use is `Process(Seq(...))` with Base64 argv and `-I`, not a shell.
- [ ] Did not execute tests or inspect sibling review artifacts; this pass is read-only and limited to the assigned diff and the cited sources.

## Issues

### Issue 1: Quoted `=` still truncates stub arguments
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 426-443
- **Description**: `pyStubAdditionalArgument` treats the first `=` as the default separator and then preserves only the text before it. `splitArguments` keeps commas inside quotes, brackets, and braces, but this second parse does not. An annotation such as `value: "a=b"` or `value: "a=b" = None` is split inside the quotes. The emitted stub contains an unterminated string, for example `value: "a = ...`.
- **Risk**: A hand-written signature that is valid Python produces a `.pyi` that cannot be parsed. Type-checking fails for the whole generated stub, not only that method. The new quoted-annotation fixtures never place `=` inside a quote, so they do not catch this.
- **Suggested Fix**: Split defaults only on an `=` that is outside strings and bracket depth, reusing `ExpressionState` or `declarationTokens`. Add fixtures for `value: "a=b"` and `value: "a=b" = None`, and compile the resulting stub.

### Issue 2: NFKC-equivalent parameter names are not rejected
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 157-168
- **Description**: `validateServiceParamAliases` normalizes parameter names only to test the generated `nameCol` alias. It never requires those normalized names to be unique. Spark treats `file` and `ﬁle` (U+FB01) as different params, but Python NFKC-folds both attributes and both `__init__` keywords to `file`. The same helper already used for `model` versus `model\uA7F2ol` would see this collision and currently ignores it.
- **Risk**: Codegen succeeds and writes a module whose constructor repeats a keyword argument, which Python rejects as a syntax error, or whose class body silently overwrites one `Param`. A service parameter can disappear or bind to the wrong value with no alias error.
- **Suggested Fix**: Reject codegen when normalized parameter names are not unique, and include both raw names in the error. Keep the existing `Col` alias checks.

### Issue 3: A final confusable spelling drops a supported stub
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 16-17, 64-70
- **Description**: `effectiveMethods` groups by NFKC identity and keeps only the last unconditional definition. `pyStubAdditionalMethod` then requires the raw signature to match the ASCII-only `MethodPattern`. For `def refresh(self)` followed by `def \uFF52efresh(self)` (fullwidth `r`), the survivor is the effective `refresh` binding, but its raw spelling does not match `MethodPattern`, so the stub is omitted. The `relabel` fixture only covers differing signatures, where omission is correct.
- **Risk**: An unambiguous public method is missing from the `.pyi` even though Python binds an ASCII-compatible signature. Callers lose type information without a codegen error.
- **Suggested Fix**: If the surviving name normalizes to ASCII and the logical signature is identical after substituting that normalized name, render the normalized ASCII signature. Continue omitting alternatives whose normalized tokens differ.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No source change; this is a pre-existing annotation-parser limitation rather than a regression in the changed contract.
- **Why**: Actual `makePyFile` calls using target baseline `6c2699defcfb24d5bff5ba55f2252ba2d8cbea24` and the changed implementation both produce an unterminated quoted annotation when `=` appears inside it. The responsible `Wrappable.scala` remains byte-identical to that comparison. Broadening annotation parsing is outside this PR.
- **How verified**: Reused the already executed 12-case baseline/current public-codegen comparison, checking the target and unchanged `Wrappable.scala` source hash. The quoted-equals case fails stub compilation in both revisions; no new run or fix is claimed for this existing limitation.

### Issue 2
- **Status**: Rejected
- **What changed**: No source change; normalized parameter-name uniqueness is separate from the generated service-column alias contract changed here.
- **Why**: The target baseline already writes duplicate normalized Python constructor arguments for distinct raw parameter names. The same failure occurs after this change, independently of generated `Col` aliases.
- **How verified**: The retained baseline/current public-codegen comparison generated composed/decomposed parameter names and observed the same duplicate-argument failure in both the module and stub for both revisions. The current `Wrappable.scala` hash matches that evidence. Existing parameter-name limitations are not claimed fixed.

### Issue 3
- **Status**: Rejected
- **What changed**: No source change; raw non-ASCII method signatures remain outside the established ASCII-only stub-rendering subset documented beside `MethodStart` and enforced by `MethodPattern`.
- **Why**: Normalization is used to identify effective bindings and alias collisions, not to expand supported signature spellings. Once an unsupported fullwidth definition supersedes an ASCII definition, retaining the earlier declaration would describe a superseded binding. Its omission is intentional, including when the unsupported final definition changes arguments. Rendering the normalized spelling would expand this PR's contract.
- **How verified**: Ran eight new actual `makePyFile` cases against separately compiled baseline and current extractor/renderer sources: a sole fullwidth definition, ASCII then fullwidth with the same arguments, ASCII then fullwidth with different arguments, and fullwidth then ASCII. All generated modules and stubs compiled. A sole fullwidth definition is omitted in both revisions; a supported final ASCII definition is retained in both. The older code emitted the superseded ASCII declaration in the two mixed-order cases, including a stale argument list; the new code intentionally omits it. Source hashes and the complete pending source patch remained unchanged.