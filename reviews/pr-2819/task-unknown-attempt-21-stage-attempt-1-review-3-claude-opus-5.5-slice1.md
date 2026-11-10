---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 1
round: 3
timestamp: "2026-11-10T04:03:03Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "1db2b64c44b2a6a6427879955d3e4371184978d953516be16e09a3c2aaccf5a0"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-21-stage-attempt-1-review-3-claude-opus-5.5-slice1.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Diff coverage:** read the assigned frozen diff in the required order: [1,150], [151,300], [301,450], [451,600], [601,750], [751,900], [901,1035]. All 1035 lines came back untruncated and cover all four inventory files.
- [x] **Guidance and versions:** read AGENTS.md, .github/skills/code-review/SKILL.md, .github/skills/synapseml-branches/SKILL.md and .github/skills/synapseml-branches/references/branch-spark3p5.md.
  - build.sbt gives Spark 3.5.0 and Scala 2.12.17. spray-json is already a dependency, so the PR adds none.
  - environment.yml pins Python 3.11.8, whose Unicode 14 tables the U+A7F2 fixture needs.
- [x] **Sources read by exact path:**
  - core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala (whole file).
  - Wrappable.scala: `validateServiceParamAliases` L156–169, `pyStubAdditionalArgument` L414–441, `pyStubAdditionalArgumentTypes` L443–450, `pyStubAdditionalMethod`/`pyStubAdditionalMethods` L466–494.
  - PyCodegenFixtures.scala, and PyCodegenSuite.scala including L398–424.
  - The `GenerationUtils.indent` contract that `indentation` (L171–178) depends on.
- [x] **`pythonIdentifier` (L40–63):**
  - Pure-ASCII names return early (L41–42), so real stages never start Python.
  - Input is sent as Base64 and output read as ASCII JSON, so platform charsets can't corrupt it.
  - The script has no double quotes or backslashes, so Windows argument quoting is safe.
  - spray's parser skips the trailing line break, and `!!` closes stdin.
  - Launch, exit and parse failures are all wrapped in an `IllegalStateException` (L57–60). The missing stderr is Issue 1.
- [x] **`extract` (L202–252):**
  - `decorators.init`/`.last` (L215) can never run on an empty vector. `decoratorIndent` is set only alongside a non-empty append (L226–228) and cleared together with it (L241–242).
  - Decorators are stored after `scan` strips comments and strings, so `@staticmethod  # note` and `@(staticmethod)` still bind.
  - `logicalLines` (L180–200) joins only backslash continuations outside strings and keeps the first line's indentation.
- [x] **`splitArguments`/`hasUnfinishedLambda` (L139–169):**
  - String and escape state survives comma splits (`"\,"`, `"a\",b"`, `","`). A trailing comma is ignored.
  - Top-level multi-parameter lambdas give `None`.
  - `inferredTypes.next()` (Wrappable L475) can't run past the end: `pyStubAdditionalArgumentTypes` returns exactly `argumentCount` entries, and `*` and `/` are excluded from both the count and the lookups.
- [x] **Effective bindings:**
  - `effectiveMethods` (L65–73): the last unconditional definition of a normalized name wins. Earlier conditional definitions are dropped and later ones become alternatives.
  - `pyStubAdditionalMethods`: alternatives that differ (including async or non-ASCII ones, which render `None`) are omitted, not guessed. Output order is deterministic.
  - Alias validation checks every extracted declaration, which is conservative.
- [x] **Concurrency:** no shared mutable state was added. The regexes are immutable, `ExpressionState` is created per call, and each subprocess is independent.
- [x] **Considered and not reported:**
  - `def f[T]` would be captured, but PEP 695 doesn't exist on Python 3.11.8.
  - `=` inside annotation strings: unchanged split at Wrappable L424–425.
  - A backslash followed by whitespace or a comment, or a dangling `\` at end of file: already invalid Python.
  - Class-level assignment or `del` rebinding isn't tracked, before or after this PR.
  - No subprocess timeout or memoization: the script is trivial and only non-ASCII names start it.
  - The unreachable `.replace("\\\n", "")` at L234 is harmless.
- [x] **Method:** read-only, using exact-path `view` calls only. I ran no commands or tests and opened nothing under reviews/pr-2819.
  - The caller's evidence is noted, not reproduced: 97 codegen cases with zero skips, plus compile, Test/compile and both style gates on JDK 11, Spark 3.5.0, Scala 2.12.17 and Python 3.11.8.

## Issues

### Issue 1: Python's failure diagnostic never reaches the thrown exception
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala (related test: core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenSuite.scala)
- **Line(s)**: 50, 53, 57–60 (`pythonIdentifier`, L40–63); PyCodegenSuite.scala 415–424
- **Description**:
  - The script deliberately raises `ValueError('Identifier is invalid in Unicode ' + unicodedata.unidata_version)` (L50) to report the interpreter's Unicode version.
  - It runs through a plain `Process(...).!!` (L53). In Scala 2.12, `!!` without a `ProcessLogger` sends the child's stderr straight to the JVM's `System.err`. On a nonzero exit it throws only `RuntimeException("Nonzero exit value: 1")`.
  - The wrapper at L57–60 therefore produces "Python identifier normalization failed for '<name>' using python". Its cause contains neither Python's message nor the traceback.
- **Risk**:
  - The useful explanation never reaches the exception. In the main failure mode, a valid name is rejected because the interpreter's Unicode tables are too old (for example U+A7F2 on Python 3.10 or earlier).
  - In CI, that explanation lands in interleaved stderr or is lost where stderr isn't kept with the test report, so developers must reproduce the failure locally.
  - The passing expected-failure test prints a Python traceback into build output on every run.
  - That test's assertions (L421–422) only check the name, the substring "python" and a non-null cause. A missing interpreter (`IOException: Cannot run program`) passes them too. So the test can't distinguish "identifier rejected" from "interpreter not launched".
- **Suggested Fix**:
  - Capture stderr in a thread-safe buffer, because the logger runs on the stderr reader thread:
    `val stderr = new StringBuffer; Process(...).!!(ProcessLogger(_ => (), line => stderr.append(line).append('\n')))`
  - In the catch block, append `stderr.toString.trim` to the `IllegalStateException` message when it isn't empty.
  - Tighten the test so the message must contain "Identifier is invalid in Unicode".
  - This changes no public JVM signature or serialized Params.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Rejected
- **What changed**: No product code changed. Rejected as a blocking defect, not as a factual claim about where Python stderr is reported. The optional diagnostic enhancement remains unimplemented.
- **Why**: Failure is explicit: the exception identifies the name and executable and preserves the launch/nonzero-exit cause; the Python diagnostic is emitted to stderr, not discarded or replaced with success. The supported build activates pinned Python 3.11.8. The isolated expected-failure test is broad, but interpreter availability is independently required by the same suite's positive Unicode-14 public generation case, which compiles the source, normalizes the identifier, and requires an `IllegalArgumentException` for the resulting alias conflict. Missing Python or failed normalization cannot satisfy those assertions. Including stderr in the exception and separating failure-specific assertions would improve diagnostics, but no supported-path correctness failure or hidden whole-suite false pass is demonstrated.
- **How verified**: Retained the current 97-case qualification with exact source, XML, and log hashes; both the Unicode-14 positive case and unsupported-name failure case passed without skips. The archived full build log contains `ValueError: Identifier is invalid in Unicode 14.0.0` immediately before the passing expected-failure case. No new tests were run. Stderr still is not included in the Java exception, and the isolated expected-failure assertion still does not distinguish launch failure; neither limitation is claimed fixed.