---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 1
timestamp: "2026-11-09T04:58:04Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "f202b51f737a78c5450e4eaf053622e7fd594eb9e9e807006e1caa4075c87897"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-11-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Read every line of the assigned frozen diff (692 lines) in the required chunks: [1,150], [151,300], [301,450], [451,600] and [601,692].
- [x] Read the guidance files `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md` and `.github/skills/code-review/SKILL.md`. Read `build.sbt` (Spark 3.5.0, Scala 2.12.17, spray-json 1.3.5 at compile scope) and `environment.yml` (Python 3.11.8).
- [x] Checked the diff against the current sources: `PythonMethodExtractor.scala` (whole file), `Wrappable.scala` (lines 1–800), `PyCodegenFixtures.scala` (whole file), `PyCodegenSuite.scala` (lines 1–450), and `GenerationUtils.scala`, which defines the `indent` behavior that `indentation` relies on.
- [x] Ran the new fixture forms through `logicalLines` and `extract` (`PythonMethodExtractor.scala:117-185`) by hand:
  - lines containing only a backslash;
  - declarations split as `async\`, `def\` and `def \`;
  - string literals continued with `''\` and `""\`;
  - indentation using form feeds, and tab plus form feed;
  - decorators that are parenthesized, commented, multiline or continued with a backslash.
  
  In each case, indentation and line joining match CPython's tokenizer rules and the fixture expectations.
- [x] Checked that `decorators.init` and `decorators.last` (line 152) are safe: whenever `decoratorIndent` is set, `decorators` is non-empty. Checked that `bindingDecorator` (lines 44–53) returns a binding only when stripping balanced outer parentheses leaves exactly `staticmethod` or `classmethod`, so it cannot report a false binding.
- [x] Checked that `*` and `/` are excluded both when counting value arguments and when assigning inferred types (`Wrappable.scala:424,474,478`). The iterator of inferred types therefore always has enough entries.
- [x] Checked that all-ASCII names take the fast path (lines 21–22). Existing stages never start an interpreter, and ASCII alias checks and messages behave as before.
- [x] No public JVM signature or serialized Param shape changes. `PythonMethodExtractor` is `private[codegen]`, the added `Method` field only affects the pattern match at `Wrappable.scala:471`, and the other new members are test fixtures.
- [x] No dependency was added: `spray.json` is already a compile dependency and `scala.sys.process` is part of the standard library.
- [x] Security: the subprocess gets an argument list with no shell, the script is a constant, the input comes from developer-written Scala source, and the code only runs at build time. No credentials or deserialization of untrusted data are involved.
- [x] Spot-checked line lengths against the 120-character limit. The longest new line is `Wrappable.scala:161` at 119 characters.
- [ ] I did not run any tests. This review is read-only. I relied on the caller's evidence: compile, Test/compile, 95 tests with 0 skips, and both scalastyle checks passed on JDK 11, Spark 3.5.0 and Scala 2.12.17.
- [ ] I did not search the repository for other users of `PythonMethodExtractor.Method`, or for existing `pyAdditionalMethods` that use the newly detected forms (`async def`, continued declarations, form-feed indentation). The read profile allows exact file views only. The caller's compile evidence covers `core`.
- [x] Observation, not an issue: the "Unicode 14" tests (U+A7F2) need Python 3.11 or later on PATH, both to compile the fixture and to normalize the name. `environment.yml` pins 3.11.8.
- [x] Considered, not raised: when a hand-written additional method overrides a generated accessor for a regular Param, the stub still contains both definitions. This predates the PR and falls outside the additional-method scope of this diff.

## Issues

### Issue 1: Non-ASCII name normalization breaks when the JVM's default charset is not UTF-8
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala
- **Line(s)**: 19–32, with the subprocess call at line 27. Callers: lines 34–42, and `Wrappable.scala` lines 160–169.
- **Description**: `pythonIdentifier` passes the raw Unicode name to Python as a command-line argument. On non-Windows JDK 11, `ProcessBuilder` encodes arguments with the JVM default charset. If that charset is not UTF-8, characters it cannot represent become `?` before Python receives them. Python then normalizes the damaged text and returns it without any error. One example is a POSIX/C locale with no `-Dfile.encoding=UTF-8`. Windows passes arguments as UTF-16, so the caller's passing run does not test this path.
- **Risk**: When a param or method name is non-ASCII, two failures are possible:
  - **Missed collision:** the NFC and NFD spellings of "café" end up as `caf?Col` and `cafe?Col`, so a real collision is not detected.
  - **False rejection:** distinct names such as `café` and `cafè` both become `caf?`, so a valid stage is rejected.
  
  `effectiveMethods` would also choose the wrong final binding. No current ASCII stage is affected.
- **Suggested Fix**: Pass the name to Python in an ASCII-safe form. For example, send the UTF-8 bytes as hex and decode them with `bytes.fromhex(sys.argv[1]).decode("utf-8")`. Alternatively, build JSON with `\uXXXX` escapes on the JVM side and read it with `json.loads`. Optionally also:
  - run Python with `-I`;
  - include the failing name in error messages;
  - cache results, since each name is currently normalized several times per stage.

### Issue 2: No test covers the new conditional-binding rule or the suppression of stale stubs
- **Severity**: Low
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala; core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala; core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenSuite.scala
- **Line(s)**: `PythonMethodExtractor.scala` lines 34–42 and 176; fixtures lines 161–186; suite lines 229–245 and 265–285
- **Description**: `unconditional` only changes the result when a conditional definition comes after an unconditional one with the same name; in that case both are kept. No fixture has such a pair. All of `TypedPythonStage`'s overrides are unconditional, and `ConditionalPythonStage`'s conditional methods all have unique names. If `indent == indentation("")` were replaced with `true`, the whole suite would still pass. A second gap: nothing checks that an unsupported final binding suppresses the earlier stub. The multiline `describe(` in `MultilineFunctionPythonStage` and `MultilineStringPythonStage` should hide the inherited `describe(self)` stub, but no assertion covers this.
- **Risk**: A regression in the main rule for choosing the final binding would go unnoticed. The stub could then drop the unconditional binding, or bring back a stale signature.
- **Suggested Fix**: Add one small fixture and a few assertions:
  - Define `def maybe(self)` followed by `if True:` with `def maybe(self, value)` inside it, and assert that both stub signatures appear.
  - Add the reverse order, a conditional definition before an unconditional one, and assert that only the unconditional one appears.
  - Assert that `def describe(` is absent from the stubs of the multiline function and multiline string stages.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed
- **What changed**: Encode non-ASCII names as UTF-8 bytes and pass their Base64 representation as an ASCII argument. Python decodes the bytes explicitly as UTF-8 before NFKC normalization. ASCII names still avoid the interpreter.
- **Why**: Raw Unicode arguments were independently confirmed to lose characters on Linux with JDK 11 and the C locale. ASCII transport avoids platform argument-charset loss without changing identifier semantics.
- **How verified**: The actual compiled production normalizer failed all four probe cases before the fix and passed all four afterward using the same Linux JDK 11 runtime, US-ASCII default charset, probe and dependencies. Cases cover NFC/NFD equivalence, distinct accented names and a Unicode 14 compatibility character. `core/compile`, `core/Test/compile`, all 95 `PyCodegenSuite` cases (zero skips), `core/scalastyle` and `core/Test/scalastyle` passed.

### Issue 2
- **Status**: Addressed
- **What changed**: Extend the existing conditional fixture with an unconditional `maybe` followed by a conditional override, and a conditional `certain` followed by an unconditional override. Assert both `maybe` signatures, exactly one final `certain` signature, and absence of stale `describe` stubs after unsupported multiline final bindings.
- **Why**: These public `makePyFile` assertions distinguish both binding orders and directly cover suppression of an inherited supported signature by an unsupported final signature. This finding was a coverage gap, not a demonstrated production failure.
- **How verified**: All 95 `PyCodegenSuite` cases passed with zero skips, including the strengthened conditional and multiline fixtures. Both generated runtime files and stubs compile under Python 3.11.8; both Scala compile and scalastyle gates passed.