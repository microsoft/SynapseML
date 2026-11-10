---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 16
stage_attempt: 1
round: 1
timestamp: "2026-11-09T08:55:33Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "191defb8ad11f564145a1cfe339e2c9258fd227bb741e9bff9d2a5253761958a"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-16-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Diff coverage:** I read all 946 lines of the assigned frozen diff in the 7 required ranges ([1,150] through [901,946]). Its hunks touch only the 4 listed files: `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala`, `.../codegen/Wrappable.scala`, `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala` and `.../codegen/PyCodegenSuite.scala`.
- [x] **Guidance and versions:** I read `AGENTS.md`, `.github/skills/code-review/SKILL.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `build.sbt`, `environment.yml` and `scalastyle-config.xml`. The PR targets `master`. The versions match the caller's evidence (Spark 3.5.0, Scala 2.12.17, Python 3.11 environment).
- [x] **Diff matches source:** The current `PythonMethodExtractor.scala` (all of it) matches the diff. Its new pieces are `pythonIdentifier`, `effectiveMethods`, `bindingDecorator`, `explicitlyContinues`, `splitArguments`, `indentation`, `logicalLines`, and the `extract` changes that add the `unconditional` flag.
- [x] **Class-body indent:** `indentation("")` returns 4, because `GenerationUtils.indent("", 1)` splits an empty string into one empty element and adds 4 spaces (`core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/GenerationUtils.scala`). So `unconditional = indent == indentation("")` correctly marks class-body definitions. Tabs (advance to the next multiple of 8) and form feeds (reset to column 0) follow CPython's tokenizer. Continued lines keep the indentation of their first physical line.
- [x] **Subprocess security (`pythonIdentifier`):**
  - Arguments are passed as a vector with no shell, and the name travels as base64, so nothing can be injected.
  - The interpreter runs with `-I` (isolated mode), and the script contains no double quotes or backslashes, so Windows argument quoting is safe.
  - Output is ASCII-only JSON (`ensure_ascii=True`) parsed by spray-json, which allows the trailing CRLF.
  - Any failure becomes an `IllegalStateException` that keeps the cause, so the build fails closed.
  - ASCII names never start a process. Scalastyle's `NonASCIICharacterChecker` keeps non-ASCII names out of production stages, so production builds never need the interpreter.
- [x] **Python name semantics:** Checking `isidentifier()` on the raw name and then applying NFKC matches how CPython binds identifiers (PEP 3131). A backslash line continuation acts as a token separator, which matches replacing it with a space in `logicalLines`. Continuations are joined only when no string is open, so the two expression scanners stay in sync.
- [x] **Stub binding logic:**
  - `effectiveMethods` keeps the last unconditional definition plus any later conditional ones. `pyStubAdditionalMethods` groups them by Python identity and leaves out names whose rendered alternatives differ, including where one alternative can't be rendered.
  - Traced fixture results: `loadOutput` gets `@staticmethod` and `fromPath` gets `@classmethod`. `describe`, `refresh` (async), `relabel` (full-width effective definition) and `maybe` are left out. `certain` is emitted once.
  - `bindingDecorator` strips nested parentheses across multiple lines and terminates.
  - `decorators.init`/`.last` can't run on an empty list, because whenever `decoratorIndent` is set at least one decorator has been recorded.
- [x] **`Wrappable.scala` changes:**
  - `validateServiceParamAliases` normalizes Param names, the generated alias and the generated accessor names. It runs over `extract`, which covers conditional and async definitions, and it runs before any file is written.
  - The `*` and `/` markers are excluded from both the value-argument count and the typing loop. The type-inference iterator therefore always has exactly as many entries as it is asked for.
  - `splitArguments` handles brackets, strings and escapes that fall on a comma boundary.
- [x] **Tests are not vacuous:**
  - `assertAliasConflict` first compiles the fixture with Python, so the async, continued and Unicode 14 declarations are proven valid. It then intercepts `IllegalArgumentException` and checks the exact message.
  - The Unicode failure test checks the name, the interpreter name and that a cause is attached.
  - All parametrized test names (decorator syntax, body strings, class statements, alias names, alias stages) are unique, so ScalaTest won't reject duplicates.
- [x] **Compatibility:**
  - No public JVM signature or serialized Param changes: `PythonMethodExtractor` and `Method` are `private[codegen]`, and the new `Wrappable` helpers are private.
  - No new dependencies: spray-json is already a core dependency and `scala.sys.process` ships with Scala.
  - The blank-line removals in `BaseWrappable` leave `Wrappable.scala` at exactly 800 lines, the scalastyle `maxFileLength`. They are needed to pass scalastyle, so they are not unrelated refactoring.
- [x] **Observations that are not findings:**
  - Lambda defaults are now left out of stubs, as the code comment documents.
  - The `.replace("\\\n", "")` call before `bindingDecorator` is unreachable but harmless.
  - The Unicode 14 fixtures need Python 3.11 or later, which `environment.yml` provides.
  - Duplicate stubs for overridden generated accessors and the narrower decorator spellings that are recognized were already there before this PR, so they are out of scope.
- [ ] **Not run:** I ran no tests or builds in this read-only review. Validation relies on the caller's evidence: compile, Test/compile and scalastyle passed, and the continued-default case was rerun and passed.