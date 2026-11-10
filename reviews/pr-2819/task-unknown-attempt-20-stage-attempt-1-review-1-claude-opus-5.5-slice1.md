---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 20
stage_attempt: 1
round: 1
timestamp: "2026-11-09T12:31:19Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "00b69ca2529ba886b4b88648911f41f0361ba322d7cece0b2b72a5152f5f5b4e"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-20-stage-attempt-1-review-1-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Frozen diff fully read.** I read all 1003 lines of the assigned frozen diff, in the required ranges [1,150], [151,300], [301,450], [451,600], [601,750], [751,900] and [901,1003]. It covers 4 files: `PythonMethodExtractor.scala`, `Wrappable.scala`, `PyCodegenFixtures.scala` and `PyCodegenSuite.scala`. I also read the current versions of those files and `GenerationUtils.scala` by exact path.
- [x] **Guidance and versions.** I read `AGENTS.md`, `.github/skills/synapseml-branches/SKILL.md`, `.github/skills/synapseml-branches/references/branch-spark3p5.md`, `.github/skills/code-review/SKILL.md`, `build.sbt` (Spark 3.5.0, Scala 2.12.17, spray-json already a core dependency) and `environment.yml` (Python 3.11.8). No new library dependency is added; `scala.sys.process` is part of the Scala standard library.
- [x] **Security: `pythonIdentifier`** (`core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala:40-63`).
  - ASCII names return immediately and never start an interpreter. Current all-ASCII generation therefore has no new process or tool dependency.
  - Non-ASCII names are Base64-encoded and passed as a separate argument, with no shell involved, so nothing can be injected.
  - The interpreter runs with `-I` (isolated mode), so environment variables, user site-packages and the working directory cannot redirect the standard-library imports.
  - It checks the raw name with `isidentifier()`, which matches CPython's own identifier check, then returns the NFKC form as ASCII-only JSON. NFKC is how Python normalizes identifiers.
  - Launch failures, non-zero exits and parse failures all raise `IllegalStateException` naming the identifier.
- [x] **Effective bindings: `effectiveMethods`** (`PythonMethodExtractor.scala:65-73`). The last unconditional definition for each normalized name wins, and conditional definitions after it are kept. That matches the order in which Python executes a class body.
- [x] **Alias validation** (`core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala:156-169`).
  - It intentionally uses `extract`, which returns every candidate binding but excludes definitions nested inside functions. This keeps collision detection conservative.
  - It normalizes param names, `${name}Col` and `get/set${Cap}Col` the same way. Fixtures cover café written composed and decomposed, the Kelvin sign, U+A7F2, and `async`/continued declarations.
- [x] **Line joining and indentation** (`PythonMethodExtractor.scala:158-187`).
  - Backslash continuations are joined only outside strings, and the joined line keeps the first physical line's indentation, as Python does.
  - Columns are measured on the generated class body, which `GenerationUtils.indent` indents by 4 spaces. Tabs advance to the next multiple of 8 and form feeds reset to column 0, as in CPython's tokenizer. So `unconditional` is true exactly when a definition is directly in the class body.
  - The two line scanners stay in step, because blank and comment lines carry no string or bracket state.
- [x] **`extract`** (`PythonMethodExtractor.scala:189-239`).
  - `decorators.init` and `decorators.last` run only when `decoratorIndent` is set, which always implies at least one decorator (`:213-215`, `:228-229`).
  - `bindingDecorator` (`:75-84`) removes balanced parentheses and rejects decorator calls.
  - `MethodStart` now finds `async` and Unicode definitions, while stub rendering remains limited to the ASCII-only `MethodPattern` (`:16-17`).
- [x] **Tokenizer and argument splitting.**
  - `IdentifierToken` (`:28`) treats any non-ASCII character as part of an identifier, as CPython's tokenizer does, so `lambda℘` is not read as the `lambda` keyword.
  - `splitArguments` (`:139-156`) rejects real `lambda` tokens, keeps commas inside strings and brackets together, and returns `None` for unbalanced input.
  - The string and number token patterns use mutually exclusive alternatives, so there is no catastrophic backtracking.
- [x] **Stub rendering** (`Wrappable.scala:414-441`, `466-494`).
  - `*` and `/` pass through and are excluded from `valueArgCount`, so `inferredTypes.next()` is called exactly `valueArgCount` times.
  - Definitions grouped by normalized name are emitted only when they are token-identical; otherwise the method is omitted. Previously, duplicate definitions or an invalid `*: Any` stub could be emitted.
  - Supported single-line ASCII stubs are unchanged, apart from normalized spacing after annotation colons.
- [x] **Spec conformance.**
  - Only `private`/`private[codegen]` members changed, so public JVM signatures and serialized Params are preserved. The Params changes are in test fixtures only.
  - The blank-line removals are consistent with the 800-line `FileLengthChecker` in `scalastyle-config.xml`: `Wrappable.scala` and `PyCodegenSuite.scala` both end at exactly line 800.
  - The U+A7F2 case needs Unicode 14 (Python 3.11 or later), which matches the pinned 3.11.8.
- [x] **Existing limitations, not counted as findings.** None of these is a regression, and all are outside the changed contract:
  - class-level assignment or `del` rebinding, and binding decorators other than `staticmethod`/`classmethod` (for example `@property`);
  - signatures split across lines inside brackets;
  - `=` inside a string annotation.
- [ ] **Not executed:** I ran no build or tests in this read-only review. The caller's evidence (97 codegen cases with zero skips; compile, Test/compile and both style gates passing) was considered but not reproduced.
- [ ] **Not checked: other `pyAdditionalMethods` overrides in the repository.** The read profile forbids searching, so I could not list them. My regression reasoning covers only the changed code paths.