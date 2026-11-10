---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 21
stage_attempt: 2
round: 4
timestamp: "2026-11-10T09:16:55Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "ee85a66e1b3391714e2acfe305811026f8ba1429b6baba4abfda7945dac3a608"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2819/task-unknown-attempt-21-stage-attempt-2-review-4-claude-opus-5.5-slice1.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Read coverage:** I read every line of the assigned frozen diff in the seven prescribed ranges (1–150, 151–300, 301–450, 451–600, 601–750, 751–900, 901–1035). I then read the full post-change sources: `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/PythonMethodExtractor.scala`, `Wrappable.scala` and `GenerationUtils.scala` (the last for the `indent` contract), plus `core/src/test/scala/com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures.scala` and `PyCodegenSuite.scala`. I also read `AGENTS.md`, the branch skill, the master reference and the code-review skill. Versions come from `build.sbt` (Spark 3.5.0, Scala 2.12.17, existing spray-json 1.3.5 dependency) and `environment.yml` (Python 3.11.8).
- [x] **Logical lines and indentation (`PythonMethodExtractor.scala`, `logicalLines`/`indentation`, L171–200):** I checked this against CPython's tokenizer rules:
  - A backslash-newline separates tokens, so the inserted space does not change meaning.
  - `explicitlyContinues` is false inside strings, so string continuations are never joined.
  - The first physical line sets the logical line's indentation.
  - Tabs advance to the next multiple of 8 and form feeds reset the column. Both are measured on the generated line with its 4-space prefix, so `indent == indentation("")` (which is 4) correctly marks the top level of the class body.
  - `\r` is stripped before the trailing `\`.
- [x] **Decorators in `extract` (L202–252):**
  - On every branch, a defined `decoratorIndent` means `decorators` is non-empty, so `decorators.init`/`last` (L215) cannot throw.
  - Lines inside nested scopes cannot record decorators.
  - Method and class statements clear decorator state before `scan`, so a multi-line header is never appended to a decorator.
  - `bindingDecorator` (L75–84) only takes a substring when both parentheses are present (length ≥ 2), and it recurses in tail position.
  - `@(_with_label("staticmethod"))` correctly produces no binding, because `scan` removes string contents.
  - The `.replace("\\\n", "")` at L234 is now redundant (continuations are already joined) but harmless.
- [x] **Effective bindings (`effectiveMethods`, L65–73):**
  - `toMap` keeps the last unconditional index for each NFKC-normalized name.
  - `forall(_ <= index)` keeps that definition and any later conditional alternatives, and drops earlier superseded ones.
  - I traced every expectation in `TypedPythonStage`, `ConditionalPythonStage`, `MultilineFunctionPythonStage` and `MultilineStringPythonStage` through `pyStubAdditionalMethods` (`Wrappable.scala` L485–494):
    - `certain`, `formatted` and `annotated` are emitted once.
    - `maybe`, `relabel`/`ｒelabel`, `refresh`/async, `conditionalAsync`, `bindingChange`, `literalChange`, `keywordChange`, `identifierBoundary` and the stale `describe` are omitted.
  - `groupBy` keeps source order within each group, `alternatives(name)` always exists, and `rendered.head` is never taken from an empty group.
- [x] **Argument splitting (`splitArguments`/`hasUnfinishedLambda`, L139–169):** Pieces are rejoined correctly for:
  - strings containing escaped quotes and commas (`"\",y"`, `"a\,b"`);
  - nested brackets and dicts;
  - trailing and empty pieces from `split(",", -1)`.

  Lambda handling:
  - Every top-level lambda with several parameters leaves an unfinished header in its first piece, so the method is omitted (`None`).
  - Single-parameter, nested, bracketed and annotated lambda defaults are not misclassified, and neither are `:=` or the identifier `lambda℘`.
  - All `ContinuedDefaultStubs` strings match the renderer's output exactly.
- [x] **Argument rendering (`Wrappable.scala`, `pyStubAdditionalArgument`/`pyStubAdditionalMethod`, L418–482):**
  - `*` and `/` are excluded both from `valueArgCount` and from `inferredTypes.next()`, so the type iterator is consumed exactly as many times as its length (no `NoSuchElementException`).
  - JVM type lookup for `setAlias`/`setPositionalAlias` therefore uses one value parameter.
  - The `split(":", 2)` normalization only touches text before the first `=`, so it cannot break an annotation that was previously valid.
- [x] **Alias collisions (`validateServiceParamAliases`, L156–169, and `pythonIdentifier`, L40–63):**
  - Param names, aliases, generated accessor names and every extracted `def` (conditional, `async` and backslash-continued) are compared by NFKC identity.
  - ASCII names take the fast path with no interpreter, so existing stages behave as before.
  - Passing the name as Base64 and printing with `ensure_ascii` avoids platform charset problems.
  - The `-c` script contains no double quotes, so Windows argument quoting is safe.
  - `-I` prevents local modules from shadowing the standard library.
  - Every failure is wrapped in an `IllegalStateException` with a cause, which the U+0378 test relies on.
  - The `\uA7F2` fixtures need Unicode 14 (Python ≥ 3.11), which the pinned Python 3.11.8 provides.
- [x] **Test changes:**
  - Converting `DecoratedPythonStage` to `s"""` correctly doubles the `@_with_label \\` escape.
  - Parameterized test names are unique: the two `ContinuedScopePythonStage` cases get an index suffix, and declaration and whitespace labels are escaped.
  - `assertAliasConflict` compiles each fixture before expecting rejection.
  - The expected error messages match the `require` text, which is built from the raw names.
- [x] **Pre-existing limitation, checked and not counted:** `pyStubAdditionalArgument` still splits on the first `=`, so a string annotation containing `=` (for example `x: "a=b"`) still produces an invalid stub. The old comma split reached the same code with the same result, so this diff neither introduces the problem nor makes it easier to hit.
- [x] **Scope and compatibility:**
  - No public JVM signatures or serialized Params changed: `PythonMethodExtractor` is `private[codegen]`, and the new trait members are private and non-abstract, consistent with the abstract-method test.
  - No dependency, pipeline or release files changed.
  - The code stays portable to the Scala 2.13 port branches: `prefixLength` is removed and `flatMap` over `Option` works there.
  - The blank lines removed from `Wrappable.scala` and `PyCodegenSuite.scala` are whitespace-only.
- [ ] **Tests not run:** I did not run tests under this read-only review. The caller's evidence (97 codegen cases with zero skips, compile, Test/compile and style gates on JDK 11, Spark 3.5.0, Scala 2.12.17 and Python 3.11.8) was read but not reproduced.