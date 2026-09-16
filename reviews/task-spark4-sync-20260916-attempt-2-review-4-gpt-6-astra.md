# Spark 4.0 CI repair review

## Review summary
- Attempt: 2; round: 4; theme: Detailed Correctness; mode: sequential.
- Model: `gpt-6-astra`; reviewed directly without agents or factories.
- Verdict: **CLEAN**. Issues found: **0**.
- PR #2718 targets `spark4.0`; HEAD: `e2ea243ce83fa4a8d529d6ede7bcaa04529dfb24`.
- Artifact: `reviews\task-spark4-sync-20260916-attempt-2-review-4-gpt-6-astra.md`.
- Scope: exactly the four CI-repair files below, with direct callers inspected for context. The incoming master sync and guide PR are outside this review.

## Source snapshot
Working-tree SHA256 values, rechecked on 2026-09-16 immediately before writing:

| ID | File relative to this worktree | SHA256 |
| --- | --- | --- |
| J | `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala` | `5a1f1de29b966c7e95b94cb351df1bb53d86e4e5d4e0e1743b432cc90e36ca86` |
| D | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala` | `0c1d309b089013d6c19621ba2abf864fe78ffcc318299e54dc5ce9286625e84d` |
| P | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala` | `2c73e4f97aac6064aad0db7921a6fdc23b2ac9746af1ee79ff3bfb42d63631d3` |
| O | `cognitive\src\test\python\synapsemltest\services\openai\test_OpenAIResponseSchema.py` | `675c70fa2f6049b7682abffa4ecba54f414526809447bc2697ea91e6b4497b7d` |

J, D and O are byte-identical across both ports and were reviewed once. All 183 lines of untracked D were read, including the probe and packaging helpers. P has the same comment-only repair on both ports; its existing blank-line/test-title differences do not change fixture constructors.

## Evidence checklist
- [x] **Archive selection, J:33-40.** Protocol dispatch precedes decoding. The standard `jar` handler returns a `JarURLConnection`; `getJarFileURL` obtains its container URL without connecting or opening the archive. `toURI.getPath` decodes that container path, not the combined archive/entry string. `lastIndexOf('/') + 1` starts after the final separator and cannot underflow; only the basename enters equality. Encoded parent directories cannot supply the compared artifact name.
- [x] **Snapshot and classifier identity, J:39-40.** The anchored lookahead removes only the terminal snapshot marker before `.jar` or `-tests.jar`. Static traces map `x-1.0.0-SNAPSHOT.jar` to `x-1.0.0.jar`, and both `x-1.0.0-SNAPSHOT-tests.jar` and `x-1.0.0-tests-SNAPSHOT.jar` to `x-1.0.0-tests.jar`. `-tests` never disappears. Module, Scala binary, version-prefix and sources-classifier differences remain unequal. Build-stamp components are not stripped.
- [x] **Exploded output scope, J:42-46.** Decoded file URLs retain directory matching even with literal `!` or encoded `%21`. Both test snapshot spellings select `test-classes`; main requests select `classes`. Whole-string `matches` requires the module and exact output-directory segments, with only the intermediate `scala-*` directory optional. The extracted module alphabet contains no regex metacharacters outside a character class. Archive URLs cannot fall through to this module fallback.
- [x] **Discovery data flow and types, J:50-80.** Both public service overloads, their default argument and `instantiateObjects` remain unchanged. `None.forall` bypasses resource lookup; scoped calls still throw `IOException` on missing resources. Assignability and abstract-class filtering precede callback execution. The existing `List[T]` cast is unchanged; D's callback returns `Class` instances under `AnyRef`, which is type-consistent. Constructor failures still propagate, including unwrapping `InvocationTargetException`; there is no new catch, fixture blacklist or fake constructor.
- [x] **Probe assertions, D:19-47.** The caller supplies exactly the three indexed arguments. The probe checks that the fixture resource comes from the test JAR, invokes real Python/R production generation, requires SelectColumns Python/stub/R files and rejects fixture output. Class-returning callbacks check explicit test-artifact inclusion, production exclusion and unscoped inclusion without invoking constructors. Only the deliberately invalid explicit constructor call catches `NoSuchMethodException`; unexpected exceptions fail the probe.
- [x] **Boundary regressions, D:52-119.** Positive and negative pairs cover main/test archives under `target`, encoded spaces, both exploded layouts, snapshot aliases, misleading parent basenames, different modules and version prefixes. The two added URL tests exercise literal/encoded exclamation marks and encoded archive-parent paths without conflating protocol and decoded delimiter.
- [x] **Packaging and subprocess logic, D:122-181.** Canonical source paths are compared before excluding exploded main/test entries. Relative class entries normalize the filesystem separator to `/`; only the selected test fixture/probe classes enter the test archive. The Scala collection chain produces a classpath string without mutable/immutable casts. The child receives one classpath argument, logs both streams to a file, has a five-minute timeout and must satisfy both exit-code and marker assertions. Streams and temporary output have `finally` cleanup.
- [x] **Fixture comment, P:20.** The correction agrees with JVM discovery: nesting does not remove concrete class resources. Scala default constructor arguments are not substituted with a JVM no-argument constructor.
- [x] **Private stub versus public API, O:43,75-100.** Public stage types remain unchanged. Identity selection uses `_OpenAIPrompt` only for stub lookup and AST class-name matching, with `issubclass` guarding the relationship. `with_suffix(".pyi")` selects the generated file. The class-count assertion precedes indexing; the method set includes only direct public function declarations, must contain `setResponseSchema`, and checks every name against the public runtime class using `hasattr`. Missing names are sorted for deterministic diagnostics.
- [x] **Both port contexts.** `project\CodegenPlugin.scala:266-302` derives separate Compile/Test artifact basenames and passes them through `codegenArgs`/`testgenArgs`. The plugin, configuration, R caller, Python/R testgen callers, `Wrappable.scala` and hand-written `OpenAIPrompt.py` are byte-identical across ports. Both `PyCodegen.generatePythonClasses` implementations forward `conf.jarName` unchanged despite unrelated existing file differences. Both `OpenAIPrompt.scala:42` declarations request the internal wrapper; `Wrappable.scala:87-95,586,621-625` uses that same class name for runtime and stub output.
- [x] **Branch baseline.** Live `build.sbt:33,36,373` selects Spark 4.0.1, Scala 2.13.16 and forked tests. `environment.yml:6,18,27` specifies Python 3.12.11, Java 17 and PySpark 4.0.1. The sibling's Spark 4.1.1 / Scala 2.13.17 / Python 3.13 context was checked separately, not inferred from this port.

## Existing validation evidence
Evidence root E: `C:\Users\singhrana\.copilot\session-state\9e8e3352-e4d3-40c1-bbec-869822c1c661\files`.

- [x] Read `E\codegen-testjar-repair\repair-report.json` and `validation-commands.json`. Their earlier fixture failure, packaging and wheel records establish repair history, not the current J/D source hashes; those records predate the round-1 URL correction.
- [x] `E\spark40-url-red.log:37-44` records both compiled URL regressions failing. `E\spark40-url-green.log:331-375` records those tests and the published-JAR probe passing, 29 successes across four suites, no failed/canceled/ignored/pending tests, and both packaging-task completions. Its task list and success records at lines 10,220,222,272,322 cover all-module compile/testcompile/style; there are 14 zero-error style reports. ASCII SBT result records were extracted from the log containing NUL bytes without treating it as uniformly UTF-16.
- [x] The sibling `spark41-url-red.log:37-44` and `spark41-url-green.log:326-370` independently show the same red/green regressions, 29/29 tests across four suites and packaging success. The fixed round-1 defect is not reopened.
- [x] `E\spark40-openai-stub-red.log` fails on missing public `OpenAIPrompt.pyi`; `-green.log` passes the exact changed file-only method on Python 3.12.3 / PySpark 4.0.1 and records Black passing. The sibling logs pass on Python 3.13.13 / PySpark 4.1.1. These interpreter versions are evidence, not a claim that every environment pin was reproduced.

## Limitations
This round performed static source/data-flow review, cross-port comparisons and inspection of existing results, not a new build or runtime probe. The parent's additional actual-main/test-JAR SBT Python/R codegen, `pyTestgen`/`rTestgen` and wheel checks were not duplicated or assumed complete. Earlier wheel payloads are recorded evidence; the cleaned wheel binaries were not reinspected.

The Python runner omitted module-level Spark startup only, so its result does not prove the full JVM-backed schema suite. No native/cloud execution, remote publication retry or merge-ready conclusion is claimed. No source, index, refs, prior reviews or guide artifacts were changed. Only the two requested round-4 review artifacts were written; this review stops after attempt 2, round 4.
