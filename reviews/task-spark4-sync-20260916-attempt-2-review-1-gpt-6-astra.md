# Spark 4.0 CI repair review

## Review summary
- Attempt: 2; round: 1; theme: Broad correctness; mode: sequential.
- Model: `gpt-6-astra`; requested reasoning: `max`; reviewed directly without agents.
- Verdict: **ISSUES_FOUND**. One shared, path-dependent correctness finding, Medium / P2.
- PR: #2718, targeting `spark4.0`; HEAD: `e2ea243ce83fa4a8d529d6ede7bcaa04529dfb24`.
- Artifact: `reviews\task-spark4-sync-20260916-attempt-2-review-1-gpt-6-astra.md`.
- Scope: the four uncommitted repair files below and necessary direct callers, not the 654-file sync.

## Source snapshot
SHA256 values were checked against the working files before writing this review.

| ID | File relative to this worktree | SHA256 |
| --- | --- | --- |
| J | `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala` | `d1ce546c95593d9a439006e3deff5b3463710edf6b36fd487a204f2027f3c0e1` |
| D | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala` | `228fb24a5259b21ad30c3f40c26d496f1b9d167b8378fca7dea6b45b02033948` |
| P | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala` | `2c73e4f97aac6064aad0db7921a6fdc23b2ac9746af1ee79ff3bfb42d63631d3` |
| O | `cognitive\src\test\python\synapsemltest\services\openai\test_OpenAIResponseSchema.py` | `675c70fa2f6049b7682abffa4ecba54f414526809447bc2697ea91e6b4497b7d` |

J, D and O are byte-identical across both ports. D was read in full, including its untracked probe. P changes only the incorrect nesting comment; fixture constructors remain unchanged.

## Evidence checklist
- [x] J:33-47 compares complete JAR basenames. Normalization preserves `-tests` across release, `-SNAPSHOT-tests` and `-tests-SNAPSHOT` aliases. A JDK 17 trace of the Java operations passed 243 ordinary basename/classifier comparisons, including misleading parent-directory names, version prefixes, other modules, Scala binaries and sources classifiers.
- [x] D:52-93 covers main/test JAR separation under `target`, encoded spaces and both exploded output layouts. Its paths do not cover the `!` boundary described below.
- [x] `project\CodegenPlugin.scala:266-302` derives production names from Compile artifacts and test names from Test artifacts. `PyCodegen.scala:109-115`, `RCodegen.scala:19-25`, `PyTestGen.scala:22-23` and `RTestGen.scala:21-23` pass those scopes to discovery.
- [x] J:50-80 retains both public `instantiateServices` signatures, the default argument, `instantiateObjects`, unscoped `None` discovery and constructor exception unwrapping. No new catch or production fixture blacklist was added.
- [x] D:19-47,120-153 loads fixtures from a test JAR in a separate JVM, generates real SelectColumns Python/stub/R files, rejects fixture output, checks explicit test and unscoped discovery, and retains `NoSuchMethodException` for invalid fixture constructors. Temporary archives/streams are closed in `finally`.
- [x] O:43,75-100 keeps all three public stage types. Only stub lookup selects `_OpenAIPrompt`; `issubclass`, the required `setResponseSchema` assertion and `hasattr(stage_type, name)` still check the public API. The public `OpenAIPrompt.py:4-10` inherits `_OpenAIPrompt`; Scala `OpenAIPrompt.scala:42` requests the internal wrapper.

Evidence prefix E identifies retained local validation logs, not repository files.
- [x] Read `E\codegen-testjar-repair\repair-report.json`, `validation-commands.json`, focused-test and packaging evidence. Current Scala hashes match that report. Independently rehashed its actual main/test JARs and inspected entries without extraction: SelectColumns only in main; TypedPythonEstimator only in tests.
- [x] `spark40-red-repro.log:123` records the exact fixture `NoSuchMethodException`; `spark40-green-packaging.log:70-90` records 27/27 tests across four suites, no skips, and clean core style. The same log records main/test packaging and packaged-classpath Python codegen. `spark40-r-wheel.log:288` and the packaging report establish the corrected R/wheel run; the earlier `rCodeGen` typo is resolved.
- [x] `E\spark40-openai-stub-red.log` fails on missing public `OpenAIPrompt.pyi`; `-green.log` passes the exact file-only method on Python 3.12.3 / PySpark 4.0.1 and records Black passing. The runner omitted only module-level Spark startup.

## Finding 1: file URLs can bypass the class-output discriminator
Severity: **Medium / P2**. Location: **J:34-41**, identical on both ports.

`getSchemeSpecificPart` decodes the path, then `indexOf("!/")` selects the JAR branch without checking the URL protocol. A valid exploded-class directory ending in `!`, including encoded `%21`, is therefore treated as an archive. For requested name `synapseml-core_2.13-1.0.0.jar`, the changed operations produce:

| Resource URL | Expected match | Actual match |
| --- | --- | --- |
| `file:/workspace/build!/core/target/scala-2.13/classes/example/Stage.class` | true | false |
| `file:/workspace/build%21/core/target/scala-2.13/classes/example/Stage.class` | true | false |
| `file:/workspace/synapseml-core_2.13-1.0.0.jar!/core/target/scala-2.13/test-classes/example/Fixture.class` | false | true |

The JDK 17.0.20.1 trace reported `protocol=file`, `jarEnd=16`, `mainMatch=false` for the first two URLs. The old module fallback returned true for those production paths, so the exclusion is introduced by this repair. The third URL reaches basename equality before `test-classes` is checked.

Impact: valid workspace names can silently remove production wrappers, since `PyCodegen.generatePythonClasses` accepts an empty discovery list and `pyGen:290-294` cleans existing output first. An artifact-named `!` directory can instead admit test fixtures into production discovery. Ordinary CI paths still pass.

Suggested fix: distinguish archive URLs by protocol and parse their container URL before basename matching; keep file URLs on the exploded main/test path. Add positive production and negative test-discovery cases for literal `!` and encoded `%21`. Preserve existing JVM signatures and exception behavior.

## Limitations and resolution
The new counterexample ran Java equivalents of the exact changed URI/String operations in memory, not the compiled Scala helper. Existing Scala/runtime evidence was inspected, not rerun. Wheel binaries were already cleaned; their payload results are recorded evidence. No full JVM-backed Python schema run, remote publication retry, native/cloud validation or merge-ready conclusion is claimed.

Finding 1 remains open. No source, index, refs, remote state, prior review artifacts or master-guide artifacts were changed. This artifact completes only attempt 2, round 1.

### Resolution recorded after review

Finding 1 is resolved. `matchesJar` now selects archive handling by the `jar`
protocol and obtains the container URL through `JarURLConnection` before
decoding its path. File URLs retain exploded main/test-directory matching.
This also handles encoded exclamation marks in an archive's parent directory.
The connection is not connected and its archive is not opened.

Two compiled Scala regressions failed before the fix in `spark40-url-red.log`.
They cover literal and encoded exclamation marks, misleading artifact-named
directories, both output layouts, and encoded archive-parent paths.
`spark40-url-green.log` records all-module main/test compilation and Scala
style passing, 29 passing tests across four suites without skips, and main/test
JAR packaging. The production Python/stub/R discovery probe passed with real
JARs and retained explicit test discovery and constructor failures.
Original review evidence and its pre-fix hashes above are retained unchanged.
