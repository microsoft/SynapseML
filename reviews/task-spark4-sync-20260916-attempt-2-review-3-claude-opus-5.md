# Spark 4.1 CI repair review: attempt 2, round 3

## Review summary
- Attempt: 2; round: 3; theme: Edge Cases & Robustness; mode: sequential.
- Model: `claude-opus-5`; reviewed directly, no agents, factories, builds or edits.
- Verdict: **CLEAN**. 0 defects. 1 actionable coverage recommendation, 2 non-blocking notes.
- PR #2720 -> `spark4.1`; worktree `sync-spark41-20260916`, HEAD `1c5f8921a7e6ebfd29dc49b24646877f1b3cc605`.
- Scope: the four uncommitted repair files. `git status` shows exactly those (3 modified, D untracked) plus the round 1/2 artifacts; nothing else is dirty.

## Source snapshot (rehashed from the working tree before review)
| ID | File relative to this worktree | SHA256 | Lines |
| --- | --- | --- | --- |
| J | `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala` | `5a1f1de29b966c7e95b94cb351df1bb53d86e4e5d4e0e1743b432cc90e36ca86` | 150 |
| D | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala` | `0c1d309b089013d6c19621ba2abf864fe78ffcc318299e54dc5ce9286625e84d` | 183 |
| P | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala` | `5fe89b55c0dfad07c89eeeea337b9f08fb362ba66d3386f1c4983864fe92aab0` | 613 |
| O | `cognitive\src\test\python\synapsemltest\services\openai\test_OpenAIResponseSchema.py` | `675c70fa2f6049b7682abffa4ecba54f414526809447bc2697ea91e6b4497b7d` | 252 |

J, D and O are byte-identical to the Spark 4.0 port; P differs only by its pre-existing base. J and D hashes intentionally differ from `repair-report.json`, which predates the round 1 URL repair; they agree with the round 2 artifact. P changes only the nesting comment (line 20); fixture constructors are unchanged.

## Method
`matchesJar` (J:33-47) was traced on the port's JDK (`openjdk 17.0.20.1`, Microsoft build) by executing the exact changed JDK/regex expressions in a scratch Java program outside the repository, then deleting it. 24 boundary cases ran; every result matched the intended contract. No source was compiled or modified.

## Boundary results
Classpath shape matters because no `exportJars` is set, so `Test / fullClasspath` (`project\CodegenPlugin.scala:138,171,197,230`) feeds codegen exploded directories, while CI publication also supplies packaged artifacts. All four shapes resolve correctly with this port's real CI names `synapseml-core_2.13-1.1.3-python3.13-205-1c5f8921-20260916-1145-SNAPSHOT[-tests].jar`:

| Production class from | Fixture class from | Production request | Fixture request |
| --- | --- | --- | --- |
| main JAR | test JAR | true | false |
| `classes/` | `test-classes/` | true | false |
| `classes/` | test JAR | true | false |
| main JAR | `test-classes/` | true | false |

- Classifier/snapshot identity holds: `-SNAPSHOT-tests` and `-tests-SNAPSHOT` both normalize to `-tests`, main vs tests never cross-match, and `-sources` is rejected on the archive path. Build-stamped snapshots cannot alias a release, because `...-1145-SNAPSHOT` normalizes to `...-1145`, not to `1.1.3`.
- Path false positives are excluded: sibling directory `mycore`, module `opencv`, root-aggregate name `synapseml_2.13-...` (unmatched before and after this repair), and a case-shifted basename all return false. The module group is `[a-z0-9\-]+`, so nothing regex-special is interpolated into the exploded pattern.
- URL/protocol handling: encoded `%21` in an archive's parent directory, a literal `!` in an exploded directory, UNC paths, encoded spaces, and Spring-Boot-style nested `!/` all behave correctly. This confirms the round 1 fix.
- `resource.openConnection()` parses only. After `getJarFileURL`, a temp JAR was still deletable on Windows, proving the matcher takes no file handle or lock even though it runs once per discovered class.
- `toURI` can throw `URISyntaxException` on an unencoded space, and a `jar:` URL lacking `!/` throws `MalformedURLException`. Both are unreachable on the path actually used: `Class.getResource` through `jdk.internal.loader.ClassLoaders$AppClassLoader` percent-encodes, verified by loading a class from `My Work\my probe.jar`, which yielded `file:/.../My%20Work/my%20probe.jar` and a successful `toURI`. Not filed as a defect.
- Contracts preserved: both public `instantiateServices` overloads, the default argument, `instantiateObjects`, the null-resource `IOException`, and `InvocationTargetException => throw e.getCause`. No blanket catch, fake constructor or fixture blacklist. D:38-44 asserts `NoSuchMethodException` still escapes explicit test-JAR discovery.
- Test process and stream hygiene (D:156-181): stderr is merged and redirected to a file, so no pump can deadlock; `waitFor(5, MINUTES)` is followed by `destroyForcibly()` and a blocking `waitFor()`; exit code and the success marker are both asserted, with log text in every failure message; `Files.walk` and `JarOutputStream` close in `finally`; the temp root is deleted in `finally`. The temp root contains a space and the classpath is one `-cp` argument through `ProcessBuilder`, so no shell quoting is involved. `Test / fork := true` (`build.sbt:373`) makes `java.class.path` the real test classpath, and the probe adds only two entries to a command line sbt already issues.
- O:43,78-84 selects `_OpenAIPrompt` only for the stub lookup, asserts `issubclass`, requires `setResponseSchema`, and still checks every public stub method against the public runtime class, so the hand-written wrapper contract is verified rather than bypassed.

## Recommendation 1 (actionable, low severity, not a defect)
`pyTestGen` / `rTestGen` consume the same changed matcher with the test-artifact name (`core\src\test\scala\...\PyTestGen.scala`, `RTestGen.scala`, wired at `CodegenPlugin.scala:168-171,227-230`), but neither ran locally: `spark41-url-green.log` and `spark40-url-green.log` contain zero `testgen` mentions. Static evidence is favorable, since `PyTestFuzzing`/`RTestFuzzing` occur in 14 test-source files and 0 main-source files, and the traced `test-classes` and `-tests.jar` mappings are both true. Confirm the CI testgen and Databricks test-publication jobs before treating the repair as fully exercised; this port has not yet had its first Azure run.

## Non-blocking notes
- `PyCodegen.generatePythonClasses` (`PyCodegen.scala:109-115`) still accepts an empty discovery list silently. This is pre-existing and identical on master `1305587a`; the real runs produced 164 Python, 123 stub and 95 R files with `SelectColumns` present and no fixtures, so the ordinary path is non-empty on both ports.
- Test-only nits, no change requested: the probe would raise `NullPointerException` rather than its `require` message if the fixture resource were missing, and an interrupted `waitFor` would leave the child JVM undestroyed. Both remain visible through the captured log.

## Limitations
Java equivalents of the exact changed expressions were traced, not the compiled Scala helper; calling it directly forces the strict `WrappableClasses` val and a full classpath. Existing evidence (27 then 29 tests across 4 suites per port, all-module compile/testcompile/style, real main/test JAR packaging, real-JAR Python/stub/R codegen, wheel payloads, Black 22.3.0, and the PySpark 4.1.1 / Python 3.13.13 stub red/green logs) was read, not rerun; the parent's independent SBT rerun was not duplicated. No cloud, publication, native or full JVM-backed Python validation was performed, and no merge-ready claim is made. No source, index, refs, remote state, prior artifacts or master-guide artifacts were changed. This artifact completes only attempt 2, round 3.

## Driver disposition after review

The additional packaged-JAR `core/pyTestgen` and `core/rTestgen` runs passed.
They produced 81 Python and 67 R test files, including the SelectColumns
regressions. Production Python/R generation and wheel payload checks also
passed against actual SBT main/test JARs. Source hashes remain those above.
The native `/tmp` output target and repository `getDatasets` prerequisite
resolved the filesystem and missing-data failures encountered on the sibling
Spark 4.0 run; no product workaround was added to either port.
Evidence is `spark41-final-packaging-complete.log` and
`spark41-final-packaging-evidence.json` in the session evidence directory.
These are generation checks, not execution of the generated Python/R tests.
Remote test publication remains a required CI check.

The two test-only diagnostic/interruption notes do not change production
behavior and were explicitly non-blocking. They remain recorded rather than
expanding this repair into a test-process refactor.
