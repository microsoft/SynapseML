# Spark 4 sync, attempt 2, review 6: polish and hardening

Round 6, sequential mode, model claude-opus-5, theme polish and hardening.
Review only. No source edits, no builds, no cloud jobs, no agents.

## Source snapshot

Worktree `sync-spark40-20260916`, HEAD `e2ea243ce83fa4a8d529d6ede7bcaa04529dfb24`.
Source unchanged since the round 1 URL fix. Four files in scope:

| File | SHA256 prefix | Parity with spark4.1 |
| --- | --- | --- |
| `core/.../core/utils/JarLoadingUtils.scala` | `5A1F1DE29B966C7E` | identical |
| `core/.../codegen/CodegenDiscoverySuite.scala` (untracked) | `0C1D309B089013D6` | identical |
| `core/.../codegen/PyCodegenSuite.scala` | `2C73E4F97AAC6064` | differs, see note |
| `cognitive/.../openai/test_OpenAIResponseSchema.py` | `675C70FA2F6049B7` | identical |

PyCodegenSuite differs between ports by one pre-existing test name only. This port
reads "generated wrappers and stubs tolerate Spark 4 rejecting foreign parameter
defaults"; spark4.1 reads "generated runtime wrappers and stubs tolerate
foreign-owned parameters". That line is outside the repair hunk. The repair itself
is the same one-line comment change on both ports (diffstat: 2 lines touched each).

## Findings

No blocking issue on this theme. Seven observations, all optional.

1. Allocation in `matchesJar` does not need optimizing, and I measured instead of
   assuming. The exploded branch compiles two patterns per call, the literal
   `"synapseml-([a-z0-9\-]+)_".r` and the interpolated `String.matches`. Volume is
   bounded because `instantiateServices` applies
   `classTag[T].runtimeClass.isAssignableFrom` before the jar filter, so calls track
   concrete PythonWrappable and RWrappable stages, about 122 Wrappable mixin sites
   across 89 main sources. Those few hundred compiles sit against `getAllClasses`,
   which calls `Class.forName` on every synapse class on the classpath; core alone
   emits 1038 main and 855 test class files. Hoisting the module regex into a private
   val would be tidier and change nothing measurable. Keep the current ordering:
   `jarName.forall` short-circuits on `None`, so unscoped discovery does no URL or
   regex work per class.
2. The unclosed `openConnection()` leaks nothing, and I confirmed it. A standalone
   JDK 17 probe deleted a temp jar on Windows right after `openConnection()` and
   `getJarFileURL()`, while the control that called `getJarFile()` failed with
   FileSystemException because the handle was open. The probe was sensitive and the
   reviewed path does no I/O. Worth one comment saying `getJarFileURL` does not open
   the archive and `getJarFile` must not be called here. Without it, a later edit can
   cache a JarFile and Windows builds will stop being able to delete target output.
3. Best documentation fix available: `matchesJar` records only the snapshot-alias
   rule. It does not say why the jar branch must read `JarURLConnection.getJarFileURL`
   before decoding, which is exactly the round 1 `!` and `%21` regression, nor state
   the invariant the exploded branch rests on, that each module directory name equals
   the artifact name after `synapseml-`. I verified the invariant for all six modules
   in build.sbt lines 433 to 495 (core, deep-learning, lightgbm, vw, cognitive,
   opencv) and that the aggregate root cannot reach it, since root sets
   `.disablePlugins(CodegenPlugin)`. Nothing in the source states the coupling, so a
   module whose sbt `name` drifted from its directory would discover zero stages and
   emit empty codegen silently. Two or three scaladoc lines would keep both facts.
4. The PyCodegenSuite comment is now accurate. Line 20 reads "Nesting does not hide
   these JVM classes; discovery must exclude their test artifact", which matches CI
   236207038, and a grep for nest, exclud, and test-only across the core codegen tests
   found no other stale claim. Fixtures still declare default-argument constructors
   only (`class TypedPythonEstimator(override val uid: String = ...)`), so
   `getConstructor()` still throws NoSuchMethodException and the RED is preserved.
5. Compatibility of the jar branch checked directly, not inferred. Encoded paths work
   (`jar:file:/C:/Program%20Files/...` yields the right basename), remote jars work
   (`jar:http://h/p/...-tests.jar`), and nested jar URLs cannot reach the code because
   the JDK rejects them with "Nested JAR URLs are not supported", so there is no
   null-path case to guard. An unencoded URL raises URISyntaxException, a loud build
   failure rather than a silent mismatch, and class loaders encode, so that is
   theoretical. sbt takes declared names from the same `artifactName` that names the
   file on disk (CodegenPlugin.scala lines 290 to 303), so declared and actual agree
   by construction, and the lookahead normalizes sbt's `-SNAPSHOT-tests.jar` ordering.
6. Test cost is bounded but real. The integration test packages the whole core main
   class directory (1038 files, 8.5 MB) with default deflate and forks a JVM on every
   run. `setLevel(Deflater.NO_COMPRESSION)` would cut most of the write time for an
   archive that is deleted immediately. `codeSource(classOf[Wrappable])` also calls
   `require(source.isDirectory)`, so the suite fails rather than cancels if it is ever
   run from packaged jars. Fine under sbt, worth knowing.
7. The Python change lines up with the shipped wrapper. The new import matches
   `cognitive/src/main/python/.../OpenAIPrompt.py` line 4 exactly, and that file
   declares `class OpenAIPrompt(_OpenAIPrompt)`, so the test tracks real inheritance.
   Generated setup.py sets `package_data` to `["*.pyi", "py.typed"]` (PyCodegen.scala
   lines 268 to 269), so `_OpenAIPrompt.pyi` ships in the wheel and the stub lookup
   works against an install, not only a source tree. Small drift: the method is still
   named for nested conversion helpers while it now also asserts inheritance and
   public method existence. Rename only if the file is touched again.

## Limitations

- I ran no sbt, no tests, and no cloud jobs. The two probes were standalone JDK
  17.0.20.1 files from PATH, written outside the repository and deleted afterward.
  They exercise JDK URL semantics, not SynapseML code, and that JDK may differ from
  the sbt-selected one.
- The performance statements come from call ordering plus file counts, not from a
  profiled codegen run.
- `pyTestgen`, `rTestgen`, and the wheel runs that follow the dataset restore are
  still pending and parent-owned. Nothing here validates them.
- I did not re-review the incoming master files or the guide PR, and I propose no
  general empty-discovery change, since that behavior is inherited unchanged from
  master and is out of scope.
- Rounds 1 through 5 already closed correctness. This round covered polish,
  allocation, resources, compatibility, and comment accuracy only.

## Driver disposition after review

The optional observations do not identify a current correctness defect.
Keep the reviewed source unchanged. The URL regression names document the
decode boundary; the API deliberately calls `getJarFileURL`, not
`getJarFile`; module matching and incorrect-module rejection are tested.
Additional comments, regex caching, archive compression tuning and a broader
test rename are not needed for the targeted repair. No performance gain is
claimed from the analytic review.

The pending generation run completed successfully. With real SBT main/test
JARs, Python/R production generation and test generation passed, producing
81 Python and 67 R test files with SelectColumns controls. The wheel contains
the production Python and stub files and no fixture output. See
`spark40-final-packaging-complete.log` and
`spark40-final-packaging-evidence.json`. The generated tests were not executed
by this check; fresh remote CI remains required.
