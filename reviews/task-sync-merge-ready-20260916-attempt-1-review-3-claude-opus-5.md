# Round 3 — edge cases and robustness (public master follow-up, PR #2719)

- **Model:** claude-opus-5
- **Theme:** failure paths, boundaries, concurrency, resource cleanup, exact compatibility guards
- **Scope:** uncommitted diff + untracked sources in `.worktrees/branch-context-20260916`
  (`docs/spark4-branch-context-20260916`, on `e97b63c43c`). `reviews/` excluded from code scope.
  Round 1 findings are fixed with resolution notes; round 2 was clean.

## Findings

### R3-1 · Low–Medium · `JarLoadingUtils.matchesJar` turns two URL shapes into thrown exceptions
The previous `jarResource.toString.contains(name)` could never throw; the new body can. Read-only
JDK 11 probe on this machine: `jar:jar:file:/app.jar!/BOOT-INF/lib/...jar!/com/Foo.class` →
`openConnection()` throws `MalformedURLException: Nested JAR URLs are not supported`, and an
unencoded `file:/work/a b/...jar` → `toURI()` throws `URISyntaxException: Illegal character in
path at index 12`. (`%20` decodes correctly, confirming the intended design.)
Both escape `instantiateServices` and abort the entire codegen/testgen/fuzzing run instead of
skipping one classpath entry. Not reachable on validated paths — SBT and the new
`CodegenDiscoveryLauncher` build URLs via `File.toURI`, which percent-encodes (the suite's
`"codegen-discovery space-"` temp root proves it) — but a loader handing back a raw `file:` URL,
or a nested-jar layout, turns a non-match into a hard failure. Suggested: return `false` on
`IOException`/`URISyntaxException`, matching the fail-closed semantics the `else` branch has.

### R3-2 · Low · Exploded matching is silently coupled to "module dir == artifact stem"
`"synapseml-([a-z0-9\-]+)_"` → `.*/<group>/target/(scala-[^/]+/)?{classes,test-classes}/.*`.
Exact today: `build.sbt:324-386` gives `core`, `deep-learning`, `lightgbm`, `vw`, `cognitive`,
`opencv` artifact names each equal to their `project in file(...)` directory, and root `synapseml`
is `.disablePlugins(CodegenPlugin)` (`build.sbt:389-401`) so its unparseable `synapseml_2.12-*.jar`
is never a `jarName`. If that convention breaks the failure mode is *zero discovered classes*, not
an error. One line in the existing comment naming the invariant would help. Relatedly,
IntelliJ-style `out/production/<module>` matches neither branch, so a filtered run there discovers
nothing — a non-regression, but `getAllClasses` documents IntelliJ as a supported path, so the
comment should say exploded support means *SBT* layouts.

### R3-3 · Low · `CodegenDiscoverySuite` codifies a normalization collision as expected behaviour
`assert(matchesJar(resource(tests), "...-1.0.0-tests-SNAPSHOT.jar"))` passes because `normalized`
strips `-SNAPSHOT` whenever `.jar` follows, conflating a version ending in `-tests` with the
`-tests` classifier. Unreachable with SBT version strings, but the assertion reads as a guarantee
rather than a known limitation.

### R3-4 · Low · Documented Python/R asymmetry is unpinned
`RWrappable.rParamArg` (`Wrappable.scala:648`) still calls `thisStage.getDefault(p)`, so a
foreign-owned parameter aborts `rCodeGen` while `pyCodegen` now succeeds. Deliberate and
documented (`branch-spark4-common.md`), and `safeGetDefault` is `private` to `PythonWrappable`, so
R cannot inherit it by accident. But `CodegenDiscoveryProbe` runs `RCodegen.generateRClasses` with
the *main* jar name, so `ForeignParamPythonStage` never reaches the R path and nothing would catch
a later refactor that silently changes R output from "throw" to `NULL`. One
`intercept[IllegalArgumentException]` on `rParamArg` in `PyCodegenSuite` locks it.

### R3-5 · Low · OpenAI stub lookup cannot distinguish "no generated base" from "base regressed"
`next((base for base in stage_type.__mro__ if base.__name__ == f"_{name}"), stage_type)` falls
back to the public class, which is correct for master but means a port where the `_`-prefixed base
*should* exist but was renamed still passes against the public class's own stub. The following
`assertTrue(issubclass(stage_type, generated_type))` is tautological for this implementation —
every `__mro__` entry is a base and the fallback is the class itself — so it only rejects a future
rewrite that resolves the symbol by module lookup. Worth a comment stating that intent.

### R3-6 · Low · Recommendation export guard is an allowlist, so the bug class can recur
`test_package_exports.py` pins three literal names. `recommendation/__init__.py` is the only
non-empty hand-written `__init__.py` under `core/src/main/python/synapse/ml`, and its `__all__`
stays hand-maintained, so a *future* recommendation stage is hidden from `import *` again with
nothing failing. Comparing `__all__` against the modules present in the package directory is
equally cheap and does not go stale. (The `exec()` wrapper for the wildcard import is correct —
a literal `from x import *` in a function body is a `SyntaxError`.)

### R3-7 · Low · `validate_legacy_codegen` check order and reference-scan breadth
- `plugin.read_text()` runs before `if not build.is_file()`, so a checkout missing
  `project/CodegenPlugin.scala` exits 2 with a raw `FileNotFoundError` instead of the intended
  message; the "Missing Internal build definition" text is unreachable when both are absent.
  Reordering is one line.
- The reference scan covers root `*.sbt` and `project/**.{sbt,scala}` but not per-module `.sbt`
  files (`<root>/core/build.sbt` is valid SBT) or pipeline/shell references. A reference living
  only there yields a wrong "legacy build, no patch required" and `return 0`.
  `root.rglob("*.sbt")` with the existing `target` filter closes the SBT half.
- `direct_codegen` hard-codes the interpolation variable `$arg`; a rename fails closed with
  "Unrecognized Internal codegen", which is the safe direction but tighter than the documented
  "active direct-codegen task". Fine — noted so the diagnosis is fast.

## Clean on this theme

- **`_active_scala` boundaries.** `/*` is tested before string handling, but a string is consumed
  atomically from its opening quote, so `"/*"`, `'"'`, `"https://…"`, `/* outer /* nested */ */`
  and `""""` runs all behave. Unterminated `"`, `"""`, `/*` raise `ValueError` → exit 2;
  non-UTF-8 build sources raise `UnicodeDecodeError` (a `ValueError`) and are caught too.
- **`safeGetDefault` catch width.** `Params.getDefault` throws `IllegalArgumentException` only from
  `shouldOwn`; `ParamMap.get` does not throw. Nothing genuine is masked, and the `(_, None)` branch
  already produced `name=None` / `Optional[T] = ...`, which `pyStubAdditionalArgument`'s
  `hasDefault` parser consumes consistently.
- **Resource cleanup / concurrency in `CodegenDiscoverySuite`.** Child `destroyForcibly()` +
  `waitFor()` in an inner `finally` *before* the log is read (required on Windows for
  `deleteDirectory`), `Files.walk` and `JarOutputStream` closed in `finally`, temp root deleted in
  an outer `finally`, launcher restores the context classloader and closes the `URLClassLoader`.
  `Test / parallelExecution := false` (`build.sbt:282`) bounds this to one extra forked JVM.
  `matchesJar` adds no shared state — a `jar:` `openConnection()` is never connected, so no archive
  handle or `JarFileFactory` entry is created. Only timing assumption: the
  `waitFor(5, TimeUnit.MINUTES)` budget for a cold JVM running full Py+R codegen over all of
  `synapseml-core`. Watch it on slow agents.
- **The new Python guards really run.** `PyTestGen.main` copies `pyTestOverrideDir` into the
  generated test tree and `makeInitFiles` recursively writes `__init__.py`, so the new
  `synapsemltest/io/` and `io/http/` dirs need no checked-in one (all siblings have one — cosmetic
  only). The `core` matrix leg (`pipeline.yaml:616-637`) sets neither `TEST_SUB_PATH` nor
  `IGNORE_TEST_PATH`, so the whole tree is collected. `__pycache__/*.pyc` is ignored
  (`.gitignore:19`).
- **Minor-series Python pin, end to end.** The relaxed regex rejects `3`, `3.13.*`, `3.13.0.1`,
  `3.13rc1`, `>=3.13` (parameterized). Consumers are `pipeline.yaml:418` → `PYTHON_VERSION` and the
  demo README; both Dockerfiles use it as `conda install "python=${…}"` and already fail on an
  empty value, and a minor series is a valid conda spec. Nothing splits on three fields.
- **Port portability.** Every source change is version-neutral (no Spark/Scala/Java literal, no
  branch-named path), so it can merge into the existing `sync-spark40-20260916` /
  `sync-spark41-20260916` worktrees unadapted — `CodegenDiscoverySuite` derives the Scala binary
  version from `scala.util.Properties` rather than hardcoding it. Ports were neither mutated nor
  re-validated here.

## Validation limitations

Source review plus one read-only local JDK probe (R3-1). No build, test, network, cloud, or git
mutation was performed in this round, and no port branch was touched. Findings above are derived
from the diff and the surrounding tree, not from a fresh run. Exact-head CI, dependency
resolution, and Fabric workspace quota remain open; nothing here asserts readiness.

## Driver dispositions

- R3-1: Do not silently return false for malformed/unsupported resource URLs.
  That would report successful generation while omitting public APIs. SBT emits
  valid encoded URLs, covered by the real-JAR regression; unsupported loader
  shapes should fail explicitly. Added a source comment stating this contract.
- R3-2: Clarified that exploded matching supports SBT layouts and relies on
  the verified module-directory/artifact-stem invariant.
- R3-3: Retained the documented filename convention and existing snapshot
  alias contract. A filename alone cannot distinguish an arbitrary version
  ending in `-tests` from a test classifier; no such build version is used here.
- R3-4: No R behavior was changed or claimed. Pinning a known unsupported
  foreign-owned R default failure is outside this Python fix; R production
  generation remains covered. The branch guide records the asymmetry.
- R3-5: The shared assertion checks the actual generated stub and public
  methods, not a mandatory wrapper architecture. An equivalent public
  generated class is valid on master. Separate installed-wheel validation
  asserts the expected layout per target without weakening the method checks.
- R3-6: Removed the redundant handwritten recommendation initializer instead
  of maintaining its export list. Actual generated output already exports all
  nine classes. The test also checks every generated model module, while
  retaining the three original missing-name regressions.
- R3-7: Check for the root build before reading its plugin and scan all SBT
  build files outside generated targets. Added a per-module build negative.
  The adapter intentionally recognizes the known direct task shape, not
  arbitrary shell/pipeline implementations; all later packaging/tests remain
  enabled and unknown build shapes still fail explicitly.
