# Round 6 — polish, hardening, performance, observability, docs

- **Model:** claude-opus-5
- **Theme:** round 6 only, sequential. Diagnostics, durability, cost, and documentation accuracy.
- **Verdict:** ISSUES_FOUND — six low-severity polish items. No correctness defect found; nothing changed.
- **Scope:** uncommitted diff + untracked sources in `.worktrees/branch-context-20260916`
  (`docs/spark4-branch-context-20260916`, HEAD `e97b63c43c`), public `master` / PR #2719:
  11 modified tracked files, 1 deletion (`core/.../python/synapse/ml/recommendation/__init__.py`),
  3 untracked sources. `reviews/` is evidence, not code scope. Rounds 1–4 findings verified fixed in
  the current text; round 5 clean. Round 3 and 4 dispositions were treated as binding.

## Findings

### R6-1 · Low–Medium · `matchesJar` failures name nothing
The R3-1 disposition (fail explicitly rather than silently omit APIs) is right, but "explicitly" is
currently the raw JDK exception escaping `instantiateServices` with no class, expected artifact, or
URL. Read-only JDK 17 probe (temp dir, deleted; no repo build):

| resource URL | outcome |
| --- | --- |
| `jar:file:relative-core.jar!/com/A.class` | `NullPointerException: Cannot invoke "String.lastIndexOf(int)" because "<local4>" is null` |
| `jar:jar:file:/app.jar!/BOOT-INF/lib/core.jar!/com/A.class` | `MalformedURLException: Nested JAR URLs are not supported` |
| `jar:file:/work/a b/core.jar!/com/A.class` | `URISyntaxException: Illegal character in path at index 12` |
| `jar:file:/work/a%20b/...`, `jar:file://host/share/...` | resolve to `core.jar` (correct) |

The opaque-URI case is an NPE naming a JIT temporary — the one shape with zero diagnosis. The
adjacent branch sets the precedent: `IOException(s"Could not find resource for class ...")`.
Wrapping the call so all three rethrow as `IOException(s"Could not match resource $jarResource for
artifact $name", e)` keeps fail-loud semantics unchanged and makes the failure actionable.

### R6-2 · Low · Documented plugin check is only incidental
`tools/ci/README.md` says the adapter accepts the legacy layout "only when the root build and
codegen plugin exist". `validate_legacy_codegen` checks `build.is_file()` explicitly but then calls
`plugin.read_text(...)` directly, so `build.sbt` present without `project/CodegenPlugin.scala`
exits 2 as `error: [Errno 2] No such file or directory: ...CodegenPlugin.scala`. Correct exit code,
weaker message, and the negative matrix covers only the missing build. One symmetric `is_file()`
check plus one parametrized negative closes it.

### R6-3 · Low · Whole-tree walk before filtering
`root.rglob("*.sbt")` walks the entire paired checkout — `target/`, `.git/`, `website/node_modules/`
— and discards `target` paths only after the walk. Results are correct (`pathlib` does not follow
directory symlinks, so no cycles) and this runs once per adaptation, but pruning during the walk
(`os.walk` with an in-place `dirs` filter) avoids statting build output and vendored packages.

### R6-4 · Low · Forked-probe assertions print "requirement failed"
`CodegenDiscoveryProbe` has eight `require(...)` calls with no message — the four generated-file
existence checks and the four `testClasses`/`allClasses` membership checks. The suite does surface
the whole log, but in a forked JVM that log is the only evidence, and two neighbouring `require`s
already carry messages. Matching them is cheap and pays for itself on the first CI failure.

### R6-5 · Low · Documentation durability
- `tools/ci/README.md` pins "all four `Fine-tune`/`Phi Model` notebooks". Exactly four match today
  (`Fine-tune a Text Classifier`, `Fine-tune a Vision Classifier`, `Apply Phi Model with HuggingFace
  CausalLM`, `End-to-end Local RAG with Phi Model`), so the corrected count is accurate. But both
  selectors are substring markers — `DatabricksUtilities.scala:279` and `GPU_NOTEBOOK_MARKERS`
  (`databricks_impact.py:76`) — and this line was just corrected from "three". Phrase it by marker.
- Same file: a sentence breaks mid-phrase ("A missing\nreferenced helper, ..."). Reflow.
- `branch-spark4-common.md` "Portable sync lessons" introduces its bullets as fixes #2719 carries,
  but the Fabric provisioning bullet is a process lesson with no code in #2719. Split or retitle so
  a reader does not look for a Fabric change in the PR.

### R6-6 · Low · `reviews/` is not ignored
`git check-ignore` exits 1 in this worktree, so `git add -A` would commit all six review artifacts
into PR #2719. Stage explicitly, or add an ignore rule.

## Clean on this theme

- **Cost of the new matching.** Two to three `Pattern.compile` calls and one non-connecting
  `JarURLConnection` per candidate class, and only after the `ClassTag` filter — hundreds of classes,
  not the full `AllClasses` list. No measurable codegen impact; hoisting the two constant regexes to
  `private val`s is optional style, not a fix.
- **Test package layout.** No `__init__.py` exists anywhere under any module's `src/test/python`
  (glob: zero matches), so the new `synapsemltest/io/http/` correctly has none — R3's "all siblings
  have one" note was inaccurate; do not add one. No duplicate test basenames, so pytest rootdir
  import collisions do not apply, and `__pycache__` is ignored.
- **Pin plumbing.** `${1:-environment.yml}` is CWD-relative and the new test pins it with
  `cwd=REPO_ROOT`; `pipeline.yaml:418` and the demo README both invoke it from the root.

## Portability for the planned merge into both sync PRs

Read-only comparison against `sync-spark40-20260916` and `sync-spark41-20260916`. Neither port was
modified, validated, or re-run; no old CI result is treated as current.

| File | Port delta | Merge assessment |
| --- | --- | --- |
| `Wrappable.scala` | identical on both | no-op |
| `JarLoadingUtils.scala` | +3 lines, all comments | comment-only; no behavior drift |
| `patch_internal_typing_support.py` | +98 / −0 | clean addition, no port-side edits |
| `test_http_package.py` | +4 / −1 (identity vs `is not None`) | take master; strictly stronger |
| `test_package_exports.py` | +11 / −0 dynamic loop | safe even if a port keeps its initializer: all four public `*Model.py` stems are in the port `__all__` |
| `get_python_version.sh` | spark4.1 identical; spark4.0 +2 / −1 | spark4.0 pins `3.12.11`, still matched by the relaxed form |
| `CodegenDiscoverySuite.scala` | +72 / −32; ports have the same six tests but no `CodegenDiscoveryLauncher` | the one real conflict; master is a superset, but the launcher exists for a Spark 3 shaded-Guava loader shape, so re-run it on each port rather than assuming |
| `recommendation/__init__.py` deletion | ports still carry it, listing all nine names | portable only if each port's regenerated package init exports all nine; verify per port with a regenerated wheel |

No changed file names a Spark, Scala, or Java version or a version-bearing path, so the merge needs
no per-port adaptation beyond the two rows above.

## Limits

Source review plus one read-only JDK 17 probe written to `%TEMP%` and deleted. No build, test,
codegen, lint, install, network, cloud call, or git mutation in this round; no subagent or factory;
no source edit. Earlier rounds' Scala/style/codegen, helper/parser, and installed-wheel results are
prior evidence, not re-verified here, and the driver's final full helper/Black rerun was still in
progress and is not claimed. Private-branch work and Spark 4.0 model validation are out of scope.
This is not a readiness claim: exact-head CI, dependency resolution, and Fabric quota remain open.
Only this artifact was written. Review stops after round 6.

## Driver resolutions

- R6-1: `matchesJar` now wraps malformed resource failures in `IOException`
  with both the resource URL and requested artifact, preserving the original
  cause. The new malformed-file and opaque-archive regression passed.
- R6-2: Added the explicit missing-plugin check and its negative test.
- R6-3: The build-source scan now uses a pruned directory walk. Generated
  output, Git metadata, virtual environments, and dependency directories are
  excluded before descent. Added exclusion regressions.
- R6-4: Added diagnostic messages to the forked probe's output and discovery
  assertions.
- R6-5: Describe the complete marker-selected GPU notebook set, reflow the
  helper guidance, and distinguish Fabric quota diagnosis from a code fix.
- R6-6: Review artifacts are intentionally versioned under the required
  review contract. They will be staged explicitly with the reviewed files.

The final master run passed aggregate compile and test compile, all 28 targeted
tests, aggregate main/test Scala style, and all-module codegen on JDK 11.
The Python run passed all 164 CI-helper tests, pinned Black across 205 files,
and the actual legacy-layout adapter check. These are local results, not
exact-head CI or downstream packaging proof. Downstream validation remains open.
