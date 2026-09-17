# Round 1 review

## Review summary

| Field | Result |
| --- | --- |
| Task | sync-merge-ready-20260916, attempt 1 |
| Round / mode | 1 of 6, sequential |
| Model | GPT-6 Astra, `gpt-6-astra` |
| Theme | Broad correctness, security-conscious review, logic errors, requirement conformance |
| Target | Public SynapseML `master`, PR #2719, as supplied in the request |
| Issues found | 4: two fail-closed logic defects, one regression-test gap, one formatting defect |
| Verdict | ISSUES_FOUND |
| Readiness | Not established. This artifact is neither a completed gauntlet nor exact-head CI approval. |
| Artifact | `reviews\task-sync-merge-ready-20260916-attempt-1-review-1-gpt-6-astra.md` |

The catalog resolves the sequential rotation to `gpt-6-astra`,
`gemini-3.8-flash`, and `claude-opus-5`. Only round 1 ran, directly in the
current GPT-6 Astra session. No other agents or factory were launched.

## Exact scope

Worktree: `C:\Users\singhrana\Documents\SynapseML\.worktrees\branch-context-20260916`

Local branch: `docs/spark4-branch-context-20260916`.
HEAD: `e97b63c43ce194c8128fceac7e866463864745af`.
The review covers only the current uncommitted changes relative to that HEAD:
12 modified tracked files and three untracked source files. Nothing was staged.
Existing review artifacts and old committed changes were excluded. Unchanged
code was read only to trace callers, build configuration, and test execution.

| State | File relative to this worktree |
| --- | --- |
| Modified | `.github\skills\synapseml-branches\references\branch-spark3p5.md` |
| Modified | `.github\skills\synapseml-branches\references\branch-spark4-common.md` |
| Modified | `cognitive\src\test\python\synapsemltest\services\openai\test_OpenAIResponseSchema.py` |
| Modified | `core\src\main\python\synapse\ml\recommendation\__init__.py` |
| Modified | `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala` |
| Modified | `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala` |
| Modified | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala` |
| Modified | `tools\ci\README.md` |
| Modified | `tools\ci\get_python_version.sh` |
| Modified | `tools\ci\patch_internal_typing_support.py` |
| Modified | `tools\ci\tests\test_patch_internal_typing_support.py` |
| Modified | `tools\ci\tests\test_python_version.py` |
| Untracked | `core\src\test\python\synapsemltest\io\http\test_http_package.py` |
| Untracked | `core\src\test\python\synapsemltest\recommendation\test_package_exports.py` |
| Untracked | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala` |

The source manifest SHA-256 is
`032df1b4324e92fa7036aebcc9270dae9e151a1778932ae9a65a04415d0484c0`.
It hashes the PowerShell-sorted Git-relative `path<TAB>file-sha256` entries,
joined with LF and no trailing newline. The manifest covers the 15 files above
and was unchanged when rechecked at 2026-09-16T14:49:09-07:00.

## Issues

### R1-P1: Missing-helper detection omits SBT meta-build files

**Severity:** Medium. **Type:** Fail-closed logic defect.
**File:** `tools\ci\patch_internal_typing_support.py:83-85`.

`build_sources` includes root `*.sbt` files and `project\*.scala`, but omits
`project\*.sbt`. Those files are SBT build definitions too, including the usual
`project\plugins.sbt`. A legacy-looking plugin therefore causes acceptance even
when a meta-build file references the absent typing helper.

An in-memory probe executed the current `validate_legacy_codegen` function,
using the fixture text already present in the public helper tests. Adding the
helper reference to `project\plugins.sbt` was accepted. The same reference in
root `build.sbt` or `project\Other.scala` was rejected. Acceptance reaches the
success return in `main` at line 111.

This violates the requested rejection of a referenced missing helper. It does
not disable later packaging, but it removes the intended early failure and
misidentifies the checkout as requiring no adaptation.

**Suggested fix:** Inspect the relevant SBT meta-build sources as well as the
currently covered files. Add a negative CLI test with the missing-helper
reference in `project\plugins.sbt`, requiring exit code 2.

### R1-P2: Commented markers can identify an unknown pipeline as supported

**Severity:** Medium. **Type:** Fail-closed logic defect.
**File:** `tools\ci\patch_internal_typing_support.py:86-96`.

The direct-codegen check searches three substrings without distinguishing
active Scala code from comments. A plugin whose only direct-OSS markers are
commented out still passes even if its active package task uses a different
generator.

The actual validator accepted a synthetic plugin containing the three known
markers in `//` comments and an active `packagePython := customCodegen.value`
task. It correctly rejected that custom task when the comments were absent.
This is a supported-layout false positive, not evidence from private source.

**Suggested fix:** Make recognition comment-aware and require the known
direct-codegen/package-task relationship in active code. Add an unknown-layout
negative with obsolete markers retained in comments. Do not replace rejection
with a warning or skip packaging.

### R1-P3: The HTTP export guard passes when the imported symbol is a module

**Severity:** Medium. **Type:** Regression-test coverage gap.
**File:** `core\src\test\python\synapsemltest\io\http\test_http_package.py:4-9`.

`from package import HTTPTransformer` can resolve the `HTTPTransformer`
submodule when the package does not re-export its class. A module is not
`None`, so the new assertion does not establish that the public import returns
the generated class.

The unchanged test body was executed against an in-memory package with
`http_udf`, an available `HTTPTransformer` submodule, and no class re-export.
The test passed. Its imported `HTTPTransformer` was a module and was not the
class defined inside that module. No Spark, filesystem fixture, or network
access was involved.

**Suggested fix:** Assert that the package export is the actual class from
`synapse.ml.io.http.HTTPTransformer`, or make an equivalent class/identity
assertion. Keep the manual-function assertion. The missing-class-export
mutation must fail.

### R1-P4: A newly added test statement fails the pinned Black check

**Severity:** Low. **Type:** Formatting defect that blocks the style gate.
**File:** `tools\ci\tests\test_patch_internal_typing_support.py:94-96`.

Black 22.3.0 collapses the three-line call to:

```python
    (tmp_path / "build.sbt").write_text(reference, encoding="utf-8")
```

This was verified with the installed pinned formatter through its in-memory
API, using Black's own newline decoder. Of all seven changed Python files,
only this file requires formatting. No cache or source file was written.
The statement is newly added, not a committed baseline formatting problem.
The existing checks in `pipeline.yaml:192-193` and
`.github\workflows\pr-validation.yml:24-25` use this formatter pin.

**Suggested fix:** Apply the pinned formatter to this test file. No formatter
upgrade, dependency change, or workflow edit is needed.

## Requirement-to-test coverage

| Requirement | Current evidence | Assessment |
| --- | --- | --- |
| Exact main/test JAR identity, snapshot aliases, decoded URL delimiters | `JarLoadingUtils.matchesJar`; discovery-suite tests at lines 75, 89, 119, and 135 cover target directories, spaces, exclamation marks, classifiers, near-version names, and other modules | No additional defect found in the JAR matching changes. |
| Exploded classpaths retain module and main/test boundaries | Discovery-suite test at line 104 checks both Scala-qualified and plain target output directories, opposite output types, and a different module | Covered for the intended module/output cases. |
| Real packaged discovery, including SBT-like loader behavior | `CodegenDiscoveryLauncher`, `CodegenDiscoveryProbe`, and the test at line 184 build separate JARs, remove the original core class directories, collect URL-loader dependencies, and run a bounded child JVM | Positive Python, stub, and R outputs are required; test fixtures are excluded. Explicit test-JAR and unfiltered discovery remain covered, including constructor failure propagation. |
| Foreign-owned Python defaults, including stub generation | `Wrappable.scala:109-116,149-168,315-328`; `PyCodegenSuite` checks that Spark rejects the foreign parameter, then generates and compiles both runtime and stub files | All three Python default-retrieval paths use the guard. Existing abstract-trait-state and normal-wrapper tests provide compatibility coverage. No public signature or serialized parameter change was found. |
| Portable generated OpenAI stub lookup | `test_OpenAIResponseSchema.py:74-107` follows the public class MRO and checks the selected generated class's stub against public runtime methods | Correctly avoids a fixed master-versus-port stub layout. The full current Python/Spark test was not run by this review. |
| Minor-series Python pins without inventing patch versions | `get_python_version.sh:18-24`; `test_python_version.py` covers exact/minor pins, malformed and range forms, duplicates, and the actual repository pin | Matches the requested numeric minor-series behavior. PyYAML is already declared in `environment.yml`. |
| Strict legacy adapter recognition without disabling packaging | `test_patch_internal_typing_support.py` covers the supported missing-helper layout, path/module references, unknown layout, misspelled path, and missing build file | Incomplete for R1-P1 and R1-P2. The existing present-helper/idempotence path remains unchanged. |
| Generated and manual package exports | HTTP guard plus recommendation wildcard-import guard; recommendation initializer adds all three missing model imports and `__all__` entries | The recommendation change addresses the specified missing names. The HTTP class assertion needs R1-P3. The repository's `testPython` task invokes pytest, so these function-style tests are discoverable. |
| Accurate branch/CI guidance and public/private separation | All three documentation diffs; GPU notebook names checked against `DatabricksUtilitiesSuite`; proposed fixes distinguished from pinned target snapshots | Added text stays at generic, already-public compatibility concepts and paths. No private source snippets or private review findings are included here. |
| No runtime-version, dependency-pin, pipeline/workflow changes or test skipping | Complete current diff and untracked-source inventory | No such changes were found. Existing configuration and committed test behavior were not treated as new changes. |

## Evidence checklist

- [x] Read this worktree's `AGENTS.md`, branch context, exact tracked diff, and all three untracked source files. No child `AGENTS.md` was found.
- [x] Checked live baseline values: Spark 3.5.0 and Scala 2.12.17 in `build.sbt`, Python 3.11.8 in `environment.yml`, and JDK 11 in the local build evidence and existing workflow.
- [x] Traced discovery through both Python and R codegen and checked the existing pytest runner.
- [x] Executed the helper and HTTP counterexamples entirely in memory; parsed all seven changed Python sources; checked all seven with Black 22.3.0 without filesystem writes.
- [x] Rechecked the source manifest. Source, index, and branch state were not changed by this review.
- [ ] No fresh exact-head remote CI, complete Python/Spark regression run, or port integration was performed. These are outside the read-only round.

## Existing evidence and readiness blockers

The following logs were read from the supplied session evidence directory.
They are local-run evidence, not remote CI approval.

| Evidence | What it establishes |
| --- | --- |
| `master-upstream-codegen-red.log` | Reproduces the foreign-parameter ownership exception and snapshot/exploded matching failures on the master-based reproduction. It also includes a historical child-classpath failure; those failures are not reported as current defects. |
| `master-upstream-codegen-green3.log` | 27 tests passed across three suites. The composite command then failed the old null-use style check. It did not complete all later requested tasks. |
| `master-upstream-codegen-green4.log` | By the review's final evidence read, the run had completed: 6 discovery tests passed, both core style tasks found zero errors, and aggregate `compile`, `Test/compile`, and `codegen` succeeded. The final success is at line 1294, 2026-09-16 21:43:30 UTC. This run selected six tests, not all 27 from green3. |
| User-supplied CI-helper result | 143 tests passed before two additional negatives were added. That count is not proof that the complete current helper suite passed. |

R1-P1 through R1-P4 remain open. No finding was fixed in this read-only round.
The completed local build does not cover those Python helper/test defects or
establish final wheel-import and exact-head compatibility readiness.

The request reports that port PRs #2718 and #2720 still use their prior pushed
heads and do not yet contain this follow-up. Their earlier checks cannot
validate this source snapshot. No remote state was queried or changed.

The reported Fabric warehouse quota failure, followed by retry-masked name
conflicts, is an infrastructure blocker rather than a reason to change naming
logic. No shared-resource deletion or other cloud action was attempted.

## Resolution log

| Finding | Status | Resolution |
| --- | --- | --- |
| R1-P1 | Open | Review only; meta-build reference rejection needs a source fix and negative test. |
| R1-P2 | Open | Review only; active-code recognition needs a source fix and negative test. |
| R1-P3 | Open | Review only; strengthen the class-export assertion and prove the mutation fails. |
| R1-P4 | Open | Review only; apply Black 22.3.0 to the changed helper test. |

Only this review artifact was written in this worktree. No builds, dependency
installs, Spark sessions, network/cloud calls, staging, commits, pushes,
subagents, factories, or later review rounds were initiated.

## Driver resolutions

- R1-P1: The adapter now checks recursive SBT/Scala meta-build sources, excluding
  generated target directories. Negative tests cover `project/plugins.sbt` and
  a nested meta-build Scala file.
- R1-P2: Recognition now requires active direct-codegen and packaging task
  definitions. A bounded scanner removes line/nested block comments and
  multiline documentation strings while retaining ordinary quoted arguments.
  Tests cover commented tasks, quoted task examples, valid URLs/character
  literals/quoted multiline strings, and unterminated constructs. The actual
  paired legacy checkout is recognized without creating a helper.
- R1-P3: The HTTP package export must be identical to the generated class,
  rather than merely non-null. The manual-function assertion remains.
- R1-P4: Applied pinned Black 22.3.0 to the changed Python files.

The focused helper/parser run passed 30 tests, and the strengthened generated
package export checks passed two tests. These local results resolve the four
code findings, not the separate pending exact-head CI and platform gates.
