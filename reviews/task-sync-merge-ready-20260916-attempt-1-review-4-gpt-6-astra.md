# Round 4 review

## Review summary

- Model: GPT-6 Astra, `gpt-6-astra`.
- Round/mode: 4 only, sequential. Theme: detailed correctness, data flow, type safety and boundaries.
- Verdict: **ISSUES_FOUND**, two concrete correctness defects. Neither was changed in this review.
- Artifact: `reviews\task-sync-merge-ready-20260916-attempt-1-review-4-gpt-6-astra.md`.

## Exact scope

Checkout: repository root on `docs/spark4-branch-context-20260916`.
Target supplied by the driver: public `master`, PR #2719.
HEAD: `e97b63c43ce194c8128fceac7e866463864745af`.
Reviewed the current uncommitted follow-up: 11 modified files, one deletion and three untracked source files.
The index was empty. Earlier reviews and appended dispositions were evidence, not additional code scope.

| State | Worktree-relative path |
| --- | --- |
| Modified | `.github\skills\synapseml-branches\references\branch-spark3p5.md` |
| Modified | `.github\skills\synapseml-branches\references\branch-spark4-common.md` |
| Modified | `cognitive\src\test\python\synapsemltest\services\openai\test_OpenAIResponseSchema.py` |
| Deleted | `core\src\main\python\synapse\ml\recommendation\__init__.py` |
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

Source manifest SHA-256: `460b6e0eb08d05123a29072f1e357ad31b78305af53fe5f9054b1278ca8107e9`.
This hashes PowerShell-sorted Git-relative `path<TAB>file-sha256` rows, joined with LF without a final newline.
The deleted file uses `DELETED` instead of a hash. Source fingerprints and HEAD were rechecked unchanged.

## Findings

### R4-P1: The model-export loop requires non-public generated wrappers

**Severity: Medium.** `core\src\test\python\synapsemltest\recommendation\test_package_exports.py:21-23`.

`glob("*Model.py")` also selects `_SARModel.py` and `_RankingTrainValidationSplitModel.py`.
These are required generated implementation modules: `SARModel.scala:33` and
`RankingTrainValidationSplit.scala:280` enable `pyInternalWrapper`, and the corresponding
handwritten public model modules import those generated bases.
`PyCodegen.scala:125` deliberately excludes underscore-prefixed modules from package imports.
With the redundant initializer removed, Python's wildcard import also excludes underscore-prefixed names.
Consequently `namespace[path.stem]` raises `KeyError` for a normal, correctly generated package.

The unchanged test function was executed in memory against public model exports and both generated
underscore modules. It failed with `KeyError: '_RankingTrainValidationSplitModel'`; the public-only
control passed. No wheel, Spark import or filesystem fixture was used.

**Suggested fix:** Filter out underscore-prefixed module stems before the dynamic identity checks.
Keep the three original missing-name assertions and exact public class identity checks.
Do not export implementation wrappers or restore a handwritten initializer to satisfy this test.

### R4-P2: Legacy recognition can take its CodeGen call from a different task

**Severity: Medium.** `tools\ci\patch_internal_typing_support.py:144-154`, especially line 146.

The search stops only at a line beginning with `}.value)` after optional whitespace.
A no-op `codegen` ending with `} .value),`, or closing on its declaration line, bypasses that boundary.
The lazy match then consumes a following `otherTask` and accepts its direct `CodeGen` call.
Meanwhile `packagePython := { codegen.value }` still invokes the no-op task, not `otherTask`.
The adapter therefore reports the known direct-codegen layout for an unrelated task arrangement.

An in-memory execution of the actual validator accepted both spaced and inline closing variants.
The supported direct-task control passed; the same unrelated call after a canonical closing line
was correctly rejected. The difference is task-boundary recognition, not comments or private source.

**Suggested fix:** Bound recognition to the matched `codegen` task's balanced delimiters rather
than one closing-line spelling. Add spaced and same-line closing negatives with the only
`CodeGen` invocation in another task. Preserve explicit rejection and downstream packaging/tests.

## Evidence checklist

- [x] Read `AGENTS.md`, branch guidance, the complete tracked diff and all untracked sources; checked rounds 1-3 dispositions against current code.
- [x] Traced JAR container decoding, snapshot normalization, main/test classifiers, SBT module/output matching and discovery callers. Malformed URLs still fail explicitly; no skipped-API fallback is requested.
- [x] Read the real-JAR regression through classpath collection, platform-parent URL loader, reflection, fixture provenance, constructor-error assertion and cleanup. Scala binary version is derived rather than pinned.
- [x] Traced all three Python default lookups through typed `Option[T]` handling. Runtime/stub defaults stay consistent; no public JVM signature changed. R behavior remains outside this Python fix.
- [x] Checked OpenAI MRO-based stub selection, HTTP class identity, generated recommendation imports and initializer preservation. The deletion is appropriate; R4-P1 is in its new guard.
- [x] Checked numeric minor/patch pin parsing and Docker consumers, recursive SBT/meta-build reference rejection, lexer indices and error propagation. R4-P2 remains after the earlier lexer fixes.
- [x] Checked documentation against public source, including all four GPU notebooks and the distinction between proposed follow-ups and pinned target snapshots.

## Evidence limits

The counterexamples ran with `python -B` and memory-only fixtures. No build, project import,
dependency install, network/cloud call, git mutation, source edit, subagent or factory was used.
Reported Scala/helper/style and component-wheel results are prior evidence, not fresh validation here.
The final Scala/style/codegen run was not classified; regenerated wheels after initializer removal remain unverified.
Sibling sync worktrees were not changed or validated. Exact-head CI, dependencies and Fabric quota remain open.
Only this review artifact was written in this worktree. No readiness claim or later review round.

## Driver resolutions

- R4-P1: Skip private underscore-prefixed model modules in the dynamic export
  guard. The three missing public names and all public model identities remain
  asserted. Generated-package checks pass without exporting private wrappers.
- R4-P2: Replaced the cross-task search with recognition of the complete known
  direct task body. Only whitespace/comments may separate its statements and
  braces; another task's invocation cannot satisfy it. Negative tests cover
  spaced and inline task endings with the direct call in another task.
  The real paired legacy checkout still passes recognition.

The focused helper/parser run now passes 33 tests. Both generated export tests
pass after removing the redundant initializer. Regenerated installed wheels
pass all three metadata tests and three OpenAI stage subtests.
