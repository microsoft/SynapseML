# Spark 4.0 master sync, attempt 1, round 1

## Review summary

- **Round:** 1 only.
- **Theme:** Broad sweep. Correctness, security-conscious logic, and requirements.
- **Mode:** Sequential.
- **Model:** `gpt-6-astra`, maximum reasoning requested.
- **Issues found:** 1 merge-integration regression.
- **Verdict:** ISSUES_FOUND.
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-1-gpt-6-astra.md`.

The merge preserves the existing Spark 4.0 adaptation hunks, but the new typing
generator introduces another default-lookup path that bypasses the branch's
guard. It can abort Python package generation for parameters the runtime-wrapper
generator already tolerates. The review concentrated on the 34 incoming files
that differ from master, the conflict resolutions, and their callers, tests,
and packaging. The other 620 incoming files were checked for preservation,
not independently re-reviewed as upstream feature changes.

No agents were launched. No product, documentation, configuration, or test
source was changed by this reviewer. No staging, commits, pushes, or CI requests
were performed. Fixes and later rounds belong to the parent.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark40-20260916` |
| Branch | `sync/spark4.0-master-20260916` |
| Target and HEAD | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` |
| Incoming master and MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Actual graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Previously integrated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Index tree from `git write-tree` | `306e08e24ddb515ba14cabdab79fc6376836e62c` |
| SHA-256 of raw `git ls-files --stage -z` output | `b4b86085a8729a3a29aea6cf42b26d3daf037ec2ef53fb52e97c9b0d9824aa13` |
| Working residual manifest SHA-256 | `33dbe6548b8bb00713e23613d03a8aacd32ad2a2e85e41991ec592e3124fe24d` |
| Final source check | `2026-09-16T08:40:11Z` |

There were no unmerged entries or unstaged tracked changes. Tracked source did
not change between the initial snapshot at 08:27Z and the final source check.
The index tree preserves the reviewed tracked text. The residual manifest
covers all 172 tracked paths differing from incoming master. It hashes the
compact UTF-8 JSON array of `[git-path, SHA-256-of-working-bytes]` pairs in
`git diff --name-only -z <master>` order.

The running owner validation had created two untracked files under
`lightgbm\.synapseml-lightgbm-validation-spool-11602389218522523802`.
Their contents were not reviewed, staged, modified, or removed. They are not
part of this source verdict.

## Evidence checklist

- [x] Read this worktree's `AGENTS.md` and the Spark 4.0 and shared branch
  references. Applied the project code-review and branch guidance, the PR-loop
  evidence standard, and the Round 1 review prompt. `AGENTS.md`,
  `CONTRIBUTING.md`, `.github\skills\code-review\SKILL.md`, and
  `.github\skills\synapseml-branches\SKILL.md` match the pinned master bytes.
- [x] Verified both merge inputs and the real `MERGE_HEAD`. Used the recorded
  content baseline as well as the much older graph base. An ancestry-only
  claim that all intervening master commits are missing would be incorrect.
- [x] Independently recomputed 654 incoming changed paths, 620 matching
  master, and 34 residuals. No changed path outside that incoming set was
  introduced by this merge. The supplied conflict classification contains
  40 exact-master, 19 unchanged-port, and 17 combined resolutions.
- [x] Compared zero-context edit bodies for each residual against
  `a6fd536ad7 -> HEAD` and `master -> working`. The port edits predate this sync
  except the combination with master's expanded Fabric condition and its new
  corresponding test. This establishes provenance instead of assuming that a
  surprising port difference is either new or harmless.
- [x] Read `build.sbt` and `environment.yml`. Confirmed Spark 4.0.1, Scala
  2.13.16, JDK 17, Python 3.12.11, NumPy 1.26.4, cp312 wheels, PyArrow 18.0.0,
  MLflow 2.21.3, sparklyr 1.9.5, and R 4.4. `environment.yml` is byte-identical
  to the target, not a new dependency-policy change.
- [x] Reviewed the workflow, Docker, pipeline, and publish residuals together.
  The setup-java action update to v6.0.1 retains JDK 17. Removed CMS flags stay
  removed. Publishing keeps master's `sbt_retry.sh update` and the port's
  `packagePython verifyPythonPackaging`. The prerequisites file matches master.
- [x] Checked `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\DatabricksUtilities.scala`
  and the pipeline selection. DBR 17.3 CPU/GPU runtime strings, shared
  `synapseml-build-14.3-gpu`, and consolidated `DatabricksGPUTests` remain.
  Fabric remains unconditionally disabled. No runtime or trigger enablement
  was smuggled into the resolution.
- [x] Reviewed `test_pipeline_yaml.py`'s new Fabric test against the pipeline.
  It asserts `condition is False`, rather than treating a YAML boolean as a
  string, and retains the new Key Vault/template checks. The test was not
  skipped. The import union retains both upstream and branch-specific tests.
- [x] Traced the combined `CognitiveServiceBase.scala` parameter/authentication
  path. `asImmutableCollection` still normalizes column-backed collections.
  Upstream credential precedence, lazy Fabric fallback, null-safe header
  persistence, auth-marker filtering, and fallback-only retry marking remain.
  The merge did not add a credential value or remove those checks.
- [x] Reviewed the OpenAI request and response residuals with their surrounding
  transform/validation code. Collection conversions remain on the message and
  response-array boundaries while upstream schema-only response formatting,
  structured multimodal handling, and authentication retry code are retained.
- [x] Traced `PyCodegen.scala -> Wrappable.makePyFile`,
  `PythonInitMerger.preserve`, and `project\CodegenPlugin.scala` packaging.
  `makeInitStub` runs before the port's safe directory recursion.
  Generated `.pyi` files, typing markers, package data, and manual-initializer
  preservation coexist. Runtime defaults still use `safeGetDefault`.
- [x] Continued Round 1 inspection found that the new `pyStubParamArgs` does
  not use that guard. The owner's subsequent Spark 4.1 regression log fails
  at the exact call still present in this worktree. Verified this method is
  incoming from master and absent from the pre-merge target; see Issue 1.
- [x] Checked `OpenAIPrompt.scala`, its Python override generator, and the
  public Python-wrapper arrangement. `_OpenAIPrompt` and zero-argument
  `super().clear` / `super().copy` survive.
- [x] Checked the unchanged SAR production files directly. `SAR.scala`
  uses `Seq[SAR.ItemAffinity]` and explicitly named `itemIndex` / `affinity`
  struct fields. `SARModel.scala` retains the qualified
  `sarUserFactors.flatList` join expression.
- [x] Verified both HTTP-source files retain the Spark 4.0 streaming namespace.
  Both Petastorm compatibility files and the serialized Horovod compatibility
  path remain. R ANSI settings, `SPARK_HOME`, and nested-stage JVM loading
  remain in the branch's codegen and pipeline.
- [x] Reviewed the LightGBM and VW residual hunks alongside incoming changes.
  The surviving differences are the existing Scala/Spark API conversions and
  guarded VW zero-denominator statistics, not reversions of the incoming
  streaming, ranking, or validation changes.
- [x] Confirmed `website\blog\overview.md` matches incoming master exactly and
  contains one `{/* truncate */}` separator. Inspected the celebrity-quote
  notebook's source-cell delta rather than dumping execution outputs.
- [x] Ran `git diff --check HEAD` and the supplied read-only
  `audit-sync.py <exact-worktree> spark4.0`. Both succeeded.

### Exact diff basis

All native Git calls used the supplied executable, process-local PATH, optional
index locking disabled, and `-C` pointing to this worktree. The comparisons used
were:

```powershell
$env:PATH = 'C:\Users\singhrana\AppData\Local\GitHubDesktop\app-3.6.4\resources\app\git\cmd;' + $env:PATH
$env:GIT_OPTIONAL_LOCKS = '0'
$git = 'C:\Users\singhrana\AppData\Local\GitHubDesktop\app-3.6.4\resources\app\git\cmd\git.exe'
$wt = 'C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark40-20260916'
$master = '1305587a4afe92d27c8e28894b90e38020252e04'
& $git --no-pager -C $wt diff --name-only a6fd536ad7 $master
& $git --no-pager -C $wt diff --name-status --no-renames $master
& $git --no-pager -C $wt diff --shortstat HEAD
& $git --no-pager -C $wt diff --check HEAD
& $git --no-pager -C $wt diff --name-only --diff-filter=U
& $git --no-pager -C $wt write-tree
```

Semantic diff reads used `--unified=3`, `--unified=4`, or `--unified=5` for the
selected groups. The per-file provenance comparison used
`diff --no-ext-diff --unified=0 <master> -- <path>` and
`diff --no-ext-diff --unified=0 a6fd536ad7 HEAD -- <path>` for every residual.
Source-context reads and the actual `HEAD -> working` delta were used where
the combination required them.

### Incoming residual files reviewed

```text
.github\workflows\pr-validation.yml
build.sbt
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\CognitiveServiceBase.scala
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\language\AnalyzeText.scala
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAI.scala
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIChatCompletion.scala
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIPrompt.scala
cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIResponses.scala
cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\form\FormRecognizerV3Suite.scala
cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\language\AnalyzeTextLROSuite.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegen.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\ParamsStringBuilder.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\featurize\CleanMissingData.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\param\UntypedArrayParam.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\stages\Repartition.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\Secrets.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\RTestGen.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\core\test\fuzzing\Fuzzing.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\featurize\VerifyDataConversion.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\param\VerifyJsonEncodableParam.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\train\VerifyTrainClassifier.scala
deep-learning\src\main\python\synapse\ml\dl\LitDeepVisionModel.py
docs\Explore Algorithms\AI Services\Quickstart - Analyze Celebrity Quotes.ipynb
docs\Reference\R Setup.md
lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\LightGBMBase.scala
lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\booster\LightGBMBooster.scala
pipeline.yaml
templates\publish.yml
tools\ci\tests\test_pipeline_yaml.py
tools\docker\demo\Dockerfile
tools\docker\minimal\Dockerfile
vw\src\main\scala\com\microsoft\azure\synapse\ml\vw\VowpalWabbitBaseLearner.scala
website\doctest.py
```

## Issues

### Issue 1: New Python stub generation bypasses the Spark 4 default guard

- **Severity:** Medium.
- **File:** `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`.
- **Lines:** 315-327, specifically line 317.
- **Description:** `pyStubParamArgs` evaluates `thisStage.getDefault(p)`
  directly. Spark 4 rejects a foreign-owned parameter with
  `IllegalArgumentException`. The existing `pyParamArg` and `pyParamDefault`
  deliberately use `safeGetDefault` to handle this case. `makePyFile` now
  generates both runtime Python and a stub, so the unguarded new stub lookup
  aborts generation even when the runtime wrapper was generated successfully.
- **Concrete case:** A `Wrappable` stage exposes
  `new Param[String]("otherStage", "text", "text value")`. Its runtime wrapper
  emits `text=None` through `safeGetDefault`, but `pyStubParamArgs` throws
  instead of emitting `text: Optional[str] = ...`.
- **Evidence:** The owner's
  `spark41\stub-regression-red.log:24-40` records the dedicated foreign-parameter
  regression failing with
  `Param otherStage__text does not belong to foreignParamPythonStage`,
  through `Params.getDefault` and `PythonWrappable.pyStubParamArgs` at line 317.
  At that failing Spark 4.1 snapshot, the complete `Wrappable.scala` file was
  byte-identical to this Spark 4.0 file, SHA-256
  `584f1cfb8e289557bf47268efc67010095c606fa55c293111b0617dd5b3f1aee`.
  This reviewer did not rerun that fixture on Spark 4.0; the source defect is
  the unguarded call under the branch's explicitly preserved rejection contract.
- **Regression provenance:** `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57`
  already contains `safeGetDefault` but has no `pyStubParamArgs` method.
  Master `1305587a4afe92d27c8e28894b90e38020252e04` introduces that method
  with the direct lookup. This is a new interaction between incoming typing
  generation and existing Spark 4 behavior, not an old target defect.
- **Impact:** Codegen/package generation fails for this previously tolerated
  parameter shape. The old guard remaining in the file is insufficient to
  preserve the behavior across the new stub path.
- **Suggested fix:** Use `safeGetDefault(p)` in `pyStubParamArgs`, and carry
  the foreign-parameter runtime-and-stub regression into Spark 4.0. Confirm
  the generated `.py` and `.pyi` outputs and relevant packaging path. The
  Spark 4.1 owner has made the corresponding one-line correction there;
  it was not present in this Spark 4.0 source at completion of the inspection.

Reviewed source excerpt:

```scala
  private def pyStubParamArgs(p: Param[_]): Seq[String] = {
    val pyiType = getPythonTypeInfo(p).pyiType
    (p, thisStage.getDefault(p)) match {
```

The sibling Spark 4.1 review records a separate, proven mismatch between the
stated SAR invariant and that target's existing implementation. It is not a
Spark 4.0 defect.

## Validation limitations

This reviewer ran read-only source/preservation checks, not another build or
Spark job. Owner-produced logs in the session `files` directory were inspected:
`spark40-ci-fix-black.log` records 77 pipeline tests passing and 214 Python
files unchanged by Black; `spark40-targeted.log` records 35 PyCodegen/RCodegen
tests and 89 selected OpenAI tests passing. These observations do not certify
the final packaged jars or the rest of the running validation.

The initial 35-test codegen/R pass did not include the newly added
foreign-parameter fixture. It must not be used to dismiss Issue 1. The failed
fixture was observed in the owner's Spark 4.1 log, not executed by this reviewer.

Full Scala/native/offline completion, Python packaging/codegen execution,
Databricks, R runtime, and final PR-head Azure evidence were not all complete
or independently established in this review. Fabric remains intentionally
disabled. The earlier Windows shell-path failure is not used as evidence of
a product defect. No pending gate is counted as a passing gate or a code bug.
Rounds 2 through 6 were not run.

## Resolution log

Issue 1 is open. No fix was applied by this reviewer.

The initial artifact draft recorded zero established issues and a scoped CLEAN
assessment. Before completing Round 1, the final source check detected the
owner's changed Spark 4.1 `Wrappable.scala`. Inspection of that one-line change,
the red regression trace, and this unchanged Spark 4.0 source established
Issue 1. The ISSUES_FOUND verdict above supersedes that preliminary assessment;
no product source or original reviewed source hash was changed.

Preserve this completed Round 1 review. The parent owns fixes, subsequent
resolution notes, and later rounds.

## Supplemental wrapper-smoke evidence, 2026-09-16

At the parent's request, a bounded follow-up inspected the completed local
results and generated wrappers. It did not run another Spark job or advance
to Round 2. Tracked source still matched reviewed tree
`306e08e24ddb515ba14cabdab79fc6376836e62c`.

- The module `target\test-reports` XML records 186 tests across 19 suites,
  with zero failures, errors, or skipped tests. Core contributes 35 tests
  across 4 suites, cognitive 89 across 8, and LightGBM 62 across 7.
- `spark40-targeted.log` contains the corresponding 35/89/62 pass summaries
  and a subsequent codegen success at `2026-09-16T08:46:16Z`.
- A read-only `ast.parse` scan independently parsed all generated Python
  source and stub files in the six modules: 325 `.py` and 222 `.pyi` files,
  with no syntax failures. No generated file was edited or imported by this
  scan, and Python bytecode writing was disabled.
- Generated `OpenAIPrompt.py` extends `_OpenAIPrompt`. AST inspection confirmed
  zero-argument `super()` in the internal wrapper's `clear` and `copy`,
  presence of `_OpenAIPrompt.pyi`, and `setResponseSchema` in both runtime
  Python and stubs for `OpenAIChatCompletion` and `OpenAIResponses`.
- The owner's `spark40-generated-python.log` reports PySpark 4.0.1, the same
  325/222 parse counts, and successful imports of all three OpenAI schema APIs
  from generated sources.
- The owner's `spark40-python-exports.log` reports 2 passing export-guard
  tests in 9.87 seconds. The log alone is not a full JVM fit/transform or
  persistence test.

These results supersede the earlier limitation that the targeted native tests
and codegen had not yet completed. They do not resolve Issue 1: ordinary
generated-module imports and the existing 35 core tests do not cover the
foreign-owned parameter fixture that exposes the unguarded stub lookup.
The `safeGetDefault` correction and its Spark 4.0 regression remain open.
No new finding arose from this bounded wrapper-smoke inspection. The Round 1
verdict remains ISSUES_FOUND; later rounds remain the parent's responsibility.

### Parent resolution, 2026-09-16

Issue 1 is resolved. `pyStubParamArgs` now uses the existing `safeGetDefault`
guard, matching runtime wrapper generation without adding an exception policy
or changing public signatures. `PyCodegenSuite` adds a nested foreign-parameter
fixture and exercises `makePyFile`, both generated outputs, optional stub
typing, and Python syntax compilation.

On Spark 4.0.1 and JDK 17, the new test first failed at the reported
`Wrappable.scala:317` call with `otherStage__text` ownership rejection.
After the one-line correction, all 17 `PyCodegenSuite` tests passed.
Core main and test Scala style also passed. The red/green logs are retained
locally as `spark40-stub-red.log` and `spark40-stub-green.log`. All-module
codegen is being rerun separately; no pending build is counted as complete.

The all-module codegen rerun subsequently passed at 09:22 UTC. Regenerated
Python verification again parsed 325 `.py` and 222 `.pyi` files, imported
the three schema APIs from candidate-generated modules, and passed both
package-export tests on PySpark 4.0.1.
