# Spark 4.1 master sync, attempt 1, round 1

## Review summary

- **Round:** 1 only.
- **Theme:** Broad sweep. Correctness, security-conscious logic, and requirements.
- **Mode:** Sequential.
- **Model:** `gpt-6-astra`, maximum reasoning requested.
- **Issues found:** 2. One inherited requirement gap remains open; one codegen
  regression has an owner-applied source correction awaiting final validation.
- **Verdict:** ISSUES_FOUND.
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-1-gpt-6-astra.md`.

The incoming changes preserve the reviewed Spark 4.1 runtime differences.
However, the request explicitly requires typed SAR affinity case classes on
both ports. This target and candidate still use a `Seq[Row]` affinity UDF.
The existing branch source, rather than this merge, is the source of that
mismatch. It prevents certifying the stated invariant as fulfilled.

Continued Round 1 inspection also established a new stub/default-lookup
regression in the initial snapshot. The owner changed that call during review.
Its narrow source correction was read separately; see Issue 2 and the
completion addendum. It is not presented as a reviewer-authored fix or a
completed validation result.

No agents were launched. This reviewer made no source edits, staged nothing,
and did not commit, push, or queue CI. Fixes, acceptance-contract decisions,
and later rounds belong to the parent.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.1-master-20260916` |
| Branch | `sync/spark4.1-master-20260916` |
| Target and HEAD | `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` |
| Incoming master and MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Actual graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Previously integrated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Index tree from `git write-tree` | `d581a5ca670de9ec524a89e55276d55a2c2526d7` |
| SHA-256 of raw `git ls-files --stage -z` output | `3da220af0cc27a25a476da7b1d11de2ad3b3babd8a27b75eaf081b85ea3b9659` |
| Working residual manifest SHA-256 | `f47fb415dee5a18668356c6db88f16bb7381825fc24817f7c6cf7df122d28575` |
| Final source check | `2026-09-16T08:40:49Z` |

There were no unmerged entries. One tracked file was unstaged:
`core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala`.
Its working text was reviewed, including the added foreign-parameter fixture
and test. Its SHA-256 is
`ba2fa91c3c6f0f86bdfacdc5a341b72a0cf62eebf606dc52f8eab183068a8d5a`.
The index tree alone does not include that extra test. The exact added text
is retained below.

No tracked source drift was detected between the initial 08:27Z snapshot and
the final source check. The residual manifest covers all 179 tracked paths
differing from master, hashing compact UTF-8 JSON of
`[git-path, SHA-256-of-working-bytes]` pairs in
`git diff --name-only -z <master>` order. Untracked `.venv` contents were not
reviewed and are not part of the source verdict.

## Evidence checklist

- [x] Read this worktree's `AGENTS.md` and its Spark 4.1 branch reference,
  together with the shared branch guidance. Applied the project code-review
  checklist, PR-loop evidence rules, and the Round 1 prompt. The shared
  `AGENTS.md`, `CONTRIBUTING.md`, code-review skill, and branch skill match
  pinned master.
- [x] Verified exact target, master, graph base, content baseline, and
  `MERGE_HEAD`. Earlier squash syncs explain why ancestry and content
  preservation differ. No rebase or fabricated ancestry was used by this
  review.
- [x] Recomputed 654 incoming paths. The current working tree matches master
  for 615 and retains adaptations in 39. The unstaged PyCodegen test accounts
  for one residual beyond the owner's earlier 38-file combined inventory.
  No `HEAD -> working` change lies outside the incoming set.
- [x] Compared current residual edit bodies against
  `a6fd536ad7 -> HEAD`, rather than labeling branch differences pre-existing
  without evidence. All port residual edits predate the merge except the new
  test and the combination of disabled Fabric with master's new condition
  assertions.
- [x] Compared all incoming residual files with the reviewed Spark 4.0 tree
  `306e08e24ddb515ba14cabdab79fc6376836e62c`. Twenty-six are byte-identical.
  Read the differing workflow, build, codegen, Secrets, Python, release-script,
  pipeline, test, and Docker hunks separately. No Spark 4.0 implementation was
  assumed applicable to Spark 4.1 merely because its validation passed.
- [x] Read `build.sbt` and `environment.yml`. Confirmed Spark 4.1.1, Scala
  2.13.17, JDK 17, Python 3.13, unpinned NumPy, cp313 wheels, PyArrow 18.0.0,
  MLflow 2.21.3, sparklyr 1.9.5, and R 4.4. The environment file is unchanged
  from the target. Existing Spark 4.1 Netty and isolation-forest dependency
  exclusions remain.
- [x] Confirmed DBR 18.0 CPU/GPU runtime strings, the shared
  `synapseml-build-14.3-gpu` pool, and the consolidated GPU suite. Fabric remains
  `condition: false`; CMS flags are absent. Workflow action updates retain
  JDK 17 and Python 3.13. Publishing retains retry preparation and the
  branch's packaging verification.
- [x] Verified `HTTPSource.scala` and `DistributedHTTPSource.scala` import
  `org.apache.spark.sql.execution.streaming.runtime.LongOffset`. Reviewed the
  retained bytes/bytearray `np.frombuffer` conversion in
  `opencv\src\main\python\synapse\ml\opencv\ImageTransformer.py`.
- [x] Reviewed the combined service collection/authentication code and the
  OpenAI message/response conversions. The immutable-collection adapter,
  header precedence and sanitization, lazy fallback, retry marker filtering,
  schema-only response formatting, and multimodal request changes remain.
- [x] Traced the new `.pyi` generation, typing markers, initializer generation,
  wrapper defaults, manual-initializer preservation, and packaging. This
  branch retains its own `ManualInitPackageFolders` and UTF-8
  `parseConfigArg`, not Spark 4.0's different codegen helper layout.
  `safeGetDefault` and the `_OpenAIPrompt` zero-argument `super()` overrides
  remain.
- [x] Reviewed the unstaged PyCodegen regression's full fixture context.
  `TypedPythonStage` does not call an overridden `text` default during
  construction. The new fixture deliberately supplies a foreign parameter,
  checks Spark's rejection, then checks generated runtime and stub text.
  Its execution result was not assumed.
- [x] Verified both Petastorm compatibility halves and the serialized Horovod
  path are retained from the target. R ANSI settings, `SPARK_HOME`, nested-stage
  JVM loading, and R codegen guards remain.
- [x] Read both SAR production files. The qualified join is present, but the
  requested typed affinity UDF is not. Independently verified the exact
  pre-merge blob and byte-identical current file; see Issue 1.
- [x] Reviewed the LightGBM/VW residual conversions and preserved upstream
  streaming, ranking, validation, and serialization changes. The merge does
  not introduce an RDD implementation or a new public-signature change in
  those residual hunks.
- [x] Reviewed the existing Scala-2.13 release-anchor additions together with
  the imported release-script changes. Their presence is not new policy in
  this sync.
- [x] Confirmed `website\blog\overview.md` is byte-identical to incoming master
  with one `{/* truncate */}` separator, and inspected the adapted notebook's
  source cells rather than its execution outputs.
- [x] Ran `git diff --check HEAD`. No whitespace or conflict-marker error was
  reported. This is not a substitute for runtime validation.

### Exact diff basis

```powershell
$env:GIT_OPTIONAL_LOCKS = '0'
$git = (Get-Command git).Source
$wt = '.'
$master = '1305587a4afe92d27c8e28894b90e38020252e04'
& $git --no-pager -C $wt diff --name-only a6fd536ad7 $master
& $git --no-pager -C $wt diff --name-status --no-renames $master
& $git --no-pager -C $wt diff --name-status
& $git --no-pager -C $wt diff --check HEAD
& $git --no-pager -C $wt diff --name-only --diff-filter=U
& $git --no-pager -C $wt write-tree
& $git --no-pager -C $wt diff HEAD -- 'core\src\main\scala\com\microsoft\azure\synapse\ml\recommendation\SAR.scala'
& $git --no-pager -C $wt show '06897e5b27e28d84ce7ffa33e93d7f756992d0f2:core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala'
```

Residual edit-body comparisons used
`diff --no-ext-diff --unified=0 a6fd536ad7 HEAD -- <path>` and
`diff --no-ext-diff --unified=0 <master> -- <path>`.
The cross-port review used
`diff --no-ext-diff --unified=4 306e08e24ddb515ba14cabdab79fc6376836e62c -- <path>`
for every differing incoming residual, followed by source-context reads.

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
core\src\main\python\synapse\ml\cyber\utils\spark_utils.py
core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegen.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\ParamsStringBuilder.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\featurize\CleanMissingData.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\param\UntypedArrayParam.scala
core\src\main\scala\com\microsoft\azure\synapse\ml\stages\Repartition.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\Secrets.scala
core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala
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
opencv\src\main\python\synapse\ml\opencv\ImageTransformer.py
pipeline.yaml
scripts\bump-version.py
scripts\test_bump_version.py
templates\publish.yml
tools\ci\tests\test_pipeline_yaml.py
tools\docker\demo\Dockerfile
tools\docker\minimal\Dockerfile
vw\src\main\scala\com\microsoft\azure\synapse\ml\vw\VowpalWabbitBaseLearner.scala
website\doctest.py
```

## Issues

### Issue 1: Required typed SAR affinity implementation is absent on Spark 4.1

- **Severity:** Medium.
- **Classification:** Explicit requirement gap, inherited from the target.
  Not a merge-introduced regression and not a newly reproduced runtime failure.
- **File:** `core\src\main\scala\com\microsoft\azure\synapse\ml\recommendation\SAR.scala`.
- **Lines:** 172-176 and 192-193.
- **Requirement:** The supplied review contract requires typed SAR affinity
  case classes and qualified joins on both ports.
- **Evidence:** The candidate still declares
  `udf((itemUserAffinityPairs: Seq[Row]) => ...)`, accesses pair fields by
  `getDouble`, and builds an unnamed affinity-pair struct. There is no
  `SAR.ItemAffinity` case class in this file. Spark 4.0 instead uses
  `Seq[SAR.ItemAffinity]` and aliases the struct fields to `itemIndex` and
  `affinity`. Both branches do retain the qualified `SARModel` join.
- **Pre-existing proof:** The complete Spark 4.1 working file is byte-identical
  to `06897e5b27e28d84ce7ffa33e93d7f756992d0f2`'s blob
  `6d111a4411097dc321c32c449135b8dcf6e8ccd4`.
  Its working SHA-256 is
  `e9c77c68e8fa0aea61ab960d5791fc9590a2add1906f3511e0c3a67f0b9ec2d7`.
  `git diff HEAD -- <SAR.scala>` is empty. The merge did not remove this fix;
  this target did not have it.
- **Impact:** The parent cannot truthfully report that both candidates satisfy
  the stated typed-affinity invariant. The supplied preservation checks do not
  test this invariant, and a compilation result would not establish the
  public `SAR.fit` behavior.
- **Suggested fix:** The parent should resolve the contract explicitly.
  If typed affinity rows are required on Spark 4.1, apply the narrow named
  case-class/struct-field adaptation and cover the public fit/transform path
  on Spark 4.1. Otherwise document a justified Spark 4.1 exception using
  runtime evidence. In either case, correct the shared guide's claim about
  what is already present at the pinned target. Do not present a candidate
  fix as already landed.

Reviewed source excerpt:

```scala
    val seqToArray = udf((itemUserAffinityPairs: Seq[Row]) => {
      val values = Array.fill[Float](itemCount)(0.0f)
      itemUserAffinityPairs.foreach(pair => values(pair.getDouble(0).toInt) = pair.getDouble(1).toFloat)
      values
    })
```

### Issue 2: New stub generation bypassed the existing default guard

- **Severity:** Medium.
- **File:** `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`.
- **Line:** 317 in the initial reviewed snapshot.
- **Status:** Owner-applied source correction inspected during Round 1;
  final regression/package validation not independently established here.
- **Description:** The incoming `pyStubParamArgs` called
  `thisStage.getDefault(p)` directly, unlike the guarded runtime-wrapper
  argument/default paths. A parameter owned by another stage therefore made
  the new `.pyi` generation throw after runtime-wrapper generation had
  tolerated it.
- **Evidence:** `spark41\stub-regression-red.log:24-40` records the
  foreign-parameter regression failing with
  `Param otherStage__text does not belong to foreignParamPythonStage`.
  The stack includes `Params.getDefault` and
  `PythonWrappable.pyStubParamArgs(Wrappable.scala:317)`.
  This is an observed owner test failure, not an inference from a pending gate.
- **Regression provenance:** The exact target has `safeGetDefault` but no
  `pyStubParamArgs`. The new method and direct lookup come from the incoming
  master typing work. The original reviewed file's SHA-256 was
  `584f1cfb8e289557bf47268efc67010095c606fa55c293111b0617dd5b3f1aee`.
- **Impact:** Python codegen fails for the parameter shape deliberately
  tolerated by the port's existing wrapper guard.
- **Correction observed:** The owner replaced that call with
  `safeGetDefault(p)`. The existing helper catches the specific
  `IllegalArgumentException` and returns `None`, so the stub follows the
  optional-parameter path consistently with runtime generation.
- **Remaining proof:** Complete the foreign-parameter regression and relevant
  codegen/package validation on the corrected source. Carry the same fix and
  regression to Spark 4.0, where the direct call remained at this review.

The source delta inspected was exactly:

```diff
-    (p, thisStage.getDefault(p)) match {
+    (p, safeGetDefault(p)) match {
```

## Unstaged test text included in the review

These additions to `PyCodegenSuite.scala` are included in the working-file
hash above, but not the recorded index tree. No execution result is inferred
from this source review.

```scala
  class ForeignParamPythonStage extends TypedPythonStage("foreignParamPythonStage") {
    override protected lazy val classNameHelper: String = "ForeignParamPythonStage"

    override val text = new Param[String]("otherStage", "text", "text value")
  }
```

```scala
  test("generated wrappers and stubs tolerate Spark 4 rejecting foreign parameter defaults") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val stage = new ForeignParamPythonStage
      intercept[IllegalArgumentException](stage.getDefault(stage.text))

      stage.makePyFile(conf)

      val folder = packageDir(conf.pySrcDir, "/codegen")
      val runtimeFile = new File(folder, "ForeignParamPythonStage.py")
      val stubFile = new File(folder, "ForeignParamPythonStage.pyi")
      assert(readUtf8(runtimeFile).contains("text=None"))
      assert(readUtf8(stubFile).contains("text: Optional[str] = ..."))
      assertPythonCompiles(runtimeFile)
      assertPythonCompiles(stubFile)
    }
  }
```

## Completion addendum: owner source changed after the initial snapshot

The final artifact check detected one changed production file after the
08:40:49Z snapshot. At `2026-09-16T08:47:24Z`, the current index tree was
`0eabccd3a82c4ff1e50721b2fe60e4d051a2bc0e` and the current working residual
manifest SHA-256 was
`f436b6c50a263fdf43f466581430273c4ac1d4dcc4cd223acb66e939d41ca016`.
HEAD and MERGE_HEAD were unchanged. The initially unstaged test was now staged
with unchanged text.

The only production-source delta was `Wrappable.scala`, from SHA-256
`584f1cfb8e289557bf47268efc67010095c606fa55c293111b0617dd5b3f1aee`
to `6c570eabc4a263e67fc106d6f394c213abef9f69c5a10b0a479f1fe1a23aa8f3`.
The reviewer read
`git diff --no-ext-diff --unified=6 d581a5ca670de9ec524a89e55276d55a2c2526d7 -- core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`
with native Git and this exact worktree, then checked the helper and new stub
caller. That one-line correction is included in the completed Round 1 source
review. Its execution is not marked passed.

The initial source text, hashes, and failed regression remain recorded above.
Any source changes after this addendum are not covered by this review without
a further explicit comparison.

## Validation limitations

The reviewer did not run another build, Spark job, environment install, or
remote validation. The owner's `spark41\sbt-compile-style.log` had successful
compile/style entries, including style completions at 08:36Z. This is not
full current-head validation.

The inspected `spark41\ci-python-tests-with-hypothesis.log` recorded 340 passes
and 11 release-script snapshot/history failures. Their cause was not
independently established here, so they are neither declared pre-existing
product defects nor counted as passing. The parent owns that triage and the
remaining native, offline, codegen, Python, R, Databricks, and Azure evidence.
The foreign-parameter test and the SAR public runtime path were not
independently executed by this reviewer. The owner's dedicated red log was
read after the initial draft, but a passing run of the corrected
`Wrappable.scala` was not established before this addendum.

The 615 byte-identical incoming master files were audited for preservation,
not subjected to a new upstream feature review. Fabric remains disabled.
No pending gate is treated as a code bug or a green gate. Rounds 2 through 6
were not run.

## Resolution log

Issue 1 remains open. Issue 2 has an owner-applied one-line source correction,
inspected as described above; final verification belongs to the parent.
No fix or acceptance exception was applied by this reviewer.

The initial draft's statement that no merge-introduced product defect had
been established is superseded by the failed stub regression inspected before
completion of Round 1. The original failing snapshot and source excerpt are
preserved, together with the owner correction rather than rewritten as though
the first snapshot were clean.

The parent owns resolution and should append evidence without replacing the
completed reviewed text.

## Coordinator-directed resolution addendum, 2026-09-16

This addendum records the coordinator's scope decision and the owner's completed
validation. The original review and its snapshot remain unchanged.

### Issue 1: resolved for this sync by preserving the target

The coordinator clarified that this bounded sync must preserve Spark 4.1's
existing SAR implementation, not introduce a pre-existing SAR repair. The earlier
requirement for named affinity case classes on both ports is therefore superseded.
Spark 4.1 uses `Seq[Row]`, not `SAR.ItemAffinity`. The coordinator owns the separate
master-guide correction distinguishing Spark 4.0's named affinity type from
Spark 4.1's implementation and documenting the shared qualified join.

At staged tree `ff4844a951c3848f59018accb34e1d733acd4220`, both SAR files have
exactly the same Git blobs as target
`06897e5b27e28d84ce7ffa33e93d7f756992d0f2`:

| File | Target and staged blob | Preserved evidence |
| --- | --- | --- |
| `SAR.scala` | `6d111a4411097dc321c32c449135b8dcf6e8ccd4` | Line 172 declares `itemUserAffinityPairs: Seq[Row]`. |
| `SARModel.scala` | `65f0118ee7d153ebe584182a1f97351acfe4e60f` | Line 438 uses `col("sarUserFactors.flatList")`. |

Issue 1 is closed as a sync requirement gap by that explicit scope clarification.
This is not a claim that the SAR public runtime path has been validated or that
Spark 4.1 has the named `ItemAffinity` implementation. No SAR source was changed.
The session's `spark41\sar-round1-resolution.json`, `invariants.json`, and
`final-preservation-audit.json` record the corrected preservation facts.

### Issue 2: owner validation completed

The foreign-default regression failed before the `safeGetDefault` correction and
passed afterward. The completed owner batch passed 235 tests across 26 suites,
including 60 core tests, and generated R/Python output for all six modules.
Compile, test compile, and both Scala style checks passed. Evidence is in
`spark41\stub-regression-red.log`, `targeted-tests-codegen.log`, and
`scala-test-results.json`.

The current staged tree also contains later CI-only follow-ups for the explicit
disabled-Fabric assertion and Docker's preserved `python=3.13` selection. The
full Linux CI suite passed 142 tests; pinned Black 22.3 passed all 214 files.
These later edits do not change the product sources validated above and do not
constitute another review round. The master-baseline release snapshot failure,
Python 3.13/PySpark 4.1.1 runtime smoke gap, and remote gates remain documented in
`spark41\validation-report.json`. This addendum does not declare merge readiness.
