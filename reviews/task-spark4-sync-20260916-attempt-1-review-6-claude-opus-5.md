# Spark 4.0 sync, attempt 1, round 6

## Review summary

- Round: **6 only**, Polish & Hardening, sequential, `claude-opus-5` (max reasoning).
- Verdict: **CLEAN**. Concrete findings: **0**. Precision notes: 2 (neither is a defect).
- Scope: merge adaptations, documentation accuracy, performance/observability implications,
  and naming. Not a re-audit of unchanged upstream algorithms.
- No agents or factories spawned. No source, docs, tests, staging, commits, or remotes touched.
  Only this artifact was written; rounds 1-5 remain byte-intact.

## Snapshot

| Item | Value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark40-20260916` |
| Branch | `sync/spark4.0-master-20260916` |
| Target / HEAD | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` |
| Master / MERGE_HEAD (real, retained) | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Computed merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Incorporated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` (**not** an ancestor of HEAD) |
| Staged index tree (`write-tree`) | `8b737ee65320c24fa5f3e1658ee313c9f234d5d0` (identical to round 5) |
| Unmerged paths / unstaged tracked | 0 / 0 |
| `git diff --check HEAD` | 0 issues |
| Snapshot timestamp | `2026-09-16T10:40:00Z` |

Source is unchanged since round 4. All Git calls used the native client with
`GIT_OPTIONAL_LOCKS=0` and `-C` on this exact worktree.

## Evidence checklist

- [x] **Adaptation set recounted independently.** Intersecting `diff --cached --name-only MERGE_HEAD`
  with `diff --cached --name-only HEAD` yields 38 paths differing from *both* parents; 3 are staged
  review artifacts, leaving **35 true merge adaptations**. This corroborates the recorded count
  rather than restating it. The 619 byte-exact incoming paths were not re-examined.
- [x] **Stub default path reuses the existing guard.** `Wrappable.scala`: `safeGetDefault` catches only
  `IllegalArgumentException` and returns `None`; `pyStubParamArgs` consumes it through the same
  `(p, safeGetDefault(p))` match shape as `pyParamArg`/`pyParamDefault`. `ServiceParam` emits both the
  value and `Col` argument; `ComplexParam` and `None` collapse to `Optional[...]`; a real default keeps
  the non-optional stub type. No second lookup helper was introduced.
- [x] **Naming follows the file's own conventions.** `pyStubParamArgs`/`pyStubParamsArgs` mirror the
  established singular/plural `pyParamArg`/`pyParamsArgs` pair, and `pyStubOptionalType`,
  `pyStubKeywordOnlyMethod`, `pyStubInitFunc`, `pyStubSetParamsFunc` are consistently prefixed and
  `private`. Fixtures `PyCodegenFixtures`, `TypedPythonStage`, `ForeignParamPythonStage` name what they
  are; the fixture object carries the comment explaining why it stays nested.
- [x] **Regression test is genuinely red-green, not exception-satisfied.** `PyCodegenSuite` asserts
  Spark's rejection with `intercept[IllegalArgumentException](stage.getDefault(stage.text))`, then calls
  `makePyFile` **outside** that assertion and checks `text=None` in the `.py`, `text: Optional[str] = ...`
  in the `.pyi`, and `assertPythonCompiles` on both.
- [x] **No hot-loop, algorithm, or public-signature change from any resolution.** The only combined
  LightGBM deltas are `LightGBMBooster.scala:461,482` (`session.sqlContext.createDataset` →
  `session.createDataset` inside `saveNativeModel`/`dumpModel`, once per save, surrounding
  `sparkContext.parallelize` is master's pre-existing line, not added here) and `LightGBMBase.scala:239`
  (`.distinct.toArray`, once per fit while computing categorical indexes). Neither is per-row.
- [x] **Remaining combined main-source files verified as correct two-sided resolutions.** Diffing the
  index against *each* parent separates what the merge imported from what the port preserved:
  `CleanMissingData.scala` took master's documentation rewording while the port's `convertCustomValue`
  typing stayed intact; `Repartition.scala` took master's `partitoned`→`partitioned` fix while keeping
  `sparkSession.createDataFrame`; `ParamsStringBuilder.scala` took only the `boost allows` comment fix;
  `UntypedArrayParam.scala` took master's new null/`Long`/`Short`/`Byte`/`Float` cases and ordered
  `JsObject` while preserving the port's widened `case v: scala.collection.Seq[_]` — which is what keeps
  a Spark-supplied mutable sequence from falling through to `throwFailure`. `AnalyzeText.scala` keeps the
  same widening for `documents` and `errors`. In Scala 2.13 `Map` is not a `Seq`, so the case order is safe.
- [x] **CI policy preserved, not merely present.** `pipeline.yaml` keeps `condition: false` on `FabricE2E`
  with its branch-specific comment rather than adopting master's
  `and(succeeded(), runTests, testFabricE2E, non-fork)` gate. `.github/workflows/pr-validation.yml` takes
  master's newer `actions/setup-java` pin (`b6effb05` v5.7.0 → `de7274f0` v6.0.1) while retaining
  `java-version: 17`, the `Set up JDK 17` step name, its "do not take master's JDK 11 when syncing"
  guard comment, and `python-version: "3.12"`.
- [x] **Duplicate constant cleanup is complete and one-sided.** `tools/ci/tests/test_pipeline_yaml.py`
  retains a single `BUILD_SBT` after `REPO_ROOT`; the later identical assignment is gone and no
  assertion or condition was removed with it.
- [x] **Helper divergence is justified, not drift.** This branch's `tools/ci/get_python_version.sh` and
  `tools/ci/README.md` are byte-identical to master (`5b0c77dcdc`, `2c7d22d8fe`). That is correct:
  `environment.yml` pins `python=3.12.11`, which already satisfies master's stricter grammar, so this
  port needs no counterpart to the Spark 4.1 relaxation.
- [x] **Whitespace and marker hygiene.** `git diff --check HEAD` reports zero; no conflict markers,
  debug output, or trailing whitespace were introduced.

## Findings

**CLEAN — no concrete polish or hardening defect found in this round's scope.**

Two precision notes, neither actionable in this PR:

1. **Observability of the codegen guard (accepted trade-off, not a defect).** `safeGetDefault` converts
   a foreign-owner `IllegalArgumentException` into `None` with no log line, so a stage whose parameter is
   owned elsewhere silently renders a `None` Python default instead of its Scala default. The catch is
   narrowly typed and `Params.getDefault` raises that exception only from its ownership check, so nothing
   unrelated is swallowed, and this runs at codegen time, never on an executor. Adding logging would edit
   shared code that AGENTS.md requires to land on `master`, so it is correctly out of scope here.
2. **Pre-existing cosmetic residue, not merge-introduced.** `ParamsStringBuilder.scala` has a stray blank
   line where `sb.to` was removed for Scala 2.13. Confirmed present at the pinned target `ecec8dd58b`
   — the index-versus-target diff for that file contains only master's comment fix — so this sync does
   not introduce it, and repairing it here would be the unrelated expansion this round was told to avoid.

### Note on a prior artifact's wording (flagged, not rewritten)

Round 4 states "master action updates retain JDK 17" for `pr-validation.yml`. The underlying fact is
correct — the merged file takes master's action bump and keeps JDK 17 — but the phrasing can be misread
as master itself using 17. Measured: master's `pr-validation.yml` pins `java-version: 11`; the target and
this candidate pin `17`. No prior artifact claim was found to be false; only this one reads ambiguously.

## Limitations

Static, evidence-based inspection of the staged snapshot plus Git object comparison against both pinned
parents. No suites were rerun in this round: the compile, style, 186-test/19-suite, 17-codegen-test,
137-Linux-CI, Black 22.3.0/214-file, codegen, and PySpark 4.0.1 export and 325 `.py`/222 `.pyi` parse
results are cited from the owner's record, not reproduced. No JVM-backed Python or R runtime, Docker
image build, Databricks, GPU, or cloud execution is claimed. `FabricE2E` stays disabled by design; the
`236185691` master baseline failed Fabric provisioning before any test ran and is not candidate evidence.
Remote Azure Pipelines CI is planned after the PRs exist; its absence is a stated gap, not a code defect.
Round 6 artifacts for rounds 4-6 are untracked here — the parent owns staging.

## Resolution

Nothing to resolve. No source, documentation, or test change is requested by this round.
Sequential review of this candidate ends at round 6 with **CLEAN**.
