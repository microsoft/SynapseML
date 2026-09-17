# Master Spark 4 branch guides, attempt 1, round 1

## Review summary

- **Round:** 1 only.
- **Theme:** Broad sweep, adapted to documentation completeness, correctness,
  and the requested live-branch guidance.
- **Mode:** Sequential.
- **Model:** `gpt-6-astra`, maximum reasoning requested.
- **Issues found:** 1 inaccurate retained baseline claim.
- **Verdict:** ISSUES_FOUND.
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-1-gpt-6-astra.md`.

The three edited guides correct the reviewed stale runtime, package, R,
Petastorm, JDK, Fabric-condition, and prior-sync descriptions. One statement
still misdescribes the exact targets the new introduction says were checked:
the named SAR affinity case class is present on Spark 4.0, not Spark 4.1.
This is the documentation side of the requirement gap in the Spark 4.1 review.

Only this review artifact was created by the reviewer in this worktree.
No agents, source edits, staging, commits, pushes, or CI requests were made.
The parent owns corrections and later rounds.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Checkout | Repository root, branch `docs/spark4-branch-context-20260916` |
| Branch | `docs/spark4-branch-context-20260916` |
| Target master and HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Initial staged tree | `ab05f8f716021f7626187405b8f5618aaadece9a` |
| Final reviewed staged tree | `11224c917329c402e208bf39084c51702603576f` |
| SHA-256 of final raw `git ls-files --stage -z` output | `e476018558702d38d0231b91b07139332ff568bf1e934b7a0550a458be00730c` |
| Working residual manifest SHA-256 | `79381e92e4e4b72143d6a7606fdc1ed9405d4eea61188c828c7f815f0f52d9e5` |
| Final snapshot check | `2026-09-16T08:40:51Z` |

The only tracked changes are these three files:

| File | Working SHA-256 |
| --- | --- |
| `.github\skills\synapseml-branches\references\branch-spark4-common.md` | `e9c6aa7b850e3c4179eee0076403bca110d225b494f4c5d7f8de8b00e9ea3816` |
| `.github\skills\synapseml-branches\references\branch-spark4p0.md` | `9fa5eccc0dc421cc8974968b11858107eee40d1d408b40dbe5ce22714ca759f7` |
| `.github\skills\synapseml-branches\references\branch-spark4p1.md` | `f86944d249ff7cac6cd3bc2985cec9415894af55f3bff49d7b57af871d95970a` |

There were no unmerged entries or unstaged tracked changes. During review,
the owner added four lines to the common guide about GitHub merge settings and
recording the integrated master SHA. That delta was separately read and is
included in the final tree and hashes above. Its external settings claim has
the verification limitation noted below. No unresolved tracked source is
silently covered by an earlier tree hash.

## Evidence checklist

- [x] Read this worktree's `AGENTS.md`, the master/Spark 3.5 branch reference,
  and all three edited guides. Applied the project code-review/branches
  guidance and the documentation adaptation of Round 1.
- [x] Verified `AGENTS.md` and `CONTRIBUTING.md` remain byte-identical to master.
  The shared rules were not changed to embed branch-specific versions.
- [x] Checked the guide claims against Git objects from the actual targets:
  master `1305587a4afe92d27c8e28894b90e38020252e04`,
  Spark 4.0 `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57`, and
  Spark 4.1 `06897e5b27e28d84ce7ffa33e93d7f756992d0f2`.
  Proposed sync worktrees were not substituted for already-landed evidence.
- [x] Read target versions from `build.sbt` and `environment.yml`.
  Both targets already pin PyArrow 18.0.0 and MLflow 2.21.3. Spark 4.0 has
  Python 3.12.11 and NumPy 1.26.4; Spark 4.1 has Python 3.13 and unpinned
  NumPy. Both have sparklyr 1.9.5, R 4.4, and JDK 17.
- [x] Verified the JDK table against
  `.github\workflows\pr-validation.yml`, `environment.yml`,
  `environment.dev.yml`, `templates\java_setup.yml`, `pipeline.yaml`,
  and the two Dockerfile residuals. Master has the Java template at JDK 11;
  both ports retain 17. All three ReleaseBranchCompat selections specify
  Spark 4.1, so the added warning that this does not prove Spark 4.0 is sound.
- [x] Confirmed the target blobs for the OpenAI Python override generator,
  both package-export guard tests, `RCodegenSuite`, `_petastorm_compat.py`,
  and `_horovod.py`. The guards and compatibility layer exist on both ports.
  Master lacks the two cited package-export guard files.
- [x] Verified the live-target `CognitiveServiceBase.getValueOpt` and
  `asImmutableCollection` conversion on both ports and its absence on master.
  The new warning about direct collection casts elsewhere does not claim
  central normalization covers every request path.
- [x] Verified R ANSI flags and `SPARK_HOME` in `RTestGen.scala`, and
  `new_ml_pipeline_stage` in `EstimatorParam.scala`. The guide now separates
  historical failure/pass counts from current-head proof.
- [x] Verified DBR/pool declarations in
  `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\DatabricksUtilities.scala`.
  The GPU pool is shared while the two runtime strings remain branch-specific.
  The pipeline selects the consolidated GPU suite.
- [x] Verified both target Fabric jobs are `condition: false`, master's
  expanded condition remains distinct, and the Fabric workspace payload still
  requests Spark 3.5 in `FabricOperations.scala`. The guide appropriately
  separates a future re-enable PR from this sync.
- [x] Verified the two Spark 4.1 `LongOffset` runtime imports, its Python
  `np.frombuffer` handling, and the unchanged qualified SARModel join.
  Direct source inspection contradicted the common guide's typed-affinity
  statement; see Issue 1.
- [x] Reviewed the explanation of squash-integrated master content
  `a6fd536ad7` versus the actual older graph base. It does not equate
  ahead/behind counts with missing functionality.
- [x] Reviewed the four-line concurrent addition separately against the
  initial staged tree. Recording master content provenance without changing
  repository merge settings is consistent with the bounded sync task.
- [x] Ran `git diff --check HEAD`; it reported no whitespace error.

### Exact diff and reference commands

```powershell
$env:GIT_OPTIONAL_LOCKS = '0'
$git = (Get-Command git).Source
$wt = '.'
& $git --no-pager -C $wt diff --no-ext-diff --no-renames --unified=3 HEAD -- '.github\skills\synapseml-branches\references\branch-spark4-common.md' '.github\skills\synapseml-branches\references\branch-spark4p0.md' '.github\skills\synapseml-branches\references\branch-spark4p1.md'
& $git --no-pager -C $wt diff --no-ext-diff --unified=4 ab05f8f716021f7626187405b8f5618aaadece9a -- '.github\skills\synapseml-branches\references\branch-spark4-common.md'
& $git --no-pager -C $wt show 'ecec8dd58b7a07ebc24d816e321a85ff5dc19d57:core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala'
& $git --no-pager -C $wt show '06897e5b27e28d84ce7ffa33e93d7f756992d0f2:core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala'
& $git --no-pager -C $wt show '1305587a4afe92d27c8e28894b90e38020252e04:.github/skills/synapseml-branches/references/branch-spark4-common.md'
& $git --no-pager -C $wt diff --check HEAD
& $git --no-pager -C $wt write-tree
```

The other reference checks used the same exact-worktree native Git executable
with `show <full-target-SHA>:<file>` for the files named in the checklist.
Only matching declarations and file-existence/blob results were returned for
the version matrix.

## Issues

### Issue 1: The refreshed common guide still attributes typed SAR affinity rows to both ports

- **Severity:** Low.
- **File:** `.github\skills\synapseml-branches\references\branch-spark4-common.md`.
- **Lines:** 115-119 in the final reviewed file.
- **Description:** Under "Common deliberate differences from master," the
  guide says the affinity pairs in `SAR.scala` / `SARModel.scala` use a named
  case class with explicit fields. The introduction now states that this
  reference was checked against exact current targets. At the cited Spark 4.1
  target, `SAR.scala:172` instead uses `Seq[Row]`; lines 192-193 create a struct
  without the `itemIndex` / `affinity` aliases. The typed form exists on the
  cited Spark 4.0 target. The qualified SARModel join exists on both.
- **Evidence:** Spark 4.1 target blob
  `6d111a4411097dc321c32c449135b8dcf6e8ccd4` lacks `ItemAffinity`;
  Spark 4.0 target blob `8f97bf37aa0978b674bdfadf9c9eefb4c23b5c52`
  defines and uses it. These are target objects, not proposed sync results.
- **Pre-existing proof:** The entire SAR guidance paragraph is unchanged
  from master `1305587a4afe92d27c8e28894b90e38020252e04`.
  This is a retained inaccuracy within the requested live-baseline refresh,
  not a newly introduced implementation regression.
- **Impact:** A future reviewer or sync author can incorrectly certify the
  typed-affinity invariant on Spark 4.1 or use the guide to dismiss a real
  difference between the ports. The current review request already assumes
  that both have this adaptation.
- **Suggested fix:** Split the statements. Record that the pinned Spark 4.0
  target uses the named affinity case class, the pinned Spark 4.1 target still
  uses `Seq[Row]`, and both retain the qualified join. Do not assert that the
  historical Spark 4.0 encoder failure has been reproduced on Spark 4.1
  without runtime evidence. If a later Spark 4.1 PR adds the typed form,
  update the guide against its landed SHA rather than a candidate worktree.

Exact disputed source text:

```text
- Preserve the Spark 4 adaptations. In `SAR.scala`/`SARModel.scala` the affinity
  pairs use a named `case class` with explicit struct fields because Spark 4
  rejects the old `Seq[Row]` UDF shape with `UnboundRowEncoder`, and the join
  column is qualified (`col("sarUserFactors.flatList")`) because a self-join now
  trips `DetectAmbiguousSelfJoin`.
```

## Validation limitations

This is a source-based documentation review, not fresh runtime or cloud
validation. Historical notebook timings, R counts, native wheel availability,
Fabric capacity, and old trigger-driven build outcomes were not rerun.
The prompt supplies the recent ADO trigger-filter observation; this reviewer
did not issue an independent definitions API request.

The concurrent `allow_merge_commit: false` GitHub-setting statement was read
and checked for consistency with the proposed guidance, but its live API value
was not independently retrieved by this reviewer. It remains an externally
reported fact, not independently verified source evidence. No external service
or repository setting was changed.

Full sync validation remains the parent's responsibility. Nothing here treats
the running master Azure build or either candidate's pending gates as passing.
Rounds 2 through 6 were not run.

## Resolution log

Issue 1 is open. No document fix was made. The parent should append the
correction and its target-source evidence without replacing this review text.

### Parent resolution, 2026-09-16

Issue 1 is resolved in the common guide. It now attributes `SAR.ItemAffinity`
and named struct fields to the pinned Spark 4.0 target, records `Seq[Row]` on
the pinned Spark 4.1 target, and identifies the qualified join shared by both.
The reviewed target blobs establish the distinction. No Spark 4.1 runtime
failure is claimed, and the sync does not add an unrelated SAR implementation
change. The guide also directs new Python stub generation paths to preserve
the Spark 4 foreign-parameter default guard.
