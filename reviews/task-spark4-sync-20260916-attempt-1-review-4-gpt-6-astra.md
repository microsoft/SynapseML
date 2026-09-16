# Master Spark 4 guides, attempt 1, round 4

## Review summary

- Round: **4 only**, Detailed Correctness, sequential, `gpt-6-astra`.
- Verdict: **CLEAN** for the bounded source snapshot. Issues found: **0**.
- Scope: corrected SAR/default-guard statements and their interaction with the pinned-baseline wording.
- No builds, services, agents, source edits, staging, or commits were performed.
- Only this new artifact was written; all earlier review artifacts remain intact.

## Snapshot

| Item | Value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\branch-context-20260916` |
| Branch | `docs/spark4-branch-context-20260916` |
| Target / HEAD / master | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Round 1 comparison tree | `11224c917329c402e208bf39084c51702603576f` |
| Round 3 comparison tree | `14e3057db49bd6e46133fd7beb4bd776071be383` |
| Final source check | `2026-09-16T10:04:32Z` |
| Raw `ls-files --stage -z` SHA-256 | `8a904ae653c83223af1a88214e819a5a8da6dce70d71942e48fb8cd51c686bcc` |

The only source change since Round 1 is
`.github\skills\synapseml-branches\references\branch-spark4-common.md`,
SHA-256 `0164298bf06650fe11632d7d839f3a64551ce4e967f67c1701d4c283937d1176`.
The owner staged the latest wording during review; its reviewed bytes stayed stable.
No unmerged entries or unstaged tracked source remained at the final check.
The other two branch references are unchanged from the already reviewed Round 1 tree.

## Evidence checklist

- [x] Read the local Round 2 Gemini and Round 3 Opus artifacts and parent resolution
  notes. Independently checked the corrected claims against pinned Git objects.
- [x] `branch-spark4-common.md:111-117` now attributes `SAR.ItemAffinity` and the named
  `itemIndex`/`affinity` fields only to Spark 4.0 target
  `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57`. Its
  `core\src\main\scala\com\microsoft\azure\synapse\ml\recommendation\SAR.scala`
  contains that implementation.
- [x] The same path at Spark 4.1 target
  `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` still uses `Seq[Row]`.
  The new wording does not infer a Spark 4.1 runtime failure from the Spark 4.0 history.
  Both targets' sibling `SARModel.scala` files contain the qualified
  `col("sarUserFactors.flatList")` expression.
- [x] `branch-spark4-common.md:118-121` limits the landed `safeGetDefault` claim to
  Python wrapper lookups, explicitly names the direct R lookup, and treats Python
  stub protection as guidance for newly imported paths rather than already-landed code.
- [x] In `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`,
  each landed port has the Python helper and its two runtime callers, but no
  `pyStubParamArgs` method. Master has stub generation but no Python helper.
  Thus the guide does not confuse the candidate's new guarded stub caller with a target fact.
- [x] The extracted `rParamArg` body has SHA-256
  `515954451a6ddd0eae43daa109a05a958504e6b581561d935ba9f64aa03783a0`
  at master and both exact targets. This proves the direct R lookup is pre-existing.
  No R code change or broader baseline audit is requested by the corrected wording.
- [x] The post-Round-3 diff only narrows the Python/R wording. The pinned commit
  descriptions, squash-content baseline, merge-policy guidance, and branch-specific
  references were not changed by that correction.
- [x] `git diff --check` against the Round 3 tree returned zero. The source edit is
  bounded to the common reference; `AGENTS.md` and `CONTRIBUTING.md` remain untouched.

## Diff and evidence commands

Native Git was used with process-local PATH, `GIT_OPTIONAL_LOCKS=0`, and this exact worktree.

```powershell
$git = 'C:\Users\singhrana\AppData\Local\GitHubDesktop\app-3.6.4\resources\app\git\cmd\git.exe'
$wt = 'C:\Users\singhrana\Documents\SynapseML\.worktrees\branch-context-20260916'
& $git --no-pager -C $wt diff --no-ext-diff --unified=6 11224c917329c402e208bf39084c51702603576f -- '.github\skills\synapseml-branches\references\branch-spark4-common.md'
& $git --no-pager -C $wt diff --name-only 14e3057db49bd6e46133fd7beb4bd776071be383 --
& $git --no-pager -C $wt diff --check 14e3057db49bd6e46133fd7beb4bd776071be383 --
```

Source checks used `git -C $wt show '<full-pinned-SHA>:<repository-path>'` for
`core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala`,
`core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala`,
and `core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SARModel.scala`
at the three full SHAs recorded above.

## Findings and limitations

No new correctness finding. The Round 1 SAR wording and Round 3 overbroad guard
claim are resolved in the inspected source without claiming an unlanded R fix.
No historical runtime measurement, external GitHub policy value, or cloud result
was revalidated here. Prior-round conclusions were not substituted for the
target-object checks above.

This is a documentation-source verdict, not PR readiness. The parent's report that
master build `236185691` failed Fabric provisioning with zero tests does not prove
or disprove these candidate changes. No candidate CI or JVM-backed smoke is claimed.
Rounds 5 and 6 were not run. The parent owns subsequent validation and review.
