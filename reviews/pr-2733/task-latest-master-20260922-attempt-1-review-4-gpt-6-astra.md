# Round 4 detailed correctness review

## Review summary

| Field | Result |
| --- | --- |
| Target | microsoft/SynapseML#2733, base `spark4.0` |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark40-20260921` |
| Reviewed HEAD | `96e5ac204b45b1d0c83c8407ca97904c56f7bd94` |
| Merge source / MERGE_HEAD | `0a7fdafaa33ff4785dadc8d7eebee68efde110fb` |
| Reviewed staged tree | `b7c6bd34f1eeac39dccaab00ca38cd54539772b4` |
| Prior locally validated tree | `c95db6cba93345b1d3fd3912dbb5e8cfc17acaac` |
| Round / attempt / mode / model | 4 / 1 / sequential / `gpt-6-astra` |
| Scope | Detailed data-flow, type-conversion, and branch-condition review of the incremental staged merge, using retained source context |
| Issues found / verdict | **0 / CLEAN** |
| Artifact | `reviews\pr-2733\task-latest-master-20260922-attempt-1-review-4-gpt-6-astra.md` |

## Snapshot and documentation-only change

Verified the index equals the advertised staged tree, with unchanged HEAD and
MERGE_HEAD, no unresolved index entries, and no unstaged tracked changes.

SHA-256 of raw `git ls-files --stage -z` output:

`0f0f368a6989f2de9991063f4073ad721000902dea0e962a59ee4c1f09d727c0`

SHA-256 of raw `git --no-pager diff --cached --binary --full-index --no-ext-diff --no-textconv HEAD --` output:

`049915dd33a504a837ddf303165abd46e1ad9f52114f21afb0607157a6440611`

Compared the complete prior/new staged trees. Their only changed path is
`tools\ci\README.md`; the inspected patch changes prose only. All executable
files, tests, pipeline conditions, dependency declarations, and templates are
identical to the prior validated tree.

The correction at `tools\ci\README.md:100-146` now describes five CPU jobs and
one GPU job, states that Fabric remains disabled regardless of selector output,
and limits full-test claims to enabled jobs. This matches the actual consumers,
including explicit family-disable parameters. The round-3 finding/resolution
records were not edited by this review.

Read `files\latest-master-content-proof-final.json`. It identifies this exact
staged tree and records 18 non-review paths: 13 identical path states, including
deletions, and five explained port differences. The fifth is this README;
the others remain pipeline, Developer Setup, existing pipeline guards, and the
disabled-Fabric assertion. No production/runtime change is introduced.

## Detailed correctness evidence

- [x] `tools\ci\e2e_impact.py:39-70`: checked every classification return and
  the union operation. Empty change sets retain all families. Empty/dot path
  components, control/non-ASCII characters, backslashes, and colons cannot
  enter the skip allowlist. One runtime or unknown path dominates any number
  of isolated paths; duplicate paths do not change the set result.
- [x] `tools\ci\e2e_impact.py:83-125`: traced Git bytes through ASCII commit
  decoding, exact queued-SHA/source-parent checks, and the first-parent diff.
  The two-parent requirement makes `parents[1]` and `parents[2]` safe to index.
  Nonempty raw output, terminal NUL, and even field count precede the two-field
  loop; five header fields precede indexing modes/status. Disabling renames
  exposes both deleted and added paths. Unsupported modes/statuses and strict
  UTF-8 decode failures cannot produce a partial skip decision.
- [x] `tools\ci\e2e_impact.py:128-173`: non-PR builds and any full-test value
  other than case-insensitive `"false"` return all families before consulting
  Git. Git/time-out, OS, validation, and decode errors reach the warning and
  all-family fallback. Each of the three output names is emitted exactly once
  as an output-variable command, using membership in the final selected set;
  there is no conversion through Python truthiness of `"false"`.
- [x] `pipeline.yaml:231-292`: CPU/GPU conditions require dependency success,
  enabled family parameters, and an output not equal to the string `'false'`.
  Missing output therefore cannot authorize a skip. Fabric's Boolean
  `condition: false` is independent of the selector, so even a true Fabric
  output or `fullTests=true` cannot enable that job. The CPU matrix has five
  legs and the non-matrix GPU job has one, matching the corrected README.
- [x] `tools\ci\tests\test_e2e_impact.py:382-392`: the Fabric branch checks
  `condition is False` before any string-membership assertion, avoiding the
  original Boolean/string mismatch. The shared dependency assertion remains
  outside that branch. Lines 424-454 still count the four coverage-producing
  job matrices as 40 + 7 + 6 + 1 and compare both thresholds to 54.
- [x] `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\geospatial\GeospatialCoreSuite.scala:168-202`:
  latitude/longitude arrays and the UDID column match their configured names.
  Scalar and column-bound stages use distinct indexed save paths. Assertions
  use each loaded stage's own Params and compare complete stored values,
  UID, URL, fake key, and output/error schema. No unchecked cast or numeric
  conversion was added. The loaded public `transform` reaches the unchanged
  retirement exception in `CheckPointInPolygon.scala:30-38`, not HTTP execution.
- [x] `core\src\test\scala\com\microsoft\azure\synapse\ml\featurize\VerifyValueIndexer.scala:42-45,76-90`:
  removing the unused Boolean loop leaves the same inputs, fit/transform calls,
  collection comparisons, and categorical assertions. Moved local values remain
  confined to the same test and retain their types. The ignored null case is unchanged.
  AzureMaps import/blank-line cleanup introduces no expression change, and
  removing the unused Python runner leaves the existing
  `project\CodegenPlugin.scala:428-448` pytest dispatch unchanged.

Static traces from the reviewed code, not new runtime executions. The first
three rows assume `BUILD_REASON=PullRequest`, `SYNAPSEML_FULL_TESTS=false`, and
valid merge/diff metadata:

| Input to selection | Selector result | Pipeline consequence |
| --- | --- | --- |
| Verified PR containing only allowlisted Python-test or governance paths | No notebook families | CPU/GPU skipped; Fabric stays disabled |
| Runtime/unknown path mixed with isolated paths | All three selector families | Enabled CPU/GPU eligible; Fabric stays disabled |
| Runtime file moved into an allowlisted directory | All three, because the deletion remains visible | No runtime-impacting change is hidden |
| Non-PR, forced/unknown full-test setting, or failed metadata/Git/decode validation | All three selector families | No selective skip; existing success/family gates still apply |

## Conclusion and limits

No actionable detailed-correctness finding. Retained source reads are bound to
the prior snapshot by the documentation-only tree comparison; this round
adds no broad discovery or test rerun. Previous local-validation evidence is
not presented as a new run against the updated documentation.

Only this unstaged review artifact was written. No source edit, staging,
commit, push, dirty-root modification, remote-CI operation, or later theme was
performed. This is a GPT round-4 result, not a full multi-family gauntlet claim.
