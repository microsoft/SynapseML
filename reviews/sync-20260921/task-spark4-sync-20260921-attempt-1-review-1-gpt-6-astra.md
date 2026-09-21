## Review Summary

- **Round**: 1 only, attempt 1
- **Theme**: Broad sweep, correctness, security, logic, and spec conformance
- **Mode**: sequential
- **Model**: gpt-6-astra
- **Reasoning**: xhigh
- **Target**: spark4.0
- **Artifact**: `reviews/sync-20260921/task-spark4-sync-20260921-attempt-1-review-1-gpt-6-astra.md`
- **Reviewed index tree**: `5325163b0c60b8394b6fa77e3ed48e21e21ff2d7`
- **HEAD**: `7251246d4513f597838bcd53402a9025943a4142`
- **MERGE_HEAD**: `714d365e71f6d2db5b7072094a4a3ad22485eb57`
- **Content baseline**: `7c1bf9eb56`
- **Real merge base**: `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f`
- **Issues Found**: 1 High, an imported upstream defect; no concrete merge-resolution defect found
- **Verdict**: ISSUES_FOUND

This is a review of the frozen, staged candidate, not a readiness assessment.
The artifact itself is not part of the reviewed index tree. No source edits,
staging, commits, pushes, agent dispatch, or remote-service calls were performed.

## Evidence Checklist

- [x] Read this worktree's `AGENTS.md`, code-review checklist, Spark 4 common
  rules, Spark 4.0 reference, `build.sbt`, and `environment.yml`. Applied the
  review-code Round 1 prompt and required evidence format.
- [x] Inspected the staged inventory and production, test, CI, and documentation
  diffs: 40 files, 3,697 additions, and 202 deletions. `git diff --cached --check`
  passed; the tracked working tree matched the index and had no unmerged entries.
- [x] Independently compared target, incoming master, content baseline, and index
  blobs for all 53 paths in `spark40-original-conflicts.json`: 35 retain the
  target blob, 12 equal master, and 6 combine both sides. Also verified the
  recorded prior head `1fc510fbeb` differs from HEAD only in the supplied nine
  guide/review files.
- [x] Checked all 34 non-review paths changed on master since the content
  baseline. Twenty-eight candidate blobs equal master. For the other six,
  the added/deleted lines in baseline-to-target equal those in
  master-to-candidate, ignoring hunk coordinates. Reviewed those retained
  adaptations in `CognitiveServiceBase.scala`, `OpenAIPromptPythonOverrides.scala`,
  `FabricOperations.scala`, `pipeline.yaml`, `templates\publish_coverage_ado.yml`,
  and `tools\ci\tests\test_pipeline_yaml.py`.
- [x] Verified `AGENTS.md` and `CONTRIBUTING.md` equal incoming master. The
  original `reviews\task-post-sync-lessons-20260917-review.md` equals the target
  copy, and its `reviews\master-sync-20260921\` counterpart equals master's copy.
  Imported historical reviews were treated as records, not current validation.
- [x] Verified unchanged target pins and adaptations: Spark 4.0.1, Scala 2.13.16,
  Python 3.12.11, JDK 17, NumPy 1.26.4, branch workflows and setup templates,
  Databricks 17.3 CPU/GPU profiles, SAR encoder adaptations, and Scala 2.13
  collection normalization. Fabric E2E remains `condition: false`.
- [x] Traced the imported microsoft/SynapseML#2724 bridge through
  `Wrappable.scala`, `core\src\main\python\synapse\ml\core\schema\Utils.py`,
  `HasOpenAIResponseSchema.scala`, and the prompt overrides. Reviewed regression
  tests for setter precedence, scalar/column aliases, copy, persistence,
  constructor dispatch, and scratch-copy rollback. The port's zero-argument
  `super()` fixes remain present.
- [x] Ran the exact Python helper and generated-template methods in memory with
  fake objects on this candidate. Alias conflicts and explicit null service
  arguments were rejected before dispatch; constructor null skipping, ordinary
  setter dispatch, failed conversion, failed scratch setter rollback, and
  successful service updates passed. These probes did not import Spark or
  connect to a JVM and are not generated-wrapper integration evidence.
- [x] Reviewed microsoft/SynapseML#2729 from header extraction through
  `ServiceAuthHeaders`, Text Analytics batching, and the loopback test suite.
  The helper accepts mutable collection representations, leaves payload
  parameters batched, selects the documented first usable header value,
  preserves auth precedence, and keeps Fabric fallback lazy. Inspected copy,
  persistence, partial-batch, and submit/poll assertions.
- [x] Reviewed microsoft/SynapseML#2725 CI cleanup and microsoft/SynapseML#2728
  ownership, pagination, age/activity checks, deletion confirmation, cached
  preflight, and per-job cleanup. Ran 12 selected existing read-only
  configuration assertions covering preflight ordering, disabled Fabric
  authentication wiring, evidence retention, replay exclusions/prerequisite
  format, cache gating, target runtime/workflow pins, and three retry-template
  cases. All passed. Parsed all five changed Python files with `ast.parse`.
- [x] Independently traced the supplied malformed-reference report through
  `FabricArtifactCleanup.item`, `safeStore`, and `run`, and checked the parser
  and missing-edge tests. The defect is present in the reviewed source.
- [ ] Scala compilation/style, generated-wrapper Spark tests, Scala fake-client
  tests, and HTTP loopback suites were not executed in this review. The parent
  is validating independently; its results were not assumed.
- [ ] No live Fabric, Databricks, Azure, or GitHub validation was attempted.
  Shared-GPU full-build serialization remains a parent orchestration
  requirement, not something established by this local review.

## Issues

### Issue 1: Reject mixed valid and malformed relationship IDs before cleanup

- **Severity**: High
- **File**: `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala`
- **Line(s)**: 47-52 and 65-70; deletion consequences at 187-191 and 213-227
- **Classification**: Imported upstream defect from microsoft/SynapseML#2728,
  matching the supplied `discussion_r4051626209`, not a merge mistake.
- **Provenance**: The file is absent from target HEAD. Its candidate blob
  `35c1bdf6b197ba02d511045473409088ddfefb44` exactly equals incoming master and
  the Spark 4.1 candidate.
- **Description**: `references` recursively keeps any GUID-shaped string but
  returns an empty set for malformed strings, numbers, and other unsupported
  leaves. `item` validates only that each relation entry yielded at least one
  GUID. A valid GUID elsewhere in the same entry therefore hides a malformed
  relationship ID, and cleanup treats the resulting incomplete graph as known.
- **Concrete source trace**: Consider a foreign notebook with otherwise
  accepted metadata, all other relation arrays null, and this relation entry:

  ```json
  {
    "artifactObjectId": "00000000-0000-0000-0000-000000000004",
    "dependentArtifactObjectId": "00000000-0000-0000-0000-000000000001 "
  }
  ```

  The trailing-space ID is not a GUID match and is silently discarded. The
  other ID makes `references(v).nonEmpty` true. The resulting notebook has a
  nonempty reference set, so the no-reference safeguard at line 189 does not
  apply. With an unchanged, expired, owned store ending in `0001`, no owned
  jobs, and no other edges to that store, `safeStore` returns true and `run`
  reaches deletion. This is a deterministic source trace, not a live deletion
  or an executed Scala reproduction.
- **Risk**: Manual cleanup or a subsequently enabled preflight can delete an
  owned lakehouse/warehouse without a complete account of its consumers.
  Disabled Fabric CI limits current automatic exposure but does not make the
  cleanup parser fail closed.
- **Test evidence**: `FabricTestArtifactTrackerSuite.scala:497-518` rejects an
  entry containing only `"not-an-id"` and an invalid top-level parent ID.
  It does not test mixed valid/malformed IDs inside one relation entry.
  The missing-edge guard tested at lines 424-431 does not cover this nonempty
  but incomplete graph.
- **Suggested Fix**: Parse recognized relationship ID fields and reject
  malformed IDs or unknown relationship shapes before authorizing deletions.
  Alternatively propagate an explicit unsafe-inventory result that blocks
  deletion. Add parser and fake-client regressions with a valid GUID beside
  an invalid string, numeric ID, and nested malformed ID; assert failure or
  retention and zero DELETE calls. Preserve legitimate non-ID metadata.

## Resolution Log

### Issue 1

- **Status**: Open
- **What changed**: Nothing in source; review artifact only.
- **Why**: The review contract freezes source and permits Round 1 only.
- **How verified**: Direct code-path and existing-test inspection plus exact
  target/master/candidate blob comparison. No live cleanup was run.

## Supplemental validation evidence

Evidence supplied on 2026-09-21 after the initial review:

- The parent reports that this exact staged tree passed 89 pipeline regressions
  and pinned Black checks across 215 files. These larger checks were not rerun
  by this reviewer.
- Inspected the startup of session `files\spark40-validation.log`. It identifies
  this worktree, JDK 17.0.19, and the compile, style, targeted Scala test, and
  codegen commands. Validation was reported as running; successful completion
  is not established by the inspected startup output.
- Inspected `files\spark40-merge-audit.json` metadata and initial per-file
  records. Its target, incoming master, content baseline, and candidate tree
  match this review. A fresh index comparison against the recorded tree passed,
  and there were no tracked unstaged changes.

This evidence does not resolve Issue 1: the existing parser tests omit the
mixed valid/malformed relation case. The finding remains Open and the verdict
remains ISSUES_FOUND. Review scope remains the code delta and preservation of
port adaptations, not an exhaustive rereview of historical review records.

## Round 1 fix verification

### Issue 1 resolution, 2026-09-21

- **Status**: Fixed
- **Current narrow verdict**: CLEAN; no remaining concrete defect found in this
  fix. This resolution supersedes the earlier Open status, without rewriting
  the original finding or claiming overall readiness.
- **Scope**: Only the three-file delta from reviewed tree
  `5325163b0c60b8394b6fa77e3ed48e21e21ff2d7` to the current source was checked.
  No broader merge audit or later gauntlet round was repeated.
- **Exact reviewed blobs**:
  `FabricArtifactCleanup.scala` = `fc9f27368c896bba8c5934d3824a7ef015d8442f`;
  `FabricTestArtifactTrackerSuite.scala` = `a8d0676aa6b292a581843c09a6249c940fe19586`;
  `docs\Reference\Developer Setup.md` = `77d6aac1e6d6e79d4b7527465fd0b181dbf8be65`.
  Both Scala blobs equal the Spark 4.1 fix.
- **What changed**: `references` validates every leaf while collecting
  canonical GUIDs. Nonempty nested objects and arrays recurse; malformed
  strings, numbers, booleans, null leaves, and empty nested containers throw
  an `IllegalArgumentException` naming the relation field without echoing
  payload values. `item` traverses relation entries once. Null or empty outer
  relation collections still represent no relationships.
- **Why this resolves the finding**: A valid sibling GUID can no longer mask
  an invalid value. Materializing the complete inventory fails before cleanup
  can authorize a deletion using that incomplete graph. The conservative
  GUID-only contract deliberately rejects unknown metadata rather than
  guessing undocumented ID field names. The documentation now describes this
  contract and the branch-disabled Fabric E2E job.
- **Regression coverage**: The fake-client test covers all four relation
  fields with nine malformed values each, including the original trailing-space
  reproducer and nested failures. All 36 combinations require an inventory
  exception, zero DELETE calls, and the unchanged store. The added positive
  nested-container case preserves valid GUIDs and case normalization.
- **Red evidence inspected**: Session `files\spark41-validation.log` records
  the new mixed-valid/malformed test failing because no exception was thrown;
  73 other tests passed, with zero canceled, ignored, or pending tests. The
  imported helper baseline blob was identical on both ports.
- **Green evidence inspected**: Session `files\spark40-fixed-validation.log`
  identifies this worktree and JDK 17.0.19. The selected
  `FabricTestArtifactTrackerSuite` and `FabricArtifactNamesSuite` run completed
  with 44 succeeded, 0 failed, 0 canceled, 0 ignored, 0 pending, and 0 aborted
  suites. The named mixed-valid/malformed regression passed. The inspected
  Spark 4.1 fixed log also reports 44/44 for the same two suites.
- **Validation boundary**: This verifies the narrow fix and selected cleanup
  regressions. Completion of the remaining fixed-build compile/style,
  additional runtime tests, and codegen commands is not established here.
  No live Fabric validation was performed.
- **Reviewer changes**: Appended this resolution only. Original finding text
  and prior evidence remain intact; no source or index edits were made.
