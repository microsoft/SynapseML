## Review summary

Publication note: this prerequisite-specific directory preserves the separate
port review records. Paths in the original review describe its review-time
location. Machine-local prefixes were removed: artifact references are
repository-relative and locally retained validation logs are named by file.

- Round: 4
- Theme: Detailed correctness, data flow, type safety, and exception propagation
- Mode: sequential
- Model: gpt-6-astra
- Target: master companion, `fix/fabric-cleanup-relations-20260921`
- HEAD: `714d365e71f6d2db5b7072094a4a3ad22485eb57`
- Reviewed index tree: `15746e61f14873124f2d00c53aa74c1aa3cfb070`
- Artifact: `reviews\sync-20260921\task-spark4-sync-20260921-attempt-1-review-4-gpt-6-astra.md`
- Issues found: 1 Low
- Verdict: ISSUES_FOUND

## Evidence checklist

- [x] Applied the repository guide, branch context, code-review checklist, and required Round 4 prompt. Reviewed only the four-file companion delta and necessary surrounding cleanup code.
- [x] Checked every relation leaf, nullable outer collections, UUID normalization, single traversal, and the parser-contract documentation. The 36 mixed-metadata cases still exercise fail-closed rejection.
- [x] Traced candidate ordering, strict retention comparisons, inventory equality, confirmed-deletion bookkeeping, and the 31-read confirmation boundary.
- [x] Verified the R3 loop-head inventory guard and tracker same-instance suppression fix. Their original covered paths remain correct.
- [x] Read `master-cleanup-round3-green-v2.log`: Test/scalastyle reports zero errors; 46 tests succeeded with zero failures, canceled, ignored, or pending tests.
- [x] Executed two additional offline fake-client probes against this worktree's compiled cleanup helper under JDK 11. Both reproduced Issue 1 without contacting services or modifying repository source.
- [x] Compared current shared source blobs across all three candidates. Cleanup is `e92bc35a944023511bd7e2edf8d0a1008bf67edd`, tracker is `1e01591ffddf5202b04d9c2aa3e343cd05443798`, and suite is `30748f8e0b76a308bb0b41faf524d85511fc698f` in each.
- [x] Confirmed the index still matches the reviewed tree and no tracked unstaged changes exist.
- [ ] No live endpoint/schema review or cloud validation was performed. Broader master code is outside this companion review.

Evidence comes from locally retained validation logs that are not tracked in this repository: `master-cleanup-round3-green-v2.log`, `master-cleanup-round4-red.log`, and `master-cleanup-round4-green.log`.

## Issues

### Issue 1: Later job metadata failures discard earlier deletion errors

- Severity: Low
- File: `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala`
- Lines: 212-224, especially the unguarded `safeJob` call at 222; metadata calls at 151-152
- Classification: A remaining shared cleanup diagnostics defect, not a port merge mistake. The staged diff confirms this path predates the narrow fix; the new inventory guard does not cover it.
- Description: A failed DELETE accumulates its exception in `failures`. For the next owned job, `safeJob` calls `jobs` and `schedules` outside both exception handlers. If either throws, that exception exits `run` before final aggregation, dropping the earlier deletion exception.
- Risk: Cleanup still stops safely and retains stores, but the caller loses the earlier deletion failure's message and stack. The ordinary log records only its exception class.
- Suggested fix: Apply the same fail-fast diagnostic handling to candidate safety evaluation. Attach earlier failures except the thrown instance, then rethrow that same metadata exception immediately. Add job-history and schedule variants, including a reused exception, and assert no later DELETE occurs.

Concrete reproducer using the existing fake-client fixture shape:

1. Inventory contains an expired owned store and two unchanged expired owned jobs linked to it. Both jobs have terminal old history and no schedules.
2. The first job's DELETE throws exception A. Inventory remains unchanged.
3. The next inventory read succeeds. For the second job, make `jobs` throw exception B; repeat separately with `schedules` throwing B.
4. Expected: B escapes with A suppressed. Actual: B escapes with no suppressed exceptions. Only the first job's DELETE was attempted.

Actual offline output from the compiled master helper:

```text
REPRODUCED jobs: metadata error rethrown; suppressed deletion errors=0; DELETE attempts=job1 only
REPRODUCED schedules: metadata error rethrown; suppressed deletion errors=0; DELETE attempts=job1 only
```

The probes used reflective access to the existing package-private helper and an in-memory fake client. They loaded `core\target\scala-2.12\test-classes`, used cached dependencies, and did not run SBT or Spark.

## Resolution log

### Issue 1

- Status: Open
- What changed: No source changes. This artifact records the remaining diagnostic path.
- Why: The review is source-frozen and limited to Round 4.
- How verified: Direct control-flow inspection and both offline reproductions above. The 46-test green log does not include these additional metadata-failure sequences.

## Review boundary

Gemini 3.8, 3.7, and 3.6 attempts returned backend 400 errors and executed no review, as reported by the driver. Round 2 used an explicit direct-GPT fallback. The three-family gate remains unfulfilled. This artifact neither completes the gauntlet nor declares readiness. No agents, source edits, staging, commits, pushes, or remote calls were performed.

## R4 Issue 1 resolution verification

- Status: Fixed. Narrow resolution verdict: CLEAN.
- Verified staged blobs: cleanup `e7a385bf5896f5e1113f231c20cc3c81364ab44c`, tracker `1e01591ffddf5202b04d9c2aa3e343cd05443798`, expanded suite `490dcf1bb215719f0b537e47fb821c91f4d40909`, moved suite `321d004392a8763b0bc5ebb0d7dba8c391701787`. All four match across the three worktrees.
- Reviewed only the resolution delta against the original reviewed tree, not the full candidate.
- The Boolean safety evaluation now guards inventory, equality, job history/schedules, and store checks. Its NonFatal handler preserves prior failures except the thrown instance and immediately rethrows that same exception before further deletion.
- The regression enumerates three failed-read kinds with distinct/reused exceptions, asserting exception identity, exact suppression, only the first DELETE attempt, and retained store.
- The repeated-tracker-throwable test body moved unchanged to `FabricTestArtifactTrackerFailureSuite.scala`; the original suite is 796 lines and the new suite is 25.
- Inspected `master-cleanup-round4-red.log`: the expanded metadata test fails on missing suppression; three other selected tests pass.
- `master-cleanup-round4-green.log` showed startup only when inspected. Final style/test results were not yet available; no green result is claimed. The command includes both tracker suites and the names suite.
- Original finding text remains intact. This reviewer changed only the review addendum and did not rerun builds or tests.
- The three-family gate remains unfulfilled; this narrow resolution does not establish overall readiness.
