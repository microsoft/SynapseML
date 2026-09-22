## Review summary

- Round: 4
- Theme: Detailed correctness, data flow, type safety, and exception propagation
- Mode: sequential
- Model: gpt-6-astra
- Target: spark4.0 sync candidate
- HEAD: `7251246d4513f597838bcd53402a9025943a4142`
- Reviewed index tree: `3ec9f403724129eb8f48e4179ac9423ac58779c9`
- Artifact: `reviews\pr-2733\task-spark4-sync-20260921-attempt-1-review-4-gpt-6-astra.md`
- Issues found: 1 Low
- Verdict: ISSUES_FOUND

## Evidence checklist

- [x] Applied the repository guide, branch context, code-review checklist, and required Round 4 prompt. Inspected the current staged delta without repeating the ancestor/blob audit or historical reviews.
- [x] Checked strict relation recursion, nullable outer collections, UUID normalization, single traversal, candidate retention/equality checks, and confirmed-deletion bookkeeping.
- [x] Verified the R3 inventory-error and tracker same-instance fixes on their covered paths. Traced the remaining job metadata exception path separately.
- [x] Followed `Utils.py` argument validation and JVM transfers through generated setters/getters in `Wrappable.scala`. Checked scalar/column exclusivity, pending-map removal only after success, generated service-default omission, copy/save/load, and ordinary-param handling.
- [x] Traced `OpenAIPromptPythonOverrides.scala` scratch validation, restoration in `finally`, successful live updates, and `HasOpenAIResponseSchema.scala` pending-value removal.
- [x] Followed header values through `getValueAnyOpt`, `ServiceHeaderValues`, authentication precedence, lazy fallback, and TextAnalytics submission/polling. Payload arrays retain immutable-collection normalization and row alignment. Batch-size-one and partial-batch tests exercise the public path.
- [x] No additional concrete bridge/header defect found in this bounded review. Current focused bridge/header source blobs match the spark4.1 candidate.
- [x] Read `spark40-cleanup-round3-green-v2.log`: Test/scalastyle reports zero errors; 46 tests succeeded with zero failures, canceled, ignored, or pending tests.
- [x] Read `spark40-python-wheel-runtime.log`: 54 public Python tests and 160 subtests passed. Full compile, codegen, cognitive, pipeline, and Black results were supplied by the driver; they were not rerun here.
- [x] Executed two offline fake-client probes on the master companion's compiled helper. This candidate has the identical cleanup blob `e92bc35a944023511bd7e2edf8d0a1008bf67edd`, tracker blob `1e01591ffddf5202b04d9c2aa3e343cd05443798`, and suite blob `30748f8e0b76a308bb0b41faf524d85511fc698f`.
- [x] Confirmed the index still matches the reviewed tree and no tracked unstaged changes exist.
- [ ] The new probes were not separately executed on Spark 4.0 bytecode. No live Fabric validation was performed; Fabric E2E remains branch-disabled.

Evidence comes from locally retained validation logs that are not tracked in this repository: `spark40-cleanup-round3-green-v2.log`, `spark40-cleanup-round4-green.log`, `spark40-python-wheel-runtime.log`, and `master-cleanup-round4-red.log`.

## Issues

### Issue 1: Later job metadata failures discard earlier deletion errors

- Severity: Low
- File: `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala`
- Lines: 212-224, especially the unguarded `safeJob` call at 222; metadata calls at 151-152
- Classification: An imported shared diagnostics defect, not a merge-resolution regression. The master companion has the same remaining path outside its new inventory guard.
- Description: A failed DELETE accumulates its exception in `failures`. For the next owned job, `safeJob` calls `jobs` and `schedules` outside both exception handlers. If either throws, that exception exits `run` before final aggregation, dropping the earlier deletion exception.
- Risk: Cleanup still stops safely and retains stores, but the caller loses the earlier deletion failure's message and stack. The ordinary log records only its exception class.
- Suggested fix: Fix master first and carry the same fix by merge. Guard candidate safety evaluation with the same fail-fast diagnostic handling: suppress earlier failures except the thrown instance, then rethrow that same metadata error. Cover job-history and schedule failures, reused errors, and no subsequent DELETE.

Concrete reproducer using the existing fake-client fixture shape:

1. Inventory contains an expired owned store and two unchanged expired owned jobs linked to it. Both jobs have terminal old history and no schedules.
2. The first job's DELETE throws exception A. Inventory remains unchanged.
3. The next inventory read succeeds. For the second job, make `jobs` throw exception B; repeat separately with `schedules` throwing B.
4. Expected: B escapes with A suppressed. Actual: B escapes with no suppressed exceptions. Only the first job's DELETE was attempted.

Actual offline output from the identical master helper under JDK 11:

```text
REPRODUCED jobs: metadata error rethrown; suppressed deletion errors=0; DELETE attempts=job1 only
REPRODUCED schedules: metadata error rethrown; suppressed deletion errors=0; DELETE attempts=job1 only
```

The probes used the companion's existing compiled helper, cached dependencies, and an in-memory fake client. They did not contact services, start Spark, or modify source.

## Resolution log

### Issue 1

- Status: Open
- What changed: No source changes. This artifact records the remaining diagnostic path.
- Why: The review is source-frozen and limited to Round 4.
- How verified: Direct control-flow inspection, shared-blob equality, and both master offline reproductions. The existing 46-test green log does not cover these sequences.

## Review boundary

Gemini 3.8, 3.7, and 3.6 attempts returned backend 400 errors and executed no review, as reported by the driver. Round 2 used an explicit direct-GPT fallback. The three-family gate remains unfulfilled. This artifact neither completes the gauntlet nor declares readiness. No agents, source edits, staging, commits, pushes, or remote calls were performed.

## R4 Issue 1 resolution verification

- Status: Fixed. Narrow resolution verdict: CLEAN.
- Verified staged blobs: cleanup `e7a385bf5896f5e1113f231c20cc3c81364ab44c`, tracker `1e01591ffddf5202b04d9c2aa3e343cd05443798`, expanded suite `490dcf1bb215719f0b537e47fb821c91f4d40909`, moved suite `321d004392a8763b0bc5ebb0d7dba8c391701787`. All four match across the three worktrees.
- Reviewed only the shared resolution delta and its equality with master, not the full candidate.
- The Boolean safety evaluation now guards inventory, equality, job history/schedules, and store checks. Its NonFatal handler preserves prior failures except the thrown instance and immediately rethrows that same exception before further deletion.
- The regression enumerates three failed-read kinds with distinct/reused exceptions, asserting exception identity, exact suppression, only the first DELETE attempt, and retained store.
- The repeated-tracker-throwable test body moved unchanged to `FabricTestArtifactTrackerFailureSuite.scala`; the original suite is 796 lines and the new suite is 25.
- Inspected `master-cleanup-round4-red.log`: the expanded metadata test fails on missing suppression; three other selected tests pass.
- `spark40-cleanup-round4-green.log` showed startup only when inspected. Final style/test results were not yet available; no green result is claimed. The command includes both tracker suites and the names suite.
- Original finding text remains intact. This reviewer changed only the review addendum and did not rerun builds or tests.
- The three-family gate remains unfulfilled; this narrow resolution does not establish overall readiness.
