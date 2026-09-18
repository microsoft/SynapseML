# Round 5 - Testing & Coverage

**Model:** Gemini 3.8 Flash (`gemini-3.8-flash`)
**Mode:** Sequential `review-code`; read-only
**Status:** COMPLETE
**Issues:** 2 Medium
**Verdict:** ISSUES_FOUND
**Base:** OSS `master` at `cd45147c7025f483e86fc028069d72b070e73a55`
**Worktree:** `.worktrees/fabric-test-cleanup-20260918`

## Evidence checklist

- [x] Reviewed uncommitted diffs across `FabricOperations.scala`, `FabricNotebookTests.scala`, `FabricTestArtifactTrackerSuite.scala`, `docs/Reference/Developer Setup.md`, and the new untracked `FabricArtifactCleanup.scala`.
- [x] Verified Round 4 fix: the aggregate rethrow (`failures.headOption.foreach`) was moved **after** the candidate loop (`FabricArtifactCleanup.scala:210-213`), permitting independent job deletions to proceed while all stores remain protected.
- [x] Verified prior round fixes: `OffsetDateTime` parses numeric offsets on JDK 11 with fallback to unzoned UTC (`FabricArtifactCleanup.scala:37-39`), global store guards fail closed if any owned job remains or any notebook/job has empty references (`FabricArtifactCleanup.scala:168-169`), single-slash inventory URL (`FabricOperations.scala:69`), and identical duplicate row deduplication with conflicting-ID abort (`FabricArtifactCleanup.scala:128-132`).
- [x] Evaluated test fake (`CleanupClient`) behavior against real service invariants across all 7 dimensions:
  1. **Exact boundary & UTC:** `expired` strictly enforces `isBefore(cutoff)` (`FabricArtifactCleanup.scala:23`); unit tests cover item timestamps at the 24h boundary and numeric offsets (`+02:00`).
  2. **Malformed metadata explicit abort:** Missing/invalid relation shapes, bad parent IDs, and non-GUID references throw `IllegalArgumentException` and abort the sweep.
  3. **Active jobs shared dependents:** The test fake uses a single shared `var history`, masking multi-job scenarios with heterogeneous execution status.
  4. **Pagination:** `pages` enforces HTTPS, origin authority, exact path, and visited loop prevention; positive multi-page tests cover `continuationToken` query encoding, but `continuationUri` and `@odata.nextLink` are only tested in negative rejection blocks.
  5. **Mutable state:** `CleanupClient.remove` artificially mutates `references - id` on remaining in-memory items upon deletion, masking potential discrepancies if inventory relations are not automatically cleaned by the service.
  6. **Errors:** Deletion failures aggregate into `failures`, independent jobs proceed, and stores fail closed via `failures.isEmpty`.
  7. **Not-found:** `PowerBIEntityNotFound` falls through to `confirmAbsent`, but tests only verify immediate absence.
- [x] Confirmed live inventory preview examined 24 items with zero eligible, leaving real `DELETE`, `/jobs/instances`, and `/schedules` endpoints unexercised against live service.
- [x] Strictly read-only: no edits, nested agents, builds, live API calls, or deletions performed.

## Findings

### 1. Test fake `CleanupClient` masks active-job store protection due to uniform global history

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTrackerSuite.scala:39-40`
**Severity:** Medium

**Problem:** In `CleanupClient`, `override def jobs(id: String): Vector[JsValue] = history` returns a single mutable `history` vector for all jobs. Because all jobs in any test run receive the exact same status, the suite cannot model or test multiple owned jobs with heterogeneous execution states (e.g. Job 1 is stale/idle, while Job 2 referencing the same store is actively running `InProgress`). Consequently, the suite lacks regression coverage verifying that when multiple jobs share a store, an active sibling job prevents store deletion while the idle job is cleaned.

**Evidence:** In `FabricArtifactCleanup.scala:159-166`, `safeJob` allows deleting an idle job if its store's other dependents are `ownedJob(j)`. When Job 2 is `InProgress`, `safeJob` succeeds for Job 1, Job 1 is deleted, and Job 2 is retained. Store deletion must then be blocked by `!current.values.exists(ownedJob)` (`FabricArtifactCleanup.scala:168`). Because `CleanupClient` returns identical history for all jobs, this multi-job coexistence invariant is unverified in unit tests.

**Suggested fix:** Parameterize `jobs` in `CleanupClient` by `id` (e.g., `var jobHistory: Map[String, Vector[JsValue]]`) and add a regression test with an expired completed job and an active `InProgress` job sharing an expired store, asserting that the idle job is deleted while the active job and store are retained.

### 2. Missing fail-closed regression test for unconfirmed `PowerBIEntityNotFound` (replica lag / false-404)

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTrackerSuite.scala:216-224`
**Severity:** Medium

**Problem:** `deleteAndConfirm` (`FabricArtifactCleanup.scala:177-183`) catches `PowerBIEntityNotFound` and proceeds to `confirmAbsent`. The test suite only exercises the happy path where `super.delete(id)` immediately strips the item from inventory, making `confirmAbsent` succeed instantly. The suite lacks a test for the failure branch where `client.delete` throws `PowerBIEntityNotFound` but the item remains visible in inventory (e.g. replica lag or false not-found), which must poll, throw `IllegalArgumentException`, aggregate into `failures`, and retain stores.

**Evidence:** In `test("Confirm concurrent not-found deletions...")` (`FabricTestArtifactTrackerSuite.scala:216-223`), `raced.delete` calls `super.delete(id)` before throwing `PowerBIEntityNotFound`. `super.delete` calls `remove(id)`, instantly emptying `items`. There is no test verifying that if the entity remains present in inventory after `PowerBIEntityNotFound`, bounded retries are exhausted and store deletion is halted.

**Suggested fix:** Add a test case where `delete(id)` throws `PowerBIEntityNotFound` without removing the item from `items`, asserting that `confirmAbsent` retries to exhaustion, the resulting `IllegalArgumentException` is captured in `failures`, and dependent stores remain untouched.

## Resolutions

1. Added per-ID job histories and a shared-store regression proving an idle job is deleted while its active sibling and their store remain.
2. Added a not-found response with persistently visible inventory, asserting 30 pauses, one DELETE attempt, an explicit failure, and retained stores.
3. Added positive URI/next-link pagination, explicit ownership for lakehouses/warehouses, and multiple-error aggregation assertions. Extracted the pagination cursor helper to meet the existing scalastyle complexity limit without changing behavior.
