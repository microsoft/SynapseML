## Review summary

- **Round:** 3
- **Theme:** Edge Cases & Robustness
- **Mode:** sequential
- **Model:** claude-opus-5
- **Base:** OSS `master` at `cd45147c7025f483e86fc028069d72b070e73a55`
- **Worktree:** `fabric-test-cleanup-20260918` (local task checkout)
- **Issues Found:** 4
- **Verdict:** ISSUES_FOUND

Paths are repository-relative; machine-specific prefixes were removed.

## Evidence checklist

- [x] Read `AGENTS.md`, the four tracked diffs, and the untracked `FabricArtifactCleanup.scala` in full.
- [x] Traced the surrounding integration surface: `FabricConnection`, `FabricAuthenticatedHttpClient`, `FabricTokenProvider`, `FabricSchemas`, `RESTHelpers.safeSend/sendAndParseJson`, `FabricTestArtifactTracker`, `FabricNotebookTests.isTestArtifactName`.
- [x] Checked prior rounds 1-2 artifacts; no finding below repeats them (mixed-case UUIDs and endpoint age are resolved in the current code).
- [x] Verified boundary logic by trace: exact-cutoff retention, `Option` timestamps, `state == "Active"`, 31 reads / 30 pauses, jobs-before-stores ordering, dry-run suppression, `deleted` subtraction in `expected`.
- [x] Verified no `Map.apply` on an absent key in `neighbors` (every call site is guarded by `unchanged`, `current.get`, or `initial.contains`).
- [x] Verified CI JDK pins: `pipeline.yaml:935` schedules the suite in `UnitTests`, whose JDK comes from `templates/update_cli.yml:7-11` (**8**); `.github/workflows/pr-validation.yml:35-39` pins **11**; the local-setup skill mandates **11**. Only JDK **17** is installed on this machine.
- [x] Inspected `java.base/java/time/format/DateTimeFormatterBuilder.java` from the local JDK 17 `lib/src.zip`: `InstantPrinterParser.parse` builds its parser with `.appendOffsetId()`.
- [x] Read-only: no edits, nested agents, builds, deletions, or service calls.
- [ ] Not executed: no JDK 8/11 runtime exists locally, so Finding 1 is a source-level trace plus CI-pin evidence, not an executed failure.
- [ ] Not exercised: `Client.jobs`/`Client.schedules` hit `api.fabric.microsoft.com` while every other call uses the internal `metadataUri` host. The live preview found zero eligible items, so `idle()` never ran; auth audience and response shape for those two endpoints remain unproven.

## Findings

### 1. Offset timestamps fail on the CI JDKs, and `timestamp` aborts the sweep instead of reporting "unknown"

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala:41-44`, test at `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTrackerSuite.scala:237-238`
**Severity:** High
**Problem:** `Try(Instant.parse(s)).getOrElse(LocalDateTime.parse(s).toInstant(ZoneOffset.UTC))` relies on `Instant.parse` accepting a numeric offset. `DateTimeFormatter.ISO_INSTANT` only gained offset parsing in JDK 12; on JDK 8/11 the instant parser appends a literal `'Z'`, so `Instant.parse("2026-09-17T13:00:00+02:00")` throws, and the fallback `LocalDateTime.parse` then throws too on the trailing `+02:00`. The new assertion `FabricArtifactCleanup.item(offset) == staleJob` therefore fails wherever this suite is actually scheduled. Separately, this makes `timestamp` non-total: any third date format on *any* artifact in the workspace, owned or not, aborts the whole cleanup, contradicting the deliberate design in which an unknown timestamp is `None` and merely retains the item (`Item.created/updated: Option[Instant]`, `expired` at line 30, and the "unknown timestamps" case in the suite).
**Evidence:** Local JDK 17 `lib/src.zip` shows `.appendOffsetId()` in `InstantPrinterParser.parse`, which is why the author's local run passes; JDK 17 is the only JDK on this machine. `pipeline.yaml:935` runs `FabricTestArtifactTrackerSuite` in the `UnitTests` job, whose `templates/update_cli.yml:7-11` pins `versionSpec: '8'`; GitHub Actions pins 11; the repo's local-setup skill pins 11. Round 2's note confirms the offset case was added after that round, so it has not yet been validated on a pinned JDK.
**Suggested fix:** Parse explicitly: try `OffsetDateTime.parse(s).toInstant`, then `LocalDateTime.parse(s).toInstant(UTC)`, and return `None` when every attempt fails, so an unexpected format retains the item rather than ending the run. Keep the offset assertion; it then passes on 8, 11, and 17.

### 2. A lakehouse can be deleted while a retained, still-running owned job uses it

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala:180-183`
**Severity:** Medium
**Problem:** Jobs get an independent activity check (`idle`, lines 145-152); stores get none. `safeStore` derives safety purely from the reference graph, and `forall` over an empty neighbor set is `true`. `item` explicitly accepts an artifact with no relation data (`case Some(JsNull) => Set.empty` at line 66, `case None | Some(JsNull) => Set.empty` for `extendedProperties` at lines 75-77), so an inventory that does not surface the job-to-lakehouse edge yields zero neighbors, which `safeStore` reads as "nothing depends on this". A >24h job that is still `InProgress` is correctly retained by `idle`, and in the same sweep its store is deleted underneath it.
**Evidence:** Trace with `J = {kind: SparkJobDefinition, owned, expired, references: empty}` plus a non-terminal job instance, and `S = {kind: Lakehouse, owned, expired, references: empty}`: `safeJob` returns false (J retained), then for S `neighbors(S, current) == empty` makes `safeStore` true and reaches `client.delete(S)`. This is untested because every suite fixture wires `references` explicitly (`FabricTestArtifactTrackerSuite.scala:20-24`). The reachability is real: the only place this repo records a job's default store is the `workloadPayload` string built in `FabricOperations.updateSJDArtifact`, and the cleanup deliberately does not read it (asserted at `FabricTestArtifactTrackerSuite.scala:234`); whether the inventory exposes `extendedProperties.DefaultLakehouseArtifactId` was not confirmed by the 24-item live preview.
**Suggested fix:** Give store deletion a guard that does not depend on the graph being populated, e.g. skip all store candidates when any owned job candidate in this run was retained or unconfirmed. Add a regression where a non-idle owned job has an empty `references` set and assert the store survives.

### 3. The first delete failure ends the whole sweep, including the already-deleted race this repo already tolerates

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala:206`
**Severity:** Medium
**Problem:** `client.delete` is called inside `foreach` with no per-candidate error handling, so one failure skips every remaining candidate. The sibling code this change sits next to takes the opposite approach on purpose: `FabricTestArtifactTracker.cleanup` (`core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTracker.scala:24-31`) swallows `PowerBIEntityNotFound` as "already deleted" and aggregates other failures so every artifact is still attempted. `FabricArtifactCleanup` has no such tolerance, so a benign race or one permanently undeletable item blocks all later candidates on every future run, the accumulation this change exists to stop.
**Evidence:** `pipeline.yaml:355-364` runs `FabricTestCleanup` first in every Fabric E2E run; two overlapping runs iterate the same id-sorted candidate list, so between one run's re-inventory (line 193) and its `client.delete` the other run can remove the same item, and `deleteArtifact` then throws the `PowerBIEntityNotFound` `RuntimeException` the tracker documents. Because the pipeline passes cleanup and the E2E suites as two separate `testOnly` commands in one `sbt` invocation, a failed first command stops sbt before `FabricSmokeTests`/`FabricNotebookTests` run at all.
**Suggested fix:** Treat not-found as a confirmed deletion, matching the tracker, and collect per-candidate failures to rethrow after the sweep instead of aborting it, while keeping the existing rule that an unconfirmed job deletion prevents deleting that job's store.

### 4. Inventory pagination is fatal on duplicate ids and on a normalized continuation path

**File:** `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala:137-139` and `:88-100`
**Severity:** Low
**Problem:** `index` requires globally distinct ids, and it is re-applied on every confirmation read (line 160), so a duplicate entry, a normal consequence of continuation-token paging over a workspace that is changing, aborts the sweep *after* a DELETE was issued, leaving the dependent store orphaned and the build red. Separately, `pages` demands `uri.getPath == origin.getPath`, but the inventory origin is `s"$sspHost/metadata/..."` where `sspHost` is forced to end with `/` (`core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/FabricConnection.scala:49-56, 64-65`), giving an origin path of `//metadata/workspaces/<id>/artifacts`; a service-supplied `continuationUri` carrying the normalized single-slash path fails the guard.
**Evidence:** Read directly from `index`, `confirmAbsent`, and the `require` in `pages`; the double separator is visible in `metadataUri`/`artifactsUri`. Both paths are reachable only if that endpoint actually paginates, which the single-page 24-item preview could not exercise, hence Low.
**Suggested fix:** Drop exactly-equal duplicates and fail only when two entries share an id with differing content; compare page paths in a way that tolerates the duplicated separator this repo's `metadataUri` produces.

## Notes

- No `src/main` or public API change; `pipeline.yaml` and workflows are untouched. `lazy val platform` correctly defers `Secrets.Platform` for the cleanup-only path.
- Boundary, dry-run, re-inventory, confirmation-bound, interrupt, and fail-closed relation parsing all behave as documented in `docs/Reference/Developer Setup.md`; the doc's retry numbers match the code.

## Driver resolutions and evidence corrections

1. The parent validation actually uses JDK 11 in WSL, not the reviewer's JDK 17 environment. It reproduced the offset regression with a `DateTimeParseException`. Changed parsing to `OffsetDateTime`; missing timestamps retain items, while malformed inventory still fails explicitly by design.
2. Store deletion now requires no remaining owned jobs and no notebook/job with unknown reference edges. Added missing-edge active/foreign consumer regressions.
3. Concurrent not-found responses still require absence confirmation. Other deletion errors are aggregated while independent jobs are attempted; any failure retains stores and is rethrown afterward. Added race and independent-job regressions.
4. Identical duplicate rows are deduplicated; conflicting rows still fail closed. The adapter builds a single-slash inventory URL, preserving exact same-host/path pagination checks. Added identical/conflicting-row coverage.
