# Round 6 - Polish & Hardening

- **Round:** 6
- **Theme:** Polish & Hardening (verified bugs, performance, logging/no-false-success, documentation accuracy)
- **Mode:** Sequential `review-code`; strictly read-only
- **Model:** Claude Opus 5 (`claude-opus-5`)
- **Base:** OSS `master` at `cd45147c7025f483e86fc028069d72b070e73a55`
- **Branch:** `fix/fabric-test-cleanup-24h-20260918` (uncommitted working tree)
- **Issues Found:** 0
- **Verdict:** NO_ISSUES_FOUND

Paths are repository-relative; machine-specific prefixes, workspace/tenant identifiers, and account names are omitted.

## Scope reviewed

| File | State |
| --- | --- |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala` | new, untracked (245 lines) |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/fabric/FabricOperations.scala` | modified (+28/-8) |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricNotebookTests.scala` | modified (+17/-9) |
| `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricTestArtifactTrackerSuite.scala` | modified (+314) |
| `docs/Reference/Developer Setup.md` | modified (+28) |

No `src/main`, public API, dependency, `pipeline.yaml`, or `.github/workflows/` change is present in the diff.

## Findings

No significant issues found in the reviewed changes.

## Evidence checklist

### Prior-round regressions

- [x] **Round 1 / mixed-case UUIDs** - `item` canonicalizes the artifact ID, relation IDs, `parentArtifactObjectId`, and `extendedProperties.Default{Lakehouse,Warehouse}ArtifactId` through `java.util.UUID`, and removes the self-edge (`FabricArtifactCleanup.scala:83-88`). Regression present at `FabricTestArtifactTrackerSuite.scala:292-302`.
- [x] **Round 1 / endpoint age** - `managedEndpoint` now requires `i.expired(cutoff)` (`FabricArtifactCleanup.scala:155-157`); the four-case retention regression is present (`FabricTestArtifactTrackerSuite.scala:283-290`).
- [x] **Round 3 #1 / JDK 11 offsets** - parsing is explicit `OffsetDateTime` then unzoned-UTC `LocalDateTime` (`FabricArtifactCleanup.scala:41-44`); absent timestamps yield `None` and retain the item via `expired` (`:29-30`).
- [x] **Round 3 #2 / graph-independent store guard** - `safeStore` fails closed when any owned job remains **or** any `SparkJobDefinition`/`Notebook` has empty references (`FabricArtifactCleanup.scala:181-183`); both regressions present (`FabricTestArtifactTrackerSuite.scala:231-237`).
- [x] **Round 3 #3 / not-found tolerance and aggregation** - `deleteAndConfirm` absorbs `PowerBIEntityNotFound` and still requires absence confirmation (`:186-194`); errors are collected per candidate (`:218-221`).
- [x] **Round 3 #4 / pagination** - `index` de-duplicates identical rows and fails only on conflicting IDs (`:139-143`); the adapter builds a single-slash inventory URL via `sspHost.stripSuffix("/")` (`FabricOperations.scala:69-71`), which is required because `FabricConnection.sspHost` is forced to end in `/` and `metadataUri` therefore contains a double separator.
- [x] **Round 4 / rethrow placement** - `failures.headOption.foreach { ... throw first }` is outside the candidate loop (`FabricArtifactCleanup.scala:226-229`). Traced: a first-job failure still allows the second job to be attempted, while `failures.isEmpty` (`:207`) keeps every store protected.
- [x] **Round 5 / test fake fidelity** - per-ID `jobHistory` exists (`FabricTestArtifactTrackerSuite.scala:38,44`), with the mixed idle/active shared-store regression (`:116-122`), the persistently-visible not-found case asserting 30 pauses and one DELETE (`:265-279`), positive `continuationUri` **and** `@odata.nextLink` coverage (`:206-211`), explicit Lakehouse/Warehouse ownership (`:73-80`), and multi-error aggregation asserting primary message plus suppressed (`:252-262`).
- [x] **Round 5 / scalastyle complexity** - the `cursor` helper is extracted (`FabricArtifactCleanup.scala:91-95`) and `pages` retains a single `@tailrec read` in tail position.

### Independent verification this round

- [x] Hand-traced the full deletion gate for both candidate kinds. A store reaches `client.delete` only when all of: `ownedStore` against the initial snapshot; `expired` (created **and** updated strictly before cutoff **and** `provisionState == "Active"`); `unchanged` against re-read inventory modulo already-deleted refs; `failures.isEmpty`; no owned job anywhere; no SJD/Notebook with empty refs; every neighbor an exclusive, expired, managed `SQLEndpoint`. A job additionally requires `idle` (all history terminal with `endTimeUtc` before cutoff, all schedules explicitly `enabled:false`) and that every neighbor is an owned, expired store whose other neighbors are owned jobs or managed endpoints. I could not construct a path that deletes an unowned, unexpired, or still-referenced item.
- [x] Verified no unguarded `Map.apply` in `neighbors`: `ownedStore` is guarded by `initial.contains(id)` short-circuit (`:175`), and both `safeJob`/`safeStore` candidate lookups are guarded by the preceding `unchanged` conjunct (`:206-208`).
- [x] Verified the confirmation contract arithmetic: `check(31)` yields 31 inventory reads and 30 pauses before `require(remaining > 1)` fails, with exactly one `client.delete` per candidate (`:159-170`).
- [x] Verified no false-success path. `deleted` is appended only after `deleteAndConfirm` returns; `"confirmed deletion"` is logged only on that path; dry run returns empty and logs `"would delete"`; `InterruptedException` is not `NonFatal`, so interrupts propagate rather than being swallowed; any collected failure is rethrown before the method returns. `addSuppressed` self-suppression is prevented by `filterNot(_ eq first)`.
- [x] Verified ownership cannot be widened by the legacy paths: `createSJDArtifact(path, artifactType)` is only ever invoked with `"SparkJobDefinition"`, and `FabricArtifactNames.store` always emits the 14-digit + 32-hex form matched by `UniqueStore`. Ownership uses exact string equality on descriptions, so descriptions that merely *contain* the legacy text do not match.
- [x] Verified the `lazy val platform` change is load-bearing: `Secrets.Platform` and `Secrets.ArtifactStore` are Key Vault lookups used only by `updateSJDArtifact`/`createStoreArtifact`/`getSparkJobDefinitionLink`, so the cleanup-only path no longer requires them. This substantiates the documented claim that cleanup needs only the Fabric integration environment variables.
- [x] Verified style limits statically: `scalastyle-config.xml` sets `maxLineLength=120`; the longest changed lines are 117 (`FabricArtifactCleanup.scala:22`) and 119 (`FabricOperations.scala:79`). No method in the new file exceeds the 60-line `MethodLengthChecker` limit.
- [x] Verified every sentence of the new `docs/Reference/Developer Setup.md` section against the implementation: 24h/UTC and strict both-timestamps rule, dry-run variable and accepted values, ownership recognition including the legacy-lakehouse `linked` requirement, jobs-before-stores ordering, 31 checks at 2 seconds, unconfirmed/failed deletion blocking stores, independent job attempts with deferred aggregate failure, SQL endpoints deferred to cascade, and the fail-the-cleanup error classes. All statements are accurate. The preview caveat ("a preview can omit lakehouses whose job definitions have not yet been deleted") is correct because `safeStore`'s owned-job guard cannot be satisfied during a dry run.
- [x] Verified `pipeline.yaml` still targets the class `FabricTestCleanup` (lines 355, 363), so the renamed test case does not break CI selection. No pipeline or workflow edit is in the diff.
- [x] Verified `reviews/` is tracked in git (71 tracked files), so the five new round artifacts follow existing repository convention rather than leaking scratch files into the PR.

### Live-service coverage (unchanged limits, plus one correction)

- [ ] **Evidence correction - the retained live preview log predates the current build.** The captured preview emits `Fabric cleanup examined 24 items and confirmed 0 deletions; dryRun=true`, whereas the current `run` emits `examined N items, found J owned jobs and S owned stores, and confirmed D deletions`. The preview therefore validates an earlier revision, not this working tree. Re-running the dry-run preview on the exact final checkout before any real deletion is the remaining validation step; the new owned-job/owned-store counters are what will distinguish "nothing owned" from "owned but not yet expired".
- [x] The preview does establish that `https://api.fabric.microsoft.com/v1/...` is reachable with this authenticated client, since workspace resolution in `FabricTestConstants.getIntegrationWorkspaceId()` succeeds against that host. This retires Round 3's open auth-audience question for the Fabric host.
- [ ] `Client.jobs` (`/jobs/instances`) and `Client.schedules` (`/jobs/sparkjob/schedules`) remain unexercised: zero candidates reached `idle`. Their response shapes and the `sparkjob` job type are unproven live.
- [ ] No real `DELETE` was issued; `deleteAndConfirm`, `confirmAbsent`, and cascade behaviour for `SQLEndpoint` are proven only against the in-memory fake.
- [ ] The inventory endpoint returns a bare JSON array in practice (consistent with the pre-existing `listArtifacts()` shape and the single-page preview), so the `JsObject` continuation branches of `pages` are covered by unit tests only.
- [ ] Targeted suite, full compile, and scalastyle were reported as in flight at handoff and were **not** run by this review (read-only constraint). Style conclusions above are static line/limit checks, not executed scalastyle output.

## Notes on deliberately unreported observations

Per the round contract, the following were evaluated and intentionally **not** raised: the abort-on-unmodelled-inventory policy (including all four relation fields being mandatory), the conservatism of retaining stores whenever any `SparkJobDefinition`/`Notebook` lacks reference edges, per-candidate re-inventory cost (bounded by a 24-item workspace and dominated by API latency), and the summary line being skipped when the aggregate failure is rethrown. Each is either documented intended behaviour, fails in the safe direction, or depends on live-service shapes that this round could not verify.

## Driver follow-up

The 31 targeted tests passed on JDK 11. Executed scalastyle still reported pagination complexity 11 despite the cursor extraction; extracted the unchanged URL guard into `validatePage`. Both positive and hostile-pagination tests cover that guard. Final executed validation and live preview results follow below.

Verified the adapter against the official [job history](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances) and [schedule](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-schedules) contracts. The documented job history returns `value`, `status`, and `endTimeUtc`; schedules return `value` and `enabled`. The official [Spark Job Definition endpoint](https://learn.microsoft.com/en-us/rest/api/fabric/sparkjobdefinition/background-jobs/run-on-demand-spark-job-definition) confirms `sparkjob`. This is documentation verification, not live execution of those endpoints.

Final local execution passed on JDK 11: 31 tests across `FabricTestArtifactTrackerSuite` and `FabricArtifactNamesSuite`, all-module `scalastyle` and `Test/scalastyle`, and full `compile` and `Test/compile`. Pinned Black 22.3.0 reported 206 files unchanged. No dependency pins, workflows, or generated files were edited.

Re-ran both preview and authorized execute mode on the final source tree against the configured live integration workspace. Each examined 24 items and found zero owned jobs and zero owned stores; execute mode confirmed zero deletions. Both cleanup-suite runs passed. No unrelated items were deleted, and this run did not reclaim capacity. Actual DELETE, deletion confirmation, job history, and schedules remain covered by deterministic tests and documented API contracts, not live candidate execution.
