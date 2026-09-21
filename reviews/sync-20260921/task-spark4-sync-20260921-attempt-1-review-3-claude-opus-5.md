## Review Summary

- **Round**: 3 only, attempt 1
- **Theme**: Edge cases and robustness — error handling, boundary conditions,
  concurrency, failure modes
- **Mode**: sequential (Round 3 slot 3 only; not a parallel three-slot run)
- **Model**: claude-opus-5 (Anthropic Opus slot)
- **Target**: spark4.0
- **Branch**: `sync/spark4.0-master-20260921`
- **HEAD**: `7251246d4513f597838bcd53402a9025943a4142`
- **MERGE_HEAD**: `714d365e71f6d2db5b7072094a4a3ad22485eb57`
- **Content baseline**: `7c1bf9eb56`
- **Reviewed index tree**: `24786fcc5fc28587e6e0f159db2ad60931b82d68`
- **Artifact**: `reviews/sync-20260921/task-spark4-sync-20260921-attempt-1-review-3-claude-opus-5.md`
- **Issues Found**: 2 Low
- **Verdict**: ISSUES_FOUND (two Low failure-signal gaps; no deletion-safety,
  header-resolution, service-parameter-atomicity, or concurrency defect found)

### Model coverage statement (do not mislabel)

This artifact is the Anthropic Opus slot for **Round 3 only**. Round 2's Gemini
slot did not execute: Gemini 3.8 / 3.7 / 3.6 returned backend HTTP 400, and the
parent performed a documented GPT fallback. The gauntlet's three-family gate
therefore remains **unfulfilled**. Nothing here constitutes Gemini coverage, a
parallel-mode round, or a completed multi-family review.

### Scope

The frozen staged sync candidate: 40 files, 3,697 additions, 202 deletions.
Reviewed for robustness only; merge-resolution fidelity and spec conformance
belong to Rounds 1 and 2. The artifact itself is not part of the reviewed tree.
No source edits, staging, commits, pushes, agent dispatch, or live cloud calls
were performed by this reviewer.

## Evidence Checklist

- [x] Read this worktree's `AGENTS.md`, plus the Round 1 and Round 2 artifacts
  in `reviews\sync-20260921\`, then re-derived every conclusion from current
  staged source per the gauntlet Independence Rule rather than inheriting the
  Round 1 fix verification.
- [x] Confirmed the reviewed content by hash rather than by trust:
  `git write-tree` = `24786fcc5fc28587e6e0f159db2ad60931b82d68`, and
  `git ls-files -s` gives `ServiceHeaderValues.scala` =
  `24db5ce6b1538e4c23a913a9f502c4cd8c5d9e8f`, `FabricArtifactCleanup.scala` =
  `fc9f27368c896bba8c5934d3824a7ef015d8442f`, `FabricTestArtifactTracker.scala`
  = `7fc54121fb8d8c14304098b38a82bddcc2c7a2ef`, and
  `FabricTestArtifactTrackerSuite.scala` =
  `a8d0676aa6b292a581843c09a6249c940fe19586` — each byte-identical to the
  master prerequisite and to the Spark 4.1 candidate.

### Batched, null, and heterogeneous headers through public paths

- [x] Traced the full public path `inputFunc` → `addHeaders` →
  `resolveServiceAuthHeaders` → `ServiceAuthHeaders.resolve`, and the new
  indirection `getHeaderStringValueOpt` / `getHeaderMapValueOpt` →
  `ServiceHeaderValues` (`CognitiveServiceBase.scala:508-517, 531-533, 549-551,
  590-604`).
- [x] **Null boundaries.** `getValueAnyOpt` returns
  `Option(row.get(row.fieldIndex(colName)))`, so a null cell is already `None`.
  `ServiceHeaderValues.values` then applies `.flatMap(value => Option(value))`
  *after* flattening, so a null element **inside** a batched array is dropped
  too, and an explicitly scalar-set `Left(null)` collapses to `None`. Verified
  against the suite's `("first", "en", Option.empty[String])` first row in
  `TextAnalyticsHeaderSuite`: the null key is skipped and the batch is sent
  with `batch-key`.
- [x] **Empty and zero-length boundaries.** An empty batched array yields an
  empty iterator → `None`. A whitespace-only credential is rejected by
  `find(ServiceAuthHeaders.nonBlank)` (`value != null && value.trim.nonEmpty`),
  so a blank value can never suppress a valid lower-priority credential. A
  header map whose entries all have null names or values collapses to empty via
  `collect { case (name: String, headerValue: String) => ... }` (a typed Scala
  pattern never matches `null`) plus `sanitizeHeaderMap`, and `find(_.nonEmpty)`
  then moves on rather than latching an empty map.
- [x] **Heterogeneous element types.** `mapValue` pre-scans each candidate map
  with `mapValue.exists { ... !isInstanceOf[String] ... }` and throws a
  parameter-named `IllegalArgumentException` before any header is emitted;
  `stringValue` throws on any non-`String` element. Both messages name the
  parameter and the expected column type and do not echo values. Confirmed the
  null-guard ordering is correct: `Option(name).exists(... )` treats a null key
  as "not a type violation" and lets the later `collect` drop it, so a null key
  cannot be misreported as a type error.
- [x] **Scala 2.13 collection shape.** `values` matches on
  `scala.collection.Seq`, not `scala.Seq`. On this port `scala.Seq` aliases
  `scala.collection.immutable.Seq`, so matching the 2.13 `immutable.ArraySeq`
  that Spark yields for an array column *and* the 2.12-style mutable wrappers
  requires exactly the written `scala.collection.Seq`. `scala.collection.Map` is
  likewise used for map columns, and `Map` is not a `Seq`, so a scalar-set map
  correctly takes the `Iterator.single` branch. Cross-checked against
  `build.sbt` / `environment.yml` for this branch's Scala and Spark pins.
- [x] **Auth precedence and fallback laziness under batching.**
  `lacksExplicitAuthCredential` changed from
  `getValueOpt(...).exists(nonBlank)` to `getHeaderStringValueOpt(...).isDefined`;
  these are equivalent because `stringValue` already terminates on
  `find(nonBlank)`. `fabricFallbackAuthHeader` remains a by-name parameter
  evaluated only after the embedded-credential step in `resolve`, so a Fabric
  token fetch (which can throw or block) is still never attempted while any
  higher-priority credential exists. `embeddedCredential` sorts by header name
  so mixed-case duplicates resolve deterministically.
- [x] **Documented semantics match observed behaviour.** The batching note added
  to `docs\Explore Algorithms\AI Services\Advanced Usage - Async, Batching, and
  Multi-Key.ipynb` states that each credential column uses its first non-blank
  value, header-map columns use their first non-empty map after null removal,
  maps from later rows are not merged, batching does not group by credential,
  and batch size 1 is required for per-row credentials. Each clause maps
  one-to-one onto `stringValue` / `mapValue`, and `TextAnalyticsHeaderSuite`
  asserts it end-to-end over a real loopback `HttpServer` for partial batches,
  batch size 1, manual array payloads, copy/save/load round-trips, and the
  submit-then-poll `AnalyzeHealthText` path. The per-batch credential collapse
  is therefore specified behaviour, not a silent failure mode, and is not filed
  as an issue.

### Service-parameter atomicity and persistence

- [x] Read the whole bridge: `Wrappable.scala` generated setters/getters and
  `.pyi` stubs, `core\src\main\python\synapse\ml\core\schema\Utils.py`
  (`_is_service_param`, `_service_param_name_for_argument`,
  `_validate_service_param_arguments`, `_service_param_value_to_java`,
  `_service_param_scalar_to_python`, `_set_params_via_setters`,
  `_transfer_params_from_java`, `_transfer_params_to_java`),
  `HasOpenAIResponseSchema.scala`, and `OpenAIPromptPythonOverrides.scala`.
- [x] **Rollback on partial failure.** `_set_params_atomically` dry-runs every
  service setter against `original_java_obj.copy(self._empty_java_param_map())`
  with `self._paramMap` swapped to `dict(original_param_map)`, restores both in
  `finally`, and only then replays the setters on the real object. A failure in
  the dry run therefore leaves the original JVM object and the original
  `_paramMap` *identity* untouched — which is exactly what
  `test_failed_atomic_update_preserves_pending_service_values` and
  `test_prompt_ordinary_updates_remain_atomic_without_jvm` assert via
  `assertIs(prompt._paramMap, original_param_map)`.
- [x] **Stale-value and alias boundaries.** `_transfer_params_from_java` now
  `continue`s for service params so a loaded stage cannot resurrect a stale
  Python-side scalar over a JVM column binding; the generated setters
  `self._paramMap.pop(self.<name>, None)` so a pending generic `set()` cannot
  later overwrite a named setter; and `_transfer_params_to_java` guards the
  default-pair path with `and not is_service_param`. Verified that generated
  wrappers never create this hazard for *unset* service params either, because
  `pyParamDefault` returns `None` for `ServiceParam`, so `hasDefault` is false.
  `validateServiceParamAliases` fails code generation if a `<name>Col` Param or
  a hand-written `get/set<Name>Col` would collide.
- [x] **Persistence round-trip.** `_service_param_scalar_to_python` falls back
  to `java_param.jsonEncode(Left(value))` and reads `["left"]`; confirmed
  against `ServiceParamJsonProtocol.eitherFormat` in
  `core\src\main\scala\com\microsoft\azure\synapse\ml\param\JsonEncodableParam.scala:17-22`,
  which writes exactly `JsObject("left" -> a.toJson)`. `HasOpenAIResponseSchema`
  adds the matching `_paramMap.pop(self.responseFormat, None)` so the schema
  setter cannot be shadowed. `test_named_service_updates_survive_save_and_load`
  covers both scalar and column bindings through `save`/`load`.
- [x] Reviewed `test_ServiceParamPythonBridge.py` and `PyCodegenSuite.scala`
  for error-path rather than happy-path assertions: `Py4JJavaError` on unset and
  wrong-binding getters, `TypeError` on `None` service arguments, `ValueError`
  on `text` + `textCol` in one call, preserved pending value after a failed
  named setter, and a JVM-call-count assertion that ordinary configuration does
  not touch the gateway.

### Fabric cleanup, preflight, and deletion safety

- [x] **Null / empty / mixed relation references.** Re-derived
  `references` (`FabricArtifactCleanup.scala:47-52`) and `item` (62-72) over the
  boundary matrix: outer `Some(JsNull)` and empty outer arrays still mean "no
  relations"; inside an entry, only a `Guid`-matching `JsString` or a *nonempty*
  nested object/array is accepted, and `Vector.flatMap` is strict, so a valid
  sibling GUID cannot short-circuit a malformed one. Errors name the field and
  never echo payload values. `item` is the sole `Item` constructor, so a
  malformed artifact aborts the read before `run` computes `initial`.
- [x] **Deletion safety on empty reference sets.** `safeStore` retains every
  store while any `SparkJobDefinition`/`Notebook` has `references.isEmpty`, so
  the conservative "missing edges are not proof of no consumers" rule still
  holds when the strict parser yields an empty set. `managedEndpoint` requires
  the neighbour to be an expired `SQLEndpoint` whose only neighbour is the
  candidate, so a newly created consumer (which cannot be expired) always blocks
  deletion. `canonicalReferences - canonicalId` prevents self-vouching.
  `confirmAbsent` performs 31 reads and 30 pauses, matching the suite's
  `assert(pauses == 30)` and the documented retry budget.
- [x] **Concurrency and interruption.** `Test / parallelExecution := false` in
  `build.sbt`, so `FabricSmokeTests` and `FabricNotebookTests` do not run
  concurrent workspace-wide cleanups in the single E2E sbt invocation. Within a
  suite, notebook work is bounded by `MaxConcurrency = 3`; `artifactIds` is a
  `ConcurrentLinkedDeque`; `shutdownExecutor` escalates `shutdown` →
  `shutdownNow` with two bounded 30 s waits and re-asserts the interrupt flag;
  `shutdownAndCleanup` still attempts artifact cleanup after a shutdown failure
  and restores interrupt status; `captureFabricSetup` / `getFabricSetup`
  correctly treat `InterruptedException` as fatal-to-`Try` and re-interrupt on
  every cached access. Verified the lazy-val ordering cannot touch `fabric`
  before `fabricWorkspaceId` is set: `artifactTracker`'s delete function is a
  closure, and `preparedStore` / `submissions` both call
  `ensureFabricPreflight()` first.
- [x] **Resource ownership.** `withArtifact` deletes in `finally`, tolerates
  `PowerBIEntityNotFound`, and deliberately leaves the ID queued when deletion
  fails so `cleanup()` retries it; `executorStarted` prevents `afterAll` from
  forcing the executor lazy val into existence when preflight failed.
  `FabricOperations.platform` became `lazy val` so `Secrets.Platform` is not
  read at construction.
- [x] **Preflight gating.** In `pipeline.yaml` the new task captures the sbt
  exit code, records `cleanup_step` transitions, copies the cleanup report into
  `$artifact_root/test-reports/`, and `exit`s with that code; the E2E task is
  `condition: succeeded()`; the always-run evidence step `mkdir -p`s the report
  directory and backfills `e2e_step=not-started`, which is what makes the new
  `failTaskOnMissingResultsFile: true` safe on an aborted run.
  `SYNAPSEML_FABRIC_CLEANUP_DRY_RUN` is validated against exactly `{"true",
  "false"}`, so a typo fails closed rather than silently enabling deletion.
- [x] Confirmed the port keeps its own adaptations in the reviewed robustness
  paths: Scala 2.13 collection handling, the zero-argument `super()` fixes in
  the prompt overrides, and `condition: false` on the branch Fabric E2E job, so
  the preflight wiring reviewed above does not authorise deletions from this
  branch today.

- [ ] No Scala compile, scalastyle, codegen, ScalaTest, or PySpark execution was
  performed in this review. The parent reports 44 cleanup, 30 codegen, and 36
  cognitive tests green plus 89 pipeline tests and Black over 215 files; those
  results were inspected, not reproduced here, and aggregate compile/style/
  codegen was still finishing.
- [ ] No live Fabric, Azure, Databricks, or GitHub call was made, and no remote
  CI run exists for this candidate. Real endpoint payload shapes were
  deliberately not assumed; see the recorded non-issue below.

## Explicitly considered and *not* raised

- **Per-batch credential collapse.** Row 1's credential authenticates the whole
  batch and later rows' credentials are dropped. Fully specified in the
  Multi-Key notebook note and asserted by `TextAnalyticsHeaderSuite`
  (`Seq("batch-key", "last-key")`, and `forall(_.headers(keyHeader) ==
  "key-one")` for `batchSize(10)`). Behaviour, not a defect.
- **Lazy `find` skips type validation of elements after the first usable one.**
  Reachable only with a genuinely heterogeneous array, which Spark's typed
  `array<string>` / `array<map<string,string>>` schema prevents. Constructing
  the counterexample would require inventing a column shape outside the public
  path.
- **Fail-closed blast radius of the GUID-only relation contract.** One foreign
  artifact with a non-GUID leaf aborts cleanup workspace-wide and, with the new
  gate, blocks E2E. This is the documented, intentional trade-off; judging its
  real-world likelihood would require asserting Fabric endpoint schema, which is
  out of scope by instruction. Recorded so the trade-off stays visible.
- **Missing relation field and blank `parentArtifactObjectId` both throw.**
  Unchanged context in the diff, inherited, and fail-closed.
- **Non-atomic `_set_params_via_setters` for non-`OpenAIPrompt` stages.**
  PySpark's own `Params._set` converts and assigns per key and is equally
  non-atomic, so this is not a regression introduced by the delta.

## Issues

### Issue 1: A mid-run inventory failure discards already-recorded deletion failures

- **Severity**: Low
- **File**: `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala`
- **Line(s)**: 212 (`val current = index(client.inventory())`), 228-232
  (`catch { case NonFatal(e) => failures :+= e ... }`), 238-241
  (`failures.headOption.foreach { first => ... throw first }`)
- **Description**: The per-candidate inventory re-read at the top of the loop
  body is outside any `try`. Every other failure source in the loop is
  protected — `deleteAndConfirm`, including the `confirmAbsent` inventory reads
  it performs, runs inside the `NonFatal` handler that appends to `failures`.
  If candidate *i* fails to delete (`failures = [e1]`) and the loop-head read
  for candidate *i+1* throws, that throwable propagates straight out of `run`,
  bypassing the aggregation at 238-241. `e1` is never rethrown and never
  attached via `addSuppressed`, and the closing summary line is skipped.
- **Coupling to this delta**: `FabricArtifactCleanup.scala` is newly added to
  this branch by the sync, and `FabricOperations.cleanupTestArtifacts` wires
  `inventory()` as `pages(...).map(FabricArtifactCleanup.item)`. The new
  strict-parser contract makes every loop-head read a throwing operation, which
  materially raises the probability of the interleaving that loses `failures`.
- **Risk**: Failure-signal quality only. No artifact is deleted that should
  have been retained — `failures.isEmpty && safeStore(...)` still blocks store
  deletion within the run, and each failure is still printed by
  `log(s"Fabric cleanup failed for ${candidate.id}: ...")`. The operator sees
  only the inventory error in the thrown exception, so a deletion failure can
  be missed when triaging a failed preflight that now gates the E2E job.
- **Test evidence for the gap**: `FabricTestArtifactTrackerSuite.scala:344-357`
  exercises an inventory failure only with `deleted.isEmpty` and no prior
  deletion failure; lines 450-455 exercise multi-delete-failure aggregation
  with no subsequent inventory failure. The combination is untested.
- **Suggested Fix**: Evaluate the loop-head read as
  `Try(index(client.inventory()))` and, on `Failure(e)`, append `e` to
  `failures` and stop iterating so the existing block at 238-241 throws the
  first failure with the rest suppressed, reusing the `filterNot(_ eq first)`
  guard already at line 239. Add a regression that fails one deletion and then
  throws from a later inventory read, asserting the thrown exception carries the
  deletion failure as suppressed.

### Issue 2: `FabricTestArtifactTracker.cleanup()` lacks the self-suppression guard its siblings received

- **Severity**: Low
- **File**: `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricTestArtifactTracker.scala`
- **Line(s)**: 64-67 (`failures.headOption.foreach { failure =>
  failures.tail.foreach(failure.addSuppressed); throw failure }`)
- **Description**: `Throwable.addSuppressed(e)` throws
  `IllegalArgumentException("Self-suppression not permitted")` when `e eq this`.
  Two other aggregators reachable from this same delta already guard against
  that: `withArtifact` (line 35) uses `if (original ne cleanupError)`, and
  `FabricArtifactCleanup.run` (line 239) uses `filterNot(_ eq first)`.
  `cleanup()` does not. If the injected `deleteArtifact` function surfaces the
  *same* `Throwable` instance for two tracked artifacts — a captured or memoized
  failure, or a wrapper that rethrows one stored error — `cleanup()` replaces
  both real failures with a confusing self-suppression error.
- **Coupling to this delta**: `FabricTestArtifactTracker.scala` is modified by
  this sync; `withArtifact` and `deleteTrackedArtifact` are new, and
  `cleanup()`'s loop body was rewritten to call `deleteTrackedArtifact`. The
  guard was added to the new sibling paths and to `FabricArtifactCleanup.run`
  but not to the aggregator in the same rewritten method, so the delta leaves an
  internal inconsistency rather than merely inheriting one.
- **Risk**: Low. The production injection is
  `artifactId => fabric.deleteArtifact(artifactId)`, whose HTTP path constructs
  a fresh exception per call, so the same instance is not expected today. The
  consequence if it does occur is a masked root cause during `afterAll` artifact
  cleanup, precisely when diagnosing leaked Fabric resources matters most.
  `FabricNotebookTests.shutdownAndCleanup` has the same unguarded pattern, but
  its two sources are distinct call sites and cannot yield one instance, so no
  separate issue is filed for it.
- **Test evidence**: `FabricTestArtifactTrackerSuite.scala:648-664` ("Attempt
  all deletions and preserve cleanup failures") deliberately uses two distinct
  instances (`firstFailure`, `secondFailure`) and asserts
  `thrown.getSuppressed.toSeq == Seq(firstFailure)`. No case exercises a
  repeated instance, so the guard's absence is invisible to the suite.
- **Suggested Fix**: Change line 65 to
  `failures.tail.filterNot(_ eq failure).foreach(failure.addSuppressed)`,
  matching `FabricArtifactCleanup.run`, and add a tracker regression whose
  `deleteArtifact` throws one shared instance for two tracked IDs, asserting
  the thrown exception is that instance with no suppressed entries. Apply the
  same one-line guard to `FabricNotebookTests.shutdownAndCleanup` for
  consistency.

## Resolution Log

### Issue 1

- **Status**: Open
- **What changed**: Nothing. This review contract forbids source edits,
  staging, commits, and pushes.
- **Why**: Round 3 is review-only for this run.
- **How verified**: Direct control-flow reading of `run` (lines 202-247) plus an
  explicit search of `FabricTestArtifactTrackerSuite.scala` for a combined
  deletion-failure-then-inventory-failure case, which is absent.

### Issue 2

- **Status**: Open
- **What changed**: Nothing.
- **Why**: Round 3 is review-only for this run.
- **How verified**: Side-by-side reading of the three aggregation sites
  (`FabricTestArtifactTracker.scala:35` and `:64-67`,
  `FabricArtifactCleanup.scala:238-241`) and of the existing tracker failure
  test at `FabricTestArtifactTrackerSuite.scala:648-664`.

## Resolution Addendum — bounded fix verification (2026-09-21)

Re-checked the two fixes only. Tree `3ec9f40372`; blobs `e92bc35a94` (cleanup),
`1e01591ffd` (tracker), `30748f8e0b` (suite) — identical across all three trees.

- **Issue 1 — Fixed.** The loop-head `index(client.inventory())` now sits in
  `try/catch NonFatal`, attaches prior `failures` via `filterNot(_ eq e)`, and
  rethrows that same instance, so no `deleteAndConfirm` runs after it.
- **Issue 2 — Fixed.** Tracker line 65 is now
  `failures.tail.filterNot(_ eq failure).foreach(failure.addSuppressed)`,
  matching `FabricArtifactCleanup.run` and `withArtifact`.
- **Regressions.** The new tests cover distinct *and* reused inventory
  throwables, only `staleJob` attempted with the store retained, and a shared
  tracker throwable with both attempts plus a drained queue.
- **Negative control.** `master-cleanup-round3-red.log`: 3 run, 1 pass, 2 fail
  with the predicted symptoms; the 803→800 reduction was semantics-preserving.
- **Green.** `spark40-cleanup-round3-green-v2.log`: scalastyle 0 errors at 800
  lines, 46/46 tests (44 + 2 new), all three trees; earlier `-green.log` files
  are scalastyle failures, not passes. **Verdict: CLEAN** for both issues;
  residual non-defect `FabricNotebookTests.scala:292` unchanged as recorded.
