## Review Summary

Publication note: this prerequisite-specific directory preserves the separate
port review records. Paths in the original review describe its review-time
location. Machine-local prefixes were removed: artifact references are
repository-relative and locally retained validation logs are named by file.

- **Round**: 3 only, attempt 1, master companion
- **Theme**: Edge cases and robustness — error handling, boundary conditions,
  concurrency, failure modes
- **Mode**: sequential (Round 3 slot 3 only; not a parallel three-slot run)
- **Model**: claude-opus-5 (Anthropic Opus slot)
- **Target**: master
- **Branch**: `fix/fabric-cleanup-relations-20260921`
- **HEAD**: `714d365e71f6d2db5b7072094a4a3ad22485eb57`
- **Reviewed index tree**: `e8d864108bf2b8f285890e794604ee464bed2969`
- **Artifact**: `reviews/pr-2732/task-spark4-sync-20260921-attempt-1-review-3-claude-opus-5.md`
- **Issues Found**: 1 Low
- **Verdict**: ISSUES_FOUND (one Low diagnostics-quality failure-mode gap; no
  deletion-safety, correctness, or concurrency defect found)

### Model coverage statement (do not mislabel)

This artifact is the Anthropic Opus slot for **Round 3 only**. Round 2's Gemini
slot did not execute: Gemini 3.8 / 3.7 / 3.6 returned backend HTTP 400, and the
parent performed a documented GPT fallback. The gauntlet's three-family gate
therefore remains **unfulfilled**, and nothing here should be read as Gemini
coverage or as a completed multi-family review.

### Scope

The three-file master prerequisite only, staged in this worktree:

| File | Reviewed blob |
| --- | --- |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala` | `fc9f27368c896bba8c5934d3824a7ef015d8442f` |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricTestArtifactTrackerSuite.scala` | `a8d0676aa6b292a581843c09a6249c940fe19586` |
| `docs\Reference\Developer Setup.md` | `86607666023a9611cbdbe7cce3d877637a37c512` |

Not a readiness assessment. No source edits, staging, commits, pushes, agent
dispatch, or live cloud calls were performed by this reviewer.

## Evidence Checklist

- [x] Read this worktree's `AGENTS.md` (Scala-first API rules, validation
  ladder, port-branch policy) and the Round 1 and Round 2 artifacts in
  `reviews\sync-20260921\` before reviewing, then re-derived every conclusion
  from current source per the gauntlet Independence Rule.
- [x] Inspected the actual staged delta with
  `git diff --cached -- core/.../FabricArtifactCleanup.scala` and
  `-- core/.../FabricTestArtifactTrackerSuite.scala` and
  `-- "docs/Reference/Developer Setup.md"`. `git status --short` shows exactly
  three staged paths plus the untracked `reviews/sync-20260921/` directory;
  the artifact itself is not part of the reviewed tree.
- [x] **Null / empty / mixed relation references.** Traced
  `FabricArtifactCleanup.references` (lines 47-52) and `item` (lines 62-72) by
  hand over the boundary matrix. Outer `Some(JsNull)` yields `Set.empty`;
  outer `Some(JsArray(Vector()))` flat-maps to `Set.empty`; both still mean "no
  relations". Inside an entry, `JsString` matching `Guid` is canonicalised via
  `UUID.fromString(...).toString`; a nonempty `JsObject`/`JsArray` recurses;
  every other leaf (`JsNumber`, `JsBoolean`, nested `JsNull`, empty `{}`,
  empty `[]`, trailing-space GUID, arbitrary string) throws a field-named
  `IllegalArgumentException` that does not echo payload values. Because
  `flatMap` is strict on `Vector`, a valid sibling GUID cannot short-circuit
  evaluation of a malformed sibling, so the Round 1 masking defect is closed at
  the boundary rather than only in the tested cases.
- [x] **Deletion safety under the new strictness.** `item` is the only
  construction path for `Item`, so any malformed artifact anywhere in the
  workspace makes the *whole* inventory read throw before `run` computes
  `initial`, `jobs`, or `stores`. Verified that `run` (lines 202-247) therefore
  cannot reach `deleteAndConfirm` with a partially parsed graph, and that the
  pre-existing conservative guards still hold on an empty reference set:
  `safeStore` retains every store while any `SparkJobDefinition`/`Notebook` has
  `references.isEmpty`, and `neighbors` requires each edge to resolve to an
  expired `managedEndpoint` whose own neighbours are exactly the candidate.
  Self-edges are removed (`canonicalReferences - canonicalId`) so an
  artifact cannot vouch for itself.
- [x] **Idempotency and repeated-failure boundaries.** Re-derived
  `confirmAbsent`: `ConfirmationAttempts = 31` yields 31 inventory reads and 30
  `pause()` calls before `require(remaining > 1)` fails — matching the suite's
  `assert(pauses == 30)` and the documented "up to 31 times, two seconds apart".
  `deleteAndConfirm` swallows only `RuntimeException` whose message contains
  `PowerBIEntityNotFound`, then still confirms absence, so a concurrent deleter
  cannot produce a false positive. `index` rejects conflicting duplicate IDs
  via `require(distinct.map(_.id).distinct.size == distinct.size, ...)`.
- [x] **Regression coverage read directly.** The new suite test
  (`FabricTestArtifactTrackerSuite.scala:524-546`) iterates the four relation
  fields against nine malformed values — trailing-space GUID, `"not-an-id"`,
  `JsNumber(1)`, `JsBoolean(false)`, `JsNull`, `JsObject()`, `JsArray()`,
  nested `{"nestedId": 1}`, and `[guid, null]` — and for all 36 combinations
  asserts an `IllegalArgumentException` naming the field, `deleted.isEmpty`,
  and `items == Vector(staleStore)`. The positive case at lines 495-498 proves
  a nonempty nested array of GUIDs is still retained and case-normalised
  (`storeId.toUpperCase` canonicalises back to `storeId`).
- [x] **Documentation matches the implemented contract.** The added paragraph
  in `docs\Reference\Developer Setup.md` states relation entries must contain
  only GUID references in nonempty objects or arrays, that malformed or unknown
  metadata fails the inventory read, and that a valid reference elsewhere in the
  entry cannot hide them. Each clause corresponds one-to-one to lines 47-52.
- [x] **Cross-tree identity.** `git ls-files -s` shows blob
  `fc9f27368c896bba8c5934d3824a7ef015d8442f` for `FabricArtifactCleanup.scala`
  and `a8d0676aa6b292a581843c09a6249c940fe19586` for the suite in all three
  worktrees, so this Round 3 reading of the shared fix applies identically to
  the Spark 4.0 and Spark 4.1 candidates.
- [ ] No Scala compile, scalastyle, or ScalaTest execution was performed in
  this review; the parent reports 44/44 selected cleanup tests green on JDK 11
  for this tree, and that result was inspected but not reproduced here.
- [ ] No live Fabric, Azure, or GitHub call was made. Real-endpoint payload
  shapes were deliberately **not** assumed; see the recorded non-issue below.

## Explicitly considered and *not* raised

- **Fail-closed blast radius of the GUID-only relation contract.** Any leaf that
  is not a GUID aborts the entire inventory read, so one foreign artifact can
  make cleanup unavailable workspace-wide. This is the documented, intentional
  trade-off (Round 1 resolution and the new doc paragraph both state it), and
  quantifying its real-world likelihood would require asserting what the Fabric
  metadata endpoint returns. That is out of scope by instruction, so no issue is
  filed. Recorded here so the trade-off is visible rather than silently assumed.
- **Missing relation field (`None`) throws while `Some(JsNull)` is accepted.**
  This asymmetry predates the fix — `case _ => throw ... "Missing or invalid
  $field metadata"` is unchanged context in the diff — and is fail-closed.
  Inherited, not introduced by this delta.
- **Blank `parentArtifactObjectId`.** `reference` throws on `Some(JsString(""))`
  because `""` does not match `Guid`. Also unchanged context, also fail-closed.

## Issues

### Issue 1: A mid-run inventory failure discards already-recorded deletion failures

- **Severity**: Low
- **File**: `core\src\test\scala\com\microsoft\azure\synapse\ml\nbtest\FabricArtifactCleanup.scala`
- **Line(s)**: 212 (`val current = index(client.inventory())`), 228-232
  (`catch { case NonFatal(e) => failures :+= e ... }`), 238-241
  (`failures.headOption.foreach { first => ... throw first }`)
- **Description**: The per-candidate inventory re-read at the top of the loop
  body sits outside any `try`. Every other failure source in the loop is
  protected: `deleteAndConfirm` — including the `confirmAbsent` inventory reads
  it performs — runs inside the `NonFatal` handler that appends to `failures`.
  If candidate *i* fails to delete (`failures = [e1]`) and the loop-head read
  for candidate *i+1* throws, that second throwable propagates straight out of
  `run`, bypassing the aggregation block at 238-241. `e1` is never rethrown and
  never attached via `addSuppressed`, and the closing summary line
  ("examined N items, … confirmed K deletions") is skipped.
- **Coupling to this delta**: this is the reason it is reportable rather than
  inherited noise. Before the fix, `item` tolerated unknown leaves and
  `client.inventory()` was effectively non-throwing for malformed metadata. The
  new contract deliberately makes `inventory()` throw, and `FabricOperations`
  builds `inventory()` as `pages(...).map(FabricArtifactCleanup.item)`, so every
  loop-head read is now a throwing operation. The fix therefore materially
  raises the probability of the exact interleaving that loses `failures`.
- **Risk**: Operator-facing diagnostics only. A store-deletion failure followed
  by a newly malformed artifact surfaces as "invalid relation metadata" with no
  trace in the thrown exception that a deletion also failed. No artifact is
  deleted that should have been retained: the `failures.isEmpty && safeStore(...)`
  guard still blocks store deletion within the run, and the per-failure
  `log(s"Fabric cleanup failed for ${candidate.id}: ...")` line still prints, so
  the information exists in the job log but not in the failure signal.
- **Test evidence for the gap**: `FabricTestArtifactTrackerSuite.scala:344-357`
  ("Preserve interrupts, inventory failures, and deletion failures") exercises
  an inventory failure only with `beforeRead = n => if (n == 2) throw ...` and
  asserts `inventoryFailure.deleted.isEmpty` — that is, with no prior recorded
  deletion failure. Lines 450-455 exercise multi-delete-failure aggregation
  without a subsequent inventory failure. Neither covers the combination.
- **Suggested Fix**: Wrap the loop-head read so a mid-run inventory error joins
  the existing aggregation, for example by evaluating
  `Try(index(client.inventory()))` and, on `Failure(e)`, appending `e` to
  `failures` and breaking out of the loop so the block at 238-241 throws the
  first failure with the rest suppressed. Reuse the existing
  `filterNot(_ eq first)` self-suppression guard already present at line 239.
  Add a regression that fails one deletion and then throws from a later
  inventory read, asserting the thrown exception carries the deletion failure
  as suppressed.

## Resolution Log

### Issue 1

- **Status**: Open
- **What changed**: Nothing. This review contract forbids source edits,
  staging, commits, and pushes.
- **Why**: Round 3 is review-only for this run.
- **How verified**: Direct control-flow reading of `run` at lines 202-247 plus
  an explicit search of `FabricTestArtifactTrackerSuite.scala` for a combined
  deletion-failure-then-inventory-failure case, which is absent.

## Resolution Addendum — bounded fix verification (2026-09-21)

Re-checked the two fixes only. Tree `15746e61f1`; blobs `e92bc35a94` (cleanup),
`1e01591ffd` (tracker), `30748f8e0b` (suite) — identical across all three trees.

- **Issue 1 — Fixed.** The loop-head `index(client.inventory())` now sits in
  `try/catch NonFatal`, attaches prior `failures` via `filterNot(_ eq e)`, and
  rethrows that same instance, so no `deleteAndConfirm` runs after it.
- **Issue 2 — Fixed.** Tracker line 65 is now
  `failures.tail.filterNot(_ eq failure).foreach(failure.addSuppressed)`; the
  master companion is now 4 staged paths, not 3, so this issue is in scope here.
- **Regressions.** The new tests cover distinct *and* reused inventory
  throwables, only `staleJob` attempted with the store retained, and a shared
  tracker throwable with both attempts plus a drained queue.
- **Negative control.** `master-cleanup-round3-red.log`: 3 run, 1 pass, 2 fail
  with the predicted symptoms; the 803→800 reduction was semantics-preserving.
- **Green.** `master-cleanup-round3-green-v2.log`: scalastyle 0 errors at 800
  lines, 46/46 tests (44 + 2 new), all three trees; earlier `-green.log` files
  are scalastyle failures, not passes. **Verdict: CLEAN** for both issues;
  residual non-defect `FabricNotebookTests.scala:292` unchanged as recorded.
