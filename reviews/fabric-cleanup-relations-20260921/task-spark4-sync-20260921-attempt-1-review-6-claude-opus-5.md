## Review Summary

- **Round**: 6 only, attempt 1. **Theme**: polish and hardening — performance,
  observability, documentation, naming. **Mode**: sequential, slot 3. **Model**:
  claude-opus-5 (Anthropic Opus slot).
- **Target**: master prerequisite, branch `fix/fabric-cleanup-relations-20260921`,
  HEAD `714d365e71`, index tree `b4dd50774784a1fd7fca611883c333a5a21458c2`
- **Artifact**: `reviews\sync-20260921\task-spark4-sync-20260921-attempt-1-review-6-claude-opus-5.md`
- **Issues Found**: 2 Low
- **Verdict**: ISSUES_FOUND — two Low polish gaps; no performance, compatibility, naming,
  dead-code, or documentation-inaccuracy defect found

### Gate status (do not overstate)

Round 6 Anthropic slot only. **No Gemini version has executed in this gauntlet**:
3.8/3.7/3.6 returned backend HTTP 400 in Round 2 and 3.5 failed the same way with zero
turns before Round 5, which used a direct GPT tests review. The three-family gate is
**unfulfilled** and this is **not** a full-gauntlet green. Azure Pipelines and current-head
GitHub review have not run because the PRs do not exist yet, so all evidence here is local.
Scope: the frozen staged candidate of 5 files — 4 Scala test-infrastructure sources plus
`docs/Reference/Developer Setup.md`, 106 insertions, 16 deletions. This is the master
companion only; the port-only surface (runtime pins, workflows, Python bridge, batched
headers, CI README) is reviewed in the two sync trees, not here. Rounds 1/3/4 are resolved.

## Evidence Checklist

Publication note: this prerequisite-specific directory preserves the separate
port review records. Paths in the original review describe its review-time location.

- [x] Frozen tree confirmed by hash (`git write-tree` = `b4dd50774784a1fd7fca611883c333a5a21458c2`)
  and the staged set is exactly the 5 declared paths. The four changed Scala blobs are
  byte-identical to the ports — cleanup `e7a385bf58`, tracker `1e01591ffd`, new failure
  suite `fb905d3e3d`, suite `490dcf1bb2` — so this review transfers to both ports unchanged.
- [x] **No Spark cost from the new suite.** `TestBase.spark/sc/ssc` are `lazy val`
  (`TestBase.scala:156-158`) and `beforeAll` (`:189-192`) only sets `log4j1.compatibility`
  and resets `suiteElapsed`, so no session starts and `logTime` evidences final-source execution.
- [x] **Naming and structure.** The new suite name states what it holds, both imports are
  used, the moved body is unchanged, the split keeps the main suite at 796 lines against
  the 800-line scalastyle limit with no waiver, and `TestBase` matches AGENTS.md.
- [x] **No new N+1 or hot loop.** The per-candidate `index(client.inventory())` and up-to-31
  `confirmAbsent` reads (`FabricArtifactCleanup.scala:163-172`) are inherited and unchanged in
  count by Rounds 3/4, which only widened the `try` around existing work; the added handler
  allocates nothing beyond the existing `failures` vector and does no extra I/O.
- [x] **Dead code, markers, imports.** Zero `TODO`/`FIXME`/`HACK`/`XXX:` on added lines;
  all imports used; nothing commented out; no debug or `println` left behind.
- [x] **Backward compatibility.** The delta touches no `src/main/scala` at all — only
  `core/src/test/scala/.../nbtest/` and one doc — so no public JVM signature, serialized
  parameter shape, or generated Python wrapper can be affected, per AGENTS.md.
- [x] **Docs — relation contract.** The paragraph added to `docs/Reference/Developer Setup.md`
  matches `references(value, field)` and the removed `forall(nonEmpty)` heuristic; it is
  branch-neutral prose and names no Spark, Scala, Java, or Python version.
- [ ] No compile, scalastyle, codegen, or ScalaTest run here; the reported green gates
  (`master-cleanup-round4-green.log`: style 0 errors, 46/46 across 3 suites) and the negative
  control (`master-cleanup-round4-red.log`: 3 pass, 1 fail) were read, not reproduced.
- [ ] No live Fabric, Azure, Databricks, or GitHub call; no endpoint schema assumed.

**Considered, not filed:** `index` uses `Vector.distinct` (O(n²) on 2.12) but inputs are tens of
items and it is not introduced here; the two tracker suites now sit on different bases, which is
deliberate and the stricter base is the new one.

## Issues

### Issue 1: The cleanup summary line is skipped on every failure path

- **Severity**: Low
- **File**: `core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/FabricArtifactCleanup.scala`
- **Line(s)**: 243-247 (aggregation `throw`), 248-250 (summary `log`), 221-225 (rethrow)
- **Description**: The summary `log("… examined ${initial.size} items, found ${jobs.size} owned
  jobs and ${stores.size} owned stores, and confirmed ${deleted.size} deletions")` sits *after*
  `failures.headOption.foreach { … throw first }`, and the Round 4 handler rethrows out of the
  loop, so the one aggregate observability line is emitted only on full success.
- **Risk**: The preflight gates E2E through `condition: succeeded()`, so the failing run is exactly
  the one being triaged; inventory size and owned job/store counts are lost from `test-reports/`.
- **Suggested Fix**: Wrap the candidate loop in `try { … } finally { log(summary) }`, wording the
  counts as partial, e.g. `confirmed ${deleted.size} deletions before failing`.

### Issue 2: The new failure-aggregation contract is undocumented in code and doc

- **Severity**: Low
- **File**: `docs/Reference/Developer Setup.md`; `FabricArtifactCleanup.scala`
- **Line(s)**: doc 108-112; code 221-225 and 243-246
- **Description**: Rounds 3 and 4 changed operator-visible behaviour — a metadata, safety, or
  inventory error now aborts the remaining candidates immediately, is rethrown as the *same*
  instance, and demotes earlier deletion errors to `getSuppressed`. The doc still says only
  "Independent job deletions are still attempted, and collected errors fail the cleanup afterward"
  plus "Authentication, inventory, and deletion errors fail the cleanup": true for deletion-only
  failures, silent on abort-and-suppress. It contains no "suppress", "abort", or "remaining", and
  the changed Scala files carry only a licence header, leaving `filterNot(_ eq e)` unexplained.
- **Risk**: An operator reads the thrown inventory error as the primary cause and can miss a
  deletion failure attached only as suppressed — the exact signal Rounds 3/4 preserved.
- **Suggested Fix**: One doc sentence stating that a metadata, safety, or inventory error stops the
  run immediately and carries earlier deletion errors as suppressed exceptions, plus a comment at
  each `filterNot(_ eq …)` noting the guard exists because `Throwable.addSuppressed(this)` throws.

## Resolution Log

- **Issue 1 — Open.** Nothing changed: this contract forbids source edits, staging, commits, and
  pushes, and the candidate is frozen. Verified by statement-order reading of `run` (`:211-251`).
- **Issue 2 — Open.** Nothing changed, same reason. Verified by keyword search of
  `Developer Setup.md` (zero hits) and a comment-line count of the changed Scala sources.

## Round 6 Resolution Addendum (verification only)

- **Issue 2 — Resolved in docs.** `Developer Setup.md` now separates continuing independent DELETE failures
  from immediate abort on an inventory, job-history, or schedule read, and states suppressed prior errors,
  the reused-instance guard, and unchanged interrupt/fatal propagation. The paragraph is byte-identical in
  all three trees (md5 `96A3584ECE3968C1A73BE395F5DB3839`) and every claim holds: `:212-225` wraps `index`/
  `safeJob`/`safeStore`, `:223`/`:245` guard `filterNot(_ eq ...)`, `NonFatal` keeps interrupts and fatal
  errors propagating. Code comments declined as redundant — `FabricTestArtifactTrackerFailureSuite:11` and `...TrackerSuite:344,462` are named regressions stating the same contracts.
- **Issue 1 — Declined as out of scope; the finding above stands unedited, and is not fixed.** Baseline
  `git show 714d365e71:...FabricArtifactCleanup.scala` already logs the summary at `:244-246` after the
  `:240-243` throw, so the ordering is inherited, not a regression or spec violation; per-candidate attempt,
  confirm, retain, and failure logs survive at `:197,227,233,237,241`, and a `finally` summary would extend observability rather than repair a silent error, deletion, or compatibility defect.
- **No in-scope R6 blocker remains.** All four Scala blobs are unchanged (`e7a385bf58`, `1e01591ffd`,
  `fb905d3e3d`, `490dcf1bb2`), so the 46/46 green run still describes this source and the docs-only correction needs no rerun. Gate status above is unchanged: no Gemini version has ever executed.