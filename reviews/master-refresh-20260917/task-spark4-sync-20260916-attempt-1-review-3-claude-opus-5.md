# Master Spark 4 branch guides, attempt 1, round 3

## Review summary

- **Round:** 3.
- **Theme:** Edge Cases & Robustness — non-code adaptation: unusual inputs, missing cases, guidance
  that fails at its own boundaries.
- **Mode:** Sequential.
- **Model:** `claude-opus-5`, max reasoning requested.
- **Issues found:** 1 (Low).
- **Verdict:** ISSUES_FOUND (Low only; no inaccurate claim about landed state).
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-3-claude-opus-5.md`.

This round tests the guides against the failure mode that matters for a reference document: an agent
reading it and drawing a wrong conclusion at an edge the text does not bound. The critical
requirement — that the snapshot describe **landed target blobs, not candidates** — holds. One
statement is broader than the code it describes. No agents or factories were spawned. No source,
docs, tests, staging, commits, or live services were touched; only this artifact was written.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Checkout | Repository root, branch `docs/spark4-branch-context-20260916` |
| Branch | `docs/spark4-branch-context-20260916` |
| Target master and HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Staged index tree (`write-tree`) | `14e3057db49bd6e46133fd7beb4bd776071be383` |
| Unmerged paths / working-tree delta | 0 / 0 |
| Blobs the guides describe | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` (4.0), `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` (4.1) |
| Snapshot check timestamp | `2026-09-16T09:50:00Z` |

## Evidence checklist

- [x] **Landed-vs-candidate separation verified against Git objects, not prose.** Counted
  `safeGetDefault` occurrences in `core/.../codegen/Wrappable.scala`: **3** at the 4.0 target
  `ecec8dd58b`, **3** at the 4.1 target `06897e5b27`, **0** at master `1305587a4a`. The guard the guide
  documents therefore exists at both landed targets and is genuinely branch-specific. The candidate
  merges raise this to 4 by adding the `pyStubParamArgs` call site; the guide does **not** assert that
  fourth site as existing fact — it states the rule and says to preserve it in newly imported
  generation paths (`branch-spark4-common.md:118-120`). That is forward guidance, not a claim about
  unlanded candidate state, so the landed-blob requirement is met.
- [x] **SAR differentiation is accurate per port, and correctly hedged.**
  `branch-spark4-common.md:111-117` records typed `SAR.ItemAffinity` with `itemIndex`/`affinity` on the
  4.0 target, `Seq[Row]` still in use on the pinned 4.1 target, and both qualifying
  `col("sarUserFactors.flatList")` against `DetectAmbiguousSelfJoin`. It explicitly states the
  `UnboundRowEncoder` failure "has not been established there by this source audit" for 4.1 — the
  correct epistemic boundary, and the one that prevents a future agent from manufacturing a 4.1 SAR
  change. Matches `conflict40-classification.json`, which classifies `SAR.scala` and `SARModel.scala`
  as `port-unchanged`.
- [x] **Version facts stay out of the shared guides.** `AGENTS.md` and `CONTRIBUTING.md` are unmodified
  in this candidate, so the AGENTS.md rule that neither may name a Spark, Scala, Java, or Python
  version holds. Version matrices live only under
  `.github/skills/synapseml-branches/references/branch-spark4p0.md` and `branch-spark4p1.md`.
- [x] **Squash-merge provenance edge case is covered.** The guides explain why ancestry diverges from
  content presence and name the last integrated master content `a6fd536ad7` against graph base
  `a833941704`. This is the exact trap this sync hit; documenting it prevents a reviewer from reading
  "commit absent" as "feature absent".
- [x] **Guidance is non-destructive at its boundaries.** The GitHub merge-settings section explains
  disabled merge commits and advises recording the incorporated master SHA rather than instructing
  anyone to change repository settings, so following the text cannot alter shared configuration.
- [x] **Formatting and references.** `git diff --check HEAD` reports no whitespace errors; local
  cross-references resolve.

## Findings

### Finding 1: "guards `getDefault`" is broader than the code it describes — Low

- **File / lines:** `.github/skills/synapseml-branches/references/branch-spark4-common.md:118-120`.
- **Current text:** "`Wrappable.safeGetDefault` guards `getDefault`, which throws on Spark 4 for
  foreign-owned parameters. Preserve that guard in newly imported generation paths, including Python
  stub defaults."
- **Description:** The unqualified phrase reads as though every `getDefault` call in `Wrappable.scala`
  is guarded. It is not. At both landed targets the guard covers only the Python wrapper paths
  (`pyParamArg:150`, `pyParamDefault:161`); `RWrappable.rParamArg:648` still calls
  `thisStage.getDefault(p)` directly, and that path is live — `RCodegen.scala:20-24` calls `makeRFile`
  → `rParamsArgs:644` → `rParamArg`, and `Wrappable` extends `PythonWrappable with RWrappable`
  (`:750`). Measured in both port indexes: 4 `safeGetDefault` occurrences and 2 raw
  `thisStage.getDefault`, the second being `:648`.
- **Risk:** An agent trusting the sentence could conclude the foreign-parameter failure mode is fully
  handled on the ports, skip the R path when importing a future master codegen change, and ship a
  `sbt codegen` break in `RCodegen` — the same class of defect round 1 fixed in `PyCodegen`. The risk
  is guidance-accuracy only; no shipped stage owns a foreign parameter today, so nothing fails now.
- **Suggested fix:** Narrow the claim and name the gap, for example: "`Wrappable.safeGetDefault` guards
  the **Python wrapper and stub** `getDefault` lookups, which throw on Spark 4 for foreign-owned
  parameters. `RWrappable.rParamArg` is not yet guarded. Preserve the guard in newly imported
  generation paths, including Python stub defaults."
- **Note on placement:** This is a documentation-only correction to the master-targeted guide, which is
  the correct home for it. The underlying code gap is shared and should not be patched on a port
  branch.

## Validation limitations

This is a documentation review validated against Git objects at `1305587a4a`, `ecec8dd58b`, and
`06897e5b27`, plus the port worktrees' staged indexes. Guide statements about runtime behavior —
Spark 4.0.1/4.1.1, Scala 2.13.16/2.13.17, Python 3.12.11/3.13, NumPy, DBR 17.3/18.0, Arrow, MLflow, R,
sparklyr, Petastorm — were checked for internal consistency and correct branch attribution, not by
executing those runtimes. No live service, cloud pipeline, or repository setting was touched, and no
Azure Pipelines run exists for the candidates. Rounds 4-6 were not executed.

## Resolution log

### Finding 1
- **Status:** Open — documentation-only correction proposed; this review edits no file but its own
  artifact.
- **What changed:** Nothing yet.
- **How verified:** Guard coverage counted at both landed targets (3 occurrences) and master (0), and
  in both port indexes (4 `safeGetDefault`, 2 raw `thisStage.getDefault`); R path reachability traced
  `RCodegen.scala:20-24` → `Wrappable.scala:644` → `:648`.

### Carried forward

### Parent resolution, 2026-09-16

Finding 1 is resolved. The common guide now limits the landed guard claim to
Python wrapper default lookups, explicitly notes the unguarded R path, and
separately directs newly imported Python stub paths to preserve the guard.
This matches the pinned target-source evidence in this review without claiming
that candidate-only stub behavior has already landed.
- Round 1 Issue 1 (typed SAR affinity attributed to both ports): confirmed still resolved —
  `:111-117` attributes `SAR.ItemAffinity` to 4.0 only, keeps `Seq[Row]` for 4.1, and records that both
  qualify the `SARModel` join column.
- Round 2: CLEAN; no documentation-architecture regression observed from the robustness angle.
