# Spark 4.0 master sync, attempt 1, round 3

## Review summary

- **Round:** 3.
- **Theme:** Edge Cases & Robustness (error handling, boundary conditions, concurrency, failure modes).
- **Mode:** Sequential.
- **Model:** `claude-opus-5`, max reasoning requested.
- **Issues found:** 2 (both Low; no correctness defect; neither blocking).
- **Verdict:** ISSUES_FOUND (Low only).
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-3-claude-opus-5.md`.

Reliability review scoped to the merge boundary: incoming master code meeting Spark 4.0 port
edits. No merge-introduced failure mode was found in auth resolution, Scala 2.13 row handling,
null/empty inputs, retry/resource paths, or CI policy conditions. The two findings are a
merge-introduced duplicate constant (cosmetic) and a pre-existing gap in the port's own
`safeGetDefault` guard coverage. No agents or factories were spawned. No source, docs, tests,
staging, commits, or live services were touched; only this artifact was written.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.0-master-20260916` |
| Branch | `sync/spark4.0-master-20260916` |
| Target and HEAD | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` |
| Incoming master and MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Last integrated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Staged index tree (`write-tree`) | `cd6bd3b6eacb3062707f0f3247ec04f4f019d106` |
| Unmerged paths / working-tree delta | 0 / 0 |
| Snapshot check timestamp | `2026-09-16T09:50:00Z` |

## Evidence checklist

- [x] **Boundary narrowed, not re-audited.** Used `files\conflict40-classification.json` to scope to
  `combined` paths rather than traversing the 654 incoming files. Confirmed `HTTPClients.scala` and
  `HTTPSchema.scala` blobs are byte-identical to master (`rev-parse :<path>` == `rev-parse master:<path>`),
  so the new Fabric-auth retry algorithm is unchanged upstream and was not re-implemented here.
- [x] **Lazy Fabric token acquisition cannot fire early.** `CognitiveServiceBase.scala:397-408`
  computes `explicitAuthHeader` first and evaluates the by-name `fabricFallbackAuthHeader` only when
  `explicitAuthHeader.isEmpty`. A subscription key, AAD token, custom auth header, or embedded
  `customHeaders` credential therefore short-circuits any token call that could throw or block.
- [x] **Marker header cannot leak to the service.** `addHeaders` (`:548-561`) calls
  `req.removeHeaders(...)` before emitting, filters `isFabricAuthMarker` out of resolved headers, and
  re-adds the marker only when `usesFabricFallback && supportsImplicitFabricAuthRetry`.
  `HTTPSchema.scala:209` strips it again in `toHTTPCore` before the wire. `OpenAI.scala:580` gates
  `supportsImplicitFabricAuthRetry` on `usingImplicitFabricEndpoint`, and `HTTPSchema.scala:170-172`
  additionally requires an `MwcToken` Authorization value, so a spoofed marker alone cannot divert routing.
- [x] **Null/empty row boundaries on the incoming multimodal path.**
  `OpenAIChatCompletion.scala:137` reads `r.getAs[scala.collection.Seq[Row]](getMessagesCol).toSeq`,
  which would NPE on a null column; `:143-144` `shouldSkip` returns true when that column is null, so
  the NPE is unreachable. `OpenAIResponses.scala:159` has the identical guard at `:216-217`.
  `validateMessages` (`:469-472`) rejects empty message arrays and null elements (`:519-521`).
- [x] **Scala 2.13 mutable-collection boundary.** The port's `asImmutableCollection`
  (`CognitiveServiceBase.scala:102-115`) normalizes top-level `Seq`/`Map` in `getValueOpt`. The incoming
  multimodal code independently widens to `scala.collection.Seq` (`OpenAIChatCompletion.scala:20`
  alias, used at `:171, 229, 269, 282, 284, 384, 469, 517, 531, 546`) rather than narrowing to the
  immutable alias, so mutable Spark values never reach a `checkcast`. Grepped
  `ServiceParam\[(Seq|Map|Array)\[` across `cognitive/`: 32 hits, none nested two levels deep, so the
  shallow conversion has no reachable gap. `Map[String, Any]` params (`OpenAIChatCompletion.scala:25`,
  `OpenAIResponses.scala:42`) erase their values to `Any`, inserting no element checkcast.
- [x] **Codegen guard coverage measured, not assumed.** In both port indexes `Wrappable.scala` contains
  4 `safeGetDefault` occurrences (definition `:109` plus call sites `:150`, `:161`, `:317`) and 2 raw
  `thisStage.getDefault` occurrences: one inside `safeGetDefault` itself, one at `:648`. See Finding 1.
- [x] **CI policy conditions re-executed, not inferred.** `python -m pytest tools/ci/tests/test_pipeline_yaml.py -q`
  in this worktree: **50 passed, 27 skipped** in 3.75s. `FabricE2E` stays `condition: false`, asserted by
  `test_spark40_ci_uses_supported_runtime` and `test_fabric_e2e_keeps_key_vault_authentication_while_disabled`.
  `test_every_sbt_running_job_waits_for_the_prewarm_cache` was widened to accept `condition is False`,
  so a disabled job is not misread as an ungated sbt job.
- [x] **Skipped tests inspected (not treated as passes).** All 27 skips are host-driven
  ("release replay script requires Bash", "certificate retrieval requires Bash") at
  `test_pipeline_yaml.py:452, 480, 833, 902, 1058, 1078, 1122, 1211, 1286, 1370, 1451`. They gate on
  Windows only and will execute on Linux CI; none masks port logic.
- [x] **Resource handling at the port's own edits.** `PyCodegen.scala` `main` closes the `@file`
  config source in a `try/finally` (`:301-306`); `safeListFiles` (`:190-192`) null-guards
  `File.listFiles`; `generatePyPackageData` uses `mkdirs()` (`:213`). No unclosed handle introduced.

## Findings

### Finding 1: R wrapper generation still calls `getDefault` unguarded — Low

- **File / line:** `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala:648`
  (`RWrappable.rParamArg`).
- **Description:** The port-only `safeGetDefault` guard (`:109-115`) now covers all three Python
  paths — `pyParamArg` `:150`, `pyParamDefault` `:161`, and `pyStubParamArgs` `:317` from the round 1
  fix — but `rParamArg` still evaluates `(p, thisStage.getDefault(p))` directly.
- **Reachability:** `RCodegen.scala:20-24` instantiates `RWrappable` implementations and calls
  `makeRFile`, which renders `rParamsArgs` (`:644`) → `rParamArg`. `Wrappable` extends
  `PythonWrappable with RWrappable` (`:750`), so every generated stage traverses both paths in one
  `sbt codegen` run.
- **Risk:** A stage exposing a foreign-owned `Param` would fail `sbt codegen` with the same
  `IllegalArgumentException` round 1 removed from the Python path, relocated to `RCodegen`.
- **Scope — not merge-introduced, currently latent.** `:648` is byte-identical to master and to the
  landed target (`safeGetDefault` count is 3 at `ecec8dd58b` and 0 at master, and the master→index
  diff for this file contains no `:648` hunk). No shipped stage owns a foreign parameter, which is
  why this candidate's full `sbt codegen` passes. The round 1 fixture is test-scope and nested in
  `private[codegen] object PyCodegenFixtures`, so `RCodegen`'s jar scan never instantiates it.
- **Suggested handling:** Do **not** fix in this sync PR. `Wrappable.scala` is shared code and
  AGENTS.md requires cross-version changes to land on `master`; patching it only on the ports would
  widen the port delta. Track as separate follow-up.

### Finding 2: Merge produced a duplicate `BUILD_SBT` constant — Low

- **File / lines:** `tools/ci/tests/test_pipeline_yaml.py:20` and `:33`.
- **Description:** Master defines `BUILD_SBT = REPO_ROOT / "build.sbt"` in its new top constant block
  while the port's existing block already defined the same name. Combining both sides kept both
  assignments. Confirmed merge-introduced: the target→index diff adds `:20`, and the master→index diff
  adds `:33`; neither parent contains both.
- **Risk:** None at runtime — both bind an identical value, and flake8 `F811` does not flag
  module-level constant rebinding. Verified harmless: 50 passed / 27 skipped. This is merge hygiene
  only; the duplicate invites a future divergent edit to one copy.
- **Suggested handling:** Optional one-line cleanup — delete the later duplicate (`:33`) and keep the
  master-side `:20`. Deferring is acceptable.

### Observation (no action in this PR): port-divergent `PyCodegen` robustness

`PyCodegen.scala` is the only reviewed file whose merged blob differs between the two ports. Spark 4.0
carries `safeListFiles`, `mkdirs()`, and the `/explainers` ICE import shim that 4.1 lacks; Spark 4.1
carries `ManualInitPackageFolders = Set("/cognitive", "/dl", "/hf")` and explicit UTF-8 config decoding
that 4.0 lacks. Two consequences on 4.0: `/hf` is not excluded from generated `__init__.py`
(`:135`, guard `packageFolder != "/cognitive" && packageFolder != "/dl"`) even though
`deep-learning/src/main/python/synapse/ml/hf/__init__.py` is hand-written; and `main` decodes the
`@file` config with `scala.io.Source.fromFile` at the platform default charset, which JDK 17 does not
force to UTF-8 (JEP 400 lands in JDK 18). **Both conditions are pre-existing at the landed target** —
the 4.0 target guard is already `!= "/cognitive" && != "/dl"`, and `hf/__init__.py` is present at
master, both targets, and `a6fd536ad7`. Neither is caused by this sync, and repairing them here would
be the unrelated expansion this review was told to avoid.

## Validation limitations

Static merge-boundary inspection plus locally re-executed CI-policy tests (50 passed / 27 skipped).
The 27 skips require Bash and did not run on this Windows host. I did not rerun the owner's Scala
compile, style, 186-test, 77-pipeline-regression, Black, or codegen evidence; those are cited from the
round 1/2 record, including the 09:22 re-run of 17 `PyCodegenSuite` tests, core style, and full codegen.
No Azure Pipelines run exists for this candidate; the `236185691` master baseline failed only on Fabric
artifact provisioning and is not candidate CI. Finding 1's exception mechanism is inferred from the
round 1 reproduction of the identical expression at `:317`, not separately reproduced at `:648`.
Rounds 4-6 were not executed.

## Resolution log

### Finding 1
- **Status:** Open (deferred by design).
- **What changed:** Nothing. Shared-code fix belongs on `master`, not a port branch.
- **How verified:** Guard coverage counted in the index (4 `safeGetDefault`, 2 raw `thisStage.getDefault`);
  reachability traced through `RCodegen.scala:20-24` → `Wrappable.scala:644` → `:648`.

### Finding 2
- **Status:** Open (owner decision).

### Parent disposition, 2026-09-16

Finding 1 is a verified pre-existing limitation, not a sync regression.
The sync preserves that behavior and does not add a new R change. The separate
master guide correction explicitly records the unguarded R path so it is not
mistaken for complete foreign-parameter support.

Finding 2 is addressed by deleting the later identical `BUILD_SBT` assignment
and retaining master's definition. No test condition or assertion changes.
Linux pipeline tests and pinned Black are being rerun for this cleanup.

The final Linux run passed all 137 CI helper tests, including all 77 pipeline
tests with no Windows-only skips. Black 22.3.0 reported all 214 files clean.
- **What changed:** Nothing; this review edits no source.
- **How verified:** Duplication confirmed at `:20`/`:33`; suite green at 50 passed / 27 skipped.

### Carried forward
- Round 1 Issue (stub default lookup bypassing the Spark 4 guard at `Wrappable.scala:317`): confirmed
  still fixed in this snapshot — `:317` uses `safeGetDefault`.
- Round 2: CLEAN; no architectural regression observed from the reliability angle.
