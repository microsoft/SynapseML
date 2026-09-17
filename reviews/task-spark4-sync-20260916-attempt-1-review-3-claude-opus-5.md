# Spark 4.1 master sync, attempt 1, round 3

## Review summary

- **Round:** 3.
- **Theme:** Edge Cases & Robustness (error handling, boundary conditions, concurrency, failure modes).
- **Mode:** Sequential.
- **Model:** `claude-opus-5`, max reasoning requested.
- **Issues found:** 3 (1 Medium-Low process risk — **resolved by the owner during this round** — and
  2 Low; no correctness defect).
- **Verdict:** ISSUES_FOUND (none blocking correctness; 1 of 3 already closed).
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-3-claude-opus-5.md`.

Reliability review scoped to the merge boundary: incoming master code meeting Spark 4.1 port edits.
No merge-introduced failure mode was found in auth resolution, Scala 2.13 row handling, null/empty
inputs, retry/resource paths, or CI policy conditions. The highest-value finding is a **staging**
risk: the working tree carries an unstaged CI-test improvement that a commit from the index would
silently drop. No agents or factories were spawned. No source, docs, tests, staging, commits, or live
services were touched; only this artifact was written.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.1-master-20260916` |
| Branch | `sync/spark4.1-master-20260916` |
| Target and HEAD | `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` |
| Incoming master and MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Last integrated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Staged index tree at review start | `0eabccd3a82c4ff1e50721b2fe60e4d051a2bc0e` |
| Staged index tree at review end | `947975a17fc9362dfd2afb9950eca690d80f98a6` |
| Unmerged paths | 0 |
| Working-tree delta at review start | **1 file** — `tools/ci/tests/test_pipeline_yaml.py` (Finding 1) |
| Working-tree delta at review end | 3 files — `tools/ci/README.md`, `tools/ci/get_python_version.sh`, `tools/ci/tests/test_python_version.py` |
| Snapshot check timestamps | `2026-09-16T09:50:00Z` start, `2026-09-16T09:56:00Z` end |

This worktree moved during the round: the owner's local validation is active. `HEAD` and `MERGE_HEAD`
were re-verified unchanged at review end, so the merge identity is stable and every code conclusion
below still applies. Finding 1 was staged by the owner mid-round and is closed. The three paths
unstaged at review end are new owner work-in-progress outside this round's scope and were not reviewed.

## Evidence checklist

- [x] **Boundary narrowed, not re-audited.** Scoped via `files\conflict40-classification.json` to
  `combined` paths. `HTTPClients.scala` and `HTTPSchema.scala` blobs are byte-identical to master
  (`rev-parse :<path>` == `rev-parse master:<path>`), so the incoming Fabric-auth retry algorithm is
  unchanged upstream and was not re-implemented by this sync.
- [x] **Cognitive/OpenAI merged blobs match Spark 4.0 exactly.** `CognitiveServiceBase.scala`,
  `OpenAI.scala`, `OpenAIChatCompletion.scala`, `OpenAIPrompt.scala`, `Wrappable.scala`, and
  `LightGBMBase.scala` hash-compare identical across both port indexes, so the auth/multimodal
  boundary conclusions below are the same evidence on both ports, not a re-derivation.
- [x] **Lazy Fabric token cannot fire early, and the marker cannot leak.**
  `CognitiveServiceBase.scala:397-408` resolves `explicitAuthHeader` first and evaluates the by-name
  `fabricFallbackAuthHeader` only when it is empty, so a key, AAD token, custom auth header, or
  embedded `customHeaders` credential short-circuits any token call that could throw or block.
  `addHeaders` (`:548-561`) removes the marker, filters `isFabricAuthMarker` from resolved headers, and
  re-adds it only when `usesFabricFallback && supportsImplicitFabricAuthRetry`; `HTTPSchema.scala:209`
  strips it again in `toHTTPCore`, and `:170-172` also requires an `MwcToken` Authorization value, so a
  spoofed marker alone cannot divert routing.
- [x] **Null/empty row boundaries on the incoming multimodal path.**
  `OpenAIChatCompletion.scala:137` would NPE on a null messages column, but `shouldSkip` at `:143-144`
  returns true for exactly that case; `OpenAIResponses.scala:159` is guarded identically at `:216-217`.
  `validateMessages` (`:469-472`) rejects empty arrays and null elements (`:519-521`).
- [x] **Scala 2.13 mutable-collection boundary.** The port's `asImmutableCollection`
  (`CognitiveServiceBase.scala:102-115`) normalizes top-level `Seq`/`Map` in `getValueOpt`, and the
  incoming multimodal code independently widens to `scala.collection.Seq`
  (`OpenAIChatCompletion.scala:20` alias, used at `:171, 229, 269, 282, 284, 384, 469, 517, 531, 546`)
  instead of narrowing to the immutable alias. Grepped `ServiceParam\[(Seq|Map|Array)\[` across
  `cognitive/`: 32 hits, none nested two levels deep, so shallow conversion has no reachable gap.
  Scala 2.13.17 changes nothing here relative to 2.13.16 — the blobs are identical.
- [x] **Codegen guard coverage measured, not assumed.** The 4.1 index `Wrappable.scala` has 4
  `safeGetDefault` occurrences (`:109` definition; `:150`, `:161`, `:317` call sites) and 2 raw
  `thisStage.getDefault`: one inside `safeGetDefault`, one at `:648`. See Finding 3.
- [x] **CI policy conditions re-executed on the live tree.**
  `python -m pytest tools/ci/tests/test_pipeline_yaml.py -q` → **50 passed, 27 skipped** in 3.52s;
  targeted `-k "fabric_e2e or spark41_ci or github_pr_validation or prewarm_cache"` → 6 passed.
  `FabricE2E` stays `condition: false`, and `test_every_sbt_running_job_waits_for_the_prewarm_cache`
  accepts `condition is False`, so a disabled job is not misread as an ungated sbt job. All 27 skips
  are host-driven ("requires Bash"), gate on Windows only, will run on Linux CI, and mask no port logic.
- [x] **Spark 4.1 runtime boundaries preserved.** `test_spark41_ci_uses_supported_runtime` asserts
  Spark 4.1.1, sbt-scoverage 2.4.0, `FabricE2E` disabled, and quoted `${IGNORE_TEST_PATH:-}` /
  `${TEST_SUB_PATH:-}` expansion so an unset variable cannot become a literal argument;
  `test_github_pr_validation_supports_spark41` asserts Python 3.13 and JDK 17. All green above.
- [x] **SAR baseline untouched, as instructed.** No `SAR.scala` / `SARModel.scala` change was proposed
  or made; `conflict40-classification.json` classifies both as `port-unchanged`.

## Findings

### Finding 1: Unstaged CI-test improvement risked being dropped by a commit from the index — Medium-Low (RESOLVED during this round)

- **File:** `tools/ci/tests/test_pipeline_yaml.py`.
- **Description:** At review start the edit was **unstaged**. It renames
  `test_fabric_e2e_keeps_key_vault_authentication_and_blocks_forks` to `..._while_disabled` and
  replaces the staged tolerant block
  (`condition = fabric_e2e["condition"]; if condition is not False: assert ...`) with a direct
  `assert fabric_e2e["condition"] is False`, matching Spark 4.0's shape exactly.
- **Risk (as filed):** This merge commits from the index, so `git commit` would have shipped the
  tolerant form and silently discarded the edit, leaving the two ports with different assertions for
  the same invariant and a stale "blocks_forks" name. A ship/consistency risk, **not** a correctness
  defect — the staged form also passed, and `test_spark41_ci_uses_supported_runtime` asserts
  `condition is False` independently, so coverage never depended on which form landed.
- **Resolution:** Closed. Re-inspecting `git show :tools/ci/tests/test_pipeline_yaml.py` at review end
  confirms the staged blob now contains `def test_fabric_e2e_keeps_key_vault_authentication_while_disabled():`
  and `assert fabric_e2e["condition"] is False`, and the path no longer appears in `git diff --name-only`.
  Spark 4.0 and 4.1 now assert this invariant identically. No action remains.
- **Residual note (not a defect):** Both ports have now given up the fork-guard assertion on the
  FabricE2E **re-enable** path, since `System.PullRequest.IsFork` is only checked when the condition is
  a string. If that guard is wanted when the job is re-enabled, it belongs on `master`, not here.

### Finding 2: Merge produced a duplicate `BUILD_SBT` constant — Low

- **File / lines:** `tools/ci/tests/test_pipeline_yaml.py:20` and `:32`.
- **Description:** Master defines `BUILD_SBT = REPO_ROOT / "build.sbt"` in its new top constant block
  while the port block already defined the same name; the merge kept both. Confirmed merge-introduced:
  the target→index diff adds `:20` and the master→index diff adds `:32`; neither parent has both.
- **Risk:** None at runtime — identical values, and flake8 `F811` does not flag module-level constant
  rebinding. Merge hygiene only; the duplicate invites a future divergent edit to one copy.
- **Suggested handling:** Optional one-line cleanup — delete the later duplicate (`:32`) and keep the
  master-side `:20`. Still present in the review-end staged blob `947975a17f` (verified at lines 20 and
  32). Deferring is acceptable.

### Finding 3: R wrapper generation still calls `getDefault` unguarded — Low

- **File / line:** `core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala:648`
  (`RWrappable.rParamArg`).
- **Description:** The port-only `safeGetDefault` guard (`:109-115`) covers all three Python paths
  (`:150`, `:161`, and `:317` from the round 1 fix), but `rParamArg` still evaluates
  `(p, thisStage.getDefault(p))` directly. Reachable via `RCodegen.scala:20-24` → `makeRFile` →
  `rParamsArgs:644` → `rParamArg`, and `Wrappable` extends `PythonWrappable with RWrappable` (`:750`),
  so every stage traverses both paths in one `sbt codegen` run. A stage exposing a foreign-owned
  `Param` would fail codegen with the same `IllegalArgumentException` round 1 removed from the Python
  path, relocated to `RCodegen`.
- **Scope — not merge-introduced, currently latent.** `:648` is byte-identical to master and to the
  landed target (`safeGetDefault` count is 3 at `06897e5b27`, 0 at master; no `:648` hunk in the
  master→index diff). No shipped stage owns a foreign parameter, and the round 1 fixture is test-scope
  inside `private[codegen] object PyCodegenFixtures`, so `RCodegen`'s jar scan never instantiates it.
- **Suggested handling:** Do **not** fix in this sync PR — `Wrappable.scala` is shared code and
  AGENTS.md requires cross-version changes to land on `master`. Track as separate follow-up.

## Validation limitations

Static merge-boundary inspection plus locally re-executed CI-policy tests on the live working tree
(50 passed / 27 skipped); the 27 skips require Bash and did not run on this Windows host. The staged
form of `test_pipeline_yaml.py` was reviewed by reading the index blob, not executed, because running
it would require modifying the worktree. Per instruction, the Spark 4.1 owner's local validation is
still in progress and its absence is **not** treated as a code finding: no Scala compile, style, test,
Black, or codegen run is claimed here for 4.1, and no Azure Pipelines run exists for this candidate.
The three paths unstaged at review end are owner work-in-progress and were not reviewed. Finding 3's
exception mechanism is inferred from the round 1 reproduction of the identical expression at `:317`,
not separately reproduced at `:648`. Rounds 4-6 were not executed.

## Resolution log

### Finding 1
- **Status:** **Fixed** — resolved by the owner during this round, not by this review.
- **What changed:** The owner staged `tools/ci/tests/test_pipeline_yaml.py`; the index tree moved from
  `0eabccd3a8` to `947975a17f`. This review edits no source and stages nothing.
- **How verified:** `git show :tools/ci/tests/test_pipeline_yaml.py` now contains
  `..._while_disabled` and `assert fabric_e2e["condition"] is False`; the path is absent from
  `git diff --name-only`; `HEAD`/`MERGE_HEAD` re-verified unchanged. Live tree previously green at
  50 passed / 27 skipped.

### Finding 2
- **Status:** Open (owner decision).
- **How verified:** Duplication confirmed at `:20`/`:32`; suite green.

### Finding 3
- **Status:** Open (deferred by design; belongs on `master`).
- **How verified:** Guard coverage counted in the index (4 `safeGetDefault`, 2 raw
  `thisStage.getDefault`); reachability traced `RCodegen.scala:20-24` → `Wrappable.scala:644` → `:648`.

### Carried forward
- Round 1 Issue 1 (SAR expectation mismatch): confirmed correctly resolved as documentation-only.
  Spark 4.1 retains its baseline `Seq[Row]`; no SAR change was proposed in this round.
- Round 1 Issue 2 (stub default lookup bypassing the guard): confirmed still fixed — `:317` uses
  `safeGetDefault`.
- Round 2: CLEAN; no architectural regression observed from the reliability angle.

## Coordinator-directed resolution addendum, 2026-09-16

The original review above is preserved. The following records the owner's
bounded resolution at the coordinator's request.

### Finding 2: fixed

Removed only the second `BUILD_SBT = REPO_ROOT / "build.sbt"` assignment from
`tools/ci/tests/test_pipeline_yaml.py`, retaining the assignment at line 20.
An AST comparison against staged tree
`ff4844a951c3848f59018accb34e1d733acd4220` verified that the two original
assignments had identical values, exactly one remains, and removing the second
assignment accounts for the entire parsed source difference.

Focused validation ran in WSL with Python 3.12.3, without inherited Git
worktree variables:

```bash
unset GIT_DIR GIT_WORK_TREE
python3 -m pytest -q tools/ci/tests/test_pipeline_yaml.py
python3 -m black --version
python3 -m black --check tools/ci/tests/test_pipeline_yaml.py
```

The pipeline suite passed all 77 tests with no skips in 264.12 seconds.
Black reported version 22.3.0 and the file passed its formatting check.
The complete output is in `spark41/round3-pipeline-validation.log` under the
session artifact directory. The fix and this round-three resolution are staged;
the final index identity is recorded in `spark41/validation-report.json`.

### Finding 3: deferred outside this sync

The coordinator confirmed the raw `RWrappable.rParamArg` default lookup is
pre-existing on master and the port targets. No R wrapper change was made.
The existing guard and its regression evidence concern Python generation;
they are not a claim that R defaults have the same guard. The coordinator
owns the separate guide clarification and any later R follow-up.

### Handoff limitations

The source change in this resolution is confined to the duplicate test constant.
No further Scala build, Spark job, codegen run, or broad audit was launched.
Prior product-source validation remains applicable: 235 tests across 26 suites,
all-module compile/test compile/style, and six-module R/Python codegen.
The earlier full Linux CI run passed 142 tests; this final focused rerun passed
77 pipeline tests after the one-line cleanup.

The master-baseline release snapshot failure remains. Full Python 3.13 with
PySpark 4.1.1 runtime smoke, R runtime, Docker image builds, cloud/remote CI,
and remaining review rounds are not certified by this addendum. No commits,
pushes, PRs, or CI queues were performed.
