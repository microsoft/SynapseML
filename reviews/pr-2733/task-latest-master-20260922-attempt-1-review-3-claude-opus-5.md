# Latest-master sync into spark4.0, round 3

## Review summary

- PR: microsoft/SynapseML#2733. Theme: edge cases and robustness.
- Model: Claude Opus 5.
- HEAD: `96e5ac204b45b1d0c83c8407ca97904c56f7bd94`
- MERGE_HEAD: `0a7fdafaa33ff4785dadc8d7eebee68efde110fb`
- Merge base: `714d365e71f6d2db5b7072094a4a3ad22485eb57`
- Staged tree fingerprint: `c95db6cba93345b1d3fd3912dbb5e8cfc17acaac`
- Unmerged paths: none. Unstaged modifications: none, so the index is the reviewed artefact.
- Findings: 1 Low. Non-blocking observations: 6.
- Verdict: **ISSUES_FOUND (1 Low, documentation only)**. No code, selection, or safety defect.

Scope was the incremental staged change only, `git diff --cached HEAD` excluding `reviews/`:
11 paths — `tools/ci/e2e_impact.py`, `tools/ci/tests/test_e2e_impact.py`,
`tools/ci/tests/test_pipeline_yaml.py`, `tools/ci/README.md`, `pipeline.yaml`, the deletions of
`tools/ci/databricks_impact.py`, `tools/ci/tests/test_databricks_impact.py` and
`tools/pytest/run_all_tests.py`, plus `GeospatialCoreSuite.scala`, `AzureMapsSuite.scala` and
`VerifyValueIndexer.scala`.

## Nothing incoming was dropped

Independently recomputed rather than taken from the proof file. The incoming master increment
(merge base to MERGE_HEAD, excluding `reviews/`) is **18 paths**: 11 byte-identical in the staged
tree, 3 deleted exactly as master deletes them, and 4 differing. That is 14 aligned and 4
port differences, which matches the recorded claim. No path was missing from the port and no
master deletion was left behind.

The four differences are all justified and all sit on the Fabric-disable boundary or port runtime:
`pipeline.yaml`, `tools/ci/tests/test_pipeline_yaml.py`, `tools/ci/tests/test_e2e_impact.py`, and
`docs/Reference/Developer Setup.md`. The doc difference is a single paragraph that prefixes
"Fabric E2E is disabled on this branch" while **retaining** master's preflight wording, so no
incoming documentation content was lost.

`tools/ci/e2e_impact.py` is byte-identical to master, confirming the selector itself was not forked.
The five incoming `nbtest` files are byte-identical too. The staged incremental diffs for this
port and microsoft/SynapseML#2734 are textually identical apart from two blob index lines, so
both ports received the same change.

## Error paths and fail-open selection

`select_suites` catches `OSError`, `subprocess.SubprocessError` and `ValueError`; `UnicodeDecodeError`
is a `ValueError`, so strict UTF-8 path decoding is covered, and all four are exercised. Every
caught path returns `ALL_SUITES`, so no error can produce a skip. `json.dumps(detail)` wraps the
message, which matters because a newline in captured git stderr would otherwise break out of the
`##vso[...]` line; the path-level test case `website/\n##vso[task.setvariable variable=x]false.md`
covers the same injection shape from the other direction and yields `ALL_SUITES`.

The pipeline gate is genuinely fail-open: `ne(dependencies.BuildAndCacheSbt.outputs['detectTestImpact.runDatabricksCpuE2E'], 'false')`.
An unset, empty or malformed output runs the job. `tools/ci/tests/test_e2e_impact.py` pins that exact
`ne(...,'false')` literal, so a silent flip to `eq(...,'true')` would fail the suite.

The one non-fail-open path is by design and worth stating: an exception type outside the caught set
would exit non-zero, `set -euo pipefail` would fail the step, `BuildAndCacheSbt` would fail, and
`succeeded()` would skip the E2E jobs. That is a red build, not a silent skip, so the failure is
visible. `main()` otherwise always returns 0 and emits a decision for every suite.

## Git metadata validation

`changed_paths` refuses anything that is not a real queued merge: `BUILD_SOURCEBRANCH` must fullmatch
`refs/pull/[1-9][0-9]*/merge` (leading zeros rejected), `HEAD` must equal `BUILD_SOURCEVERSION` and
match the 40/64-hex object pattern, and `rev-list --parents` must yield exactly three ids whose first
equals HEAD and whose third equals `SYSTEM_PULLREQUEST_SOURCECOMMITID`. Each mismatch is tested, as is
a source-branch checkout impersonating a merge.

`test_target_advancement_cannot_erase_the_queued_runtime_change` is the strongest case: after the
target fast-forwards to contain the source, `git diff master HEAD` is empty, yet the selector still
reads the recorded first parent and returns the runtime path. Comparing against a moving tip would
have skipped the tests.

Raw-mode parsing is strict. `--no-renames` forces a move to appear as delete plus add, so a file
leaving a runtime directory cannot be reclassified as docs-only. The NUL framing is validated
(trailing empty field, even field count), the header must be five fields, both modes must be regular,
and the status must be A, D or M — so symlinks, gitlinks and type changes are rejected. Modes 120000
and 160000 planted under `README.md` are tested through a real `commit-tree` merge and yield `ALL_SUITES`.

## Shallow refs

The `fetchDepth: 1` to `2` change is the matching fix for the parent-based diff, and it is verified
empirically rather than asserted: a real depth-1 clone returns `ALL_SUITES` and a depth-2 clone
returns the skip set. Either way a shallow checkout cannot cause an incorrect skip, because a missing
first parent makes `git diff` fail into the fail-open branch. `test_pipeline_gates_only_audited_families_and_keeps_full_schedule`
also asserts `fetchDepth >= 2`, so the pipeline cannot silently regress to depth 1.

## Deleted files

All three deletions match master, and `tools/pytest` has no tracked files left. An index-wide search
finds exactly one remaining mention of the removed runner, `tools/ci/tests/test_e2e_impact.py:106`,
where it is a test **input string** asserting that an unrecognized path keeps every family. Nothing
executes it, and `tools/ci/README.md` contains no reference to either removed helper. Deletion of a
runtime file is tested to keep all families; deletion of a Python test is tested to remain skippable.

## Fabric-disable boundary

Correct in the pipeline and tests. `FabricE2E` keeps `condition: false` with the Spark 4.0 runtime
comment, `runFabricE2E` appears **zero** times in the port `pipeline.yaml`, and exactly one boolean
condition exists in the file. The port tests assert the boundary explicitly:
`test_fabric_e2e_keeps_key_vault_authentication_while_disabled` and the Spark 4.0 runtime guard both
assert `condition is False`, and `test_pipeline_gates_only_audited_families_and_keeps_full_schedule`
takes a Fabric-specific branch while keeping master's `ne(...)` and `succeeded()` checks for both
Databricks jobs and `dependsOn == "BuildAndCacheSbt"` for all three. `docs/Reference/Developer Setup.md`
states the branch fact. The selector still emits an unused `runFabricE2E`, which is harmless and asserted.

### L1 (Low) — `tools/ci/README.md` overstates Fabric E2E on this branch

Lines 102–104 state that `e2e_impact.py` "can skip the five Databricks CPU jobs, one Databricks GPU
job, and Fabric E2E" and that "any unrecognized path enables all seven jobs". On this branch only six
jobs are selector-gated; the seventh never runs for any input, because `FabricE2E` is unconditionally
disabled and its decision variable is not referenced. A reader on spark4.0 could conclude that an
unrecognized path re-enables Fabric E2E.

Documentation only — no selection, safety or coverage behaviour is affected, and the neighbouring
counts are accurate (the CPU matrix is 5, and `after_n_builds` is 54 here and on master). A one-clause
branch note, or narrowing "seven" to the Databricks jobs, resolves it.

Declining is defensible and should then be recorded: the file is byte-identical to master, which keeps
future merges conflict-free, and the branch fact is already stated in `docs/Reference/Developer Setup.md`
and in the `pipeline.yaml` comment. Repo rules require only `AGENTS.md` and `CONTRIBUTING.md` to match
across branches, so a branch-specific note here is permitted but not required.

## Scala changes

`GeospatialCoreSuite` is genuinely offline: it extends `TestBase` without the `AzureMapsKey` mixin, so
no secret is read; requests are built through `inputFunc` and never sent; `transformSchema` and the
retirement `UnsupportedOperationException` need no service. The persistence test writes each stage to a
distinct `tmpDir` subdirectory, so the scalar and column variants cannot collide, and it re-checks
schema equality and the retirement error after load. `AzureMapsSuite` only drops imports left unused by
the earlier suite removal and adds a pointer comment. `VerifyValueIndexer` removes two
`for (mmlStyle <- ...)` loops whose variable was never read, so the bodies now run once instead of
twice identically; the locals move to method scope without changing behaviour.

## Non-blocking observations

1. An uncaught exception type fails the step rather than failing open; the result is a red build, not a
   silent skip. Correct trade-off, recorded for clarity.
2. The warning is written to stderr while decisions go to stdout. If logging commands are not parsed
   from stderr the text still reaches the log, only without the warning annotation.
3. `test_databricks_e2e_uses_fail_open_pr_impact_detection` checks the output reference but not the
   `ne` operator; the direction is pinned separately in `tools/ci/tests/test_e2e_impact.py`, so combined
   coverage is adequate.
4. `jobs[job].get("condition", "")` and `job["condition"]` in the pipeline tests would raise
   `TypeError`/`KeyError` if another job gained a boolean or absent condition. Today exactly one boolean
   condition exists and it is excluded from those loops, so this is latent only.
5. `"tools/pytest/run_all_tests.py"` survives as a sample unrecognized path for a file that no longer
   exists. Still valid as a case, mildly stale as an example.
6. `SYNAPSEML_FULL_TESTS` is normalised with `.lower()`, so `False` from parameter expansion is accepted.
   Tests pin the unsafe direction (unknown runs everything) but not the accepted capitalised form; a
   regression there would fail safe.

## Evidence and limits

Verified directly: merge state, the 18-path incoming audit, per-path blob identity, cross-port diff
equality, index-wide reference search, and the pipeline/test/doc contents quoted above. Local gates
(aggregate compile, style, 68 Scala tests, 291 CI helper tests, Black) are taken as reported and were
not re-run; no Azure or live-service result is claimed.

Rounds 1 and 2 exist for this attempt and their conclusions were deliberately not used as evidence.
The recorded round-2 Gemini artefact is an availability record: the backend returned
`400 invalid request body` before the reviewer started, so no Gemini-family review executed. The
three-family gate is **unfulfilled** and this is not a completed gauntlet.

No source edit, staging, commit, push, agent dispatch or cloud call was performed in this round.

## Resolution log

L1 is fixed. `tools/ci/README.md` now describes the six Databricks jobs, states
that Fabric stays disabled regardless of selector output, and limits full-test
claims to enabled jobs. This branch-specific correction preserves the disabled
runtime boundary rather than promising coverage that cannot run. The existing
291-test validation pins the unchanged job conditions; `git diff --check` checks
the documentation-only follow-up. No executable source changed.
