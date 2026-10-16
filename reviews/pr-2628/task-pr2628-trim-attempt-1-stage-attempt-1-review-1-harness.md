# CI and workflow simplification audit

PR: microsoft/SynapseML#2628. Verdict: recommend bounded reductions, not wholesale removal.
Base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`.
Head: `800589ac8dfe4bd23596cb174c9ec997e574b6ae`.
Verified 0 behind, 10 ahead against the supplied master base. Parent performed the no-op rebase.
Fresh audit of the complete owned base-to-head diff and current files; no earlier reports used.
Effective Medium, human-directed six-component fleet; this is the CI/workflow assignment.
Scope is substantial, with tag recovery, conditional jobs and publication failure paths.
Exposed model: `gpt-6-astra`. No model overrides or nested reviewers.
No source edits, repository ref mutations, network calls or live publishing operations.

## Complete scope inventory

13 changed files, +2,903/-183 lines; 6,411 current lines inspected. New files were reviewed in full.

| File | Decision and reason |
| --- | --- |
| `.github\workflows\pr-validation.yml` | Keep. Dispatch, complete release tests, full history and launcher verification are necessary wiring. |
| `.github\workflows\release-notebook-validation.yml` | Keep. Its 33 lines cover notebook-only PRs otherwise excluded by PR validation. |
| `.github\workflows\release-notes.yml` | Simplify historical commentary; fix C1. Keep manual approval, evidence checks and confirmed-absence publication. |
| `.github\workflows\release-prepare.yml` | Simplify commentary. Keep reviewed PR creation, App permissions, exact merged source and docs checks. |
| `.github\workflows\release-tag-spark.yml` | Simplify printed recovery instructions. Keep same-repository merge authorization, immutable tags and leased cleanup. |
| `.github\workflows\release-tag.yml` | Simplify redundant failure bookkeeping and ancestry check. Keep target selection, bootstrap and recovery behavior. |
| `.github\workflows\website-deploy.yml` | Keep both added lines. Preview mode does not authorize Pages deployment. |
| `pipeline.yaml` | Simplify repeated immutable parameter checks only. Keep publication ordering, per-job source guards and producer receipts. |
| `tools\ci\tests\test_pipeline_yaml.py` | Simplify conditional-job simulators and duplicated fork-test setup; preserve structural and execution coverage. |
| `tools\ci\tests\test_e2e_impact.py` | Keep. The changed assertions distinguish full release checkout from shallow PR-impact checkout. |
| `scripts\release\test_release_workflows.py` | Simplify one redundant branch-preservation text test. Keep event, permission and guard-order contracts. |
| `scripts\release\test_release_tag_recovery.py` | Simplify tests of the duplicate predecessor helper and long error wording. Keep real disposable-Git recovery cases. |
| `scripts\release\test_prev_tag.sh` | Drop after transferring remaining cases to the actual workflow execution test. It tests a different algorithm. |

## Ranked reductions

Estimates are net physical lines across the named files, not counts of parametrized test cases.
Recommendations are disjoint and total approximately 200-215 lines; none needs a new helper file or public API.

1. **Remove the second predecessor implementation, about 80 lines.**
   Delete `scripts\release\test_prev_tag.sh:1-66` and the helper-only test in
   `scripts\release\test_release_tag_recovery.py:286-304`.
   Extend the existing workflow execution cases at `:241-283` with gap, derivative-only, later-tag and large-list inputs.
   Evidence: the shell helper uses Python sorting and a non-exiting awk; production uses `sort -V` and early exit.
   The duplicate passes while C1 fails. The existing test already executes the real YAML and verifies git failures.

2. **Replace hand-written Azure condition simulations, about 40 lines.**
   In `tools\ci\tests\test_pipeline_yaml.py:1089-1158`, replace the 16-case optional-job expansion
   and four-case top-level expansion with direct assertions of the conditional keys, dependency lists and referenced jobs.
   These loops evaluate test-owned boolean maps, not Azure's expression engine. Their repeated cases add no engine proof.
   A read-only probe verified the smaller structural assertions against this head, including the ordinary-Publish else case.
   Retain enabled optional dependencies, required gates and the `publishRelease && publishArtifacts` Release condition.

3. **Remove duplicate static setup where execution tests already prove the contract, about 24 lines.**
   Drop `scripts\release\test_release_workflows.py:110-125`; recovery tests at
   `scripts\release\test_release_tag_recovery.py:811-854` actually preserve orphan/closed branches and reject a concurrent ref.
   Keep those execution tests and new-branch chain coverage at `:1017-1037`.
   Fold the stronger fork predicate from `tools\ci\tests\test_pipeline_yaml.py:324-334` into the existing
   Fabric authorization test at `:376-390`, replacing its weaker substring assertion. Do not delete the predicate.

4. **Remove single-failure aggregation, about 14-17 lines.**
   In `.github\workflows\release-tag.yml`, remove `FAILED` at `:251`, replace append-and-break pairs at
   `:342-350`, `:371-375`, `:380-384`, `:447-453` with immediate failure, and delete `:457-460`.
   Every append immediately stops the loop; the array can never aggregate multiple failures.
   Remove the duplicate merged-result ancestry check at `:347-351`, already enforced by `reconcile_target_tags` at `:275-278`.
   Keep the target-specific diagnostic, rebase abort, exact-ref creation lease, existing-PR handling and guarded tag push.

5. **Shorten repeated narrative, about 40-45 lines.**
   Condense `.github\workflows\release-notes.yml:3-19` and `release-prepare.yml:3-10,157-161`
   to purpose and non-obvious constraints, without historic release anecdotes or a second description of bump internals.
   Replace `release-tag-spark.yml:74-85` with a short refusal, immutable-tag warning and existing recovery-runbook link.
   Correspondingly trim wording/order assertions in `scripts\release\test_release_tag_recovery.py:323-337`.
   Keep the executable empty-merge rejection and check the diagnostic directs maintainers to guarded recovery.

6. **Check immutable pipeline parameters once, seven lines.**
   Remove the repeated style/unit/Python shell check at `pipeline.yaml:230-233` and its environment entries at `:239-241`.
   `BuildAndCacheSbt:149-165` already rejects those same immutable parameters plus disabled artifact publication;
   Publish depends on its successful completion. Keep Publish's own `release_guard.py maven` invocation at `:234`.
   That invocation validates a different checkout and exports job-local source/runtime variables, so it is not redundant.

## Correctness finding

C1, low-frequency release-notes failure: `.github\workflows\release-notes.yml:103-106`.
With `pipefail`, awk's early `exit` closes sort's pipe. An offline fixture with `v1.2.0` followed by
20,000 higher primary tags reproduced exit 141 and no output record. All ordinary tests still passed.
Use the existing non-short-circuiting form `$0 == cur {found=1} !found {last=$0} END {print last}`.
An in-memory substitution returned zero and the correct empty predecessor on the same fixture. Add this case under R1.

## Rejected cuts

- Keep approved plan identities, source/runtime binding, explicit optional Spark 4.0 selection and bootstrap inputs unchanged.
- Keep exact remote-ref checks, atomic tag publication, orphan-branch refusal and empty creation leases. Local tags are not remote evidence.
- Keep the separate Publish and Release jobs, producer-attempt artifact names and receipt downloads. Rebuilding bytes is not receipt handoff.
- Do not replace Release's inline Conda setup at `pipeline.yaml:674-693` blindly with `templates\conda.yml`.
  The template also cleans SDKs and has different retry/failure conditions; that is a behavior change, not equivalent deduplication.
- Do not introduce a shared workflow framework merely to combine two launcher or App-configuration blocks.

## Validation and bounded rerun

331 cases passed across the four owned Python suites under WSL, with disposable repositories, file-only Git transport and no pytest cache.
The existing predecessor shell check passed all eight checks under Git Bash. Native WSL Git cannot read this Windows worktree's gitdir pointer.
The C1 stress probe and direct dependency-map probe were separate offline checks. No hosted CI, Scala build or publication was run.
For authorized edits, rerun the owned suites from this worktree using Linux Python; native-Windows skips are not equivalent coverage:

```powershell
wsl -d Ubuntu --cd "$PWD" --exec python3 -c "import pytest; from pathlib import Path as P; raise SystemExit(pytest.main([str(P('scripts','release','test_release_workflows.py')),str(P('scripts','release','test_release_tag_recovery.py')),str(P('tools','ci','tests','test_pipeline_yaml.py')),str(P('tools','ci','tests','test_e2e_impact.py')),'-q','-p','no:cacheprovider'])))"
```

Parent aggregation and authorization are still required before implementing these reductions.

## Authorized resolution and evidence

Implemented R1-R6 and C1 against the same head: nine code/test files, +73/-273, net -200 lines.
Transferred first-release, gap, numeric-order, derivative-only and later-tag cases to actual-YAML execution before deleting the shell helper.
The new 20,000-later-tag regression failed before C1 with exit 141; it now passes with the correct nonempty predecessor.
Targeted command: `pytest.main([*files, '-q', '-p', 'no:cacheprovider', '--basetemp', str(Path(temp, 'tests')), '--tb=short'])`: 194 passed.
`files` selected `scripts\release\test_release_workflows.py`, `scripts\release\test_release_tag_recovery.py`, `tools\ci\tests\test_pipeline_yaml.py`, and `tools\ci\tests\test_e2e_impact.py::test_pipeline_gates_only_audited_families_and_keeps_full_schedule`.
Pinned Black 22.3 `--check` passed on the three changed Python files; `git diff --check` passed.
Execution used WSL Python 3.12, disposable temporary repositories, file-only Git transport, `/dev/shm`, and disabled bytecode/pytest cache.
The actual prewarm shell gate passed all five probes: all-enabled proceeds; each disabled required job rejects before the source guard.
No unresolved owned issues. Leases, orphan refusal, predecessor ordering, independent source guards and receipt handoff remain; parent owns aggregate CI. No commits or pushes.
