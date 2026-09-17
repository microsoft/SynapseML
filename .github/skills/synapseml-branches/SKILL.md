---
name: synapseml-branches
description: >-
  Resolve SynapseML branch-specific rules, runtime baselines, sync policy, and
  CI expectations. Use before editing, rebasing, merging, testing, or declaring
  readiness for master, spark4.0, spark4.1, port/sync branches, or a PR
  whose base branch determines behavior.
compatibility: SynapseML repository with git and GitHub CLI.
---

# SynapseML branch context

Use the PR base branch as the context. A feature branch name does not determine
the runtime, sync policy, or CI that must pass.

## Active branches

Only `master`, `spark4.0`, and `spark4.1` are active. Requests to work on
"branches", "all branches", or branch syncs mean these three unless the user
explicitly names another target. This scope also applies when repairing
downstream SynapseML-Internal compatibility checks.

Historical remote refs are not evidence that a branch is active. Do not create,
refresh, or validate PRs for them as part of an unqualified branch request.
The Spark version used by `master` does not make a similarly named release
branch active.

## Workflow

1. Read root `AGENTS.md`.
2. Resolve the target:
   - For a PR, read `baseRefName` from GitHub.
   - For direct branch work, use the checked-out shared branch.
3. Load the mapped reference. Filenames use `p` for the version decimal:
   - `master` ->
     [branch-spark3p5.md](references/branch-spark3p5.md).
   - `spark4.0` ->
     [branch-spark4p0.md](references/branch-spark4p0.md).
   - `spark4.1` ->
     [branch-spark4p1.md](references/branch-spark4p1.md).
   - Any other explicitly requested target ->
     [branch-fallback.md](references/branch-fallback.md). Verify its historical
     configuration rather than assuming an active branch's baseline.
4. Verify every version, dependency, trigger, skip, and test command against
   that branch's live `build.sbt`, `environment.yml`, workflows, and
   `pipeline.yaml`. References are decision guides, not stale-value authority.
5. Recheck branch context at three points: before implementation, before
   validation, and immediately before push/readiness. Target movement or a
   changed base invalidates earlier evidence.

## Sources of truth

Read these on the target branch; do not maintain a copied version matrix.

| Concern | Source |
| --- | --- |
| Spark and Scala versions | [build.sbt](../../../build.sbt) |
| Python and dependency constraints | [environment.yml](../../../environment.yml), plus `environment.dev.yml` where present |
| JDK selection | [pipeline.yaml](../../../pipeline.yaml), [Java templates](../../../templates), workflow jobs, environment files, and Dockerfiles |
| Selected CI suites and replay targets | [pipeline.yaml](../../../pipeline.yaml) and the Azure definition's trigger settings |
| Databricks runtimes, pools, and notebooks | [DatabricksUtilities.scala](../../../core/src/test/scala/com/microsoft/azure/synapse/ml/nbtest/DatabricksUtilities.scala) |

## Responsibilities

- Ordinary PRs rebase onto their latest target with `--force-with-lease`.
- Shared port branches receive `master` by merge; never rebase or force-push
  the shared branch.
- For conflict resolution, compare merge base, `master`, and port branch;
  ancestry alone does not prove both sides survived.
- Inspect CI definitions on the target branch and confirm builds actually
  queued. Never infer port-branch coverage from `master`.
- Confirm the relevant suites ran by test result/class, not only job status.
- Treat `.github/skills/` as authoritative. `.agents/` is compatibility-only
  and may contain stale copies.
- If no exact reference exists, follow the fallback, state uncertainty, and
  add a concise reference from the
  [branch template](references/branch-template.md) when the branch is an active
  supported target.

## Keep references durable

Keep only branch purpose, compatibility constraints, porting boundaries, and
decision-changing validation rules. Link shared guidance and live sources.
PR numbers, commit IDs, build results, benchmark snapshots, and incident
narratives belong in PR descriptions or review artifacts, not branch references.
Update a reference when a branch contract changes, not after every merge.
