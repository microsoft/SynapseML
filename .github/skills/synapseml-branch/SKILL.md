---
name: synapseml-branch
description: >-
  Resolve SynapseML branch-specific rules, runtime baselines, sync policy, and
  CI expectations. Use before editing, rebasing, merging, testing, or declaring
  readiness for master, spark3.5, spark4.0, spark4.1, port/sync branches, or a PR
  whose base branch determines behavior.
compatibility: SynapseML repository with git and GitHub CLI.
---

# SynapseML branch context

Use the PR base branch as the context. A feature branch name does not determine
the runtime, sync policy, or CI that must pass.

## Workflow

1. Read root `AGENTS.md`.
2. Resolve the target:
   - For a PR, read `baseRefName` from GitHub.
   - For direct branch work, use the checked-out shared branch.
3. Load the matching reference:
   - [master](references/master.md)
   - [spark3.5](references/spark3.5.md)
   - [spark4.0](references/spark4.0.md)
   - [spark4.1](references/spark4.1.md)
   - [fallback](references/fallback.md) for any other branch
4. Verify every version, dependency, trigger, skip, and test command against
   that branch's live `build.sbt`, `environment.yml`, workflows, and
   `pipeline.yaml`. References are decision guides, not stale-value authority.
5. Recheck branch context at three points: before implementation, before
   validation, and immediately before push/readiness. Target movement or a
   changed base invalidates earlier evidence.

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
  add a concise reference when the branch is an active supported target.
