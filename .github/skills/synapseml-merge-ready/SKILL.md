---
name: synapseml-merge-ready
description: Make one or more SynapseML issues or pull requests evidence-based merge-ready. Use for "5/5 confidence", "200% ready", stale/outdated PR remediation, rebase-and-test requests, resolving all review comments, or proving a feature ships without correctness, compatibility, performance, or Spark regressions.
compatibility: SynapseML repository with git, GitHub CLI, PowerShell, WSL/Linux, sbt, Python, and network access to GitHub/Azure Pipelines.
---

# SynapseML merge-ready workflow

Treat "5/5" or "200%" as an evidence standard, never a literal guarantee.
The exit condition is: the requested value is proven through the public API,
the current target is integrated, review is exhausted, and every required check
is complete and green.

## Workflow

### 1. Establish scope and isolation

- Read the issue, PR body, linked work items, commit history, changed files,
  active threads, and review bodies containing suppressed comments.
- Give each PR a dedicated worktree and branch. Parallelize independent PRs,
  but identify overlapping files and required merge order first.
- Run
  [scripts/Get-PrReadiness.ps1](scripts/Get-PrReadiness.ps1)
  with `-PullRequest <numbers>` and retain its JSON as the initial snapshot.

### 2. Integrate the current target

- Fetch the PR's target branch and rebase an ordinary PR before validation.
- Use `--force-with-lease`, never an unguarded force push.
- Merge, rather than rebase, shared `spark4.x` branches.
- Record target SHA, head SHA, merge base, ahead/behind counts, and conflicts.
- Fetch again immediately before the final push. If the target advanced,
  integrate it and rerun affected validation.

### 3. Define the value and regression contract

- State the user-visible bug or feature, supported/unsupported cases, default
  behavior, compatibility contract, and measurable acceptance criteria.
- Trace the real public path: Scala stage, generated/hand-written Python,
  schema, serialization, persistence, service/native boundary, and packaging.
- Establish a baseline when failures, performance, or external systems are
  involved. A passing new test is insufficient if the old behavior was never
  shown to fail.

### 4. Review and implement

- Apply the [code-review skill](../code-review/SKILL.md).
- Resolve root causes, not only the reported line. Recheck sibling APIs and
  language surfaces that share the same serializer, schema, parameter, or
  native/service path.
- Preserve public JVM and serialized compatibility unless explicitly approved.
- Follow the Spark and performance gates in
  [references/spark-performance.md](references/spark-performance.md).
- Reply in the existing thread with the fix and evidence, then resolve it.
  Re-audit after every push because new Copilot comments may appear.

### 5. Add proof-oriented tests

- Add a regression that fails before the fix and passes after it.
- Cover positive, negative, null/empty, boundary, schema, copy, save/load, and
  Python/codegen behavior as applicable.
- Exercise the public transformer/estimator or request path end to end; helper
  tests alone do not prove the feature ships.
- Use real hardware, native libraries, clusters, network families, or services
  when the claim depends on them. Do not infer capability from configuration or
  provider discovery alone.

### 6. Validate locally and across branches

- Use the [local setup skill](../synapseml-local-setup/SKILL.md) and its JDK
  wrapper.
- Run the smallest targeted suites, compile, test compile, Scala style, pinned
  Black, codegen, generated-wrapper checks, and relevant Python tests.
- Run release compatibility for Spark 4.1 and any other branch affected by the
  change.
- Benchmark representative scale before/after when a hot path, network path,
  accelerator, allocation pattern, or algorithmic complexity changes.

### 7. Run and triage full CI

- Push the exact validated head and comment `/azp run`.
- Inspect every failed, canceled, skipped, and pending job. Use
  [references/ci-triage.md](references/ci-triage.md) to separate product
  defects, test defects, baseline failures, and infrastructure failures.
- Fix product/test defects and rerun. Infrastructure classification requires
  logs proving tests did not exercise the change; "looks flaky" is not evidence.
- Do not declare readiness while any required check is pending.

### 8. Final readiness loop

Run `Get-PrReadiness.ps1` again and confirm every gate in
[references/readiness-gates.md](references/readiness-gates.md).

For multiple PRs, after each merge:

1. fetch the new target;
2. rebase overlapping downstream PRs;
3. rerun targeted, compatibility, and full CI;
4. re-audit review threads and suppressed comments.

Report the exact remaining blocker. "Only human approval remains" is valid only
when all engineering gates are complete.
