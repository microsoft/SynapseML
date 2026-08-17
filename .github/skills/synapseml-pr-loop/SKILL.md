---
name: synapseml-pr-loop
description: >-
  Make one or more SynapseML issues or pull requests evidence-based merge-ready.
  Use for "5/5 confidence", "200% ready", stale/outdated PR remediation,
  rebase-and-test requests, resolving all review comments, or proving a feature
  ships without correctness, compatibility, performance, or Spark regressions.
compatibility: >-
  SynapseML repository with git, GitHub CLI, PowerShell, WSL/Linux, sbt, Python,
  and network access to GitHub/Azure Pipelines.
---

# SynapseML PR loop

Treat "5/5" or "200%" as an evidence standard, never a literal guarantee.
The exit condition is: the requested value is proven through the public API,
the current target is integrated, review is exhausted, and every required check
is complete and green.

## Workflow

### 1. Establish scope and isolation

- Load the [branch context skill](../synapseml-branches/SKILL.md) using the PR
  base branch. Recheck it before validation and immediately before final push.
- Read the issue, PR body, linked work items, commit history, changed files,
  and every review thread/body, including resolved, outdated, minimized, and
  suppressed comments. Verify prior resolutions rather than trusting status.
- Inspect formal review decisions, requested-change votes, ownership gates, and
  coverage thresholds; resolved threads do not clear those blockers.
- Check recently merged/closed related PRs and issues. Identify follow-up PRs
  needing rebase/remediation, superseded work to close, and remaining issue
  action items; do not assume closure completed the feature lifecycle.
- Give each PR a dedicated worktree and branch. Parallelize independent PRs,
  but identify overlapping files and required merge order first.
- Run
  [scripts/Get-PrReadiness.ps1](scripts/Get-PrReadiness.ps1)
  with `-PullRequest <numbers>` and retain its JSON locally as the initial
  snapshot. It can contain review text; redact it before public sharing.

### 2. Integrate the current target

- Fetch the PR's target branch and rebase an ordinary PR before validation.
- Use `--force-with-lease`, never an unguarded force push.
- Merge, rather than rebase, shared `spark<version>` port branches.
- Record target SHA, head SHA, merge base, ahead/behind counts, and conflicts.
- Compare the intended patch before and after rebase/conflict resolution.
- Fetch again immediately before the final push. If the target advanced,
  integrate it and rerun affected validation.

### 3. Define the value and regression contract

- Keep the PR title and description aligned with the current scope. Lead with a
  short human-readable change/value summary; put detailed design and validation
  evidence afterward. Refresh both after material changes.
- State the user-visible bug or feature, supported/unsupported cases, default
  behavior, compatibility contract, and measurable acceptance criteria.
- Trace the real public path: Scala stage, generated/hand-written Python,
  schema, serialization, persistence, service/native boundary, and packaging.
- Confirm the published package actually contains the capability; local jars,
  custom natives, or provider discovery do not prove that users receive it.
- Establish a baseline when failures, performance, or external systems are
  involved. A passing new test is insufficient if the old behavior was never
  shown to fail.

### 4. Review and implement

- Apply the [code-review skill](../code-review/SKILL.md).
- Resolve root causes, not only the reported line. Recheck sibling APIs and
  language surfaces that share the same serializer, schema, parameter, or
  native/service path.
- Preserve public JVM and serialized compatibility unless explicitly approved.
- Update user-facing documentation/examples for changed public behavior. Edit
  Scala sources rather than generated files under `target/`.
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
- Before external service tests, audit resource creation/deletion and use only
  authorized test resources.

### 6. Validate locally and across branches

- Use the [local setup skill](../synapseml-local-setup/SKILL.md) and its JDK
  wrapper.
- Run the smallest targeted suites, compile, test compile, Scala style, pinned
  Black, codegen, generated-wrapper checks, and relevant Python tests.
- Run release compatibility for every port branch affected by the change.
- Benchmark representative scale before/after when a hot path, network path,
  accelerator, allocation pattern, or algorithmic complexity changes.

### 7. Run and triage full CI

- Push the exact validated head, comment `/azp run`, then confirm a build
  actually queued -- a comment is not evidence that CI ran, so cite the build
  ID. A trigger-driven build records `reason=pullRequest`; one you queued
  yourself records `reason=manual`, which is the quickest way to tell whether
  the trigger really fired or you merely re-ran it by hand.
- If no build appears, check the pipeline definition's own pull-request trigger
  rather than assuming a transient failure. That trigger can be defined in the
  pipeline UI, in which case it overrides the `pr:` block in `pipeline.yaml`
  entirely and silently ignores targets the YAML lists. Read its branch filters
  through the definitions API. Until the filter is corrected, queue explicitly
  against `refs/pull/<number>/merge` -- never `refs/heads/<branch>`, which
  validates the branch instead of the merge result.
- Inspect every failed, canceled, skipped, and pending job. Use
  [references/ci-triage.md](references/ci-triage.md) to separate product
  defects, test defects, baseline failures, and infrastructure failures.
- Fix product/test defects and rerun. Infrastructure classification requires
  logs proving tests did not exercise the change; "looks flaky" is not evidence.
- If path filters or a CI-only diff bypass the behavior being repaired, validate
  it with a representative product change or controlled integration PR.
- Do not declare readiness while any required check is pending.

### 8. Final readiness loop

Run `Get-PrReadiness.ps1` again and confirm every gate in
[references/readiness-gates.md](references/readiness-gates.md).

For multiple PRs, after each merge:

1. fetch the new target;
2. rebase overlapping downstream PRs;
3. rerun targeted, compatibility, and full CI;
4. re-audit review threads and suppressed comments.

After any merge or closure, reconcile linked work: update or close fulfilled
issues, close superseded PRs with an explanation, and rebase/remediate still
valuable follow-ups. Preserve separate unresolved scope rather than closing it
for convenience.

Report the exact remaining blocker. "Only human approval remains" is valid only
when all engineering gates are complete.
