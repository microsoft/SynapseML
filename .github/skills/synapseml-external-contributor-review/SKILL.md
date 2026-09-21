---
name: synapseml-external-contributor-review
description: >-
  Independently triage and review external-contributor SynapseML pull requests,
  and apply authorized maintainer follow-ups without taking over the contribution.
  Use for user-submitted or fork PRs, checking whether a PR solves its linked
  issue, pushing review fixes to a contributor branch, and requesting contributor
  sign-off after current-head tests and Azure Pipelines pass.
compatibility: >-
  SynapseML checkout with git, GitHub CLI, and GitHub/Azure Pipelines access.
  The shared readiness helper uses PowerShell; local Scala validation uses the
  repository's WSL/Linux setup.
---

# SynapseML external-contributor review

Determine whether the contribution solves a real problem before endorsing or
changing it. Preserve the contributor's intent, ownership, and ability to reject
maintainer additions. Use the existing branch, code-review, and PR-loop skills;
this skill adds the contributor-specific safeguards.

## 1. Establish authority and capture the PR

- A request to review or triage is **read-only**: do not push, edit the PR body,
  post comments/reviews, trigger CI, or resolve threads without authorization.
- When asked to apply fixes to the submitted PR, work on that existing PR and
  fork branch. Do not open a replacement PR or merge/close it unless requested.
- Resolve the actual repository from the PR URL, not the local fork remote.
  Pass `--repo <owner/repo>` to GitHub CLI operations. Qualify issue references
  when discussing another repository.
- Read the original issue and all comments, PR body, full diff, commits,
  formal reviews, inline threads, and suppressed/minimized review bodies.
  Check related merged/closed work rather than assuming the issue is current.
- Record author, head repository/branch/SHA, base branch/SHA,
  `maintainerCanModify`, and current checks. Load
  [branch context](../synapseml-branches/SKILL.md) for the PR base.
- Capture an initial snapshot with
  [Get-PrReadiness.ps1](../synapseml-pr-loop/scripts/Get-PrReadiness.ps1)
  using `-Repo <owner/repo> -PullRequest <number>`. Do not use its write switches
  during read-only review.

## 2. Diagnose the issue independently

- Separate the reported symptom, proposed explanation, and proven root cause.
  Do not infer correctness from a plausible one-line change or bot approval.
- Trace the public API through schema/parameter handling, preprocessing,
  transfer/execution mode, shared state, and the native/service boundary.
  Use commit-pinned source; a local checkout may differ from both PR and target.
- Check the reported release and history of relevant defaults. A workaround or
  an older working version is evidence to explain, not confirmation by itself.
- Identify missing facts such as datatype, actual group size, runtime,
  partition/executor topology, or a private dataset. State the conditions under
  which the fix applies instead of inventing the reporter's inputs.
- Reproduce with synthetic data when the original data is unavailable. Include
  unaffected-type/mode controls and genuine invalid-input cases where relevant.
  Check silent incorrect results as well as exceptions.
- Distinguish a real library defect from proof that it caused the reporter's
  exact incident. If that link remains conditional, recommend `Related to`
  rather than an unconditional `Fixes` claim. Edit it only in authorized mode.
- Report introduced defects separately from existing limitations. Do not expand
  a valid narrow fix to unrelated problems or present missing evidence as a
  confirmed production-code bug.

For review-only requests, stop with a verdict, source-linked findings,
reproduction evidence, and explicit uncertainty. The remaining steps apply only
to authorized maintainer changes.

## 3. Isolate changes and preserve the contribution

- Use a dedicated checkout/worktree at the recorded head. Inspect dirty state
  and local instructions; never reset, clean, or stage unrelated work.
- Verify `maintainerCanModify` and actual branch-specific push access.
  A fork's repository-level `permissions.push=false` is not conclusive:
  maintainer access may be limited to the PR branch. A non-mutating push
  dry-run can check access. Do not bypass a permission denial.
- Reuse configured authentication without printing or committing tokens.
  Keep temporary credentials/helpers and evidence out of the source tree.
- Keep a correct contributor fix intact. Make focused, additive commits for
  missing tests, correctness fixes, and directly related documentation.
  Explain necessary changes to the contributor's implementation.
- Prefer a fast-forward follow-up. Do not amend/squash the contributor's
  commits or rewrite their branch history without explicit permission.
  Target integration still follows the
  [PR loop](../synapseml-pr-loop/SKILL.md); obtain permission if it requires
  rewriting the contributor branch. Never rebase shared port branches.
- Re-fetch before pushing. If the author or target moved, reconcile and
  revalidate; do not overwrite new contributions. When a rewrite is authorized,
  use a lease against the observed head, never an unguarded force push.

## 4. Prove the public behavior

- Add a regression that fails for the original behavior and passes with the
  fix. Check that the baseline failed for the intended reason, not missing
  dependencies, timeout, or unrelated setup errors.
- Helper tests are useful but insufficient when the claim concerns a public
  estimator/transformer. Exercise `fit`/`transform` or the equivalent public
  path and force evaluation of the affected output.
- Use the actual failure boundary. For a per-query limit, for example, each
  query must be valid while their merged total exceeds the limit. Control task
  count and execution mode so a different partition layout cannot hide the bug.
- Cover distinct execution paths, such as dense/sparse streaming, when they
  cross different native APIs. Keep tests deterministic and bounded; assert
  meaningful output values, shape, and validity, not just successful fitting.
- Release cached data, native handles, and other test resources in `finally`
  or the repository's resource helpers. Use DataFrame/Dataset APIs, not new RDD
  implementations.
- Read live dependency/runtime versions and use
  [local setup](../synapseml-local-setup/SKILL.md). Run the smallest relevant
  tests, main/test compilation, and style checks; expand for affected surfaces.
  Use [Scala guidance](../scala-code/SKILL.md) and
  [code review](../code-review/SKILL.md) as applicable.
- Label evidence accurately: helper-only, native replay, local public Spark,
  and Fabric end-to-end validation are different claims. Use the `fabric-e2e`
  skill when available and relevant; never describe a native harness as a full
  public Spark run.
- Keep baseline mutations in the isolated checkout, restore the intended fix,
  and verify the exact final diff before committing. Retain logs and source
  identifiers in session artifacts, not generated `target/` files.

## 5. Push the validated follow-up and run CI

- Follow the applicable commit workflow and stage only the reviewed files.
  Preserve attribution and include required session/co-author trailers.
- Push to the contributor's existing PR branch. Verify local, remote, and GitHub
  head SHAs match and the PR diff contains only intended changes.
- Update the description with the actual scope, conditional issue linkage,
  before/after evidence, and remaining limitations. Do not claim pending CI
  passed or erase the distinction between contributor and maintainer changes.
- After **every push**, post `/azp run` and confirm a pipeline actually queued.
  Record build ID, trigger reason, and source version. For a synthetic PR merge
  build, verify its parents include the intended head and current target.
- Check GitHub workflow runs as well as check runs: fork workflows can report
  `action_required` without visible test results. Approve only workflows whose
  code and execution risks were inspected and whose execution is authorized.
  Do not weaken policies or change workflows/dependency pins to make CI green.
- Wait for the current head's automated review, not merely the latest review by
  timestamp. Read both inline threads and suppressed review-body findings.
- Reuse `Get-PrReadiness.ps1 -Repo <owner/repo> -PullRequest <number>
  -WaitForReview` and the PR loop's
  [CI triage](../synapseml-pr-loop/references/ci-triage.md). Use `-RunPipeline`
  only when authorized and a required build is missing; avoid duplicate runs.
- Inspect published per-test results to prove the added tests actually ran.
  Review skipped, canceled, failed, and `succeededWithIssues` results rather
  than trusting job badges. Classify failures from logs, fix relevant defects,
  and repeat validation after any new push.

## 6. Close the loop with the contributor

- Post the final thank-you/sign-off comment **only after** the exact-head
  engineering checks and relevant tests finish successfully and review findings
  are addressed. If blocked, report the blocker without a success-shaped
  completion comment. Keep monitoring asynchronous work instead of treating a
  queued build as completion.
- Refresh head/base and read recent comments immediately before posting. New
  code invalidates older evidence; do not duplicate an already-posted message.
- Adapt [the contributor comment](assets/contributor-comment.md): thank the
  author, acknowledge their fix, identify your commit, summarize your additions,
  link actual validation, request their sign-off, and explicitly offer to revert
  maintainer changes if they do not fit the contributor's intent.
- Distinguish engineering success from human gates. CLA acknowledgment,
  contributor sign-off, and maintainer approval may remain outstanding even
  with green tests. Never accept a CLA for someone else or claim merge readiness
  while a required human gate is pending.
- Verify the posted comment and retain its URL. Report the pushed commit,
  validation result, comment link, and exact remaining human actions. Do not
  merge the PR merely because this workflow is complete.
