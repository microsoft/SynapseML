# Unlisted target branch

- Determine whether the target is an ordinary feature branch, a shared port
  branch, a release branch, or an automation branch.
- Read its live build files, workflows, pipeline triggers, recent sync PRs, and
  open/closed PRs targeting it.
- Do not guess versions, supported environments, merge policy, or CI coverage
  from the branch name.
- Rebase ordinary PRs; merge into shared branches unless repository history
  proves a different maintained policy.
- Confirm actual queued checks and per-test execution.
- State unresolved branch-specific uncertainty as a blocker. If the target is
  actively supported, add a focused reference using
  [branch-template.md](branch-template.md) before claiming readiness.
