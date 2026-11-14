# Execution checklist

Use this checklist on entry, handoff, and resume. Keep evidence and next actions
in the existing [checkpoint](loop-control.md#checkpoint-before-yielding), and
mirror unfinished work in the agent's task tracker. Do not create a second
checkpoint or reset budgets.

The [readiness gates](readiness-gates.md) define the requirements. This checklist
keeps the work visible; it does not replace those gates.

| Step | Evidence to retain |
| --- | --- |
| Scope | Request, allowed actions, trusted guidance, safety clearance, head and target SHAs |
| Value | Acceptance criteria, baseline, supported behavior, and remaining uncertainty |
| Integration | Current target and patch comparison, or the permission blocking integration |
| Local validation | Applicable commands, results, tested revision, and justified skips |
| Review findings | Active, resolved, outdated, and suppressed findings with dispositions |
| CI | Current-revision check/build IDs, actual test results, and any applicable exemption |
| Final review | For the full engineering loop, required rounds on the frozen patch within the original limits |
| Handoff | Fresh remote evidence, unresolved gates, and separate human approval or sign-off |

Mark excluded steps not applicable with a reason, never passed. Validation-only
mode does not authorize integration, edits, publication, discussion resolution,
or the review gauntlet. Review-only mode does not authorize CI.

## Continue without another prompt

- Work through the next authorized action until the requested scope is complete
  or a specific blocker remains. A correct external PR still needs applicable
  validation even when no maintainer edits are needed.
- Check for current-revision CI before triggering. Reuse an existing build and
  record trigger/build IDs so a handoff or resume cannot queue duplicates.
- While review or CI is pending, keep the task open and use the existing bounded
  wait. When it finishes, read the results, update the checkpoint, and continue.
  A queued build or attached watcher is not a completed task.
- Triage failures before retrying. Make fixes only within the granted scope.
  Preserve the original deadlines and consumed attempts from
  [loop control](loop-control.md#state-and-cost-discipline). At a limit, report
  the unresolved gate rather than starting another loop.
- Before finishing, reread the live head, target, checks, and review discussion.
  Reconcile changed revisions and new findings before reporting the outcome.

Report engineering readiness only when every applicable gate passes.
For validation-only work, report the validation result and remaining gates,
including **validated without maintainer changes** when appropriate.
For review-only work, return the verdict and identify validation not performed.
If blocked, name the missing permission, evidence, failed check, or exhausted
limit and the next action. Do not ask the user to repeat an authorization already
recorded for the same scope.
