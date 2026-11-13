# Shared execution checklist

Read this on entry to either the PR loop or external-contributor review. This
is the operational index to [readiness gates](readiness-gates.md), not a second
set of weaker gates. Keep it in the existing checkpoint and the host's task
tracker; do not create a planning file in the PR or start a separate agent just
to transfer ownership.

## Record once, update continuously

For each gate below record `id`, `status`, `evidence`, `headSha`, `baseSha`,
`nextAction`, and `blocker`. Status is `pending`, `running`, `passed`,
`not_required`, or `blocked`. Evidence includes command/result or a build,
review, or artifact link, not an assurance such as "looks good". A
`not_required` gate needs the exact mode/scope rule and supporting paths.
Map these to the host's available task states without losing the full status
in the checkpoint; a blocked gate must not become a completed task.

Record the original request, execution mode, trusted guidance source,
authorization, owner, checkpoint path, and existing budgets alongside the gates.
Never infer permission from a gate status. Reuse earlier evidence only when
its revision, scope, and environment still match. Apply
[evidence invalidation](loop-control.md#evidence-invalidation) after any change.

| ID | Gate and required evidence | Next action when incomplete |
| --- | --- | --- |
| scope | Trusted instructions, actual repo/base/head, mode, permissions, contributor classification and any safety clearance | Resolve missing facts; block only the actions whose authority or safety is missing |
| value | Issue/root cause, acceptance criteria, bounded diff and public behavior/compatibility contract | Trace the actual path and establish the baseline; do not manufacture edits for a correct PR |
| integration | Current target and patch preservation; local/remote/head identity when publishing | Integrate only with permission; validation-only records the unmet gate without rewriting the contributor branch |
| local | Applicable regression, public-path, compile/style, wrappers, compatibility and runtime evidence | Run the smallest sufficient checks; triage failures and repair only within granted scope |
| review | Whole-patch review and dispositions for every substantive finding, including suppressed bodies | Reproduce, fix or rebut; discussion writes require their own permission |
| azure | Scope decision; if required, verified build/merge identity, terminal jobs and actual relevant test evidence | Reuse existing CI or trigger once when authorized; monitor, inspect results and triage |
| github | Applicable Actions, required checks, hidden approval gates and current-head automated review | Inspect workflow runs, request authorized missing review, wait for terminal checks and consume feedback |
| gauntlet | Required pre-commit passes and final six-round pass on the frozen patch | Execute the existing review contract; validation-only is not required, never a fabricated pass |
| reconcile | Fresh head/base, complete evidence, no unresolved engineering findings, invalidation audit | Return to the affected gate on new findings or revision movement; preserve reusable evidence |
| handoff | Final outcome, evidence links, residual risk and human gates; authorized contributor message when additions exist | Finish authorized publication/handoff; distinguish engineering readiness from sign-off and merge permission |

Use the [Azure scope exception](../SKILL.md#7-run-and-triage-full-ci) exactly:
"small", "safe", "only tests", or unrelated-looking filenames are not new
exceptions. Existing pipeline path selection may intentionally omit jobs;
record the selector's actual decision and prove required scenarios still ran.
Never alter CI or check requirements just to turn a gate green.

Validation-only must assess all rows but cannot acquire full engineering
readiness by marking prohibited work passed. Out-of-mode actions are
`not_required` **for validation-only**, with unmet full-readiness conditions
reported separately. Explicit review-only/no-CI ends with a review verdict,
not "validated". New work without a PR keeps remote gates pending until
authorized publication; it does not mark them not required.

## Continue rather than hand back

1. Select the next incomplete, authorized gate and execute it. A correct
   external PR still activates the trusted PR loop and reaches `azure` and
   `github`; no maintainer edit is needed to justify CI.
2. Before any remote write, check the checkpoint and live state for an existing
   request/build/reply. Persist the action and receipt so a resume cannot repeat
   it. Unknown trigger outcomes require inspection, not another POST.
3. While a build runs, use the existing attached watcher and complete independent
   gates. When only asynchronous work remains, retain `waiting`, the process/run
   IDs and next action; yield to the tool notification without a final completion
   claim. Do not poll with model turns or ask the user to return later.
4. On completion, consume the result, inspect tests/review bodies, and fix
   in-scope findings. Revalidate and repeat within the unchanged budgets.
   Watcher success or `completeness.complete` alone does not close the checklist.
5. Before finalizing, read the live PR again and reconcile every row. Continue
   if any authorized action remains; a human-only gate need not stop other
   independent engineering work.

## Allowed outcomes

- **Engineering-ready:** all applicable engineering gates passed for the
  recorded head/base. List outstanding human approval, CLA, contributor sign-off,
  or merge-policy gates separately; do not claim merge approval.
- **Validated without maintainer changes:** validation-only completed its
  applicable checks, with any full-readiness gaps explicitly listed.
- **Review-only verdict:** the requested read-only scope is complete; CI was
  not authorized and no validation-complete claim is made.
- **Blocked:** a concrete permission, safety, unavailable dependency/reviewer,
  failed evidence gate, exhausted retry/cost budget, or observation deadline
  prevents further authorized progress. Record attempts, evidence, exact next
  action, and who can unblock it. Do not call ordinary pending CI a blocker
  before using its existing observation window.

The [loop limits](loop-control.md#state-and-cost-discipline) still apply. A
watcher deadline is a real unresolved outcome, not authority to reset its clock,
queue a duplicate build, or claim success. High confidence means complete
evidence and explicit residual risk, never a numerical guarantee.
