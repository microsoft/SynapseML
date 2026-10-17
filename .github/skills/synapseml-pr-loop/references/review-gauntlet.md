# Six-round review gauntlet

For the final CI-qualified pass, after the fast loop's tests, current-head
reviews, and required CI are green,
invoke the installed copilot-toolkit `/review-code`. Use its direct contract,
all six themes, runtime-resolved model families, and sequential mode unless
parallel mode was requested. Do not hardcode model IDs or substitute the
repository's single-review checklist.

Mandatory pre-commit passes instead require passing local gates for the pending
patch. This applies both to the first commit and fixes on an existing PR:
failed or stale CI on the old head cannot block reviewing its locally validated
fix. Defer the pending commit's remote evidence until it is pushed, then return
to the fast loop for current-head CI and comments before the final gauntlet.

Generate an explicit full merge-base-to-index diff including committed and pending changes and
excluding the checkpoint's exact allocated review-output paths. Use the same
manifest for that diff and the fingerprint; do not feed earlier review verdicts
to later reviewers. Supply it to the toolkit's `-DiffFile` / `--diff-file`.
Its default uncommitted diff can omit the entire published PR.
Follow `AGENTS.md` for artifact placement: pass
`reviews/pr-<pr_number>/` explicitly when the number exists. Before publication,
write drafts to the session workspace, never a placeholder repository folder.
Allocate noncolliding attempt filenames and preserve every clean or failed
artifact with actual model, theme, revision, and resolution evidence.
The installed direct prompt generator rejects output directories outside the
repository. Before a PR number exists, or when no verified Task ID exists,
assemble the six round prompts directly
from its installed `REVIEW-PROMPTS.md` and attach the same explicit diff. Set
session output paths only before the PR exists; with a PR, set
`reviews/pr-<number>/` even when there is no Task. Record this generation method;
do not change the themes, model selection, or pass criteria. Use a checkpointed
descriptive task token when no Task exists; the generator can silently mistake
branch-name digits for a Task ID. With a PR and verified Task ID, pass both
explicitly to the generator and check its output names before dispatch.
Store prompts outside the worktree and enforce the generator's byte budget
for manual prompts too. An oversized prompt stays blocked unless the user
approves a complete split under one recorded manifest; never silently trim it.

The driver owns collision checks and supplies each reviewer one unused output
path. Every reviewer prompt must explicitly forbid opening or searching prior
review artifacts, checkpoints, or other reviewer prompts through tools as
well as supplied context. Exclude those locations from broad searches.
If the assigned output already exists, stop rather than read or overwrite it.

Triage findings before advancing and preserve evidence-based rebuttals. Any fix
to reviewed content ends and consumes the current pass: return to the fast loop,
rerun affected tests, regenerate the prompt, and start a complete new pass.
Require all six rounds clean on the same final frozen patch, not
six clean results accumulated across different patches. Follow
[the invalidation rules](loop-control.md#evidence-invalidation).
Unavailable required reviewers or exhausted budgets are blockers.

When a commit changes reviewed content, include its review artifacts once a
PR number exists. The final CI-qualified pass can publish its artifacts in a
separate artifact-only commit, relying on that completed pass.
For a new PR, the repository's explicit session-draft rule takes precedence
over the toolkit's same-commit bundling default: complete the required six
pre-commit rounds, retain their drafts, commit/push the reviewed change, and
create the authorized PR. Then move publication-safe drafts without rewriting
their original feedback into `reviews/pr-<number>/` and commit them before final
readiness. Follow the unsafe-draft procedure in loop control. Record this
bootstrap handoff. The
[artifact-only rule](loop-control.md#evidence-invalidation) avoids recursively reviewing
review text but does not waive final-SHA CI or remote review.

Honor installed six-round pre-commit policy for changes to reviewed content.
The bootstrap pass does not replace the final CI-qualified gauntlet. Do not
weaken pre-commit review to achieve the preferred cheap-first ordering.
