# Branch reference template

Keep each branch file concise and use these headings:

1. **Purpose and baseline sources** — what targets the branch and which live
   files define versions.
2. **Sync policy** — merge/rebase direction and conflict rules.
3. **Differences from master** — only deliberate runtime or tooling deltas.
4. **Sibling-port rules** — what should and must not move between branches.
5. **Runtime and CI** — supported environments, intentional skips, triggering,
   capacity, and required real-environment validation.
6. **Known failures** — evidence-backed current exceptions, with a reminder to
   revalidate rather than normalize them forever.
7. **Before merge** — content comparison, target refresh, tests, and sibling
   branch diff.

Link shared material instead of copying it. Treat versions and known failures
as snapshots; verify them against the live target branch.
