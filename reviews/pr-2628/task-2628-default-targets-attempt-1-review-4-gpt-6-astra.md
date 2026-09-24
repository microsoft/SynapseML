# Default release targets: round 4 detailed correctness

Reviewer: GPT-6 Astra (`gpt-6-astra`).

Reviewed HEAD: `2391aa166ab46238307de9edf9650102cbff950e`.
Master base: `681bd96990c421de3b91d2b1bf8f8f470764199d`.

Scope: the current unstaged default-target delta and untracked
`scripts/release/test_release_defaults.py`. Detailed review focused on changes
since R1: bump counting and error handling, optional-reference preservation,
the opt-in preparation guide, workflow recovery and their regressions.
Source/approval bindings were rechecked. The committed branch-to-master delta,
review bookkeeping and unrelated architecture were not re-audited.

## Result

No concrete findings. Both R3 resolutions are independently confirmed within
the checks below.

**DT-R3-1 closed.** In `scripts/bump-version.py:776-813`, the destination-version
baseline comes from the original content of exactly the files being rewritten.
The expected count is that baseline plus the replacement count. Post-write
analysis uses the same optional-reference exclusions as the initial scan at
lines 303-316, rather than treating retained Spark 4.0 examples as stale.

The original pre-updated opt-in shape now succeeds for seven source files:
the actual `1.1.3` to `1.2.0` CLI bump preserved 24 already-current references
and exited zero. Its dry run changed no bytes. Four injected failures covering
stale selected references, an extra new-version token, a lost preexisting token
and a write error all returned failure. Recovery messages preserved the
instruction to retain prior edits and did not recommend discarding the tree.

`scripts/release/README.md:116-145` now separates the bump from optional edits,
names all five source guides plus metadata, and orders conversion, snapshot
creation and finalization before candidate approval. The publication lock
still distinguishes nondeploying preview from production.

**DT-R3-2 closed.** The guide at `scripts/release/README.md:103-114` and workflow
header at `.github/workflows/release-tag.yml:3-8` accurately describe preservation
of existing PRs and require explicit inclusion for Spark 4.0 repair. The actual
Git/Bash regression preserved the reviewed Spark 4.1 branch, left a missing
Spark 4.0 tag untouched on a default run, then repaired it at the recorded
commit on an explicit run without prematurely tagging Spark 4.1.

## Independent verification

- 14 native Python cases passed, including successive retained/pre-updated
  optional-runtime bumps, postconditions and saved-plan identity.
- 5 POSIX workflow cases passed using disposable local Git repositories,
  covering both summaries, default orchestration and opt-in recovery.
- 9 Node installation/publication-lock cases passed.
- Binding probes accepted both valid selections and rejected an unselected
  Spark 4.0 source, an old approval for an expanded plan, a wrong source commit,
  and an edited saved selection without a new valid identity.

## Limits

No full suites, SBT documentation conversion, full website build, live service
APIs or production operations were run. Git mutations occurred only in
disposable test repositories. The authoritative worktree received only this
report, with no implementation edits, commits or pushes.

These are local correctness results, not production proof or release
authorization. This report covers R4 only, not the complete six-round review.
