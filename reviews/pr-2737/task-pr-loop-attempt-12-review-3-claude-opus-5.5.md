# PR 2737 attempt 12 - review 3 (robustness, non-code)

- **Verdict:** CLEAN. I found no high-confidence robustness defects.
- **Model:** Claude Opus 5.5 (`claude-opus-5.5`)
- **Theme:** Round 3 of the six-round `/review-code` gauntlet: robustness (non-code). I judged the change as a manual, single-writer natural-language workflow, not as an engine.
- **Date:** 2026-09-24
- **Independence:** I did not open or search prior review artifacts, checkpoints, or other reviewer prompts. The driver ran collision checks and assigned this output path. The path did not exist before this file was written.

## Reviewed identity

| Item | Value |
| --- | --- |
| Branch | `chore/pr-loop-feedback-20260924`: PR 2737 head `e0b3b12024ef2fffd451629c65b00370906ff719` plus pending staged corrections |
| Target / merge base | `681bd96990c421de3b91d2b1bf8f8f470764199d` (the merge base equals the target SHA) |
| Frozen manifest SHA-256 | `1b6aa6ad0b95822a677321b0e60d151f9c786fa39591b04bc3b06bcbffbfd1a6` (398 bytes, 3 entries) |
| Review diff SHA-256 | `47478a273c9b2f31b1695c94307b884fe808076acb776d2574587da41aa63a08` (3 file sections, 426 lines) |

| Path | Mode | Blob |
| --- | --- | --- |
| `.github/skills/synapseml-pr-loop/SKILL.md` | `100644` | `979200e29afcb3d82e4ae9e219a5efe36f629527` |
| `.github/skills/synapseml-pr-loop/references/loop-control.md` | `100644` | `c8a53ef0b6ef3701786c5a0717874361fe130505` (added) |
| `.github/skills/synapseml-pr-loop/references/readiness-gates.md` | `100644` | `3ff834fc6352f36c7aac17833ff50f40efa7ecf1` |

## Verification evidence

1. The SHA-256 of the supplied diff matched the value above.
2. Disposable-index round trip. I initialized a disposable index from the merge base with `git read-tree`, using a temporary `GIT_INDEX_FILE`, and applied the diff with `git apply --cached --binary`. The result contained exactly the three entries above. I serialized it as `loop-control.md` requires: compact JSON, keys `path`/`mode`/`blob`, ASCII escaping, UTF-8 byte order, and one final LF. It hashed to the frozen value. The real index was not modified.
3. Real-index check. The real index, excluding `reviews/` paths, gave the same manifest hash. I ran the prescribed diff command (`--literal-pathspecs`, `--binary --no-ext-diff --no-textconv --ignore-submodules=none --submodule=short`, `--output`) against both indexes. Both runs reproduced the supplied diff byte for byte.
4. The scoped paths had no unstaged changes before or after the review, so the documents I read are the frozen blobs.
5. Scratch-repository checks:
   - `git rev-parse --path-format=absolute --git-path pr-loop` resolves to `.git/pr-loop` in the main worktree and to `.git/worktrees/<id>/pr-loop` in linked worktrees. The checkpoint directory is per-worktree, as the document states.
   - `diff.noprefix=true` makes `git apply` reject the diff.
   - `apply.whitespace=fix` makes the round-trip manifest mismatch.
   - Forcing `color.ui=always` does not change the `--output` diff.
   - None of these configurations can turn a mismatch into a false pass.

## Robustness traces

I traced the whole contract through the paths below. Each one either completes or stops `blocked` by design:

- **Budgets and states.** The cap of 6 mandatory pre-commit passes equals one initial pass plus the 5 fix/integration cycles. Final CI-qualified passes are capped at 2, and a pass counts from its first reviewer. Artifact-only commits, triage-only replies, rebuttals, and duplicate bot notes spend no cycle. Repeated target movement on unrelated files stops at the cycle cap.
- **New-PR bootstrap.** The order is: reviewed commit, then the authorized PR, then drafts moved into `reviews/pr-<number>/` and recorded as allocated outputs, then the handoff commit. At each commit, the manifest recomputed from HEAD and the full `<merge-base>..HEAD` path-set equality hold. An unsafe draft stays private, gets a public exclusion note, and its round is rerun on the same frozen patch.
- **Existing-PR fix.** Pre-commit artifacts are committed with the fix. The final pass publishes its artifacts in an artifact-only commit. That head still needs new-head CI and a current-head automated review. A new substantive finding returns the loop to `fast` and consumes a cycle.
- **Crash and resume.**
  - Checkpoints are written atomically.
  - A corrupt checkpoint is preserved and never read as zero usage.
  - Unknown usage blocks automatic budget renewal.
  - Lost drafts are rerun, never recreated.
  - A restarted watcher keeps the deadline set by the run's kickoff.
  - In-flight builds and replies are reused only after their identity is confirmed.
- **Concurrency and ownership.**
  - Force pushes use an explicit lease on the verified old remote SHA, which is not refreshed by later fetches.
  - A moved remote head, a rejected lease, or another owner's push blocks the loop.
  - Shared install, publish-local, and JAR resources are either isolated or held exclusively.
- **Git edge cases.** The canonical manifest plus the disposable-index round trip covers deletions, gitlinks, reverted paths, the empty-manifest guard, and Unicode or space-containing paths. Literal pathspecs also match directory prefixes. Any extra path this could pull in fails the complete changed-path comparison.
- **False-pass search.** I found no route by which unreviewed content, a stale head or review, or missing CI reaches `engineering-ready`:
  - An extra committed path fails the full path-set equality check.
  - A commit altered by hooks fails the manifest-from-HEAD check.
  - A head behind the target fails the integration gate.
  - An absent build or review remains a blocker.

## Findings

None.

## Limitations

- This is a static review of a natural-language workflow. I made no remote calls and did not run against GitHub or Azure Pipelines.
- The driver reported that nine Git protocol tests and static validation passed. I did not rerun them beyond the checks listed above.
- I read only the three scoped documents, the verified diff, and the pull-request/review-artifact section of `AGENTS.md`.
  - I did not open linked targets outside that scope: `references/ci-triage.md`, `references/writing-prs.md`, `references/spark-performance.md`, sibling skills, and helper scripts.
  - I did not open the installed copilot-toolkit's prompt generator, `REVIEW-PROMPTS.md`, or pre-commit policy.
  - Link, anchor, and flag validity rests on the reported static validation. That includes inbound links to the heading renumbered from section 8 to "9. Final readiness loop".
- Relative to the merge base, the index also contains paths under `reviews/pr-2737/`, including `reviews/pr-2737/README.md`. These are committed and untracked review evidence outside the frozen manifest. I did not open them. Whether the checkpoint records each one as an allocated review output is a fact this review could not see.
- I did not fetch the primary-source URLs.
