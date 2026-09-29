## Review Summary
- **Round**: 3
- **Theme**: Edge Cases & Robustness (non-code adaptation: unusual workflow inputs, missing guards, failure modes, concurrency)
- **Mode**: sequential (manual, round 3 only)
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2737/task-pr-loop-attempt-9-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Reviewed Identity
- Branch `chore/pr-loop-feedback-20260924`, HEAD `e0b3b12024ef2fffd451629c65b00370906ff719`, plus staged edits to the three files below. No unstaged edits.
- Target and merge base are the same commit: `681bd96990c421de3b91d2b1bf8f8f470764199d`.
- Normalized manifest SHA-256 `a8dfe5771757e7e9283dda890ac5fc76bc772c50a86e1dbba0528a4bac3b7dee`. Recomputed independently as `references/loop-control.md` specifies: merge-base-to-index diff, review outputs excluded, compact ASCII JSON, UTF-8 byte order, one final LF. It matches the supplied value.

| Path | Mode | Blob |
| --- | --- | --- |
| `.github/skills/synapseml-pr-loop/SKILL.md` | 100644 | `09e7dbb016525017dfa09631075967c815e62386` |
| `.github/skills/synapseml-pr-loop/references/loop-control.md` | 100644 | `18acef3dcf6da54658c6e16e41d6e9424ba4b979` |
| `.github/skills/synapseml-pr-loop/references/readiness-gates.md` | 100644 | `3ff834fc6352f36c7aac17833ff50f40efa7ecf1` |

- The prescribed review diff, run from the worktree root, has 3 file headers and 23,559 bytes. Worktree hashes equal the index blobs. This review changed no source file.

## Evidence Checklist
- [x] Manifest recomputed and matching. All three blobs are LF-only ASCII. `git ls-files --eol` reports `i/lf w/crlf` for `references/loop-control.md`, yet Git treats the file as clean and hashes it to the indexed blob. Checkout line endings therefore do not change the manifest.
- [x] Read the complete in-scope contract: all three files, the staged edits against HEAD, and the full merge-base-to-index diff. Read `AGENTS.md` for artifact placement and secret handling.
- [x] Skill frontmatter parses as YAML, and `name` matches the directory. All 16 relative links resolve, and the `references/ci-triage.md#waiting-for-azure-pipelines` anchor exists.
- [x] Checked the referenced helpers against the docs:
  - `scripts/Get-PrReadiness.ps1`: `-PullRequest`, a read-only `-WaitForReview` with a finite timeout, and `-RunPipeline`, which posts once per invocation.
  - `scripts/watch_azure_pipeline.py`: 10-minute polling and a 120-minute deadline counted from kickoff.
  - The installed `/review-code` prompt generator:
    - Rejects output directories outside the repository.
    - Can infer a Task ID from branch digits.
    - Fails loudly above its 1 MiB prompt budget.
    - Summarizes per-file only in its default untracked-file diff, never in an explicit `-DiffFile`.
- [x] Concurrency probe with a disposable local bare remote and clone (Git 2.55.0). Setup: the target advances, and another owner pushes H2 to the PR branch while the loop holds a fix rebased onto the target. The loop then pushes:
  - c1: `git fetch origin` then a bare `--force-with-lease`. Exit 0 with "forced update"; H2 is lost.
  - c2: `git fetch origin master` then a bare `--force-with-lease`. Rejected as stale; H2 is kept.
  - c3: broad fetch then `--force-with-lease=<branch>:<recorded H1>`. Rejected; H2 is kept.
  - c4: broad fetch then `--force-with-lease --force-if-includes`. Rejected; H2 is kept.
- [x] `git rev-parse --path-format=absolute --git-path pr-loop` resolves per worktree (`.git/pr-loop` for the main worktree, `.git/worktrees/<name>/pr-loop` for a linked one), so checkpoints cannot collide.
- [x] Path-set probes:
  - Default rename detection emits a 3-field `R100 old new` record. The post-commit `--no-renames` check emits `A new` and `D old`, so a manifest that omits the old path fails equality. This fails closed.
  - The prescribed diff run from a subdirectory returns 0 bytes with exit 0, because pathspecs are relative to the current directory. See "Examined, Not Reported".
- [ ] Remote PR, CI, and bot state were not checked, because this round had no remote access.

## Issues

### Issue 1: The staged ownership stop cannot fire at push time
A bare `--force-with-lease` after a broad fetch silently overwrites another owner's push.
- **Severity**: Medium
- **File**: `.github/skills/synapseml-pr-loop/references/loop-control.md`, `.github/skills/synapseml-pr-loop/SKILL.md`
- **Line(s)**: loop-control.md 52-53 (staged); SKILL.md 80 and 84. Related: loop-control.md 31-32 and readiness-gates.md 16.
- **Description**:
  - The staged state-machine rule says that when another owner pushes a reviewed-content change, the loop stops `blocked` until ownership is clear.
  - The only push-time guard is step 2 of SKILL.md: "Use `--force-with-lease`" with no expected value, after "Fetch again immediately before the final push."
  - A bare lease compares the remote ref with the local remote-tracking ref. Any fetch that updates `origin/<pr-branch>` therefore approves overwriting what it fetched: `git fetch origin`, `git pull`, or editor autofetch. The "general note on safety" in Git's `git-push` documentation says this protection "is trivially defeated" by background fetches.
  - Other-writer checks exist only on resume (loop-control.md 31-32).
  - The readiness gates then require matching local, remote, and GitHub head SHAs (readiness-gates.md 16) and a matching manifest. The overwritten branch satisfies both.
- **Failure trace**:
  1. Another owner pushes H2 to the PR branch during a long mandatory pre-commit gauntlet.
  2. Following step 2, the loop runs `git fetch origin` before the final push, which moves `origin/<pr-branch>` to H2.
  3. The loop rebases the reviewed fix onto the target and runs `git push --force-with-lease`.
  4. The push succeeds as a forced update and drops H2 (probe c1).

  No rule ever observes H2, so the `blocked` transition never fires, and the head-SHA and manifest gates pass.
- **Risk**: A collaborator's or parallel session's commits vanish silently from a shared PR branch, and the loop then reports engineering readiness. This contradicts the staged ownership rule and the resume-time other-writer checks.
- **Suggested Fix**: Docs only, no new engine:
  1. Pin the lease to the head SHA recorded at the last ownership check: `git push --force-with-lease=refs/heads/<branch>:<recorded-head-sha> origin HEAD:<branch>`. Optionally add `--force-if-includes`.
  2. Before pushing, compare the live remote head, from `git ls-remote` or the PR API, with that recorded SHA.
  3. Treat a mismatch or a lease rejection as the `blocked` ownership stop, never as a reason to refetch and retry.

  Probes c3 and c4 show that both pinned forms reject the overwrite.

## Examined, Not Reported
- **Review diff generated from a subdirectory**: the diff is empty, and the generator accepts an empty explicit diff file. Not reported: "check the path set before prompts" covers this, and an empty diff is conspicuous. Optional hardening: run from `git rev-parse --show-toplevel` and compare the diff headers with the manifest.
- **Rename records and allocated-but-unwritten review outputs**: both hit the exact changed-path equality check and fail closed.
- **Serializer escaping differences across sessions** (optional escapes, hex case): a spurious mismatch that fails closed.
- **Empty manifest after a full revert**: contrived, since nothing remains to review.
- **Target churn, the 5/2/6 caps, and the single infrastructure retry**: intentional finite-budget blocked states.
- **The "gates green, then gauntlet" summary versus mandatory pre-commit passes**: staged step 8 and the state machine state the exception explicitly.
- **Losing the checkpoint on worktree removal**: covered by missing-checkpoint reconstruction.

## Limitations
- This is a non-code adaptation of round 3 only. No other round, no other reviewer's output, and no earlier artifact was read.
- No checkpoint was provided, so I could not confirm that the `reviews/pr-2737/` files committed at HEAD are recorded as allocated outputs. The matching hash only shows that the manifest excludes them.
- No remote access: PR threads, bots, and CI were not checked. The staged edits still need new-head CI and comment checks after push.
- The six session Git tests cited in the request were not rerun. This round independently reproduced only the manifest hash, line-ending normalization, rename record shape, cwd-relative pathspecs, and lease behavior.
- This artifact replaces round-3 evidence that was missing because an earlier round-3 transport returned no output. It is not a re-review of another reviewer's findings.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Driver resolution

The skill now records the verified old remote SHA before rewriting a published
branch and uses `--force-with-lease=<remote-ref>:<verified-old-remote-SHA>`.
A subsequent fetch cannot silently update that expected value. A changed
remote head or lease rejection blocks publication until ownership is resolved.
This uses the pinned-lease form verified by probe c3 above.
