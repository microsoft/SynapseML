# PR 2737: pr-loop attempt 11, review round 3 (edge cases and robustness)

## Review Summary

| Field | Value |
|---|---|
| Round | 3 of 6 (this round only) |
| Theme | Edge cases and robustness. Non-code adaptation: unusual inputs, boundary conditions, failure modes, missing safeguards |
| Mode | Sequential gauntlet; single reviewer; no nested agents |
| Model | `claude-opus-5.5` (Claude Opus 5.5; slot 3, Anthropic Opus family) |
| Generation method | Manual round-3 instruction from the dispatcher, bound to the frozen manifest. The reviewer read the staged manifest files and derived diffs locally. No toolkit-generated prompt file was used. |
| Artifact | `reviews/pr-2737/task-pr-loop-attempt-11-review-3-claude-opus-5.5.md` |
| Issues found | 1 (Medium: 1) |
| Verdict | **ISSUES_FOUND**: round 3 is not clean |

## Source Identity

- Repository and PR: public `microsoft/SynapseML`, PR 2737, pending corrections.
- Branch: `chore/pr-loop-feedback-20260924`.
- HEAD: `e0b3b12024ef2fffd451629c65b00370906ff719`.
- Target SHA and merge base (review baseline): `681bd96990c421de3b91d2b1bf8f8f470764199d`.
- Reviewed content: merge base to index. The pending corrections are staged.
  `git status --porcelain=v1 --untracked-files=no` shows exactly the three
  staged modifications below and no unstaged tracked changes.
- Frozen manifest SHA-256:
  `64fe2320e97f9bba1dd4a1d16099fecaa478d6ca85c095ced9ecc8f424c374f3`.
  I recomputed it independently and it matched exactly, both before and after
  writing this artifact.
- Manifest bytes (398 bytes, including one final LF):

```json
[{"path":".github/skills/synapseml-pr-loop/SKILL.md","mode":"100644","blob":"bcbb63e6768aaae7bff075ddd63cd1d57f32590d"},{"path":".github/skills/synapseml-pr-loop/references/loop-control.md","mode":"100644","blob":"a036d22c89fafb329e0959449739bbe120b837de"},{"path":".github/skills/synapseml-pr-loop/references/readiness-gates.md","mode":"100644","blob":"3ff834fc6352f36c7aac17833ff50f40efa7ecf1"}]
```

| Path | Merge base | HEAD | Index (reviewed) |
|---|---|---|---|
| `.github/skills/synapseml-pr-loop/SKILL.md` | `23127e4bd4` | `10637476ca` | `bcbb63e676` |
| `.github/skills/synapseml-pr-loop/references/loop-control.md` | absent (added) | `37f0039e97` | `a036d22c89` |
| `.github/skills/synapseml-pr-loop/references/readiness-gates.md` | `aca7b576b3` | `f78de58f9c` | `3ff834fc63` |

- Allocated review outputs are excluded: every path under `reviews/pr-2737/`,
  including `README.md` and the earlier attempt artifacts on the branch. If
  `README.md` is included, the digest changes, so the frozen identity treats the
  whole directory as allocated output. This artifact is also an allocated
  output.

## Evidence Checklist

- [x] Took the round-3 theme and report shape from the installed `/review-code`
  round definitions, adapted for non-code content.
- [x] Verified the frozen identity: branch, HEAD, merge base, and staged-only
  pending changes.
- [x] Recomputed the manifest using the procedure in
  `references/loop-control.md`, "Evidence invalidation":
  - index against the merge base with `-z --no-renames --ignore-submodules=none`
  - stage-0 entries only
  - compact, ASCII-escaped JSON sorted by UTF-8 bytes, with one final LF
  - SHA-256 of those bytes

  It matched exactly before and after writing.
- [x] Read the whole contract at the reviewed index state:
  - `.github/skills/synapseml-pr-loop/SKILL.md`
  - `references/loop-control.md`
  - `references/readiness-gates.md`

  Also read the merge-base-to-index and HEAD-to-index diffs for change context.
  Consulted the `AGENTS.md` placement rule (`reviews/pr-<pr_number>/`) for
  context only.
- [x] Checked the repository's path shapes in the index:
  - 3,889 tracked paths, of which 2,515 contain spaces
  - none contain glob metacharacters, backslashes, non-ASCII bytes, or a
    leading colon
  - no `.gitmodules` file and no gitlinks
- [x] Confirmed the per-worktree checkpoint claim.
  `git rev-parse --path-format=absolute --git-path pr-loop` resolves inside the
  linked worktree's own `.git/worktrees/<name>/` admin directory, not the shared
  common directory.
- [x] Reproduced Issue 1 in disposable Git repositories outside the worktree,
  which were deleted afterwards. Also tested the suggested verification against
  one correct handoff and six defective ones.
- [x] Respected the declared intentional design and did not flag it:
  - a bounded, manual, single-writer workflow rather than an engine
  - optional clarifications and honest finite-budget blocked states
  - the local-gate exception for pre-commit passes on pending fixes, while the
    final pass stays CI-qualified and new-head CI remains required after push
  - the merge base as review baseline, with the target SHA as context and
    provenance
  - no diff dispatch for empty manifests
  - explicit inclusion of ignored gitlinks
  - resolved-fix comments that wait for verified published fixes
  - isolated or serialized install environments
  - leases that pin the old remote SHA
- [x] Did not re-run the driver-reported checks: the eight disposable Git tests
  (manifest, baseline, empty manifest, gitlink, CRLF, canonical order,
  extra/reverted paths, Unicode) and the link/metadata/whitespace checks. As
  described, they cover manifest derivation and the final gates. None of them
  can cover diff-to-manifest fidelity, because the contract defines no such step.

## Issues

### Issue 1: Nothing verifies that the diff given to reviewers matches the frozen manifest

**Files:**

- `.github/skills/synapseml-pr-loop/references/loop-control.md:109-112`, `:124-125`, `:129-136`
- `.github/skills/synapseml-pr-loop/SKILL.md:222-225`, `:234-235`, `:242-244`, `:249-250`
- `.github/skills/synapseml-pr-loop/references/readiness-gates.md:74-76`, `:81-82`

**Severity:** Medium

**Problem:** The manifest is derived NUL-safely. The review diff comes from a
second, separately typed command, `git diff ... <merge-base-sha> -- <manifest-paths>`,
and no step checks its output:

- "check the path set before prompts" validates the manifest, not the diff.
- The post-commit gates (the HEAD manifest recompute and changed-path-set
  equality) never read the diff or the prompts.
- Git exits 0 with no output for pathspecs that match nothing.
- Shells can alter the bytes after Git writes them.

An empty, partial, or altered diff can therefore pass through all six rounds
unchallenged. Readiness would then certify "all six rounds clean on the same
final patch" and a matching manifest for content that no reviewer saw.

The "never silently trim it" rule has no detector. The gap applies equally to
generator dispatch (`-DiffFile`), manual prompt assembly ("attach the same
explicit diff"), and approved splits "under one recorded manifest". The only
guarded boundary is the empty manifest; a non-empty manifest that yields an
empty diff is not guarded.

**Trace through the contract:**

1. `loop-control.md:109-112` derives the path set with
   `git diff --cached --name-status -z --no-renames ...` and says to "check the
   path set before prompts".
2. `loop-control.md:113-123` builds the manifest from index entries and records
   its SHA-256. An empty manifest never dispatches.
3. `loop-control.md:124-125` builds the review diff from `-- <manifest-paths>`
   and gives no:
   - argv or quoting rule
   - repository-root or literal-pathspec requirement
   - byte-preserving output rule
   - coverage check
4. `SKILL.md:222-225`, `:234-235` and `:242-244` state the binding but never
   check it: "Use the same manifest for that diff and the fingerprint", supply
   it to `-DiffFile` or attach it to manual prompts, and keep a split under one
   manifest.
5. `loop-control.md:129-136` and `readiness-gates.md:81-82` compare HEAD with the
   manifest only.
6. `readiness-gates.md:74-76` and `SKILL.md:249-250` certify six clean rounds on
   "the same final patch".

**Evidence:** I used disposable repositories with Git 2.55.0 for Windows. They
staged changes to `core.txt` and `docs/Get Started/Install SynapseML.md`, a path
shape shared by 2,515 of this repository's 3,889 tracked paths. Rows E and F
used a second repository containing only a CRLF-to-LF change.

| # | How `<manifest-paths>` or the diff bytes were handed off | `git diff` exit | What reviewers would receive |
|---|---|---|---|
| A | Each path as its own argv entry, run from the repository root | 0 | Both files (correct) |
| B | Paths joined into one space-separated argument | 0 | Empty diff |
| C | Correct root-relative paths, run from `docs/` (pathspecs are relative to the working directory) | 0 | Empty diff |
| D | Paths word-split on spaces, as an unquoted `$(...)` expansion does | 0 | `core.txt` only; the doc change is silently missing |
| E | A CRLF-to-LF-only change captured through a PowerShell 7 pipeline, then written out | 0 | Both CR bytes lost. The hunk reads `-a -b +a +b`, which looks like a no-op. Git's own `--output` file kept both bytes. |
| F | Windows PowerShell `>` redirection | 0 | UTF-16LE file (BOM `FF FE`) of 386 bytes, instead of Git's 185 bytes |

The final gates do not read the diff. After scenarios A to D, committing the
staged change passed the HEAD manifest recompute and full changed-path-set
equality with a clean worktree, the same result as the correct handoff.

**Suggested fix (contract text; not implemented):**

1. Generate the review diff from the repository root, with one argv entry per
   manifest path, as literal top-level pathspecs (`git -C <root>
   --literal-pathspecs diff ...` or `:(top,literal)<path>`). Add
   `--full-index --no-renames` and write the bytes with `git diff
   --output=<file>`, not shell capture or redirection.
2. Prove fidelity before any dispatch: generator, manual prompts, or every
   approved split part applied together.
   - Seed a temporary index from the merge base:
     `GIT_INDEX_FILE=<tmp> git read-tree <merge-base>`.
   - Run `git apply --cached --binary <diff-file>`.
   - On that temporary index, require
     `git diff --cached --raw --no-abbrev -z --no-renames --ignore-submodules=none <merge-base>`
     to equal the frozen manifest exactly (path, mode, object ID; deletions
     absent).

   Any apply failure or set mismatch blocks dispatch.
3. Record the verified diff file's SHA-256, and each split part's, with the
   manifest in the checkpoint and in every round artifact. Readiness then
   requires all six rounds to cite the same verified digest.

I tested step 2 in disposable repositories:

| Diff | Result |
|---|---|
| Correct | Accepted |
| Empty (C) | Rejected: `git apply` exit 128 |
| CR-stripped (E) | Rejected: exit 1 |
| UTF-16 (F) | Rejected: exit 128 |
| Partial (D) | Rejected: applied, but the sets did not match |
| Over-inclusive: a pathspec that also pulled in a prior `reviews/` verdict | Rejected: applied, with an extra path |

The last row shows that the same check also enforces "do not feed earlier review
verdicts to later reviewers" against accidental over-inclusion.

A cheaper partial check reruns the identical argv with
`--raw --no-abbrev -z --no-renames` and compares the entry sets. It detected B
and C in testing, but it cannot see damage introduced at capture time (E, F).

**Why Medium and not High:** CI, the fast review, the remote automated review
and human review still cover the pushed head. The failure also needs a mistake
in handing off the paths or diff, not a Git defect. However, the contract
treats the six-round pass as evidence ("never silently trim it"; "all six rounds
clean on the same final frozen patch"). Every trigger above exits 0 and leaves
every current gate green, so that certificate can be false without any signal.

## Considered, Not Reported

- **Glob or magic-pathspec over-matching of `<manifest-paths>`:** 0 of 3,889
  tracked paths contain glob metacharacters or a leading colon, so this cannot
  happen today. Fix step 1 would make it impossible.
- **JSON escaping or canonicalization variance:** any variance changes the
  digest, so it fails closed. No tracked path is non-ASCII.
- **A rebase that reverts content while the after-state manifest still
  matches:** guarded by the explicit before/after content comparison, context
  reassessment, and the target-advance restart rules.
- **Checkpoint loss when a linked worktree is removed or pruned:** the state
  path is per-worktree as claimed, and loss falls under missing-state recovery.
- **A crash between writing a draft and updating the checkpoint:** noncolliding
  attempt names and recorded evidence identities keep stale state detectable,
  and lost drafts are rerun as a new attempt.
- **Unmerged index entries or unstaged edits during manifest derivation:** the
  stage-0 and clean-worktree requirements fail closed.
- **Rename rendering in the review diff:** no content is hidden. It appears in
  the fix only so that diff entries map 1:1 to manifest entries.
- **Allocated outputs that are not yet published, under strict path-set
  equality:** fails closed.
- **Wording ambiguities** (budget counting, rebutted rounds, how the summary
  orders steps) and **watcher deadline expiry:** these are clarifications or
  resumable blocked states, which the dispatcher declared intentional. They are
  not robustness defects.

## Resolution Log

| # | Finding | Severity | Status | Resolution |
|---|---|---|---|---|
| 1 | The diff given to reviewers is not verified against the frozen manifest | Medium | Open | Waiting for driver triage: either a fix followed by a new complete six-round pass, or an evidence-based rebuttal |

## Limitations

- I ran round 3 only. There were no other rounds, no nested agents and no model
  dispatch.
- I had no remote access. I did not check CI, PR 2737 threads and comments, the
  remote automated review, or target movement after `681bd96990c4`. New-head CI
  is still required.
- As instructed, I did not read earlier review artifacts,
  `reviews/pr-2737/README.md`, or the loop checkpoint, so I did not deduplicate
  against earlier findings.
- I did not run the installed prompt generator and do not rely on its internal
  behavior. Issue 1 concerns the contract's missing check, whatever the
  generator does.
- The disposable tests ran on Windows with Git 2.55.0 for Windows, PowerShell
  7.6 and Windows PowerShell. I reproduced word-splitting by passing split argv
  entries, not by running bash.
- This was a non-code review, so there was no product build or test run; the
  manifest contains no product code. The driver-reported tests and the
  link/metadata/whitespace checks were not re-run.
- I reviewed only the staged index content bound by the manifest. I used
  `AGENTS.md` for artifact-placement context only.
