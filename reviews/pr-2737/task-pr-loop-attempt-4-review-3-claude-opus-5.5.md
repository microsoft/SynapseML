# Round 3 review - SynapseML attempt 4

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness (non-code adaptation: unusual inputs, missing prerequisites, failure modes)
- **Mode**: sequential; installed `/review-code` direct contract
- **Model**: claude-opus-5.5
- **Artifact**: `task-pr-loop-attempt-4-review-3-claude-opus-5.5.md` (pre-PR session draft)
- **Issues Found**: 1 (Low)
- **Verdict**: ISSUES_FOUND for this round and snapshot only. This does not claim the gauntlet or live CI is complete.

## Source identity
Repository `microsoft/SynapseML`, branch `chore/pr-loop-feedback-20260924`, target `upstream/master`. HEAD, the
target and the merge base are all `681bd96990c421de3b91d2b1bf8f8f470764199d`. These are local refs; no fetch was
performed. The reviewed source is that HEAD plus uncommitted changes: two tracked modifications and one untracked
addition. The index is unchanged.

| Scoped file | Normalized blob |
| --- | --- |
| `.github/skills/synapseml-pr-loop/SKILL.md` | `f3bcf4579c5b8a5d8454c0500db77b64b010a69d` |
| `.github/skills/synapseml-pr-loop/references/loop-control.md` | `37f0039e97b698f3ebb00c1f5f774bd0c94ecb33` |
| `.github/skills/synapseml-pr-loop/references/readiness-gates.md` | `f78de58f9ce6ace39d59e682a74b9d77a31fb069` |
| `AGENTS.md` (unchanged context) | `fe86f9165b1f3160ad634fc2d4b8eb84fe79e649` |

## Evidence Checklist
- [x] Identity re-verified just before writing (`git rev-parse`, `git merge-base`, `git status --porcelain
  --untracked-files=all`, `git hash-object`); the pending set is exactly the three skill files.
- [x] Mechanics: 46 relative links and anchors resolve, 0 broken; no trailing whitespace or tabs; final LF present.
  `.gitattributes` (`* text=auto eol=lf`) normalizes the CRLF working copy of `loop-control.md` to LF. The
  "steps 3-7" and "step 8" references match the renumbered headings (8 gauntlet, 9 final readiness).
- [x] Installed generator checked in `review-code/scripts/Run-ReviewRound.ps1` and `run-review-round.sh`:
  both accept `-DiffFile` / `--diff-file`. The direct contract rejects out-of-repo, ignored, `.git`, and linked
  output directories, so pre-PR drafts need manual prompt assembly, as SKILL §8 says. Prompts over the 1 MiB
  default budget exit 3. Task IDs are numeric-only and silently derived from branch digits, which justifies the
  descriptive-token rule.
- [x] Checkpoint `git rev-parse --path-format=absolute --git-path pr-loop` resolves per worktree, outside tracked
  content. LC L10-39 specifies an atomic temp-file rename, corrupt-file preservation, cold-start rebuild, and
  rerunning (never recreating) lost drafts.
- [x] Budget walk: bootstrap and per-fix pre-commit passes fit the separate cap of 6, with 2 final CI-qualified
  passes. Aborted passes count at first reviewer start, and nothing resets per phase. Duplicates and rebuttals
  spend no cycle (LC L51-53, L155).
- [x] Waits: `ci-triage.md` keeps one 10-minute watcher with a two-hour kickoff deadline that restarts cannot
  extend. Review waits have a finite timeout (`Get-PrReadiness.ps1 -TimeoutMinutes`, default 20).
  readiness-gates L82-84 keeps `/azp run` and `-RunPipeline` behind explicit authorization and external checks.
- [x] Unsafe-content path: drafts are public-safe from the start. An unsafe draft stays private with an exclusion
  note, and its round reruns on the same frozen patch. Moved paths become allocated outputs; `--name-status
  --no-renames` path-set equality, a clean worktree, and a recomputed manifest gate readiness.
- [x] All 12 prior round-3 findings (attempts 2-3) are fixed in the current text: pass counting and reopen edges,
  pre-PR manual assembly, the unsafe-draft and appended-SHA handoff, and the per-worktree checkpoint. Also fixed:
  the shared diff/fingerprint manifest, canonical recompute, substantive-only reopen, the descriptive token, and
  path-set equality with moved-path allocation. The rest: the manual-prompt byte budget, atomic writes, and
  artifact-only commit scope. Earlier verdicts were not used as evidence. Issue 1 is a residue of the token fix.
- [ ] Not exercised: live GitHub, Azure Pipelines, and bot behavior. No remote, API, CI, commit, or push actions.

## Issues

### Issue 1: Post-PR dispatches without a Task ID are routed to the session output path
- **Severity**: Low
- **File**: `.github/skills/synapseml-pr-loop/SKILL.md`
- **Line(s)**: 213-217 (versus 208-210; `AGENTS.md` 142-147; `references/loop-control.md` 59-64, 135-139)
- **Description**: L214-217 applies manual assembly "Before a PR number exists, or when no verified Task ID exists"
  and says to "set the session output path in each dispatch". A GitHub PR here normally has a number but no Task,
  so the literal text sends every post-PR round, including the final CI-qualified pass, to session storage. That
  contradicts L208-209 ("pass `reviews/pr-<pr_number>/` explicitly when the number exists"), `AGENTS.md` L142
  ("Write review artifacts directly to `reviews/pr-<pr_number>/`"), and LC L135-136 (session storage only "before
  the PR exists"). The only session-to-repository handoff, LC L59-64, covers new-PR bootstrap drafts.
- **Risk**: On the dominant path, a literal reader leaves final-pass artifacts unversioned. It cannot satisfy
  readiness-gates L74-75 ("versioned review artifacts") without improvising a move. `AGENTS.md` precedence
  resolves the conflict, so there is no unsafe outcome.
- **Suggested Fix**: "set each dispatch's explicit output path: the session workspace before the PR number
  exists, `reviews/pr-<pr_number>/` afterward." Keep manual assembly for both the no-PR and the no-Task cases.

## Resolution Log

Driver resolution: manual prompt construction no longer implies session-only
output when a PR already exists. The skill explicitly selects session drafts
only before publication, and `reviews/pr-<number>/` afterward even without a
Task ID. The correction is included in the next frozen public review series.

_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Limitations
- This is a Markdown-only, agent-run workflow. Missing implementation code is intentional and not a defect.
- Findings apply only to the blobs above. Any change to them invalidates this round.
