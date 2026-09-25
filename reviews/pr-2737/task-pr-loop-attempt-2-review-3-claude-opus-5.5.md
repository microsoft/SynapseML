# Round 3 review: edge cases and robustness

## Review Summary
- **Round**: 3 of 6, run alone as requested, using the installed copilot-toolkit `review-code` direct contract, attempt 2
- **Theme**: Edge Cases & Robustness, adapted to workflow docs: missing inputs, failures, recovery and concurrency
- **Mode**: sequential
- **Model**: `claude-opus-5.5`, the actual model. I reviewed directly with no subagents, workflows or substitute models.
- **Artifact**: session draft `task-pr-loop-attempt-2-review-3-claude-opus-5.5.md`. No PR number exists yet, so it belongs in `reviews/pr-<number>/` after publication.
- **Issues Found**: 6, all Medium
- **Verdict**: ISSUES_FOUND
- **Gauntlet**: incomplete and not certified. Round 2 on Gemini was unavailable, and this run did not cover rounds 4-6.

## Reviewed snapshot
- Repository `microsoft/SynapseML`. Source HEAD, target `master` at local `upstream/master`, and merge base are all `681bd96990c421de3b91d2b1bf8f8f470764199d`, ahead/behind 0/0, from local refs that I did not fetch.
- The patch is the uncommitted change to the three files below, relative to `.github/skills/synapseml-pr-loop/`. Bytes match the round-1 inputs.
- SHA-256 covers raw working-tree bytes. The Git blob ID covers content after `.gitattributes` normalization, `* text=auto eol=lf`.

| File | Change | SHA-256 of raw bytes | Blob |
| --- | --- | --- | --- |
| `SKILL.md` | +64/-5 | `ac92ea827c1a85c961dfd787a8761117686d872b3504ec4b666a26a3f4bf62db` | `e9df41d6` |
| `references/readiness-gates.md` | +7 | `4cb9a14f215fc8724fad6e2e53e144eb91fb5a8c376b1f84a27a386e112a7a58` | `35ffd4fe` |
| `references/loop-control.md` | new, 126 lines, CRLF | `d8592d624d90c35634155cc26246ba556190bc37f4232507390daef92ba59359`; LF-normalized `6a7ae4d342d0c4f66683e7849698c73d4174059ad03d7139b6d476d9756ea08e` | `fa54799c` |

## Evidence Checklist
- [x] Read all three changed files in full, plus `AGENTS.md`, the CI-triage waiting contract, helper parameters in `scripts/Get-PrReadiness.ps1` and `scripts/watch_azure_pipeline.py`, and the installed `review-code` skill, round-3 prompt and prompt generator.
- [x] 15 relative links and anchors resolve, frontmatter is valid, `git diff --check` exits 0, and there are no trailing spaces or TODO markers.
- [x] A missing PR defers remote gates and never passes them (`SKILL.md:33-35`; `references/loop-control.md:40-43,98`). I accept as given that the `AGENTS.md:142-147` session-draft rule overrides same-commit bundling (`SKILL.md:218-224`). Six rounds still come before the code commit (`SKILL.md:227`).
- [x] A missing task ID uses the named-task fallback, with no invented IDs (`references/loop-control.md:89`).
- [x] Older-SHA reviews wait for current-head coverage, rewritten SHAs invalidate remote evidence, and zero threads don't clear suppressed findings (`references/loop-control.md:80-83,101-102`; `references/readiness-gates.md:16,84-90`).
- [x] Artifact-only commits keep a recorded fingerprint exception and still need final-SHA CI and review (`references/loop-control.md:76-79,104`).
- [x] Authority holds. Triage stays read-only, comments are data, evidence gates never authorize CI, and external PRs need an exact-head safety check (`SKILL.md:36,43-45,51-56`; `references/readiness-gates.md:80-83`; `references/loop-control.md:3-5,108`).
- [x] No-progress and timeout rules hold: a 3-attempt cap, an early stop without new evidence, one evidenced infrastructure retry, a kickoff-based CI deadline that restarts cannot extend, and finite review waits (`references/loop-control.md:50-64`). `-WaitForReview` defaults to 20 minutes, range 1-240.
- [x] I traced the requested cases through the states and all 12 scenarios (`references/loop-control.md:33-38,96-109`). Missing PR ok plus I2, I3. Missing task ok. Target change I1, I6. Stale evidence ok. Artifact-only ok plus I5. No progress ok. Timeout ok. Unauthorized CI ok. Resume I4. Cycles and deadlocks I1, I3, I4.
- [ ] Live CI durations, bot arrival and GitHub state were not observed, because remote APIs were out of scope.

Pre-existing and not scored: `scripts/watch_azure_pipeline.py:15,225-226` caps waits at 120 minutes while `pipeline.yaml:240,273` allows 300-minute jobs, and `references/loop-control.md:57-60` restates that deadline.

## Issues

### Issue 1: The pass budget cannot hold the mandated passes, and target restarts go uncounted
- **Severity**: Medium. **File / Line(s)**: `SKILL.md:218-229`; `references/loop-control.md:33-43,50-52,73-75,82-83`
- **Description**: The default cap is 2 full gauntlet passes. The skill also requires six pre-commit rounds for the bootstrap commit and for later changes to reviewed content (`SKILL.md:227`), plus a final CI-qualified pass that the bootstrap pass cannot replace (228). It never says which passes count, or whether an aborted pass counts. Trace: bootstrap is pass 1, a CI or comment fix needs pre-commit pass 2, and the final CI-qualified pass 3 exceeds the cap on an ordinary PR. If those passes don't count, only "5 fast fix cycles per phase" bounds them, and nothing defines "phase". Target advances force integration, a new merge build and possible restarts (73-75) that no counter tracks. The state list also lacks a `reconcile` or `engineering-ready` to `fast` edge, though new feedback reopens triage (82-83).
- **Risk**: Ordinary PRs stop blocked at the cap, or a busy target or comment stream restarts expensive passes with no limit.
- **Suggested Fix**: Name the pass kinds, bootstrap, pre-commit and final, and say which count. Either let a clean pre-commit pass whose fingerprint matches the CI-proven head serve as the final pass, or size the cap to fit the mandated passes. Count target-driven restarts and add the reopen transitions.

### Issue 2: The installed direct contract cannot write the required pre-PR drafts
- **Severity**: Medium. **File / Line(s)**: `SKILL.md:203-207`; `references/loop-control.md:44-48,87-88,98`
- **Description**: Before a PR number exists, drafts must go to the session workspace, "never a placeholder repository folder". Under the direct contract this skill requires, the installed `review-code` prompt generator, in both its PowerShell and Bash forms, rejects an output directory outside the repository, ignored by Git or inside Git metadata. With no output directory, it falls back to a task or feature `reviews/` folder inside the repository. So round 1 of the new-PR path either fails or creates the forbidden folder, and the skill names no supported invocation. I accept the `AGENTS.md` precedence over same-commit bundling. This finding covers only prompt generation.
- **Risk**: The main new-issue scenario stalls at its first round, or agents improvise unrecorded workarounds, such as hand-edited prompts or stray folders, that the drafts can't prove later.
- **Suggested Fix**: Align `SKILL.md:207` with `AGENTS.md:145-147`, which forbids committing a placeholder, not writing one. Allow a temporary, uncommitted draft folder inside the repository, move drafts to session storage, check `git status` before every commit, and record the method in each draft. Otherwise report the gap as a blocker.

### Issue 3: The unchanged draft move has no path for unsafe content or the reviewed commit SHA
- **Severity**: Medium. **File / Line(s)**: `SKILL.md:69-71,218-224`; `references/loop-control.md:44-48,76-79`; `AGENTS.md:142-147`
- **Description**: Drafts must move "unchanged" and "without rewriting", and then the agent inspects them for secrets and unexpected content. This repository bans private logs, internal work items and cross-repository review text. If inspection finds that content, redaction rewrites the draft, leaving the draft out breaks the required handoff, and publishing leaks it. No rule picks one. `AGENTS.md:143-144` also asks for the reviewed commit SHA, which a pre-commit draft cannot contain, and adding it changes the draft. This series already has a case. The round-1 attempt-2 draft records an absolute local workstation path on line 13.
- **Risk**: The loop deadlocks before readiness, or local or private details land in a public repository.
- **Suggested Fix**: Require public-safe drafts from the start, with repository-relative paths and public facts only. If a draft is unsafe, keep it private, record the exclusion, and rerun that round as a new attempt on the same frozen patch. Allow an appended handoff block with the commit SHA and fingerprint match below the byte-preserved original.

### Issue 4: Session-scoped checkpoints and drafts are lost across sessions
- **Severity**: Medium. **File / Line(s)**: `references/loop-control.md:9-11,22-29,45-46,52`; `SKILL.md:69-75`
- **Description**: The checkpoint and the pre-PR drafts both live in session storage, yet the skill counts attempts "across compaction and restarts", and the drafts must last until a PR number exists. A new session, another agent tool or a cleaned session folder can't find either one. Budgets reset, in-flight job IDs and ownership vanish, and a second writer shows up only after it pushes. Lost drafts make the handoff impossible, and nothing says whether to rerun the pass or block.
- **Risk**: A restart bypasses every cap, concurrent sessions edit one worktree, and a session change either blocks readiness or tempts an agent to rebuild drafts from memory.
- **Suggested Fix**: Name one durable, discoverable per-worktree location for the checkpoint, an owner heartbeat and the drafts. The checkpoint can live under `git rev-parse --git-path`, but generated drafts can't, per Issue 2. If the checkpoint is missing, rebuild used budget from durable evidence. If drafts are lost, rerun the pre-commit pass as a new attempt on the same frozen patch and never recreate the text.

### Issue 5: The explicit full diff feeds handed-off drafts and earlier rounds into later rounds
- **Severity**: Medium. **File / Line(s)**: `SKILL.md:203-205,218-224`; `references/loop-control.md:76-79`
- **Description**: Every pass reviews an explicit target-to-head diff with pending changes. The prompt generator skips earlier review artifacts only when it builds its own default uncommitted diff, and it uses an explicit diff file verbatim. Once the handoff puts six bootstrap drafts in `reviews/pr-<number>/`, the final CI-qualified pass reviews them, and each round also picks up earlier rounds' pending artifacts. The artifact exception covers the fingerprint, not the reviewed diff.
- **Risk**: Later rounds see earlier verdicts, which weakens round independence. Prompts change while the patch is frozen, and nobody can fix findings about review text that must stay preserved.
- **Suggested Fix**: Build the reviewed diff and the fingerprint from one file set. Exclude only allocated artifact paths, for example with `:(exclude)` pathspecs, and state the exclusion in each draft.

### Issue 6: The fingerprint is undefined, depends on line endings, and never binds to the final head
- **Severity**: Medium. **File / Line(s)**: `references/loop-control.md:17,76-81`; `references/readiness-gates.md:13,16,74-78`
- **Description**: Nothing defines how to compute the reviewed-content fingerprint. `.gitattributes` normalizes text to LF, but the new `loop-control.md` is CRLF in this working tree. Its raw SHA-256, `d8592d62` as round 1 recorded it, won't match the committed or checked-out file at `6a7ae4d3`. Only the blob ID stays stable. Pre-commit drafts therefore can't prove they reviewed the committed content, and whole-file hashes change on a clean rebase even when the effective patch doesn't. Readiness checks head equality and "the same final patch" but never recomputes the fingerprint from the final head or requires a clean worktree.
- **Risk**: False invalidations burn passes against the cap from Issue 1, or content changed after the last round ships unreviewed.
- **Suggested Fix**: Hash the normalized patch against the merge base and exclude only allocated artifacts. Recompute it from each committed head, compare it before and after a rebase, and require empty `git status --porcelain` before readiness.

## Resolution Log
_Updated by the driving agent as findings are addressed._
- Issues 1-6. **Status**: Open. **What changed**: nothing, since this was a review-only request. **Why**: the author hasn't dispositioned them yet. **How verified**: pending.

## Limitations

## Driver resolutions

1. Separated mandatory pre-commit and final CI-qualified budgets, counted
   aborted passes and target movement, and added reopen transitions.
2. Documented direct prompt assembly from the installed theme reference for
   pre-PR session drafts; the numbered-directory generator is used afterward.
3. Required public-safe drafts, private retention and a fresh attempt for unsafe
   drafts, and appended publication provenance after preserved original text.
4. Added a discoverable per-worktree metadata checkpoint, ownership heartbeat,
   evidence pointers, and missing-state recovery without silently reset budgets.
5. The explicit diff and fingerprint now exclude the same exact allocated
   review-output paths to keep reviewers independent.
6. Defined normalized Git-blob manifest hashing, committed-HEAD comparison, and
   clean-worktree verification before readiness.

These fixes are pending the next frozen-patch review series. The original
findings and reviewed snapshot above are preserved.

## Original limitations
- This is a static tabletop review of documentation. I used no remote APIs, fetches, CI, runtime tests or external-citation fetches, and local refs may be stale.
- Only round 3 ran, and it did not rely on round-1 conclusions. Round 2 on Gemini was unavailable, so the six-round gauntlet is incomplete. This draft certifies nothing, merge readiness included.
- I did not run the prompt generator. Issues 2 and 5 describe its behavior from reading its source.
- No patch tool was available, so I wrote this draft with a file-creation tool. It is the only write. I made no source edits, staging, commits or pushes and used no subagents.
