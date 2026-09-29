# Round 3 review: edge cases and robustness
## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness (failure paths, contradictory mandatory requirements)
- **Mode**: sequential (installed copilot-toolkit `/review-code` direct contract). This is the pre-PR path: the round-3 prompt comes from the installed `REVIEW-PROMPTS.md`, and the output path is set explicitly.
- **Model**: claude-opus-5.5
- **Artifact**: session draft `task-pr-loop-attempt-3-review-3-claude-opus-5.5.md`, destined for `reviews/pr-<number>/` after publication
- **Issues Found**: 6 (3 Medium, 3 Low)
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Target `upstream/master` 681bd96990c421de3b91d2b1bf8f8f470764199d. HEAD and merge-base equal it on a local `chore/<topic>-20260924` branch. Reviewed content is pending: 2 modified and 1 untracked scoped file, with nothing else pending. Manifest SHA-256 (PowerShell default sort, LF-terminated `mode blob path` records; see Issue 5): `0b4b9af1b201a4b55f9bc5d55d49b55dfee058140cb8e6e4f0910cea3e8d7fcb`.
- [x] Read `AGENTS.md` (L142-147 session-draft rule), the three scoped files, their diffs (SKILL +79/−5, RG +9, LC new), and `references/ci-triage.md#waiting-for-azure-pipelines`. Helpers: `Get-PrReadiness.ps1 -WaitForReview` has `TimeoutMinutes` default 20 (range 1-240). The watcher polls every 600 s, stops 120 min after `--kickoff-at`, and reports `replaced` for newer builds.
- [x] Frontmatter parses. All 15 relative links and anchors resolve. `git diff --check` is clean. With `.gitattributes` set to `* text=auto eol=lf`, normalized blobs equal the LF blobs.
- [x] Generator probe in a scratch repository outside the worktree (deleted afterward), date-suffixed branch, explicit diff, in-repo output directory. With no task arguments it gave `task-20260924-attempt-2`, exit 0, and no warning. `-TaskName pr-loop` gave `task-unknown-attempt-2` plus a warning. `-TaskId pr-loop` exited 2 (numeric only).
- [x] Installed review-code generator behavior: the task token is the first run of 4+ digits in `-TaskName` or the current branch, otherwise `unknown`. An explicit diff file is embedded whole. Prompts over 1,048,576 bytes fail unless `-AllowOversize`/`--allow-oversize` is passed.
- [x] Target facts: `cognitive/src/test/resources/audio1.wav` is 1,127,416 bytes, and its binary add-diff is 1,178,360 bytes. `reviews/pr-*` already contains descriptive names (for example `task-job-wait-errors-attempt-1-…`) and `task-unknown-attempt-1-…`. `.dev-loop/` is ignored, and `reviews/pr-<n>/` is trackable.
- [x] Earlier fixes are present: session-draft precedence (SKILL L228-235; AGENTS L145-147), pre-PR assembly from `REVIEW-PROMPTS.md` (SKILL L213-218), exact-output exclusion (SKILL L203-206; LC L89-90), HEAD manifest plus clean worktree (LC L97-99; RG L80-81), checkpoint (LC L9-14, L28-36), budgets (LC L67-76), finite waits (LC L78-81), and unsafe-draft handling with provenance (LC L57-65).
- [ ] Live GitHub, Azure, and reviewer state: not read (no remotes). Static and tabletop analysis only. Other rounds were not assessed. The driver reports that round 2 ran clean on actual gemini-3.8-flash via Copilot CLI; I did not re-verify that and do not call the model unavailable. I make no claim about CI or overall gauntlet completion.

| Normalized blob | Lines | Path (mode 100644) |
| --- | --- | --- |
| 6ab7b616292d5f372e9671e2b4cc929dc310f8af | 276 | `.github/skills/synapseml-pr-loop/SKILL.md` |
| f78de58f9ce6ace39d59e682a74b9d77a31fb069 | 117 | `.github/skills/synapseml-pr-loop/references/readiness-gates.md` (RG) |
| b910f9dea15af61b1900fab284fc343c46636463 | 160 | `.github/skills/synapseml-pr-loop/references/loop-control.md` (LC, new) |

## Issues
### Issue 1: Unqualified "New feedback" burns fix cycles on artifact-only heads
- **Severity**: Medium | **File**: LC, SKILL.md | **Line(s)**: LC L46-47 vs L116; LC L110-113, L136, L138; SKILL L119-127, L231-235
- **Description**: LC L46-47 sends `reconcile` or `engineering-ready` back to `fast` and spends a cycle on *any* new feedback, but L116 reopens triage only for *substantive* feedback. SKILL requires waiting for the automated review of every pushed head and treating collapsed or suppressed notes as ordinary findings. Artifact-only commits (the bootstrap handoff and final-pass artifacts) still need remote review.
- **Risk**: Each artifact push can draw low-confidence notes on the review Markdown, and each one spends one of the 5 total cycles. Added to normal fix cycles, reviewer noise alone can force a false `blocked`. Tabletop rows L136 and L138 do not cover this case.
- **Suggested Fix**: Spend a cycle only on new substantive findings that match no recorded finding and need a reviewed-content change. Record triage-only dispositions and return to `reconcile`. Add a tabletop row for a new finding on an artifact-only head.
### Issue 2: "Its named-task fallback" does not exist, and the generator silently derives numeric tokens
- **Severity**: Medium | **File**: LC, SKILL.md | **Line(s)**: LC L121-123; SKILL L211-218
- **Description**: Once a PR exists the generator is mandatory, but it emits only a digit token or `task-unknown` (probe). With no task argument, a date, issue number, or PR number in the branch name silently becomes the token. The working branch here is date-suffixed.
- **Risk**: Artifacts show a date or number as if it were a work-item ID, which contradicts "Do not invent work-item IDs", or they show `task-unknown`. Either way they diverge from the manually named pre-PR drafts in the same `reviews/pr-<n>/`, and the existing descriptive names there cannot be produced.
- **Suggested Fix**: Define the fallback as a descriptive, non-numeric, checkpointed token. Set each dispatch's output path explicitly, as the pre-PR assembly does, or verify `taskSlug`/`outputFilePattern` before dispatch. Never omit both task arguments.
### Issue 3: Manifest checks never compare against the real PR diff, and moved drafts are not allocated outputs
- **Severity**: Medium | **File**: LC, SKILL.md, RG | **Line(s)**: LC L12-13, L55-56, L89-99; SKILL L203-206, L231-233; RG L80-81
- **Description**: The manifest is the "explicitly scoped" set, and the post-commit and readiness checks recompute that same set. Nothing requires the target-to-HEAD changed-path set to equal the manifest plus the allocated outputs. The handoff moves drafts to new `reviews/pr-<n>/` paths, but the checkpoint recorded only their session paths, and exclusions cover only "exact allocated" paths.
- **Risk**: An unscoped file committed with a fix passes both checks, for example a local `build.sbt` tweak picked up by `git commit -a`. A later required pass's full diff will either include earlier verdicts, violating SKILL L205-206, or fail to match.
- **Suggested Fix**: At post-commit and readiness, require `git diff --name-status --no-renames <merge-base> HEAD` to equal the manifest plus the allocated outputs. At handoff, record the moved repository paths as allocated outputs.
### Issue 4: No over-budget path, and pre-PR manual prompts skip the size check
- **Severity**: Low | **File**: SKILL.md, LC | **Line(s)**: SKILL L203-207, L213-216; LC L94-95
- **Description**: A full binary-capable diff that touches only `audio1.wav` already exceeds the generator's 1 MiB budget. The generator then fails and suggests trimming or overriding, while manually assembled pre-PR prompts get no size check at all.
- **Risk**: Trimming breaks the same-manifest rule. Forcing the size through, or skipping the check, can produce a truncated review that gets recorded as clean.
- **Suggested Fix**: Apply the same byte budget to manual prompts and record the prompt size. Treat over-budget as `blocked` unless the user approves a split recorded under one fingerprint. Never trim or force the size for a clean verdict.
### Issue 5: Recomputation after a resume is not deterministic (corrupt checkpoint, non-canonical manifest hash)
- **Severity**: Low | **File**: LC | **Line(s)**: LC L9-14, L34-36, L91-93, L97-98
- **Description**: One JSON file is rewritten at every transition, but only a *missing* checkpoint has a recovery rule. "Sorted manifest … LF separators" leaves the sort order, record layout, final newline, and deletion form open. For this manifest the hashes differ: default sort `0b4b9af1b201…`, ordinal/Python sort `67e85f5dd1ad…`, no final LF `6b8010c81543…`, path-first TSV `74790fe73ef5…`.
- **Risk**: After compaction or a tool change, identical content fails to match. That forces spurious stops or re-runs, or leaves "same frozen patch" unprovable.
- **Suggested Fix**: Write via temp file plus rename. Treat an unparseable checkpoint as missing and preserve it. Specify `<mode> <blob> <path>\n` records, byte-ordinal sort, and a deletion form, then compare the records, not only the digest.
### Issue 6: Final-pass artifacts cannot be committed "with the reviewed changes", and the "below" reference points nowhere
- **Severity**: Low | **File**: SKILL.md, LC | **Line(s)**: SKILL L197, L227, L234-235; LC L110-113
- **Description**: L227 requires committing review artifacts with the reviewed changes once a PR exists. The final pass starts only after current-head CI (L197), so its artifacts can only land in an artifact-only commit. L234 cites "the artifact-only rule below", but nothing below it in SKILL.md defines that rule; it lives in LC.
- **Risk**: Read literally, this either forces an extra content commit or rejects the required final artifact commit. Readers may also conclude the exception is missing.
- **Suggested Fix**: Scope L227 to commits that change reviewed content. Point the final-pass case to `references/loop-control.md#evidence-invalidation` by name.

## Resolution Log

Driver update: all six findings are addressed in the next source revision.
1. Duplicate/rebuttal triage preserves the clean pass; only substantive changes
   or relevant target movement consume a fix cycle.
2. Explicit descriptive dispatch tokens replace unverified numeric inference.
3. Compare the complete changed-path set and record moved artifact paths.
4. Manual and generated prompts share the byte budget and block oversize input.
5. Atomic checkpoint writes, corrupt-state recovery, and canonical compact JSON
   manifest records remove ambiguous serialization.
6. Final artifacts may use an artifact-only commit; the reference now links to
   the actual rule rather than saying it is below.
Validation requires fresh structural checks and a full frozen-patch series.
Original findings and evidence above remain unchanged.

_Updated by the driving agent as findings are addressed._
- Issue 1 — **Status**: Open · **What changed**: pending · **Why**: pending · **How verified**: pending
- Issue 2 — **Status**: Open · **What changed**: pending · **Why**: pending · **How verified**: pending
- Issue 3 — **Status**: Open · **What changed**: pending · **Why**: pending · **How verified**: pending
- Issue 4 — **Status**: Open · **What changed**: pending · **Why**: pending · **How verified**: pending
- Issue 5 — **Status**: Open · **What changed**: pending · **Why**: pending · **How verified**: pending
- Issue 6 — **Status**: Open · **What changed**: pending · **Why**: pending · **How verified**: pending
