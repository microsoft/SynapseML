# Round 3 review: edge cases and robustness

## Review Summary
- **Round**: 3
- **Theme**: Edge Cases & Robustness (error handling, boundary conditions, concurrency, failure modes)
- **Mode**: sequential (direct contract)
- **Model**: claude-opus-5.5
- **Artifact**: task-pr-loop-attempt-5-review-3-claude-opus-5.5.md (pre-PR session draft; its first committed
  location will be `reviews/pr-<number>/`)
- **Issues Found**: 0
- **Verdict**: CLEAN

## Reviewed identity
- Target: `upstream/master` 681bd96990c421de3b91d2b1bf8f8f470764199d. Worktree HEAD equals the target on
  `chore/pr-loop-feedback-20260924`. The patch is the uncommitted working tree: 2 modified files and 1 new file.
- Normalized blobs (`git hash-object`, mode 100644):
  - `.github/skills/synapseml-pr-loop/SKILL.md` 10637476ca71d46c454dddd304c672fd559d3837 (286 lines)
  - `.github/skills/synapseml-pr-loop/references/loop-control.md` 37f0039e97b698f3ebb00c1f5f774bd0c94ecb33 (177)
  - `.github/skills/synapseml-pr-loop/references/readiness-gates.md` f78de58f9ce6ace39d59e682a74b9d77a31fb069 (117)
- Canonical manifest, built with the skill's own recipe (compact JSON, UTF-8 byte order, one final LF): 398 bytes,
  SHA-256 19fec4fff5dba098eb359bb9cd8e1cad4936a6c4bb3426e090d56cabd7b3305e.
- I did not read earlier review drafts or use them as inputs.

## Evidence Checklist
- [x] Structure: `git diff --check` exits 0 and the frontmatter is valid. All 16 local links and anchors resolve,
  including `loop-control.md#evidence-invalidation` and `ci-triage.md#waiting-for-azure-pipelines`.
  `.gitattributes` (`* text=auto eol=lf`) normalizes the CRLF working copy of `loop-control.md`, so its blob is LF.
- [x] Output-location fix (SKILL L213-222): the three cases are disjoint and cover every combination:
  - Before a PR exists: session drafts, with prompts assembled manually.
  - A PR without a verified Task: `reviews/pr-<number>/`, with manual assembly and a descriptive token.
  - A PR with a Task: the generator, with both values passed and output names checked.

  This matches `AGENTS.md` L142-147 and `loop-control.md` L135-139. The installed `Run-ReviewRound.ps1` confirms the
  premise: without `-TaskId` it infers 4+ digits from TaskName or the branch (this branch ends in 8 digits), and it
  rejects output directories outside the repo. `reviews/pr-<n>/` is inside the repo (`git check-ignore` exits 1).
  Upstream history already uses descriptive `task-<token>` names.
- [x] Checkpoint failure modes (`loop-control.md` L7-39): Git 2.55 resolves `--git-path pr-loop` to
  `<common-dir>/worktrees/<name>/pr-loop`, and `git ls-files` rejects that path as outside the repository. The text
  requires an atomic rename and preserves a corrupt file without treating it as a zero budget. It rebuilds a missing
  file (for example after worktree removal), and unknown usage blocks renewal. Lost drafts are rerun as new attempts,
  a stale owner is checked, and nothing is requeued or reposted just because of a context reset.
- [x] Termination (L74-92): caps count aborted passes and survive compaction, and the loop stops early when there
  is no new evidence. The watcher contract matches `scripts/watch_azure_pipeline.py` (`POLL_SECONDS = 600`,
  `MAX_TIMEOUT_MINUTES = 120` from kickoff). The `Get-PrReadiness.ps1` review wait defaults to 20 minutes. A
  duplicate bot note on an artifact-only head needs no commit, so final-SHA CI cannot re-trigger itself.
- [x] No deadlock on a red-CI fix: the pre-commit review applies to every reviewed-content commit (SKILL L247-249;
  loop-control L56-58 and L76-77). It is independent of the step 8 entry condition (SKILL L197), so a fix can be
  reviewed, committed, and retested.
- [x] Stray or partial content (L94-139): staged paths are checked before commit. Before readiness, the merge-base
  `--name-status --no-renames` set must equal the manifest plus the allocated outputs (L111-114). Deletions encode
  `000000`/`null`. None of the 3870 tracked paths contains pathspec glob metacharacters, and paths with spaces only
  need quoting. The `readiness-gates.md` L80-81 bullet can only be performed through that recipe.
- [x] CI authorization and external safety (`readiness-gates.md` L82-85; SKILL L251-261): evidence never
  authorizes CI. `/azp run`, `-RunPipeline`, and workflow approval need explicit authorization, plus a fresh trusted
  check of the head for external PRs. `-RunPipeline` is not combined with the wait loop, and a missing build stays
  a blocker. Step 8 starts only after required CI, which itself required the trusted safety check of that head.
- [x] Unsafe drafts (L67-72): the original stays private and a public-safe exclusion note is recorded. The round is
  rerun on the same patch as a new attempt, and its findings are kept. The no-new-evidence stop (L81) bounds
  repeats. An oversize prompt stays blocked unless a complete split is approved (SKILL L223-225).
- [x] Tabletop cases, each traced to a governing line: new issue with no PR; existing PR without a Task;
  review-only external PR; no Azure build; green review on an older SHA; collapsed finding; round-5 fix; artifacts
  only; duplicate bot note; head or target movement; same failure three times; CI timeout; secret request; missing
  approval.

## Limitations
- Documentation-only review. No product build, live GitHub/Azure call, remote fetch, or execution of the proposed
  loop.
- Covers round 3 only. It makes no claim about CI status or overall gauntlet completion. The driver appends
  resolution notes and publication provenance.

Clean review round: zero issues found.
