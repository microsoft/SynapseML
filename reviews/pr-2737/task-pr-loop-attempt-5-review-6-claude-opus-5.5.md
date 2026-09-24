# Round 6 review: polish, documentation accuracy, safe evidence publication

## Review Summary
- **Round**: 6 of 6 (sequential slot 3)
- **Theme**: Polish & hardening, focused on documentation accuracy and safe evidence publication
- **Mode**: sequential; direct contract via the pre-PR session-draft path (descriptive token `pr-loop`, no verified Task)
- **Model**: claude-opus-5.5
- **Artifact**: session draft `task-pr-loop-attempt-5-review-6-claude-opus-5.5.md`, moved unchanged to `reviews/pr-<number>/` after the PR exists
- **Issues Found**: 0
- **Verdict**: CLEAN

## Reviewed identity
- Public SynapseML worktree, branch `chore/pr-loop-feedback-20260924`. Target `upstream/master`, HEAD, and merge base are all
  `681bd96990c421de3b91d2b1bf8f8f470764199d`.
- Source is that HEAD plus exactly three pending scoped paths (`git status --porcelain=v1 --untracked-files=all`).
  Only Markdown files changed. No executable or product file changed.
- Normalized manifest in the canonical loop-control form (compact JSON sorted by UTF-8 path bytes, 398 bytes including the final LF).
  SHA-256 `19fec4fff5dba098eb359bb9cd8e1cad4936a6c4bb3426e090d56cabd7b3305e`:

| Path | Change | Base blob | Normalized blob (mode 100644) |
| --- | --- | --- | --- |
| `.github/skills/synapseml-pr-loop/SKILL.md` | M +84/-5 | `23127e4bd47c67a042084e6fc3cbf3bbccddd276` | `10637476ca71d46c454dddd304c672fd559d3837` |
| `.github/skills/synapseml-pr-loop/references/loop-control.md` | A, 177 lines | none | `37f0039e97b698f3ebb00c1f5f774bd0c94ecb33` |
| `.github/skills/synapseml-pr-loop/references/readiness-gates.md` | M +9/-0 | `aca7b576b3964a24e8ed0ff85ff9083fb8760c84` | `f78de58f9ce6ace39d59e682a74b9d77a31fb069` |

## Evidence Checklist
- [x] Line endings: `* text=auto eol=lf` applies to all three files. `SKILL.md` and `readiness-gates.md` use LF. The untracked
  `loop-control.md` working copy uses CRLF, so its raw-byte blob `2bb442bdbaaa59525b0b443b3ca1fa82b90253a5` differs
  from the normalized blob above. Staging normalizes it, and the documented rule to compare normalized object IDs handles this case. Compare
  only normalized IDs.
- [x] Frontmatter: the YAML parses to `name`, `description`, and `compatibility`. `name` matches the directory. The
  description is 387 characters and compatibility is 163.
- [x] Links: all 16 relative links resolve, including 5 anchors (`#evidence-invalidation` x3,
  `#waiting-for-azure-pipelines` x2). Every link target except the new `loop-control.md` already exists at the target
  commit. The 3 external source links are well formed; they were not fetched.
- [x] Rendering and renumbering: a repository-wide `git grep` found no reference to the old step-8 number or anchor, so
  renumbering step 8 to 9 breaks nothing. Headings are unique, all 26 table rows have 2 columns, code fences are
  balanced, and no bare `<...>` placeholder appears outside code. `git diff --check` is clean. The normalized content
  has no trailing whitespace or tabs, and a scan for TODO/FIXME/TBD markers found none.
- [x] Helper claims: `Get-PrReadiness.ps1` defines `-PullRequest [int[]]`, `-WaitForReview` (bounded by
  `-TimeoutMinutes`, default 20), and `-RunPipeline`. The conditions behind its `completeness.complete` match the
  readiness text. `watch_azure_pipeline.py` polls every 600 seconds, caps at 120 minutes, and measures its deadline
  from kickoff, which matches SKILL, loop control, and CI triage.
- [x] Git commands: in this linked worktree, `git rev-parse --path-format=absolute --git-path pr-loop` resolves inside
  the per-worktree Git directory (`.../worktrees/<name>/pr-loop`), outside tracked source.
  `git diff --name-status --no-renames <merge-base> HEAD` runs correctly (its output is empty here because HEAD equals the target).
- [x] Toolkit statements: I checked every step-8 statement about the toolkit against the locally installed `/review-code`
  skill, its `REVIEW-PROMPTS.md`, and both generator scripts. Confirmed: `-DiffFile`/`--diff-file`; the default diff
  covers only uncommitted changes; direct-contract output directories must be inside the repository; the generator
  silently infers a Task ID from branch-name digits; there is a prompt byte budget; artifacts are bundled in the same
  commit by default; six rounds must run before commit; sequential is the default mode; and there are six themes.
- [x] Existing PR with no Task: routing is explicit. The docs set `reviews/pr-<number>/` "even when there is no Task",
  prompts are assembled manually, and a descriptive token is used. With a PR and a verified Task ID, an explicit output
  directory bypasses the generator's task-file lookup. `reviews/pr-<n>/` is not Git-ignored (`git check-ignore
  --no-index` exits 1), so the documented generator path works.
- [x] Cross-document consistency: SKILL, loop control, readiness gates, CI triage, `AGENTS.md`, and the
  external-contributor skill agree on these points:
  budgets (5 cycles, 3 same-failure attempts, 2 final CI-qualified passes, 6 pre-commit passes), the watcher contract,
  pre-PR artifact placement, the read-only external-contributor path, and all 13 tabletop rows.
- [x] Publication safety: none of the three files contains an absolute path, user name, private work-item or Feature
  ID, private host, secret, or cross-repository content. The installed review toolkit is named only as a prerequisite,
  with no private URL, ID, or copied text. The docs require drafts that use repository-relative paths and public facts,
  inspection before publication, private retention and a rerun for any unsafe draft, and append-only provenance.
- [x] Considered but not raised: some lines exceed 80 columns, but no Markdown line-length check exists and the target
  already had such lines. The mention of the unsafe-draft procedure has no link, but the loop-control link is nearby
  (a navigation nit).

## Limitations
- This is a static documentation review of the frozen working tree. No PR, CI run, bot review, or other remote evidence
  exists or was queried. No network or remote Git operation ran, so the external source URLs are unverified.
- Toolkit behavior was checked against the local installation at review time. Later toolkit versions may differ.
- I read neither earlier review reports nor the loop checkpoint. The driver should compare the manifest above with the
  frozen manifest.
- The review was read-only: it changed no source, index, ref, or remote. This round does not establish that the whole
  gauntlet is complete, what CI status is, or that the PR is ready to merge.

Clean review round: zero issues found.
