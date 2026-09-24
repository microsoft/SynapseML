# PR #2737 pending corrections: review-code round 6, attempt 12

## Review Summary

- **Round**: 6 of 6
- **Theme**: Polish and hardening. In the non-code adaptation this becomes security and polish, covering documentation accuracy, observability, publication safety, naming clarity, and performance/cost.
- **Mode**: sequential, slot 3 (Anthropic Opus)
- **Model**: Claude Opus 5.5 (`claude-opus-5.5`)
- **Date**: 2026-09-24
- **Gate type**: Mandatory local-gate pre-commit pass over the pending corrections. This is not the final CI-qualified review. After this round, the corrections must be pushed and pass new-head CI and current-head automated review, followed by the final CI-qualified pass.
- **Artifact**: `reviews/pr-2737/task-pr-loop-attempt-12-review-6-claude-opus-5.5.md`
- **Issues Found**: 0
- **Verdict**: CLEAN

## Reviewed identity

| Item | Value |
| --- | --- |
| Branch | `chore/pr-loop-feedback-20260924` |
| Published head (`HEAD`) | `e0b3b12024ef2fffd451629c65b00370906ff719` |
| Recorded target / merge base | `681bd96990c421de3b91d2b1bf8f8f470764199d` |
| Pending corrections | Staged, not yet committed: 3 files, +93/-20 lines relative to `HEAD` |
| Reviewed diff | Merge base to index, 3 paths, 27,449 bytes, SHA-256 `47478a273c9b2f31b1695c94307b884fe808076acb776d2574587da41aa63a08` |
| Frozen manifest | SHA-256 `1b6aa6ad0b95822a677321b0e60d151f9c786fa39591b04bc3b06bcbffbfd1a6` |

Manifest entries (mode and index object ID):

- `.github/skills/synapseml-pr-loop/SKILL.md`: modified, `100644`, `979200e29afcb3d82e4ae9e219a5efe36f629527`
- `.github/skills/synapseml-pr-loop/references/loop-control.md`: added, `100644`, `c8a53ef0b6ef3701786c5a0717874361fe130505`
- `.github/skills/synapseml-pr-loop/references/readiness-gates.md`: modified, `100644`, `3ff834fc6352f36c7aac17833ff50f40efa7ecf1`

## Evidence Checklist

- [x] **Independence.** I checked only that the assigned output path did not exist. I did not open or search any earlier review artifact, checkpoint, or reviewer prompt. The names of other `reviews/pr-2737/` files appeared in `git status` output, but I did not open those files.
- [x] **Snapshot.** `git status --porcelain=v1 --untracked-files=all` shows two things: the three scoped files, staged and changed only in the index, and, before this report was added, 14 untracked review outputs under `reviews/pr-2737/`. `git diff --quiet` returned 0, so there are no unstaged edits. `git merge-base HEAD 681bd969...` returned `681bd969...`. I rechecked immediately before writing this report and the manifest was unchanged.
- [x] **Path set.** `git diff --cached --name-status -z --no-renames --ignore-submodules=none 681bd969...` lists 21 paths. Three are the scoped files. The other 18 are review outputs under `reviews/pr-2737/` that were already committed. They are excluded from the manifest, and I did not open them.
- [x] **Manifest.** I recomputed the manifest exactly as `references/loop-control.md` specifies: a compact JSON array with keys in the order `path`, `mode`, `blob`, ASCII-escaped, sorted by UTF-8 path bytes, and ending in one LF. Its SHA-256 is `1b6aa6ad...`, which equals the frozen value.
- [x] **Diff.** I regenerated the diff with the documented command, `git --literal-pathspecs diff --cached --binary --no-ext-diff --no-textconv --ignore-submodules=none --submodule=short --output=<file> <merge-base> -- <paths>`. The output is byte-identical to the supplied diff (SHA-256 `47478a27...`) and uses only LF line endings.
- [x] **Disposable index.** I set a temporary `GIT_INDEX_FILE` for these subprocesses only and ran `read-tree 681bd969...`. `git apply --cached --binary --check` returned 0. After the apply, the changed-path/mode/object manifest was byte-equal to the frozen manifest (`1b6aa6ad...`). The real index was not modified.
- [x] **Committed-only state.** The manifest computed from `HEAD` alone is `19fec4fff5dba098eb359bb9cd8e1cad4936a6c4bb3426e090d56cabd7b3305e`, which does not match the frozen value. This is expected while the corrections are uncommitted. The post-commit `HEAD` recompute required by `readiness-gates.md` is still open.
- [x] **Format and metadata.** `git diff --cached --check 681bd969... -- .github/skills/synapseml-pr-loop` returned 0. In the index blobs:
  - Line endings are LF, and each file ends with an LF.
  - There are no tabs, no trailing whitespace, no TODO/FIXME/TBD markers, and no non-ASCII characters.
  - The frontmatter parses as YAML. `name` is `synapseml-pr-loop` and matches the directory. `description` is 387 characters and `compatibility` is 163.
- [x] **Links.** All 16 relative links and anchors in the three files resolve against the index snapshot, including `loop-control.md#evidence-invalidation` and `ci-triage.md#waiting-for-azure-pipelines`. Both Anthropic sources load, and their content matches the summaries in `loop-control.md`. The OpenAI URL returns HTTP 403 to automated fetches. A web search confirms that "Harness engineering: leveraging Codex in an agent-first world" is published at that URL.
- [x] **Renumbering.** A grep of `.github/` found no stale step-number or "readiness loop" references outside `SKILL.md`. "Run steps 3-7 ... step 8" is consistent with the renumbered `### 9. Final readiness loop`.
- [x] **Claims about the installed `/review-code` skill.** I checked each claim against its PowerShell and Bash prompt generators:
  - When no diff file is supplied, the default is `git diff HEAD`, which omits committed PR content.
  - `-DiffFile` and `--diff-file` both exist.
  - Under the direct contract, both generators exit when the output directory is outside the repository.
  - When an output directory is given but no task ID, the first run of four or more digits in the task or branch name silently becomes the task ID. For this branch that would be `20260924`.
  - The PowerShell generator enforces a 1,048,576-byte prompt budget and exits with code 3 when it is exceeded.
  - The defaults are the direct contract and sequential mode.
- [x] **Repository conventions.** `AGENTS.md` is unchanged from the merge base (blob `fe86f9165b1f3160ad634fc2d4b8eb84fe79e649`). It contains the two cited rules: review artifacts go directly in `reviews/pr-<pr_number>/`, and drafts stay in the session workspace until the PR number exists.
- [x] **Helper claims.** In `watch_azure_pipeline.py`, `POLL_SECONDS = 600`, `MAX_TIMEOUT_MINUTES = 120`, and the deadline is computed from `--kickoff-at`. This matches the CI-monitoring text in loop control. `Get-PrReadiness.ps1` declares `-PullRequest`, `-WaitForReview`, and `-RunPipeline`.
- [x] **Checkpoint location.** `git rev-parse --path-format=absolute --git-path pr-loop` resolves to `.git/worktrees/<name>/pr-loop`, which is the linked worktree's private Git directory. It is not the common directory, the index, or the config (tested with Git 2.55.0).
- [x] **Disposable-repository protocol exercise.** I ran this in a temporary repository with no remotes. The manifest had 11 entries: a modification, a deletion (`000000` with `null` blob), a rename recorded as a delete plus an add, a `100755` mode change, a binary file, a CRLF file marked `-text`, a non-ASCII path, `[ab].txt`, a symlink (`120000`), and a gitlink (`160000`) under `.gitmodules` `ignore = all`. Results:
  - The gitlink was hidden without `--ignore-submodules=none` and listed with it.
  - `--output` preserved the CR bytes.
  - `--literal-pathspecs` kept an unscoped `a.txt` out of the diff. Without it, `a.txt` leaked into the diff and the disposable-index check rejected the result.
  - The disposable-index manifest equaled the frozen manifest.
  - After commit, the `HEAD` manifest also equaled it, and the `HEAD` path set equaled the manifest plus the allocated output.
  - An empty pathspec selected the whole change, which confirms that the empty-manifest prohibition is needed.
  - Setting `diff.renames=copies`, `color.ui=always`, `diff.mnemonicPrefix`, `diff.srcPrefix`, `diff.relative`, or `apply.whitespace=fix` left the verified result unchanged.
- [x] **Pre-commit versus final review.** All three files keep this distinction consistently:
  - `SKILL.md` step 8 gates mandatory pre-commit passes on local checks only, and runs the final CI-qualified pass after current-head CI and review are green.
  - In loop control, the `fast -> gauntlet` gate uses the same split, and a pre-commit pass cannot satisfy `gauntlet -> reconcile`.
  - The "Existing PR has red CI" scenario follows the same rule.
  - `readiness-gates.md` requires the final CI-qualified pass.
- [x] **Publication safety.** A grep of the three files found no workstation paths, private repository names, work-item IDs, or credentials. The new rules keep checkpoints and prompts outside tracked source. They require drafts to use repository-relative paths and public facts only, and they define how to exclude an unsafe draft.
- [x] **Observability and cost.**
  - The checkpoint schema records commands, environment, revisions, exit codes, IDs, attempts, and budgets.
  - The budgets are internally consistent: 6 pre-commit passes (1 initial plus one per each of 5 fix cycles), 2 final passes, 3 attempts per failure fingerprint, and a watcher deadline that cannot be extended.
  - I found no unbounded loop.

## Non-blocking observation (not a finding)

The documented review-diff command depends on the local Git configuration:

- With `color.diff=always`, ANSI escape codes appear in the `--output` file.
- With `diff.noprefix=true`, the `a/` and `b/` prefixes are dropped.

In both cases the documented disposable-index check rejected the diff in the temporary repository, so the protocol fails closed and nothing unsafe is dispatched. Adding `--no-color --src-prefix=a/ --dst-prefix=b/` made both cases match. This is optional hardening for a bounded manual workflow, not a defect. It did not affect this round: the frozen diff was regenerated here byte for byte.

## Limitations

- I made no Git remote calls and did not fetch, so I did not verify the live PR head, the current target, CI, or automated review. The local `origin/master` tracking ref (`98196110b4`) is an ancestor of the recorded merge base, so it is stale. Target currency therefore rests on the driver-recorded `681bd969...`.
- The pending corrections do not have a commit SHA yet. The post-commit `HEAD` manifest match, new-head CI, current-head automated review, and the final CI-qualified gauntlet are all still required. This clean pre-commit round satisfies none of them.
- I did not re-run the driver's nine Git protocol tests because they were not supplied. My independent protocol evidence is the disposable-repository exercise above. I did not exercise `--force-with-lease`, which needs a remote.
- I did not run `Get-PrReadiness.ps1` or the watcher. Both need network access, and this diff does not change either. I assessed the tabletop scenarios by reading them, not by running them.
- The review covered the three scoped files. I read other files only to verify cross-references and claims.

Clean review round: zero issues found.
