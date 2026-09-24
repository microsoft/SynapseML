# Round 3 review: edge cases and robustness (non-code adaptation)

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness, adapted for process docs (unusual inputs, missing cases, failure modes)
- **Mode**: sequential, independent round 3 only
- **Model**: claude-opus-5.5 (Claude Opus 5.5)
- **Artifact**: reviews/pr-2737/task-pr-loop-attempt-10-review-3-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Reviewed identity

- Branch `chore/pr-loop-feedback-20260924`. Local HEAD is `e0b3b12024ef2fffd451629c65b00370906ff719`, with staged pending edits to the three scoped files and no unstaged changes to them.
- Recorded target and merge base: `681bd96990c421de3b91d2b1bf8f8f470764199d`. `git merge-base HEAD 681bd969...` returns the same commit. I did not fetch the live target.
- Scope: `.github/skills/synapseml-pr-loop/SKILL.md`, `references/loop-control.md`, and `references/readiness-gates.md`. I read `AGENTS.md` only for artifact-placement context.
- I recomputed the manifest from the index using the contract's rules. The path set is the merge-base-to-index `--name-status -z --no-renames` set minus the allocated `reviews/pr-2737/` outputs, sorted by UTF-8 bytes and written as compact ASCII JSON with one final LF:

```json
[{"path":".github/skills/synapseml-pr-loop/SKILL.md","mode":"100644","blob":"f832976dc19728cc957e803f6437ff4caba99029"},{"path":".github/skills/synapseml-pr-loop/references/loop-control.md","mode":"100644","blob":"18acef3dcf6da54658c6e16e41d6e9424ba4b979"},{"path":".github/skills/synapseml-pr-loop/references/readiness-gates.md","mode":"100644","blob":"3ff834fc6352f36c7aac17833ff50f40efa7ecf1"}]
```

Its SHA-256 is `a92c57bc651db7763e0d90f154be659ca611967a7432f9a4de51019812f8ed08`, an exact match for the frozen manifest.

## Evidence Checklist
- [x] Read the three scoped files in full. Also read the staged diff against HEAD (3 files, +56/-19) and the full diff against the recorded merge base.
- [x] Recomputed the manifest shown above. All three index blobs are LF-only and end in LF. The worktree copy of `loop-control.md` has CRLF endings (`i/lf w/crlf` under `* text=auto eol=lf`), but the normalized blob is unaffected. That matches the contract's normalization claim.
- [x] Ran `git diff --cached --check` against the merge base for the scoped paths. It came back clean.
- [x] Checked the contract's claims against the installed review-code toolkit. Its round 3 prompt and non-code table set this theme. When the prompt generator gets an explicit `-DiffFile`, it doesn't append untracked files. When it gets an explicit output directory, it skips the task-file lookup. With no explicit task ID, it treats any run of four or more digits in the task label or branch name as the Task ID. This branch name contains `20260924`, so the contract's warning and its descriptive-token rule hold here.
- [x] Ran two disposable Git probes in a throwaway repository and deleted it afterward. Issue 2 describes them.
- [x] Traced these paths and found no defect:
  - checkpoint corruption or loss, and a stale owner;
  - a lease after the remote head moved, or after the agent's own earlier push (both fail closed);
  - target movement during a pass;
  - the watcher deadline and a watcher restart;
  - a missing Azure build, and an external head change between clearance and trigger;
  - budget arithmetic: the pre-commit cap of 6 = 1 + 5 cycles still holds with aborted passes and integrations;
  - unsafe drafts, artifact-only heads, and duplicate bot notes;
  - oversized prompts and unavailable reviewers;
  - rename or quoted-path derivation errors, which the post-commit `-z --no-renames` equality check catches.
- [ ] Remote state (live target, PR threads, CI, bot reviews) was not checked. The instructions ruled out remote access.

## Issues

### Issue 1: Fix replies and thread resolution can go out before the fix is reviewed or pushed
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/SKILL.md; .github/skills/synapseml-pr-loop/references/loop-control.md; .github/skills/synapseml-pr-loop/references/readiness-gates.md
- **Line(s)**: SKILL.md 38-39, 127-128, 213-217; loop-control.md 35, 60-64; readiness-gates.md 87
- **Description**: The fast loop is where comments get handled. It lists "consume comments" before "fix CI" (SKILL.md 39). Step 4 says "Reply in the existing thread with the fix and evidence, then resolve it", and the next bullet is "Re-audit after every push" (127-128). Nothing says the fix has to be committed and pushed first. This PR adds a mandatory six-round pre-commit pass after the local gates and before the push (SKILL.md 213-217, loop-control.md 60-64). That pass can take hours. It can change the fix or throw it out. The loop can also hit a cap or stop blocked on a rejected lease. Here's one way it goes wrong. An existing PR has thread T. The agent fixes the problem locally, replies "fixed", and resolves T. Round 4 of the pre-commit pass then shows the fix is wrong, and the loop stops blocked. The PR head still has the defect, but T shows as resolved with a fix claim, and the old head's CI can still be green. The readiness gate "Active review threads: zero" (readiness-gates.md 87) only counts unresolved threads, so it can't catch this. Neither can the helper's unresolved-thread count. On resume, loop-control.md 35 ("Do not ... repost replies just because context reset") pushes the agent to leave the stale reply alone.
- **Risk**: A human could merge the PR, or drop a requested change, based on a resolved thread whose fix never reached the PR. Replying this way also breaks the rule "Remote evidence for the pending commit is deferred until publication, not passed."
- **Suggested Fix**: Post fix replies and resolve threads only after the fix commit is the verified pushed PR head, and cite that SHA. Until then, leave the thread open and keep the planned reply in the checkpoint's `thread response` field. Rebuttals and triage replies that don't depend on unpublished code can still go out at any time. Also add an acceptance row: "Pre-commit pass changes or abandons a fix that answers a thread | Thread stays open; reply once the pushed head has the final fix."

### Issue 2: An empty manifest widens the prescribed diff to earlier review verdicts
- **Severity**: Medium
- **File**: .github/skills/synapseml-pr-loop/references/loop-control.md; .github/skills/synapseml-pr-loop/SKILL.md
- **Line(s)**: loop-control.md 109-110, 118, 126-127, 139-140, 175; SKILL.md 219-222
- **Description**: The contract drops any path whose content matches the merge base (loop-control.md 109-110 and 175). It has no rule for the case where every product path drops out. With zero manifest paths, the prescribed command `git diff --cached --binary --no-ext-diff --no-textconv <merge-base-sha> -- <manifest-paths>` (line 118) runs with an empty pathspec. Git then diffs every path. The only changed paths left are the allocated review outputs, so reviewers get the earlier verdicts. SKILL.md 221-222 forbids exactly that. After that, the post-commit equality check (126-127) and the artifact-only exception (139-140) both pass trivially on the `[]` fingerprint. Both probes reproduced this:
  - A fix reverts the only product path. The manifest becomes `[]`, and the prescribed command outputs only a probe `reviews/pr-9/` artifact that holds an earlier `ISSUES_FOUND` verdict.
  - The target lands the same fix independently. A routine `git rebase` drops the PR's product commit, and only the artifact commit is left ahead of the target. The manifest becomes `[]`, the prescribed diff again holds only the earlier artifact, and the `--name-status -z --no-renames` set equals the manifest plus the allocated outputs.
- **Risk**: A superseded or no-op PR uses up a gauntlet pass reviewing review text. The final-pass cap is 2. This breaks the independence rule. It can also produce a "clean" six-round pass and a matching manifest for a PR that contains nothing but review artifacts. At that point only judgment on the value gates stands between the PR and a readiness claim. The mechanical review and manifest gates won't stop it.
- **Suggested Fix**: Treat an empty manifest as a hard stop, and never run the prescribed diff with an empty pathspec. Record that the PR has no reviewed content and move to reconcile, where the agent either closes the PR as superseded (with authorization) or reports it as a no-op. Also add an acceptance row: "Every product path reverts or is upstreamed | Empty manifest; do not start a gauntlet; reconcile as superseded or no-op."

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Limitations

- This covers round 3 only. It is not a six-round pass, and it is not the final CI-qualified pass.
- I had no remote access, so I didn't check the live target, the PR 2737 threads and reviews, or CI. The pending edits still need new-head remote CI.
- I didn't open earlier review artifacts or the out-of-scope linked references (contributor safety, branch context, CI triage, PR writing guide, helper scripts). Behavior that depends only on those files wasn't evaluated.
- This is a tabletop trace of a manual workflow. The only checks I actually ran were the manifest recomputation, the whitespace check, and the two disposable Git probes.
