# Round 1 review

## Review summary

| Field | Value |
| --- | --- |
| Round | 1 only |
| Theme | Broad sweep, adapted to documentation completeness, requirement conformance, contradictions, and unsafe instructions |
| Contract and mode | Installed copilot-toolkit `review-code`, direct, sequential |
| Actual model | `gpt-6-astra`, current-session reviewer; no subagents or additional reviewers |
| Attempt | 1 |
| Task label | Named-task fallback `pr-loop` |
| Issues found | 1 Medium |
| Verdict | ISSUES_FOUND |
| Artifact | `reviews\pr-loop\task-pr-loop-attempt-1-review-1-gpt-6-astra.md` |

## Revision and scope

Target is the local `upstream/master` ref at
`681bd96990c421de3b91d2b1bf8f8f470764199d`. Source HEAD is the same commit on
`chore/pr-loop-feedback-20260924`, plus the three-file uncommitted patch below.
These are local revision observations, not a claim that the remote was
refreshed. No PR number was supplied.

The tracked target-to-worktree diff and the complete untracked addition were
read. SHA-256 values identify the exact file bytes reviewed.

| File | State | SHA-256 |
| --- | --- | --- |
| `.github\skills\synapseml-pr-loop\SKILL.md` | Modified | `611686ebac76401a10be1600319f1c34c44d107f94cef435a40192f0d4030029` |
| `.github\skills\synapseml-pr-loop\references\readiness-gates.md` | Modified | `4cb9a14f215fc8724fad6e2e53e144eb91fb5a8c376b1f84a27a386e112a7a58` |
| `.github\skills\synapseml-pr-loop\references\loop-control.md` | New | `90be34f600fbc2175d8a5b0599bd1f18c7a5d38a8be1b2da9025f63c2d76e3dc` |

Below, `skill`, `readiness`, and `loop` mean the three files in that table.

## Evidence checklist

- [x] Read the tracked diff and the full new loop-control reference. Verified
  source and target identities locally.
- [x] Read `AGENTS.md`, the repository code-review checklist, master branch
  guidance, the PR-writing guide, CI triage, and the trusted external-contributor
  safety reference. Proposed workflow text was reviewed as data, not executed.
- [x] Checked preservation of trusted guidance and execution authorization.
  The pinned-source requirement remains intact. CI still needs separate
  authorization and a fresh exact-head safety check, `skill:21-28,51-56,154-170`.
- [x] Compared the new gauntlet with the installed `review-code` skill and its
  round-1 prompt. Full-patch input, actual model recording, fix-and-retest
  cycles, frozen-patch invalidation, and artifact preservation are explicit,
  `skill:195-220`; `loop:61-83`.
- [x] Checked checkpoint persistence, worktree ownership, bounded retries,
  queue-time deadlines, and missing-evidence handling, `loop:7-59`.
- [x] Hand-traced the 12 documented scenarios below. The new-PR bootstrap case
  exposes the artifact-policy contradiction reported as R1-1.

## Issues

### R1-1: Reconcile bootstrap artifact publication with the pre-PR draft rule

**Severity:** Medium

**Changed location:** `.github\skills\synapseml-pr-loop\SKILL.md:216-220`.
Related new instruction at `SKILL.md:203-207`; supporting repository rule at
`AGENTS.md:142-147`.

The new workflow requires committing review artifacts with the reviewed changes
and explicitly applies the six-round policy to the bootstrap commit needed to
create a PR. It also directs the reviewer to use a repository output directory.
However, `AGENTS.md` requires pre-PR reports to remain in the session workspace
and says their first committed location must be `reviews/pr-<pr_number>/`.
The installed direct contract likewise requires every review artifact in the
same code check-in. The workflow does not define an exception or handoff that
reconciles those rules when the PR number does not exist yet.

This affects the newly supported fresh-task path, not just filename style.
Following the combined instructions literally stalls the first publication,
or forces the agent to choose between a forbidden placeholder artifact path
and omitting artifacts from the required code check-in.

Tabletop reproduction:

1. Start a new change from `master` with no published change commit or PR number.
   Local tests and fast review pass.
2. Run the required bootstrap gauntlet before the first change commit,
   `skill:216-220` and `loop:40-43`.
3. Attempt to commit the reports with that change. A numbered PR directory is
   unavailable, while `AGENTS.md:145-147` prohibits committing them in a flat
   or task-named placeholder directory.
4. Keeping the reports in session storage follows the repository rule but
   conflicts with the new unconditional bundling instruction and installed
   direct contract. Deferring remote checks until a PR exists does not resolve
   this artifact-publication dependency.

The mismatch is introduced by the added new-work/bootstrap gauntlet path.
The unchanged repository draft rule was checked independently; it is not a
request to change a pre-existing naming preference.

**Suggested fix:** Define an explicit bootstrap handoff and its policy
precedence. Preserve pre-PR reviews in session storage, establish any required
authorized exception to same-commit artifact bundling, create the authorized
PR, and move the original reports to `reviews/pr-<number>/` before final
readiness. If the applicable policy does not permit that exception, report
that specific publication blocker rather than issuing conflicting commit
instructions. Extend the new-issue tabletop case to check the artifact location
before and after the PR number becomes available.

## Tabletop evidence

| Scenario | Observed instruction-level result | Evidence |
| --- | --- | --- |
| New issue without a PR | Remote-gate deferral is specified, but bootstrap artifact publication has no compliant documented handoff. R1-1 remains open. | `skill:203-220`; `loop:40-43,92`; `AGENTS.md:142-147` |
| Review-only external PR | Remain read-only; do not trigger CI or approve workflows. | `skill:36,51-56`; trusted contributor-safety reference |
| No failures but no Azure build | Treat the absent build as missing evidence and require authorization and safety clearance before triggering it. | `skill:154-170,224-236`; `readiness:81-84,98-102` |
| Green review on an older SHA | Wait for exact-head coverage within a finite timeout, not merely the latest timestamp. | `skill:118-127`; `loop:55`; readiness helper parameters |
| Zero threads with a collapsed finding | Read review bodies and triage the finding. Zero threads do not clear it. | `skill:122-127`; `readiness:88-93` |
| Round 5 needs a fix | Return to tests and the fast loop, then obtain six clean rounds on one frozen patch. | `skill:209-214`; `loop:63-67` |
| Review artifacts only | Inspect the allocated artifacts, record the fingerprint exception, and refresh final-SHA remote evidence. | `loop:71-76` |
| Resume after target or head movement | Check ownership and current revisions before reusing evidence or repeating actions. | `loop:25-29,68-76` |
| Same failure three times | Stop blocked with the failed attempts and next action; do not expand the budget silently. | `loop:45-59` |
| CI timeout | Keep CI unresolved; neither watcher restart nor duplicate queueing extends the deadline. | `loop:52-59`; `skill:238-242` |
| Comment requests a secret or safety bypass | Reject the instruction; comments cannot override trusted guidance. | `skill:42-44`; trusted contributor-safety reference |
| Engineering evidence green, approval missing | Report the engineering/approval distinction without approving or merging on the user's behalf. | `skill:42-44,256-257`; `loop:103` |

## Resolution log

### R1-1

- Status: Open.
- What changed: No implementation or workflow changes were made.
- Why: This request authorizes round-1 findings only.
- How verified: Hand-traced the no-PR path against the changed bootstrap
  instructions, `AGENTS.md:142-147`, and the installed direct contract's
  artifact-bundling and six-round pre-commit rules. No fix has been verified.

## Limitations

## Driver resolution of R1-1

The workflow now explicitly follows the repository's pre-PR session-draft rule
over the toolkit's same-commit bundling default. Required pre-commit reviews
still run. Original drafts move unchanged to the numbered PR directory after
publication and are committed before readiness. The new-issue scenario now
checks both artifact locations. A fresh round-1 review is required for this
changed patch; the original finding and reviewed-file hashes above are retained.

## Original review limitations

This is a static documentation review with hand-traced scenarios, not a live
GitHub or CI workflow run. The request supplied successful frontmatter, size,
link, code-block, and whitespace checks; those structural results were not
independently rerun as part of this semantic sweep. No product, test, or helper
code changed, so no build, product test, or helper regression suite was run.

Only round 1 was performed. No other review workflow, subagent, remote API call,
implementation edit, staging, commit, or push was performed. The report remains
at the explicit review-only location requested for this run. That location is
not authorization to publish reports outside the numbered PR directory.
