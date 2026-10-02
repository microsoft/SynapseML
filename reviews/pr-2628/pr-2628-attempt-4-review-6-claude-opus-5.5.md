# PR 2628, attempt 4, round 6

> **Current status, from the latest recheck at the end of this file:**
>
> - All review findings are closed: R3-1 to R3-5, R6-1 and R6-2.
> - Publication of `1.2.0` is still **blocked** by a pre-existing, unresolved consumer-wheel distribution issue. It is described below under "Release blocker, separate from these findings".
> - The automation PR stays unmerged.
> - This is not a production-readiness claim.
>
> The original round-6 result follows unchanged.

**Result: R3-1 to R3-5 are closed. One new low finding, R6-1, is a gap in the post-publication merge runbook. It needs documentation, not code.** Nothing here blocks package publication.

- Theme: edge cases and robustness, closing round 3, plus polish. Model: `claude-opus-5.5`.
- Base HEAD: `a617374f54c7629da3216112ce8c1196bd735224`, the same as round 3.
- Scope: the frozen dirty tree, limited to:
  - the new primary-integration helper and CLI in `scripts\release\release_guard.py`;
  - its wiring in `.github\workflows\release-notes.yml`;
  - the operator guide, `scripts\release\README.md`, and `.github\skills\synapseml-release\SKILL.md`;
  - the runtime fixes for R3-2, R3-3 and R3-4.
- Freeze: the 16 reviewed source, workflow, documentation and test files had identical hashes before and after this review.
- Not re-audited: unchanged runtime that earlier rounds closed.

## Closure of round-3 findings

### R3-1: closed

- **The guard.** `scripts\release\release_guard.py:227-275` checks, in order:
  1. The tag and commit formats, before any I/O.
  2. A canonical origin (`:232-238`).
  3. Complete, non-shallow history (`:239-242`).
  4. The peeled remote tag.
  5. It then fetches `master` and the tag with `--no-tags`.

  The guard accepts plain ancestry from the fetched `master`. The ancestry check (`:159-180`) uses `--no-replace-objects` and refuses any exit code other than 0 or 1.
- **The pull-request fallback.** Without ancestry, the guard looks up pull requests (`:183-224`):
  - The call pins the host and the REST API version and projects only the needed fields.
  - It has a 60-second timeout, a 60,000-byte limit and a 100-record limit, and parses strictly.

  A record passes (`:256-271`) only when all of these hold:
  - it is closed and merged;
  - its head is the tagged commit, on `release-candidate/<tag>-master`;
  - both its head and base are the canonical repository, and the base is `master`;
  - its number is a positive integer;
  - its 40-hex merge commit is an ancestor of the fetched `master`.
- **Git calls.** They now stop after 180 seconds with fixed, non-echoing advice (`:103-118`).
- **CLI.** The CLI (`:455-458,483-489`) only prints JSON.
- **Workflow.** `.github\workflows\release-notes.yml`:
  - adds a read-only pull-request scope (`:37-39`);
  - keeps full history (`:54`);
  - runs the check against the dispatched tag's `HEAD` (`:68-75`), before artifact verification (`:77`).
- **Tests.** `scripts\release\test_release_guard.py:103-301` covers:
  - merge, squash and cherry-picked rebase;
  - unbound proofs and unmerged candidates;
  - non-canonical tags and origins;
  - bad service data;
  - the pins and projection;
  - non-echoing timeouts;
  - invalid inputs, which do no I/O;
  - shallow history;
  - the CLI.

  `scripts\release\test_release_workflows.py:31-35` covers step order and scope.
- **Not verified here.** That the endpoint returns merged pull requests rests on GitHub's public REST documentation and a read-only probe that the parent reported.

### R3-2: closed

`scripts\release\bootstrap_release.py:365-369` catches the dispatch timeout. It raises a fixed message, "may have been accepted; inspect workflow runs", using `from None`, so it echoes no command, plan or approval. `main` (`:413-420`) omits tag guidance for dispatch.

Tests: `scripts\release\test_release_bootstrap.py:382`, and `:326`, which checks that the host stays pinned despite an inherited `GH_HOST`.

### R3-3: closed

`scripts\release\release_config.py:28-34` converts `RecursionError` to a fixed `ValueError` that does not include the payload. The plan, evidence, external-profile and Maven-admission paths inherit this behavior.

Tests: `scripts\release\test_release_config.py:104,114`, `scripts\release\test_release_bootstrap.py:361` and `scripts\release\test_release_public.py:199`.

### R3-4: closed

`scripts\release\verify_release.py:507` binds the Internal repository from the approved plan's profile, and binds none for public plans.

Test: `scripts\release\test_verify_release.py:690` changes the loaded profile and checks that the approved repository is kept.

### R3-5: closed

Skill step 8 (`.github\skills\synapseml-release\SKILL.md:44-47`) and the guide (`scripts\release\README.md:298-308`) now cover:

- the order: the lock follow-up comes after verified publication and after the primary documentation reaches `master`;
- that the strict `master` website check is expected to fail until then;
- not opening the follow-up against older `master` metadata;
- confirming the deployment;
- the preview command.

The optional clearer failure message was not adopted, which is acceptable.

### Round-3 residuals

- **Git timeouts.** `_git` is now bounded at 180 seconds, so this residual is closed.
- **Shallow-clone recovery.** Closed in the guide (`scripts\release\README.md:169`).

## New finding

### R6-1. Low: the tagged primary candidate's head can change before merge, and the runbook gives no merge order or recovery

Status: closed by the documentation update. See the docs-only recheck at the end of this file.

Severity: low. It is medium if the `master` rules require branches to be up to date before merging, which is unverified.

Locations:

- `scripts\release\README.md:270-275`;
- `.github\skills\synapseml-release\SKILL.md:38-42`;
- `scripts\release\release_guard.py:256-275`;
- general guidance that conflicts with this: `.github\skills\synapseml-pr-loop\SKILL.md:59` and `.github\skills\synapseml-pr-loop\references\readiness-gates.md:10`.

The guard correctly requires the merged pull request's final head to equal the tagged commit. Tags cannot move, and the notes workflow runs from the tag. So for `1.2.0`, any change to the primary candidate's head after tagging permanently removes the guarded notes path.

The window for such a change is long, because the guide merges the candidate only after publication. Meanwhile, `master` can move. Several ordinary actions change the head without the amend, rebase or force-push that the guide forbids:

- **GitHub "Update branch".** It merges `master` into the candidate. It is needed when the candidate conflicts with newer `master` changes, or when a rule requires up-to-date branches.
- **The repository's PR-loop guidance.** It rebases ordinary PRs and treats a PR that is behind its target as not ready.
- **Merging the automation PR first.** If that PR changed after tagging, the candidate's copies of the same files conflict with it.

The guide says to "merge the automation and the unchanged primary candidate", with no order and no fallback. The refusal message then asks the operator to merge the unchanged candidate, which may no longer be possible.

Repro: a temporary test outside the repository, using local Git, the guard called in-process and a stubbed PR record. All 3 cases passed.

1. Tag the candidate, then commit a conflicting edit to the same line on `master`. Squash-merging the unchanged candidate fails with a conflict.
2. Merge `master` into the candidate (as "Update branch" would), squash-merge that new head, and supply a closed, merged record whose head is the updated commit. The guard refuses with "cannot confirm primary released source is integrated into master".
3. Instead, land a separate reconciliation commit on `master`, then squash-merge the unchanged candidate. The guard accepts it as a merged PR.

Fix. This needs documentation only; the guard should stay strict.

- Merge the unchanged primary candidate first, promptly after publication. Reconcile the automation PR afterwards.
- Never use "Update branch" on a tagged candidate, and never rebase it or push to it. If it conflicts, land a separate reviewed reconciliation PR on `master` so that the unchanged candidate merges cleanly.
- Before bootstrap, confirm with a read-only check that the `master` rules let a behind but unchanged candidate merge. If they do not, settle how it will be merged before tagging.
- Exempt tagged candidates from the PR-loop "rebase" and "not behind" gates, or say in the release guide that those gates do not apply to them.
- Document what the release owner does if the guard refuses after an accidental update.

## Polish, non-blocking

- **Uncaught deep nesting.** `release_ops._json` (`scripts\release\release_ops.py:104-112`) and bootstrap `github()` (`scripts\release\bootstrap_release.py:31-45`) still let a `RecursionError` escape. Their inputs are local state written by the tool, or GitHub responses. The failure is closed.
- **Different origin sets.** The guard accepts an `ssh://` origin that bootstrap's `check_origin` (`scripts\release\bootstrap_release.py:48-55`) refuses. Both fail closed, but the difference is confusing.
- **Shallow-history refusal.** The refusal names no command. The notes section of the guide could repeat the unshallow step.
- **Missing merge commit.** If a matching record's merge commit is missing locally, the guard raises instead of trying later records. This fails closed, and happens only after every other field has matched.

## Evidence and limits

- `python -m pytest` on `scripts\release\test_release_guard.py`, `test_release_bootstrap.py`, `test_release_config.py`, `test_release_public.py`, `test_verify_release.py`, `test_release_workflows.py` and `test_public_release_docs.py`: **280 passed**. Run on Windows with native Git, a neutral Git configuration and `PYTHONUTF8=1`.
- R6-1 repro: 3 passed. The temporary file was outside the repository and was removed afterwards.
- The parent's own results are not claimed here: the Linux, native and bootstrap runs, the focused checks, the final frozen suites and the live probe.
- No external API calls, pushes, tags, approvals or packages. No product code changed.

Unverified:

- actual CI, candidate PRs and their heads, and hosted website runs;
- the associated-PR endpoint's behavior;
- the `master` merge rules;
- publication, artifacts and evidence size;
- that the candidates receive this frozen patch before tagging.

This is one model's bounded round 6. It is not multi-model coverage, and it makes no claim about production readiness.

## Docs-only recheck: R6-1 closure and current status

Scope: `scripts\release\README.md` and `.github\skills\synapseml-release\SKILL.md` after the R6-1 update. No code was rechecked. The findings above are left as written.

### R6-1: closed

Each fix item now appears in both documents:

| R6-1 fix | Guide | Skill |
| --- | --- | --- |
| Read `master`'s rules, read-only, before bootstrap. Unreadable rules, or rules that require an up-to-date head, block bootstrap, and protection is not changed | `:140-144` | step 2, `:35-37` |
| Merge the unchanged primary candidate first, promptly after publication, and reconcile the automation PR afterwards | `:146-147,154-156,286-287` | step 6, `:46-48` |
| Never use "Update branch", amend, rebase, force-push or add commits on a tagged candidate. The PR-loop rebase and not-behind gates do not apply to it | `:147-149` | rules, `:25-26` |
| Resolve conflicts with a separate reconciliation PR on `master` | `:149-151` | `:50-51` |
| Fallback: keep notes unpublished, preserve tags and artifacts, and escalate to the release owner. Never move tags or republish to make notes pass | `:156-159` | `:51-52` |

The parent ran a read-only check of the repository settings; it was not independently verified here. It found:

- no classic protection rule on `master`;
- that the required status checks in the active ruleset do not require an up-to-date branch.

So a behind but unchanged candidate can currently merge. No settings were changed.

Residuals, non-blocking:

- **The rules can change later.** They can change between the read before bootstrap and the merge after publication. Re-reading them just before the merge would catch this, and the documented fallback already fails closed.
- **Port candidates.** The reconciliation text names `master`. A conflicting port candidate would need the same treatment on its own port branch.
- **The PR-loop skill.** It has no cross-reference to the exemption. An agent that loads only the PR-loop skill for a candidate PR could still apply its gates. A one-line note in its readiness gates would help.

### R6-2. Low: in both documents, the new consumer-wheel gate comes after tag creation

Status: closed. See the ordering recheck at the end of this file.

Locations: `scripts\release\README.md:112-121,326-333`; `.github\skills\synapseml-release\SKILL.md:32-42`.

**The problem.**

- The guide's new gate (`:328-329`) says to prove the consumer wheel on every advertised runtime "before approving production tags". But the bootstrap steps that request approval and create the tags (`:112-121`) come about 200 lines earlier, and they do not mention the gate.
- The skill introduces bootstrap in step 2 and binds tag commits in step 3, but places the wheel proof in step 4.

**The consequence.** An operator or agent who follows either document in order can create the immutable `1.2.0` tag family before the gate runs. Suppose the gate then requires a change to the primary Python code, such as a shared-wheel backport. The tagged source cannot carry that change without moving tags, so `1.2.0` could not ship the fix.

**Fix.**

- At README `:112`, require the Python distribution readiness gate before requesting bootstrap approval.
- In the skill, move the wheel proof into step 2 before bootstrap, or state in step 4 that it must come before bootstrap.

### Release blocker, separate from these findings

This blocker is pre-existing and parent-reported; it was not independently verified here.

- The primary wheel's hand-written Python differs from the ports' in 19 of 21 files, and packaging includes only the current branch's code.
- Replaying the primary `ImageTransformer.toNDArray` on Spark 4.1-style image bytes raises `ValueError`. The port's version works.
- The consumer docs point every runtime at the primary PyPI wheel.

What follows:

- `1.2.0` publication stays held until there is a wheel strategy and a consumer proof.
- Choosing between per-runtime wheels and a shared-wheel backport needs the user's decision. This review infers no approval and no new destination.
- The automation PR stays unmerged, and the release stays blocked.

### Evidence and current status

- `python -m pytest scripts\release\test_public_release_docs.py`: **13 passed** on the updated documents.
- Reported by the parent and not claimed here:
  - the frozen release and CI suites: 1,181 passed, 1 opt-in test skipped, 63 subtests;
  - one real JDK 11 SBT test;
  - the clean bump suite of 270 tests.
- No API calls, pushes, tags, approvals or packages. No product code changed.

Current status:

- R3-1 to R3-5 and R6-1 are closed.
- R6-2 is open; it needs a documentation change only.
- The release is blocked by the consumer-wheel issue.

Actual CI, candidate PRs, repository rules and artifacts are unverified. This is not a production-readiness claim.

## Ordering recheck: R6-2 closure

Scope: `scripts\release\README.md`, `.github\skills\synapseml-release\SKILL.md` and the one new documentation test. Nothing else was reviewed, and the text above is left as written.

### R6-2: closed

- **The guide's gate is at the top.** The consumer-wheel gate is now at `:7-11`, immediately after the introduction. That is before `## Safety and public information` (`:21`), `## 1. Preview and prepare source` (`:41`) and every tag-creating command. It covers requesting approval and any tag-creating operation, including the normal preparation path in section 1.
- **The merge-rules check precedes bootstrap.** The pre-merge merge-rules block (`:90-94`) now comes before all bootstrap commands (`:113,120,130`).
- **The pre-tag check is not circular.** `:332-341` separates two checks:
  - before tagging, the planned packaging and runtime behavior are checked from the pinned candidates;
  - after the build, the final wheel's contents and hashes are verified.

  The pre-tag check therefore does not need published bytes.
- **The skill matches.** Step 2 (`:32-35`) places the gate before the bootstrap entry point (`:37-38`) and the merge-rules check (`:39-41`). Step 4 (`:44`) no longer contains the gate.
- **A test enforces the order.** `scripts\release\test_public_release_docs.py:42-54` asserts four orderings:
  - the guide gate comes before `## 1.`;
  - the guide gate comes before the approved dispatch;
  - the merge-rules check comes before the approved dispatch;
  - the skill gate comes before the bootstrap entry point.

  Each marker phrase occurs once, and a missing phrase also fails the test.

A red check, run in memory on the file contents without writing anything, confirms that each of these changes makes the test fail:

- moving the guide gate to the end;
- moving the merge-rules block after the approved dispatch;
- moving the skill gate back to step 4.

Evidence:

- `python -m pytest scripts\release\test_public_release_docs.py`: **14 passed**.
- The new test alone: **1 passed**.
- The parent's pinned formatter and test runs are not claimed here.

### Release blocker: unchanged, pre-existing and unresolved

This blocker was reported by the parent and was not independently verified here.

- The primary wheel's hand-written Python differs from the ports' code.
- The primary `ImageTransformer.toNDArray` fails on Spark 4.1-style image bytes.
- Consumer docs point every runtime at the primary PyPI wheel.

What follows:

- `1.2.0` publication stays held until the user chooses a distribution strategy and a consumer proof succeeds. This review infers no approval and no new destination.
- The automation PR stays unmerged, and the release stays blocked.

### Final current status

- **Review findings:** R3-1 to R3-5, R6-1 and R6-2 are closed. The non-blocking residuals listed above remain.
- **Release:** blocked by the consumer-wheel distribution issue.
- **Unverified:** actual CI, candidate PRs, repository rules and artifacts.
- **Actions:** this review made no API calls, pushes, tags, approvals or packages, and changed no product code.
- **Not claimed:** multi-model coverage or production readiness.
