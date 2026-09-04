## Review Summary

- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: gpt-5.6-sol
- **Artifact**: C:\Users\singhrana\.copilot\session-state\16e6d9b2-ce73-41f9-9d38-9386edc5c48d\files\direct-pr-2628\reviews\release-skill\task-2628-attempt-1-review-1-gpt-5.6-sol.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist

- [x] The GitHub PR API still resolves PR #2628 to `master`, its head SHA
      matches the local checkout, and the base is an ancestor of the reviewed
      commit.
- [x] The complete current tracked and untracked change set and the existing
      Round 1 artifact were reviewed without modifying implementation files.
- [x] All seven prior resolutions were checked against `SKILL.md`, its three
      references, `release_matrix.py`, `verify_release.py`,
      `bump_bbcvhd.py`, `bump-version.py`, and the four release workflows.
- [x] The combined version-bump and focused release suites passed all 304
      tests.
- [x] `test_prev_tag.sh` passed all eight tag-history checks from the full
      worktree clone.
- [x] `bump-version.py --to 1.1.4 --dry-run` exited 0, printed both
      documentation commands without an error, and left tracked and untracked
      review inputs byte-for-byte unchanged.
- [x] Direct matrix probes confirmed that Internal-only plans disable OSS
      publication, selected-target rebuild counters reach both UPack versions,
      and current or older OSS versions are rejected by the full-release bump
      helper.
- [x] The `SKIP_SPARK40` preflight reads the same repository variable consumed
      by `release-tag.yml`; the BBC-VHD preview passes both counters accepted by
      `bump_bbcvhd.py`.
- [x] `git diff --check`, all 37 local Markdown-link checks, and Black 26.5.1
      checks for both changed Python files passed.
- [ ] The repository-pinned Black 22.3.0 was not available in this environment;
      no Scala files changed, so SBT validation was not applicable.

## Prior Findings

### Issue 1: Approved scope and target selections are discarded after preflight

- **Severity**: High
- **File**: `.github/skills/synapseml-release/SKILL.md`
- **Lines**: 61-105
- **Description**: The preflight supports Internal-only and subset plans, but
  the main workflow unconditionally directs both through the full OSS Release
  Prepare, derivative-tag, public Maven, and GitHub Release sequence.
  Internal-only plans use an existing OSS version that Release Prepare rejects.
  Subset plans do not constrain `release-tag.yml` or the default target set in
  `release-notes.yml`.
- **Risk**: The Internal-only flow cannot pass Release Prepare. A subset flow
  can create tags and release PRs outside the approved plan, then fail the
  GitHub Release gate because unselected artifacts were not published.
- **Suggested Fix**: Separate full OSS and Internal-only tracks. Either carry
  subsets through every workflow or document them only for scoped
  publication, verification, and recovery after the full tag set exists.

### Issue 2: Recovery counters do not exist for Maven or pip

- **Severity**: Medium
- **File**:
  `.github/skills/synapseml-release/references/recovery-and-rollout.md`
- **Lines**: 13-18
- **Description**: The text groups Maven, pip, and UPack as immutable package
  families and tells the engineer to generate rebuild counters.
  `release_matrix.py` exposes counters only for OSS and Internal UPack.
- **Risk**: An engineer can retry the same immutable Maven or pip coordinate
  while expecting an UPack counter to change it.
- **Suggested Fix**: State that counters apply only to UPack. Document Maven
  and pip recovery separately, including when retrying the same coordinate is
  safe and when a new OSS version or Internal patch is required.

### Issue 3: The BBC-VHD preview command drops approved rebuild counters

- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/references/preflight.md`
- **Lines**: 76-80
- **Description**: The BBC-VHD preview omits both rebuild-counter options, so
  the script defaults them to zero even when the approved matrix uses rebuilt
  UPacks.
- **Risk**: The preview can name the original package instead of the approved
  rebuilt artifact.
- **Suggested Fix**: Pass both counters from the reviewed matrix and require
  the previewed values to match the target row before applying.

### Issue 4: A workflow opt-out can omit Spark 4.0 from a full release

- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/SKILL.md`
- **Lines**: 48-53, 95-104
- **Description**: The full-release track requires the complete target matrix
  and says the derivative workflow opens the Spark release PRs, but it never
  tells the operator to inspect or clear `SKIP_SPARK40`. In
  `.github/workflows/release-tag.yml`, a persistent repository variable with
  that name makes the automatically dispatched run omit the `spark4.0` PR.
  The matrix does not model that opt-out as a valid partial primary release.
- **Risk**: The reviewed version PR can merge and create the immutable primary
  tag before the operator learns that the Spark 4.0 tags and package inputs
  will not be produced. The full plan then remains incomplete, and Spark 4.1
  can be rebased directly onto the primary release with the workflow's own
  ancestry warning.
- **Suggested Fix**: Add a full-release preflight check that
  `SKIP_SPARK40` is false or unset before approval and merge. Document the
  workflow opt-out as recovery-only behavior, or make any intentional target
  omission explicit in a release track that all downstream steps support.

### Issue 5: The documented dry run reports an error but exits successfully

- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/references/preflight.md`
- **Lines**: 29-36
- **Description**: The reference says `bump-version.py --dry-run` prints the
  documentation commands. A normal checkout does not contain the ignored
  `website/docs/` output. `scripts/bump-version.py` checks for that directory
  before its Docusaurus dry-run branch, prints `ERROR: website/docs/ does not
  exist after convertNotebooks`, omits the Docusaurus command, and returns exit
  code 0 because the caller ignores both dry-run helper results. The documented
  command reproduced that behavior in this worktree.
- **Risk**: A human or automated preflight can record a successful exit even
  though one documented preview did not run and the command emitted an error.
  That makes the no-write gate unreliable and can hide an incomplete release
  preview.
- **Suggested Fix**: In dry-run mode, print the Docusaurus command before
  requiring generated docs, and add a fresh-checkout regression test. If
  generated docs are instead a prerequisite, make the command exit nonzero
  and document the prerequisite.

### Issue 6: Selected-target completion omits public artifacts

- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/SKILL.md`
- **Lines**: 162-165
- **Description**: The final paragraph applies one Internal-artifact
  completion rule to both Internal-only patches and selected-target recovery.
  A selected-target plan with the default `full` scope enables public and
  Internal package rows, and the same skill earlier requires every selected
  row to be present.
- **Risk**: An operator can treat a public package recovery as complete after
  checking only Internal artifacts, despite the selected public Maven, pip, or
  UPack row still being absent.
- **Suggested Fix**: Keep the Internal-only completion rule limited to
  Internal artifacts. For selected-target recovery, require proof for every
  selected public and Internal row, plus only the rollout steps that are
  actually in scope.

### Issue 7: Non-OSS tracks still run the full-release version-bump preview

- **Severity**: Medium
- **File**: `.github/skills/synapseml-release/SKILL.md`;
  `.github/skills/synapseml-release/references/preflight.md`
- **Lines**: 70-78; 40-50
- **Description**: After selecting a release track, the shared preflight still
  unconditionally requires `bump-version.py --dry-run`. Internal-only patches
  intentionally reuse an existing OSS version, and selected-target recovery
  can target an older published version. `bump-version.py` rejects both cases
  because its target must be strictly greater than the checkout's current
  version. In this worktree, the valid Internal-only matrix for OSS 1.1.3 and
  Internal patch 1 exited 0, but `bump-version.py --to 1.1.3 --dry-run` exited
  1; the historical recovery probe for 1.1.1 also exited 1.
- **Risk**: The documented Internal-only and selected-target tracks stop at an
  irrelevant full-release gate before their scoped validation can complete.
  An operator must either violate the exact workflow by skipping the gate or
  supply an unrelated newer OSS version.
- **Suggested Fix**: Limit the repository version-bump preview to full OSS
  releases in both documents. Give Internal-only and selected-target recovery
  explicit scoped preflight steps based on their matrix plan, existing tags,
  artifact verification, and BBC-VHD preview when applicable.

## Resolution Log

### Issue 1

- **Status**: Resolved
- **What changed**: `SKILL.md` now separates full, Internal-only, and
  selected-target tracks before the workflow instructions. Internal-only
  releases skip OSS preparation, public tags, public Maven, and GitHub Release
  creation. Selected targets are limited to recovery or verification after the
  full tag set exists.
- **Why**: Each track now has its own permitted actions. The skill no longer
  sends Internal-only or partial recovery work through the full public release
  path.
- **How verified**: The Internal-only plan disables every OSS pip and UPack
  publication flag, enables the corresponding Internal flags, and emits no
  public Maven commands. The focused release suite passed all 84 tests.

### Issue 2

- **Status**: Resolved
- **What changed**: `recovery-and-rollout.md` now limits rebuild counters to
  OSS and Internal UPack. It gives separate retry and version-bump rules for
  public Maven, public pip, Internal Maven, and Internal pip.
- **Why**: The recovery table no longer implies that Maven or pip accept UPack
  counters. It tells the operator when a retry is safe and when an immutable
  package requires a new version.
- **How verified**: A selected-target matrix probe changed the OSS and Internal
  UPack versions with their counters while leaving Maven and pip versions
  unchanged.

### Issue 3

- **Status**: Resolved
- **What changed**: Both BBC-VHD dry-run examples in `preflight.md` now include
  `--upack-iteration` and `--internal-upack-iteration`. The surrounding
  instruction requires the preview values to match the reviewed matrix row.
- **Why**: The BBC-VHD preview now checks the same package versions chosen
  during release planning, including rebuild counters.
- **How verified**: All documented flags match the helper CLIs, all 37 local
  Markdown links resolve, `SKILL.md` is 165 lines, and `git diff --check`
  passes.

### Issue 4

- **Status**: Resolved
- **What changed**: `SKILL.md` now requires a full release to confirm that
  `SKIP_SPARK40` is unset or false before the version PR merges.
  `preflight.md` provides a read-only `gh variable list` check and limits the
  opt-out to a recorded workflow replay after Spark 4.0 is complete for that
  version.
- **Why**: A persistent opt-out can no longer silently turn the documented
  full track into a partial release after the immutable primary tag exists.
- **How verified**: The documented check reads the same `SKIP_SPARK40`
  repository variable consumed by `.github/workflows/release-tag.yml`.

### Issue 5

- **Status**: Resolved
- **What changed**: `_run_docusaurus` now prints the planned command and
  returns before checking generated docs in dry-run mode. New unit and
  integration assertions require both documentation commands, no error, and no
  file changes. The version scanner also excludes committed review artifacts,
  which are immutable evidence rather than release content.
- **Why**: The no-write preflight now previews the command that would run after
  `convertNotebooks` creates `website/docs/`; it no longer reports a false
  prerequisite error with a successful exit.
- **How verified**: The full dry run exited 0, printed both documentation
  commands, emitted no error, and left the worktree unchanged. The combined
  version-bump and release suites passed all 304 tests.

### Issue 6

- **Status**: Resolved
- **What changed**: The completion section now gives Internal-only and
  selected-target recovery separate rules. Selected-target recovery must prove
  every public or Internal row enabled by the reviewed plan and repeat only
  affected rollout gates.
- **Why**: Completion now follows the generated plan instead of applying the
  narrower Internal-only rule to public package recovery.
- **How verified**: The completion rule matches the earlier requirement to run
  `verify_release.py` until every selected plan row is present.

### Issue 7

- **Status**: Resolved
- **What changed**: `SKILL.md` and `preflight.md` now limit
  `bump-version.py --dry-run` to full OSS releases. Internal-only and
  selected-target tracks instead confirm an existing complete tag set and run
  `verify_release.py` with the reviewed targets and source skips.
- **Why**: Tracks that reuse the current or an older OSS version no longer run
  a helper designed to accept only a newer repository version.
- **How verified**: The documented track split matches the helper's strict
  increasing-version check and the matrix's successful Internal-only and
  historical selected-target plans.

## Re-review Result

**CLEAN.** All seven prior resolutions match the current scripts and workflows.
The 304 focused tests, eight tag-history checks, direct CLI probes, formatting
check, and local-link scan found no remaining or new review issue.
