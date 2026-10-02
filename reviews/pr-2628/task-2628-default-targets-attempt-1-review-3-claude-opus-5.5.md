# Default release targets: round 3 edge cases and robustness

Theme: edge cases, failure paths, boundary inputs and partial state (round 3).
Reviewer: Claude Opus 5.5 (`claude-opus-5.5`).

Reviewed HEAD: `2391aa166ab46238307de9edf9650102cbff950e`.
Master base (confirmed merge-base): `681bd96990c421de3b91d2b1bf8f8f470764199d`.

Scope: the current tracked unstaged delta plus untracked
`scripts/release/test_release_defaults.py`. The delta is 24 release files; the
review index `reviews/pr-2628/README.md` is also modified but is bookkeeping
only. The review covered:

- default selection compared with saved-plan identity;
- guard and notes defaults, and the optional-target policy;
- bootstrap selection and executor policy, including legacy ledgers;
- tag-dispatch inclusion and selected-port summaries;
- how version bumps preserve the optional Spark 4.0 references and the pinned
  website metadata.

The committed branch-to-master delta and unchanged runtime paths were not
re-reviewed.

Method: source reading plus bounded local reproductions in disposable copies
outside the repository. There were no network or service calls, and no tags,
packages, commits or pushes. The only repository write is this report.

## Result

There are two open findings. Neither affects the default two-target path. Both
concern the explicit Spark 4.0 opt-in, and neither can create an incorrect tag
or package.

| ID | Severity | Area |
| --- | --- | --- |
| DT-R3-1 | Medium | Order and file list for the Spark 4.0 opt-in documentation update |
| DT-R3-2 | Low | Opt-in port-PR "chain" wording and recovery of Spark 4.0 tags |

### DT-R3-1 (Medium): the documented opt-in update cannot be run as written

`scripts/release/README.md:111-116` says to update the Spark 4.0 examples in
`README.md` and `docs/Get Started/Install SynapseML.md`, and `spark40Version` in
`website/src/installArtifacts.js`, "before generating its release snapshot".
That instruction has two problems.

1. **Order.** The snapshot comes from the same bump that the pre-update breaks.
   `scripts/bump-version.py:788` counts every new-version string in each changed
   file, and lines 797-804 require that count to equal the number of
   replacements. References already set to the new version are counted even
   though the bump did not replace them, so the run aborts after every write has
   completed. There is also no point at which the update could be made:
   - `.github/workflows/release-prepare.yml:150-161` runs the bump and the
     snapshot in a single step.
   - Pre-setting the references on master instead fails the website check,
     because the version is not yet listed in `website/versions.json`.

   Reproduction, using a disposable copy of `README.md`, the Install guide,
   `installArtifacts.js` and `docusaurus.config.js` at version 1.1.3:
   1. Set the 21 Spark 4.0 coordinate, tag, Python and `spark40Version`
      references to 1.2.0.
   2. Run `scripts/bump-version.py --to 1.2.0 --skip-docs`.

   The command exits 1 with `Post-condition violated: expected 55 new-version
   occurrences, found 76`, and all four files have already been rewritten. The
   recovery it prints, `git checkout .` (`scripts/bump-version.py:803`), would
   also discard the manual Spark 4.0 edits.

2. **File list.** `website/test/installDocs.test.js:321-325`, `327-336` and
   `346-349` require `installArtifacts.spark40.coordinate`, or its deep-learning
   form, in these files:
   - `docs/Explore Algorithms/Deep Learning/Getting Started.md:36`
   - `docs/Explore Algorithms/Deep Learning/ONNX.md:48`
   - `docs/Reference/R Setup.md:69`
   - the versioned copies of Getting Started and R Setup

   Default bumps leave the pinned Spark 4.0 coordinate in these guides, and the
   README does not list them. An operator who updates only the three listed
   files therefore fails website CI.

**Impact:** fail-closed, either an abort or a CI failure, and nothing incorrect
is published. However, the only documented opt-in procedure fails, and the
recovery it suggests loses work.

**Suggested fix:**
- Document an order that can actually be run:
  1. Bump with `--skip-docs`, or prepare with `skip_docs`.
  2. Update every Spark 4.0 reference, including the specialized guides.
  3. Run the documentation steps that `--skip-docs` omits, as listed in the
     script's help: `sbt convertNotebooks`, `docusaurus docs:version`, then
     `--finalize-docs`.
- Alternatively, make the new-version post-condition subtract a pre-bump
  baseline.
- Add a regression test for an optional target that was already updated.
- List every coupled file, or name the website test as the authority.

### DT-R3-2 (Low): opt-in "chain" wording and recovery of Spark 4.0 tags

This finding comes from source reading; the Bash steps were not executed here.

- **The default run always comes first.**
  `.github/workflows/release-prepare.yml:338-342` always dispatches
  `release-tag.yml` without inputs. The resulting default run handles Spark 4.1
  only and rebases it directly onto the primary release commit
  (`.github/workflows/release-tag.yml:217-218`). `scripts/release/README.md:104-105`
  also requires that run to finish before the explicit dispatch.
- **Spark 4.1 is never restacked.** The explicit `include_spark40=true` run
  processes Spark 4.0 first. When it reaches Spark 4.1, it finds the default
  run's PR and either:
  - leaves the open PR untouched (`release-tag.yml:296-305`), or
  - only reconciles tags if that PR has merged (`release-tag.yml:312-331`).

  In the documented order, therefore, Spark 4.1 is never rebuilt on top of the
  Spark 4.0 release branch. Even so, `scripts/release/README.md:103` and
  `release-tag.yml:4-6` still describe a `master -> spark4.0 -> spark4.1` chain.
- **Spark 4.0 tags are not recovered by default runs.** A later default dispatch
  or rerun processes only Spark 4.1 and succeeds without checking Spark 4.0.
  `release-tag-spark.yml` still creates the `v<version>-spark4.0` and
  `v<version>-python3.12` tags when the Spark 4.0 PR merges. If that step fails,
  only another explicit `include_spark40=true` dispatch repairs the tags.
  `release-tag.yml:7` says reruns recover missing derivative tags, without that
  qualifier.

**Impact:** functionally safe. The result is two independent port PRs, each
based on the primary release commit. Spark 4.1 carries its own port commits, and
the merge-order text in each PR matches its actual base. The risks are operator
confusion and a missed repair of the Spark 4.0 tags.

**Suggested fix:**
- Reword README line 103 and the workflow header: an existing Spark 4.1 PR is
  preserved, not restacked. Close it first only if a stacked chain is intended.
- Document that recovering Spark 4.0 tags requires `include_spark40=true`.

## Verified without findings

- **Plan identity.**
  - Building `1.2.0` with fixed synthetic commits using the committed HEAD
    matrix code, whose default selected all three targets, gives the same
    digest as the current explicit three-target plan:
    `2872f6280e4022bb5c86cf46c0eaae88b1fd4b3c96f231d31ed7a6bdc8f14443`.
    The current default pair gives a distinct ID.
  - A plan document generated by the HEAD code reloads unchanged through the
    current `load_plan(require_bound=True)`. That loader re-derives the plan from
    the saved `targets` list and never consults the defaults. Removing Spark 4.0
    from the saved plan is refused as a digest mismatch.
  - Commit bindings for unselected targets, duplicate targets and an empty
    `--targets` are all refused.
- **Guard.**
  - `full-release` accepts only `true` and `false`, and refuses an include
    together with a veto. The prepare workflow omits `--include-spark40`, which
    defaults to `false`.
  - `notes_plan` refuses any plan that lacks the default pair. Bootstrap calls
    it first, so a master-only plan cannot be bootstrapped.
  - Installation rows come from the validated plan.
  - `--installation-output` is created exclusively, and only after the evidence
    validates. If the file already exists, the command returns 2 and leaves the
    file unchanged.
  - The guard and publish steps run in one job, so the temporary file stays
    available.
- **Bootstrap.**
  - Policy, remote refs, runtime, candidate CI and tags are checked only for
    selected targets. Without Spark 4.0, the policy is never read.
  - The preview writes nothing.
- **Executor.**
  - The optional-target policy is required only for full plans that select
    Spark 4.0, and it is recomputed before every queue.
  - Legacy full ledgers with `required=true` still resume.
  - Plans that select Spark 4.0 still reject a waived policy.
- **Tag orchestration.** `INCLUDE_SPARK40` comes only from the dispatch input and
  is false on a tag push. Repository variables cannot enable it.
- **Version bump.**
  - Repeated default bumps from 1.1.3 to 1.2.0 to 1.3.0 on CRLF copies behave
    as intended:
    - `README.md` and the Install guide each keep all 10 Spark 4.0 references,
      and `spark40Version` stays at 1.1.3.
    - Spark 4.1 and the primary version advance.
    - No bare LF line endings are introduced.
  - The publication lock and `website/versions.json` stay outside the bump.
  - The post-condition re-analysis applies the same skip rules as the bump.
- **Website.** `validatePublicationLock` requires the pinned Spark 4.0 version to
  be a listed documentation version. Outside preview, it applies a strict lock
  per port.

## Notes (not findings)

- The workflow expression, the executor and bootstrap each parse `SKIP_SPARK40`
  differently. That predates this change, which only narrows the veto to runs
  that select Spark 4.0.
- Default plans still serialize the catalog `base_branch` for Spark 4.1. It has
  no behavioral effect, and keeping it preserves existing plan identities.
- `scripts/release/test_release_defaults.py` is untracked. It must be committed
  before `pytest scripts/release` in `.github/workflows/pr-validation.yml:79` will
  run it.

## Verification

Run here:
- 251 tests passed across `test_release_defaults.py`, `test_release_matrix.py`,
  `test_release_guard.py`, `test_release_public.py` and
  `test_release_bootstrap.py`, using native Git with no network.
- 9 tests passed in `website/test/installDocs.test.js` under Node.
- The two bump reproductions, the golden-digest check and the saved-plan checks
  above.

Reported by the coordinator and not rerun here:
- 863 release tests passed, with one unchanged opt-in SBT skip.
- 271 native bump and history tests passed.
- 36 Node website tests passed.
- Round 1 reported no outstanding findings.
- The round 2 model failed before reviewing; the coordinator's fallback is
  documented separately.

## Limits

The following were not verified here:
- actual GitHub Actions, Azure and hosted-artifact behavior;
- the Bash workflow steps (tag recovery and summaries), because no Bash runtime
  was usable;
- the full website build.

This report does not claim production readiness or authorize a release. It
covers round 3 only; round 6 follows.

## Coordinator resolutions

Both findings were addressed before round 4. The original findings above
are retained.

**DT-R3-1: resolved.** The operator guide now separates the source bump with
`--skip-docs` from the optional-runtime edits and subsequent conversion,
snapshot and finalization commands. It names the specialized Deep Learning,
ONNX and R guides, supplies a source-reference search, and identifies the
website contract test. The bump post-condition also counts references already
at the destination version before writing, so pre-updated optional references
no longer produce the reproduced false failure. Write and post-condition
errors no longer suggest discarding the entire worktree.

The real CLI regression now uses seven source files, exercises both an
unchanged optional runtime and a pre-updated one, and performs two successive
bumps plus no-write previews. The default and pre-updated cases passed.

**DT-R3-2: resolved.** The workflow header and guide distinguish a fresh
stacked chain from the usual existing Spark 4.1 PR, which remains unchanged.
The guide requires `include_spark40=true` on every dispatch intended to repair
Spark 4.0 tags. A real local Git/Bash regression preserves a reviewed default
Spark 4.1 PR while including Spark 4.0, demonstrates that a default rerun does
not repair its missing Python tag, and verifies that explicit inclusion repairs
the tag without restacking or prematurely tagging Spark 4.1.

Resolution checks run by the coordinator: nine focused bump, recovery and
summary tests passed, nine installation-documentation tests passed, and
18 public-documentation checks passed. No production operation was performed.
