---
name: synapseml-release
description: Plan, preview, validate, publish, recover, and monitor SynapseML releases. Use for version bumps, release tags, dry runs, Maven, pip, UPack, Publish-Official, SynapseML-Internal, BBC-VHD, ESRP, White-Glove, or Fabric release-train work.
compatibility: Requires a SynapseML checkout, Python, git, GitHub CLI, read access to GitHub Actions repository variables, and Bash for tag-history tests. Azure CLI and authorized ADO, Internal, or BBC-VHD access are needed only for their release steps.
---

# SynapseML release

Use this skill for the full release path, not only for creating tags or
packages.

## Rules

- Read [scripts/release/README.md](../../../scripts/release/README.md) before
  acting. It is the command-level source for the repository tooling.
- Load the [branch context skill](../synapseml-branches/SKILL.md). Start a
  primary release only from the canonical development branch named there.
- Derive versions, tags, coordinates, and queue commands from
  `release_matrix.py`. Do not copy them from an old release or type them from
  memory.
- Treat the reviewed version PR merge as a state-changing approval. Its merge
  starts primary and derivative tag automation.
- Before any state-changing step, require explicit human confirmation of the
  version, scope, targets, counters, and exact plan.
- Do not treat `skip_docs` as a dry run. It creates an incomplete version PR.
- A green publish pipeline does not prove packages exist. Run
  `verify_release.py` against every selected target and package family.
- Never move a published tag or reuse an immutable package version. UPack
  rebuilds use the matrix counters. Maven and pip recovery follows the separate
  rules in
  [references/recovery-and-rollout.md](references/recovery-and-rollout.md).
- Never print, store, or move release credentials into GitHub. ESRP, SAW,
  Internal, BBC-VHD, White-Glove, and train approvals remain human gates.

## Workflow

### 1. Resolve release state

1. Confirm the target branch, current version, proposed version, release scope,
   selected targets, Internal patch, and rebuild counters.
2. Fetch tags and inspect open or merged version and Spark release PRs.
3. Check whether the proposed primary tag, release branch, GitHub Release, or
   package versions already exist.
4. For a full release, confirm the `SKIP_SPARK40` repository variable is unset
   or does not equal `true` under GitHub's case-insensitive comparison.
5. Generate the text and JSON plans with `release_matrix.py`. Capture the plan
   as release evidence.

Read [references/preflight.md](references/preflight.md) for commands and
expected output.

### 2. Choose the release track

- **Full OSS release**: use the complete target matrix and follow every step
  below. Do not merge the version PR when any casing of `SKIP_SPARK40` equals
  `true`. That persistent opt-out would omit a required target after creating
  the primary tag.
- **Internal-only patch**: use an existing OSS release with
  `--scope internal-only`. Skip **Release Prepare**, public and derivative tag
  automation, public Maven, and **Release Notes**. Prepare, publish, and verify
  only the Internal plan rows with `verify_release.py --scope internal-only`
  before updating BBC-VHD.
- **Selected-target recovery**: use `--targets` only to recover or verify
  package rows after the full release tag set exists. The GitHub tag and
  Release workflows do not consume this selection, so it cannot approve a
  partial primary release.

Stop if the requested track cannot follow those boundaries. Do not widen a
reviewed Internal-only or selected-target plan to the full release flow.

### 3. Run the no-write preflight

1. Run the matrix generator. It is read-only.
2. For a full release, run `bump-version.py --dry-run`.
3. For an Internal-only patch, do not run the version bump. Capture both the
   OSS-base and Internal-only verification commands from `preflight.md`.
4. For selected-target recovery, do not run the version bump. Verify the
   reviewed scope and targets to record their current state.
5. Run the focused release script and workflow tests.
6. Replay `verify_release.py` against a documented known release and scope to
   confirm live source access and expected rows.
7. If BBC-VHD is in scope, run `bump_bbcvhd.py --dry-run` in an authorized
   checkout.

Stop if the plan, version replacements, existing tags, or package coordinates
do not match the intended release.

Use [references/preflight.md](references/preflight.md) for the exact commands,
track-specific checks, and expected output.

### 4. Create the reviewed version PR

This step applies only to a full OSS release.

Run the **Release Prepare** workflow on the canonical development branch with
the approved version. This is not a dry run. It:

1. validates the branch, version, existing tags, and existing release branch;
2. applies the version bump and documentation snapshot;
3. pushes a release branch and opens a pull request; and
4. starts the pull-request and website checks.

Review the complete diff. Do not merge a PR created with `skip_docs` until the
missing documentation snapshot is added and checked.

### 5. Merge and verify GitHub automation

This step applies only to a full OSS release.

After explicit human approval, merge the version PR. Confirm that automation:

1. tags the exact merge commit with the primary version;
2. creates the derivative tags for the canonical branch;
3. opens the Spark release PRs in their required order; and
4. creates the matching Spark and Python tags only after each Spark PR merges.

Do not infer success from a workflow summary. Compare each tag to its expected
commit and inspect every release PR. See
[references/automation-boundaries.md](references/automation-boundaries.md).

### 6. Publish Maven, pip, UPack, and Internal artifacts

Use only the queue commands emitted by the reviewed matrix plan.

1. Prepare and review SynapseML-Internal from the same JSON plan when it is in
   scope.
2. Queue each in-scope public and Internal Maven tag build.
3. Complete the required ESRP and SAW approvals.
4. Verify every in-scope Maven coordinate.
5. Queue Publish-Official for pip and UPack only after Maven verification.
6. Run `verify_release.py` until every selected row is present.

For an Internal-only patch, keep public publication disabled in the generated
plan. For selected-target recovery, use only existing reviewed tags and the
selected package rows.

The matrix prints commands but never queues a pipeline. Keep that human
authorization boundary.

### 7. Publish the GitHub Release

This step applies only to a full OSS release.

Run **Release Notes** with the primary tag selected. The workflow checks that
the tag belongs to the canonical branch and that public Maven and PyPI
artifacts exist before it creates the GitHub Release.

Do not create separate Releases for derivative Spark or Python tags.

### 8. Update BBC-VHD and monitor rollout

1. Preview the BBC-VHD edit with `bump_bbcvhd.py --dry-run`.
2. Apply it, review the exact three-line package and component revision change,
   and open the BBC-VHD PR.
3. Complete BBC-VHD CI and White-Glove approval.
4. Select the release train from the official schedule.
5. Monitor deployment and confirm the released version in a Fabric notebook.
6. Announce the release and update tracking work.

Read [references/recovery-and-rollout.md](references/recovery-and-rollout.md)
before rerunning a failed step or starting rollout.

## Completion

A full release is complete only when:

- every expected public and Internal tag points to the reviewed commit for its
  target;
- every selected Maven, pip, and UPack artifact exists;
- the primary GitHub Release exists with the correct comparison base;
- the BBC-VHD change has passed its required approvals;
- the selected train has deployed; and
- a Fabric runtime reports the expected SynapseML version.

For an Internal-only patch, require proof for every selected Internal artifact
and in-scope rollout step. For selected-target recovery, require proof for every
public or Internal row enabled by the reviewed plan. Repeat only the downstream
rollout gates affected by the recovered artifact. Neither track creates a new
primary OSS tag or GitHub Release.
