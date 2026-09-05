---
name: synapseml-release
description: Plan, preview, publish, resume, and recover SynapseML releases. Use for release automation, independent Internal patches, tags, Maven, pip, UPack, rehearsal, release evidence, BBC-VHD, ESRP, White-Glove, or Fabric rollout.
compatibility: Python and git; Bash for the documented examples. GitHub CLI and authorized Azure CLI access are needed for remote reads and approved publication. SBT checks require the branch-selected JDK.
---

# SynapseML release

Read [scripts/release/README.md](../../../scripts/release/README.md) for executable
commands. This skill defines which commands may run and what evidence to keep.

## Rules

- Load the [branch context skill](../synapseml-branches/SKILL.md). Read versions
  and supported targets from current source, not a previous release.
- Generate a schema-v1 plan with `release_matrix.py`. Never edit its coordinates,
  flags, destinations, source bindings, or digest by hand.
- Keep plans, state, and evidence outside source checkouts. Release builds
  require clean reviewed source. Use one authoritative ledger directory per
  approved plan; never replace its state or persistent claim to restart work.
- A plan ID is an identity, not permission. Remote publication requires explicit
  human approval, `--apply`, and the exact `--approve-plan` value.
- Never move published tags, overwrite release packages, erase unknown
  submissions, or widen the selected repositories/families to make progress.
- Keep credentials out of plans, logs, PRs, and GitHub workflow inputs.
- Do not automate PR merges, workflow approvals, ESRP/SAW approval, White-Glove
  sign-off, permissions, or release-train selection.

## Workflow

### 1. Select the release track

Use the smallest repository, target, and artifact-family selection.

- New OSS base: `full` with `repositories=oss`.
- Initial Internal release on that published base: `full`, `repositories=internal`,
  and Internal patch `0`.
- Later Internal hotfix: `internal-only` with a nonzero Internal patch.
- Recovery: existing reviewed tags, selected repositories/families, and explicit
  UPack counters where needed.
- Rehearsal: existing tags and explicitly isolated pip/UPack feeds; no Maven,
  GitHub release, or BBC-VHD operation.

Publish an OSS base before requiring Internal CI to resolve it. Neither
Internal-only-repository track may queue OSS jobs. Do not impose a circular
requirement that Internal must merge before its OSS dependency is published.
Use an OSS-only Maven plan for GitHub Release evidence. Keep private
pip/UPack publication in a separate plan.

### 2. Preview and bind source

Generate a draft matrix. For a new OSS release, preview `bump-version.py` and
inspect the full-release policy before starting **Release Prepare**.
`skip_docs` is not a dry run.

Review version, port, and Internal changes through their PRs. Bind the actual
merged commits in the final publication plan. A changed commit, counter, feed,
scope, or selection requires a regenerated plan and new approval.

Use the Internal helper's preview-first preparation/tagging commands when
Internal is selected. Bare legacy OSS pins require its explicit verified-base
policy and matching historical runtime checks.

See [preflight](references/preflight.md).

### 3. Preflight and execute

Run `release_ops.py preflight --plan ... --state ...`, then preview with
`resume` without `--apply`. Both make no remote writes.

After approval, use `resume --apply --approve-plan ...`. It records intent and
build IDs, queues only missing authorized work, and leaves human service
approvals untouched. Use `status` to reconcile pending work.

Do not requeue a failed or unknown operation blindly. Inspect Azure and use
explicit `--adopt action-id=build-id` only for a matching run. Adoption itself
must not queue unrelated jobs.

For a recorded failed pip/UPack operation, `--retry action-id` needs the same
approval and fresh definitive absence for its entire original group. It
preserves attempt history and queues only that group. Maven retries remain
blocked because aggregate inventory cannot prove a completely empty namespace;
use a new version or Internal patch and a newly approved plan.

If a lock blocks status, use `status --inspect-lock` for bounded local metadata
without Azure calls or lock removal. Follow the confirmed-dead-owner procedure
in [recovery and rollout](references/recovery-and-rollout.md); age or PID alone
must never authorize deleting a lock.

See [automation boundaries](references/automation-boundaries.md).

### 4. Require producer evidence

Export `verify_release.py --plan ... --state ... --json` only after the driver
can revalidate the selected runs and artifact receipts.

Inventory alone is not approval. Require exact plan identity, complete required
coverage, current tag/source matches, successful producer outcomes, matching
requests and hashes, and fresh evidence. Never use the publisher repository's
own `sourceVersion` as the released-source SHA.

For historical diagnostics, use `--version` or explicit `--inventory-only`.
Neither can authorize rollout.

### 5. Publish notes and roll out

**Release Notes** consumes only a public-only Maven plan and its evidence.
Use `verify_release.py --github-evidence` to produce its bounded compressed
input. Do not send Internal or combined plans/evidence to GitHub, including
Internal commit bindings in an otherwise public-only plan.

Preview `bump_bbcvhd.py --plan ... --target ...`. A write additionally requires
`--evidence`, `--apply`, and matching `--approve-plan`. Internal-only updates
must preserve the exact existing OSS pin, including its rebuild suffix.

For separate full-scope OSS and Internal UPack releases, pair the original
Internal `--plan` with `--oss-plan`. Writes require both original evidence files
and both approval IDs. The updater checks their shared base, source, target,
counter, and destination before changing both pins. Never relabel receipts
under a new combined plan. This handoff is not available to hotfix scope.

Bind the OSS plan's existing UPack counter into the initial Internal plan with
`--upack-iteration TARGET=N` before approval or publication. This is required
for full-scope pairing too. Changing the counter creates a new plan ID; it
cannot repair receipts from an already published plan.

Review the BBC-VHD PR, complete its CI and White-Glove approval, then monitor the
approved train and inspect the installed versions in Fabric.
See [recovery and rollout](references/recovery-and-rollout.md).

## Completion

Report the exact completed track, source commits, plan ID, build IDs, published
coordinates, evidence, and remaining human gates. Package publication does not
mean Fabric rollout completed. Do not claim success for skipped required jobs,
missing producer receipts, stale evidence, or an unrelated failing baseline.
