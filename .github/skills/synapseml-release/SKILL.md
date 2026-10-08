---
name: synapseml-release
description: Operate the SynapseML publishing agent runbook, run offline release dry runs, prepare candidates, and publish or recover approved source-bound releases.
compatibility: Python, pytest, PyYAML and Git for offline rehearsal; GitHub CLI and authorized Azure CLI for live operations. SBT requires the branch-selected JDK.
---

# Public SynapseML release

Use [the operator guide](../../../scripts/release/README.md) for commands.
Load the branch skill and read runtime versions from the selected source.
For agent execution, follow the checkpoint and handoff rules in
[the agent runbook](references/agent-runbook.md). Use the existing guarded
scripts rather than constructing a second publication engine.

## Rules

- Public releases select OSS and Maven, including primary public PyPI.
  New schema-4 plans also require a runtime-matched public DBC archive per
  selected target; saved schema-2 plans retain their original scope.
  Private integrations and their deployment procedures are outside this guide.
- Default targets are `master` and `spark4.1`. Spark 4.0 requires explicit
  selection in a newly approved plan; never change a saved plan's targets.
  Default version bumps retain its last published installation examples.
- Check optional-runtime policy and readiness before promising new artifacts
  in its guides. If that runtime is dropped, use the operator guide's back-out
  procedure; do not weaken the publication lock or edit a tagged candidate.
- Generate plans rather than editing their fields or approval IDs.
- Use only public allowlisted plans and evidence in GitHub inputs. Base64 and
  compression do not redact private information.
- Keep credentials, local profiles, state, private-source bindings and operator
  records out of public source and output.
- A digest is not approval. Publication requires explicit maintainer approval,
  `--apply` and the exact `--approve-plan`.
- Never move release tags, overwrite packages, erase ambiguous submissions,
  bypass protection or automate human signing approvals and PR merges.
- Freeze tagged candidates completely, including ordinary pushes and **Update
  branch**. Do not apply PR-loop rebase or not-behind gates after tagging.

## Procedure

1. Establish the release owner, requested version/targets, operation permissions
   and one protected plan/ledger directory outside the checkout. Confirm App
   setup and service access before live preparation. Run
   `python scripts/release/release_dry_run.py --report <new-local-report.json>`
   for an offline rehearsal. Require a passing report with zero skips.
   It never grants publication authority or validates live services.
2. Preview the version change and matrix. Check existing coordinates, source
   refs and full-release policy. `skip_docs` is not a dry run.
3. Prepare and review final source for every target. **Consumer-wheel gate:**
   the release owner must qualify the same source/version-bound primary candidate
   wheel on every selected runtime before approval or any production tag. Retain
   the wheel and results. Port CI using different wrappers is not that proof;
   resolve distribution first.
   Normal workflows require
   the primary commit on `master`; for pre-merge publication use the approved
   bootstrap entry point in the operator guide, not a disabled ancestry guard.
   Before bootstrap, confirm master's merge rules permit the unchanged
   candidate to merge even if master advances. Unreadable or incompatible rules
   block bootstrap; do not alter protection.
4. Generate a new public plan binding final canonical tag commits, or reviewed
   candidate commits before bootstrap tags exist. Changing source or coordinates
   invalidates the old approval. Preview bootstrap before requesting tag
   approval; run publication preflight only after canonical tags exist.
5. Run preflight, then resume without `--apply`. Both queue nothing.
6. After exact-plan approval, resume with apply and retain the authoritative
   ledger. Complete required human signing gates.
7. Revalidate producer runs, exact artifacts and hashes. The release owner must
   compare the rebuilt published wheel's payload and relevant metadata with the
   qualified candidate and verify its installation on every selected runtime.
   ZIP metadata can change archive hashes without changing payload; hash equality
   is not the qualification rule. A mismatch or missing qualification evidence
   blocks readiness and notes. This human gate is not inferred by the verifier.
   Export public evidence immediately before use; it expires after one hour. Publish primary
   notes only after all selected targets complete and the unchanged primary candidate
   has merged first, before other automation changes. Run the read-only
   integration check before dispatch. Squash and rebase merges use canonical
   merged-PR provenance, not a moved tag. Resolve conflicts through a separate
   reconciliation PR on master; if proof is unavailable, keep notes unpublished
   and escalate to the release owner.
8. Retain the human qualification sign-off with the release record; automated
   producer evidence alone does not establish consumer readiness.
9. After the primary versioned documentation merges to master, land the reviewed
   `published-spark-ports.lock` follow-up using verified artifact versions.
   Keep the unselected Spark 4.0 entry at its retained published version.
   Primary API docs are generated and publicly checked by the approved producer
   before Maven upload; those mutable docs are not an immutable package receipt.
   Expect strict master Website Deploy to fail until that follow-up lands.
   Confirm successful deployment before reporting the website updated.

Read [preflight](references/preflight.md),
[automation boundaries](references/automation-boundaries.md), and
[recovery](references/recovery-and-rollout.md).

The old Spark 4.1 PR replay is removed. Every selected release candidate still
requires its own validation. Report concrete completed coordinates, source
commits and remaining gates. Do not call previews or skipped tests production proof.
