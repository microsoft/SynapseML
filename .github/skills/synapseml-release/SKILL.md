---
name: synapseml-release
description: Prepare, preview, publish and recover public SynapseML releases using exact-source plans, approved publication and producer evidence.
compatibility: Python, Git, GitHub CLI and authorized Azure CLI access. SBT requires the branch-selected JDK.
---

# Public SynapseML release

Use [the operator guide](../../../scripts/release/README.md) for commands.
Load the branch skill and read runtime versions from the selected source.

## Rules

- Public releases select OSS and Maven, including primary public PyPI.
  Private integrations and their deployment procedures are outside this guide.
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

1. Preview the version change and matrix. Check existing coordinates, source
   refs and full-release policy. `skip_docs` is not a dry run.
2. Prepare and review final source for every target. **Consumer-wheel gate:**
   validate the planned wheel packaging and consumer behavior from these sources
   on every advertised runtime before approval or any production tag. Port CI
   using different wrappers is not that proof; resolve distribution first.
   Normal workflows require
   the primary commit on `master`; for pre-merge publication use the approved
   bootstrap entry point in the operator guide, not a disabled ancestry guard.
   Before bootstrap, confirm master's merge rules permit the unchanged
   candidate to merge even if master advances. Unreadable or incompatible rules
   block bootstrap; do not alter protection.
3. Bind final canonical tag commits in a new public plan. Changing source or
   coordinates invalidates the old approval.
4. Run preflight, then resume without `--apply`. Both queue nothing.
5. After exact-plan approval, resume with apply and retain the authoritative
   ledger. Complete required human signing gates.
6. Revalidate producer runs, exact artifacts and hashes. Export public
   evidence immediately before use; it expires after one hour. Publish primary
   notes only after all targets complete and the unchanged primary candidate
   has merged first, before other automation changes. Run the read-only
   integration check before dispatch. Squash and rebase merges use canonical
   merged-PR provenance, not a moved tag. Resolve conflicts through a separate
   reconciliation PR on master; if proof is unavailable, keep notes unpublished
   and escalate to the release owner.
7. Verify installation and consumer behavior for every released runtime.
8. After the primary versioned documentation merges to master, land the reviewed
   `published-spark-ports.lock` follow-up using verified artifact versions.
   Expect strict master Website Deploy to fail until that follow-up lands.
   Confirm successful deployment before reporting the website updated.

Read [preflight](references/preflight.md),
[automation boundaries](references/automation-boundaries.md), and
[recovery](references/recovery-and-rollout.md).

Spark 4.1 PR replay is advisory, but required validation of an actual release
candidate is not. Report concrete completed coordinates, source commits and
remaining gates. Do not call previews or skipped tests production proof.
