---
name: synapseml-release
description: Prepare, publish or recover approved SynapseML releases, including offline rehearsal and pre-merge candidates. Use for release operations and agent handoffs.
compatibility: Python, pytest, PyYAML and Git for rehearsal; authorized GitHub/Azure CLI for live operations. SBT needs the branch-selected JDK.
---

# Public SynapseML release

1. Load the branch skill and resolve runtime versions from selected source.
   Start the single [operator and agent procedure](../../../scripts/release/README.md#1-preview-and-prepare-source).
   Use its guarded scripts and capture their results, not a separate publisher.
2. **Consumer-wheel gate:** qualify the same primary candidate wheel on every
   selected runtime before tag approval. Offline rehearsal and producer CI
   grant neither qualification nor publication authority.
3. Follow the guide's normal preparation or approved
   [bootstrap](../../../scripts/release/README.md#before-the-automation-pr-is-merged).
   Require explicit authorization for live actions and exact-plan approval for
   publication. Keep signing/merges manual and tagged candidates frozen.
4. At each stop, record the [handoff](../../../scripts/release/README.md#handoff-at-every-stop);
   on resume, reread it and the original ledger. Keep private records out of
   public inputs. Complete the guide's final-wheel sign-off and verified website
   deployment before claiming those outcomes.
