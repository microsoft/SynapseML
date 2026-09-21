---
name: synapseml-external-contributor-review
description: >-
  Review external-contributor SynapseML PRs, check whether they solve the linked
  issue, and make authorized follow-up changes. Use for user-submitted or fork
  PRs and contributor thank-you/sign-off messages.
compatibility: >-
  SynapseML checkout with git, GitHub CLI, and GitHub/Azure Pipelines access.
---

# SynapseML external-contributor review

Help the contributor finish their PR without taking it over. Use
[synapseml-pr-loop](../synapseml-pr-loop/SKILL.md) for review, tests, CI, and
readiness checks, with the contributor safeguards below.

## Procedure

1. Read the linked issue, PR diff, and discussion. Trace the code and reproduce
   the problem independently. Explain whether the fix addresses the issue and
   what remains uncertain; do not claim it fixes an incident you cannot verify.
2. Keep review-only requests read-only. Do not push, change the PR, post
   comments, or trigger CI unless authorized. For a review, report your findings
   and stop here.
3. When asked to make fixes, check maintainer access and work from the current
   PR head in an isolated checkout. Keep the contributor's approach where it is
   sound. Make small follow-up commits on their existing branch, preserving
   their authorship and history. Do not rewrite their commits without permission.
4. Follow the [branch guidance](../synapseml-branches/SKILL.md) for the target
   branch, and use the PR loop for validation rather than duplicating its checks.
   For a bug fix, add a focused regression that fails before the fix and passes
   afterward, exercising the public API when that is where the bug occurs.
5. Check for new contributor commits before pushing. Use the PR loop to trigger
   and monitor CI for the new head. Wait for the checks to pass and confirm the
   relevant tests actually ran. If blocked, say so rather than claiming success.
6. Once validation is complete, thank the contributor for their specific fix.
   Briefly explain your additions, link the commit and CI result, and ask them
   to confirm the changes fit their intent. Offer to revert your additions.
   Use the [message example](assets/contributor-comment.md) as a starting point,
   not a script. Do not merge the PR unless asked; contributor sign-off, CLA,
   or human approval may still be needed.

Leave existing PR comments and discussions untouched, including when following
the PR loop. Do not delete, rewrite, hide, or resolve them. Add a reply only
when useful and authorized.
