# PR 2737 review evidence

PR: https://github.com/microsoft/SynapseML/pull/2737

The [follow-up series](#follow-up-frozen-source-review) supersedes the initial
source review below. Both series and their original feedback are retained.

This series reviews the three PR-loop Markdown files, not product code.
Target: `681bd96990c421de3b91d2b1bf8f8f470764199d`.
The reviewed pending content was published as
`e9540565f9d88f20ae4e280019bd07d742450c82`.

Canonical reviewed-file manifest SHA-256:
`19fec4fff5dba098eb359bb9cd8e1cad4936a6c4bb3426e090d56cabd7b3305e`.
The staged manifest matched the frozen review manifest before commit.

| Round | Theme | Accepted report | Result |
| --- | --- | --- | --- |
| 1 | Completeness | [GPT](task-pr-loop-attempt-5-review-1-gpt-6-astra.md) | Clean |
| 2 | Architecture | [Gemini](task-pr-loop-attempt-7-review-2-gemini-3.8-flash.md) | Clean |
| 3 | Robustness | [Opus](task-pr-loop-attempt-5-review-3-claude-opus-5.5.md) | Clean |
| 4 | Correctness | [GPT](task-pr-loop-attempt-5-review-4-gpt-6-astra.md) | Clean |
| 5 | Validation | [Gemini recheck](task-pr-loop-attempt-6-review-5-gemini-3.8-flash.md) | Clean after evidence-based dispositions |
| 6 | Documentation and hardening | [Opus](task-pr-loop-attempt-5-review-6-claude-opus-5.5.md) | Clean |

All six accepted results cover the same final source content. Different attempt
numbers record retries, not permission to combine different source patches.
This was the direct sequential contract. GPT and Opus ran independently through
the agent runner. Gemini ran through Copilot CLI with the exact supplied source;
the task transport rejected the newest Gemini model before execution.
No model substitution was used. Prompts were assembled from the installed
review themes because no verified Task ID or PR number existed at review time.
`pr-loop` is a descriptive token, not a work-item ID.

## Preserved history and exceptions

Earlier reports preserve the original findings and appended driver resolutions.
They are historical evidence, not the final pass. Fixes covered bootstrap draft
publication, retry accounting, durable checkpoints, full changed-path checks,
canonical hashing, task-token inference, and existing-PR/no-Task output routing.
The original artifact paths quoted in old reports are historical labels.

- `task-pr-loop-attempt-5-review-2-gemini-3.8-flash.md` contains only a startup
  response. It is incomplete and excluded from accepted evidence.
- `task-pr-loop-attempt-6-review-2-gemini-3.8-flash.md` used the wrong target and
  expanded scope after a CLI input-delivery error. Its clean claim is invalid
  for this PR. Attempt 7 delivered the full frozen input correctly.
- `task-pr-loop-attempt-5-review-5-gemini-3.8-flash.md` retains the initial
  validation findings and driver dispositions. Three actual Git probes and
  additional tabletop traces supported the clean recheck without a source edit.
- One early round-1 draft contained a local workstation path. It remains private
  and is excluded from publication and accepted evidence. Later public-safe
  round-1 reviews supersede it; no finding was suppressed by that exclusion.

Seventeen publication-safe drafts moved from session storage into this numbered
directory with before/after SHA-256 checks. Original feedback was not rewritten.
This README records the bootstrap handoff and is allocated review output.
The artifact-only commit is excluded from the source manifest, but requires
fresh SHA-bound remote checks and review. It is not a new product patch.

## Validation and limits

Skill metadata, local links and anchors, size limits, and whitespace passed.
Three disposable Git-index/tree tests passed: added/modified/deleted manifest
round-trip with CRLF normalization, deterministic canonical ordering, and
rejection of an extra unreviewed changed path.

Additional tabletop cases verified private-draft exclusion, prompt byte-budget
blocking, and the final-pass cap with distinct failures. No Spark build was
needed for this documentation-only patch.

This is pre-publication review evidence, not proof of remote CI, automated
current-head review, or human approval. Those remain separate PR gates.

## Follow-up frozen-source review

The follow-up clarifies pre-commit fixes for red CI, recorded merge-base
baselines, environment isolation, pinned push leases, reviewer independence,
post-push thread resolution, and complete diff-to-manifest verification.
It does not add an executable workflow engine or change any pipeline.

Target and merge base remain `681bd96990c421de3b91d2b1bf8f8f470764199d`.
Canonical source manifest SHA-256:
`1b6aa6ad0b95822a677321b0e60d151f9c786fa39591b04bc3b06bcbffbfd1a6`.
Verified diff SHA-256:
`47478a273c9b2f31b1695c94307b884fe808076acb776d2574587da41aa63a08`.

| Source path | Mode | Normalized Git blob |
| --- | --- | --- |
| `.github/skills/synapseml-pr-loop/SKILL.md` | `100644` | `979200e29afcb3d82e4ae9e219a5efe36f629527` |
| `.github/skills/synapseml-pr-loop/references/loop-control.md` | `100644` | `c8a53ef0b6ef3701786c5a0717874361fe130505` |
| `.github/skills/synapseml-pr-loop/references/readiness-gates.md` | `100644` | `3ff834fc6352f36c7aac17833ff50f40efa7ecf1` |

| Round | Accepted report | Result |
| --- | --- | --- |
| 1 | [GPT completeness](task-pr-loop-attempt-12-review-1-gpt-6-astra.md) | CLEAN |
| 2 | [Gemini consistency](task-pr-loop-attempt-12-review-2-gemini-3.8-flash.md) | CLEAN |
| 3 | [Opus robustness](task-pr-loop-attempt-12-review-3-claude-opus-5.5.md) | CLEAN |
| 4 | [GPT correctness](task-pr-loop-attempt-12-review-4-gpt-6-astra.md) | CLEAN |
| 5 | [Gemini validation](task-pr-loop-attempt-12-review-5-gemini-3.8-flash.md) | CLEAN |
| 6 | [Opus polish](task-pr-loop-attempt-12-review-6-claude-opus-5.5.md) | CLEAN |

These six independent sequential rounds cover exactly the manifest above.
GPT/Gemini ran through CLI; Opus ran through the agent runner after CLI returned
empty output. Incomplete output never counts as a review. Earlier verdicts
were excluded from all reviewer contexts and tool searches.

Preserved follow-up history:

| Attempt | Rounds | Disposition |
| --- | --- | --- |
| 8 | 1, 2; round 3 had no usable output | Superseded; empty round is excluded |
| 9 | 3 | Pinned lease finding fixed; appended resolution retained |
| 10 | 1, 2, 3 | Thread resolution now waits for a published fix; empty manifests cannot dispatch diffs |
| 11 | 1, 2, 3 | Diff fidelity finding fixed with byte-preserving generation and disposable-index replay |
| 12 | 1-6 | Accepted frozen-source pre-commit pass |

Nine disposable Git protocol tests pass: add/modify/delete with CRLF
normalization; canonical sorting; rejection of extra paths; omission of
reverted paths; NUL-safe Unicode/space paths; divergent target versus merge
base; empty-manifest handling; ignored gitlinks; and binary/space-path diff
replay that rejects a partial handoff. The actual publication diff also
round-tripped to the exact source manifest. Source links and whitespace passed.

The earlier published head passed CI after one targeted coverage-publication
retry. That result does not cover this follow-up commit. This is a mandatory
pre-commit pass, not a claim of final CI-qualified readiness; current-head CI,
review coverage, and human approval remain separate remote gates.
