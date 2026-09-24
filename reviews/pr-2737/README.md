# PR 2737 review evidence

PR: https://github.com/microsoft/SynapseML/pull/2737

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
