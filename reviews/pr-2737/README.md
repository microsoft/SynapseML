# PR 2737 review evidence

PR: https://github.com/microsoft/SynapseML/pull/2737

The [GPT-only follow-up](#gpt-only-follow-up-review) is the latest accepted
pre-commit source review. Earlier series, blockers, and original feedback below
are historical records, not current readiness claims.

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

## External-contributor workflow follow-up

The next follow-up routes external and unverified authors through the full
trusted contributor skill, not only its safety checklist. It preserves scoped
permissions, contributor history and discussions, and the contributor handoff.
Both workflows share one checkpoint rather than recursively invoking each other.

The worktree was rebased onto target
`384f27a5d0e01271f67fdc81505c37e3016d52ea`; that target advance changes only
`website/package-lock.json`. The published PR head remains
`027f0b06f0a7b4cb394112ba08010a3d0fbd690e`. These pending changes have not
been committed or pushed.

Pending source manifest SHA-256:
`47f5e2ea3bd2fbcf5f6f95496788fc66a82d7ca8e0adff156ed143c23fd80442`.
Verified full source diff SHA-256:
`a4df14d861e0fba7d6a343e248820752dd06e98b311a9a7c27809fcea0f7d499`.

Attempt 13 is incomplete:

| Round | Evidence | Result |
| --- | --- | --- |
| 1 | [GPT completeness](task-pr-loop-attempt-13-review-1-gpt-6-astra.md) | CLEAN |
| 2 | [Gemini consistency](task-pr-loop-attempt-13-review-2-gemini-3.8-flash.md) | CLEAN |
| 3 | Opus task transport and CLI both returned no report | BLOCKED, no usable review artifact |
| 4-6 | Not dispatched after the required reviewer failed | NOT RUN |

Metadata, local links, size limits, source whitespace, contributor-routing
checks, and nine disposable Git protocol tests passed. The full diff replayed
to the exact frozen manifest. Tabletop cases cover unverified classification,
edit permission without rewrite permission, missing thread-resolution authority,
shared checkpoint reuse, and the contributor handoff.

Do not treat the earlier frozen-source pass or published head's passing CI as
coverage of this pending patch. Resume the missing review only after verifying
source identity and reviewer availability; no required review was waived.

## Conditional CI delegation follow-up

The user subsequently requested that invoking the external-contributor reviewer
delegate `/azp run` after evidence-based safety clearance, then use the PR loop
for validation. The pending seven-document patch adds that conditional grant,
explicit no-CI opt-out, revision/resource restrictions, and validation-only
handoff without authorizing edits, history rewrites, or merges.

Target and merge base:
`95b718bb7f7cf4d22ebf40d0b4d0ac3ee9a093de`.
Source manifest SHA-256:
`03c984a85481ff02ffdb6505c2ee67dea75f45b37c540df2f828bcf38ec2b426`.
Verified full source diff SHA-256:
`1c3bb3cd7665b0ed9fe47cdb4831d99cab655eb45c4b3118796128d8d2e473a0`.

Attempt 14 supersedes attempt 13's source snapshot but remains incomplete:

| Round | Evidence | Result |
| --- | --- | --- |
| 1 | [GPT completeness](task-pr-loop-attempt-14-review-1-gpt-6-astra.md) | CLEAN |
| 2 | [Gemini consistency](task-pr-loop-attempt-14-review-2-gemini-3.8-flash.md) | CLEAN |
| 3 | Opus CLI and task transport returned no report | BLOCKED |
| 4-6 | Not dispatched | NOT RUN |

A short Opus availability probe succeeded, but it is not review evidence.
No missing review is treated as a clean result. Local links/anchors, skill
metadata/size, whitespace, changed-default checks, and diff-to-manifest replay
passed. Tabletop cases cover a cleared maintainer request, explicit no-CI,
untrusted contributor instructions, unsafe secret-bearing head races, reuse of
an existing build, and product failures without edit permission.

These changes remain local and unpublished. The published PR's older passing
CI does not validate this snapshot. Resume from the recorded source identity
when the required reviewer can return a complete report.

## GPT-only follow-up review

The user explicitly requested GPT-only reviews with no Gemini or Opus. Attempt
15 therefore ran all six themes in separate GPT-6 Astra contexts at maximum
reasoning. This is a user-authorized single-model-family exception, not a
multi-model pass. Older Gemini/Opus evidence remains historical and was not
supplied to these reviewers.

All six rounds cover the unchanged seven-document manifest
`03c984a85481ff02ffdb6505c2ee67dea75f45b37c540df2f828bcf38ec2b426`
and full source diff
`1c3bb3cd7665b0ed9fe47cdb4831d99cab655eb45c4b3118796128d8d2e473a0`
against target and merge base
`95b718bb7f7cf4d22ebf40d0b4d0ac3ee9a093de`.

| Round | Theme | Report | Result |
| --- | --- | --- | --- |
| 1 | Completeness | [GPT](task-pr-loop-attempt-15-review-1-gpt-6-astra.md) | CLEAN |
| 2 | Consistency | [GPT](task-pr-loop-attempt-15-review-2-gpt-6-astra.md) | CLEAN |
| 3 | Robustness | [GPT](task-pr-loop-attempt-15-review-3-gpt-6-astra.md) | CLEAN |
| 4 | Correctness | [GPT](task-pr-loop-attempt-15-review-4-gpt-6-astra.md) | CLEAN |
| 5 | Validation | [GPT](task-pr-loop-attempt-15-review-5-gpt-6-astra.md) | CLEAN |
| 6 | Documentation and hardening | [GPT](task-pr-loop-attempt-15-review-6-gpt-6-astra.md) | CLEAN |

The exact binary diff replayed into a disposable base index and reproduced
the full source manifest. Metadata, size, local links/anchors, whitespace, and
the conditional-CI decision cases passed local checks. Each reviewer received
the same complete diff and frozen source without earlier reviewer opinions.
The driver assigned and persisted the exact report filenames linked above.

This supersedes the unavailable-reviewer blockers in attempts 13 and 14.
It remains a pre-commit review: publication requires final source identity and
complete path-set verification, followed by fresh SHA-bound remote CI and
review. No older build or human approval is claimed for this patch.
