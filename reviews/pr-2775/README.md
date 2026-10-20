# Review evidence for microsoft/SynapseML#2775

## Current pre-commit evidence

The driving human requested lightweight reviews rather than further six-lens
passes. [Attempt 78](task-unknown-attempt-78-review-0-harness.md) completed one
script-owned Low review with runtime model `gpt-6-astra`, high reasoning,
`outcome: clean`, and zero findings. The runner confirmed valid transport,
successful exit, actual model identity and persisted evidence. This is
pre-commit evidence, not final CI-qualified clearance.

- Source target/merge base: `d34f762a785ee8a7c7c624806f8c112256168833`.
- Published predecessor: `bb032091980d44fb7042f84571af5ebf40907b92`.
- Frozen index tree: `61a2ea83e40c7eb55929a4d7d6567c995cf415c0`.
- [Complete source path/mode/blob manifest](attempt-78-source-manifest.json).
- Manifest SHA-256: `11aaf1ffda6ce9e088caeae3e7dc608dcd78e36f297997c04c296cc7db7a3ee5`.
- Full binary source-diff SHA-256: `6214a751f6335039ffc0d85048dc978c6aa1d5d025c5666b1ce1a3a175aed944`.

The follow-up fixes the current-head finding that isolated parent Python flags
do not sanitize a credential-fetching child's environment. The collector and
watcher now remove `PYTHON*` variables from native and Windows batch CLI
environments while preserving other values. A real isolated-parent/Python-CLI
regression failed before the fix and passes afterward; no real credentials or
cloud operations are used. The complete collector/watcher suites passed:
Windows 88 tests and 285 subtests with no skips; Linux Python 3.10, 86 tests and
280 subtests with two Windows-only skips. Pinned Black 22.3.0 passed.

Attempts 71-74 retain their original feedback and resolutions in the
[published archive](https://github.com/microsoft/SynapseML/tree/2025a2155137a9a03d302abc788b02bf1a5ed305/reviews/pr-2775). Accepted
documentation and regression findings are addressed; duplicate and nonblocking
findings retain their reasoned dispositions. Attempts 75 and 76 failed transport
validation and do not count as clean. Attempt 77 used the already installed
CLI 1.0.98 through its supported process-local package selector; no global
settings, redaction, runner or retained logs were changed.

Azure build 241745526 belongs to the published predecessor, not this follow-up.
New-head CI, final lightweight review and human approval remain outstanding.

## Superseded evidence

[Attempts 70-77](https://github.com/microsoft/SynapseML/tree/2025a2155137a9a03d302abc788b02bf1a5ed305/reviews/pr-2775)
retain original verdicts, failures and driver dispositions in the source
publication commit. All 24 hosted review file modes and blob IDs were verified
against that published PR head before pruning superseded files. Archived
feedback does not qualify later source revisions; the current manifest is above.

[Attempts 1-12](https://github.com/microsoft/SynapseML/tree/38685fad3d2e3ec93e6757adf36aa270cc3a24d1/reviews/pr-2775)
and [attempts 13-67](https://github.com/microsoft/SynapseML/tree/0f8d7d011838c86a2a80b6392a9aab8880676119/reviews/pr-2775)
retain their original feedback and finding dispositions in immutable commits.
Both commits were verified reachable through the published PR head, and hosted
files were verified before pruning the superseded artifacts from the final diff.
Earlier passes do not qualify the revised source.

The [attempt-68 source commit](https://github.com/microsoft/SynapseML/commit/9da85296332272173f10cda7432e71de127889ed)
retains all attempt-68 feedback, attempt 67 and the format-invalid attempt-68
invocation. Its hosted review blobs and reachability were verified before pruning.

Attempt 69 is retained in verified, reachable hosted history:
[round 3](https://github.com/microsoft/SynapseML/blob/2d4bf69fcf252c865fafeca5f1993bdc961e1d49/reviews/pr-2775/task-unknown-attempt-69-stage-attempt-1-review-3-claude-opus-5.5.md),
[round 5 and disposition](https://github.com/microsoft/SynapseML/blob/2d4bf69fcf252c865fafeca5f1993bdc961e1d49/reviews/pr-2775/task-unknown-attempt-69-stage-attempt-1-review-5-claude-opus-5.5.md),
and [round 6 and fixes](https://github.com/microsoft/SynapseML/blob/2d4bf69fcf252c865fafeca5f1993bdc961e1d49/reviews/pr-2775/task-unknown-attempt-69-stage-attempt-1-review-6-claude-opus-5.5.md).
It stopped after concrete findings; the fixes changed the source and required
attempt 70. Its partial pass does not qualify the current patch.

Private excluded drafts remain private; their public exclusion records explain
the disposition. Exclusions and incomplete passes do not count as clean reviews.
