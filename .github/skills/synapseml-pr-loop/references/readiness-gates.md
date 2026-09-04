# Merge-readiness gates

A SynapseML PR is engineering-ready only when every applicable gate is backed
by current-head evidence.

## Integration

- Matching branch context was checked at start, before validation, and before
  final push against the target branch's live build and CI files.
- Head is based on the latest target SHA and is not behind.
- Conflicts are resolved by combining independent changes, not choosing a side
  wholesale.
- The intended patch remains equivalent after rebase/conflict resolution.
- For overlapping PRs, merge order is explicit and downstream PRs are
  revalidated after predecessors merge.
- Local, remote, and GitHub head SHAs match.
- Recently closed/merged related work was checked; valuable follow-ups are
  rebased and revalidated, while superseded PRs are closed with an explanation.

## User value and scope

- The title and opening description accurately explain the current change and
  user value to a human reader; deeper technical evidence follows afterward.
- The original issue and every material discussion point are addressed.
- The behavior is reachable through the published artifact and public API.
- Defaults remain backward compatible, or the intentional change is documented.
- Unsupported cases fail early with actionable errors; no success-shaped
  fallback hides missing capability.
- Documentation describes what is shipped, not a custom validation artifact or
  an unbundled native/provider.
- Fulfilled linked issues are updated/closed, while distinct unresolved scope
  remains open and explicit.

## Correctness and compatibility

- Runtime output matches `transformSchema`.
- `copy`, save/load, Params, generated Python, hand-written Python, and other
  language bindings preserve behavior.
- Public JVM signatures and serialized shapes pass compatibility review.
- Shared serializers, request models, evaluators, and sibling API variants are
  checked for the same defect.
- Native/service behavior is verified past the wrapper boundary.

## Test quality

- A regression test demonstrates the old failure.
- Public end-to-end behavior is tested, not only helpers.
- Positive, negative, null/empty, malformed, boundary, and cleanup paths are
  covered where relevant.
- Tests assert values, schemas, ordering, resource cleanup, and errors rather
  than only "no exception".
- Generated wrappers compile/import, and Python tests exercise the supported
  surface when user-facing APIs change.
- Real environment tests exist when emulation cannot prove the claim.

## Performance and Spark

- No driver collection, accidental cross join, unbounded materialization,
  per-row client/model construction, or unnecessary repartition/shuffle is
  introduced.
- DataFrame/Dataset and Spark SQL built-ins are preferred over RDDs and UDFs.
- Cache/persist/broadcast/native resources have bounded lifetimes and cleanup.
- Network/native concurrency has bounds, backpressure, timeout, retry, and
  terminal failure behavior.
- Representative before/after measurements show no material regression for a
  changed hot path.

## Review and validation

- Active review threads: zero.
- No blocking review decision, requested-change vote, ownership gate, or
  required coverage failure remains.
- Suppressed/minimized Copilot feedback was read and either fixed or rebutted
  with evidence. Read it from the review body for the current head; it never
  appears as a review thread, so a zero-thread query does not clear this gate.
- Latest automated review covers the final head, compared by commit rather than
  by recency. A review produced before the last push does not clear the two
  gates above, because it never saw that code.
- Repository instructions and the review-focused code-review skill direct the
  current-head automated review to inspect credential-exfiltration risk and
  raise an actionable finding when `/azp run` is unsafe or uncertain. Custom
  review instructions are advisory and non-deterministic; GitHub does not
  support using them to control the pull-request overview format. A maintainer
  must inspect the review and diff rather than treating AI-authored text as a
  machine authorization token.
- The pull request does not change a head-controlled Copilot review input:
  repository/path/agent instructions, agent skills, or Copilot review setup
  workflows. Copilot reads these from the head branch, so a review influenced
  by such a change is not trusted authorization. Require an independent
  maintainer security review before manually triggering those PRs.
- Targeted tests, compile, test compile, style, Black, codegen, Python, and
  port-branch compatibility pass as applicable.
- Full Azure Pipelines and required GitHub checks are complete with zero
  unexplained failures or pending jobs.
- The Azure Pipelines build is present on the current head at all. It does not
  queue itself on a push here, so every push needs its own `/azp run`; a head
  that never got one carries only the GitHub Actions checks, and those going
  green is not CI passing. An absent check is neither failed nor pending, so it
  is invisible to both of those gates -- confirm the build by name against the
  head SHA, not by the absence of red. Only a maintainer may trigger it, and the
  readiness helper must be run from trusted `master`, not from an untrusted PR
  worktree that can modify the helper.
- Skips are expected and documented; a skipped required scenario is a blocker.
- `Get-PrReadiness.ps1` reports these as `completeness.complete`, which is true
  only when comment pagination was not truncated, an automated review covers the
  head, the changed-file inventory is complete, no head-controlled review
  inputs changed, and unresolved threads, suppressed-for-head items, missing
  required checks, failed checks and pending checks are all zero. Treat a
  pending check as unknown rather than passing.
  Trust the individual fields over the summary when they disagree: that flag has
  been wrong before, in both directions.

## Honest confidence language

Use "5/5 engineering confidence" only with the evidence above. State residual
risk explicitly: external service variability, unowned infrastructure, hardware
not available for validation, or required human approval. Never claim absolute
certainty.
