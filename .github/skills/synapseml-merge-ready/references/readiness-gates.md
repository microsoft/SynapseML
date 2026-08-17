# Merge-readiness gates

A SynapseML PR is engineering-ready only when every applicable gate is backed
by current-head evidence.

## Integration

- Head is based on the latest target SHA and is not behind.
- Conflicts are resolved by combining independent changes, not choosing a side
  wholesale.
- For overlapping PRs, merge order is explicit and downstream PRs are
  revalidated after predecessors merge.
- Local, remote, and GitHub head SHAs match.

## User value and scope

- The original issue and every material discussion point are addressed.
- The behavior is reachable through the published artifact and public API.
- Defaults remain backward compatible, or the intentional change is documented.
- Unsupported cases fail early with actionable errors; no success-shaped
  fallback hides missing capability.
- Documentation describes what is shipped, not a custom validation artifact or
  an unbundled native/provider.

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
- Suppressed/minimized Copilot feedback was read and either fixed or rebutted
  with evidence.
- Latest review covers the final head.
- Targeted tests, compile, test compile, style, Black, codegen, Python, and
  Spark 4.1 compatibility pass as applicable.
- Full Azure Pipelines and required GitHub checks are complete with zero
  unexplained failures or pending jobs.
- Skips are expected and documented; a skipped required scenario is a blocker.

## Honest confidence language

Use "5/5 engineering confidence" only with the evidence above. State residual
risk explicitly: external service variability, unowned infrastructure, hardware
not available for validation, or required human approval. Never claim absolute
certainty.
