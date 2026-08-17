# `spark4.1`

- Shared Spark 4.1 port branch with Scala 2.13 and newer Java/Python-era
  adaptations; verify exact versions and pins in the branch's live files.
- Merge `master` into this branch and preserve intentional codegen, packaging,
  dependency, and runtime compatibility changes.
- `master` compatibility replay commonly applies release-relevant patches here
  and runs `test:compile`; it does not replace full branch validation.
- Some environment suites can be intentionally unavailable. For example,
  Fabric E2E was disabled while no managed Spark 4.1 runtime existed. Verify the
  current pipeline and document any skip rather than assuming it remains valid.
- Databricks/GPU/native claims require real compatible runtime execution.
- `.agents/README.md` is a compatibility pointer; `.github/skills` is the
  authoritative skill location.
