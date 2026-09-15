# Round 2: architecture and patterns

**Verdict: CLEAN.** No concrete architectural or pattern defects found.

Reviewed `AGENTS.md`, master branch guidance, dependency manifests, the complete
uncommitted production/docs diff, three new Scala suites, and the Fabric script.
This review is limited to #2702/#2703 architecture, conventions, and plan changes.

Production references below are relative to
`lightgbm\src\main\scala\com\microsoft\azure\synapse\ml\lightgbm\`.

## Evidence

- `dataset\DatasetUtils.scala:13-31` centralizes package-scoped dimension/null checks.
  Preflight and native-copy checks protect different execution boundaries; their reuse is not redundant policy.
- `LightGBMBase.scala:675-683` consumes full Rows in the existing counting action,
  collecting only one ID/count pair per partition. It adds no feature-only projection, shuffle, or full-row driver collection.
- `LightGBMBase.scala:578-588` checks validation through a lazy DataFrame iterator.
  `ValidationDataServer.scala:510-514` and `ValidationDataIngest.scala:45-51` consume it during existing spool ingestion, before training.
- `BasePartitionTask.scala:81-112` derives thread slots and indices from the same
  executor-local partition list, retaining empty slots and existing offset calculations.
- `dataset\ReferenceDatasetUtils.scala:137-152` keeps native-reference validation
  beside deserialization and reuses established cleanup helpers. Logging at lines 117-122 follows the existing verbosity gate.
- `LightGBMPerformance.scala:132` observes the existing timestamp without new state
  or altered duration semantics. `StreamingFeaturePreflightSuite.scala:155-165` explicitly distinguishes started-but-incomplete training.
- The Scala suites retain `TestBase`/`LightGBMTestUtils` conventions and public estimator paths.
  The extracted rejection helper stays test-local. Fabric-specific inspection remains confined to the runtime test script.

Existing JVM signatures, parameter definitions, serialized parameter shapes, and defaults remain unchanged.
The new Dataset/encoder operations fit the Spark 3.5 baseline and introduce no Spark-private API or RDD implementation.
No dependencies, pipelines, generated wrappers, or shared branch guidance changed.
`docs\Explore Algorithms\LightGBM\Overview.md:131-150` states the stable-input prerequisite and native-recovery limit.

Static review only. No tests, builds, source edits, or subagents.
Runtime AQE, cross-version, and multi-executor results remain with the parent; this verdict does not claim those checks passed.
# Follow-up: fixture architecture

The ranker uses the existing `repartitionByGroupingColumn` parameter only in the
standalone Fabric control. No global Spark configuration, service setting,
production default, or public API changes. Input group alignment is a checked
precondition rather than an assumption.
