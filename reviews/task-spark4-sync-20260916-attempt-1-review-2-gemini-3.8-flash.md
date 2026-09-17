# Spark 4.0 master sync, attempt 1, round 2

## Review summary

- **Round:** 2.
- **Theme:** Architecture & Patterns (design consistency, abstraction quality, convention adherence).
- **Mode:** Sequential.
- **Model:** `gemini-3.8-flash`, high reasoning requested.
- **Issues found:** 0.
- **Verdict:** CLEAN.
- **Artifact:** `reviews\task-spark4-sync-20260916-attempt-1-review-2-gemini-3.8-flash.md`.

This round evaluates architectural integrity, pattern consistency, and AGENTS.md compliance across the merged Spark 4.0 candidate. The merge maintains clear abstraction boundaries: Scala 2.13 collection conversions are cleanly encapsulated at service boundaries, authentication precedence and lazy Fabric fallback decouple environment checks, Python stub generation consistently mirrors runtime wrapper default guards via `safeGetDefault`, test fixtures are properly isolated from production stage discovery, and preserved port-specific adaptations strictly follow repository branch policy without introducing RDD regressions or altering public signatures.

No agents were launched. No source, documentation, test files, git staging, or commits were altered.

## Source snapshot

| Item | Reviewed value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.0-master-20260916` |
| Branch | `sync/spark4.0-master-20260916` |
| Target and HEAD | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` |
| Incoming master and MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Graph merge base | `a833941704b5e8334ddb40a9d601d7e0c7c0ce9f` |
| Last integrated master content | `a6fd536ad76eb1b60ac82f31a362ae624886c6ff` |
| Staged index tree (`write-tree`) | `cd6bd3b6eacb3062707f0f3247ec04f4f019d106` |
| Raw stage hash (`ls-files -s -z`) | `a356ea797c48a3d361d272ac467b9eed1769ce46b333fb3221ff79a123bcdc8a` |
| Working tree delta vs index | 0 files (working tree matches staged index) |
| Snapshot check timestamp | `2026-09-16T09:20:00Z` |

## Evidence checklist

- [x] **AGENTS.md & Branch Guidance Adherence**: Verified `AGENTS.md`, `CONTRIBUTING.md`, and branch skills in `.github/skills/` remain byte-identical to master. No branch-specific versions were leaked into shared documentation.
- [x] **Scala 2.13 Collection Abstraction**: Inspected `CognitiveServiceBase.scala` (`asImmutableCollection`, lines 105-115) and callers (`getValueOpt`). Mutable Spark arrays/maps are normalized to immutable IndexedSeq/Map before reaching service layers, preventing runtime `ClassCastException` without leaking mutation. Verified call-site conversions in `AnalyzeText.scala`, `OpenAI.scala`, `OpenAIChatCompletion.scala`, and `OpenAIResponses.scala` (`.toSeq` conversions) maintain uniform collection immutability.
- [x] **Auth & Retry Architecture**: Inspected `CognitiveServiceBase.scala` (lines 520-565). `lacksExplicitAuthCredential`, `getFabricFallbackAuthHeader`, and `resolveServiceAuthHeaders` maintain strict precedence: explicit keys > AAD tokens > custom headers > lazy Fabric fallback. Fabric fallback is invoked by-name only when running on Fabric without explicit credentials, avoiding premature token acquisition or tight coupling.
- [x] **Codegen Architecture & Default Guard Uniformity**: Inspected `core/.../codegen/Wrappable.scala` (lines 109-115, 150, 161, 317). `pyStubParamArgs` now utilizes `safeGetDefault(p)` identically to `pyParamArg` and `pyParamDefault`, gracefully handling foreign-owned parameters (`IllegalArgumentException`) rather than crashing stub generation.
- [x] **Fixture Isolation Pattern**: Inspected `PyCodegenSuite.scala` (lines 20-65). `TypedPythonStage` and `ForeignParamPythonStage` are nested inside `private[codegen] object PyCodegenFixtures`, preventing SynapseML classpath stage discovery from registering test-only fixtures into production pipelines.
- [x] **OpenAI Wrapper & Inheritance Design**: Inspected `OpenAIPrompt.scala` (`pyInternalWrapper = true`) and `OpenAIPromptPythonOverrides.scala`. The generated Python wrapper cleanly emits `_OpenAIPrompt` while public `OpenAIPrompt.py` extends it. Overridden `clear` and `copy` methods strictly use zero-argument `super()`, avoiding `NameError` from nonexistent global symbols in generated code.
- [x] **Public JVM Signatures & No RDD Violations**: Verified all public stage interfaces retain DataFrame/Dataset APIs. No new RDD-based implementations were introduced; `CleanMissingData.scala` typed conversion (`convertCustomValue`) and `Repartition.scala` (`sparkSession.createDataFrame`) preserve existing signatures while conforming to Spark 4.
- [x] **Recommendation Port Separation**: Confirmed `SAR.scala` preserves typed `Seq[SAR.ItemAffinity]` with explicit `itemIndex`/`affinity` fields and `SARModel.scala` qualifies `col("sarUserFactors.flatList")`, preventing ambiguous self-joins under Spark 4 analyzer rules.
- [x] **Subsystem Streaming & Packaging Conventions**: Verified Spark 4.0 preserves `org.apache.spark.sql.execution.streaming.LongOffset` in `HTTPSource.scala` and `DistributedHTTPSource.scala`. Verified `templates/publish.yml` preserves `verifyPythonPackaging`. Verified package export guards (`test_package_exports.py`, `test_http_package.py`) and Petastorm compatibility shims (`_petastorm_compat.py`) remain intact.
- [x] **CI & Runtime Policy Discipline**: Confirmed `pipeline.yaml` keeps `FabricE2E` job strictly at `condition: false`, matching `test_pipeline_yaml.py` (`assert jobs["FabricE2E"]["condition"] is False`). Verified JDK 17, DBR 17.3, Python 3.12.11, and NumPy 1.26.4 matrix pins are fully preserved.

## Architectural findings

No architectural violations, abstraction leaks, coupling anti-patterns, or AGENTS.md non-conformance were identified. Design patterns across Scala 2.13 collection boundaries, authentication resolution, codegen stubs, and Spark 4 port-specific adaptations are clean, idiomatic, and robustly tested.

## Validation limitations

This review assesses architecture, patterns, and design consistency based on direct static inspection and local validation evidence (186 targeted test passes, 17 PyCodegen tests passing red-to-green on the foreign param fixture, 77 pipeline tests, 214 Black-formatted files, 325 `.py` and 222 `.pyi` syntax checks). Remote Azure Pipelines validation has not run. Subsequent rounds (Rounds 3-6) were not executed.

## Resolution log

- Round 1 Issue 1 (Python stub generation default lookup bypassing Spark 4 guard at `Wrappable.scala:317`): Verified resolved. The code now uses `safeGetDefault(p)` in `pyStubParamArgs`, verified by `PyCodegenSuite` red-to-green regression coverage.
