---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 18
round: 1
timestamp: "2026-11-05T05:19:38Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "e8d6408ce371b13021c1d78a81f7093942898eef2e0be6c5d2cb0d39768b4e29"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot_cli
attempt: 1
stage_attempt: 18
round: 1
timestamp: "2026-11-05T05:09:18.287Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: "758782fc859c16e0d660e574f288729473fe0654fee349f429b6d334904e07f7"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-18-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed all 32 supplied diff paths, including writer removal, documentation, tests, and the `pipeline.yaml` package selector.
- [x] Inspected CU request validation, Dataset-resolver planning, parameter snapshots, and output parsing in `ContentUnderstanding.scala`.
- [x] Inspected protocol recovery, partition stops, transport ambiguity, DNS capacity, JSON/header bounds, and Retry-After handling across the CU protocol and helper files.
- [x] Traced Python parameter/default propagation through `ContentUnderstandingParams.scala`, `Wrappable.scala`, `Utils.py`, and `ComplexParamsSerializer.scala`.
- [x] Reviewed Scala/Python regression coverage and supplied validation claims; existing default-column tests exercise `load`, not construction with `java_obj`.
- [ ] Tests were not executed in this read-only review. Reported passing results were supplied by the caller, not independently rerun.

## Issues

### Issue 1: Wrapping an existing JVM stage promotes explicit column names into defaults
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 166–173
- **Description**: The generated defaults use JVM `getOutputCol()`/`getErrorCol()`, which return effective settings rather than original defaults. The supported `ContentUnderstanding(java_obj=existing_java_stage)` constructor assigns that object without calling `_transfer_params_from_java` (`Wrappable.scala:475–486`). If it has explicit column names, those names become Python defaults. Subsequent parameter transfer writes them back as JVM defaults (`Utils.py:212–218`), so clearing the parameters no longer restores their original values. The refresh added for normal loading does not cover this constructor path.
- **Risk**: Copying, executing, or saving the wrapper can permanently change the supplied stage’s defaults. Later clear/load operations use unexpected output columns or encounter schema collisions.
- **Suggested Fix**: Hydrate the supplied JVM object’s UID, explicit parameters, and actual defaults through the same initialization path used by loading. Read column defaults with JVM `getDefault`, not effective getters. Add a regression covering `java_obj` wrapping with explicit names, followed by save/reload and clear.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Explicit settings and original JVM defaults must remain independent on the constructor path.
- **How verified**: Static trace through constructor generation, CU default initialization, and Python-to-JVM default transfer; no tests executed.

### Driver resolution: Issue 1 (2026-11-05)
- **Status**: Fixed locally; independent re-review pending. Original finding and verdict above are preserved.
- **What changed**: CU-local generated constructor defaults use JVM `getDefault`, not effective getters. Supplying `java_obj` now resets the Python UID and hydrates explicit parameters/defaults through `_transfer_params_from_java`. Shared constructor behavior is unchanged.
- **Why**: Construction, loading and copying must keep identity, explicit settings and original defaults separate before any transfer back to the JVM.
- **How verified**: The new Python lifecycle regression failed in all four column configurations before the fix. After regeneration/publication, all 24 Python tests passed, including supplied-JVM wrapping, explicit settings, original JVM default preservation after save, copy/load/clear and actual local HTTP-backed schema. All 109 CU Scala tests, cognitive main/test style, compilation, codegen and publication passed. Black left 205 files unchanged; diff whitespace check passed.
- **Loaded artifact fingerprints**: Core `8bd5e1e05d73a81831a6795d00597110a0f8133905295702e78a9f1d0054a05e`; cognitive `93d404e1d6050db18bc4e90ee0c9404123de40f93b7563d6b9c2fd8b6c3bbc6e`. Built, locally published and actually loaded JARs matched.

### Driver decision: Stage 1, attempt 19
- **Tier/source**: Max/direct and clean-only advancement from the driving human. Grok low reasoning and unavailable-Grok exception are retained human overrides; other slot selection follows review-code policy.
- **Change size/complexity**: Broad, 32 source paths and 385857 patch bytes; high interaction complexity across HTTP recovery/concurrency, Spark schema/session selection, persistence and generated Python.
- **Primary/reviewers**: Primary `gpt-6-astra`; freshly resolved current task-catalog reviewers `gpt-6.1-sol`/max, `grok-4.7`/low, `claude-opus-5.5`/max. Three independent concurrent Execute invocations; stage 1 advances only when all required reviewers are clean. No new timeout, permission bypass or reduced scope.
- **Snapshot**: `cu-stage1-attempt19.patch`, SHA256 `d833848c330cb66e4728e311cd57930daf5495639dd953e4b28d88a5f1306305`, HEAD `f3a7963c217677e9b6564cc6736495e0f1bcdce4`. Source frozen through collection; prior/sibling feedback excluded and forbidden to reviewers.
- **Delta accounting**: Seven paths changed from attempt 18: CU Params and Protocol, the Python suite, the three Scala CU suites, and CU documentation. Changes cover constructor hydration/defaults, management diagnostic precedence, regressions and their documentation/assertions; no other source paths changed.
- **Runtime monitoring**: Existing schedule 1 remains active every ten minutes. Attempt 18 Opus completed in 26.3 minutes with 65 model calls, 83 tools and one exact repeat; no outlier. Investigate non-convergence/runtime anomalies using public progress and execution metadata, never hidden reasoning. Do not automatically cancel.
- **Limits**: Still stage 1 of 6; no merge-ready claim, commit, push, live service call, Azure CI or ADO edit. Fresh remote-base verification remains unavailable.