---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 5
round: 6
timestamp: "2026-11-05T23:48:52Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "ceb96752ff8e82e592790b76672906d9844903394764a2856fe4335e5eed9ac9"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-5-review-6-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Poll mode does not require document or apiVersion columns. `getVectorParamMap` is overridden and filtered through `activeParams(poll)` in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala` (around lines 84–96). Scalar settings are validated on the driver, and `validateRow` checks only column-sourced values (around lines 267–283).
- [x] The row-auth provenance marker never reaches the network. `HTTPRequestData.toHTTPCore` removes `X-SynapseML-Implicit-Fabric-Auth` (`core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\HTTPSchema.scala:209`, 218–221). CU uses its own marker value, `content-understanding-row-auth` (`ContentUnderstandingProtocol.scala:99`, 496–497), so it does not collide with the Fabric `"true"` value written at `CognitiveServiceBase.scala:554–561`. The reuse is explained by a comment at `ContentUnderstanding.scala:336`.
- [x] Logging does not expose secrets or service messages. `logManagement` records only outcome, executionSeconds and the exception class name (`ContentUnderstanding.scala` around 449–465). `ContentUnderstandingLogging.safely` restores the interrupt flag and swallows logging failures. `sharedStop` logs a fixed category plus task IDs, and falls back to `taskContext=unavailable` when no TaskContext exists (`ContentUnderstandingLogging.scala:12–35`). The inherited `logTransform` → `logErrorBase` path logs `e.getMessage` (`SynapseMLLogging.scala:34–39`, 155–159), but on the transform/driver path those messages come from fixed `require` text. The only interpolated `require` names the parameter, not its value (`ContentUnderstandingHeaderValidation.scala:16`).
- [x] Snapshot binding and column naming were checked. `buildInternalTransformer` copies the stage and validates that copy with the Dataset resolver. It picks internal column names case-insensitively from a set that includes outputCol and errorCol. It also rejects outputCol/errorCol names that collide with input columns (`ContentUnderstanding.scala` around 405–430). `DatasetExtensions.resolverFor` reads the Dataset session config and does not mutate SQLConf (`DatasetExtensions.scala:66–72`).
- [x] Wrappable subclasses keep the new default `isSet` override. Every `pyAdditionalMethods` override under `cognitive\src\main\scala\...\services` calls `super` (`CognitiveServiceBase.scala:284/636/679`, `ContentUnderstandingParams.scala:216`, `AzureMapsTraits.scala:23`, `OpenAIPrompt.scala:168`).
- [x] The CI leg is wired the same way as the other service legs. `pipeline.yaml:821–822` adds `PACKAGE: "services.contentunderstanding"`, which feeds `TEST_SPEC=com.microsoft.azure.synapse.ml.$(PACKAGE).**` (line 970). That matches the documented dedicated UnitTests leg.
- [x] Retry-After handling, exception summaries and resource bounds match the retained contract. Retry-After accepts at most 19 ASCII digits or an RFC 1123 date and otherwise falls back; it never shortens a valid long delay. The `ContentUnderstandingException` summary uses only allowlisted codes from bounded input. The DNS and deadline executors are bounded and HTTP clients are created per operation. This was checked by reading source only.
- [ ] I did not run any tests, builds or codegen; this review is read-only. The driver's validation results (scalastyle, compile, five Retry-After regressions, codegen, and the earlier 165-test run) are recorded as the driver's claims and were not re-verified by running anything.

## Issues

### Issue 1: Python `extractParamMap` and `hasDefault` repeat Py4J lookups for every ServiceParam
- **Severity**: Low
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: 62–73, 92–96, 108–122
- **Description**: For each ServiceParam that is not pending, `extractParamMap` calls `_java_service_value`, which makes three JVM calls (`getParam`, `isDefined`, `getOrDefault`). It then calls `.isLeft()`. For a scalar value it calls `self.getOrDefault(param)`, which runs `_resolveParam` and `_java_service_value` again before converting the value. That is roughly seven Py4J round trips per scalar ServiceParam, and the same JVM value is read twice. `hasDefault` also calls `_java_service_value` when the param is already pending in `_paramMap`. In that case `getDefault`/`getParam` decide the result and the fetched value is used only for an `isRight` check that cannot apply. This mixin is shared by every generated cognitive service wrapper, not just CU. `copy()`, save, `explainParams`, MLflow parameter enumeration and the new default `isSet` path all go through it.
- **Risk**: The cost stays on the driver and grows linearly with the number of params (CU has roughly 30 ServiceParams). It is not a correctness problem. However, it adds avoidable latency to every `copy()`, pipeline save and autologging call for all service stages, and the cost grows on high-latency Py4J gateways such as remote notebooks.
- **Suggested Fix**: In `extractParamMap`, reuse the value already fetched: `result[param] = self._service_value_to_python(value.value())` instead of calling `self.getOrDefault(param)` again. In `hasDefault`, call `_java_service_value` only when `param not in self._paramMap`. Behaviour stays the same, including the order of pending versus JVM values, column-binding omission and bytes passthrough. The existing Python lifecycle tests should still pass without changes.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver resolution

Original reviewer output above is preserved. **Issue1 resolved locally:** extractParamMap converts the scalar value it already fetched instead of re-entering getOrDefault; hasDefault avoids the effective-value lookup when a pending override exists. No caching, new state, precedence change or lifecycle consolidation was added. These duplicate reads were introduced in earlier local changes and appear in pre-polish snapshot2; they were not caused by the latest management or Retry-After corrections.

Call-count regressions against real JVM-backed CU and AnalyzeDocument stages failed before the fix and passed afterward. All32 offline Python lifecycle tests passed on current generated wrappers/compiled classes, including existing scalar/column, pending-map, raw-byte, clear/copy/save/load controls. Core/cognitive codegen, cognitive main/test style/compile, pinned Black205 unchanged, and source/generated Utils equality passed. Snapshot8 SHA256: 89a2e0462cf1a441513583ff7d96d9e228a1142e80009db3a994b018f8c97523. Input hash/runtime identity and actual tool results were audited; no feedback exposure occurred in this slot. Independent re-review remains pending.