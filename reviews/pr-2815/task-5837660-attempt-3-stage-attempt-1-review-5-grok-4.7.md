---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 5
timestamp: "2026-11-06T19:55:55Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "e031843f12f49b6c6877db0a7e0cd81ef2ff73f78b371dc5e3c5b5ba8175a4e5"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-5-grok-4.7.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared `validateDocumentUrl` (`ContentUnderstanding.scala` 216–220) with schema-validation cases in `ContentUnderstandingSuite.scala` 495–527; only empty and `file://` document URLs are rejected.
- [x] Compared operation-location userinfo/fragment cases (`ContentUnderstandingSuite.scala` poll URL test) with document URL coverage; those checks are not reused for `documentUrl`.
- [x] Checked `customUrlRoot` rejection (`ContentUnderstanding.scala` 104) and `getFabricFallbackAuthHeader` (`ContentUnderstanding.scala` 75) against cognitive CU tests; no assertion names either path.
- [x] Read the OpenAI-defaults test (`ContentUnderstandingSuite.scala` 583–599): it asserts no subscription key and the CU API version, not absence of an Authorization header.
- [x] Spot-checked admission, recovery, concurrency, submission, Python lifecycle, and ServiceParam bridge tests for polling, shared-stop, DNS budget, bytes, and copy/load; those paths have direct assertions and are not the gaps below.

## Issues

### Issue 1: document URL userinfo and fragment checks are untested
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 216–220, 226
- **Description**: `validateDocumentUrl` rejects non-HTTP(S) URLs and any URL with user information or a fragment. `ContentUnderstandingSuite` schema validation only includes `""` and `file:///document.pdf`. Userinfo and fragment cases exist for endpoints and operation locations, not for scalar or column `documentUrl`. A column value is validated per row, so a regression would submit the URL instead of failing schema planning.
- **Risk**: A regression can send credential-bearing or fragmented document URLs to the service without a failing test.
- **Suggested Fix**: Add scalar `transformSchema` rejections for `******example.invalid/doc` and `https://example.invalid/doc#fragment`, plus one column row that returns `InvalidInput` and sends no request.

### Issue 2: inherited auth guards are not asserted
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 73–75, 104
- **Description**: `transferGlobalParamsToParamMap` and `getFabricFallbackAuthHeader` disable inherited OpenAI and Fabric credentials, and `customUrlRoot` is rejected. The OpenAI-defaults test never asserts that `authorization` is absent. Nothing calls `setCustomUrlRoot` or the Fabric override. Deleting the override would still pass the offline suite.
- **Risk**: A future edit can attach a Fabric MWC token or honor `customUrlRoot` without a test failure.
- **Suggested Fix**: Assert the unauthenticated submit has no `authorization` header. Reject `setCustomUrlRoot` before any request. Invoke `getFabricFallbackAuthHeader` and assert `None`.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

### Issue 2
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s5t1-grok-4.7-i1: URL and column-bound invalid values

- Disposition: **test-improvement**.
- Evidence: Current negative matrix and null/empty binary rows do not establish all userinfo/fragment/column option cases. Add focused no-dispatch cases, not a broad coverage chase.
- Consolidated topic: `coverage-row-values`.

### s5t1-grok-4.7-i2: Driver-only and inherited-auth guards

- Disposition: **test-improvement**.
- Evidence: Targeted tests should prove pre-dispatch customUrlRoot rejection, driver-only management and absence of inherited auth; test-only proposals are distinct from demonstrated failures.
- Consolidated topic: `coverage-guards`.
