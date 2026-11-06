---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 3
timestamp: "2026-11-06T19:28:44Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "e3456a1c7d76fc23c4862008a6ec2613feb20bc89a32bda1ad3910891118cca3"
---

I found one issue: a Low-severity Python bridge inconsistency.

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-3-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] **Submit and poll state machine:** reviewed in the supplied diff of `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala`. Covered `decodeSubmission`, safe-submission assessment, `retainPollState` and the `pollWithClient` termination conditions. Accepted or uncertain submissions keep their operation handle and are never replayed. Running and NotStarted results with an empty `result` are not reported as success.
- [x] **Deadline and transport failures:** checked in the same file. Interrupts are checked separately. `InterruptedIOException` and `InterruptedException` are handled apart from `NonFatal`. After the deadline fires, `release` skips `releaseConnection`. Late failures keep the handles that are already available.
- [x] **DNS bounding:** checked in `ContentUnderstandingDnsResolver.scala` (diff). The semaphore is acquired before submit and released both in the task `finally` and on executor rejection. The deadline is rechecked after acquisition, so permits cannot leak on rejection or timeout.
- [x] **Shared-stop circuit:** checked in Protocol (diff). The AtomicBoolean is created per deserialized task closure. Accepted-handle and Unknown outcomes skip auth or shared-failure classification. Per-row column auth is excluded from shared stops through `rowUsesColumnAuth` and `allAuthScalar` (`ContentUnderstanding.scala`, diff).
- [x] **Provenance marker stays off the wire:** the row-auth marker header is stripped before sending, at `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\HTTPSchema.scala:188-213` (strip at line 209). CU's direct `exchange` uses `toHTTPCore`, so the marker never reaches the service.
- [x] **Header resolution:** checked `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\CognitiveServiceBase.scala:376-432`. Auth precedence ignores blank values. Telemetry and custom headers cannot inject auth. The new `addContentType` replaces case-variant Content-Type keys, a deliberate global de-duplication. CU overrides `getFabricFallbackAuthHeader` to `None` and `transferGlobalParamsToParamMap` to a no-op, so inherited OpenAI and Fabric credentials are excluded.
- [x] **Input validation:** checked in `ContentUnderstanding.scala` (diff):
  - `validateConfiguration` rejects non-finite or oversized timeouts, `concurrentTimeout`, `customUrlRoot`, and empty or same-named output/error columns, comparing names with the resolver.
  - `validateParamSchema` rejects missing or ambiguous columns and wrong types.
  - `validateValue` rejects nulls, empty values, oversized bytes, and documentUrls with user info or fragments.
  - Row-level validation throws only `IllegalArgumentException`, which is the only exception caught.
- [x] **Spark planning:** Spark 3.5 CollapseProject does not inline non-deterministic or expensive UDFs that are referenced more than once, so the row-error and parser UDFs are not evaluated twice per row. `findUnusedColumnName` now uses a case-insensitive default resolver (`core\src\main\scala\...\core\schema\DatasetExtensions.scala`, diff).
- [x] **Proxy handling, ruled out:** the per-row `HttpClients.custom()` does not call `useSystemProperties`. I did not report this because master's shared clients don't either (`core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\HTTPClients.scala:55` and `RESTHelpers.scala:33` use `HttpClientBuilder` without it), so it is not a regression.
- [x] **Python bridge:** checked `core\src\main\python\synapse\ml\core\schema\Utils.py:130-139` (`_service_param_scalar_to_python`) and `:171-220` (`_service_value_to_python`, `getOrDefault`, `extractParamMap`) against `cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py:685-705`. Issue 1 comes from this comparison.
- [x] **Validation evidence:** I relied on the supplied evidence (scalastyle, compile, codegen, 57 offline Python tests, Black). I did not run any tests or commands myself.
- [ ] **Live service and cluster validation:** not applicable to this read-only review. There were no live CU requests, packaged-runtime runs or Azure CI runs; these remain the documented external gates.

## Issues

### Issue 1: Generic ServiceParam inspection returns raw py4j JavaObjects for non-Map collection scalars
- **Severity**: Low
- **File**: `core\src\main\python\synapse\ml\core\schema\Utils.py`; test that relies on the behavior: `cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py`
- **Line(s)**: Utils.py 171-187 (`_service_value_to_python`), 195-203 (`getOrDefault`), 205-220 (`extractParamMap`), compared with 130-139 (`_service_param_scalar_to_python`); test lines 693-696
- **Description**: The new JVM-backed scalar view converts values with `_service_value_to_python`. That function special-cases only bytes and `scala.collection.Map`; everything else goes to `_java2py`. `_java2py` does not convert Scala `Seq`/`List` values, so a sequence-valued ServiceParam comes back as an opaque py4j `JavaObject`. Two cases show this:
  - Setting `TextSentiment().setText(["synthetic text"])` makes `getOrDefault(param)` return a JVM proxy. The new test asserts this with `.size()` and `.apply(0)`.
  - `extractParamMap()` puts the same proxy into the returned dict.

  The generated getters use `_service_param_scalar_to_python` (lines 130-139), which falls back to `jsonEncode(Left(value))` and returns a Python list. The same parameter therefore has two different Python representations, depending on whether it is read through `getText()` or through the generic Params API.
- **Risk**: Generic consumers of `extractParamMap()` and `getOrDefault()` get gateway-bound objects instead of Python values. Examples include MLflow parameter logging (whose docs this PR changes), Python-side copy and introspection tools, and JSON or pickle serialization. These consumers may fail to serialize the value, log a Scala `toString` such as `List(synthetic text)` instead of the value, or hold proxies that become invalid once the gateway or session ends. The new test codifies the inconsistent behavior, so a later fix would look like a regression.
- **Suggested Fix**: Make `_service_value_to_python` take the parameter name and, after the Map branch, reuse the existing fallback. When `_java2py` still returns a `JavaObject`, call `self._java_obj.getParam(name).jsonEncode(Left.apply(value))` and return `json.loads(...)["left"]`. The simplest option is to call `_service_param_scalar_to_python(param.name, value)` for non-Map values. Update test lines 695-696 to `assertEqual(service.getOrDefault(param), ["synthetic text"])`. Add one `extractParamMap()` assertion that the value is a Python list.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s3t2-claude-opus-5.5-i1: Generic collection inspection returns JVM proxies

- Disposition: **confirmed**.
- Evidence: TextSentiment typed getter returns a list; generic getter/extractParamMap return JavaObject for the same value. cu-native-review-probes.log.
- Consolidated topic: `sequence-projection`.
