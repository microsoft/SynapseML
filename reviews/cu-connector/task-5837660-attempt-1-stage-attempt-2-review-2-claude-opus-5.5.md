---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 2
round: 2
timestamp: "2026-11-05T19:40:45Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "61fcba2d53dc1cf848890a76dd845718255ad2b38195fd5144d2c2469ae6cbdc"
---

## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-2-review-2-claude-opus-5.5.md
- **Issues Found**: 5
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Checked the Fabric marker in `core\src\main\scala\com\microsoft\azure\synapse\ml\io\http\HTTPSchema.scala`:
  - Lines 171-174 (`usesFabricAuth` requires value `"true"`), line 209 (strip before wire) and lines 218-221 (`FabricAuthMarkerHeader`).
  - Checked against `CognitiveServiceBase.scala` 554-561 (shared marker handling) and the CU reuse at `ContentUnderstanding.scala` 329-332 and `ContentUnderstandingProtocol.scala` 102 and 488-491.
- [x] Confirmed the CU transport sends through `data.toHTTPCore` (`ContentUnderstandingProtocol.scala`:367), so the marker is stripped before the wire. Not flagged as a leak.
- [x] Compared the CU `transform` override (`ContentUnderstanding.scala` 61-66) with the base `CognitiveServicesBaseNoHandler.transform` (`CognitiveServiceBase.scala` 765-769).
  - The CU override keeps `transferGlobalParamsToParamMap` and `logTransform`.
  - It replaces only the resolver-aware internal transformer.
  - Schema-only `getInternalTransformer` uses the active-session `SQLConf.get.resolver` (line 355-356), as the contract requires.
- [x] Checked the `pyAdditionalMethods` chain.
  - Every override (PyCodegenSuite, AzureMapsTraits, OpenAIPrompt, ContentUnderstandingParams:214, CognitiveServiceBase:284/636/679) calls `super.pyAdditionalMethods + ...`, so the default `isSet` (Wrappable.scala 83-92) reaches all generated classes.
  - All generated classes inherit `ComplexParamsMixin` (Wrappable.scala 113-114).
- [x] Checked `core\src\main\python\synapse\ml\core\schema\Utils.py` 61-200 (the `ComplexParamsMixin` overrides `hasDefault`, `getOrDefault`, `extractParamMap`, `explainParam`, `clear` and `_transfer_params_*`).
- [x] Checked stage conventions from AGENTS.md:
  - `ComplexParamsReadable` companion, `uid` constructor and no-arg random-UID constructor.
  - `logClass(FeatureNames.AiServices.ContentUnderstanding)`, `defaultCopy` and the `saveDefaultParamsAsExplicit` opt-in.
  - No RDD usage was introduced.
- [x] Confirmed the CU test relocation: the seven suites are under `services\contentunderstanding\` and none remain under `services\form\contentunderstanding`.
- [x] Searched for operation-mode string literals across `services\contentunderstanding` (`"analyze"`, `"submit"`, `"poll"`).
- [ ] No tests or commands were run. This was a read-only review under the resolved plan.

## Issues

### Issue 1: CU row-auth provenance reuses the Fabric implicit-auth marker header
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala; cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: ContentUnderstanding.scala 329-332; ContentUnderstandingProtocol.scala 102, 488-491
- **Description**: CU marks "a row-bound credential was selected" by adding `HTTPRequestData.FabricAuthMarkerHeader` (`X-SynapseML-Implicit-Fabric-Auth`) with a new value, `content-understanding-row-auth`.
  - That header is a shared core abstraction meaning "implicit Fabric fallback auth was used."
  - Shared code in `HTTPSchema.scala` 171-174 and `CognitiveServiceBase.scala` 554-561 removes, re-adds and interprets that header.
  - The CU meaning is unrelated. It is kept separate only by a value comparison.
- **Risk**: The same header now carries two unrelated meanings.
  - A future shared change could check only for the marker's presence (for example, using `isFabricAuthMarker(name)` alone), or call `addHeaders` after the CU marker is added. Either would silently misclassify CU rows as Fabric-auth, or drop the CU provenance.
  - Losing the provenance changes the shared-stop classification in `FailureClassifier.isSharedAuthFailure`, which is a behavioral-contract area (column auth must stay row-level).
- **Suggested Fix**: Add a separate, generically named internal provenance header in `HTTPRequestData`, with its own `private[ml]` constant and predicate, and strip it in `toHTTPCore` alongside the Fabric marker. Alternatively, carry row-auth provenance outside the headers, for example as a flag computed next to the request in the CU row handler.

### Issue 2: Core Python mixin hardcodes cognitive-service credential parameter names
- **Severity**: Low
- **File**: core\src\main\python\synapse\ml\core\schema\Utils.py
- **Line(s)**: 125-147
- **Description**: `ComplexParamsMixin.explainParam` in core redacts by a hardcoded list of names: `subscriptionkey`, `aadtoken`, `customauthheader`, `customheaders` and `telemheaders`.
  - These parameters are defined in the cognitive module's `HasSubscriptionKey`, `HasAADToken`, `HasCustomAuthHeader`, `HasCustomHeaders` and `HasTelemHeaders`.
  - Core therefore depends on knowledge of the downstream cognitive module.
  - Matching is by lowercase name, so any core or other-module ServiceParam with a colliding name is also redacted. A new credential ServiceParam added in cognitive is not redacted unless someone edits core.
- **Risk**: The redaction policy can drift from the set of credential parameters. A future credential param could be printed by `explainParams()`, and the ownership boundary between core and cognitive is blurred.
- **Suggested Fix**: Declare the redaction set beside the credential params. One option is a JVM-side marker (such as a `isSensitive` flag on `ServiceParam`, or a `sensitiveParamNames` member on the cognitive base) that Python reads through `_java_obj`. Another is a class attribute emitted by cognitive codegen that the mixin consults.

### Issue 3: The ServiceParam `isSet` override is injected through codegen instead of living with the other ServiceParam overrides
- **Severity**: Low
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
- **Line(s)**: 83-92 (plus the `isSet` exclusion in the stub generator)
- **Description**: `hasDefault`, `getOrDefault`, `extractParamMap`, `explainParam`, `clear` and `_java_service_value` are all runtime methods on `ComplexParamsMixin` (Utils.py 61-200).
  - `isSet`, which is part of the same ServiceParam lifecycle, is instead emitted as a Python string in the default `PythonWrappable.pyAdditionalMethods`.
  - It is therefore copied into every generated class and depends on `_java_service_value` from the mixin.
  - It also needs a special case in the stub generator.
  - It survives only because every existing `pyAdditionalMethods` override happens to call `super`.
- **Risk**: The ServiceParam Python lifecycle is split across two places.
  - Any future `pyAdditionalMethods` override that does not call `super` silently loses scalar `isSet` semantics for that class, breaking the contract that pending and JVM state form one lifecycle.
  - Generated code and the runtime mixin can also drift apart.
- **Suggested Fix**: Move `isSet` into `ComplexParamsMixin` next to `hasDefault`, with the same `param.doc.startswith("ServiceParam:")` guard. Return `pyAdditionalMethods` to an empty default, and remove the stub-generator special case.

### Issue 4: `handlingFunc` is overridden to throw, so the stage breaks the base-class contract
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 339-341 (also 355-356 and 390 onward)
- **Description**: `ContentUnderstanding` extends `CognitiveServicesBaseNoHandler`, whose abstract contract is that `handlingFunc` performs the request.
  - CU overrides it to throw `UnsupportedOperationException` and routes execution through its own `buildInternalTransformer` and partition `rowHandler`.
  - The class therefore inherits an API surface it cannot honor (a Liskov violation). This deviates from the pattern used by sibling services, which either use the base handler or a `HasAsyncReply`-style handler.
- **Risk**: Any shared code that starts calling `handlingFunc` or the base `getInternalTransformer` path generically will fail at runtime only for CU. Base-class refactors must also remember this exception.
- **Suggested Fix**: Introduce a narrower base, or a trait split, without `handlingFunc`: for example, factor the shared auth, header and URL plumbing out of `CognitiveServicesBaseNoHandler` so CU can mix it in without inheriting the handler contract. If that is out of scope, document the exception on the base class and add a test that the base path is never reached for CU.

### Issue 5: Operation modes and connector states are compared as raw strings
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala; ContentUnderstandingProtocol.scala; ContentUnderstandingParams.scala
- **Line(s)**: ContentUnderstanding.scala 90, 135-136, 252, 283, 337; ContentUnderstandingProtocol.scala 473, 631, 638, 666; ContentUnderstandingParams.scala 101, 129
- **Description**: The literals `"analyze"`, `"submit"` and `"poll"` are repeated in the param validator, the defaults, settings construction, request preparation and the row handler.
  - Connector statuses (`"Running"`, `"NotStarted"`, `"Unknown"`, `"Rejected"`, and so on) are likewise matched as string sets in the protocol code.
  - Only some status names are centralized.
- **Risk**: A typo or a future mode or status rename compiles cleanly but silently changes behavior, for example in the shared-stop gating (`mode != "poll"`) or the resumability checks. This is fragile in code whose contract depends on exact mode and status semantics.
- **Suggested Fix**: Define a private `OperationModes` object (`Analyze`, `Submit`, `Poll`, `All`) and a status-constants object in the CU package. Reference them from the validator, the defaults and all comparisons. The serialized string values stay the same, so persisted parameters are unaffected.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Deferred internal-metadata separation recommendation.
- **What changed**: No source change or additional shared header/serialized request field.
- **Why**: Current Fabric detection requires the exact value `true`, while CU uses a different value and adds it after shared header assembly. Both markers are stripped before sending. The reported failure depends on a future change that removes these distinctions; it is not a demonstrated current classification or wire-leak defect. Preserve the existing tested provenance mechanism until a separate migration proves all paths equivalent.
- **How verified**: Driver read `HTTPSchema.scala:169-221` and the current CU header ordering/classification. The reviewer also explicitly verified stripping before the wire. Existing provenance, scalar/column auth and partition-stop regressions passed.

### Issue 2
- **Status**: Deferred redaction-policy extensibility recommendation.
- **What changed**: No new sensitivity metadata API or generated class attributes.
- **Why**: The current five auth-capable parameter names are covered by the existing redaction policy and tests. The report identifies possible future policy drift, not an existing credential omitted from descriptions. A generalized cross-module sensitivity framework is beyond the frozen connector correction; standard sensitive persistence remains explicitly approved and documented.
- **How verified**: Driver read `Utils.py:125-147`, the retained five-parameter auth contract and the Python description-redaction coverage.29 Python tests passed; no actual future-param support or secret-free persistence is claimed.

### Issue 3
- **Status**: Deferred; duplicate of stage2 attempt1 Opus Issue1.
- **What changed**: No source change.
- **Why**: All current overrides preserve the base method. Moving implementation ownership remains a shared codegen/mixin refactor, not a demonstrated current failure.
- **How verified**: See the first architecture artifact's driver disposition and the existing runtime/stub tests. Retain the earlier Medium severity for the combined tracking item rather than erase it with this Low restatement.

### Issue 4
- **Status**: Documentation alternative addressed after user instruction; duplicate of stage2 attempt1 Opus Issue3.
- **What changed**: Added class-level explanation of the bypassed base pipeline and fail-closed hook. No narrower shared base or live fallback handler introduced.
- **Why**: The explicitly bypassed hook fails closed so execution uses the snapshot-bound handler. Refactoring the shared base for a possible future call is outside the current defect correction.
- **How verified**: Current transform and schema paths delegate through the snapshot-aware builder; the exception identifies that path. See first architecture artifact for evidence and retained documentation follow-up.

### Issue 5
- **Status**: Deferred constant-centralization recommendation.
- **What changed**: No new mode/status objects or broad replacement.
- **Why**: The current validator accepts exactly analyze/submit/poll and no actual mistyped or mismatched state literal was identified. Centralization can reduce future maintenance risk but is not a current behavior correction; avoid editing state-machine comparisons merely for uniformity during this scope freeze.
- **How verified**: Driver read `ContentUnderstandingParams.scala:99-129`; existing tests exercise all three modes and creation/poll status handling. No claim of exhaustive future-typo prevention is made.

## Driver decision and stage outcome

- High/direct stage2 attempt2, human-selected GPT/Opus `high`, Grok `low`; primary `gpt-6-astra`, current frontier IDs `gpt-6.1-sol`, `claude-opus-5.5`, `grok-4.7`. Broad36-path contribution with qualitatively high state/concurrency/lifecycle complexity. Two completed High attempts is the explicit stage limit; unresolved findings stay visible.
- GPT clean in148.701s, Grok clean in130.452s, Opus issues_found in158.186s. Driver audited all45/18/15 read-only tool calls respectively, matching input hashes and runtime identities; no prior/sibling feedback access or tool failures. All three artifacts are valid and retained.
- There are nine distinct deferred architecture recommendations across both attempts: six from attempt1 plus three new items here. Two current findings duplicate earlier ones. None is marked fixed or silently cleared by another reviewer's clean response.
- **Stage2 outcome: advanced-with-unresolved under the High gate, not clean.** Move to stage3 edge cases/robustness with the same frozen source and prior143 Scala/29 Python validation. No production/test changes or new validation run occurred during architecture triage. Current-target integration, live CU and external CI remain outside this local review evidence.