---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 13
stage_attempt: 1
round: 1
timestamp: "2026-11-05T23:04:28Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "a8d05d2a4518c365679d3275053d1b448df9eb128b2f5fe99d13bbd5bb6975d6"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: reviews/pr-2781/task-unknown-attempt-13-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

I found no Critical, High, or Medium issues. Both findings are Low and non-blocking. Neither is a regression in behavior that worked before this PR.

## Evidence Checklist
- [x] **Serializer change.** Checked `core/src/main/scala/org/apache/spark/ml/ComplexParamsSerializer.scala` lines 112–118 (writer) and 150–168 (reader).
  - Only ServiceParams that are not `isSet` are left out of `paramMap`.
  - Ordinary defaults are still written explicitly. Explicit ServiceParams stay explicit, including values equal to the default.
  - Every non-Complex default is still written to `defaultParamMap`, and `getAndSetParams` restores it as a default.
  - The reader guard at `core/src/main/scala/org/apache/spark/ml/Serializer.scala:32` is unchanged.
- [x] **Python side of the serializer change.** In `core/src/main/python/synapse/ml/core/schema/Utils.py`, ServiceParams are never given Python defaults or pushed as defaults, so the new filter is safe for Python. Ordinary uid-based defaults such as `outputCol` keep the old behavior, which `cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py` lines 72–97 covers.
- [x] **Prompt guard.** Checked `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` lines 663–697.
  - The guard condition is `isDefined(model) && !isSet(deploymentName) && !hasAIFoundryModel`.
  - It allows only v1 or Responses; otherwise it throws.
  - Precedence holds: an explicit, column-bound or global deployment wins over a model alias, which wins over the Luna default.
  - Before this PR, a model-only setup on legacy Chat also failed, because no deployment was resolved. So the guard is not a regression.
- [x] **Prompt `transformSchema`.** Checked `OpenAIPrompt.scala` lines 49–62 and 750–754.
  - `copy(ParamMap.empty)` keeps defaults as defaults and keeps `postProcessingExplicitlySet`.
  - Globals are transferred only to the copy, so the original stage's unset params and credentials are not changed or saved.
  - `transform` (lines 530–531) is unchanged.
  - `apiType` is a global-registered Param (lines 188–192), so the copy now resolves it the same way `transform` does.
- [x] **Cache options.** Checked `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala` lines 326–350 and 463–469, and `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` lines 23–61.
  - A null member is written as JSON `null`.
  - A null value for the whole column makes `getValueOpt` return None, so the field is omitted.
  - Nested Row, Map and Seq values are converted. Save and load round-trip through `anyFormat.read`.
- [x] **Breakpoint validation.**
  - Shared check: `OpenAI.scala` lines 39–47.
  - Chat: legacy map content is rejected at `OpenAIChatCompletion.scala` lines 307–315, before text collapse. The shared check runs at line 385, and the allowed fields are at lines 348 and 356.
  - Responses: allowed fields at `OpenAIResponses.scala` lines 421, 429 and 447; the shared check runs at line 472.
  - Null values are dropped by `mapBackedContentParts` and `encodeStructuredMap`, so null markers are accepted.
  - Errors go to errorCol through the validation UDF before any HTTP call (Chat lines 165–200; Responses validation helpers).
- [x] **Foundry and Fabric.** Checked `CognitiveServiceBase.scala` lines 271–274 and 718–720, `OpenAI.scala` lines 558–564 and 584–595, and `core/src/main/scala/com/microsoft/azure/synapse/ml/fabric/OpenAIFabricSetting.scala` lines 21–65. The Foundry internal endpoint is `/cognitive/foundry/`, so the model-status check is skipped. With the Luna default on the Fabric OpenAI endpoint, an unavailable model gives a clear `ModelNotFound` error. Before this PR, an unset deployment failed.
- [x] **Tests and notebook.**
  - Read `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIRuntimeDefaultsSuite.scala` lines 47–233 and the test names in `OpenAIPromptCacheSuite.scala`.
  - The temporary `allowUnsafeJavaDeserialization` setting is scoped and restored in `finally` in `test_ServiceParamPythonBridge.py` lines 40–55 and `test_OpenAIResponseSchema.py` lines 217–228.
  - The notebook (`docs/Explore Algorithms/OpenAI/OpenAI.ipynb` lines 308–316 and 957–960) correctly explains the null versus absent-key rule and the legacy-map error.
- [x] **Checked and not raised:**
  - `dropSamplingForGpt5` matches only `gpt-5` (`OpenAIResponses.scala` lines 203–209). This is existing behavior, and the notebook tells users to leave sampling params unset.
  - The schema copy adds one constructor telemetry event. That is small next to the existing one-per-row construction in `prepareEntity`.
  - The legacy model-only setup now fails at schema time. This is intended, and a test covers it.
- [ ] I did not run any tests. This review was read-only, and I relied on the validation evidence provided.

## Issues

### Issue 1: Some Spark value types in options columns fail the whole Spark job instead of writing a row error
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAI.scala
- **Line(s)**: 343–350 (`normalizeCacheOptionValue`), 463–469; also `core/src/main/scala/com/microsoft/azure/synapse/ml/param/UntypedArrayParam.scala` 23–44 and `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/CognitiveServiceBase.scala` 595–614
- **Description**: `normalizeCacheOptionValue` passes leaf values through unchanged, and `AnyJsonFormat.write` throws for any type it does not support. That includes `java.math.BigDecimal` (Spark `DecimalType`), dates and timestamps, binary data, Scala `Array`, and map keys that are not strings. Serialization happens in `prepareEntity`, which runs inside `inputFunc` with no error handling. Unlike the breakpoint checks, this never reaches the errorCol validation UDF. Spark SQL fractional literals are `DecimalType` by default; for example, `named_struct('mode','explicit','x',1.5)` gives `decimal(2,1)`. So a struct options column built in SQL can trigger this.
- **Risk**: One row with such a value fails the Spark task and, after retries, the whole job, instead of writing a row-level error. It is also a small gap in the spec's "preserve native JSON types" goal, because decimals are JSON numbers. This is unlikely today because the documented options are strings and booleans, and it is new-feature scope rather than a regression.
- **Suggested Fix**: Write `BigDecimal` values (Java or Scala) as `JsNumber`, either in `normalizeCacheOptionValue` or as an added case in `AnyJsonFormat.write`. For dates, timestamps, binary data, arrays and non-string keys, either convert them or reject them with a clear message, ideally in the existing per-row validation step so the error goes to errorCol. Add a struct-options test that uses a SQL decimal literal.

### Issue 2: `AIFoundryChatCompletion` inherits the `gpt-6-luna` deployment default
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/aifoundry/AIFoundryChatCompletion.scala
- **Line(s)**: 50–51 (`extends OpenAIChatCompletion`), 62–64; default set at `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIChatCompletion.scala` 116–119
- **Description**: The spec limits the fallback to `OpenAIChatCompletion`, `OpenAIResponses` and `OpenAIPrompt`. Because `AIFoundryChatCompletion` extends `OpenAIChatCompletion`, it also gets `deploymentName -> "gpt-6-luna"`. It shows up in `getDeploymentName()`, `explainParams()`, Python getters and the saved `defaultParamMap`. I found no effect on requests:
  - the URL root is `models/chat/completions`;
  - the required `model` param stops `withV1DeploymentModel` from substituting the deployment;
  - the Fabric `foundry` endpoint skips `assertModelStatus`.
- **Risk**: It is confusing that a Foundry stage reports a Luna deployment. Any future code path that reads `deploymentName` for Foundry stages would silently pick Luna, which would undermine the requirement to keep Foundry model selection unchanged.
- **Suggested Fix**: Either keep the default off Foundry stages, for example by using a hook that `AIFoundryChatCompletion` overrides or by clearing the default through a helper in the `org.apache.spark.ml` package, or document the inherited default. If you keep it, add a regression test that a Foundry request never uses `deploymentName`, including with a v1-style URL.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

### Issue 2
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Repository policy requires six independent precommit lenses; Medium satisfies that policy for this bounded cross-language change.",
    "model": "Installed review-code complementary-family rule and current task-tool catalog.",
    "schedule": "Trusted target .github/skills/synapseml-pr-loop/references/review-gauntlet.md requires sequential lenses unless parallel execution is requested.",
    "budget": "First of two additional precommit passes explicitly approved under cap fourteen. Twelve consumed before this dispatch; no budget reset."
  },
  "changeSize": "Thirty-four source/test/notebook paths in the complete staged PR diff, 128523 bytes. Production changes are small; fixtures, cache examples, and regression coverage account for most of the breadth.",
  "cyclomaticComplexity": "Low new branching, with moderate interaction risk across shared service parameters, three text stages, request serialization, generated Python wrappers, global overrides, and parameter persistence.",
  "rationale": "Review the complete cache feature and unset-deployment fallback. Include implicit service-default persistence without changing ordinary default serialization, non-mutating global-aware Prompt schema resolution, the explicit legacy Chat model-only error, and consistent breakpoint object-shape validation. Neither old-head CI nor an earlier review qualifies this patch.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoningEffort": "max",
  "reviewerEligibility": "claude-opus-5.5 is the available Anthropic Opus frontier model in the live catalog and supports maximum reasoning, complementary to the OpenAI primary.",
  "overrideSource": ".github/skills/synapseml-pr-loop/references/review-gauntlet.md at d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193",
  "mode": "sequential",
  "reviewAttempt": 13,
  "stageAttempt": 1,
  "manifestSha256": "88cb68abedfd53cb5ce09f7741da65875f3c9b6b7e94efb14e2c76ad90122b98",
  "diffSha256": "4383563d729d4e2a0c4426601b5e2828695acd5cc56a307646c2215c26d7d141",
  "dispatchStatus": "not_started_local_gates_passed",
  "finalReview": "Separate CI-qualified six-lens pass remains unstarted; zero of two final passes used."
}
