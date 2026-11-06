---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 1
round: 6
timestamp: "2026-11-05T22:11:40Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "8d081c40fb9b43f8ab8d9a98ac22c37044ec39f54b75918bdb9c0ba7aed6404d"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-1-review-6-claude-opus-5.5.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] I read the full supplied diff for the 38-path snapshot: Scala transformer, protocol, logging, params and persistence; the `CognitiveServiceBase.scala` header changes; the generated-Python overrides; `pipeline.yaml`; and `docs/Explore Algorithms/AI Services/Content Understanding.md`.
- [x] I checked how logging works in `core/src/main/scala/com/microsoft/azure/synapse/ml/logging/SynapseMLLogging.scala`. `getPayload` (line 106) builds the payload; `logBase(info, featureName)` (line 135) emits certified events only when a feature name is given; `logTransform` (line 164) wraps a by-name block in `logVerb`. `logManagement` calls `getPayload(verb, None, Some(elapsed), None)` and then `logBase(payload, None)`. It records only the outcome and the error type, not service messages, and it runs inside `ContentUnderstandingLogging.safely` (`ContentUnderstandingLogging.scala:11`).
- [x] I compared the documented defaults with the code. `HTTPTransformer.scala:76` sets concurrency to 1 and timeout to 60.0. `concurrentTimeout` has no default and is rejected at `ContentUnderstanding.scala:114`. The `setDefault` calls in `ContentUnderstandingParams.scala` (lines 127–132 and 151–153) set maxPollAttempts 120, pollingDelay 1000, maxResponseBytes 32 MiB, maxDocumentBytes 32 MiB and maxOperationDuration 600000. These match the docs table at `Content Understanding.md:387`.
- [x] I checked the documented Retry-After rule against the contract: at most 19 digits, saturating, otherwise fall back to `pollingDelay`, and HTTP dates still supported (`Content Understanding.md:397-398`).
- [x] I checked that `pyAdditionalMethods` in `ContentUnderstandingParams.scala:214` extends `super.pyAdditionalMethods`, so the generated `isSet` override is kept. `_transfer_params_from_java` never infers Python-map membership for ServiceParams from the JVM `isSet`, which matches the contract.
- [x] I checked how the shared stop is triggered and logged. `checkSharedFailure` is at `ContentUnderstandingProtocol.scala:507-520`. The `rowHandler` compare-and-set and the `ContentUnderstandingLogging.sharedStop` call are at `ContentUnderstandingProtocol.scala:667-683`.
- [x] For performance, I checked that the DNS-capacity semaphore, its executor and the deadline scheduler are bounded with fixed sizes and cancel or remove their tasks. I also checked that the input-validation UDF is referenced in a way that Spark's projection collapsing should not duplicate. I found no unbounded thread or queue growth.
- [ ] No tests, builds or live CU calls were run. This review is read-only and I did not independently re-check the stated 158/158 run.

## Issues

### Issue 1: The shared-stop warning omits which shared failure tripped it, and its HTTP status can mislead
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingLogging.scala; cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingProtocol.scala
- **Line(s)**: ContentUnderstandingLogging.scala 23-26; ContentUnderstandingProtocol.scala 507-520, 680
- **Description**: `sharedStop(httpStatus)` logs only `SharedConfigurationFailure (HTTP <status>)`. However, `checkSharedFailure` trips on three different categories:
  - authentication (`isSharedAuthFailure`)
  - analyzer (`AnalyzerNotFound` code, or a 404)
  - model deployment (`DefaultsNotSet`, `DeploymentNotFound` or `ModelDeploymentNotFound` codes in the body, whatever the HTTP status)

  When a deployment or analyzer code arrives in a non-error status, such as a Failed body without a handle, the log shows an HTTP status that does not explain the stop. The row diagnostic text is the same generic sentence for all three categories, so operators cannot tell from logs which one stopped the partition. The warning also has no partition or stage identifier, so one line cannot be matched to its partition when several tasks stop.
- **Risk**: Triage is harder, because operators must inspect row output to tell a credential problem from a missing analyzer or deployment. This is an observability gap only; behavior is correct.
- **Suggested Fix**: Have `checkSharedFailure` record a category that the connector derives itself, using a fixed set of values such as `authentication`, `analyzer` or `deployment`. It can carry this on `ContentUnderstandingException` or a small subclass. Pass the category and `Option(TaskContext.get()).map(_.partitionId())` to `sharedStop`, and log them with the HTTP status. Do not include any service text, so the rule against echoing service messages still holds.

### Issue 2: The `pollingDelay` description reads as retry-only, but it also sets the normal poll interval
- **Severity**: Low
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingParams.scala; docs/Explore Algorithms/AI Services/Content Understanding.md
- **Line(s)**: ContentUnderstandingParams.scala 113-115; Content Understanding.md 387
- **Description**: The parameter text is now "Milliseconds between polling or management GET retries when Retry-After is absent or invalid." This reads as if `pollingDelay` applies only to retries. In fact it is also:
  - the normal interval between status polls of a Running or NotStarted operation;
  - the delay before the first poll after an accepted analyze POST or analyzer PUT (`retryDelay(raw.retryAfter, settings.pollingDelay)`).

  The same wording appears in the generated Python docstring and in the docs bounds table.
- **Risk**: Users tuning latency or service load may not realize this setting controls the steady polling rate. They may leave a slow default in place, or set it to 0 expecting it to affect only failures and end up with fast polling. Because the public Python docstring is generated from this text, the ambiguity reaches every wrapper user.
- **Suggested Fix**: Reword it, for example: "Milliseconds to wait before each operation poll, and between management GET retries, when Retry-After is absent or invalid." Make the same change in the docs table row at line 387 and regenerate the wrapper with codegen; do not edit the generated files by hand.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Category and available task identifiers implemented; asynchronous Spark-ID correlation remains limited.
- **What changed**: The classifier carries a connector-derived authentication/analyzer/deployment category in a private exception subclass, preserving the public response and catch behavior. The first-stop warning emits the fixed category, HTTP status, and available stage/partition/task-attempt identifiers. Missing context is explicitly labeled.
- **Why**: Categories explain shared failures even under HTTP 200 without copying service messages. Driver inspection of `Clients.scala` confirmed AsyncClient runs callbacks on ExecutionContext.global without propagating Spark's thread-local TaskContext. Adding shared scheduler propagation is outside the selected polish scope; no task ID is fabricated.
- **How verified**: New public tests cover all three categories at concurrency 1 and 2, sanitized payloads, available serial task IDs and explicitly unavailable async context. The pre-fix test failed on the missing authentication category; all cases pass in the final 161-test run.

### Issue 2
- **Status**: Wording corrected and regenerated-wrapper validation passed.
- **What changed**: Parameter and guide wording now explicitly includes every operation poll, including the first, separately from management GET retries.
- **Why**: The previous phrase could incorrectly imply failure retries only. Runtime delay semantics are unchanged.
- **How verified**: Cognitive codegen passed; generated class and parameter docstrings explicitly include the first/normal operation poll. Final local validation passed 161/161 tests across seven CU suites.